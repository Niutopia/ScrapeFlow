import importlib.util
import hashlib
import http.client
import io
import json
import os
import posixpath
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock


TEST_STATE = tempfile.TemporaryDirectory(prefix="scrapeflow-server-tests-")
os.environ["SCRAPEFLOW_STATE_DIR"] = TEST_STATE.name

SERVER_PATH = Path(__file__).resolve().parents[1] / "server.py"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "job-phases.json"
spec = importlib.util.spec_from_file_location("scrapeflow_local_server", SERVER_PATH)
assert spec and spec.loader
server = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = server
spec.loader.exec_module(server)


def media_audit_fixture() -> dict[str, object]:
    return {
        "version": 3,
        "audited_at": "2020-01-01",
        "snapshot_at": "2020-01-01T00:00:00+00:00",
        "total_missing": 0,
        "optional_missing": 0,
        "orphan_subtitles": 0,
        "metadata_issues": 0,
        "audited_projects": 0,
        "shows": [],
        "missing_subtitles": [],
        "subtitle_policy": {},
        "boundary_notes": [],
        "fixed_issues": [],
        "completed_checks": [],
    }


def title_closure_fixture(
    *,
    episode_gaps: list[dict[str, object]] | None = None,
    confirmed_subtitle_gaps: int = 0,
    pending_subtitle_verification: int = 0,
) -> dict[str, object]:
    gaps = list(episode_gaps or [])
    return {
        "schema_version": 1,
        "evidence_sha256": "e" * 64,
        "source_plan_sha256": "p" * 64,
        "audited_at": "2026-08-03T00:00:00+00:00",
        "episode_gaps": gaps,
        "subtitle_refinement": {"actions": []},
        "summary": {
            "episode_gap_count": len(gaps),
            "confirmed_subtitle_gap_count": confirmed_subtitle_gaps,
            "pending_subtitle_verification_count": pending_subtitle_verification,
            "complete": not gaps
            and confirmed_subtitle_gaps == 0
            and pending_subtitle_verification == 0,
        },
    }


def write_accepted_scrape_evidence(
    job: object, *, chinese_subtitle_gap_count: int = 0,
    external_sidecar_count: int = 0,
) -> None:
    """Persist the minimum authenticated local contract for gate tests."""
    media_type = "movie" if job.media_type == "movie" else "tv"
    target_root = (
        "/quark/影视/电影/已验收作品 (2026)"
        if media_type == "movie"
        else "/quark/影视/番剧/已验收作品"
    )
    main_video_path = (
        f"{target_root}/Example (2026).mkv"
        if media_type == "movie"
        else f"{target_root}/Season 01/Example S01E01.mkv"
    )
    plan = {"mode": media_type, "files": [], "target_root": target_root}
    plan_sha256 = server.canonical_digest(plan)
    server._atomic_json(job.directory / "media-plan.json", {
        "schema_version": 4,
        "plan": plan,
        "plan_sha256": plan_sha256,
    })
    server._atomic_json(job.directory / "media-journal.json", {"success": True})
    targets = [{
        "media_type": media_type,
        "target_root": target_root,
        "category": "电影" if media_type == "movie" else "番剧",
        "tmdb_id": 42,
        "title": "已验收作品",
    }]
    closure = {
        "schema_version": 1,
        "source_scope_kind": "signed_media_plan",
        "source_plan_sha256": plan_sha256,
        "title_targets": targets,
        "title_targets_sha256": server.canonical_digest(targets),
        "episode_gaps": [],
        "subtitle_refinement": {
            "confirmed_missing_chinese": [
                {"video_path": main_video_path}
                for index in range(chinese_subtitle_gap_count)
            ],
            "pending_review_or_probe": [],
            "resolved_with_chinese": (
                [{
                    "video_path": main_video_path,
                    "resolution": "embedded_chinese_confirmed",
                }] if chinese_subtitle_gap_count == 0 else []
            ),
        },
        "summary": {
            "episode_gap_count": 0,
            "confirmed_subtitle_gap_count": chinese_subtitle_gap_count,
            "pending_subtitle_verification_count": 0,
            "complete": chinese_subtitle_gap_count == 0,
        },
    }
    closure["evidence_sha256"] = server.canonical_digest(closure)
    server._atomic_json(job.directory / "title-closure.json", closure)
    job.plan_summary = {
        "title_closure": server._title_closure_projection(closure),
    }
    hierarchy = {
        "status": "canonical",
        "canonical_root": target_root,
        "work_tree_count": 1,
        "unexpected_outer_directory_count": 0,
        "split_same_work_root_count": 0,
        "noncanonical_path_count": 0,
        "hierarchy_kind": (
            "movie_directory" if media_type == "movie"
            else "tv_series_season"
        ),
    }
    if media_type == "movie":
        hierarchy.update({
            "movie_directory_count": 1,
            "nested_title_directory_count": 0,
        })
    else:
        hierarchy.update({
            "season_directory_count": 1,
            "episode_outside_season_count": 0,
        })
    metadata = (
        {
            "contract": "movie",
            "movie_nfo_present": True,
            "required_movie_nfo_count": 1,
            "present_movie_nfo_count": 1,
            "movie_poster_present": True,
            "required_artwork_count": 1,
            "present_artwork_count": 1,
            "missing_nfo_paths": [],
            "missing_artwork_paths": [],
        }
        if media_type == "movie"
        else {
            "contract": "tv",
            "series_nfo_present": True,
            "required_episode_nfo_count": 1,
            "present_episode_nfo_count": 1,
            "series_poster_present": True,
            "required_season_poster_count": 1,
            "present_season_poster_count": 1,
            "missing_nfo_paths": [],
            "missing_artwork_paths": [],
        }
    )
    work = {
        "media_type": media_type,
        "target_root": target_root,
        "tmdb_id": 42,
        "title": "已验收作品",
        "excluded_roots": [],
        "inventory": {
            "refresh": True,
            "directory_count": 2,
            "file_count": 4,
            "inventory_sha256": "1" * 64,
        },
        "hierarchy": hierarchy,
        "media": {
            "main_video_count": 1,
            "main_video_paths": [main_video_path],
            "duplicate_main_video_count": 0,
            "duplicate_groups": [],
        },
        "metadata": metadata,
        "residuals": {
            "novel": 0,
            "manga": 0,
            "docx": 0,
            "ncop": 0,
            "detached_audio": 0,
            "other_non_feature": 0,
            "items": [],
        },
        "subtitles": {
            "video_count": 1,
            "videos": [],
            "chinese_subtitle_gap_count": chinese_subtitle_gap_count,
            "external_sidecar_count": external_sidecar_count,
            "duplicate_external_sidecar_count": 0,
            "external_sidecars": [
                f"{target_root}/candidate-{index}.ass"
                for index in range(external_sidecar_count)
            ],
            "scope_root": target_root,
        },
    }
    video_path = work["media"]["main_video_paths"][0]
    sidecars = list(work["subtitles"]["external_sidecars"])
    internal = "absent" if chinese_subtitle_gap_count else "embedded_chinese"
    chinese_status = "missing" if chinese_subtitle_gap_count else "satisfied_internal"
    work["subtitles"]["videos"] = [{
        "video_path": video_path,
        "internal_chinese_status": internal,
        "embedded_probe": {
            "status": (
                "no_subtitle_stream" if chinese_subtitle_gap_count
                else "embedded_chinese"
            ),
        },
        "external_sidecar_count": external_sidecar_count,
        "external_sidecars": sidecars,
        "external_evidence": [{
            "path": path,
            "method": "text_content",
            "status": "non_chinese",
            "classification": {"status": "non_chinese"},
        } for path in sidecars],
        "chinese_status": chinese_status,
    }]
    completion = {
        "schema_version": 2,
        "kind": "ordinary_title_completion",
        "source_plan_sha256": plan_sha256,
        "title_closure_sha256": closure["evidence_sha256"],
        "title_targets_sha256": closure["title_targets_sha256"],
        "audited_at": "2026-08-04T00:00:00+00:00",
        "policy": {
            "scope": "signed_exact_title_roots",
            "inventory": "fresh_exhaustive_alist_list",
            "embedded_subtitle": "ffprobe_each_main_video",
            "mks_subtitle": "ffprobe_container_not_text_parser",
            "maximum_external_sidecars_per_video_without_internal_chinese": 1,
            "remote_mutations": False,
        },
        "source_departure": {
            "source_path": job.source,
            "source_parent": str(server.PurePosixPath(job.source).parent),
            "source_name": server.PurePosixPath(job.source).name,
            "refresh": True,
            "absent_from_unscraped_root": True,
            "parent_inventory_sha256": "2" * 64,
        },
        "works": [work],
        "summary": {
            "work_count": 1,
            "main_video_count": 1,
            "duplicate_main_video_count": 0,
            "missing_nfo_count": 0,
            "missing_artwork_count": 0,
            "non_feature_residual_count": 0,
            "chinese_subtitle_gap_count": chinese_subtitle_gap_count,
            "external_sidecar_count": external_sidecar_count,
        },
    }
    completion["evidence_sha256"] = server.canonical_digest(completion)
    server._atomic_json(
        job.directory / "ordinary-title-completion.json", completion,
    )


def rewrite_ordinary_completion(job: object, mutate) -> None:
    path = job.directory / "ordinary-title-completion.json"
    evidence = server.load_json(path)
    evidence.pop("evidence_sha256", None)
    mutate(evidence)
    evidence["evidence_sha256"] = server.canonical_digest(evidence)
    server._atomic_json(path, evidence)


def write_remote_transaction_fixture(
    job: object, *, state: str, transaction_id: str = "residual-fixture",
    payload: bytes | None = None, partial: bytes | None = None,
    lock: bool = True,
) -> Path:
    transaction = (
        job.directory / ".remote-file-transactions" / transaction_id
    )
    transaction.mkdir(parents=True)
    journal = {
        "schema_version": 1,
        "transaction_id": transaction_id,
        "source_path": "/quark/影视/待刮削/Example/file.mkv",
        "target_path": "/quark/影视/番剧/Example/file.mkv",
        "content_type": "application/octet-stream",
        "size": len(payload or b"fixture"),
        "sha256": hashlib.sha256(payload or b"fixture").hexdigest(),
        "state": state,
        "upload_started": state != "staged",
        "upload_calls": 0 if state == "staged" else 1,
        "source_deleted": state == "complete",
        "history": [{"state": state, "event": state}],
    }
    server._atomic_json(transaction / "journal.json", journal)
    if payload is not None:
        (transaction / "payload.bin").write_bytes(payload)
    if partial is not None:
        (transaction / "payload.part").write_bytes(partial)
    if lock:
        (transaction / "transaction.lock").write_bytes(b"")
    return transaction


def write_hybrid_transaction_fixture(
    job: object, *, source_path: str | None = None,
    target_path: str | None = None, forward: bool = True,
) -> tuple["ResidualTransactionFakeClient", object, Path]:
    """Create the same sealed receipt and local journals Engine emits."""
    from engine.scrapeflow.alist_exact_file_adapter import AListExactFileAdapter
    from engine.scrapeflow.hybrid_remote_transaction import (
        HybridTransferSpec,
        prepare_hybrid_batch,
        run_hybrid_transfer,
    )

    payload = f"payload:{job.id}".encode()
    source = source_path or f"{job.source}/source-{job.id}.mkv"
    target = target_path or f"{job.parent}/Hybrid {job.id}/final-{job.id}.mkv"
    plan = {
        "mode": "tv",
        "source_root": job.source,
        "target_root": posixpath.dirname(target),
        "metadata": {"title": f"Hybrid {job.id}", "tmdb_id": 42},
        "files": [{
            "source_path": source,
            "source_dir": posixpath.dirname(source),
            "original_name": posixpath.basename(source),
            "final_name": posixpath.basename(target),
            "target_dir": posixpath.dirname(target),
            "media_kind": "video",
            "source_size": len(payload),
        }],
        "cleanup_files": [],
        "problem_files": [],
    }
    plan_sha256 = server.canonical_digest(plan)
    batch_id = server._expected_hybrid_batch_id(plan_sha256)
    item_id = "item-" + hashlib.sha256(
        f"{source}\0{target}".encode(),
    ).hexdigest()[:48]
    spec = HybridTransferSpec(
        batch_id=batch_id,
        item_id=item_id,
        source_path=source,
        target_path=target,
        expected_size=len(payload),
    )
    client = ResidualTransactionFakeClient({source: payload})
    adapter = AListExactFileAdapter(client)
    state_root = job.directory / ".hybrid-remote-transactions"
    prepared = prepare_hybrid_batch(
        adapter, state_root=state_root, specs=[spec],
    )[0]
    if forward:
        run_hybrid_transfer(adapter, state_root=state_root, spec=spec)
    receipt = {
        "schema_version": 1,
        "kind": "hybrid_batch_sealed_receipt",
        "state_root": str(state_root.resolve()),
        "batch_id": batch_id,
        "rollback_root": spec.rollback_root,
        "plan_sha256": plan_sha256,
        "commit_authority": "local_ordinary_acceptance_only",
        "items": [{
            "operation": spec.operation,
            "spec": spec.to_dict(),
            "verified_size": prepared.size,
            "verified_sha256": prepared.sha256,
            "rollback_path": prepared.rollback_path,
        }],
    }
    server._atomic_json(job.directory / "media-plan.json", {
        "schema_version": 4,
        "plan": plan,
        "plan_sha256": plan_sha256,
    })
    server._atomic_json(job.directory / "media-journal.json", {
        "success": forward,
        "plan": plan,
        "plan_sha256": plan_sha256,
        "records": [{
            "action": "hybrid-batch-sealed",
            "source": plan["source_root"],
            "target": spec.batch_root,
            "status": "retained",
            "message": json.dumps(
                receipt, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ),
        }],
    })
    return client, spec, state_root


class ResidualTransactionFakeClient:
    """In-memory exact-file backend with duplicate-producing upload semantics."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = dict(files)
        self.directories = {"/"}
        for path in self.files:
            self.mkdir(posixpath.dirname(path))
        self.upload_calls = 0
        self.remove_calls = 0
        self.upload_mode = "normal"
        self.target_stat_failures = 0

    def mkdir(self, path: str) -> None:
        current = ""
        for part in path.strip("/").split("/"):
            if not part:
                continue
            current = f"{current}/{part}" if current else f"/{part}"
            self.directories.add(current)

    def try_list(self, path: str, refresh: bool = False):
        del refresh
        if path not in self.directories:
            return None
        rows: list[dict[str, object]] = []
        for directory in self.directories:
            if directory != path and posixpath.dirname(directory) == path:
                rows.append({"name": posixpath.basename(directory), "is_dir": True})
        for file_path, payload in self.files.items():
            if posixpath.dirname(file_path) == path:
                rows.append({
                    "name": posixpath.basename(file_path),
                    "is_dir": False,
                    "size": len(payload),
                })
        return sorted(rows, key=lambda row: str(row["name"]))

    def exact_file_info(self, path: str):
        if (
            self.target_stat_failures
            and self.upload_calls > 0
            and "/ScrapeFlow/" in path
        ):
            self.target_stat_failures -= 1
            raise RuntimeError("target exact stat temporarily unavailable")
        payload = self.files.get(path)
        if payload is None:
            return None
        digest = hashlib.sha256(payload).hexdigest()
        return {"size": len(payload), "sha256": digest, "version": digest}

    def open_file_reader(self, path: str):
        if path not in self.files:
            raise FileNotFoundError(path)
        return io.BytesIO(self.files[path])

    def upload_file(
        self, target_path: str, source: Path,
        content_type: str = "application/octet-stream",
    ) -> None:
        del content_type
        self.upload_calls += 1
        payload = source.read_bytes()
        if self.upload_mode == "source_lost_500":
            source_path = next(
                path for path in list(self.files)
                if path.startswith("/quark/影视/待刮削/")
            )
            self.files.pop(source_path, None)
            raise RuntimeError("HTTP 500 and both remote paths disappeared")
        if target_path in self.files:
            self.files[f"{target_path} (1)"] = payload
        else:
            self.files[target_path] = payload
        if self.upload_mode == "response_lost":
            raise RuntimeError("HTTP 500 after target commit")

    def remove(self, parent: str, names: list[str]) -> None:
        self.remove_calls += 1
        for name in names:
            self.files.pop(posixpath.join(parent, name), None)

    def remove_empty_dir(self, path: str) -> bool:
        if path not in self.directories:
            return True
        if self.try_list(path):
            return False
        self.directories.remove(path)
        return True


class LocalServerTests(unittest.TestCase):
    def setUp(self):
        self._previous_state_root = server.STATE_ROOT
        self._previous_jobs_root = server.JOBS_ROOT
        self._previous_root_provider = server.Job.root_provider
        self._previous_jobs = server.JOBS
        self._case_state = tempfile.TemporaryDirectory(
            prefix="case-", dir=TEST_STATE.name,
        )
        server.STATE_ROOT = Path(self._case_state.name)
        server.JOBS_ROOT = server.STATE_ROOT / "jobs"
        server.JOBS_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
        server.JOBS = {}
        self._scrape_first_gate_patcher = mock.patch.object(
            server, "scrape_first_gate_evidence",
            return_value={
                "ready": True, "status": "ready", "blocker_count": 0,
                "blockers": [], "checked_at": "2026-08-04T00:00:00+00:00",
                "message": "ordinary scrapes accepted",
                "snapshot_sha256": "a" * 64,
            },
        )
        self._scrape_first_gate_patcher.start()

    def tearDown(self):
        if self._scrape_first_gate_patcher is not None:
            self._scrape_first_gate_patcher.stop()
        server.JOBS = self._previous_jobs
        server.STATE_ROOT = self._previous_state_root
        server.JOBS_ROOT = self._previous_jobs_root
        server.Job.root_provider = staticmethod(self._previous_root_provider)
        self._case_state.cleanup()

    def test_live_target_video_names_respect_deepest_tvshow_boundary(self):
        root = "/quark/影视/番剧/Fate/命运之夜"
        nested = f"{root}/命运之夜 无限剑制"
        listings = {
            root: [
                {"name": "tvshow.nfo", "is_dir": False},
                {"name": "Season 00", "is_dir": True},
                {"name": "命运之夜 无限剑制", "is_dir": True},
            ],
            f"{root}/Season 00": [
                {"name": "Fate - S00E03.mkv", "is_dir": False},
            ],
            nested: [
                {"name": "tvshow.nfo", "is_dir": False},
                {"name": "Season 00", "is_dir": True},
            ],
            f"{nested}/Season 00": [
                {"name": "Unlimited Blade Works - S00E01.mkv", "is_dir": False},
                {"name": "Unlimited Blade Works - S00E02.mkv", "is_dir": False},
            ],
        }
        client = mock.MagicMock()
        client.list.side_effect = lambda path, refresh: listings[path]

        with mock.patch.object(server.time, "sleep"):
            names = server._stable_live_target_video_names(client, root)

        self.assertEqual(names, ["Fate - S00E03.mkv"])
        self.assertNotIn(
            mock.call(f"{nested}/Season 00", refresh=True),
            client.list.call_args_list,
        )


    def test_completed_replenishment_consumes_stale_ready_source_receipt(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "c" * 12, "/source", "/target", "tv", False, True,
                phase="completed", plan_summary={"replenishment": {
                    "status": "acquired", "followup_verified": True,
                    "followup_job_ids": ["d" * 12],
                }},
            )
            job.directory.mkdir()
            path = job.directory / "replenishment-acquisition.json"
            path.write_text(json.dumps({
                "status": "ready", "source_paths": [
                    "/quark/影视/ScrapeFlow/补源/finished",
                ], "materialized_files": 1,
                "materializations": ["fast_save"],
            }), encoding="utf-8")
            self.assertTrue(server.finalize_replenishment_acquisition_artifacts(job))
            payload = json.loads(path.read_text())
            self.assertEqual(payload["status"], "consumed")
            self.assertEqual(payload["source_paths"], [])
            self.assertEqual(payload["consumed_source_paths"], [
                "/quark/影视/ScrapeFlow/补源/finished",
            ])
            self.assertTrue(payload["followup_verified"])
            self.assertEqual(payload["materializations"], ["fast_save"])
            self.assertFalse(server.finalize_replenishment_acquisition_artifacts(job))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_replenishment_materialized_sources_are_system_internal(self):
        self.assertTrue(server.is_replenishment_system_source(
            "/quark/影视/ScrapeFlow/补源/ScrapeFlow补源-42-Example-offline"
        ))
        self.assertTrue(server.is_replenishment_system_source(
            "/quark/影视/待刮削/ScrapeFlow补源-42-Example"
        ))
        self.assertTrue(server.is_replenishment_system_source(
            "/quark/影视/待刮削/_ScrapeFlow补源-42-Example"
        ))
        self.assertFalse(server.is_replenishment_system_source(
            "/quark/影视/待刮削/Example"
        ))


    def test_quark_helper_health_is_redacted_and_requires_native_runtime(self):
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        response.status = 200
        response.read.return_value = json.dumps({
            "status": "ok", "service": "quark-native-helper",
            "runtime": "connected", "build_id": "a" * 64,
            "journal": {"complete": 2, "in_doubt": 0},
        }).encode()
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_QUARK_HELPER_URL": "http://host.docker.internal:18765",
            "SCRAPEFLOW_QUARK_HELPER_TOKEN": "SECRET_HELPER_TOKEN",
        }), mock.patch.object(
            server, "urlopen", return_value=response,
        ) as opener:
            payload = server.quark_helper_health_payload()
        self.assertTrue(payload["native_ready"])
        self.assertEqual(payload["journal"]["in_doubt"], 0)
        self.assertNotIn("SECRET_HELPER_TOKEN", json.dumps(payload))
        request = opener.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "http://host.docker.internal:18765/health/passive",
        )

    def test_share_lane_suppression_is_persisted_and_loaded(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            server.time, "time", return_value=1000,
        ):
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "a" * 12, "/source", "/target", "tv", False, True,
            )
            job.directory.mkdir()
            server._record_replenishment_lane_suppressions(job, [{
                "provider": "quark_share", "locator": "quark_share:fixture",
                "until_epoch": 1600, "reason": "bridge_unavailable",
            }])
            self.assertEqual(server._load_replenishment_lane_suppressions(job), [{
                "provider": "quark_share", "locator": "quark_share:fixture",
                "until_epoch": 1600, "reason": "bridge_unavailable",
            }])
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_cloud_provider_attempt_ledger_is_scoped_and_durable(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "f" * 12, "/source", "/target", "tv", False, True,
            )
            job.directory.mkdir()
            request = {
                "media": {
                    "tmdb_id": 42, "title": "Example",
                    "target_root": "/quark/影视/番剧/Example",
                },
                "gaps": [{"id": "S00E07", "season": 0, "episodes": [7]}],
            }
            self.assertEqual(
                server._load_replenishment_provider_attempts(job, request),
                {"quark_share": 0, "quark_magnet": 0},
            )
            server._record_replenishment_provider_attempts(
                job, request, "quark_share",
                reason="candidate_resource_failure",
                locators=["quark_share:one"],
            )
            server._record_replenishment_provider_attempts(
                job, request, "quark_magnet", count=2,
                reason="candidate_resource_failure",
                locators=["magnet:a", "magnet:b"],
            )
            self.assertEqual(
                server._load_replenishment_provider_attempts(job, request),
                {"quark_share": 1, "quark_magnet": 2},
            )
            different_gap = {**request, "gaps": [{"id": "S00E08"}]}
            self.assertEqual(
                server._load_replenishment_provider_attempts(job, different_gap),
                {"quark_share": 0, "quark_magnet": 0},
            )
            stronger_semantics = {
                **request,
                "gaps": [{
                    "id": "S00E07", "season": 0, "episodes": [7],
                    "title": "Memory Snow", "season_name": "Specials",
                }],
                "search_queries": ["Example Memory Snow"],
            }
            self.assertEqual(
                server._load_replenishment_provider_attempts(
                    job, stronger_semantics,
                ),
                {"quark_share": 0, "quark_magnet": 0},
            )
            self.assertEqual(
                server._load_replenishment_provider_exhausted(
                    job, stronger_semantics,
                ),
                {},
            )
            payload = json.loads(
                server._replenishment_provider_attempt_path(job).read_text(encoding="utf-8")
            )
            entry = next(iter(payload["entries"].values()))
            self.assertEqual(entry["gap_ids"], ["S00E07"])
            self.assertEqual(len(entry["history"]), 2)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_cloud_attempt_floor_counts_distinct_locators_not_empty_poll_rounds(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "e" * 12, "/source", "/target", "tv", False, True,
            )
            job.directory.mkdir()
            request = {
                "media": {
                    "tmdb_id": 42, "title": "Example",
                    "target_root": "/quark/影视/番剧/Example",
                },
                "gaps": [{"id": "S00E07"}],
            }
            attempts = server._record_replenishment_provider_attempts(
                job, request, "quark_share",
                reason="completed_search_without_executable_candidate",
                locators=[],
            )
            self.assertEqual(attempts["quark_share"], 0)
            for index in range(30):
                attempts = server._record_replenishment_provider_attempts(
                    job, request, "quark_share",
                    reason="candidate_resource_failure",
                    locators=[f"quark_share:{index:02d}"],
                )
            self.assertEqual(attempts["quark_share"], 30)
            attempts = server._record_replenishment_provider_attempts(
                job, request, "quark_share",
                reason="candidate_resource_failure",
                locators=["quark_share:00"],
            )
            self.assertEqual(attempts["quark_share"], 30)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_provider_exhausted_record_is_idempotent(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "6" * 12, "/source", "/target", "tv", False, True,
            )
            job.directory.mkdir()
            request = {
                "media": {
                    "tmdb_id": 42, "title": "Example",
                    "target_root": "/quark/影视/番剧/Example",
                },
                "gaps": [{"id": "S00E07"}],
            }
            first = server._record_replenishment_provider_exhausted(
                job, request, "quark_magnet", reason="all_sources_exhausted",
                proof={
                    "kind": "search_complete_no_candidates",
                    "required_sources": ["Nyaa"],
                    "completed_sources": ["Nyaa"],
                    "candidate_count": 0,
                    "excluded_candidate_count": 30,
                },
            )
            path = server._replenishment_provider_attempt_path(job)
            first_payload = path.read_text(encoding="utf-8")
            second = server._record_replenishment_provider_exhausted(
                job, request, "quark_magnet", reason="all_sources_exhausted",
                proof={
                    "kind": "search_complete_no_candidates",
                    "required_sources": ["Nyaa"],
                    "completed_sources": ["Nyaa"],
                    "candidate_count": 0,
                    "excluded_candidate_count": 30,
                },
            )
            second_payload = path.read_text(encoding="utf-8")

            self.assertEqual(first, second)
            self.assertTrue(second["quark_magnet"]["exhausted"])
            self.assertEqual(
                second["quark_magnet"]["proof"]["kind"],
                "search_complete_no_candidates",
            )
            self.assertEqual(first_payload, second_payload)
            stronger_semantics = {
                **request,
                "gaps": [{
                    "id": "S00E07", "title": "Memory Snow",
                    "season_name": "Specials",
                }],
                "search_queries": ["Example Memory Snow"],
            }
            self.assertEqual(
                server._load_replenishment_provider_exhausted(
                    job, stronger_semantics,
                ),
                {},
            )
            entry = next(iter(json.loads(second_payload)["entries"].values()))
            exhaustion_rows = [
                row for row in entry["history"]
                if row["provider"] == "quark_magnet"
                and row["reason"] == "all_sources_exhausted"
            ]
            self.assertEqual(len(exhaustion_rows), 1)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_resource_failure_count_does_not_downgrade_complete_search_proof(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "5" * 12, "/source", "/target", "tv", False, True,
            )
            job.directory.mkdir()
            request = {
                "media": {
                    "tmdb_id": 42, "title": "Example",
                    "target_root": "/quark/影视/番剧/Example",
                },
                "gaps": [{"id": "S00E07"}],
                "rules": {"minimum_attempts_per_cloud_lane": 1},
            }
            expected_proof = {
                "kind": "search_complete_no_candidates",
                "required_sources": ["AnimeTosho", "TokyoTosho"],
                "completed_sources": ["AnimeTosho", "TokyoTosho"],
                "candidate_count": 0,
                "excluded_candidate_count": 99,
            }
            server._record_replenishment_provider_exhausted(
                job, request, "quark_magnet",
                reason="configured_cloud_offline_sources_exhausted",
                proof=expected_proof,
            )

            attempts = server._record_replenishment_provider_attempts(
                job, request, "quark_magnet",
                reason="distinct_cloud_offline_resource_mismatch",
                locators=["quark_magnet:late-resource"],
            )

            self.assertEqual(attempts["quark_magnet"], 1)
            exhausted = server._load_replenishment_provider_exhausted(job, request)
            self.assertEqual(
                exhausted["quark_magnet"]["proof"], expected_proof,
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_all_required_sources_exhausted_requires_complete_lane_proofs(self):
        proof = {
            "kind": "search_complete_no_candidates",
            "required_sources": ["Required"],
            "completed_sources": ["Required"],
            "candidate_count": 0,
            "excluded_candidate_count": 4,
        }
        lanes = {
            "quark_share": {"status": "exhausted", "proof": dict(proof)},
            "quark_magnet": {"status": "exhausted", "proof": dict(proof)},
        }

        exhausted = server._all_required_sources_exhausted(
            lanes, remaining_candidate_count=0, local_torrent_unlocked=True,
        )

        self.assertEqual(exhausted["kind"], "all_required_sources_exhausted")
        self.assertIsNone(server._all_required_sources_exhausted(
            lanes, remaining_candidate_count=1, local_torrent_unlocked=True,
        ))
        self.assertIsNone(server._all_required_sources_exhausted(
            lanes, remaining_candidate_count=0, local_torrent_unlocked=False,
        ))
        lanes["quark_magnet"] = {"status": "infrastructure_failure"}
        self.assertIsNone(server._all_required_sources_exhausted(
            lanes, remaining_candidate_count=0, local_torrent_unlocked=True,
        ))

    def test_replenishment_aggregate_ignores_closed_members_and_fails_closed(self):
        cases = [
            (["no_regular_gaps", "sources_exhausted"], 0, "sources_exhausted"),
            (["no_regular_gaps", "no_match"], 0, "no_match"),
            (["no_regular_gaps", "acquired"], 0, "acquired"),
            (["no_regular_gaps"], 0, "no_regular_gaps"),
            (["sources_exhausted"], 1, "unresolved_gaps"),
            (["acquired"], 1, "partial"),
        ]
        for statuses, unresolved, expected in cases:
            with self.subTest(statuses=statuses, unresolved=unresolved):
                self.assertEqual(
                    server._aggregate_replenishment_project_status(
                        [{"status": status} for status in statuses],
                        unresolved_gap_count=unresolved,
                    ),
                    expected,
                )

    def test_legacy_boolean_exhaustion_is_ignored_fail_closed(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job("7" * 12, "/source", "/target", "tv", False, True)
            job.directory.mkdir()
            request = {
                "media": {"tmdb_id": 42, "title": "Example", "target_root": "/target"},
                "gaps": [{"id": "S00E07"}],
            }
            key = server._replenishment_attempt_key(request)
            server._atomic_json(server._replenishment_provider_attempt_path(job), {
                "version": 1,
                "entries": {key: {
                    "attempts": {"quark_share": 30, "quark_magnet": 30},
                    "exhausted": {"quark_share": True, "quark_magnet": True},
                }},
            })
            self.assertEqual(
                server._load_replenishment_provider_exhausted(job, request),
                {},
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_replenishment_attempt_key_changes_with_episode_title_aliases(self):
        request = {
            "media": {
                "tmdb_id": 65942, "title": "Re:Zero",
                "target_root": "/target",
            },
            "gaps": [{
                "id": "S00E51", "title": "沉睡鬼的枕边夜话",
                "season_name": "特别篇",
            }],
            "search_queries": ["Re:Zero S00E51"],
        }
        old_key = server._replenishment_attempt_key(request)
        request["gaps"][0]["title_aliases"] = ["眠れる鬼の夜話"]
        self.assertNotEqual(
            old_key, server._replenishment_attempt_key(request),
        )

    def test_distinct_resource_failure_floor_creates_structured_exhaustion_proof(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job("8" * 12, "/source", "/target", "tv", False, True)
            job.directory.mkdir()
            request = {
                "media": {"tmdb_id": 42, "title": "Example", "target_root": "/target"},
                "gaps": [{"id": "S00E07"}],
                "rules": {"minimum_attempts_per_cloud_lane": 3},
            }
            for index in range(3):
                server._record_replenishment_provider_attempts(
                    job,
                    request,
                    "quark_magnet",
                    reason="candidate_resource_failure",
                    locators=[f"quark_magnet:{index}"],
                )
            exhausted = server._load_replenishment_provider_exhausted(job, request)
            self.assertEqual(exhausted["quark_magnet"], {
                "exhausted": True,
                "proof": {
                    "kind": "resource_failure_floor_reached",
                    "required_floor": 3,
                    "distinct_failure_count": 3,
                },
            })
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_legacy_empty_poll_attempts_are_migrated_to_unique_locator_count(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job("f" * 12, "/source", "/target", "tv", False, True)
            job.directory.mkdir()
            request = {
                "media": {"tmdb_id": 42, "title": "Example", "target_root": "/target"},
                "gaps": [{"id": "S00E01"}],
            }
            key = server._replenishment_attempt_key(request)
            server._atomic_json(server._replenishment_provider_attempt_path(job), {
                "version": 1,
                "entries": {key: {
                    "attempts": {"quark_share": 27, "quark_magnet": 0},
                    "history": [
                        {
                            "provider": "quark_share", "count": 1,
                            "reason": "completed_search_without_executable_candidate",
                            "locators": [],
                        }
                        for _ in range(27)
                    ],
                }},
            })
            self.assertEqual(
                server._load_replenishment_provider_attempts(job, request),
                {"quark_share": 0, "quark_magnet": 0},
            )
            migrated = json.loads(
                server._replenishment_provider_attempt_path(job).read_text(encoding="utf-8")
            )
            self.assertEqual(migrated["version"], 2)
            self.assertEqual(
                migrated["entries"][key]["migrated_empty_poll_attempts"]["quark_share"],
                27,
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_delivery_failure_does_not_isolate_verified_torrent(self):
        selections = [{
            "locator": "torrent:fixture", "infohash": "abc",
            "release_name": "Verified download",
        }]
        output = "\n".join([
            "[replenishment] 下载候选 1/1: Verified download",
            "[replenishment] failure_scope=delivery reusable_candidate=true",
            "补源适配器失败: EntityTooSmall",
        ])
        self.assertEqual(
            server._failed_replenishment_selections(selections, output), [],
        )

    def test_structured_delivery_scope_does_not_depend_on_log_wording(self):
        selections = [{
            "locator": "torrent:fixture", "infohash": "abc",
            "release_name": "Verified download",
        }]
        self.assertEqual(
            server._failed_replenishment_selections(
                selections,
                "opaque adapter failure",
                failure_scope="delivery",
            ),
            [],
        )

    def test_legacy_infrastructure_marker_does_not_isolate_active_candidate(self):
        selections = [{
            "locator": "torrent:fixture", "infohash": "abc",
            "release_name": "Active download",
        }]
        output = "\n".join([
            "[replenishment] 下载候选 1/1: Active download",
            "[replenishment] failure_scope=infrastructure reusable_candidate=false",
            "补源适配器失败: 暂存空间不足",
        ])
        self.assertEqual(
            server._failed_replenishment_selections(selections, output), [],
        )

    def test_structured_candidate_identity_isolates_only_active_release(self):
        selections = [
            {"locator": "torrent:one", "infohash": "a" * 40, "release_name": "One"},
            {"locator": "torrent:two", "infohash": "b" * 40, "release_name": "Two"},
        ]
        failed = server._failed_replenishment_selections(
            selections,
            "opaque adapter failure",
            failure_scope="candidate",
            failed_candidate={"infohash": "b" * 40},
        )
        self.assertEqual([row["release_name"] for row in failed], ["Two"])

    def test_structured_local_fallback_identity_does_not_match_cloud_selection_by_hash(self):
        infohash = "b" * 40
        cloud = {
            "provider": "quark_magnet", "locator": f"quark_magnet:{infohash}",
            "infohash": infohash, "release_name": "Cloud candidate",
        }
        local = {
            "provider": "magnet", "locator": "torrent:https://fixture/release.torrent",
            "infohash": infohash, "release_name": "Local fallback",
        }
        failed = server._failed_replenishment_selections(
            [cloud], "opaque adapter failure", failure_scope="candidate",
            failed_candidate=local,
        )
        self.assertEqual(failed, [local])

    def test_failure_ledger_deduplicates_infohash_only_within_provider(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "f" * 12, "/source", "/target", "tv", False, True,
            )
            job.directory.mkdir()
            infohash = "c" * 40
            server._atomic_json(server._replenishment_failure_path(job), {
                "version": 1, "job_id": job.id, "failures": [{
                    "provider": "quark_magnet",
                    "locator": f"quark_magnet:{infohash}",
                    "infohash": infohash, "release_name": "Cloud candidate",
                    "reason": "cloud failure", "failure_scope": "candidate",
                    "failures": 7,
                }],
            })
            local = {
                "provider": "magnet", "locator": "torrent:https://fixture/release.torrent",
                "infohash": infohash, "release_name": "Local fallback",
            }
            for _ in range(2):
                server._record_replenishment_failures(
                    job, [], "opaque adapter failure", "aria2 0B/s",
                    failure_scope="candidate", failed_candidate=local,
                )

            failures = server._load_replenishment_failures(job)
            by_provider = {row["provider"]: row for row in failures}
            self.assertEqual(by_provider["quark_magnet"]["failures"], 7)
            self.assertEqual(by_provider["quark_magnet"]["reason"], "cloud failure")
            self.assertEqual(by_provider["magnet"]["failures"], 2)
            self.assertEqual(by_provider["magnet"]["locator"], local["locator"])
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_legacy_entity_too_small_is_classified_as_reusable_delivery(self):
        self.assertTrue(server._legacy_reusable_delivery_failure({
            "reason": "AList 流式上传失败: EntityTooSmall PartNumber=47 ProposedSize=0",
        }))
        self.assertFalse(server._legacy_reusable_delivery_failure({
            "reason": "aria2c 下载失败: 0B/s",
        }))

    def test_legacy_local_download_never_excludes_quark_magnet(self):
        self.assertTrue(server._legacy_cross_lane_local_failure({
            "provider": "quark_magnet",
            "reason": "aria2c 下载失败: /workspace/torrent/download-01 0B/s",
        }))
        self.assertFalse(server._legacy_cross_lane_local_failure({
            "provider": "magnet",
            "reason": "aria2c 下载失败: 0B/s",
        }))
        self.assertFalse(server._legacy_cross_lane_local_failure({
            "provider": "quark_magnet",
            "reason": "Quark offline parse differs from exact candidate manifest",
        }))



    def test_audit_date_uses_configured_timezone_instead_of_container_utc(self):
        instant = server.datetime(2026, 7, 26, 19, 0, tzinfo=server.timezone.utc)
        with mock.patch.dict(os.environ, {"TZ": "Asia/Shanghai"}, clear=False):
            self.assertEqual(server.audit_local_date(instant).isoformat(), "2026-07-27")


    def test_run_command_consumes_structured_engine_progress(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job("f" * 12, "/src", "/dst", "auto", False, True)
            job.directory.mkdir()
            command = [
                sys.executable,
                "-c",
                'print(\'SCRAPEFLOW_PROGRESS {"stage":"scan","completed":2,"total":4,"percent":50,"message":"scanning"}\'); print("normal")',
            ]
            code, output = server.run_command(job, command)
            self.assertEqual(code, 0)
            self.assertEqual(output, "normal\n")
            self.assertEqual(job.progress["percent"], 50)
            self.assertNotIn("SCRAPEFLOW_PROGRESS", job.log_path.read_text(encoding="utf-8"))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_archive_progress_is_derived_from_persisted_tasks(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "e" * 12, "/src", "/dst", "auto", False, True,
                phase="extracting_archives",
            )
            job.directory.mkdir()
            (job.directory / "archive-plan.json").write_text(json.dumps({
                "plan": {"archives": [{}, {}, {}, {}]},
            }), encoding="utf-8")
            (job.directory / "archive-journal.json").write_text(json.dumps({
                "retained_archives": ["one"],
                "blocked_archives": [{"archive_path": "/src/two.7z"}],
                "tasks": [{"state": 1, "progress": 50}],
            }), encoding="utf-8")
            progress = job.public()["progress"]
            self.assertEqual(progress["stage"], "archive_extract")
            self.assertEqual((progress["completed"], progress["total"]), (2, 4))
            self.assertEqual(progress["percent"], 49.0)
            self.assertIn("2/4", progress["message"])
            self.assertIn("50%", progress["message"])
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_job_phase_contract_matches_api_state_machine(self):
        phases = set(json.loads(CONTRACT_PATH.read_text(encoding="utf-8")))
        self.assertEqual(phases, set(server.VALID_PHASES))

    def test_state_machine_rejects_unsafe_phase_jump(self):
        job = server.Job("abc123abc123", "/src", "/dst", "auto", False, True)
        with self.assertRaisesRegex(ValueError, "不允许"):
            server.update_job(job, phase="completed")

    def test_browse_remote_can_force_alist_refresh(self):
        calls = []

        class FakeAListClient:
            def __init__(self, *_args, **_kwargs):
                pass

            def login(self):
                return "token"

            def list(self, path, refresh=False):
                calls.append((path, refresh))
                return [{"name": "新目录", "is_dir": True}]

        fake_scraper = types.SimpleNamespace(AListClient=FakeAListClient)
        with mock.patch.dict(os.environ, {"ALIST_PASSWORD": "test-password"}), mock.patch.dict(
            sys.modules, {"scraper": fake_scraper}
        ):
            result = server.browse_remote("/quark/影视/番剧", refresh=True)

        self.assertEqual(calls, [("/quark/影视/番剧", True)])
        self.assertEqual(result["directories"], [{"name": "新目录", "path": "/quark/影视/番剧/新目录"}])

    def test_browse_remote_marks_named_and_explicit_pending_delete_directories(self):
        class FakeAListClient:
            def __init__(self, *_args, **_kwargs):
                pass

            def login(self):
                return "token"

            def list(self, _path, refresh=False):
                del refresh
                return [
                    {"name": "作品（待删）", "is_dir": True},
                    {"name": "作品（待删2）", "is_dir": True},
                    {"name": "显式标记", "is_dir": True, "pending_delete": True},
                    {"name": "状态标记", "is_dir": True, "status": "pending_delete"},
                    {"name": "待刮削作品", "is_dir": True},
                ]

        fake_scraper = types.SimpleNamespace(AListClient=FakeAListClient)
        with mock.patch.dict(os.environ, {"ALIST_PASSWORD": "test-password"}), mock.patch.dict(
            sys.modules, {"scraper": fake_scraper}
        ):
            result = server.browse_remote("/quark/影视/待刮削")

        states = {row["name"]: row.get("directory_state") for row in result["directories"]}
        self.assertEqual(states["作品（待删）"], "pending_delete")
        self.assertEqual(states["作品（待删2）"], "pending_delete")
        self.assertEqual(states["显式标记"], "pending_delete")
        self.assertEqual(states["状态标记"], "pending_delete")
        self.assertIsNone(states["待刮削作品"])
        pending = next(row for row in result["directories"] if row["name"] == "作品（待删）")
        self.assertFalse(pending["selectable"])
        self.assertIn("待删除", pending["disabled_reason"])

    def test_normalize_remote_path(self):
        self.assertEqual(
            server.normalize_remote_input("http://127.0.0.1:5244/quark/%E5%BD%B1%E8%A7%86/%E7%95%AA%E5%89%A7"),
            "/quark/影视/番剧",
        )
        self.assertEqual(server.normalize_remote_input("/quark/影视/番剧/"), "/quark/影视/番剧")
        with self.assertRaises(ValueError):
            server.normalize_remote_input("quark/影视")
        with self.assertRaises(ValueError):
            server.normalize_remote_input("/quark/../影视")

    def test_default_parent(self):
        self.assertEqual(server.default_parent("/quark/影视/番剧/作品"), "/quark/影视/番剧")

    def test_media_library_root_is_a_hard_boundary(self):
        self.assertEqual(
            server.media_library_path("/quark/影视/番剧", allow_root=False),
            "/quark/影视/番剧",
        )
        self.assertEqual(
            server.media_library_path("/quark/影视", allow_root=True),
            "/quark/影视",
        )
        with self.assertRaisesRegex(ValueError, "具体媒体目录"):
            server.media_library_path("/quark/影视", allow_root=False)
        with self.assertRaisesRegex(ValueError, "必须位于"):
            server.media_library_path("/quark", allow_root=True)
        with self.assertRaisesRegex(ValueError, "必须位于"):
            server.media_library_path("/quark/影视剧", allow_root=True)

    def test_internal_backup_tree_cannot_be_used_as_new_source(self):
        with self.assertRaisesRegex(ValueError, "备份/恢复目录"):
            server.unscraped_media_path("/quark/影视/待刮削/_ScrapeFlow字幕备份/作品")
        self.assertEqual(
            server.unscraped_media_path("/quark/影视/待刮削/正常作品"),
            "/quark/影视/待刮削/正常作品",
        )
        with self.assertRaisesRegex(ValueError, "系统内部"):
            server.unscraped_media_path(
                "/quark/影视/待刮削/_ScrapeFlow补源-42-Example"
            )
        self.assertEqual(
            server.replenishment_source_path(
                "/quark/影视/待刮削/_ScrapeFlow补源-42-Example"
            ),
            "/quark/影视/待刮削/_ScrapeFlow补源-42-Example",
        )

    def test_digest_is_stable(self):
        self.assertEqual(server.canonical_digest({"b": 2, "a": 1}), server.canonical_digest({"a": 1, "b": 2}))

    def test_explicit_movie_does_not_receive_tv_subtitle_flag(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job(
                "8" * 12, "/quark/影视/待刮削/Movie", "/quark/影视/电影",
                "movie", False, True, tmdb_id=20,
            )
            job.directory.mkdir()
            with mock.patch.object(server, "run_command", return_value=(2, "failed")) as runner:
                server.plan_media(job)
            command = runner.call_args.args[1]
            self.assertNotIn("--prefer-simplified", command)
        server.JOBS_ROOT = previous_root

    def test_real_case_recovery_uses_fresh_journal_without_losing_previous_attempt(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job(
                "9" * 12, "/quark/影视/番剧/斩赤红之瞳", "/quark/影视/番剧",
                "auto", False, True, phase="starting_recovery_execution",
            )
            job.directory.mkdir()
            old_journal = job.directory / "recovery-journal.json"
            old_journal.write_text('{"status":"failed"}\n', encoding="utf-8")
            with mock.patch.object(
                server, "auto_execute_media_enabled", return_value=False
            ), mock.patch.object(server, "run_command", return_value=(0, "ok\n")) as runner:
                server.execute_recovery(job, "a" * 64)
            command = runner.call_args.args[1]
            journal_index = command.index("--journal") + 1
            self.assertTrue(command[journal_index].endswith("recovery-journal-2.json"))
            self.assertEqual(old_journal.read_text(encoding="utf-8"), '{"status":"failed"}\n')
            self.assertEqual(job.phase, "recovered")
        server.JOBS_ROOT = previous_root

    def test_recovery_review_uses_machine_records_not_ambiguous_terminal_arrows(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "7" * 12, "/quark/影视/待刮削/作品", "/quark/影视/番剧",
                "auto", False, True, phase="recovery_required",
            )
            job.directory.mkdir()
            (job.directory / "media-journal.json").write_text("{}\n", encoding="utf-8")
            source = "/quark/影视/番剧/作品/name → misleading.mkv"
            target = "/quark/影视/待刮削/作品/original.mkv"
            output = (
                f"{server.RECOVERY_DIGEST_PREFIX}{'a' * 64}\n"
                f"{server.RECOVERY_ITEM_PREFIX}"
                + json.dumps({"source": source, "target": target}, ensure_ascii=False)
                + "\n  /fake → /quark/影视/待刮削/wrong.mkv\n"
            )
            with mock.patch.object(
                server, "auto_execute_media_enabled", return_value=False
            ), mock.patch.object(server, "run_command", return_value=(0, output)):
                server.prepare_recovery(job)
            self.assertEqual(job.phase, "awaiting_recovery_approval")
            self.assertEqual(job.digest, "a" * 64)
            self.assertEqual(job.plan_summary["files"], [{
                "source": source, "target": target, "name": "original.mkv",
            }])
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_unattended_recovery_rolls_back_then_replans_same_task(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "a" * 12, "/quark/影视/待刮削/作品", "/quark/影视/番剧",
                "auto", False, True, phase="recovery_required",
            )
            job.directory.mkdir()
            (job.directory / "media-journal.json").write_text("{}\n", encoding="utf-8")
            (job.directory / "media-plan.json").write_text("{}\n", encoding="utf-8")
            output = (
                f"{server.RECOVERY_DIGEST_PREFIX}{'b' * 64}\n"
                f"{server.RECOVERY_ITEM_PREFIX}"
                + json.dumps({
                    "source": "/quark/影视/番剧/作品/E01.mkv",
                    "target": "/quark/影视/待刮削/作品/E01.mkv",
                }, ensure_ascii=False)
                + "\n"
            )
            with mock.patch.object(
                server, "auto_execute_media_enabled", return_value=True
            ), mock.patch.object(
                server, "run_command", return_value=(0, output)
            ), mock.patch.object(server, "start_execution") as execution:
                server.prepare_recovery(job)
            self.assertEqual(job.phase, "starting_recovery_execution")
            execution.assert_called_once_with(
                server.execute_approved_recovery, job, "b" * 64,
            )
            with mock.patch.object(
                server, "auto_execute_media_enabled", return_value=True
            ), mock.patch.object(
                server, "run_command", return_value=(0, "ok\n")
            ), mock.patch.object(server, "start_thread") as start_mock:
                server.execute_recovery(job, "b" * 64)
            self.assertEqual(job.phase, "queued")
            self.assertFalse((job.directory / "media-journal.json").exists())
            self.assertTrue((job.directory / "media-journal-recovered.json").exists())
            self.assertFalse((job.directory / "media-plan.json").exists())
            self.assertTrue((job.directory / "media-plan-recovered.json").exists())
            start_mock.assert_called_once_with(server.prepare_job, job)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_media_summary_only_exposes_problem_files(self):
        plan = {
            "mode": "tv",
            "source_root": "/source",
            "target_root": "/target",
            "metadata": {"title": "Example", "tmdb_id": 1},
            "files": [
                {"source_path": f"/source/{index}.mkv", "target_dir": "/target", "final_name": f"S01E{index:03}.mkv"}
                for index in range(300)
            ],
            "cleanup_files": [
                {
                    "source_path": "/source/Show [NCOP1].mkv",
                    "original_name": "Show [NCOP1].mkv",
                    "reason": "无字幕片头/片尾视频",
                }
            ],
            "problem_files": [
                {
                    "source_path": "/source/problem.mkv",
                    "target_path": None,
                    "reason": "无法唯一识别，保留原位",
                }
            ] * 201,
            "scan_report": {
                "resource_gaps": [{
                    "kind": "missing_episode",
                    "label": "Season 01 缺少 E03",
                    "reason": "TMDB 显示该集已播出，但源目录和目标库都没有对应视频。",
                    "files": ["/source/01.mkv", "/source/02.mkv"],
                }],
            },
        }
        summary = server.summarize_media_plan(plan)
        self.assertEqual(summary["file_count"], 300)
        self.assertNotIn("files", summary)
        self.assertEqual(len(summary["problem_files"]), server.MAX_SUMMARY_ISSUES_PER_KIND)
        self.assertEqual(summary["problem_file_count"], 201)
        self.assertEqual(summary["cleanup_file_count"], 1)
        self.assertEqual(summary["cleanup_groups"], [{
            "reason": "无字幕片头/片尾视频",
            "count": 1,
            "examples": ["/source/Show [NCOP1].mkv"],
            "truncated": False,
        }])
        self.assertFalse(summary["review"]["automation_eligible"])
        self.assertEqual(summary["review"]["risk_level"], "high")
        self.assertEqual(summary["problem_files"][0]["source"], "/source/problem.mkv")
        self.assertEqual(summary["cleanup_files"][0]["source"], "/source/Show [NCOP1].mkv")
        self.assertEqual(summary["resource_gap_count"], 1)
        self.assertEqual(summary["resource_gaps"], [{
            "kind": "missing_episode",
            "label": "Season 01 缺少 E03",
            "reason": "TMDB 显示该集已播出，但源目录和目标库都没有对应视频。",
            "files": ["/source/01.mkv", "/source/02.mkv"],
        }])
        self.assertTrue(summary["truncated"])

    def test_media_summary_bounds_warnings_and_cleanup_groups(self):
        plan = {
            "warnings": [f"warning-{index}" for index in range(25)],
            "cleanup_files": [
                {
                    "source_path": f"/source/item-{index}.bin",
                    "reason": f"reason-{index}",
                }
                for index in range(25)
            ],
        }
        summary = server.summarize_media_plan(plan)
        self.assertEqual(summary["warning_count"], 25)
        self.assertEqual(len(summary["warnings"]), server.MAX_SUMMARY_WARNINGS)
        self.assertEqual(summary["cleanup_group_count"], 25)
        self.assertEqual(
            len(summary["cleanup_groups"]), server.MAX_SUMMARY_CLEANUP_GROUPS,
        )
        self.assertTrue(summary["truncated"])

    def test_only_real_media_plan_problems_require_review(self):
        clean = {"files": [{"source_path": "/source/one.mkv"}], "warnings": []}
        cleanup_only = {
            "files": [],
            "cleanup_files": [{"source_path": "/source/._one.mkv"}],
            "warnings": [
                "确认执行后将删除明确无用的系统隐藏/片头片尾/广告文件：._one.mkv"
            ],
        }
        ambiguous = {
            "files": [{"source_path": "/source/one.mkv"}],
            "warnings": ["E13 超出官方集数，请核对"],
            "problem_files": [
                {"source_path": "/source/one.mkv", "reason": "无法唯一映射"}
            ],
        }
        self.assertFalse(server.media_plan_requires_review(clean))
        self.assertFalse(server.media_plan_requires_review(cleanup_only))
        trusted_cleanup = {
            "files": [],
            "cleanup_files": [{
                "source_path": "/source/Show [NCOP1].mkv",
                "reason": "无字幕片头/片尾视频",
            }],
            "warnings": ["确认执行后将删除明确无用的片头片尾文件"],
        }
        # The real engine reason includes the optical-disc menu qualifier.
        trusted_cleanup["cleanup_files"][0]["reason"] = "无字幕片头/片尾/光盘菜单视频"
        self.assertFalse(server.media_plan_requires_review(trusted_cleanup))
        high_risk_cleanup = {
            "files": [],
            "cleanup_files": [{
                "source_path": "/source/unknown.mkv",
                "reason": "根据模糊标题推测为重复内容",
            }],
            "warnings": ["确认执行后将删除明确无用的其他文件"],
        }
        self.assertTrue(server.media_plan_requires_review(high_risk_cleanup))
        resource_gap_only = {
            "files": [{"source_path": "/source/one.mkv"}],
            "warnings": [],
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "E02", "reason": "已播出但未找到", "files": [],
            }]},
        }
        self.assertFalse(server.media_plan_requires_review(resource_gap_only))
        self.assertTrue(server.media_plan_requires_review(ambiguous))
        structured_safe = {
            "notices": [{
                "code": "safe_action", "severity": "info",
                "requires_review": False, "message": "safe",
            }],
            "warnings": ["任意展示文本不再参与门禁判定"],
        }
        structured_review = {
            "notices": [{
                "code": "tmdb_match_requires_review", "severity": "warning",
                "requires_review": True, "message": "review",
            }],
        }
        self.assertFalse(server.media_plan_requires_review(structured_safe))
        self.assertTrue(server.media_plan_requires_review(structured_review))

    def test_user_auto_cleanup_rules_cover_junk_and_confirmed_tmdb_duplicates(self):
        reasons = [
            "无字幕片头/片尾/光盘菜单视频",
            "发布组广告图片",
            "字体资源包",
            (
                "同一 TMDB 集号已有同清晰度但文件更完整的版本 "
                "/source/Show.S01E01.2160p.mkv，删除较小的重复视频"
            ),
            (
                "同一 TMDB 电影 movie/42 已有更高清晰度版本 "
                "/source/Movie.2160p.mkv，删除较低清晰度重复视频"
            ),
        ]
        for index, reason in enumerate(reasons):
            with self.subTest(reason=reason):
                plan = {
                    "cleanup_files": [{
                        "source_path": f"/source/cleanup-{index}.bin",
                        "reason": reason,
                    }],
                    "warnings": [],
                    "notices": [{
                        "code": "destructive_cleanup_requires_review",
                        "severity": "warning",
                        "requires_review": True,
                        "message": "legacy engine cleanup gate",
                    }],
                }
                self.assertFalse(server.media_plan_requires_review(plan))

    def test_confirmed_official_boundary_warning_is_auto_safe_for_old_and_new_plans(self):
        message = (
            "源根目录中的裸集号完整覆盖已确认的 TMDB Season 02 边界；"
            "已按完整边界归入"
        )
        legacy = {
            "files": [],
            "warnings": [message],
            "notices": [{
                "code": "planning_warning_requires_review",
                "severity": "warning",
                "requires_review": True,
                "message": message,
                "evidence": {
                    "classification": "engine_generated",
                    "evidence_kind": "planning_rule",
                },
            }],
        }
        structured = {
            "files": [],
            "warnings": [message],
            "notices": [{
                "code": "complete_official_season_boundary",
                "severity": "info",
                "requires_review": False,
                "message": message,
                "evidence": {
                    "classification": "engine_generated",
                    "evidence_kind": "official_episode_boundary",
                    "season": 2,
                    "official_episode_count": 3,
                    "source_episode_numbers": [1, 2, 3],
                },
            }],
        }
        self.assertFalse(server.media_plan_requires_review(legacy))
        self.assertFalse(server.media_plan_requires_review(structured))

    def test_strong_evidence_warning_classes_do_not_require_media_approval(self):
        warnings = {
            "numbered_movies": (
                "编号 01–02 的完整视频序列与 TMDB 2 部电影的官方标题/"
                "别名逐一一致；已仅按官方上映日期顺序建立电影归属"
            ),
            "batch_subtitle_attached": (
                "2 个独立字幕目录文件已通过唯一同发行 basename "
                "跟随已确认视频"
            ),
            "batch_subtitle_isolated": (
                "2 个独立字幕目录文件缺少唯一视频证据，"
                "无法安全处理"
            ),
            "tv_extras": (
                "2 个明确位于特典目录的幕后/访谈/花絮视频"
                "已按 Infuse Extras 命名保留，不作为正片集号"
            ),
            "ass_title_companion": (
                "E20.5 的 ASS 文本伴侣 title 样式唯一标记为已确认的 "
                "TMDB movie/123；已按同一电影版本参与清晰度去重"
            ),
            "e00_timeline_runtime": (
                "已检索TMDB 官方开播时间与完整时长并唯一确认源文件 E00 对应 "
                "SP01（序章/第 0 话）"
            ),
        }

        for label, warning in warnings.items():
            with self.subTest(label=label):
                plan = {
                    "warnings": [warning],
                    "notices": [{
                        "code": "planning_warning_requires_review",
                        "severity": "warning",
                        "requires_review": True,
                        "message": warning,
                        "evidence": {"classification": "engine_generated"},
                    }],
                }
                self.assertEqual(
                    server.media_plan_requires_review(plan),
                    label == "batch_subtitle_isolated",
                )

    def test_complete_reset_absolute_blocks_do_not_require_manual_approval(self):
        message = (
            "源发行将长篇剧集分为重置编号的跨季 absolute 块；"
            "已仅在所有视频块完整覆盖 01–N，且与 TMDB 全部季集数"
            "边界唯一分割时自动映射：源第 1 组 01–201 → S01–S08"
        )
        plan = {
            "files": [], "cleanup_files": [], "warnings": [message],
            "notices": [{
                "code": "planning_warning_requires_review",
                "severity": "warning", "requires_review": True,
                "message": message,
                "evidence": {"classification": "engine_generated"},
            }],
        }
        self.assertFalse(server.media_plan_requires_review(plan))

    def test_proven_child_tv_boundary_does_not_require_manual_approval(self):
        message = (
            "子目录《银魂 剧场版 The Final》已通过 TMDB 标题/别名和完整 2 集边界"
            "唯一确认是独立剧集《银魂 THE SEMI-FINAL》（2021），"
            "未归入母作品 Season 00"
        )
        plan = {
            "files": [], "cleanup_files": [], "warnings": [message],
            "notices": [{
                "code": "special_mapping_evidence", "severity": "warning",
                "requires_review": True, "message": message,
                "evidence": {"classification": "engine_generated"},
            }],
        }
        self.assertFalse(server.media_plan_requires_review(plan))

    def test_retained_unmapped_problem_always_fails_closed(self):
        message = "E12.5 无法唯一识别；文件保留原位"
        source = "/source/Show.E12.5.mkv"

        def plan_with(**changes):
            plan = {
                "files": [{"source_path": "/source/Show.E01.mkv"}],
                "cleanup_files": [],
                "problem_files": [{
                    "source_path": source,
                    "target_path": None,
                    "reason": message,
                }],
                "notices": [{
                    "code": "special_mapping_evidence",
                    "severity": "warning",
                    "requires_review": True,
                    "message": message,
                    "evidence": {
                        "classification": "engine_generated",
                        "evidence_kind": "official_tmdb_episode",
                    },
                }],
            }
            plan.update(changes)
            return plan

        self.assertTrue(server.media_plan_requires_review(plan_with()))
        structured = plan_with(notices=[{
            "code": "retained_unmapped_media",
            "severity": "info",
            "requires_review": False,
            "message": message,
            "evidence": {
                "classification": "engine_generated",
                "evidence_kind": "unmapped_media_preserved",
                "source_paths": [source],
            },
        }])
        self.assertTrue(server.media_plan_requires_review(structured))

        # A diagnostic target does not make an unresolved source executable.
        self.assertTrue(server.media_plan_requires_review(plan_with(problem_files=[{
            "source_path": source,
            "target_path": "/target/Show.E12.5.mkv",
            "reason": message,
        }])))

        unsafe_cases = {
            "planned-source": plan_with(files=[{"source_path": source}]),
            "cleanup-source": plan_with(cleanup_files=[{
                "source_path": source, "reason": "unknown",
            }]),
            "missing-problem": plan_with(problem_files=[]),
            "reason-mismatch": plan_with(problem_files=[{
                "source_path": source,
                "target_path": None,
                "reason": "另一个原因，文件保留原位",
            }]),
        }
        for label, plan in unsafe_cases.items():
            with self.subTest(label=label):
                self.assertTrue(server.media_plan_requires_review(plan))

    def test_terse_unmapped_notice_is_an_unresolved_problem(self):
        message = "无法唯一映射"
        source = "/source/Show.E12.5.mkv"
        plan = {
            "files": [{"source_path": "/source/Show.E01.mkv"}],
            "cleanup_files": [],
            "problem_files": [{
                "source_path": source,
                "target_path": None,
                "reason": message,
            }],
            "warnings": [message],
            "notices": [{
                "code": "planning_warning_requires_review",
                "severity": "warning",
                "requires_review": True,
                "message": message,
                "evidence": {"classification": "engine_generated"},
            }],
        }

        self.assertTrue(server.media_plan_requires_review(plan))
        summary = server.summarize_media_plan(plan)
        self.assertFalse(summary["review"]["automation_eligible"])
        self.assertNotIn("auto_routed_problem_count", summary)
        self.assertNotIn("automatic_handling", summary["problem_files"][0])

    def test_unattended_plan_does_not_rewrite_non_allowlisted_cleanup(self):
        plan = {
            "files": [{"source_path": "/source/episode.mkv"}],
            "cleanup_files": [
                {"source_path": "/source/._episode.mkv", "reason": "hidden"},
                {"source_path": "/source/unknown.mkv", "reason": "模糊推测"},
            ],
            "warnings": [],
            "notices": [{
                "code": "destructive_cleanup_requires_review",
                "requires_review": True, "message": "cleanup",
            }],
        }
        prepared = server.unattended_media_plan(plan)
        self.assertEqual(prepared, plan)
        self.assertIsNot(prepared, plan)
        self.assertTrue(server.media_plan_requires_review(prepared))

    def test_unattended_plan_keeps_engine_proven_simplified_language_dedupe(self):
        preferred = "/source/简中/Show.S01E01.1080p.mp4"
        cleanup = {
            "source_path": "/source/繁中/Show.S01E01.1080p.mp4",
            "reason": (
                "同一 TMDB 集号已有同清晰度同字幕形态的简体中文字幕版本 "
                f"{preferred}，删除繁体中文字幕重复视频"
            ),
        }
        plan = {
            "files": [{"source_path": preferred}],
            "cleanup_files": [cleanup],
            "warnings": [],
            "notices": [],
        }

        prepared = server.unattended_media_plan(plan)

        self.assertEqual(prepared["cleanup_files"], [cleanup])
        self.assertEqual(prepared.get("problem_files", []), [])
        self.assertFalse(server.media_plan_requires_review(prepared))

    def test_subtitle_gap_summary_names_the_missing_special_video(self):
        source = (
            "/source/[Moozzi2] Toaru Kagaku no Railgun T "
            "[SP04] Speical Anime MMR V.sc.ass"
        )
        plan = {
            "files": [{"source_path": "/source/episode.mkv"}],
            "problem_files": [{
                "source_path": source,
                "target_path": (
                    "/library/某科学的超电磁炮/Season 00/"
                    "某科学的超电磁炮 - S00E08 - 更多更多的超电磁炮 MMR 05.zh-CN.ass"
                ),
                "reason": "目标中没有同名视频，字幕将保留原位",
            }],
            "scan_report": {"resource_gaps": [{
                "kind": "subtitle_without_video",
                "label": posixpath.basename(source),
                "reason": "目标中没有同名视频，字幕将保留原位",
                "files": [source],
            }]},
        }

        gap = server.summarize_media_plan(plan)["resource_gaps"][0]

        self.assertEqual(gap["kind"], "subtitle_without_video")
        self.assertIn("某科学的超电磁炮 S00E08", gap["label"])
        self.assertIn("MMR V", gap["label"])
        self.assertIn("缺少对应视频", gap["label"])
        self.assertIn("不会移动或删除该字幕", gap["reason"])
        self.assertIn("逐视频闭环证据", gap["reason"])

    def test_clean_media_plan_still_requires_manual_approval(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            source = "/quark/影视/待刮削/Example"
            parent = "/quark/影视/番剧"
            job = server.Job(
                "c" * 12, source, parent, "auto", False, True,
                phase="planning_media",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv",
                "source_root": source,
                "target_root": f"{parent}/Example",
                "warnings": [],
                "metadata": {"title": "Example", "tmdb_id": 1},
                "files": [],
            }
            digest = server.canonical_digest(plan)
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}), encoding="utf-8"
            )
            with mock.patch.object(server, "auto_execute_media_enabled", return_value=False), mock.patch.object(
                server, "run_command", return_value=(0, "ok\n")
            ), mock.patch.object(
                server, "start_execution"
            ) as start_mock:
                server.plan_media(job)
            self.assertEqual(job.phase, "awaiting_media_approval")
            self.assertEqual(job.digest, digest)
            start_mock.assert_not_called()
        server.JOBS_ROOT = previous_root

    def test_auto_pipeline_executes_unambiguous_plan_without_manual_approval(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            source = "/quark/影视/待刮削/Example"
            parent = "/quark/影视/番剧"
            job = server.Job(
                "f" * 12, source, parent, "auto", False, True,
                phase="planning_media",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv",
                "source_root": source,
                "target_root": f"{parent}/Example",
                "warnings": [],
                "problem_files": [],
                "metadata": {"title": "Example", "tmdb_id": 1},
                "files": [],
            }
            digest = server.canonical_digest(plan)
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}), encoding="utf-8"
            )
            with mock.patch.object(server, "run_command", return_value=(0, "ok\n")), mock.patch.object(
                server, "auto_execute_media_enabled", return_value=True
            ), mock.patch.object(server, "start_execution") as start_mock:
                server.plan_media(job)
            self.assertEqual(job.phase, "starting_media_execution")
            self.assertEqual(job.digest, digest)
            self.assertEqual(job.approval_source, "auto")
            start_mock.assert_called_once_with(
                server.execute_approved_media, job, digest,
            )
            executable = json.loads(
                (job.directory / "media-plan.json").read_text(encoding="utf-8")
            )
            self.assertEqual(executable["schema_version"], 4)
            self.assertIsInstance(executable["created_at"], str)
            self.assertEqual(executable["plan_sha256"], digest)
            self.assertTrue(any("自动流水线" in line for line in job.logs))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_auto_pipeline_fails_closed_for_unmapped_files(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            source = "/quark/影视/待刮削/Example"
            parent = "/quark/影视/番剧"
            job = server.Job(
                "6" * 12, source, parent, "auto", False, True,
                phase="planning_media",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv",
                "source_root": source,
                "target_root": f"{parent}/Example",
                "warnings": [],
                "problem_files": [{
                    "source_path": "/source/unknown.mkv",
                    "reason": "无法唯一映射",
                    "target_path": None,
                }],
                "metadata": {"title": "Example", "tmdb_id": 1},
                "files": [],
            }
            digest = server.canonical_digest(plan)
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}), encoding="utf-8"
            )
            with mock.patch.object(server, "run_command", return_value=(0, "ok\n")), mock.patch.object(
                server, "auto_execute_media_enabled", return_value=True
            ), mock.patch.object(server, "start_execution") as start_mock:
                server.plan_media(job)
            self.assertEqual(job.phase, "awaiting_media_approval")
            self.assertIsNone(job.approval_source)
            self.assertEqual(job.plan_summary["problem_file_count"], 1)
            self.assertFalse(job.plan_summary["review"]["automation_eligible"])
            start_mock.assert_not_called()
        server.JOBS_ROOT = previous_root

    def test_auto_pipeline_scrapes_before_post_processing_resource_gaps(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            source = "/quark/影视/待刮削/Example"
            parent = "/quark/影视/番剧"
            job = server.Job(
                "7" * 12, source, parent, "auto", False, True,
                phase="planning_media",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv",
                "source_root": source,
                "target_root": f"{parent}/Example",
                "warnings": [],
                "problem_files": [],
                "scan_report": {
                    "resource_gaps": [{
                        "kind": "missing_episode",
                        "label": "Season 01 缺少 E03",
                        "reason": "源目录和目标库都没有对应视频。",
                    }],
                },
                "metadata": {"title": "Example", "tmdb_id": 1},
                "files": [],
            }
            digest = server.canonical_digest(plan)
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}), encoding="utf-8"
            )
            with mock.patch.object(server, "run_command", return_value=(0, "ok\n")), mock.patch.object(
                server, "auto_execute_media_enabled", return_value=True
            ), mock.patch.object(server, "start_execution") as start_mock:
                server.plan_media(job)
            self.assertEqual(job.phase, "starting_media_execution")
            self.assertEqual(job.digest, digest)
            self.assertEqual(job.approval_source, "auto")
            self.assertEqual(job.plan_summary["resource_gap_count"], 1)
            start_mock.assert_called_once_with(
                server.execute_approved_media, job, digest,
            )
            self.assertTrue(any("自动流水线" in line for line in job.logs))
        server.JOBS_ROOT = previous_root

    def test_empty_completed_source_residue_is_an_idempotent_success(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            source = "/quark/影视/待刮削/恋如雨止"
            parent = "/quark/影视/番剧"
            target = f"{parent}/恋如雨止"
            job = server.Job(
                "1" * 12, source, parent, "auto", False, True,
                phase="planning_media",
            )
            job.directory.mkdir()

            class FakeClient:
                def list(self, path, refresh=False):
                    self.assert_refresh = refresh
                    return {
                        source: [],
                        parent: [{"name": "恋如雨止", "is_dir": True}],
                        target: [{"name": "恋如雨止 - S01E01.mkv", "is_dir": False}],
                    }.get(path, [])

            output = "❌ 未找到剧集视频文件；目录可能只有字幕或未解压的分卷压缩包，已停止以避免仅移动字幕。\n"
            with mock.patch.object(server, "run_command", return_value=(1, output)), mock.patch.object(
                server, "_execution_alist_client", return_value=FakeClient()
            ):
                server.plan_media(job)

            self.assertEqual(job.phase, "completed")
            self.assertEqual(job.plan_summary["kind"], "noop")
            self.assertEqual(job.plan_summary["target_root"], target)
            self.assertTrue(any("无主要媒体" in line for line in job.logs))
            self.assertIn(source, server.load_processed_paths())
        server.JOBS_ROOT = previous_root

    def test_menu_video_residue_is_an_idempotent_success(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            source = "/quark/影视/待刮削/恋如雨止"
            menu = f"{source}/Menu"
            parent = "/quark/影视/番剧"
            target = f"{parent}/恋如雨止"
            job = server.Job(
                "9" * 12, source, parent, "auto", False, True,
                phase="planning_media",
            )
            job.directory.mkdir()

            class FakeClient:
                def list(self, path, refresh=False):
                    return {
                        source: [{"name": "Menu", "is_dir": True}],
                        menu: [{"name": "[Ygm] Show [Menu01].mkv", "is_dir": False}],
                        parent: [{"name": "恋如雨止", "is_dir": True}],
                        target: [{"name": "恋如雨止 - S01E01.mkv", "is_dir": False}],
                    }.get(path, [])

            output = "❌ 未找到剧集视频文件；已停止。\n"
            with mock.patch.object(server, "run_command", return_value=(1, output)), mock.patch.object(
                server, "_execution_alist_client", return_value=FakeClient()
            ):
                server.plan_media(job)

            self.assertEqual(job.phase, "completed")
            self.assertEqual(job.plan_summary["kind"], "noop")
            self.assertIn("菜单片段", "\n".join(job.logs))
        server.JOBS_ROOT = previous_root

    def test_subtitle_only_source_remains_a_visible_failure(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            source = "/quark/影视/待刮削/缺视频的作品"
            job = server.Job(
                "2" * 12, source, "/quark/影视/番剧", "auto", False, True,
                phase="planning_media",
            )
            job.directory.mkdir()

            class FakeClient:
                def list(self, path, refresh=False):
                    if path == source:
                        return [{"name": "Show.S01E01.ass", "is_dir": False}]
                    return []

            output = "❌ 未找到剧集视频文件；目录可能只有字幕或未解压的分卷压缩包。\n"
            with mock.patch.object(server, "run_command", return_value=(1, output)), mock.patch.object(
                server, "_execution_alist_client", return_value=FakeClient()
            ):
                server.plan_media(job)

            self.assertEqual(job.phase, "failed")
            self.assertIn("未找到剧集视频文件", job.error)
        server.JOBS_ROOT = previous_root

    def test_empty_source_is_not_completed_against_metadata_only_target(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            source = "/quark/影视/待刮削/作品"
            parent = "/quark/影视/番剧"
            target = f"{parent}/作品"
            job = server.Job("3" * 12, source, parent, "auto", False, True, phase="planning_media")
            job.directory.mkdir()

            class FakeClient:
                def list(self, path, refresh=False):
                    return {
                        source: [],
                        parent: [{"name": "作品", "is_dir": True}],
                        target: [{"name": "poster.jpg", "is_dir": False}],
                    }.get(path, [])

            output = "❌ 未找到剧集视频文件；已停止。\n"
            with mock.patch.object(server, "run_command", return_value=(1, output)), mock.patch.object(
                server, "_execution_alist_client", return_value=FakeClient()
            ):
                server.plan_media(job)
            self.assertEqual(job.phase, "failed")
        server.JOBS_ROOT = previous_root

    def test_empty_source_with_ambiguous_year_targets_is_not_auto_completed(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            source = "/quark/影视/待刮削/作品"
            parent = "/quark/影视/番剧"
            target_2020 = f"{parent}/作品 (2020)"
            target_2021 = f"{parent}/作品 (2021)"
            job = server.Job("4" * 12, source, parent, "auto", False, True, phase="planning_media")
            job.directory.mkdir()

            class FakeClient:
                def list(self, path, refresh=False):
                    return {
                        source: [],
                        parent: [
                            {"name": "作品 (2020)", "is_dir": True},
                            {"name": "作品 (2021)", "is_dir": True},
                        ],
                        target_2020: [{"name": "作品 - S01E01.mkv", "is_dir": False}],
                        target_2021: [{"name": "作品 - S01E01.mkv", "is_dir": False}],
                    }.get(path, [])

            output = "❌ 未找到剧集视频文件；已停止。\n"
            with mock.patch.object(server, "run_command", return_value=(1, output)), mock.patch.object(
                server, "_execution_alist_client", return_value=FakeClient()
            ):
                server.plan_media(job)
            self.assertEqual(job.phase, "failed")
        server.JOBS_ROOT = previous_root

    def test_empty_source_proof_fails_closed_on_malformed_alist_row(self):
        class FakeClient:
            def list(self, path, refresh=False):
                return [{"name": None, "is_dir": False}]

        with self.assertRaisesRegex(ValueError, "无法验证"):
            server._directory_has_file(FakeClient(), "/quark/影视/待刮削/作品")

    def test_media_planning_retries_transient_tmdb_tls_failure_automatically(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            source = "/quark/影视/待刮削/Example"
            parent = "/quark/影视/番剧"
            job = server.Job(
                "b" * 12, source, parent, "auto", False, True,
                phase="planning_media",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv",
                "source_root": source,
                "target_root": f"{parent}/Example",
                "warnings": [],
                "metadata": {"title": "Example", "tmdb_id": 1},
                "files": [],
            }
            digest = server.canonical_digest(plan)
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}),
                encoding="utf-8",
            )
            with mock.patch.object(
                server,
                "run_command",
                side_effect=[
                    (1, "TMDB HTTPS 证书校验失败\n"),
                    (0, "ok\n"),
                ],
            ) as runner, mock.patch.object(
                server, "auto_execute_media_enabled", return_value=False
            ), mock.patch.object(server.time, "sleep"):
                server.plan_media(job)
            self.assertEqual(runner.call_count, 2)
            self.assertEqual(job.phase, "awaiting_media_approval")
            self.assertTrue(any("自动重试整理计划" in line for line in job.logs))
        server.JOBS_ROOT = previous_root

    def test_exhausted_transient_tmdb_failure_stays_in_unattended_retry_queue(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job(
                "c" * 12, "/source", "/target", "auto", False, True,
                phase="planning_media",
            )
            job.directory.mkdir()
            (job.directory / "media-plan.json").write_text("stale", encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_PLANNING_RETRY_DELAY": "1",
            }, clear=False), mock.patch.object(
                server, "run_command",
                return_value=(1, "TMDB HTTPS 证书校验失败\n"),
            ) as runner, mock.patch.object(
                server, "auto_execute_media_enabled", return_value=True,
            ), mock.patch.object(server.time, "sleep"), mock.patch.object(
                server.threading.Thread, "start",
            ) as thread_start:
                server.plan_media(job)
            self.assertEqual(runner.call_count, 3)
            self.assertEqual(job.phase, "queued")
            self.assertIsNone(job.error)
            self.assertEqual(job.progress["stage"], "planning_retry_wait")
            self.assertFalse((job.directory / "media-plan.json").exists())
            thread_start.assert_called_once()
        server.JOBS_ROOT = previous_root

    def test_explicit_tv_jobs_use_smart_multi_season_mode(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job(
                "e" * 12,
                "/source/show",
                "/target",
                "tv",
                False,
                True,
                phase="planning_media",
                tmdb_id=1,
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv",
                "source_root": "/source/show",
                "target_root": "/target/show",
                "warnings": [],
                "metadata": {"title": "Example", "tmdb_id": 1},
                "files": [],
            }
            digest = server.canonical_digest(plan)
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}), encoding="utf-8"
            )
            with mock.patch.object(
                server, "run_command", return_value=(0, "ok\n")
            ) as runner, mock.patch.object(server, "start_execution"):
                server.plan_media(job)
            self.assertIn("--auto-episode-mode", runner.call_args.args[1])
        server.JOBS_ROOT = previous_root

    def test_resume_preserves_manual_approval_for_old_cleanup_only_plan(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            plan = {
                "mode": "tv",
                "source_root": "/source/show",
                "target_root": "/target/show",
                "warnings": [
                    "确认执行后将删除明确无用的系统隐藏/片头片尾/广告文件：广告.jpg"
                ],
                "cleanup_files": [
                    {"source_path": "/source/show/广告.jpg", "reason": "发布组广告图片"}
                ],
                "problem_files": [],
                "files": [],
            }
            digest = server.canonical_digest(plan)
            job = server.Job(
                "d" * 12, "/source/show", "/source", "auto", False, True,
                phase="awaiting_media_approval", digest=digest,
            )
            job.directory.mkdir()
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}), encoding="utf-8"
            )
            server.JOBS[job.id] = job
            with mock.patch.object(
                server, "auto_execute_media_enabled", return_value=False
            ), mock.patch.object(server, "start_execution") as start_mock:
                server.resume_jobs()
            self.assertEqual(job.phase, "awaiting_media_approval")
            self.assertEqual(job.digest, digest)
            start_mock.assert_not_called()
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_resume_demotes_unmapped_auto_plan_to_review(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            plan = {
                "mode": "tv",
                "source_root": "/source/show",
                "target_root": "/target/show",
                "warnings": [],
                "problem_files": [{
                    "source_path": "/source/show/unknown.mkv",
                    "reason": "无法唯一映射",
                }],
                "metadata": {"title": "Example", "tmdb_id": 1},
                "files": [],
            }
            digest = server.canonical_digest(plan)
            job = server.Job(
                "c" * 12, "/source/show", "/target", "auto", False, True,
                phase="starting_media_execution", digest=digest,
            )
            job.directory.mkdir()
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}), encoding="utf-8"
            )
            server.JOBS[job.id] = job
            with mock.patch.object(server, "start_execution") as start_mock:
                server.resume_jobs()
            self.assertEqual(job.phase, "awaiting_media_approval")
            self.assertIsNone(job.approval_source)
            start_mock.assert_not_called()
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_resume_continues_unattended_recovery_approval(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            job = server.Job(
                "b" * 12, "/quark/影视/待刮削/Old", "/quark/影视/番剧",
                "auto", False, True, phase="awaiting_recovery_approval",
                digest="c" * 64,
            )
            job.directory.mkdir()
            server.JOBS[job.id] = job
            with mock.patch.object(
                server, "auto_execute_media_enabled", return_value=True,
            ), mock.patch.object(server, "start_execution") as start_mock:
                server.resume_jobs()
            self.assertEqual(job.phase, "starting_recovery_execution")
            self.assertIsNone(job.error)
            start_mock.assert_called_once_with(
                server.execute_approved_recovery, job, "c" * 64,
            )
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_media_plan_envelope_is_unwrapped_and_digest_verified(self):
        plan = {
            "mode": "tv",
            "source_root": "/source",
            "target_root": "/target",
            "warnings": [],
            "metadata": {"title": "Example", "tmdb_id": 1},
            "files": [],
        }
        digest = server.canonical_digest(plan)
        unwrapped, verified = server.unwrap_media_plan(
            {"schema_version": 3, "plan_sha256": digest, "plan": plan}
        )
        self.assertEqual(unwrapped, plan)
        self.assertEqual(verified, digest)
        with self.assertRaisesRegex(ValueError, "不一致"):
            server.unwrap_media_plan(
                {"schema_version": 3, "plan_sha256": "0" * 64, "plan": plan}
            )

    def test_redacts_runtime_secrets(self):
        previous = os.environ.get("ALIST_PASSWORD")
        previous_adapter = os.environ.get("SCRAPEFLOW_REPLENISHMENT_TOKEN")
        os.environ["ALIST_PASSWORD"] = "unit-test-secret"
        os.environ["SCRAPEFLOW_REPLENISHMENT_TOKEN"] = "adapter-test-secret"
        try:
            self.assertEqual(server.redact("value=unit-test-secret"), "value=[REDACTED]")
            self.assertEqual(server.redact("value=adapter-test-secret"), "value=[REDACTED]")
        finally:
            if previous is None:
                os.environ.pop("ALIST_PASSWORD", None)
            else:
                os.environ["ALIST_PASSWORD"] = previous
            if previous_adapter is None:
                os.environ.pop("SCRAPEFLOW_REPLENISHMENT_TOKEN", None)
            else:
                os.environ["SCRAPEFLOW_REPLENISHMENT_TOKEN"] = previous_adapter
        redacted = server.redact(
            "archive_pass=archive-secret /归档/压缩包密码：folder-secret/file.7z"
        )
        self.assertNotIn("archive-secret", redacted)
        self.assertNotIn("folder-secret", redacted)

    def test_command_failure_reason_prefers_actionable_engine_error(self):
        output = (
            "扫描中\n普通信息\n"
            "❌ 多集文件的结束集数小于开始集数: /media/show.mkv\n"
            "错误: 媒体识别计划生成失败，请查看实时日志\n"
        )
        self.assertEqual(
            server.command_failure_reason(output, "媒体识别失败"),
            "多集文件的结束集数小于开始集数: /media/show.mkv",
        )
        self.assertEqual(
            server.command_failure_reason("错误: 缺少 3.zip.001\n", "解压失败"),
            "缺少 3.zip.001",
        )
        self.assertEqual(server.command_failure_reason("普通输出\n", "识别失败"), "识别失败")
        self.assertEqual(
            server.command_failure_reason(
                "补源适配器失败: aria2c 下载失败: 0 B/s\n", "自动查补获取失败"
            ),
            "aria2c 下载失败: 0 B/s",
        )
        self.assertEqual(
            server.command_failure_reason(
                "Traceback (most recent call last):\n"
                "http.client.IncompleteRead: IncompleteRead(0 bytes read)\n",
                "识别失败",
            ),
            "http.client.IncompleteRead: IncompleteRead(0 bytes read)",
        )

    def test_job_state_round_trip_and_interruption_detection(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            job = server.Job(
                id="a" * 12,
                source="/media/show",
                parent="/media",
                media_type="auto",
                absolute=False,
                prefer_simplified=True,
                phase="executing_media",
            )
            job.directory.mkdir()
            server.persist_job(job)
            (job.directory / "media-journal.json").write_text("{}", encoding="utf-8")
            server.restore_jobs()
            restored = server.JOBS[job.id]
            self.assertEqual(restored.phase, "recovery_required")
            self.assertIn("恢复", restored.error)
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_restore_refreshes_user_facing_gap_summary_from_signed_plan(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            source = "/source/Railgun T [SP04] MMR V.sc.ass"
            plan = {
                "mode": "tv",
                "source_root": "/source",
                "target_root": "/library/某科学的超电磁炮",
                "files": [{"source_path": "/source/episode.mkv"}],
                "cleanup_files": [],
                "problem_files": [{
                    "source_path": source,
                    "target_path": (
                        "/library/某科学的超电磁炮/Season 00/"
                        "某科学的超电磁炮 - S00E08 - 更多更多的超电磁炮 MMR 05.zh-CN.ass"
                    ),
                    "reason": "目标中没有同名视频，字幕将保留原位",
                }],
                "warnings": [],
                "metadata": {"title": "某科学的超电磁炮", "tmdb_id": 46260},
                "scan_report": {"resource_gaps": [{
                    "kind": "subtitle_without_video",
                    "label": posixpath.basename(source),
                    "reason": "目标中没有同名视频，字幕将保留原位",
                    "files": [source],
                }]},
            }
            digest = server.canonical_digest(plan)
            job = server.Job(
                "e" * 12, "/source", "/library", "auto", False, True,
                phase="awaiting_media_approval", digest=digest,
                plan_summary={"resource_gaps": [{"label": "旧摘要"}]},
            )
            job.directory.mkdir()
            server.persist_job(job)
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}),
                encoding="utf-8",
            )

            server.restore_jobs()

            gap = server.JOBS[job.id].plan_summary["resource_gaps"][0]
            self.assertIn("某科学的超电磁炮 S00E08", gap["label"])
            self.assertIn("MMR V", gap["label"])

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_public_job_exposes_only_fields_used_by_the_web_client(self):
        job = server.Job("b" * 12, "/media/show", "/media", "auto", False, True)
        job.logs = ["one", "two", "three"]
        job.episode_map = {"1": "S01E01"}
        value = job.public()
        self.assertEqual(
            set(value),
            {
                "id", "source", "parent", "updated_at", "phase", "error", "digest", "plan",
                "recovery_available", "queue_position", "queue_kind", "progress", "settings",
                "visibility",
            },
        )
        self.assertEqual(value["visibility"], "user")
        self.assertEqual(value["settings"]["media_type"], "auto")
        self.assertIsNone(value["settings"]["tmdb_id"])
        self.assertNotIn("logs", value)
        self.assertNotIn("episode_map", value)

    def test_restore_keeps_failed_history_visible_and_byte_stable(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            older_failed = server.Job(
                "1" * 12, "/source/show", "/target", "auto", False, True,
                phase="failed", updated_at="2026-07-27T00:00:00+00:00",
            )
            latest_completed = server.Job(
                "2" * 12, "/source/show", "/target", "auto", False, True,
                phase="completed", updated_at="2026-07-28T00:00:00+00:00",
            )
            unrelated_failed = server.Job(
                "3" * 12, "/source/other", "/target", "auto", False, True,
                phase="failed", updated_at="2026-07-26T00:00:00+00:00",
            )
            for job in (older_failed, latest_completed, unrelated_failed):
                job.directory.mkdir(parents=True)
                server.persist_job(job)
            before = older_failed.state_path.read_bytes()

            server.restore_jobs()

            self.assertEqual(server.JOBS[older_failed.id].visibility, "user")
            self.assertEqual(server.JOBS[older_failed.id].phase, "failed")
            self.assertEqual(server.JOBS[latest_completed.id].visibility, "user")
            self.assertEqual(server.JOBS[unrelated_failed.id].visibility, "user")
            self.assertEqual(older_failed.state_path.read_bytes(), before)

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_legacy_one_time_lineage_is_loaded_read_only_and_never_resumed(self):
        previous_root = server.JOBS_ROOT
        previous_state_root = server.STATE_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        try:
            with tempfile.TemporaryDirectory() as directory:
                server.STATE_ROOT = Path(directory)
                server.JOBS_ROOT = server.STATE_ROOT / "jobs"
                server.JOBS_ROOT.mkdir()
                server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
                server.JOBS = {}
                owner = server.Job(
                    "1" * 12,
                    "/quark/影视/番剧/Legacy (2020) {tmdb-1}",
                    "/quark/影视/番剧",
                    "tv", False, True,
                    visibility="internal",
                    phase="planning_media",
                    approval_source=server.LEGACY_ONE_TIME_APPROVAL_SOURCE,
                )
                child = server.Job(
                    "2" * 12,
                    "/quark/影视/ScrapeFlow/补源/legacy-child",
                    "/quark/影视/番剧",
                    "tv", False, True,
                    visibility="internal",
                    phase="queued",
                    root_job_id=owner.id,
                )
                for job in (owner, child):
                    job.directory.mkdir()
                    server.persist_job(job)
                (owner.directory / "media-plan.json").write_text(
                    '{"historical":true}\n', encoding="utf-8",
                )
                before = {
                    path.relative_to(server.JOBS_ROOT).as_posix(): path.read_bytes()
                    for path in server.JOBS_ROOT.rglob("*") if path.is_file()
                }

                server.restore_jobs()
                restored = server.JOBS[owner.id]
                self.assertEqual(restored.phase, "planning_media")
                self.assertTrue(server.is_legacy_one_time_owner(restored))
                with mock.patch.object(server, "start_thread") as start_analysis, \
                        mock.patch.object(server, "start_execution") as start_execution, \
                        mock.patch.object(
                            server, "close_consumed_internal_replenishment_followup",
                        ) as close_child, mock.patch.object(
                            server, "_repair_failed_replenishment_followup",
                        ) as repair_child:
                    server.resume_jobs()
                start_analysis.assert_not_called()
                start_execution.assert_not_called()
                close_child.assert_not_called()
                repair_child.assert_not_called()
                after = {
                    path.relative_to(server.JOBS_ROOT).as_posix(): path.read_bytes()
                    for path in server.JOBS_ROOT.rglob("*") if path.is_file()
                }
                self.assertEqual(after, before)
                for operation in (
                    lambda: server.request_cancel(restored),
                    lambda: server.request_recovery(restored),
                    lambda: server.retry_job(restored),
                    lambda: server.approve_job(restored, {"digest": "a" * 64}),
                    lambda: server.resolve_failed_job(
                        restored, {"confirm": True, "action": "keep_existing"},
                    ),
                    lambda: server.delete_job(restored),
                ):
                    with self.assertRaisesRegex(ValueError, "永久停用"):
                        operation()
                restored_child = server.JOBS[child.id]
                for operation in (
                    lambda: server.request_cancel(restored_child),
                    lambda: server.retry_job(restored_child),
                    lambda: server.approve_job(restored_child, {"digest": "a" * 64}),
                    lambda: server.start_thread(lambda _job: None, restored_child),
                    lambda: server.start_execution(lambda _job: None, restored_child),
                    lambda: server.delete_job(restored_child),
                ):
                    with self.assertRaisesRegex(ValueError, "永久停用"):
                        operation()

                index_core = {
                    "schema_version": 1,
                    "kind": "retired_legacy_one_time_root_index",
                    "updated_at": "2026-08-05T00:00:00+00:00",
                    "roots": {
                        owner.id: {
                            "owner_job_id": owner.id,
                            "member_job_ids": sorted([owner.id, child.id]),
                            "migration_id": "20260805T000000Z-aaaaaaaaaaaa",
                            "plan_sha256": "a" * 64,
                            "archive_root": (
                                "archive/legacy-one-time-owners/"
                                "20260805T000000Z-aaaaaaaaaaaa/jobs"
                            ),
                            "status": "retired",
                            "updated_at": "2026-08-05T00:00:00+00:00",
                        },
                    },
                }
                server._atomic_json(
                    server.STATE_ROOT / "retired-one-time-roots.json",
                    {**index_core, "index_sha256": server.canonical_digest(index_core)},
                )
                server.shutil.rmtree(owner.directory)
                server.restore_jobs()
                orphaned_child = server.JOBS[child.id]
                self.assertTrue(server.is_legacy_one_time_lineage(orphaned_child))
                self.assertEqual(orphaned_child.phase, "queued")
                with mock.patch.object(server, "start_thread") as start_analysis:
                    server.resume_jobs()
                start_analysis.assert_not_called()
                with self.assertRaisesRegex(ValueError, "永久停用"):
                    server.retry_job(orphaned_child)
        finally:
            server.JOBS = previous_jobs
            server.JOBS_ROOT = previous_root
            server.STATE_ROOT = previous_state_root
            server.Job.root_provider = staticmethod(previous_provider)


    def test_structured_delivery_failure_overrides_old_zero_speed_log(self):
        job = server.Job(
            "d" * 12, "/source", "/target", "tv", False, True,
            phase="failed", error="delivery failed",
            plan_summary={
                "kind": "media", "title": "Example",
                "replenishment": {
                    "status": "acquire_failed",
                    "projects": [{
                        "status": "acquire_failed",
                        "message": "EntityTooSmall ProposedSize=0",
                        "failure_scope": "delivery",
                        "failure_stage": "delivery_upload",
                        "reusable_candidate": True,
                    }],
                },
            },
        )
        job.logs = [
            "older candidate ended at 0 B/s",
            "补源适配器失败: EntityTooSmall ProposedSize=0",
        ]
        detail = job.public()["plan"]["replenishment"]["failure_detail"]
        self.assertEqual(detail["stage"], "AList 文件上传")
        self.assertIn("云端交付", detail["summary"])
        self.assertIn("不隔离", detail["next_action"])

    def test_docker_loopback_alist_url_targets_host(self):
        with mock.patch.dict(os.environ, {"SCRAPEFLOW_DOCKER": "1", "ALIST_URL": "http://127.0.0.1:5244"}):
            self.assertEqual(server.alist_url(), "http://host.docker.internal:5244")
            self.assertIn("--allow-insecure-http", server.common_connection_args())

    def test_explicit_docker_alist_service_allows_private_http(self):
        environment = {
            "SCRAPEFLOW_DOCKER": "1",
            "SCRAPEFLOW_TRUST_DOCKER_ALIST": "1",
            "ALIST_URL": "http://alist:5244",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(server.alist_url(), "http://alist:5244")
            self.assertIn("--allow-insecure-http", server.common_connection_args())

    def test_job_options_reject_ambiguous_or_incompatible_combinations(self):
        common = {"category": "番剧"}
        with self.assertRaisesRegex(ValueError, "明确选择"):
            server.create_job({"path": "/quark/影视/待刮削/show", "type": "auto", "tmdb_id": 123, **common})
        with self.assertRaisesRegex(ValueError, "只适用于"):
            server.create_job({"path": "/quark/影视/待刮削/movie", "type": "movie", "absolute": True, **common})
        with self.assertRaisesRegex(ValueError, "Episode Group"):
            server.create_job({"path": "/quark/影视/待刮削/show", "type": "tv", "episode_group": "group-1", **common})
        with self.assertRaisesRegex(ValueError, "媒体类型"):
            server.create_job({"path": "/quark/影视/待刮削/show", "type": [], **common})

    def test_movie_identity_keeps_the_original_anime_target_boundary(self):
        job = server.Job(
            "f" * 12, "/quark/影视/待刮削/剧场版",
            "/quark/影视/番剧/Fate", "auto", False, True,
        )
        plan = {
            "mode": "movie",
            "target_root": "/quark/影视/番剧/Fate/剧场版 (2026)",
            "files": [{
                "target_dir": "/quark/影视/番剧/Fate/剧场版 (2026)",
            }],
        }
        server.validate_plan_target_boundary(job, plan)

        outside_root = {**plan, "target_root": "/quark/影视/电影/剧场版 (2026)"}
        with self.assertRaisesRegex(ValueError, "越出原始刮削目标目录"):
            server.validate_plan_target_boundary(job, outside_root)

        outside_file = {
            **plan,
            "files": [{"target_dir": "/quark/影视/电影/剧场版 (2026)"}],
        }
        with self.assertRaisesRegex(ValueError, "越出原始刮削目标目录"):
            server.validate_plan_target_boundary(job, outside_file)

    def test_new_jobs_require_unscraped_source_and_scraped_category(self):
        with self.assertRaisesRegex(ValueError, "源目录必须位于"):
            server.create_job({"path": "/quark/影视/番剧/show", "category": "番剧"})
        with self.assertRaisesRegex(ValueError, "请选择目标分类"):
            server.create_job({"path": "/quark/影视/待刮削/show"})
        with self.assertRaisesRegex(ValueError, "所选分类"):
            server.create_job({
                "path": "/quark/影视/待刮削/show",
                "category": "番剧",
                "parent": "/quark/影视/电影",
            })
        with self.assertRaisesRegex(ValueError, "待删除"):
            server.create_job({
                "path": "/quark/影视/待刮削/已整理作品（待删）",
                "category": "番剧",
            })

    def test_new_job_has_a_durable_log_before_async_planning(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            server.JOBS = {}
            try:
                with mock.patch.object(server, "start_thread") as starter:
                    job = server.create_job({
                        "path": "/quark/影视/待刮削/新作品",
                        "category": "番剧",
                    })
                starter.assert_called_once()
                self.assertTrue(job.log_path.is_file())
                self.assertIn("任务已创建", job.log_path.read_text(encoding="utf-8"))
            finally:
                server.JOBS = previous_jobs
                server.JOBS_ROOT = previous_root
                server.Job.root_provider = previous_provider

    def test_user_jobs_reject_system_managed_replenishment_sources(self):
        paths = [
            "/quark/影视/ScrapeFlow/补源/Example",
            "/quark/影视/ScrapeFlow/备份/Example",
            "/quark/影视/ScrapeFlow/验证/Example",
            "/quark/影视/待刮削/ScrapeFlow补源-42-Example",
            "/quark/影视/待刮削/_ScrapeFlow补源-42-Example",
            "/quark/影视/待刮削/旧目录/ScrapeFlow补源-42-Example",
            "/quark/影视/待刮削/ScrapeFlow补源-42-Example/子目录",
        ]
        with mock.patch.object(server, "start_thread") as starter:
            for path in paths:
                with self.subTest(path=path), self.assertRaisesRegex(
                    ValueError, "系统内部|系统内部管理",
                ):
                    server.create_job({"path": path, "category": "番剧"})
        starter.assert_not_called()

    def test_internal_followup_source_validator_accepts_only_replenishment_staging(self):
        accepted = [
            "/quark/影视/ScrapeFlow/补源/Example",
            "/quark/影视/待刮削/ScrapeFlow补源-42-Example",
            "/quark/影视/待刮削/_ScrapeFlow补源-42-Example",
        ]
        for path in accepted:
            with self.subTest(path=path):
                self.assertEqual(server.replenishment_source_path(path), path)
        for path in (
            "/quark/影视/ScrapeFlow/备份/Example",
            "/quark/影视/ScrapeFlow/验证/Example",
        ):
            with self.subTest(path=path), self.assertRaisesRegex(
                ValueError, "系统内部管理",
            ):
                server.replenishment_source_path(path)

    def test_restore_reconciles_historical_pending_delete_planning_failure(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "e" * 12,
                "/quark/影视/待刮削/已整理作品 (2026)（待删）",
                "/quark/影视/番剧", "auto", False, True,
                phase="failed", error="TMDB 未找到自动匹配候选",
                progress={"stage": "planning_start", "percent": 5.0},
            )
            job.directory.mkdir()
            self.assertTrue(server.reconcile_pending_delete_job(job))
            self.assertEqual(job.phase, "cancelled")
            self.assertIsNone(job.error)
            self.assertEqual(job.progress["stage"], "source_excluded")
            self.assertIn("不再查询 TMDB", job.log_path.read_text(encoding="utf-8"))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_resume_retries_only_latest_planning_miss_with_new_title_evidence(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            source = "/quark/影视/待刮削/Z自称恶役大小姐的婚约者观察记录 [1080p]"
            old = server.Job(
                "1" * 12, source, "/quark/影视/番剧", "auto", False, True,
                phase="failed", error="TMDB 未找到自动匹配候选: Z自称恶役大小姐的婚约者观察记录",
                progress={"stage": "planning_start", "percent": 5.0},
                updated_at="2026-07-27T01:00:00+00:00",
            )
            latest = server.Job(
                "2" * 12, source, "/quark/影视/番剧", "auto", False, True,
                phase="failed", error="TMDB 未找到自动匹配候选: Z自称恶役大小姐的婚约者观察记录",
                progress={"stage": "planning_start", "percent": 5.0},
                updated_at="2026-07-27T02:00:00+00:00",
            )
            ambiguous = server.Job(
                "3" * 12, "/quark/影视/待刮削/未知作品", "/quark/影视/番剧",
                "auto", False, True, phase="failed",
                error="TMDB 未找到自动匹配候选: 未知作品",
                progress={"stage": "planning_start", "percent": 5.0},
            )
            for job in (old, latest, ambiguous):
                job.directory.mkdir()
            server.JOBS = {job.id: job for job in (old, latest, ambiguous)}
            with mock.patch.object(server, "start_thread") as starter:
                server.resume_jobs()
            self.assertEqual(old.phase, "failed")
            self.assertEqual(latest.phase, "queued")
            self.assertEqual(ambiguous.phase, "failed")
            starter.assert_called_once_with(server.prepare_job, latest)
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_concurrent_normalized_source_creation_returns_original_job(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            server.JOBS = {}
            barrier = threading.Barrier(2)
            results = []

            def create(path):
                barrier.wait()
                try:
                    results.append(server.create_job({"path": path, "category": "番剧"}))
                except server.ExistingJobConflict as exc:
                    results.append(exc)

            with mock.patch.object(server, "start_thread") as starter:
                threads = [
                    threading.Thread(target=create, args=("/quark/影视/待刮削/作品/",)),
                    threading.Thread(target=create, args=("/quark/影视/待刮削/作品",)),
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=2)

            created = [row for row in results if isinstance(row, server.Job)]
            conflicts = [row for row in results if isinstance(row, server.ExistingJobConflict)]
            self.assertEqual(len(created), 1)
            self.assertEqual(len(conflicts), 1)
            self.assertIs(conflicts[0].job, created[0])
            self.assertEqual(list(server.JOBS), [created[0].id])
            self.assertEqual(len(list(server.JOBS_ROOT.iterdir())), 1)
            starter.assert_called_once()
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_completed_source_cannot_be_submitted_as_a_second_job(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            source = "/quark/影视/待刮削/作品"
            completed = server.Job(
                "f" * 12, source, "/quark/影视/番剧",
                "auto", False, True, phase="completed",
            )
            completed.directory.mkdir()
            server.JOBS = {completed.id: completed}
            with mock.patch.object(server, "start_thread") as starter:
                with self.assertRaises(server.ExistingJobConflict) as raised:
                    server.create_job({"path": source, "category": "番剧"})
            self.assertIs(raised.exception.job, completed)
            starter.assert_not_called()
            self.assertEqual(list(server.JOBS), [completed.id])
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)


    def test_delete_job_only_removes_safe_local_task_state(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()

            disposable = server.Job("1" * 12, "/media/show", "/media", "auto", False, True, phase="failed")
            disposable.directory.mkdir()
            (disposable.directory / "job.json").write_text("{}", encoding="utf-8")
            server.JOBS[disposable.id] = disposable
            server.delete_job(disposable)
            self.assertNotIn(disposable.id, server.JOBS)
            self.assertFalse(disposable.directory.exists())

            running = server.Job("2" * 12, "/media/running", "/media", "auto", False, True, phase="executing_media")
            running.directory.mkdir()
            server.JOBS[running.id] = running
            with self.assertRaisesRegex(ValueError, "先停止"):
                server.delete_job(running)

            recovery = server.Job("3" * 12, "/media/recovery", "/media", "auto", False, True, phase="failed")
            recovery.directory.mkdir()
            (recovery.directory / "media-journal.json").write_text("{}", encoding="utf-8")
            server.JOBS[recovery.id] = recovery
            with self.assertRaisesRegex(ValueError, "恢复"):
                server.delete_job(recovery)

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_delete_job_blocks_incomplete_remote_transactions_in_every_terminal_phase(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            cases = (
                ("cancelled", "upload_uncertain", b"only-local-copy"),
                ("failed", "staged", b"staged-copy"),
                ("failed", "upload_intent", b"upload-intent-copy"),
                ("completed", "target_verified", b"verified-copy"),
                ("completed", "source_delete_intent", b"delete-intent-copy"),
            )
            for index, (phase, state, payload) in enumerate(cases):
                with self.subTest(phase=phase, state=state):
                    job = server.Job(
                        f"4{index:011x}", f"/media/{index}", "/media",
                        "auto", False, True, phase=phase,
                    )
                    job.directory.mkdir()
                    transaction = write_remote_transaction_fixture(
                        job, state=state, payload=payload,
                    )
                    server.JOBS[job.id] = job

                    with self.assertRaisesRegex(ValueError, state):
                        server.delete_job(job)

                    self.assertIn(job.id, server.JOBS)
                    self.assertTrue(job.directory.exists())
                    self.assertEqual(
                        (transaction / "payload.bin").read_bytes(), payload,
                    )
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_delete_job_blocks_corrupt_and_orphan_remote_transaction_artifacts(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            cases = (
                "corrupt_journal", "payload.bin", "payload.part",
                "transaction.lock", "unknown.artifact",
            )
            for index, artifact in enumerate(cases):
                with self.subTest(artifact=artifact):
                    job = server.Job(
                        f"5{index:011x}", f"/media/{index}", "/media",
                        "auto", False, True, phase="completed",
                    )
                    job.directory.mkdir()
                    transaction = (
                        job.directory / ".remote-file-transactions" / "orphan"
                    )
                    transaction.mkdir(parents=True)
                    if artifact == "corrupt_journal":
                        (transaction / "journal.json").write_text(
                            "{broken", encoding="utf-8",
                        )
                    else:
                        (transaction / artifact).write_bytes(b"forensic")
                    server.JOBS[job.id] = job

                    with self.assertRaisesRegex(
                        ValueError, "journal|payload|part|lock|\u635f\u574f|\u5b64\u7acb",
                    ):
                        server.delete_job(job)

                    self.assertTrue(transaction.exists())
                    self.assertIn(job.id, server.JOBS)
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_complete_remote_transaction_with_payload_or_unknown_nested_artifact_blocks(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            for index, artifact in enumerate(("payload.bin", "nested/journal.json")):
                with self.subTest(artifact=artifact):
                    job = server.Job(
                        f"9{index:011x}", f"/media/{index}", "/media",
                        "auto", False, True, phase="completed",
                    )
                    job.directory.mkdir()
                    transaction = write_remote_transaction_fixture(
                        job, state="complete",
                    )
                    extra = transaction / artifact
                    extra.parent.mkdir(parents=True, exist_ok=True)
                    extra.write_bytes(b"must-survive")
                    server.JOBS[job.id] = job

                    with self.assertRaisesRegex(ValueError, "payload|\u672a\u77e5|\u5d4c\u5957"):
                        server.delete_job(job)

                    self.assertEqual(extra.read_bytes(), b"must-survive")
                    self.assertIn(job.id, server.JOBS)
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_delete_job_allows_only_fully_complete_remote_transactions(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            job = server.Job(
                "6" * 12, "/media/complete", "/media", "auto",
                False, True, phase="completed",
            )
            job.directory.mkdir()
            write_remote_transaction_fixture(job, state="complete")
            server.JOBS[job.id] = job

            server.delete_job(job)

            self.assertNotIn(job.id, server.JOBS)
            self.assertFalse(job.directory.exists())
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_hybrid_lifecycle_commit_is_bound_and_idempotent(self):
        job = server.Job(
            "a1" * 6, "/quark/影视/待刮削/Hybrid",
            "/quark/影视/番剧", "tv", False, True, phase="completed",
        )
        job.directory.mkdir()
        client, spec, state_root = write_hybrid_transaction_fixture(job)

        with mock.patch.object(
            server, "_execution_alist_client", return_value=client,
        ):
            first = server._commit_job_transaction_quarantines(job)
            second = server._commit_job_transaction_quarantines(job)

        self.assertEqual(first["status"], "committed")
        self.assertEqual(second["status"], "already_committed")
        self.assertIsNone(client.exact_file_info(spec.source_path))
        self.assertIsNotNone(client.exact_file_info(str(spec.target_path)))
        self.assertIsNone(client.exact_file_info(spec.rollback_path))
        self.assertEqual(
            server.load_json(state_root / spec.batch_id / "batch.json")["state"],
            "committed",
        )
        self.assertEqual(
            server.load_json(
                job.directory / "hybrid-transaction-lifecycle.json",
            )["outcome"],
            "accepted",
        )
        self.assertEqual(server._transaction_lifecycle_cleanup_blockers(job), [])

    def test_pending_hybrid_batch_blocks_delete_until_exact_abort(self):
        job = server.Job(
            "a2" * 6, "/quark/影视/待刮削/Hybrid",
            "/quark/影视/番剧", "tv", False, True, phase="completed",
        )
        job.directory.mkdir()
        client, spec, state_root = write_hybrid_transaction_fixture(job)
        server.JOBS[job.id] = job

        with self.assertRaisesRegex(ValueError, "hybrid"):
            server.delete_job(job)
        with mock.patch.object(
            server, "_execution_alist_client", return_value=client,
        ):
            restored = server._restore_job_transaction_quarantines(
                job, reason="test_cancel",
            )

        self.assertEqual(restored["status"], "restored")
        self.assertIsNotNone(client.exact_file_info(spec.source_path))
        self.assertIsNone(client.exact_file_info(str(spec.target_path)))
        self.assertEqual(
            server.load_json(state_root / spec.batch_id / "batch.json")["state"],
            "aborted",
        )
        self.assertEqual(server._transaction_lifecycle_cleanup_blockers(job), [])
        server.delete_job(job)
        self.assertNotIn(job.id, server.JOBS)

    def test_queued_post_commit_cancel_aborts_hybrid_before_terminal_state(self):
        job = server.Job(
            "b2" * 6, "/quark/影视/待刮削/Hybrid",
            "/quark/影视/番剧", "tv", False, True, phase="replenishing",
        )
        job.directory.mkdir()
        client, spec, state_root = write_hybrid_transaction_fixture(job)
        server.JOBS[job.id] = job

        with mock.patch.object(
            server.SCHEDULER, "cancel_pending", return_value=True,
        ), mock.patch.object(
            server, "_execution_alist_client", return_value=client,
        ):
            server.request_cancel(job)

        self.assertEqual(job.phase, "cancelled")
        self.assertEqual(
            server.load_json(state_root / spec.batch_id / "batch.json")["state"],
            "aborted",
        )
        self.assertEqual(
            server.load_json(
                job.directory / "hybrid-transaction-lifecycle.json",
            )["outcome"],
            "restored",
        )

    def test_internal_child_failure_restores_only_its_own_hybrid_batch(self):
        root = server.Job(
            "a3" * 6, "/quark/影视/待刮削/Root",
            "/quark/影视/番剧", "tv", False, True, phase="replenishing",
        )
        child = server.Job(
            "a4" * 6, "/quark/影视/ScrapeFlow/补源/Child",
            "/quark/影视/番剧", "tv", False, True,
            phase="executing_media", visibility="internal", root_job_id=root.id,
        )
        root.directory.mkdir()
        child.directory.mkdir()
        _root_client, root_spec, root_state = write_hybrid_transaction_fixture(root)
        child_client, child_spec, child_state = write_hybrid_transaction_fixture(child)
        server.JOBS = {root.id: root, child.id: child}

        with mock.patch.object(
            server, "_execution_alist_client", return_value=child_client,
        ):
            result = server._restore_transaction_failure_scope(
                child, reason="child_failed",
            )

        self.assertEqual(result["job_count"], 1)
        self.assertEqual(result["jobs"][0]["job_id"], child.id)
        self.assertEqual(
            server.load_json(child_state / child_spec.batch_id / "batch.json")["state"],
            "aborted",
        )
        self.assertEqual(
            server.load_json(root_state / root_spec.batch_id / "batch.json")["state"],
            "sealed",
        )
        self.assertFalse(
            (root.directory / "hybrid-transaction-lifecycle.json").exists(),
        )

    def test_startup_commits_child_batch_only_after_root_final_acceptance(self):
        root = server.Job(
            "a5" * 6, "/quark/影视/待刮削/Root",
            "/quark/影视/番剧", "tv", False, True, phase="completed",
        )
        child = server.Job(
            "a6" * 6, "/quark/影视/ScrapeFlow/补源/Child",
            "/quark/影视/番剧", "tv", False, True,
            phase="completed", visibility="internal", root_job_id=root.id,
        )
        root.directory.mkdir()
        child.directory.mkdir()
        write_accepted_scrape_evidence(root)
        client, spec, state_root = write_hybrid_transaction_fixture(child)
        server.JOBS = {root.id: root, child.id: child}

        with mock.patch.object(
            server, "_execution_alist_client", return_value=client,
        ):
            server.reconcile_transaction_lifecycles_on_startup()

        self.assertEqual(root.phase, "completed")
        self.assertEqual(
            server.load_json(state_root / spec.batch_id / "batch.json")["state"],
            "committed",
        )
        self.assertEqual(
            server.load_json(
                child.directory / "hybrid-transaction-lifecycle.json",
            )["outcome"],
            "accepted",
        )

    def test_runtime_completion_commits_lineage_before_completed_transition(self):
        root = server.Job(
            "b5" * 6, "/quark/影视/待刮削/Root",
            "/quark/影视/番剧", "tv", False, True, phase="replenishing",
        )
        child = server.Job(
            "b6" * 6, "/quark/影视/ScrapeFlow/补源/Child",
            "/quark/影视/番剧", "tv", False, True,
            phase="completed", visibility="internal", root_job_id=root.id,
        )
        root.directory.mkdir()
        child.directory.mkdir()
        write_accepted_scrape_evidence(root)
        closure = server.load_json(root.directory / "title-closure.json")
        client, spec, state_root = write_hybrid_transaction_fixture(child)
        server.JOBS = {root.id: root, child.id: child}
        observed_batch_states: list[str] = []

        def remember_after_commit(current):
            observed_batch_states.append(
                server.load_json(
                    state_root / spec.batch_id / "batch.json",
                )["state"],
            )
            self.assertEqual(current.phase, "completed")

        with mock.patch.object(
            server, "audit_current_job_titles", return_value=closure,
        ), mock.patch.object(
            server, "prepare_post_scrape_replenishment",
            return_value=({"status": "no_regular_gaps", "gap_count": 0}, [], {}),
        ), mock.patch.object(
            server, "_execution_alist_client", return_value=client,
        ), mock.patch.object(
            server, "remember_completed_job", side_effect=remember_after_commit,
        ):
            server.finalize_media_replenishment(root)

        self.assertEqual(root.phase, "completed")
        self.assertEqual(observed_batch_states, ["committed"])
        self.assertEqual(
            root.plan_summary["transaction_lifecycle"]["status"], "committed",
        )

    def test_startup_demotes_invalid_completed_job_and_aborts_batch(self):
        job = server.Job(
            "a7" * 6, "/quark/影视/待刮削/Invalid",
            "/quark/影视/番剧", "tv", False, True, phase="completed",
        )
        job.directory.mkdir()
        client, spec, state_root = write_hybrid_transaction_fixture(job)
        server.JOBS = {job.id: job}

        with mock.patch.object(
            server, "_execution_alist_client", return_value=client,
        ):
            server.reconcile_transaction_lifecycles_on_startup()

        self.assertEqual(job.phase, "recovery_required")
        self.assertIn("缺少严格作品闭环", str(job.error))
        self.assertEqual(
            server.load_json(state_root / spec.batch_id / "batch.json")["state"],
            "aborted",
        )
        self.assertEqual(
            server.load_json(
                job.directory / "hybrid-transaction-lifecycle.json",
            )["outcome"],
            "restored",
        )

    def test_hybrid_receipt_outer_operation_tampering_fails_closed(self):
        job = server.Job(
            "a8" * 6, "/quark/影视/待刮削/Tampered",
            "/quark/影视/番剧", "tv", False, True, phase="completed",
        )
        job.directory.mkdir()
        write_hybrid_transaction_fixture(job)
        journal_path = job.directory / "media-journal.json"
        journal = server.load_json(journal_path)
        receipt = json.loads(journal["records"][0]["message"])
        receipt["items"][0]["operation"] = "delete"
        journal["records"][0]["message"] = json.dumps(
            receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        server._atomic_json(journal_path, journal)

        with self.assertRaisesRegex(ValueError, "项目绑定"):
            server._hybrid_specs_from_media_journal(job)

    def test_completed_directory_marker_is_removed_with_deleted_task_record(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            server.JOBS.clear()
            source = "/quark/影视/番剧/原始目录"
            target = "/quark/影视/番剧/作品名 (2026)"
            completed = server.Job(
                "a" * 12, source, "/quark/影视/番剧", "auto", False, True,
                phase="completed", plan_summary={"target_root": target},
            )
            completed.directory.mkdir()
            server.JOBS[completed.id] = completed

            server.remember_completed_job(completed)
            self.assertEqual(server.directory_task_phase(source), "completed")

            server.delete_job(completed)

            self.assertFalse(completed.directory.exists())
            self.assertIsNone(server.directory_task_phase(source))
            self.assertIsNone(server.directory_task_phase(target))
            index = json.loads(server.processed_index_path().read_text(encoding="utf-8"))
            self.assertNotIn(source, index["paths"])
            self.assertNotIn(target, index["paths"])

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_clear_local_task_data_removes_jobs_and_processed_index_together(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            server.JOBS.clear()
            completed = server.Job(
                "b" * 12, "/quark/影视/待刮削/show", "/quark/影视/番剧",
                "auto", False, True, phase="completed",
            )
            completed.directory.mkdir()
            completed.state_path.write_text("{}", encoding="utf-8")
            server.JOBS[completed.id] = completed
            server.remember_completed_job(completed)

            self.assertEqual(server.clear_local_task_data(), 1)
            self.assertEqual(server.JOBS, {})
            self.assertFalse(completed.directory.exists())
            self.assertFalse(server.processed_index_path().exists())

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_clear_local_task_data_refuses_active_jobs(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            server.JOBS.clear()
            active = server.Job(
                "c" * 12, "/quark/影视/待刮削/show", "/quark/影视/番剧",
                "auto", False, True, phase="planning_media",
            )
            active.directory.mkdir()
            server.JOBS[active.id] = active
            with self.assertRaisesRegex(ValueError, "运行中"):
                server.clear_local_task_data()
            self.assertTrue(active.directory.exists())

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_clear_local_task_data_preflights_all_remote_transactions_before_delete(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            server.JOBS.clear()
            safe = server.Job(
                "7" * 12, "/media/safe", "/media", "auto",
                False, True, phase="completed",
            )
            blocked = server.Job(
                "8" * 12, "/media/blocked", "/media", "auto",
                False, True, phase="cancelled",
            )
            safe.directory.mkdir()
            blocked.directory.mkdir()
            write_remote_transaction_fixture(safe, state="complete")
            transaction = write_remote_transaction_fixture(
                blocked, state="upload_uncertain", payload=b"unique-local-payload",
            )
            server.JOBS = {safe.id: safe, blocked.id: blocked}
            server._atomic_json(
                server.processed_index_path(), {"version": 1, "paths": {}},
            )

            with self.assertRaisesRegex(ValueError, "upload_uncertain"):
                server.clear_local_task_data()

            self.assertTrue(safe.directory.exists())
            self.assertTrue(blocked.directory.exists())
            self.assertTrue(server.processed_index_path().exists())
            self.assertEqual(
                (transaction / "payload.bin").read_bytes(),
                b"unique-local-payload",
            )
            self.assertEqual(set(server.JOBS), {safe.id, blocked.id})
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_retry_job_reuses_failed_task_record_but_rejects_recovery_state(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            failed = server.Job(
                "4" * 12,
                "/quark/影视/show",
                "/quark/影视",
                "tv",
                True,
                True,
                query="Example",
                season=2,
                episode_map={"1": "S02E01"},
                phase="failed",
            )
            failed.directory.mkdir()
            server.JOBS[failed.id] = failed
            failed.log_path.write_text("旧错误\n", encoding="utf-8")
            failed.logs = ["旧错误"]
            with mock.patch.object(server, "start_thread") as start_mock, mock.patch.object(
                server, "browse_remote", side_effect=ValueError("offline")
            ):
                retried = server.retry_job(failed)
            self.assertEqual(retried.id, failed.id)
            self.assertEqual(len(server.JOBS), 1)
            self.assertEqual(retried.source, failed.source)
            self.assertEqual(retried.parent, "/quark/影视")
            self.assertEqual(retried.season, 2)
            self.assertEqual(retried.episode_map, {"1": "S02E01"})
            self.assertEqual(retried.phase, "queued")
            self.assertEqual(retried.logs, ["重新整理已启动，沿用原任务记录。"])
            start_mock.assert_called_once_with(server.prepare_job, retried)
            with self.assertRaisesRegex(ValueError, "可以重新整理"):
                server.retry_job(retried)

            blocked = server.Job("5" * 12, "/media/blocked", "/media", "auto", False, True, phase="failed")
            blocked.directory.mkdir()
            (blocked.directory / "media-journal.json").write_text(
                '{"success":false,"records":[{"action":"move","status":"ok"}]}',
                encoding="utf-8",
            )
            server.JOBS[blocked.id] = blocked
            with self.assertRaisesRegex(ValueError, "恢复"):
                with mock.patch.object(server, "browse_remote", side_effect=ValueError("offline")):
                    server.retry_job(blocked)

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_retry_job_can_repair_recognition_settings_without_recreating_task(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            failed = server.Job(
                "e" * 12,
                "/quark/影视/待刮削/错配作品",
                "/quark/影视/番剧",
                "auto",
                False,
                True,
                query="错误标题",
                phase="failed",
            )
            failed.directory.mkdir()
            server.JOBS[failed.id] = failed
            with mock.patch.object(server, "start_thread") as start_mock, mock.patch.object(
                server, "browse_remote", side_effect=ValueError("offline")
            ):
                retried = server.retry_job(failed, {
                    "tmdb_id": 1234,
                    "media_type": "tv",
                    "query": None,
                    "season": 2,
                    "archive_password": "local-secret",
                })
            self.assertEqual(retried.id, failed.id)
            self.assertEqual(retried.tmdb_id, 1234)
            self.assertIsNone(retried.query)
            self.assertEqual(retried.season, 2)
            self.assertEqual(retried.media_type, "tv")
            password_path = retried.directory / ".archive-password"
            self.assertEqual(password_path.read_text(encoding="utf-8"), "local-secret")
            self.assertEqual(password_path.stat().st_mode & 0o777, 0o600)
            self.assertNotIn("local-secret", json.dumps(retried.public()))
            start_mock.assert_called_once_with(server.prepare_job, retried)

            with self.assertRaisesRegex(ValueError, "只能填写一个"):
                server.validated_retry_settings(retried, {"tmdb_id": 12, "query": "另一个标题"})
            with self.assertRaisesRegex(ValueError, "不支持"):
                server.validated_retry_settings(retried, {"parent": "/tmp"})

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_retry_tmdb_type_is_independent_from_original_target_folder(self):
        previous = server.Job(
            "d" * 12,
            "/quark/影视/待刮削/番剧剧场版",
            "/quark/影视/番剧/Fate",
            "auto",
            False,
            True,
            phase="failed",
        )
        with self.assertRaisesRegex(ValueError, "目标文件夹不决定媒体类型"):
            server.validated_retry_settings(previous, {"tmdb_id": 321})
        settings = server.validated_retry_settings(previous, {
            "tmdb_id": 321,
            "media_type": "movie",
        })
        self.assertEqual(settings["media_type"], "movie")
        self.assertEqual(previous.parent, "/quark/影视/番剧/Fate")

    def test_retry_job_preserves_lock_only_journal_without_recovery(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            job = server.Job(
                "a" * 12,
                "/quark/影视/待刮削/Show",
                "/quark/影视/番剧",
                "auto",
                False,
                True,
                phase="recovery_required",
            )
            job.directory.mkdir()
            server.JOBS[job.id] = job
            (job.directory / "media-journal.json").write_text(
                json.dumps({
                    "success": False,
                    "records": [
                        {"action": "acquire-lock", "status": "pending"},
                        {"action": "mkdir", "status": "ok"},
                        {"action": "rollback-rmdir", "status": "ok"},
                        {"action": "abort", "status": "failed"},
                        {"action": "release-lock", "status": "ok"},
                    ],
                }),
                encoding="utf-8",
            )
            with mock.patch.object(server, "start_thread") as start_mock, mock.patch.object(
                server, "browse_remote", side_effect=ValueError("offline")
            ):
                retried = server.retry_job(job)
            self.assertEqual(retried.phase, "queued")
            self.assertFalse((job.directory / "media-journal.json").exists())
            self.assertTrue((job.directory / "media-journal-failed.json").exists())
            self.assertIn("未移动媒体", "\n".join(retried.logs))
            start_mock.assert_called_once_with(server.prepare_job, retried)

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_retry_job_resumes_cancelled_planning_without_execution_journal(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            cancelled = server.Job(
                "d" * 12,
                "/quark/影视/待刮削/Fate",
                "/quark/影视/番剧",
                "auto",
                False,
                True,
                phase="cancelled",
            )
            cancelled.directory.mkdir()
            server.JOBS[cancelled.id] = cancelled
            with mock.patch.object(server, "start_thread") as start_mock, mock.patch.object(
                server, "browse_remote", side_effect=ValueError("offline")
            ):
                retried = server.retry_job(cancelled)
            self.assertEqual(retried.phase, "queued")
            start_mock.assert_called_once_with(server.prepare_job, retried)

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_retry_job_follows_unique_quark_numeric_rename(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            failed = server.Job(
                "6" * 12,
                "/quark/影视/番剧/E 4k 恶魔高校",
                "/quark/影视/番剧",
                "auto",
                False,
                True,
                phase="failed",
            )
            failed.directory.mkdir()
            server.JOBS[failed.id] = failed
            browse_result = {
                "directories": [
                    {
                        "name": "E 4k 恶魔高校(1)",
                        "path": "/quark/影视/番剧/E 4k 恶魔高校(1)",
                    }
                ]
            }
            with mock.patch.object(server, "browse_remote", return_value=browse_result), mock.patch.object(
                server, "start_thread"
            ):
                server.retry_job(failed)
            self.assertEqual(failed.source, "/quark/影视/番剧/E 4k 恶魔高校(1)")
            self.assertEqual(failed.parent, "/quark/影视/番剧")
            self.assertIn("目录已改名", "\n".join(failed.logs))
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_waiting_media_plan_can_be_replanned_without_remote_write(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            job = server.Job(
                "7" * 12, "/quark/影视/番剧/show", "/quark/影视/番剧",
                "auto", False, True, phase="awaiting_media_approval",
                digest="a" * 64, plan_summary={"kind": "media"},
            )
            job.directory.mkdir()
            (job.directory / "media-plan.json").write_text("{}\n", encoding="utf-8")
            server.JOBS[job.id] = job
            with mock.patch.object(server, "browse_remote", side_effect=ValueError("offline")), mock.patch.object(
                server, "start_thread"
            ) as start_mock:
                server.retry_job(job)
            self.assertEqual(job.phase, "queued")
            self.assertIsNone(job.digest)
            self.assertFalse((job.directory / "media-plan.json").exists())
            start_mock.assert_called_once_with(server.prepare_job, job)
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_retry_after_successful_archive_execution_skips_duplicate_extraction(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            failed = server.Job(
                "8" * 12,
                "/quark/影视/番剧/show",
                "/quark/影视/番剧",
                "auto",
                False,
                True,
                phase="failed",
            )
            failed.directory.mkdir()
            (failed.directory / "archive-plan.json").write_text("{}", encoding="utf-8")
            (failed.directory / "archive-journal.json").write_text(
                json.dumps({"status": "success"}), encoding="utf-8"
            )
            server.JOBS[failed.id] = failed
            with mock.patch.object(server, "browse_remote", side_effect=ValueError("offline")), mock.patch.object(
                server, "start_thread"
            ) as start_mock:
                server.retry_job(failed)
            self.assertTrue((failed.directory / "archive-plan.json").exists())
            self.assertIn("不会重复解压", "\n".join(failed.logs))
            start_mock.assert_called_once_with(server.plan_media, failed)
        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_retry_after_safe_archive_failure_resumes_verified_checkpoint(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.JOBS.clear()
            failed = server.Job(
                "9" * 12,
                "/quark/影视/番剧/show",
                "/quark/影视/番剧",
                "auto",
                False,
                True,
                phase="failed",
            )
            failed.directory.mkdir()
            plan = {"source_root": "/quark/影视/番剧/show", "archives": []}
            (failed.directory / "archive-plan.json").write_text(
                json.dumps(plan), encoding="utf-8"
            )
            (failed.directory / "archive-journal.json").write_text(
                json.dumps({
                    "status": "failed",
                    "error": "format_type is null",
                    "plan_sha256": server.canonical_digest(plan),
                    "retained_archives": ["/quark/影视/番剧/show/one.exe"],
                }),
                encoding="utf-8",
            )
            server.JOBS[failed.id] = failed
            with mock.patch.object(server, "browse_remote", side_effect=ValueError("offline")), mock.patch.object(
                server, "start_execution"
            ) as start_mock:
                server.retry_job(failed)
            self.assertTrue((failed.directory / "archive-plan.json").exists())
            self.assertTrue((failed.directory / "archive-journal.json").exists())
            self.assertFalse((failed.directory / "archive-journal-failed.json").exists())
            self.assertEqual(failed.phase, "starting_archive_execution")
            self.assertIn("断点", "\n".join(failed.logs))
            start_mock.assert_called_once_with(
                server.execute_archive, failed, server.canonical_digest(plan)
            )

        server.JOBS_ROOT = previous_root
        server.JOBS.clear()

    def test_no_archives_continues_to_media_planning_without_error_log(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job("e" * 12, "/media/show", "/media", "auto", False, True)
            job.directory.mkdir()
            with mock.patch.object(
                server,
                "run_command",
                return_value=(server.NO_ARCHIVES_EXIT_CODE, "未发现需要解压的分卷，继续识别媒体。\n"),
            ), mock.patch.object(server, "plan_media") as plan_media_mock:
                server.prepare_job(job)
            plan_media_mock.assert_called_once_with(job)
            self.assertNotIn("错误", "".join(job.logs))
        server.JOBS_ROOT = previous_root

    def test_archive_password_file_is_passed_without_exposing_secret_in_command(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job("f" * 12, "/media/show", "/media", "auto", False, True)
            job.directory.mkdir()
            password_path = job.directory / ".archive-password"
            password_path.write_text("secret-value", encoding="utf-8")
            commands = []

            def capture(_job, command):
                commands.append(command)
                return server.NO_ARCHIVES_EXIT_CODE, "no archives"

            with mock.patch.object(server, "run_command", side_effect=capture), mock.patch.object(server, "plan_media"):
                server.prepare_job(job)
            self.assertIn("--archive-password-file", commands[0])
            self.assertIn(str(password_path), commands[0])
            self.assertNotIn("secret-value", commands[0])
        server.JOBS_ROOT = previous_root

    def test_safe_archive_plan_auto_extracts_before_media_confirmation(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job("7" * 12, "/media/show", "/media", "auto", False, True)
            job.directory.mkdir()
            archive_plan = {
                "source_root": "/media/show",
                "archives": [
                    {
                        "archive_path": "/media/show/subtitles.zip",
                        "dst_dir": "/media/show",
                        "parts": [{"name": "subtitles.zip"}],
                        "members": [{"path": "Show.S01E01.ass", "is_dir": False}],
                        "video_count": 0,
                        "name": "subtitles.zip",
                        "password_source": "none",
                    }
                ],
            }
            (job.directory / "archive-plan.json").write_text(
                json.dumps(archive_plan), encoding="utf-8"
            )
            with mock.patch.object(server, "run_command", return_value=(0, "ok\n")), mock.patch.object(
                server, "start_execution"
            ) as execute_mock:
                server.prepare_job(job)
            self.assertEqual(job.phase, "starting_archive_execution")
            execute_mock.assert_called_once_with(
                server.execute_archive, job, server.canonical_digest(archive_plan)
            )
            self.assertIn("自动解压", "\n".join(job.logs))
        server.JOBS_ROOT = previous_root

    def test_scheduler_runs_two_disjoint_executions_and_serializes_conflicts(self):
        scheduler = server.FifoScheduler(analysis_workers=4, execution_workers=2)
        jobs = [types.SimpleNamespace(id=str(index), queue_position=None, queue_kind=None) for index in range(5)]
        gate = threading.Event()
        first_wave = threading.Event()
        analysis_done = threading.Event()
        state_lock = threading.Lock()
        analysis_started = []
        analysis_current = 0
        analysis_max = 0
        analysis_finished = 0

        def analyze(job):
            nonlocal analysis_current, analysis_max, analysis_finished
            with state_lock:
                analysis_started.append(job.id)
                analysis_current += 1
                analysis_max = max(analysis_max, analysis_current)
                if len(analysis_started) == 4:
                    first_wave.set()
            gate.wait(2)
            with state_lock:
                analysis_current -= 1
                analysis_finished += 1
                if analysis_finished == 5:
                    analysis_done.set()

        for job in jobs:
            scheduler.submit("analysis", job, analyze)
        self.assertEqual([job.queue_position for job in jobs], [1, 2, 3, 4, 5])
        scheduler.start(lambda target, job, *args: target(job, *args))
        self.assertTrue(first_wave.wait(1))
        self.assertEqual(set(analysis_started), {"0", "1", "2", "3"})
        self.assertLessEqual(analysis_max, 4)
        gate.set()
        self.assertTrue(analysis_done.wait(2))
        scheduler.stop()
        scheduler = server.FifoScheduler(analysis_workers=4, execution_workers=2)

        execution_order = []
        execution_current = 0
        execution_max = 0
        execution_finished = 0
        execution_done = threading.Event()

        def execute(job):
            nonlocal execution_current, execution_max, execution_finished
            with state_lock:
                execution_current += 1
                execution_max = max(execution_max, execution_current)
                execution_order.append(job.id)
            time.sleep(0.02)
            with state_lock:
                execution_current -= 1
                execution_finished += 1
                if execution_finished == 3:
                    execution_done.set()

        for job in jobs[:3]:
            scheduler.submit("execution", job, execute)
        self.assertEqual([job.queue_position for job in jobs[:3]], [1, 2, 3])
        scheduler.start(lambda target, job, *args: target(job, *args))
        self.assertTrue(execution_done.wait(2))
        self.assertEqual(execution_order, ["0", "1", "2"])
        self.assertEqual(execution_max, 2)

        conflict_order = []
        conflict_current = 0
        conflict_max = 0
        conflict_done = threading.Event()

        def execute_conflict(job):
            nonlocal conflict_current, conflict_max
            with state_lock:
                conflict_current += 1
                conflict_max = max(conflict_max, conflict_current)
                conflict_order.append(job.id)
            time.sleep(0.02)
            with state_lock:
                conflict_current -= 1
                if len(conflict_order) == 3:
                    conflict_done.set()

        for job in jobs[:3]:
            scheduler.submit(
                "execution", job, execute_conflict,
                resources=["/quark/影视/番剧/Same Show"],
            )
        self.assertTrue(conflict_done.wait(2))
        scheduler.stop()
        self.assertEqual(conflict_order, ["0", "1", "2"])
        self.assertEqual(conflict_max, 1)

    def test_scheduler_analysis_worker_bounds_and_default(self):
        self.assertEqual(server.FifoScheduler().analysis_workers, 4)
        self.assertEqual(server.FifoScheduler().execution_workers, 1)
        self.assertEqual(server.FifoScheduler(analysis_workers=1).analysis_workers, 1)
        self.assertEqual(server.FifoScheduler(analysis_workers=8).analysis_workers, 8)
        for invalid in (0, 9, True, 2.5, "4"):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "1–8"):
                    server.FifoScheduler(analysis_workers=invalid)
        for invalid in (0, 5, True, 2.5, "2"):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "1–4"):
                    server.FifoScheduler(execution_workers=invalid)
        with self.assertRaisesRegex(ValueError, "pause_reader"):
            server.FifoScheduler(pause_reader=True)

    def test_scheduler_path_lease_blocks_overlap_across_analysis_and_execution(self):
        scheduler = server.FifoScheduler(analysis_workers=1, execution_workers=2)
        analysis_job = types.SimpleNamespace(
            id="analysis", queue_position=None, queue_kind=None,
        )
        overlap_job = types.SimpleNamespace(
            id="overlap", queue_position=None, queue_kind=None,
        )
        disjoint_job = types.SimpleNamespace(
            id="disjoint", queue_position=None, queue_kind=None,
        )
        analysis_started = threading.Event()
        release_analysis = threading.Event()
        overlap_started = threading.Event()
        disjoint_started = threading.Event()
        all_done = threading.Event()
        completed: list[str] = []
        completed_lock = threading.Lock()

        def finish(job_id):
            with completed_lock:
                completed.append(job_id)
                if len(completed) == 3:
                    all_done.set()

        def analysis(_job):
            analysis_started.set()
            release_analysis.wait(2)
            finish("analysis")

        def overlap(_job):
            overlap_started.set()
            finish("overlap")

        def disjoint(_job):
            disjoint_started.set()
            finish("disjoint")

        scheduler.submit(
            "analysis", analysis_job, analysis,
            resources=["/quark/影视/番剧/Same"],
        )
        scheduler.submit(
            "execution", overlap_job, overlap,
            resources=["/quark/影视/番剧/Same/Season 01"],
        )
        scheduler.submit(
            "execution", disjoint_job, disjoint,
            resources=["/quark/影视/电影/Other"],
        )
        scheduler.start(lambda target, job, *args: target(job, *args))
        self.assertTrue(analysis_started.wait(1))
        self.assertTrue(disjoint_started.wait(1))
        self.assertFalse(overlap_started.wait(0.05))
        release_analysis.set()
        self.assertTrue(overlap_started.wait(1))
        self.assertTrue(all_done.wait(2))
        scheduler.stop()

        self.assertIn("disjoint", completed)
        self.assertLess(completed.index("analysis"), completed.index("overlap"))

    def test_scheduler_prioritizes_new_media_analysis_over_background_replenishment(self):
        pause_state = {"paused": True}
        scheduler = server.FifoScheduler(
            analysis_workers=1, execution_workers=1,
            pause_reader=lambda: pause_state["paused"],
        )
        background = types.SimpleNamespace(
            id="background", phase="replenishing",
            queue_position=None, queue_kind=None,
        )
        foreground = types.SimpleNamespace(
            id="foreground", phase="queued",
            queue_position=None, queue_kind=None,
        )
        order = []
        finished = threading.Event()

        def analyze(job):
            order.append(job.id)
            if len(order) == 2:
                finished.set()

        scheduler.submit("analysis", background, analyze)
        scheduler.submit("analysis", foreground, analyze)

        self.assertEqual(scheduler.pending("analysis"), ["foreground", "background"])
        self.assertEqual(foreground.queue_position, 1)
        self.assertEqual(background.queue_position, 2)
        scheduler.start(lambda target, job, *args: target(job, *args))
        pause_state["paused"] = False
        scheduler.wake()
        self.assertTrue(finished.wait(2))
        scheduler.stop()
        self.assertEqual(order, ["foreground", "background"])

    def test_scheduler_can_remove_pending_job_and_reindex_queue(self):
        scheduler = server.FifoScheduler(analysis_workers=1, execution_workers=1)
        jobs = [types.SimpleNamespace(id=str(index), queue_position=None, queue_kind=None) for index in range(3)]
        for job in jobs:
            scheduler.submit("analysis", job, lambda _job: None)
        self.assertTrue(scheduler.cancel_pending("1"))
        self.assertIsNone(jobs[1].queue_position)
        self.assertEqual([jobs[0].queue_position, jobs[2].queue_position], [1, 2])
        self.assertFalse(scheduler.cancel_pending("missing"))

    def test_safe_stop_removes_queued_job_without_starting_work(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job("a" * 12, "/quark/影视/待刮削/Show", "/quark/影视/番剧", "auto", False, True)
            job.directory.mkdir()
            with mock.patch.object(server.SCHEDULER, "cancel_pending", return_value=True):
                server.request_cancel(job)
            self.assertEqual(job.phase, "cancelled")
            self.assertTrue(job.cancel_requested)
            self.assertIn("等待队列", "\n".join(job.logs))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_target_conflict_can_be_closed_without_touching_media(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "b" * 12, "/quark/影视/待刮削/Show", "/quark/影视/番剧",
                "auto", False, True, phase="failed",
                error="目标目录已存在同名文件: /quark/影视/番剧/Show/Season 01/Show - S01E01.mkv",
            )
            job.directory.mkdir()
            server.resolve_failed_job(job, {"action": "keep_existing", "confirm": True})
            self.assertEqual(job.phase, "cancelled")
            self.assertIsNone(job.error)
            self.assertIn("未改动目标库或新资源目录", "\n".join(job.logs))
            self.assertFalse((job.directory / "media-journal.json").exists())
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_only_confirmed_target_conflict_can_be_closed(self):
        job = server.Job(
            "c" * 12, "/quark/影视/待刮削/Show", "/quark/影视/番剧",
            "auto", False, True, phase="failed", error="TMDB 请求超时",
        )
        with self.assertRaisesRegex(ValueError, "明确确认"):
            server.resolve_failed_job(job, {"action": "keep_existing"})
        with self.assertRaisesRegex(ValueError, "不是.*目标冲突"):
            server.resolve_failed_job(job, {"action": "keep_existing", "confirm": True})
        self.assertEqual(job.phase, "failed")

    def test_analysis_worker_count_environment_is_bounded(self):
        with mock.patch.dict(os.environ, {"SCRAPEFLOW_ANALYSIS_WORKERS": "6"}):
            self.assertEqual(server.analysis_worker_count(), 6)
        for invalid in ("0", "9", "2.5", "four", "４"):
            with self.subTest(invalid=invalid), mock.patch.dict(
                os.environ, {"SCRAPEFLOW_ANALYSIS_WORKERS": invalid}
            ):
                with self.assertRaisesRegex(ValueError, "1–8"):
                    server.analysis_worker_count()
        with mock.patch.dict(os.environ):
            os.environ.pop("SCRAPEFLOW_ANALYSIS_WORKERS", None)
            self.assertEqual(server.analysis_worker_count(), 4)

    def test_execution_worker_count_environment_is_bounded(self):
        with mock.patch.dict(os.environ, {"SCRAPEFLOW_EXECUTION_WORKERS": "3"}):
            self.assertEqual(server.execution_worker_count(), 3)
        for invalid in ("0", "5", "2.5", "two", "２"):
            with self.subTest(invalid=invalid), mock.patch.dict(
                os.environ, {"SCRAPEFLOW_EXECUTION_WORKERS": invalid}
            ):
                with self.assertRaisesRegex(ValueError, "1–4"):
                    server.execution_worker_count()
        with mock.patch.dict(os.environ):
            os.environ.pop("SCRAPEFLOW_EXECUTION_WORKERS", None)
            self.assertEqual(server.execution_worker_count(), 1)

    def test_media_approval_refreshes_alist_and_rejects_target_conflict(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            source = "/quark/影视/番剧/Source"
            target_root = "/quark/影视/番剧/Show (2026)"
            target_dir = f"{target_root}/Season 01"
            plan = {
                "source_root": source,
                "target_root": target_root,
                "files": [{
                    "source_path": f"{source}/video.mkv",
                    "target_dir": target_dir,
                    "final_name": "Show - S01E01.mkv",
                }],
                "metadata": {"series_root": target_root},
            }
            digest = server.canonical_digest(plan)
            job = server.Job(
                "f" * 12, source, "/quark/影视/番剧", "auto", False, True,
                phase="starting_media_execution", digest=digest,
            )
            job.directory.mkdir()
            (job.directory / "media-plan.json").write_text(
                json.dumps({"plan": plan, "plan_sha256": digest}), encoding="utf-8"
            )

            class FakeClient:
                def __init__(self, conflict=False):
                    self.conflict = conflict
                    self.calls = []

                def list(self, path, refresh=False):
                    self.calls.append((path, refresh))
                    rows = {
                        "/quark/影视": [{"name": "番剧", "is_dir": True}],
                        "/quark/影视/番剧": [{"name": "Show (2026)", "is_dir": True}],
                        source: [{"name": "video.mkv", "is_dir": False}],
                        target_root: [{"name": "Season 01", "is_dir": True}],
                        target_dir: ([{"name": "Show - S01E01.mkv", "is_dir": False}] if self.conflict else []),
                    }
                    return rows.get(path, [])

            clean_client = FakeClient()
            with mock.patch.object(server, "_execution_alist_client", return_value=clean_client):
                server.validate_approved_execution(job, digest)
            self.assertTrue(clean_client.calls)
            self.assertTrue(all(refresh for _, refresh in clean_client.calls))

            conflict_client = FakeClient(conflict=True)
            with mock.patch.object(server, "_execution_alist_client", return_value=conflict_client):
                with self.assertRaisesRegex(ValueError, "目标文件已存在"):
                    server.validate_approved_execution(job, digest)
        server.JOBS_ROOT = previous_root

    def test_recovery_precheck_uses_journal_paths_when_original_source_is_gone(self):
        source = "/quark/影视/番剧/Show/E01.mkv"
        target = "/quark/影视/待刮削/Old/E01.mkv"
        job = server.Job(
            "a" * 12, "/quark/影视/待刮削/Old", "/quark/影视/番剧",
            "auto", False, True, phase="starting_recovery_execution",
            digest="d" * 64,
            plan_summary={"files": [{"source": source, "target": target}]},
        )

        class FakeClient:
            def list(self, path, refresh=False):
                rows = {
                    "/quark/影视/番剧/Show": [{"name": "E01.mkv", "is_dir": False}],
                    "/quark/影视": [
                        {"name": "番剧", "is_dir": True},
                        {"name": "待刮削", "is_dir": True},
                    ],
                    "/quark/影视/待刮削": [],
                }
                return rows.get(path, [])

        with mock.patch.object(
            server, "_execution_alist_client", return_value=FakeClient(),
        ):
            server.validate_approved_execution(job, "d" * 64)

    def test_unattended_recovery_precheck_failure_rechecks_without_manual_approval(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job(
                "6" * 12, "/quark/影视/待刮削/Old", "/quark/影视/番剧",
                "auto", False, True, phase="starting_recovery_execution",
                digest="e" * 64,
            )
            job.directory.mkdir()
            with mock.patch.object(
                server, "validate_approved_execution",
                side_effect=ValueError("暂时无法刷新"),
            ), mock.patch.object(
                server, "auto_execute_media_enabled", return_value=True,
            ), mock.patch.object(server, "_schedule_recovery_retry") as retry:
                server.execute_approved_recovery(job, "e" * 64)
            self.assertEqual(job.phase, "recovery_required")
            self.assertEqual(job.error, "暂时无法刷新")
            retry.assert_called_once_with(job)
        server.JOBS_ROOT = previous_root

    def test_recovery_check_failure_remains_recoverable_and_schedules_retry(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "7" * 12, "/quark/影视/待刮削/Old", "/quark/影视/番剧",
                "auto", False, True, phase="recovery_required",
            )
            job.directory.mkdir()
            (job.directory / "media-journal.json").write_text("{}\n", encoding="utf-8")
            with mock.patch.object(
                server, "run_command", return_value=(1, "temporary failure"),
            ), mock.patch.object(server, "_schedule_recovery_retry") as retry:
                server.prepare_recovery(job)
            self.assertEqual(job.phase, "recovery_required")
            self.assertTrue((job.directory / "media-journal.json").exists())
            retry.assert_called_once_with(job)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_recovery_execution_failure_schedules_retry(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "8" * 12, "/quark/影视/待刮削/Old", "/quark/影视/番剧",
                "auto", False, True, phase="starting_recovery_execution",
            )
            job.directory.mkdir()
            (job.directory / "media-journal.json").write_text("{}\n", encoding="utf-8")
            with mock.patch.object(
                server, "run_command", return_value=(1, "temporary failure"),
            ), mock.patch.object(server, "_schedule_recovery_retry") as retry:
                server.execute_recovery(job, "f" * 64)
            self.assertEqual(job.phase, "recovery_required")
            retry.assert_called_once_with(job)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_approval_joins_execution_queue_and_failed_precheck_returns_to_review(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            plan = {
                "mode": "tv", "source_root": "/quark/影视/番剧/Show",
                "target_root": "/quark/影视/番剧/Show",
                "metadata": {
                    "title": "Show", "tmdb_id": 1, "year": "2026",
                    "season": 1, "absolute": False,
                },
                "files": [], "problem_files": [],
            }
            digest = server.canonical_digest(plan)
            job = server.Job(
                "d" * 12, "/quark/影视/番剧/Show", "/quark/影视/番剧",
                "auto", False, True,
                phase="awaiting_media_approval", digest=digest,
            )
            job.directory.mkdir()
            server._atomic_json(job.directory / "media-plan.json", {
                "plan": plan, "plan_sha256": digest,
            })
            with mock.patch.object(server, "start_execution") as start_mock:
                server.approve_job(job, {"digest": digest})
            self.assertEqual(job.phase, "starting_media_execution")
            start_mock.assert_called_once_with(server.execute_approved_media, job, digest)

            with mock.patch.object(
                server, "validate_approved_execution", side_effect=ValueError("目标文件已存在")
            ), mock.patch.object(server, "execute_media") as execute_mock:
                server.execute_approved_media(job, digest)
            self.assertEqual(job.phase, "awaiting_media_approval")
            self.assertEqual(job.error, "目标文件已存在")
            execute_mock.assert_not_called()
        server.JOBS_ROOT = previous_root

    def test_auto_pipeline_precheck_failure_stops_as_failed_instead_of_review(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job(
                "e" * 12, "/quark/影视/待刮削/Show", "/quark/影视/番剧",
                "auto", False, True, phase="starting_media_execution",
                digest="a" * 64,
            )
            job.directory.mkdir()
            with mock.patch.object(
                server, "validate_approved_execution", side_effect=ValueError("目标文件已存在")
            ), mock.patch.object(
                server, "auto_execute_media_enabled", return_value=True
            ), mock.patch.object(server, "execute_media") as execute_mock:
                server.execute_approved_media(job, "a" * 64)
            self.assertEqual(job.phase, "failed")
            self.assertEqual(job.error, "目标文件已存在")
            execute_mock.assert_not_called()
        server.JOBS_ROOT = previous_root

    def test_media_execution_cleans_empty_source_directories_by_default(self):
        previous_root = server.JOBS_ROOT
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            job = server.Job(
                "9" * 12,
                "/media/show",
                "/media",
                "auto",
                False,
                True,
                phase="starting_media_execution",
            )
            job.directory.mkdir()
            with mock.patch.object(
                server, "run_command", return_value=(0, "ok\n")
            ) as run_mock, mock.patch.object(server, "start_thread") as thread_mock:
                server.execute_media(job, "a" * 64)

            command = run_mock.call_args.args[1]
            self.assertIn("--cleanup-empty-source", command)
            self.assertEqual(job.phase, "replenishing")
            thread_mock.assert_called_once_with(server.finalize_media_replenishment, job)
        server.JOBS_ROOT = previous_root

    def test_internal_replenishment_execution_completes_without_recursive_search(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "a" * 12,
                "/quark/影视/ScrapeFlow/补源/Example",
                "/quark/影视/番剧",
                "tv",
                False,
                True,
                phase="executing_media",
                visibility="internal",
                root_job_id="b" * 12,
                episode_map={"E1": "S01E01"},
            )
            job.directory.mkdir()
            (job.directory / "media-journal.json").write_text(
                '{"success":true}\n', encoding="utf-8",
            )
            with mock.patch.object(
                server, "run_command", return_value=(0, "ok\n")
            ), mock.patch.object(server, "start_thread") as thread_mock:
                server.execute_media(job, "a" * 64)

            self.assertEqual(job.phase, "completed")
            self.assertEqual(job.progress["stage"], "replenishment_followup_complete")
            thread_mock.assert_not_called()
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_internal_remuxed_followup_without_exact_map_does_not_recurse(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "b" * 12,
                "/quark/影视/ScrapeFlow/补源/Remuxed-upload",
                "/quark/影视/番剧",
                "tv",
                False,
                True,
                phase="executing_media",
                visibility="internal",
                root_job_id="c" * 12,
                episode_map=None,
            )
            job.directory.mkdir()
            (job.directory / "media-journal.json").write_text(
                '{"success":true}\n', encoding="utf-8",
            )
            with mock.patch.object(
                server, "run_command", return_value=(0, "ok\n")
            ), mock.patch.object(server, "start_thread") as thread_mock:
                server.execute_media(job, "a" * 64)

            self.assertEqual(job.phase, "completed")
            self.assertEqual(job.progress["stage"], "replenishment_followup_complete")
            thread_mock.assert_not_called()
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_restore_reconciles_committed_internal_replenishment_followup(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "c" * 12,
                "/quark/影视/ScrapeFlow/补源/Example",
                "/quark/影视/番剧",
                "tv",
                False,
                True,
                phase="replenishing",
                visibility="internal",
                root_job_id="d" * 12,
                episode_map={"E1": "S01E01"},
            )
            job.directory.mkdir()
            (job.directory / "media-journal.json").write_text(
                '{"success":true}\n', encoding="utf-8",
            )

            changed = server.complete_internal_replenishment_followup(job, restored=True)

            self.assertTrue(changed)
            self.assertEqual(job.phase, "completed")
            self.assertEqual(job.progress["stage"], "replenishment_followup_complete")
            self.assertTrue(server._replenishment_followup_closed(job))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_restore_clears_stale_followup_failure_after_verified_completion(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "e" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="completed",
                plan_summary={"replenishment": {
                    "status": "acquired", "followup_verified": True,
                    "failed_followup_job_ids": ["f" * 12],
                }},
            )
            job.directory.mkdir()

            changed = server.reconcile_completed_replenishment_summary(job)

            self.assertTrue(changed)
            self.assertNotIn("failed_followup_job_ids", job.plan_summary["replenishment"])
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_post_scrape_replenishment_uses_adapter_and_materializes_source(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "8" * 12,
                "/quark/影视/待刮削/Example",
                "/quark/影视/番剧",
                "tv",
                False,
                True,
                phase="executing_media",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv",
                "source_root": job.source,
                "target_root": "/quark/影视/番剧/Example",
                "metadata": {"title": "Example", "year": "2026", "tmdb_id": 42},
                "files": [],
                "scan_report": {"resource_gaps": [{
                    "kind": "missing_episode", "label": "S01E03 Three", "reason": "missing",
                }]},
            }
            digest = server.canonical_digest(plan)
            (job.directory / "media-plan.json").write_text(json.dumps({
                "plan": plan, "plan_sha256": digest,
            }), encoding="utf-8")
            adapter = Path(directory) / "adapter.py"
            adapter.write_text(
                "import json,sys\n"
                "args=sys.argv\n"
                "out=args[args.index('--output')+1]\n"
                "if 'search' in args:\n"
                " json.dump({'candidates':[{'provider':'cloud_share','release_name':'Example S01E01-E12','name_coverage':['S01E03'],'resolution':'1080p','updated_at':'2026-07-27T00:00:00Z','locator':'share:item'}]},open(out,'w'))\n"
                "else:\n"
                " json.dump({'status':'ready','source_path':'/quark/影视/待刮削/Example 补源'},open(out,'w'))\n",
                encoding="utf-8",
            )

            fake_client = types.SimpleNamespace(
                list=lambda path, refresh=False: (
                    [{"name": "Example.S01E03.mkv", "is_dir": False}]
                    if path.endswith("Example 补源") else []
                ),
            )
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_ADAPTER": f"{sys.executable} {adapter}",
            }, clear=False), mock.patch.object(
                server, "_execution_alist_client", return_value=fake_client,
            ), mock.patch.object(
                server, "replenishment_min_cloud_attempts", return_value=0,
            ):
                summary, sources, restored_plan = server.prepare_post_scrape_replenishment(job)

            self.assertEqual(summary["status"], "acquired")
            self.assertEqual(summary["selection"]["resolution"], "1080p")
            self.assertEqual(sources, [{
                "source": "/quark/影视/待刮削/Example 补源",
                "media": summary["projects"][0]["media"],
            }])
            self.assertEqual(restored_plan, plan)
            self.assertEqual(job.progress["stage"], "replenishment_acquire")
            self.assertEqual(job.progress["total"], 1)
            self.assertIn("正在获取 1 个缺项", job.progress["message"])
            self.assertTrue((job.directory / "replenishment-request.json").exists())
            self.assertTrue((job.directory / "replenishment-selection.json").exists())
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_fresh_title_audit_replaces_preexecution_plan_gaps(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "7" * 12,
                "/quark/影视/待刮削/Example",
                "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv",
                "source_root": job.source,
                "target_root": "/quark/影视/番剧/Example",
                "metadata": {
                    "title": "Example", "year": "2026", "tmdb_id": 42,
                },
                "files": [],
                "scan_report": {"resource_gaps": [{
                    "kind": "missing_episode", "label": "S01E03",
                    "reason": "preexecution observation",
                }]},
            }
            server._atomic_json(job.directory / "media-plan.json", {
                "plan": plan, "plan_sha256": server.canonical_digest(plan),
            })

            summary, followups, audited_plan = (
                server.prepare_post_scrape_replenishment(
                    job, current_episode_gaps=[],
                )
            )

            self.assertEqual(summary["status"], "no_regular_gaps")
            self.assertEqual(followups, [])
            self.assertEqual(audited_plan["scan_report"]["resource_gaps"], [])
            # The executable plan is immutable; only the in-memory audit copy
            # receives the fresh post-placement evidence.
            persisted, _ = server.unwrap_media_plan(
                server.load_json(job.directory / "media-plan.json")
            )
            self.assertEqual(
                persisted["scan_report"]["resource_gaps"][0]["label"],
                "S01E03",
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_post_scrape_replenishment_delegates_signed_plan_to_shared_core(self):
        job = server.Job(
            "6" * 12,
            "/quark/影视/待刮削/Example",
            "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        job.directory.mkdir()
        plan = {
            "mode": "tv",
            "source_root": job.source,
            "target_root": "/quark/影视/番剧/Example",
            "metadata": {"title": "Example", "tmdb_id": 42},
            "files": [],
            "scan_report": {"resource_gaps": []},
        }
        server._atomic_json(job.directory / "media-plan.json", {
            "plan": plan, "plan_sha256": server.canonical_digest(plan),
        })
        expected = ({"status": "no_regular_gaps"}, [], plan)

        with mock.patch.object(
            server, "prepare_replenishment_from_plan", return_value=expected,
        ) as core:
            actual = server.prepare_post_scrape_replenishment(
                job, current_episode_gaps=[],
            )

        self.assertEqual(actual, expected)
        core.assert_called_once_with(
            job, plan, current_episode_gaps=[], scrape_gate_sha256=None,
        )

    def test_explicit_plan_replenishment_core_owns_artifacts_without_media_plan(self):
        job = server.Job(
            "5" * 12,
            "/quark/影视/待刮削/Example",
            "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        job.directory.mkdir()
        plan = {
            "mode": "tv",
            "source_root": job.source,
            "target_root": "/quark/影视/番剧/Example",
            "metadata": {
                "title": "Example", "year": "2026", "tmdb_id": 42,
            },
            "files": [],
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S01E03 stale",
            }]},
        }
        original_plan = json.loads(json.dumps(plan))
        fresh_gaps = [{
            "kind": "missing_episode", "label": "S01E04 fresh",
        }]

        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "0",
            "SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS": "0",
        }, clear=False), mock.patch.object(server, "run_command") as run_mock, \
                mock.patch.object(server, "_execution_alist_client") as alist_mock:
            summary, followups, audited_plan = (
                server.prepare_replenishment_from_plan(
                    job,
                    plan,
                    current_episode_gaps=fresh_gaps,
                    request_job_id="one-time:title:tmdb-tv-42",
                    round_number=7,
                )
            )

        self.assertEqual(summary["status"], "detected")
        self.assertEqual(summary["round"], 7)
        self.assertEqual(followups, [])
        self.assertEqual(plan, original_plan)
        self.assertEqual(
            audited_plan["scan_report"]["resource_gaps"], fresh_gaps,
        )
        self.assertIsNot(
            audited_plan["scan_report"]["resource_gaps"][0], fresh_gaps[0],
        )
        batch = server.load_json(job.directory / "replenishment-requests.json")
        request = batch["requests"][0]
        self.assertEqual(request["job_id"], "one-time:title:tmdb-tv-42")
        self.assertEqual(request["round"], 7)
        self.assertEqual([gap["id"] for gap in request["gaps"]], ["S01E04"])
        self.assertFalse((job.directory / "media-plan.json").exists())
        run_mock.assert_not_called()
        alist_mock.assert_not_called()

    def test_problem_files_have_no_automatic_remote_route(self):
        plan = {
            "files": [{"source_path": "/source/episode.mkv"}],
            "cleanup_files": [],
            "problem_files": [{
                "source_path": "/source/unmatched.ass",
                "target_path": None,
                "reason": "字幕没有唯一对应视频",
            }],
            "warnings": [],
            "notices": [],
        }

        summary = server.summarize_media_plan(plan)

        self.assertTrue(server.media_plan_requires_review(plan))
        self.assertFalse(summary["review"]["automation_eligible"])
        self.assertNotIn("automatic_handling", summary["problem_files"][0])
        self.assertNotIn("auto_routed_problem_count", summary)
        self.assertFalse(hasattr(server, "route_retained_problem_files"))

    def test_finalize_checks_scrape_first_before_title_audit(self):
        job = server.Job(
            "abc123abc123", "/quark/影视/待刮削/Example",
            "/quark/影视/番剧", "tv", False, True,
            phase="replenishing",
        )
        gate = {
            "ready": False, "status": "blocked",
            "checked_at": "2026-08-05T00:00:00+00:00",
            "blocker_count": 1,
            "blockers": [{"kind": "inbox_directory", "path": job.source}],
            "message": "待刮削仍有 1 项未完成",
        }
        with mock.patch.object(
            server, "scrape_first_gate_evidence", return_value=gate,
        ), mock.patch.object(
            server, "audit_current_job_titles",
        ) as audit_mock, mock.patch.object(
            server, "prepare_post_scrape_replenishment",
        ) as replenish_mock, mock.patch.object(
            server, "_enter_scrape_first_wait",
        ) as wait_mock:
            server.finalize_media_replenishment(job)

        audit_mock.assert_not_called()
        replenish_mock.assert_not_called()
        wait_mock.assert_called_once()

    def test_post_scrape_replenishment_binds_and_reuses_matching_ready_artifact(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "7" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv", "source_root": job.source,
                "target_root": "/quark/影视/番剧/Example",
                "metadata": {"title": "Example", "tmdb_id": 42}, "files": [],
                "scan_report": {"resource_gaps": [{
                    "kind": "missing_episode", "label": "S01E03 Three", "reason": "missing",
                }]},
            }
            (job.directory / "media-plan.json").write_text(json.dumps({
                "plan": plan, "plan_sha256": server.canonical_digest(plan),
            }), encoding="utf-8")
            def run_adapter(_job, command):
                output = Path(command[command.index("--output") + 1])
                if "search" in command:
                    output.write_text(json.dumps({
                        "candidates": [{
                            "provider": "cloud_share",
                            "release_name": "Example S01E01-E12",
                            "name_coverage": ["S01E03"], "resolution": "1080p",
                            "updated_at": "2026-07-27T00:00:00Z",
                            "locator": "fixture:item",
                        }],
                        "share_discovery": {"resource_failed_locators": [
                            "quark_share:unselected-share",
                        ]},
                        "magnet_discovery": {"resource_failed_locators": [
                            "quark_magnet:unselected-magnet",
                        ]},
                    }), encoding="utf-8")
                else:
                    output.write_text(json.dumps({
                        "status": "ready",
                        "source_paths": ["/quark/影视/待刮削/Example 补源"],
                    }), encoding="utf-8")
                return 0, ""

            fake_client = types.SimpleNamespace(list=lambda path, **_kwargs: ([{
                "name": "Example.S01E03.mkv", "is_dir": False,
            }] if path.endswith("Example 补源") else []))
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_ADAPTER": "fixture-adapter",
            }, clear=False), mock.patch.object(
                server, "run_command", side_effect=run_adapter,
            ) as run_mock, mock.patch.object(
                server, "_execution_alist_client", return_value=fake_client,
            ), mock.patch.object(
                server, "replenishment_min_cloud_attempts", return_value=0,
            ):
                summary, sources, _restored_plan = server.prepare_post_scrape_replenishment(job)
                first_receipt = json.loads(
                    (job.directory / "replenishment-acquisition.json").read_text(encoding="utf-8")
                )
                selection = json.loads(
                    (job.directory / "replenishment-selection.json").read_text(encoding="utf-8")
                )
                repeated, repeated_sources, _ = server.prepare_post_scrape_replenishment(job)

            self.assertEqual(summary["status"], "acquired")
            self.assertEqual(sources[0]["source"], "/quark/影视/待刮削/Example 补源")
            self.assertEqual(first_receipt["selection_sha256"], server.canonical_digest(selection))
            self.assertEqual(repeated["status"], "acquired")
            self.assertEqual(
                server._load_replenishment_provider_attempts(
                    job, selection["request"],
                ),
                {"quark_share": 0, "quark_magnet": 0},
            )
            self.assertEqual(repeated_sources, sources)
            self.assertEqual(run_mock.call_count, 3)
            self.assertTrue(any("复用重启前" in line for line in job.logs))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_post_scrape_replenishment_resumes_selection_before_search_after_restart(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "9" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv", "source_root": job.source,
                "target_root": "/quark/影视/番剧/Example",
                "metadata": {"title": "Example", "tmdb_id": 42}, "files": [],
                "scan_report": {"resource_gaps": [{
                    "kind": "missing_episode", "label": "S01E03 Three",
                    "reason": "missing",
                }]},
            }
            candidate = {
                "provider": "cloud_share",
                "release_name": "Example S01E01-E12",
                "name_coverage": ["S01E03"], "resolution": "1080p",
                "updated_at": "2026-07-27T00:00:00Z",
                "locator": "fixture:interrupted",
            }
            calls: list[str] = []

            def interrupted_adapter(_job, command):
                action = "search" if "search" in command else "acquire"
                calls.append(action)
                output = Path(command[command.index("--output") + 1])
                if action == "search":
                    output.write_text(json.dumps({
                        "candidates": [candidate],
                    }), encoding="utf-8")
                    return 0, ""
                raise RuntimeError("simulated API shutdown after selection commit")

            environment = {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_ADAPTER": "fixture-adapter",
            }
            fake_client = types.SimpleNamespace(list=lambda path, **_kwargs: ([{
                "name": "Example.S01E03.mkv", "is_dir": False,
            }] if path.endswith("Example 补源") else []))
            with mock.patch.dict(os.environ, environment, clear=False), mock.patch.object(
                server, "run_command", side_effect=interrupted_adapter,
            ), mock.patch.object(
                server, "_execution_alist_client", return_value=fake_client,
            ), mock.patch.object(
                server, "replenishment_min_cloud_attempts", return_value=0,
            ), self.assertRaisesRegex(RuntimeError, "simulated API shutdown"):
                server.prepare_replenishment_from_plan(job, plan)

            self.assertEqual(calls, ["search", "acquire"])
            self.assertTrue((job.directory / "replenishment-selection.json").exists())
            self.assertFalse((job.directory / "replenishment-acquisition.json").exists())

            def resumed_adapter(_job, command):
                action = "search" if "search" in command else "acquire"
                calls.append(action)
                self.assertEqual(action, "acquire")
                output = Path(command[command.index("--output") + 1])
                output.write_text(json.dumps({
                    "status": "ready",
                    "source_paths": ["/quark/影视/待刮削/Example 补源"],
                }), encoding="utf-8")
                return 0, ""

            with mock.patch.dict(os.environ, environment, clear=False), mock.patch.object(
                server, "run_command", side_effect=resumed_adapter,
            ), mock.patch.object(
                server, "_execution_alist_client", return_value=fake_client,
            ), mock.patch.object(
                server, "replenishment_min_cloud_attempts", return_value=0,
            ):
                summary, sources, _ = server.prepare_replenishment_from_plan(job, plan)

            self.assertEqual(calls, ["search", "acquire", "acquire"])
            self.assertEqual(summary["status"], "acquired")
            self.assertEqual(
                sources[0]["source"], "/quark/影视/待刮削/Example 补源",
            )
            self.assertTrue(any("继续原 acquisition checkpoint" in line for line in job.logs))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_post_scrape_replenishment_rejects_unbound_or_mismatched_ready_artifact(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        try:
            for stale_digest in (None, "0" * 64):
                with self.subTest(stale_digest=stale_digest), tempfile.TemporaryDirectory() as directory:
                    server.JOBS_ROOT = Path(directory) / "jobs"
                    server.JOBS_ROOT.mkdir()
                    server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
                    job = server.Job(
                        "8" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                        "tv", False, True, phase="replenishing",
                    )
                    job.directory.mkdir()
                    plan = {
                        "mode": "tv", "source_root": job.source,
                        "target_root": "/quark/影视/番剧/Example",
                        "metadata": {"title": "Example", "tmdb_id": 42}, "files": [],
                        "scan_report": {"resource_gaps": [{
                            "kind": "missing_episode", "label": "S01E03 Three", "reason": "missing",
                        }]},
                    }
                    (job.directory / "media-plan.json").write_text(json.dumps({
                        "plan": plan, "plan_sha256": server.canonical_digest(plan),
                    }), encoding="utf-8")
                    stale_receipt = {
                        "status": "ready",
                        "source_paths": ["/quark/影视/待刮削/Stale 补源"],
                    }
                    if stale_digest is not None:
                        stale_receipt["selection_sha256"] = stale_digest
                    (job.directory / "replenishment-acquisition.json").write_text(
                        json.dumps(stale_receipt), encoding="utf-8",
                    )

                    def run_adapter(_job, command):
                        output = Path(command[command.index("--output") + 1])
                        if "search" in command:
                            output.write_text(json.dumps({"candidates": [{
                                "provider": "cloud_share",
                                "release_name": "Example S01E01-E12",
                                "name_coverage": ["S01E03"], "resolution": "1080p",
                                "updated_at": "2026-07-27T00:00:00Z",
                                "locator": "fixture:item",
                            }]}), encoding="utf-8")
                        else:
                            output.write_text(json.dumps({
                                "status": "ready",
                                "source_paths": ["/quark/影视/待刮削/Fresh 补源"],
                            }), encoding="utf-8")
                        return 0, ""

                    fake_client = types.SimpleNamespace(list=lambda path, **_kwargs: ([{
                        "name": "Example.S01E03.mkv", "is_dir": False,
                    }] if path.endswith("Fresh 补源") else []))
                    with mock.patch.dict(os.environ, {
                        "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                        "SCRAPEFLOW_REPLENISHMENT_ADAPTER": "fixture-adapter",
                    }, clear=False), mock.patch.object(
                        server, "run_command", side_effect=run_adapter,
                    ) as run_mock, mock.patch.object(
                        server, "_execution_alist_client", return_value=fake_client,
                    ), mock.patch.object(
                        server, "replenishment_min_cloud_attempts", return_value=0,
                    ):
                        summary, sources, _ = server.prepare_post_scrape_replenishment(job)

                    selection = json.loads(
                        (job.directory / "replenishment-selection.json").read_text(encoding="utf-8")
                    )
                    receipt = json.loads(
                        (job.directory / "replenishment-acquisition.json").read_text(encoding="utf-8")
                    )
                    self.assertEqual(summary["status"], "acquired")
                    self.assertEqual(sources[0]["source"], "/quark/影视/待刮削/Fresh 补源")
                    self.assertEqual(run_mock.call_count, 2)
                    self.assertEqual(receipt["selection_sha256"], server.canonical_digest(selection))
                    self.assertFalse(any("复用重启前" in line for line in job.logs))
        finally:
            server.JOBS_ROOT = previous_root
            server.Job.root_provider = staticmethod(previous_provider)

    def test_post_scrape_legacy_infrastructure_exit_does_not_poison_candidate(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory) / "jobs"
            server.JOBS_ROOT.mkdir()
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "6" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv", "metadata": {"title": "Example", "tmdb_id": 42},
                "files": [], "scan_report": {"resource_gaps": [{
                    "kind": "missing_episode", "label": "S01E03 Three", "reason": "missing",
                }]},
            }
            (job.directory / "media-plan.json").write_text(json.dumps({
                "plan": plan, "plan_sha256": server.canonical_digest(plan),
            }), encoding="utf-8")

            def run_adapter(_job, command):
                if "search" in command:
                    output = Path(command[command.index("--output") + 1])
                    output.write_text(json.dumps({"candidates": [{
                        "provider": "cloud_share", "release_name": "Example S01E01-E12",
                        "name_coverage": ["S01E03"], "resolution": "1080p",
                        "updated_at": "2026-07-27T00:00:00Z", "locator": "fixture:item",
                    }]}), encoding="utf-8")
                    return 0, ""
                return 1, "\n".join([
                    "[replenishment] 下载候选 1/1: Example S01E01-E12",
                    "[replenishment] failure_scope=infrastructure reusable_candidate=false",
                    "补源适配器失败: 暂存空间不足",
                ])

            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_ADAPTER": "fixture-adapter",
            }, clear=False), mock.patch.object(
                server, "run_command", side_effect=run_adapter,
            ), mock.patch.object(
                server, "replenishment_min_cloud_attempts", return_value=0,
            ):
                summary, sources, _restored_plan = server.prepare_post_scrape_replenishment(job)

            self.assertEqual(summary["status"], "acquire_failed")
            self.assertEqual(summary["projects"][0]["failure_scope"], "infrastructure")
            self.assertEqual(sources, [])
            self.assertFalse(server._replenishment_failure_path(job).exists())
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_successful_media_execution_checks_gaps_before_creating_followup(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "6" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={"kind": "media"},
            )
            job.directory.mkdir()
            plan = {"mode": "tv", "metadata": {"title": "Example", "tmdb_id": 42}}
            current_gaps = [{
                "kind": "missing_episode", "label": "S01E03 Three",
                "season": 1, "episodes": [3],
            }]
            with mock.patch.object(
                server, "audit_current_job_titles",
                return_value=title_closure_fixture(episode_gaps=current_gaps),
            ), mock.patch.object(
                server, "prepare_post_scrape_replenishment",
                return_value=({"status": "acquired", "gap_count": 1}, [{
                    "source": "/quark/影视/待刮削/Example 补源",
                    "media": {"title": "Example", "tmdb_id": 42},
                }], plan),
            ) as post_mock, mock.patch.object(
                server, "create_replenishment_followup",
                return_value=types.SimpleNamespace(id="followup1234"),
            ) as followup_mock, mock.patch.object(
                server, "_launch_replenishment_followup_monitor",
            ) as monitor_mock:
                server.finalize_media_replenishment(job)

            self.assertEqual(job.phase, "replenishing")
            self.assertEqual(job.progress["stage"], "replenishment_followup")
            self.assertEqual(job.plan_summary["replenishment"]["followup_job_id"], "followup1234")
            post_mock.assert_called_once_with(
                job, current_episode_gaps=current_gaps,
                scrape_gate_sha256="a" * 64,
            )
            followup_mock.assert_called_once_with(
                job, "/quark/影视/待刮削/Example 补源", plan,
                {"title": "Example", "tmdb_id": 42},
            )
            monitor_mock.assert_called_once_with(job, ["followup1234"])
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_scrape_first_gate_uses_inbox_and_local_job_evidence(self):
        self._scrape_first_gate_patcher.stop()
        self._scrape_first_gate_patcher = None
        current = server.Job(
            "a" * 12, "/quark/影视/待刮削/当前作品", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        running = server.Job(
            "b" * 12, "/quark/影视/待刮削/执行中作品", "/quark/影视/番剧",
            "tv", False, True, phase="planning_media",
        )
        accepted = server.Job(
            "c" * 12, "/quark/影视/待刮削/已验收作品", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        accepted.directory.mkdir()
        write_accepted_scrape_evidence(accepted)
        internal = server.Job(
            "f" * 12, "/quark/影视/待刮削/_ScrapeFlow补源-42-Example",
            "/quark/影视/番剧", "tv", False, True,
            phase="planning_media", visibility="internal",
        )
        server.JOBS = {
            row.id: row for row in (current, running, accepted, internal)
        }
        client = mock.MagicMock()
        client.list.return_value = [
            {"name": "另一个作品", "is_dir": True},
            {"name": "_ScrapeFlow恢复", "is_dir": True},
            {"name": "_ScrapeFlow补源-42-Example", "is_dir": True},
            {"name": "已完成（待删）", "is_dir": True},
            {"name": "散落.mkv", "is_dir": False},
        ]

        with mock.patch.object(
            server, "global_control_status", return_value={"paused": False},
        ), mock.patch.object(server, "_execution_alist_client", return_value=client):
            evidence = server.scrape_first_gate_evidence(current)

        self.assertFalse(evidence["ready"])
        self.assertEqual(evidence["status"], "blocked")
        self.assertEqual(evidence["blocker_count"], 3)
        self.assertEqual(
            {row["path"] for row in evidence["blockers"]},
            {
                "/quark/影视/待刮削/另一个作品",
                "/quark/影视/待刮削/散落.mkv",
                "/quark/影视/待刮削/执行中作品",
            },
        )
        client.list.assert_called_once_with(
            "/quark/影视/待刮削", refresh=True,
        )

    def test_scrape_first_gate_rejects_terminal_job_without_current_closure(self):
        self._scrape_first_gate_patcher.stop()
        self._scrape_first_gate_patcher = None
        current = server.Job(
            "5" * 12, "/quark/影视/待刮削/当前作品", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        stale = server.Job(
            "6" * 12, "/quark/影视/待刮削/旧假完成", "/quark/影视/番剧",
            "tv", False, True, phase="completed",
            plan_summary={"title_closure": {
                "status": "audited", "evidence_sha256": "e" * 64,
                "source_plan_sha256": "p" * 64, "summary": {"complete": True},
            }},
        )
        stale.directory.mkdir()
        server._atomic_json(
            stale.directory / "media-journal.json", {"success": True},
        )
        server.JOBS = {current.id: current, stale.id: stale}
        client = mock.MagicMock()
        client.list.return_value = []

        with mock.patch.object(
            server, "global_control_status", return_value={"paused": False},
        ), mock.patch.object(server, "_execution_alist_client", return_value=client):
            evidence = server.scrape_first_gate_evidence(current)

        self.assertFalse(evidence["ready"])
        self.assertEqual(evidence["blocker_count"], 1)
        blocker = evidence["blockers"][0]
        self.assertEqual(blocker["job_id"], stale.id)
        self.assertIn("current_signed_media_plan", blocker["failed_acceptance_checks"])
        self.assertIn(
            "valid_title_closure_evidence", blocker["failed_acceptance_checks"],
        )

    def test_scrape_first_gate_ready_evidence_has_stable_snapshot_digest(self):
        self._scrape_first_gate_patcher.stop()
        self._scrape_first_gate_patcher = None
        current = server.Job(
            "4" * 12, "/quark/影视/待刮削/当前作品", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        server.JOBS = {current.id: current}
        client = mock.MagicMock()
        client.list.return_value = []
        control = {
            "paused": False,
            "updated_at": "2026-08-04T00:00:00+00:00",
        }
        with mock.patch.object(
            server, "global_control_status", return_value=control,
        ), mock.patch.object(server, "_execution_alist_client", return_value=client):
            first = server.scrape_first_gate_evidence(current)
            second = server.scrape_first_gate_evidence(current)
        self.assertTrue(first["ready"])
        self.assertRegex(first["snapshot_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(first["snapshot_sha256"], second["snapshot_sha256"])

    def test_scrape_first_snapshot_change_blocks_before_mutation(self):
        job = server.Job(
            "3" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        changed = {
            "ready": True,
            "status": "ready",
            "checked_at": "2026-08-04T00:00:01+00:00",
            "blocker_count": 0,
            "blockers": [],
            "snapshot_sha256": "b" * 64,
        }
        action = mock.Mock(return_value="must-not-run")
        with mock.patch.object(
            server, "scrape_first_gate_evidence", return_value=changed,
        ), mock.patch.object(server, "_remote_dispatch_closed", return_value=False):
            with self.assertRaises(server.ScrapeFirstGateClosed) as raised:
                server._run_replenishment_mutation_stage(
                    job, action, scrape_gate_sha256="a" * 64,
                )
        action.assert_not_called()
        self.assertEqual(
            raised.exception.evidence["blockers"][-1]["kind"],
            "gate_snapshot_changed",
        )

    def test_ordinary_completion_contract_fails_closed_for_missing_artifact(self):
        job = server.Job(
            "9" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="completed",
        )
        job.directory.mkdir()
        write_accepted_scrape_evidence(job)
        (job.directory / "ordinary-title-completion.json").unlink()

        contract = server._ordinary_scrape_acceptance_contract(job)

        self.assertFalse(contract["accepted"])
        self.assertFalse(contract["extension_checks"]["completion_evidence_present"])

    def test_ordinary_tv_completion_contract_rejects_each_incomplete_dimension(self):
        mutations = {
            "missing_poster": lambda value: value["works"][0]["metadata"].update(
                {"series_poster_present": False},
            ),
            "missing_nfo": lambda value: value["works"][0]["metadata"].update(
                {"series_nfo_present": False},
            ),
            "unexpected_outer": lambda value: value["works"][0]["hierarchy"].update(
                {"unexpected_outer_directory_count": 1},
            ),
            "split_same_work": lambda value: value["works"][0]["hierarchy"].update(
                {"split_same_work_root_count": 1},
            ),
            "duplicate_video": lambda value: value["works"][0]["media"].update(
                {"duplicate_main_video_count": 1, "duplicate_groups": [["a", "b"]]},
            ),
            "no_main_video": lambda value: value["works"][0]["media"].update(
                {"main_video_count": 0},
            ),
            "novel": lambda value: value["works"][0]["residuals"].update(
                {"novel": 1},
            ),
            "manga": lambda value: value["works"][0]["residuals"].update(
                {"manga": 1},
            ),
            "docx": lambda value: value["works"][0]["residuals"].update(
                {"docx": 1},
            ),
            "ncop": lambda value: value["works"][0]["residuals"].update(
                {"ncop": 1},
            ),
            "detached_audio": lambda value: value["works"][0]["residuals"].update(
                {"detached_audio": 1},
            ),
        }
        for index, (label, mutate) in enumerate(mutations.items()):
            with self.subTest(label=label):
                job = server.Job(
                    f"a{index:011x}", "/quark/影视/待刮削/Example",
                    "/quark/影视/番剧", "tv", False, True,
                    phase="completed",
                )
                job.directory.mkdir()
                write_accepted_scrape_evidence(job)
                rewrite_ordinary_completion(job, mutate)

                contract = server._ordinary_scrape_acceptance_contract(job)

                self.assertFalse(contract["accepted"])
                self.assertFalse(
                    contract["extension_checks"]["completion_contract_valid"],
                )

    def test_ordinary_completion_subtitle_residual_policy_is_literal(self):
        allowed = server.Job(
            "b" * 12, "/quark/影视/待刮削/Allowed", "/quark/影视/番剧",
            "tv", False, True, phase="completed",
        )
        allowed.directory.mkdir()
        write_accepted_scrape_evidence(
            allowed, chinese_subtitle_gap_count=1, external_sidecar_count=1,
        )
        self.assertTrue(server._ordinary_scrape_job_accepted(allowed))
        self.assertFalse(
            server._ordinary_final_completion_contract(allowed)["accepted"],
        )

        chinese_present = server.Job(
            "c" * 12, "/quark/影视/待刮削/Present", "/quark/影视/番剧",
            "tv", False, True, phase="completed",
        )
        chinese_present.directory.mkdir()
        write_accepted_scrape_evidence(
            chinese_present, chinese_subtitle_gap_count=0,
            external_sidecar_count=1,
        )
        self.assertFalse(server._ordinary_scrape_job_accepted(chinese_present))

        duplicate_external = server.Job(
            "d" * 12, "/quark/影视/待刮削/Duplicate", "/quark/影视/番剧",
            "tv", False, True, phase="completed",
        )
        duplicate_external.directory.mkdir()
        write_accepted_scrape_evidence(
            duplicate_external, chinese_subtitle_gap_count=1,
            external_sidecar_count=2,
        )
        self.assertFalse(server._ordinary_scrape_job_accepted(duplicate_external))

    def test_ordinary_movie_completion_uses_movie_specific_metadata_contract(self):
        job = server.Job(
            "e" * 12, "/quark/影视/待刮削/Movie", "/quark/影视/电影",
            "movie", False, True, phase="completed",
        )
        job.directory.mkdir()
        write_accepted_scrape_evidence(job)
        self.assertTrue(server._ordinary_scrape_job_accepted(job))

        rewrite_ordinary_completion(
            job,
            lambda value: value["works"][0]["metadata"].update({
                "contract": "tv",
                "series_nfo_present": True,
                "series_poster_present": True,
            }),
        )
        self.assertFalse(server._ordinary_scrape_job_accepted(job))

    def test_ordinary_completion_requires_bound_source_departure_evidence(self):
        job = server.Job(
            "f" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="completed",
        )
        job.directory.mkdir()
        write_accepted_scrape_evidence(job)
        rewrite_ordinary_completion(
            job,
            lambda value: value["source_departure"].update({
                "absent_from_unscraped_root": False,
            }),
        )
        self.assertFalse(server._ordinary_scrape_job_accepted(job))

    def test_ordinary_completion_cannot_rebind_old_plan_or_closure(self):
        for index, field in enumerate((
            "source_plan_sha256", "title_closure_sha256",
            "title_targets_sha256",
        )):
            with self.subTest(field=field):
                job = server.Job(
                    f"1{index:011x}", "/quark/影视/待刮削/Example",
                    "/quark/影视/番剧", "tv", False, True,
                    phase="completed",
                )
                job.directory.mkdir()
                write_accepted_scrape_evidence(job)
                rewrite_ordinary_completion(
                    job, lambda value, field=field: value.update({field: "0" * 64}),
                )
                self.assertFalse(server._ordinary_scrape_job_accepted(job))

    def test_delayed_replenishment_retry_arms_only_one_timer_per_job(self):
        job = server.Job(
            "7" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        threads = []

        class FakeThread:
            def __init__(self, **kwargs):
                threads.append(kwargs)

            def start(self):
                return None

        server.DELAYED_REPLENISHMENT_RETRY_PENDING.discard(job.id)
        try:
            with mock.patch.object(server.threading, "Thread", FakeThread):
                self.assertTrue(server._launch_delayed_replenishment_retry(job, 30))
                self.assertTrue(server._launch_delayed_replenishment_retry(job, 30))
            self.assertEqual(len(threads), 1)
        finally:
            server.DELAYED_REPLENISHMENT_RETRY_PENDING.discard(job.id)
            server.DELAYED_REPLENISHMENT_RETRY_TOKENS.pop(job.id, None)

    def test_delayed_retry_releases_old_token_before_dispatching_callback(self):
        job = server.Job(
            "6" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        threads = []

        class FakeThread:
            def __init__(self, **kwargs):
                threads.append(kwargs)

            def start(self):
                return None

        def dispatch_immediately(_job, _delay, *, before_dispatch=None):
            self.assertIsNotNone(before_dispatch)
            before_dispatch()
            self.assertNotIn(job.id, server.DELAYED_REPLENISHMENT_RETRY_PENDING)
            self.assertTrue(server._launch_delayed_replenishment_retry(job, 31))

        server.DELAYED_REPLENISHMENT_RETRY_PENDING.discard(job.id)
        server.DELAYED_REPLENISHMENT_RETRY_TOKENS.pop(job.id, None)
        try:
            with (
                mock.patch.object(server.threading, "Thread", FakeThread),
                mock.patch.object(
                    server, "_delayed_replenishment_retry",
                    side_effect=dispatch_immediately,
                ),
            ):
                self.assertTrue(server._launch_delayed_replenishment_retry(job, 30))
                self.assertEqual(len(threads), 1)
                threads[0]["target"]()
            self.assertEqual(len(threads), 2)
            self.assertIn(job.id, server.DELAYED_REPLENISHMENT_RETRY_PENDING)
            self.assertIn(job.id, server.DELAYED_REPLENISHMENT_RETRY_TOKENS)
        finally:
            server.DELAYED_REPLENISHMENT_RETRY_PENDING.discard(job.id)
            server.DELAYED_REPLENISHMENT_RETRY_TOKENS.pop(job.id, None)

    def test_delayed_replenishment_retry_exits_on_shutdown(self):
        job = server.Job(
            "8" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        previous_shutdown = server.SHUTDOWN_EVENT.is_set()
        try:
            server.SHUTDOWN_EVENT.set()
            with mock.patch.object(server, "start_thread") as starter:
                server._delayed_replenishment_retry(job, 30)
            starter.assert_not_called()
        finally:
            if not previous_shutdown:
                server.SHUTDOWN_EVENT.clear()

    def test_post_scrape_replenishment_waits_without_consuming_a_round(self):
        job = server.Job(
            "d" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing", replenishment_round=4,
            plan_summary={"kind": "media"},
        )
        job.directory.mkdir()
        closure = title_closure_fixture(episode_gaps=[{
            "kind": "missing_episode", "label": "S01E03",
        }])
        gate = {
            "ready": False, "status": "blocked", "blocker_count": 1,
            "checked_at": "2026-08-04T00:00:00+00:00",
            "blockers": [{
                "kind": "inbox_directory",
                "path": "/quark/影视/待刮削/Another",
            }],
            "message": "待刮削仍有 1 项未完成",
        }
        with mock.patch.object(
            server, "audit_current_job_titles", return_value=closure,
        ) as audit_mock, mock.patch.object(
            server, "scrape_first_gate_evidence", return_value=gate,
        ), mock.patch.object(
            server, "prepare_post_scrape_replenishment",
        ) as replenish_mock, mock.patch.object(
            server, "execute_current_title_subtitles",
        ) as subtitle_mock, mock.patch.object(
            server, "_schedule_scrape_first_recheck", return_value=True,
        ) as schedule_mock:
            server.finalize_media_replenishment(job)

        replenish_mock.assert_not_called()
        subtitle_mock.assert_not_called()
        audit_mock.assert_not_called()
        schedule_mock.assert_called_once()
        self.assertEqual(job.phase, "replenishing")
        self.assertEqual(job.replenishment_round, 4)
        self.assertEqual(job.plan_summary["replenishment"]["status"], "scrape_first_wait")
        self.assertNotIn("title_closure", job.plan_summary)

    def test_resume_restores_scrape_first_wait_without_candidate_rotation(self):
        job = server.Job(
            "e" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing", replenishment_round=6,
            plan_summary={"replenishment": {
                "status": "scrape_first_wait",
                "next_check_at": "2026-08-04T00:00:00+00:00",
            }},
        )
        job.directory.mkdir()
        server.JOBS = {job.id: job}
        with mock.patch.object(
            server, "close_consumed_internal_replenishment_followup",
        ), mock.patch.object(
            server, "_repair_failed_replenishment_followup",
        ), mock.patch.object(
            server, "_schedule_scrape_first_recheck", return_value=True,
        ) as schedule_mock, mock.patch.object(
            server, "_schedule_replenishment_retry",
        ) as rotation_mock, mock.patch.object(server, "start_thread") as starter:
            server.resume_jobs()

        schedule_mock.assert_called_once_with(
            job, summary=dict(job.plan_summary), restored=True,
        )
        rotation_mock.assert_not_called()
        starter.assert_not_called()
        self.assertEqual(job.replenishment_round, 6)

    def test_scrape_first_gate_fails_closed_while_paused_without_reading_inbox(self):
        self._scrape_first_gate_patcher.stop()
        self._scrape_first_gate_patcher = None
        job = server.Job(
            "1" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        with mock.patch.object(
            server, "global_control_status", return_value={"paused": True},
        ), mock.patch.object(server, "_execution_alist_client") as client_mock:
            evidence = server.scrape_first_gate_evidence(job)

        self.assertFalse(evidence["ready"])
        self.assertEqual(evidence["status"], "blocked")
        self.assertEqual(evidence["blockers"][0]["kind"], "global_pause")
        client_mock.assert_not_called()

    def test_scrape_first_gate_requires_current_source_to_leave_live_inbox(self):
        self._scrape_first_gate_patcher.stop()
        self._scrape_first_gate_patcher = None
        job = server.Job(
            "0" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing",
        )
        job.directory.mkdir()
        write_accepted_scrape_evidence(job)
        self.assertTrue(server._ordinary_scrape_job_accepted(job))
        client = mock.MagicMock()
        client.list.return_value = [{"name": "Example", "is_dir": True}]
        with mock.patch.object(
            server, "global_control_status", return_value={"paused": False},
        ), mock.patch.object(server, "_execution_alist_client", return_value=client):
            evidence = server.scrape_first_gate_evidence(job)

        self.assertFalse(evidence["ready"])
        self.assertEqual(evidence["blocker_count"], 1)
        self.assertEqual(
            evidence["blockers"][0]["path"],
            "/quark/影视/待刮削/Example",
        )

    def test_automatic_retry_waits_at_scrape_first_gate_without_rotating_round(self):
        job = server.Job(
            "2" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="replenishing", replenishment_round=4,
            plan_summary={"replenishment": {
                "status": "acquire_failed", "gap_count": 1,
            }},
        )
        job.directory.mkdir()
        gate = {
            "ready": False, "status": "blocked", "blocker_count": 1,
            "checked_at": "2026-08-04T00:00:00+00:00",
            "blockers": [{
                "kind": "inbox_directory",
                "path": "/quark/影视/待刮削/Another",
            }],
            "message": "待刮削仍有 1 项未完成",
        }
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
            "SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS": "0",
        }, clear=False), mock.patch.object(
            server, "scrape_first_gate_evidence", return_value=gate,
        ), mock.patch.object(
            server, "_schedule_scrape_first_recheck", return_value=True,
        ) as schedule_mock, mock.patch.object(
            server, "_seed_replenishment_failures_from_last_attempt",
        ) as rotate_mock:
            scheduled = server._schedule_replenishment_retry(job)

        self.assertTrue(scheduled)
        self.assertEqual(job.replenishment_round, 4)
        self.assertEqual(job.plan_summary["replenishment"]["status"], "scrape_first_wait")
        schedule_mock.assert_called_once()
        rotate_mock.assert_not_called()

    def test_failed_post_commit_retry_enters_scrape_first_wait_before_worker(self):
        job = server.Job(
            "3" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="failed", plan_summary={"kind": "media"},
        )
        job.directory.mkdir()
        server._atomic_json(job.directory / "media-plan.json", {
            "plan": {"mode": "tv", "files": []},
        })
        server._atomic_json(job.directory / "media-journal.json", {"success": True})
        gate = {
            "ready": False, "status": "blocked", "blocker_count": 1,
            "checked_at": "2026-08-04T00:00:00+00:00",
            "blockers": [{
                "kind": "local_job", "path": "/quark/影视/待刮削/Another",
            }],
            "message": "待刮削仍有 1 项未完成",
        }
        with mock.patch.object(
            server, "scrape_first_gate_evidence", return_value=gate,
        ), mock.patch.object(
            server, "_schedule_scrape_first_recheck", return_value=True,
        ) as schedule_mock, mock.patch.object(server, "start_thread") as starter:
            resumed = server._resume_post_commit_replenishment(job)

        self.assertTrue(resumed)
        self.assertEqual(job.phase, "replenishing")
        self.assertEqual(job.plan_summary["replenishment"]["status"], "scrape_first_wait")
        schedule_mock.assert_called_once()
        starter.assert_not_called()

    def test_user_retry_restores_persisted_scrape_first_wait_without_browse(self):
        job = server.Job(
            "4" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
            "tv", False, True, phase="failed", replenishment_round=7,
            plan_summary={"replenishment": {
                "status": "scrape_first_wait", "gap_count": 2,
                "next_check_at": "2026-08-04T00:00:00+00:00",
            }},
        )
        job.directory.mkdir()
        server._atomic_json(job.directory / "media-journal.json", {"success": True})
        with mock.patch.object(
            server, "_schedule_scrape_first_recheck", return_value=True,
        ) as schedule_mock, mock.patch.object(server, "browse_remote") as browse_mock:
            resumed = server.retry_job(job)

        self.assertIs(resumed, job)
        self.assertEqual(job.phase, "replenishing")
        self.assertEqual(job.replenishment_round, 7)
        schedule_mock.assert_called_once()
        browse_mock.assert_not_called()

    def test_cancel_during_followup_creation_never_reopens_parent(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "7" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={"kind": "media"},
            )
            job.directory.mkdir()
            followup = server.Job(
                "8" * 12, "/quark/影视/待刮削/Example 补源", "/quark/影视/番剧",
                "tv", False, True, phase="queued", visibility="internal",
                root_job_id=job.id,
            )
            followup.directory.mkdir()

            def create_then_cancel(*_args, **_kwargs):
                job.cancel_requested = True
                job.phase = "cancelling"
                return followup

            with mock.patch.object(
                server, "audit_current_job_titles",
                return_value=title_closure_fixture(episode_gaps=[{
                    "kind": "missing_episode", "label": "S01E03",
                }]),
            ), mock.patch.object(
                server, "prepare_post_scrape_replenishment",
                return_value=({"status": "acquired", "gap_count": 1}, [{
                    "source": followup.source,
                    "media": {"title": "Example", "tmdb_id": 42},
                }], {"mode": "tv"}),
            ), mock.patch.object(
                server, "create_replenishment_followup", side_effect=create_then_cancel,
            ), mock.patch.object(
                server.SCHEDULER, "cancel_pending", return_value=True,
            ):
                server.finalize_media_replenishment(job)

            self.assertEqual(job.phase, "cancelled")
            self.assertEqual(followup.phase, "cancelled")
            self.assertTrue(followup.cancel_requested)
            self.assertEqual(
                job.plan_summary["replenishment"]["status"],
                "cancelled_after_media_commit",
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_current_title_subtitles_run_after_episode_gaps_close_then_reaudit(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "a" * 12, "/quark/影视/待刮削/Example",
                "/quark/影视/番剧", "tv", False, True,
                phase="replenishing", plan_summary={"kind": "media"},
            )
            job.directory.mkdir()
            write_accepted_scrape_evidence(job)
            after = server.load_json(job.directory / "title-closure.json")
            before = json.loads(json.dumps(after))
            before["summary"].update({
                "confirmed_subtitle_gap_count": 1,
                "complete": False,
            })
            with mock.patch.object(
                server, "audit_current_job_titles", side_effect=[before, after],
            ) as audit_mock, mock.patch.object(
                server, "execute_current_title_subtitles",
                return_value={"status": "complete", "unresolved_action_count": 0},
            ) as subtitle_mock, mock.patch.object(
                server, "prepare_post_scrape_replenishment",
                return_value=({"status": "no_regular_gaps", "gap_count": 0}, [], {}),
            ) as post_mock, mock.patch.object(
                server, "remember_completed_job",
            ) as remember_mock:
                server.finalize_media_replenishment(job)

            self.assertEqual(audit_mock.call_count, 2)
            subtitle_mock.assert_called_once_with(
                job, before, scrape_gate_sha256="a" * 64,
            )
            post_mock.assert_called_once_with(
                job, current_episode_gaps=[], scrape_gate_sha256="a" * 64,
            )
            self.assertEqual(job.phase, "completed")
            self.assertTrue(job.plan_summary["title_closure"]["summary"]["complete"])
            remember_mock.assert_called_once_with(job)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_unresolved_current_title_subtitles_stay_retryable(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "b" * 12, "/quark/影视/待刮削/Example",
                "/quark/影视/番剧", "tv", False, True,
                phase="replenishing", plan_summary={"kind": "media"},
            )
            job.directory.mkdir()
            unresolved = title_closure_fixture(pending_subtitle_verification=1)
            with mock.patch.object(
                server, "audit_current_job_titles",
                side_effect=[unresolved, unresolved],
            ), mock.patch.object(
                server, "execute_current_title_subtitles",
                return_value={"status": "retryable", "unresolved_action_count": 1},
            ) as subtitle_mock, mock.patch.object(
                server, "prepare_post_scrape_replenishment",
            ) as post_mock, mock.patch.object(
                server, "_schedule_replenishment_retry",
            ) as source_retry_mock, mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as reaudit_mock:
                server.finalize_media_replenishment(job)

            subtitle_mock.assert_called_once_with(
                job, unresolved, scrape_gate_sha256="a" * 64,
            )
            post_mock.assert_not_called()
            source_retry_mock.assert_not_called()
            reaudit_mock.assert_called_once()
            self.assertIs(reaudit_mock.call_args.args[0], job)
            self.assertEqual(
                job.plan_summary["replenishment"]["status"], "subtitle_retryable",
            )
            self.assertEqual(job.replenishment_round, 0)
            self.assertFalse(server._replenishment_failure_path(job).exists())
            self.assertEqual(job.phase, "replenishing")
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_paused_title_audit_does_not_consume_round_or_claim_failure(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "1" * 12, "/quark/影视/待刮削/Example",
                "/quark/影视/番剧", "tv", False, True,
                phase="replenishing", replenishment_round=7,
                plan_summary={"title_closure": {"summary": {"complete": True}}},
            )
            job.directory.mkdir()
            with mock.patch.object(
                server, "audit_current_job_titles",
                side_effect=server.TitleClosureBlocked("pause_active:entry"),
            ), mock.patch.object(
                server, "_schedule_replenishment_retry",
            ) as source_retry_mock, mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as reaudit_mock:
                server.finalize_media_replenishment(job)

            source_retry_mock.assert_not_called()
            reaudit_mock.assert_called_once_with(job, 1)
            self.assertEqual(job.phase, "replenishing")
            self.assertEqual(job.replenishment_round, 7)
            self.assertNotIn("residual_routing", job.plan_summary)
            self.assertEqual(job.plan_summary["title_closure"]["status"], "paused")
            self.assertNotIn("summary", job.plan_summary["title_closure"])
            self.assertEqual(
                job.plan_summary["replenishment"]["status"], "title_reaudit_paused",
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_failed_title_reaudit_replaces_stale_success_but_keeps_routing_truth(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "2" * 12, "/quark/影视/待刮削/Example",
                "/quark/影视/番剧", "tv", False, True,
                phase="replenishing",
                plan_summary={
                    "title_closure": {"summary": {"complete": True}},
                    "title_subtitle_execution": {"status": "converged"},
                },
            )
            job.directory.mkdir()
            with mock.patch.object(
                server, "audit_current_job_titles", side_effect=ValueError("AList timeout"),
            ), mock.patch.object(
                server, "_schedule_replenishment_retry",
            ) as source_retry_mock, mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as reaudit_mock:
                server.finalize_media_replenishment(job)

            source_retry_mock.assert_not_called()
            reaudit_mock.assert_called_once()
            self.assertNotIn("residual_routing", job.plan_summary)
            self.assertEqual(job.plan_summary["title_closure"]["status"], "unavailable")
            self.assertNotIn("summary", job.plan_summary["title_closure"])
            self.assertEqual(
                job.plan_summary["title_subtitle_execution"],
                {"status": "not_run", "reason": "title_closure_unavailable"},
            )
            self.assertEqual(job.replenishment_round, 0)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_cancel_during_title_audit_stops_before_replenishment_search(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "3" * 12, "/quark/影视/待刮削/Example",
                "/quark/影视/番剧", "tv", False, True,
                phase="replenishing",
            )
            job.directory.mkdir()

            def cancel_during_audit(_job):
                job.cancel_requested = True
                job.phase = "cancelling"
                return title_closure_fixture(episode_gaps=[{
                    "kind": "missing_episode", "label": "S01E03",
                }])

            with mock.patch.object(
                server, "audit_current_job_titles", side_effect=cancel_during_audit,
            ), mock.patch.object(
                server, "prepare_post_scrape_replenishment",
            ) as search_mock, mock.patch.object(
                server, "remember_completed_job",
            ) as remember_mock:
                server.finalize_media_replenishment(job)

            search_mock.assert_not_called()
            remember_mock.assert_not_called()
            self.assertEqual(job.phase, "cancelled")
            self.assertEqual(
                job.plan_summary["replenishment"]["status"],
                "cancelled_after_media_commit",
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_search_is_skipped_when_forced_refresh_finds_planned_gap_live(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "4" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv", "source_root": job.source,
                "target_root": "/quark/影视/番剧/Example",
                "metadata": {
                    "tmdb_id": 42, "title": "Example", "year": "2026",
                    "series_root": "/quark/影视/番剧/Example",
                },
                "files": [],
                "scan_report": {"resource_gaps": [{
                    "kind": "missing_episode", "label": "S01E01 stale",
                }]},
            }
            server._atomic_json(job.directory / "media-plan.json", {
                "plan": plan, "plan_sha256": server.canonical_digest(plan),
            })
            calls = []

            def run_adapter(_job, command):
                calls.append(command)
                raise AssertionError("实时目标已满足全部缺口时不得再搜索")

            fake_client = types.SimpleNamespace(list=lambda _path, refresh=False: [
                {"name": "Example - S01E01 - One.mkv", "is_dir": False},
            ])
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_ADAPTER": "fixture-adapter",
            }, clear=False), mock.patch.object(
                server, "run_command", side_effect=run_adapter,
            ), mock.patch.object(
                server, "_execution_alist_client", return_value=fake_client,
            ), mock.patch.object(server.time, "sleep"):
                summary, followups, _ = server.prepare_post_scrape_replenishment(job)
            self.assertEqual(summary["status"], "no_regular_gaps")
            self.assertEqual(followups, [])
            self.assertEqual(calls, [])
            self.assertFalse((job.directory / "replenishment-acquisition.json").exists())
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_live_suppression_enables_optional_manifest_mapping_before_search(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "3" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv", "source_root": job.source,
                "target_root": "/quark/影视/番剧/Example",
                "metadata": {
                    "tmdb_id": 42, "title": "Example", "year": "2026",
                    "series_root": "/quark/影视/番剧/Example",
                },
                "files": [],
                "scan_report": {"resource_gaps": [
                    {"kind": "missing_episode", "label": "S01E01 stale"},
                    {"kind": "missing_episode", "label": "S00E07 OVA"},
                ]},
            }
            server._atomic_json(job.directory / "media-plan.json", {
                "plan": plan, "plan_sha256": server.canonical_digest(plan),
            })
            seen_request = {}
            search_calls = 0

            def run_adapter(_job, command):
                nonlocal search_calls
                self.assertIn("search", command)
                search_calls += 1
                request_path = Path(command[command.index("--request") + 1])
                seen_request.update(json.loads(request_path.read_text(encoding="utf-8")))
                output = Path(command[command.index("--output") + 1])
                payload = {"candidates": []}
                if search_calls == 1:
                    payload["lane_status"] = {"quark_share": {
                        "status": "infrastructure_failure",
                        "reason": "quark_share_index_unavailable",
                    }}
                output.write_text(json.dumps(payload) + "\n", encoding="utf-8")
                return 0, ""

            fake_client = types.SimpleNamespace(list=lambda _path, refresh=False: [
                {"name": "Example - S01E01 - One.mkv", "is_dir": False},
            ])
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_ADAPTER": "fixture-adapter",
            }, clear=False), mock.patch.object(
                server, "run_command", side_effect=run_adapter,
            ), mock.patch.object(
                server, "_execution_alist_client", return_value=fake_client,
            ), mock.patch.object(server.time, "sleep"):
                first, first_followups, _ = server.prepare_post_scrape_replenishment(job)
                self.assertEqual(first["status"], "no_match")
                self.assertEqual(first_followups, [])
                self.assertEqual(
                    server._load_replenishment_provider_attempts(job, seen_request),
                    {"quark_share": 0, "quark_magnet": 0},
                )
                summary, followups, _ = server.prepare_post_scrape_replenishment(job)

            self.assertEqual(summary["status"], "no_match")
            self.assertEqual(followups, [])
            self.assertEqual([gap["id"] for gap in seen_request["gaps"]], ["S00E07"])
            self.assertTrue(seen_request["rules"]["optional_discovery_only"])
            self.assertTrue(
                seen_request["rules"]["season_zero_replenishment_required"]
            )
            self.assertEqual(search_calls, 2)
            self.assertEqual(
                server._load_replenishment_provider_attempts(job, seen_request),
                {"quark_share": 0, "quark_magnet": 0},
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_mixed_share_failure_records_resource_locators_before_infrastructure_block(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "4" * 12,
                "/quark/影视/待刮削/Example",
                "/quark/影视/番剧",
                "tv",
                False,
                True,
                phase="replenishing",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv",
                "source_root": job.source,
                "target_root": "/quark/影视/番剧/Example",
                "metadata": {
                    "tmdb_id": 42,
                    "title": "Example",
                    "year": "2026",
                    "series_root": "/quark/影视/番剧/Example",
                },
                "files": [],
                "scan_report": {"resource_gaps": [
                    {"kind": "missing_episode", "label": "S01E01 missing"},
                ]},
            }
            server._atomic_json(job.directory / "media-plan.json", {
                "plan": plan,
                "plan_sha256": server.canonical_digest(plan),
            })
            seen_request = {}

            def run_adapter(_job, command):
                self.assertIn("search", command)
                request_path = Path(command[command.index("--request") + 1])
                seen_request.update(json.loads(request_path.read_text(encoding="utf-8")))
                output = Path(command[command.index("--output") + 1])
                output.write_text(json.dumps({
                    "candidates": [],
                    "lane_status": {"quark_share": {
                        "status": "infrastructure_failure",
                        "reason": "one_share_inspection_failed",
                    }},
                    "share_discovery": {
                        "resource_failed_locators": [
                            f"quark_share:resource-{index}" for index in range(5)
                        ] + ["quark_magnet:wrong-share-scope"],
                        "infrastructure_failures": ["quark_share:infra-one"],
                    },
                    "magnet_discovery": {
                        "resource_failed_locators": [
                            "quark_magnet:resource-0",
                            "quark_magnet:resource-1",
                            "quark_share:wrong-magnet-scope",
                        ],
                        "infrastructure_failures": ["quark_magnet:infra-one"],
                    },
                }) + "\n", encoding="utf-8")
                return 0, ""

            fake_client = types.SimpleNamespace(list=lambda _path, refresh=False: [])
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_ADAPTER": "fixture-adapter",
            }, clear=False), mock.patch.object(
                server, "run_command", side_effect=run_adapter,
            ), mock.patch.object(
                server, "_execution_alist_client", return_value=fake_client,
            ), mock.patch.object(
                server, "replenishment_min_cloud_attempts", return_value=30,
            ):
                summary, followups, _ = server.prepare_post_scrape_replenishment(job)
                repeated, repeated_followups, _ = (
                    server.prepare_post_scrape_replenishment(job)
                )

            self.assertEqual(summary["status"], "no_match")
            self.assertEqual(followups, [])
            self.assertEqual(repeated["status"], "no_match")
            self.assertEqual(repeated_followups, [])
            self.assertEqual(
                server._load_replenishment_provider_attempts(job, seen_request),
                {"quark_share": 5, "quark_magnet": 2},
            )
            self.assertTrue(any(
                "已入账 5 个" in line for line in job.logs
            ))
            self.assertTrue(any(
                "quark_magnet 已入账 2 个" in line for line in job.logs
            ))
            self.assertTrue(any(
                "同时存在基础设施故障" in line for line in job.logs
            ))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_retry_successful_media_journal_only_resumes_post_commit_check(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "5" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="cancelled",
                plan_summary={"replenishment": {
                    "status": "cancelled_after_media_commit",
                }},
                progress={"stage": "replenishment_complete", "percent": 100.0},
            )
            job.cancel_requested = True
            job.directory.mkdir()
            (job.directory / "media-plan.json").write_text("{}\n", encoding="utf-8")
            (job.directory / "media-journal.json").write_text(
                '{"success":true}\n', encoding="utf-8",
            )
            with mock.patch.object(server, "start_thread") as starter:
                returned = server.retry_job(job)
            self.assertIs(returned, job)
            self.assertEqual(job.phase, "replenishing")
            self.assertFalse(job.cancel_requested)
            self.assertEqual(job.progress["stage"], "replenishment_search")
            starter.assert_called_once_with(server.finalize_media_replenishment, job)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_current_title_audit_is_plan_digest_scoped_and_persisted(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "4" * 12,
                "/quark/影视/待刮削/Example",
                "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            job.directory.mkdir()
            plan = {
                "mode": "tv", "source_root": job.source,
                "target_root": "/quark/影视/番剧/Example",
                "metadata": {
                    "title": "Example", "tmdb_id": 42, "year": "2026",
                    "season": 1, "absolute": False,
                },
                "files": [],
            }
            digest = server.canonical_digest(plan)
            server._atomic_json(job.directory / "media-plan.json", {
                "plan": plan, "plan_sha256": digest,
            })
            server._atomic_json(job.directory / "media-journal.json", {
                "success": True,
            })
            evidence = {
                "schema_version": 1,
                "source_plan_sha256": digest,
                "summary": {
                    "episode_gap_count": 0,
                    "confirmed_subtitle_gap_count": 0,
                    "pending_subtitle_verification_count": 0,
                    "complete": True,
                },
                "evidence_sha256": "f" * 64,
            }
            scanner = object()
            client = object()
            completion = {"schema_version": 1, "evidence_sha256": "a" * 64}
            with mock.patch.dict(
                os.environ, {"TMDB_API_KEY": "configured"}, clear=False,
            ), mock.patch.object(
                server, "_execution_alist_client", return_value=client,
            ), mock.patch.object(
                server, "make_current_title_episode_gap_scanner",
                return_value=scanner,
            ) as make_scanner, mock.patch.object(
                server, "build_title_closure_evidence", return_value=evidence,
            ) as build, mock.patch.object(
                server, "title_closure_evidence_is_valid", return_value=True,
            ), mock.patch.object(
                server, "build_ordinary_title_completion",
                return_value=completion,
            ) as build_completion, mock.patch.object(
                server, "ordinary_completion_evidence_is_valid",
                return_value=True,
            ), mock.patch(
                "engine.scraper.TMDBClient", return_value="tmdb",
            ):
                result = server.audit_current_job_titles(job)

            self.assertEqual(result, evidence)
            make_scanner.assert_called_once_with(
                client, "tmdb", today=server.audit_local_date(),
            )
            self.assertEqual(build.call_args.args[:2], (plan, digest))
            self.assertIs(build.call_args.kwargs["alist"], client)
            self.assertIs(
                build.call_args.kwargs["adapters"].scan_episode_gaps,
                scanner,
            )
            self.assertEqual(
                server.load_json(job.directory / "title-closure.json"),
                evidence,
            )
            build_completion.assert_called_once_with(
                plan, digest, evidence, source_path=job.source, alist=client,
            )
            self.assertEqual(
                server.load_json(job.directory / "ordinary-title-completion.json"),
                completion,
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_current_title_subtitle_execution_is_job_scoped(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "3" * 12,
                "/quark/影视/待刮削/Example",
                "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            job.directory.mkdir()
            closure = {
                "summary": {
                    "episode_gap_count": 0,
                    "confirmed_subtitle_gap_count": 1,
                    "pending_subtitle_verification_count": 0,
                },
                "subtitle_refinement": {"confirmed_missing_chinese": [{}]},
            }
            digest = "a" * 64
            prepared = {"selection": {"selection_sha256": digest}}
            result = {
                "schema_version": 1,
                "kind": "title_subtitle_execution",
                "status": "retryable_unresolved",
                "unresolved_action_count": 1,
            }
            with mock.patch.object(
                server, "title_closure_evidence_is_valid", return_value=True,
            ), mock.patch.object(
                server, "_subtitle_discovery_manifest_checkpoints",
                return_value=[{
                    "batch_id": "b" * 24,
                    "manifest_set_sha256": "c" * 64,
                }],
            ), mock.patch.object(
                server, "prepare_title_subtitle_execution",
                return_value=("client", prepared),
            ), mock.patch.object(
                server, "execute_prepared_title_subtitles",
                return_value=result,
            ) as execute:
                returned = server.execute_current_title_subtitles(job, closure)

            expected = {
                **result,
                "discovery_manifest_checkpoints": [{
                    "batch_id": "b" * 24,
                    "manifest_set_sha256": "c" * 64,
                }],
            }
            self.assertEqual(returned, expected)
            execute.assert_called_once_with(
                "client",
                prepared,
                execution_root=job.directory / "title-subtitles" / digest,
                owner_job_id=job.id,
                item_guard=server._subtitle_execution_guard,
            )
            self.assertEqual(
                server.load_json(job.directory / "title-subtitle-result.json"),
                expected,
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_recovery_request_with_successful_media_journal_resumes_post_commit_check(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "6" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="recovery_required",
                progress={
                    "stage": "replenishment_retry_wait", "percent": 94.0,
                },
            )
            job.directory.mkdir()
            (job.directory / "media-plan.json").write_text("{}\n", encoding="utf-8")
            (job.directory / "media-journal.json").write_text(
                '{"success":true}\n', encoding="utf-8",
            )
            with mock.patch.object(server, "start_thread") as starter, mock.patch.object(
                server, "run_command", side_effect=AssertionError(
                    "成功 journal 不得进入回滚检查",
                ),
            ):
                server.prepare_recovery(job)
            self.assertEqual(job.phase, "replenishing")
            self.assertIsNone(job.error)
            self.assertEqual(job.progress["stage"], "replenishment_search")
            starter.assert_called_once_with(server.finalize_media_replenishment, job)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_replenishment_followup_inherits_failed_candidate_ledger(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            server.JOBS = {}
            previous = server.Job(
                "9" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing", replenishment_round=1,
            )
            previous.directory.mkdir()
            server._atomic_json(server._replenishment_failure_path(previous), {
                "version": 1, "job_id": previous.id, "failures": [{
                    "release_name": "Dead release", "locator": "torrent:dead",
                    "infohash": "0123456789abcdef0123456789abcdef01234567",
                }],
            })
            with mock.patch.object(server, "start_thread"):
                followup = server.create_replenishment_followup(
                    previous, "/quark/影视/待刮削/Example 补源", {},
                    {
                        "tmdb_id": 42, "title": "Example",
                        "target_root": "/quark/影视/番剧/Fate/Example",
                    },
                )
            inherited = json.loads(
                server._replenishment_failure_path(followup).read_text(encoding="utf-8")
            )
            self.assertEqual(inherited["inherited_from_job_id"], previous.id)
            self.assertEqual(inherited["failures"][0]["locator"], "torrent:dead")
            self.assertEqual(followup.replenishment_round, 2)
            self.assertEqual(followup.parent, "/quark/影视/番剧/Fate")
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_replenishment_followup_inherits_exact_multiseason_map_from_system_source(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            server.JOBS = {}
            source = "/quark/影视/ScrapeFlow/补源/Example-run"
            previous = server.Job(
                "8" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            previous.directory.mkdir()
            server.JOBS[previous.id] = previous
            server._atomic_json(previous.directory / "replenishment-selection.json", {
                "request": {
                    "media": {"tmdb_id": 42, "title": "Example"},
                    "gaps": [
                        {"id": "S03E01", "kind": "missing_episode", "season": 3, "episodes": [1]},
                        {"id": "S04E14", "kind": "missing_episode", "season": 4, "episodes": [14]},
                    ],
                },
                "selection": {"selections": [{"acquisition": {"expected_files": [
                    {"path": "Season 3/Example [01].mkv", "size": 101, "gap_ids": ["S03E01"]},
                    {"path": "Season 4/Example [14].mkv", "size": 114, "gap_ids": ["S04E14"]},
                ]}}]},
            })
            server._atomic_json(previous.directory / "replenishment-acquisition.json", {
                "status": "ready", "source_paths": [source],
            })

            def list_source(path, refresh=False):
                self.assertTrue(refresh)
                tree = {
                    source: [{"name": "release", "is_dir": True}],
                    source + "/release": [
                        {"name": "Season 3", "is_dir": True},
                        {"name": "Season 4", "is_dir": True},
                    ],
                    source + "/release/Season 3": [
                        {"name": "Example [01].mkv", "is_dir": False, "size": 101},
                    ],
                    source + "/release/Season 4": [
                        {"name": "Example [14].mkv", "is_dir": False, "size": 114},
                    ],
                }
                return tree[path]

            fake_client = types.SimpleNamespace(list=list_source)
            with mock.patch.object(
                server, "_execution_alist_client", return_value=fake_client,
            ), mock.patch.object(server, "start_thread") as starter:
                followup = server.create_replenishment_followup(
                    previous, source, {}, {"tmdb_id": 42, "title": "Example"},
                )
            self.assertEqual(followup.visibility, "internal")
            self.assertIsNone(followup.season)
            self.assertEqual(followup.episode_map, {"E1": "S03E01", "E14": "S04E14"})
            evidence = json.loads(
                (followup.directory / "replenishment-episode-evidence.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(evidence["owner_job_id"], previous.id)
            self.assertEqual(len(evidence["files"]), 2)
            starter.assert_called_once_with(server.prepare_job, followup)
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_replenishment_episode_evidence_accepts_unique_quark_long_name_truncation(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            source = "/quark/影视/ScrapeFlow/补源/Example-run"
            owner = server.Job(
                "a" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            owner.directory.mkdir()
            expected = (
                "[Metal] That Time I Got Reincarnated as a Slime the Movie "
                "Scarlet Bond - S00E09 (BD 1920x1080 HEVC OPUS) [301FDCF2].mkv"
            )
            actual = expected[:expected.rfind(" [301FDCF2]")] + "....mkv"
            server._atomic_json(owner.directory / "replenishment-selection.json", {
                "request": {
                    "media": {"tmdb_id": 42},
                    "gaps": [{
                        "id": "S00E09", "kind": "missing_episode",
                        "season": 0, "episodes": [9],
                    }],
                },
                "selection": {"selections": [{"acquisition": {
                    "expected_files": [{
                        "path": expected, "size": 123456789,
                        "gap_ids": ["S00E09"],
                    }],
                }}]},
            })
            server._atomic_json(owner.directory / "replenishment-acquisition.json", {
                "status": "ready", "source_paths": [source],
            })
            evidence = server._selection_episode_evidence(
                owner,
                source,
                owner.directory / "replenishment-selection.json",
                owner.directory / "replenishment-acquisition.json",
                [{
                    "path": source + "/release/" + actual,
                    "size": 123456789,
                }],
                42,
            )
            self.assertIsNotNone(evidence)
            self.assertEqual(evidence["episode_map"], {"SP9": "S00E09"})
            self.assertEqual(evidence["files"][0]["expected_path"], expected)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_replenishment_episode_evidence_rejects_short_ellipsis_alias(self):
        self.assertFalse(server._replenishment_expected_path_matches(
            "release/Example S00E09....mkv",
            "Example S00E09 Full Release Name.mkv",
        ))

    def test_replenishment_episode_evidence_fails_closed_on_unexpected_video(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            server.JOBS = {}
            source = "/quark/影视/ScrapeFlow/补源/Example-run"
            previous = server.Job(
                "7" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            previous.directory.mkdir()
            server.JOBS[previous.id] = previous
            server._atomic_json(previous.directory / "replenishment-selection.json", {
                "request": {
                    "media": {"tmdb_id": 42},
                    "gaps": [{
                        "id": "S03E01", "kind": "missing_episode",
                        "season": 3, "episodes": [1],
                    }],
                },
                "selection": {"selections": [{"acquisition": {"expected_files": [{
                    "path": "Example [01].mkv", "size": 101,
                    "gap_ids": ["S03E01"],
                }]}}]},
            })
            server._atomic_json(previous.directory / "replenishment-acquisition.json", {
                "status": "ready", "source_paths": [source],
            })
            fake_client = types.SimpleNamespace(list=lambda _path, refresh=False: [
                {"name": "Example [01].mkv", "is_dir": False, "size": 101},
                {"name": "Unexpected [02].mkv", "is_dir": False, "size": 102},
            ])
            with mock.patch.object(
                server, "_execution_alist_client", return_value=fake_client,
            ):
                self.assertIsNone(
                    server._replenishment_followup_episode_evidence(previous, source, 42)
                )
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_replenishment_episode_evidence_degrades_when_live_source_is_missing(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            previous = server.Job(
                "6" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            previous.directory.mkdir()
            (previous.directory / "replenishment-selection.json").write_text(
                "{}\n", encoding="utf-8",
            )
            (previous.directory / "replenishment-acquisition.json").write_text(
                "{}\n", encoding="utf-8",
            )
            server.JOBS = {previous.id: previous}
            with mock.patch.object(
                server, "_replenishment_source_video_snapshot",
                side_effect=server.ApiError("AList object not found"),
            ):
                self.assertIsNone(server._replenishment_followup_episode_evidence(
                    previous,
                    "/quark/影视/待刮削/ScrapeFlow补源-Example",
                    42,
                ))
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_delivered_internal_followup_with_consumed_source_reaches_terminal_noop(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            source = "/quark/影视/ScrapeFlow/补源/Example-run"
            root = server.Job(
                "1" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={"replenishment": {
                    "status": "acquired",
                    "source_paths": [source],
                    "followup_job_ids": ["2" * 12],
                }},
            )
            child = server.Job(
                "2" * 12, source, "/quark/影视/番剧",
                "tv", False, True, phase="failed",
                error="AList object not found",
                visibility="internal", root_job_id=root.id,
                progress={"stage": "planning_start", "percent": 5.0},
            )
            root.directory.mkdir()
            child.directory.mkdir()
            server.JOBS = {root.id: root, child.id: child}
            client = types.SimpleNamespace(
                try_list=lambda path, refresh=False: None,
            )
            with mock.patch.object(
                server, "_execution_alist_client", return_value=client,
            ):
                self.assertTrue(
                    server.close_consumed_internal_replenishment_followup(child)
                )
            self.assertEqual(child.phase, "cancelled")
            self.assertIsNone(child.error)
            self.assertEqual(child.progress["stage"], "replenishment_source_consumed")
            self.assertEqual(
                child.plan_summary["replenishment_followup"]["root_job_id"],
                root.id,
            )
            self.assertTrue(server._replenishment_followup_closed(child))
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_consumed_followup_reconciliation_fails_closed_without_both_proofs(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            source = "/quark/影视/ScrapeFlow/补源/Example-run"
            root = server.Job(
                "3" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={"replenishment": {
                    "status": "search_failed",
                    "source_paths": [source],
                    "followup_job_ids": ["4" * 12],
                }},
            )
            child = server.Job(
                "4" * 12, source, "/quark/影视/番剧",
                "tv", False, True, phase="failed", visibility="internal",
                root_job_id=root.id,
            )
            root.directory.mkdir()
            child.directory.mkdir()
            server.JOBS = {root.id: root, child.id: child}
            missing = types.SimpleNamespace(
                try_list=lambda path, refresh=False: None,
            )
            with mock.patch.object(
                server, "_execution_alist_client", return_value=missing,
            ):
                self.assertFalse(
                    server.close_consumed_internal_replenishment_followup(child)
                )
            root.plan_summary["replenishment"]["status"] = "acquired"
            present = types.SimpleNamespace(
                try_list=lambda path, refresh=False: [],
            )
            with mock.patch.object(
                server, "_execution_alist_client", return_value=present,
            ):
                self.assertFalse(
                    server.close_consumed_internal_replenishment_followup(child)
                )
            self.assertEqual(child.phase, "failed")
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_root_monitor_reaudits_consumed_internal_source_without_retry_round(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            source = "/quark/影视/待刮削/ScrapeFlow补源-42-Example"
            root = server.Job(
                "5" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={"replenishment": {
                    "status": "acquired",
                    "source_paths": [source],
                    "followup_job_ids": ["6" * 12],
                }},
            )
            child = server.Job(
                "6" * 12, source, "/quark/影视/番剧",
                "tv", False, True, phase="failed", visibility="internal",
                root_job_id=root.id,
            )
            root.directory.mkdir()
            child.directory.mkdir()
            server.JOBS = {root.id: root, child.id: child}
            client = types.SimpleNamespace(
                try_list=lambda path, refresh=False: None,
            )
            with mock.patch.object(
                server, "_execution_alist_client", return_value=client,
            ), mock.patch.object(
                server, "_schedule_replenishment_retry",
            ) as retry, mock.patch.object(server, "start_thread") as starter:
                server._monitor_replenishment_followups(root, [child.id])
            retry.assert_not_called()
            self.assertEqual(child.phase, "cancelled")
            self.assertEqual(root.phase, "replenishing")
            self.assertEqual(root.replenishment_round, 0)
            self.assertTrue(root.plan_summary["replenishment"]["followup_verified"])
            self.assertEqual(
                root.plan_summary["replenishment"]["status"],
                "post_followup_reaudit",
            )
            self.assertNotIn(
                "followup_job_ids", root.plan_summary["replenishment"],
            )
            self.assertEqual(root.progress["stage"], "title_reaudit")
            starter.assert_called_once_with(server.finalize_media_replenishment, root)
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_resume_closes_missing_internal_source_owned_by_completed_root(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            source = "/quark/影视/ScrapeFlow/补源/Consumed-run"
            root = server.Job(
                "7" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="completed",
                plan_summary={"replenishment": {
                    "status": "acquired", "followup_verified": True,
                }},
            )
            child = server.Job(
                "8" * 12, source, "/quark/影视/番剧",
                "tv", False, True, phase="failed", visibility="internal",
                root_job_id=root.id,
                error="AList object not found",
                progress={"stage": "planning_start", "percent": 5.0},
            )
            root.directory.mkdir()
            child.directory.mkdir()
            server.JOBS = {root.id: root, child.id: child}
            client = types.SimpleNamespace(
                try_list=lambda path, refresh=False: None,
            )
            with mock.patch.object(
                server, "_execution_alist_client", return_value=client,
            ), mock.patch.object(server, "start_thread") as starter:
                server.resume_jobs()
            starter.assert_not_called()
            self.assertEqual(child.phase, "cancelled")
            self.assertEqual(child.progress["stage"], "replenishment_source_consumed")
            self.assertEqual(root.phase, "completed")
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_replenishment_followup_creation_reuses_linked_child_after_restart(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            source = "/quark/影视/待刮削/Example补源"
            root = server.Job(
                "a" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
            )
            child = server.Job(
                "b" * 12, source, "/quark/影视/番剧",
                "tv", False, True, phase="queued", root_job_id=root.id,
            )
            root.directory.mkdir()
            child.directory.mkdir()
            server.JOBS = {root.id: root, child.id: child}
            with mock.patch.object(server, "start_thread") as starter:
                returned = server.create_replenishment_followup(
                    root, source, {}, {"tmdb_id": 42, "title": "Example"},
                )
            self.assertIs(returned, child)
            self.assertEqual(set(server.JOBS), {root.id, child.id})
            starter.assert_not_called()
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_root_reaudits_only_after_followup_reaches_completed(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            root = server.Job(
                "1" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={"replenishment": {
                    "status": "acquired", "followup_job_ids": ["2" * 12],
                    "failed_followup_job_ids": ["2" * 12],
                }},
            )
            child = server.Job(
                "2" * 12, "/quark/影视/待刮削/Example补源", "/quark/影视/番剧",
                "tv", False, True, phase="completed",
                plan_summary={"replenishment": {"status": "no_regular_gaps"}},
            )
            root.directory.mkdir()
            child.directory.mkdir()
            server.JOBS = {root.id: root, child.id: child}
            with mock.patch.object(server, "start_thread") as starter:
                server._monitor_replenishment_followups(root, [child.id])
            self.assertEqual(root.phase, "replenishing")
            self.assertTrue(root.plan_summary["replenishment"]["followup_verified"])
            self.assertNotIn("failed_followup_job_ids", root.plan_summary["replenishment"])
            self.assertEqual(root.progress["stage"], "title_reaudit")
            starter.assert_called_once_with(server.finalize_media_replenishment, root)
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_root_rejects_completed_followup_without_post_scrape_audit_proof(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            root = server.Job(
                "3" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={"replenishment": {
                    "status": "acquired", "followup_job_ids": ["4" * 12],
                }},
            )
            child = server.Job(
                "4" * 12, "/quark/影视/待刮削/Example补源", "/quark/影视/番剧",
                "tv", False, True, phase="completed",
                plan_summary={"replenishment": {"status": "cancelled_after_media_commit"}},
            )
            root.directory.mkdir()
            child.directory.mkdir()
            server.JOBS = {root.id: root, child.id: child}
            with mock.patch.object(
                server, "_schedule_replenishment_retry", return_value=True,
            ) as retry:
                server._monitor_replenishment_followups(root, [child.id])
            self.assertNotEqual(root.phase, "completed")
            self.assertEqual(
                retry.call_args.kwargs["summary"]["replenishment"]["status"],
                "followup_failed",
            )
            retry.assert_called_once()
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_partial_root_retries_remaining_gaps_after_delivered_child_closes(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            root = server.Job(
                "5" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={"replenishment": {
                    "status": "partial", "followup_job_ids": ["6" * 12],
                }},
            )
            child = server.Job(
                "6" * 12, "/quark/影视/待刮削/Example补源", "/quark/影视/番剧",
                "tv", False, True, phase="completed",
                plan_summary={"replenishment": {"status": "no_regular_gaps"}},
            )
            root.directory.mkdir()
            child.directory.mkdir()
            server.JOBS = {root.id: root, child.id: child}
            with mock.patch.object(
                server, "_schedule_replenishment_retry", return_value=True,
            ) as retry:
                server._monitor_replenishment_followups(root, [child.id])
            self.assertNotEqual(root.phase, "completed")
            summary = retry.call_args.kwargs["summary"]
            self.assertEqual(summary["replenishment"]["status"], "partial")
            self.assertTrue(summary["replenishment"]["followup_verified"])
            retry.assert_called_once()
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)


    def test_replenishment_only_failure_is_not_presented_as_completed(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "7" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={}, replenishment_round=1,
            )
            job.directory.mkdir()
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS": "1",
            }, clear=False), mock.patch.object(
                server, "audit_current_job_titles",
                return_value=title_closure_fixture(episode_gaps=[{
                    "kind": "missing_episode", "label": "S01E03",
                }]),
            ), mock.patch.object(
                server, "prepare_post_scrape_replenishment",
                return_value=({"status": "acquire_failed", "message": "no peers"}, [], {}),
            ):
                server.finalize_media_replenishment(job)
            self.assertEqual(job.phase, "failed")
            self.assertEqual(job.error, "no peers")
            self.assertIn("未落地", job.progress["message"])
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_proven_source_exhaustion_waits_and_never_claims_completion(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "8" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={},
            )
            job.directory.mkdir()
            exhausted = {
                "status": "sources_exhausted",
                "message": "已接入的必需来源完整搜索均无候选，三级目录也无剩余候选",
                "gap_count": 2,
                "source_exhaustion": {
                    "kind": "all_required_sources_exhausted",
                },
            }
            with mock.patch.object(
                server, "audit_current_job_titles",
                return_value=title_closure_fixture(episode_gaps=[{
                    "kind": "missing_episode", "label": "S01E03",
                }]),
            ), mock.patch.object(
                server, "prepare_post_scrape_replenishment",
                return_value=(exhausted, [], {}),
            ), mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as review_mock, mock.patch.object(
                server, "remember_completed_job",
            ) as remember_mock:
                server.finalize_media_replenishment(job)

            self.assertEqual(job.phase, "replenishing")
            self.assertIsNone(job.error)
            self.assertEqual(
                job.plan_summary["replenishment"]["status"], "awaiting_sources",
            )
            self.assertEqual(
                job.plan_summary["replenishment"]["business_state"],
                "current_title_source_review",
            )
            self.assertIn("next_review_at", job.plan_summary["replenishment"])
            self.assertEqual(job.progress["stage"], "current_title_source_wait")
            self.assertIn("只重试这个作品", job.progress["message"])
            review_mock.assert_called_once()
            self.assertIs(review_mock.call_args.args[0], job)
            self.assertGreaterEqual(review_mock.call_args.args[1], 1)
            remember_mock.assert_not_called()
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_source_review_wait_is_restored_without_global_sweep(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "c" * 12, "/quark/影视/待刮削/Example",
                "/quark/影视/番剧", "tv", False, True,
                phase="replenishing",
                plan_summary={"replenishment": {
                    "status": "awaiting_sources",
                    "business_state": "current_title_source_review",
                    "next_review_at": "2099-01-01T00:00:00+00:00",
                }},
            )
            job.directory.mkdir()
            server.JOBS = {job.id: job}
            with mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as launch_mock, mock.patch.object(
                server, "start_thread",
            ) as start_mock:
                server.resume_jobs()

            launch_mock.assert_called_once()
            self.assertIs(launch_mock.call_args.args[0], job)
            self.assertGreater(launch_mock.call_args.args[1], 1)
            start_mock.assert_not_called()
            self.assertEqual(job.phase, "replenishing")
            self.assertEqual(job.progress["stage"], "current_title_source_wait")
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_failed_source_wait_resumes_without_round_or_candidate_rotation(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "e" * 12, "/quark/影视/待刮削/Example",
                "/quark/影视/番剧", "tv", False, True,
                phase="failed", replenishment_round=9,
                plan_summary={"replenishment": {
                    "status": "awaiting_sources",
                    "next_review_at": "2099-01-01T00:00:00+00:00",
                }},
                progress={"stage": "replenishment_complete"},
            )
            job.directory.mkdir()
            server._atomic_json(job.directory / "media-journal.json", {"success": True})
            server.JOBS = {job.id: job}
            with mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as review_mock, mock.patch.object(
                server, "_schedule_replenishment_retry",
            ) as source_retry_mock:
                server.resume_jobs()

            review_mock.assert_called_once()
            source_retry_mock.assert_not_called()
            self.assertEqual(job.phase, "replenishing")
            self.assertEqual(job.replenishment_round, 9)
            self.assertFalse(server._replenishment_failure_path(job).exists())
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_post_commit_cancel_never_masquerades_as_title_complete(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "d" * 12, "/quark/影视/待刮削/Example",
                "/quark/影视/番剧", "tv", False, True,
                phase="replenishing",
                plan_summary={"replenishment": {"status": "awaiting_sources"}},
            )
            job.directory.mkdir()
            with mock.patch.object(
                server.SCHEDULER, "cancel_pending", return_value=True,
            ), mock.patch.object(
                server, "remember_completed_job",
            ) as remember_mock:
                server.request_cancel(job)

            self.assertEqual(job.phase, "cancelled")
            self.assertEqual(
                job.plan_summary["replenishment"]["status"],
                "cancelled_after_media_commit",
            )
            self.assertTrue(job.cancel_requested)
            remember_mock.assert_not_called()
            self.assertTrue(any("而不是补齐" in line for line in job.logs))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_post_commit_cancel_intent_is_terminal_after_restart_reconciliation(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            for index, phase in enumerate(("cancelling", "completed"), start=1):
                with self.subTest(phase=phase):
                    job = server.Job(
                        str(index) * 12, "/quark/影视/待刮削/Example",
                        "/quark/影视/番剧", "tv", False, True,
                        phase=phase,
                        plan_summary={"replenishment": {"status": (
                            "cancellation_requested_after_media_commit"
                            if phase == "cancelling"
                            else "cancelled_after_media_commit"
                        )}},
                    )
                    job.directory.mkdir()
                    self.assertTrue(server.reconcile_post_commit_cancel(job))
                    self.assertEqual(job.phase, "cancelled")
                    self.assertTrue(job.cancel_requested)
                    self.assertEqual(
                        job.plan_summary["replenishment"]["status"],
                        "cancelled_after_media_commit",
                    )
            self.assertFalse(server.reconcile_post_commit_cancel(job))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_replenishment_failure_schedules_unattended_retry_when_unbounded(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "e" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={},
            )
            job.directory.mkdir()
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS": "0",
                "SCRAPEFLOW_REPLENISHMENT_RETRY_DELAY": "1",
            }, clear=False), mock.patch.object(
                server, "audit_current_job_titles",
                return_value=title_closure_fixture(episode_gaps=[{
                    "kind": "missing_episode", "label": "S01E03",
                }]),
            ), mock.patch.object(
                server, "prepare_post_scrape_replenishment",
                return_value=({"status": "acquire_failed", "message": "no peers"}, [], {}),
            ), mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as start_mock:
                server.finalize_media_replenishment(job)
            self.assertEqual(job.phase, "replenishing")
            self.assertIsNone(job.error)
            self.assertEqual(job.replenishment_round, 1)
            self.assertEqual(job.progress["stage"], "replenishment_retry_wait")
            self.assertIn("自动", job.progress["message"])
            start_mock.assert_called_once_with(job, 1)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_search_failure_schedules_unattended_retry_when_unbounded(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "1" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={}, replenishment_round=23,
            )
            job.directory.mkdir()
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS": "0",
                "SCRAPEFLOW_REPLENISHMENT_RETRY_DELAY": "1",
            }, clear=False), mock.patch.object(
                server, "audit_current_job_titles",
                return_value=title_closure_fixture(episode_gaps=[{
                    "kind": "missing_episode", "label": "S01E03",
                }]),
            ), mock.patch.object(
                server, "prepare_post_scrape_replenishment",
                return_value=({"status": "search_failed", "message": "provider timeout"}, [], {}),
            ), mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as start_mock:
                server.finalize_media_replenishment(job)
            self.assertEqual(job.phase, "replenishing")
            self.assertEqual(job.replenishment_round, 24)
            self.assertEqual(job.progress["stage"], "replenishment_retry_wait")
            start_mock.assert_called_once()
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_clean_no_match_rotates_in_one_second_but_infrastructure_backs_off(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            clean = server.Job(
                "4" * 12, "/source/clean", "/target", "tv", False, True,
                phase="replenishing", replenishment_round=2,
            )
            blocked = server.Job(
                "5" * 12, "/source/blocked", "/target", "tv", False, True,
                phase="replenishing", replenishment_round=2,
            )
            clean.directory.mkdir()
            blocked.directory.mkdir()
            clean_summary = {"replenishment": {
                "status": "no_match",
                "projects": [{"lane_status": {
                    "quark_share": {"status": "exhausted"},
                    "quark_magnet": {"status": "ready"},
                }}],
            }}
            blocked_summary = {"replenishment": {
                "status": "no_match",
                "projects": [{"lane_status": {
                    "quark_share": {"status": "exhausted"},
                    "quark_magnet": {"status": "infrastructure_failure"},
                }}],
            }}
            with mock.patch.object(
                server, "_automatic_replenishment_retry_allowed", return_value=True,
            ), mock.patch.object(
                server, "_seed_replenishment_failures_from_last_attempt", return_value=[],
            ), mock.patch.object(
                server, "replenishment_retry_delay", return_value=30,
            ), mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as launch:
                self.assertTrue(server._schedule_replenishment_retry(
                    clean, summary=clean_summary,
                ))
                self.assertTrue(server._schedule_replenishment_retry(
                    blocked, summary=blocked_summary,
                ))

            self.assertEqual(clean.progress["message"], "本轮未落地，1 秒后自动更换来源继续查补")
            self.assertEqual(blocked.progress["message"], "本轮未落地，120 秒后自动更换来源继续查补")
            self.assertEqual(
                launch.call_args_list,
                [mock.call(clean, 1), mock.call(blocked, 120)],
            )
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_only_acquisition_failure_can_seed_candidate_quarantine(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)

            def make_job(job_id: str, status: str):
                job = server.Job(
                    job_id, f"/source/{job_id}", "/target", "tv", False, True,
                    phase="replenishing",
                    plan_summary={"replenishment": {"status": status}},
                    error="aria2c 下载失败",
                )
                job.directory.mkdir()
                server._atomic_json(job.directory / "replenishment-selection.json", {
                    "selection": {"selections": [{
                        "provider": "magnet", "release_name": "Example S01E01",
                        "locator": f"torrent:{job_id}", "infohash": job_id,
                    }]},
                })
                job.logs = [
                    "[replenishment] 下载候选 1/1: Example S01E01",
                    "补源适配器失败: aria2c 下载失败",
                ]
                return job

            delivered = make_job("a" * 12, "followup_failed")
            candidate_failed = make_job("b" * 12, "acquire_failed")
            with mock.patch.object(
                server, "_automatic_replenishment_retry_allowed", return_value=True,
            ), mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ):
                self.assertTrue(server._schedule_replenishment_retry(delivered))
                self.assertFalse(server._replenishment_failure_path(delivered).exists())
                self.assertTrue(server._schedule_replenishment_retry(candidate_failed))

            failures = server._load_replenishment_failures(candidate_failed)
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0]["locator"], f"torrent:{'b' * 12}")
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_restart_resumes_search_failure_without_selection_artifact(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "2" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="failed", replenishment_round=9,
                error="provider timeout",
                plan_summary={
                    "replenishment": {"status": "search_failed", "message": "provider timeout"},
                },
                progress={"stage": "replenishment_complete", "percent": 100.0},
            )
            job.directory.mkdir()
            (job.directory / "media-plan.json").write_text("{}\n", encoding="utf-8")
            server.JOBS = {job.id: job}
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS": "0",
            }, clear=False), mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as start_mock:
                server.resume_jobs()
            self.assertFalse(any(job.directory.glob("replenishment-selection*.json")))
            self.assertEqual(job.phase, "replenishing")
            self.assertEqual(job.replenishment_round, 10)
            start_mock.assert_called_once_with(job, 1)
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_unbounded_failure_never_claims_retry_limit_when_retry_is_disabled(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "3" * 12, "/quark/影视/番剧/Example", "/quark/影视/番剧",
                "tv", False, True, phase="replenishing",
                plan_summary={}, replenishment_round=50,
            )
            job.directory.mkdir()
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "0",
                "SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS": "0",
            }, clear=False), mock.patch.object(
                server, "audit_current_job_titles",
                return_value=title_closure_fixture(episode_gaps=[{
                    "kind": "missing_episode", "label": "S01E03",
                }]),
            ), mock.patch.object(
                server, "prepare_post_scrape_replenishment",
                return_value=({"status": "search_failed", "message": "provider timeout"}, [], {}),
            ):
                server.finalize_media_replenishment(job)
            self.assertEqual(job.phase, "failed")
            self.assertEqual(job.error, "provider timeout")
            self.assertNotIn("上限", job.progress["message"])
            self.assertIn("provider timeout", job.progress["message"])
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_legacy_failed_replenishment_without_summary_resumes_automatically(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "9" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="failed", replenishment_round=5,
                error="历史查补未落地: acquire_failed",
                progress={
                    "stage": "replenishment_complete", "completed": 1,
                    "total": 1, "percent": 100.0, "message": "历史查补未落地",
                },
            )
            job.directory.mkdir()
            (job.directory / "media-plan.json").write_text("{}", encoding="utf-8")
            (job.directory / "replenishment-selection.json").write_text(
                '{"selection":{"selections":[]}}', encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_AUTO_REPLENISH_MISSING": "1",
                "SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS": "0",
            }, clear=False), mock.patch.object(
                server, "_launch_delayed_replenishment_retry",
            ) as start_mock:
                self.assertTrue(server._schedule_replenishment_retry(job, restored=True))
            self.assertEqual(job.phase, "replenishing")
            self.assertEqual(job.replenishment_round, 6)
            self.assertEqual(job.progress["stage"], "replenishment_retry_wait")
            start_mock.assert_called_once_with(job, 1)

            ordinary = server.Job(
                "0" * 12, "/quark/影视/待刮削/Other", "/quark/影视/番剧",
                "tv", False, True, phase="failed", error="媒体识别失败",
            )
            ordinary.directory.mkdir()
            self.assertFalse(server._automatic_replenishment_retry_allowed(ordinary))
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)

    def test_failed_auto_plan_envelope_is_repaired_before_any_media_write(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        with tempfile.TemporaryDirectory() as directory:
            server.JOBS_ROOT = Path(directory)
            server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
            job = server.Job(
                "f" * 12, "/quark/影视/待刮削/Example", "/quark/影视/番剧",
                "tv", False, True, phase="failed",
                error="计划字段 created_at 必须是字符串",
            )
            job.approval_source = "auto"
            job.directory.mkdir()
            plan = {
                "mode": "tv", "source_root": job.source,
                "target_root": "/quark/影视/番剧/Example",
                "files": [], "cleanup_files": [], "problem_files": [],
                "notices": [], "scan_report": {},
            }
            server._atomic_json(job.directory / "media-plan.json", {
                "plan": plan, "plan_sha256": server.canonical_digest(plan),
            })
            with mock.patch.object(server, "start_execution") as start_mock:
                self.assertTrue(server._resume_failed_unattended_plan(job))
            wrapper = json.loads(
                (job.directory / "media-plan.json").read_text(encoding="utf-8")
            )
            self.assertEqual(wrapper["schema_version"], 4)
            self.assertIsInstance(wrapper["created_at"], str)
            self.assertEqual(wrapper["plan_sha256"], job.digest)
            self.assertEqual(job.phase, "starting_media_execution")
            self.assertIsNone(job.error)
            self.assertFalse((job.directory / "media-journal.json").exists())
            start_mock.assert_called_once_with(server.execute_approved_media, job, job.digest)
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)












    def test_strict_json_rejects_duplicate_fields(self):
        with self.assertRaisesRegex(ValueError, "重复字段"):
            server.strict_json_loads('{"path":"/one","path":"/two"}')




class LocalHttpSecurityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        cls.host, cls.port = cls.httpd.server_address
        cls.origin = f"http://{cls.host}:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=2)

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection(self.host, self.port, timeout=2)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8")) if response.length != 0 else {}
        connection.close()
        return response.status, payload

    def same_origin_headers(self, *, json_body=False):
        headers = {
            "Origin": self.origin,
            "Sec-Fetch-Site": "same-origin",
        }
        if json_body:
            headers["Content-Type"] = "application/json"
        return headers

    def test_cross_site_get_is_rejected(self):
        status, _ = self.request("GET", "/api/jobs", headers={"Origin": "https://evil.example"})
        self.assertEqual(status, 403)

    def test_different_localhost_port_is_rejected(self):
        status, _ = self.request(
            "GET", "/api/jobs", headers={"Origin": f"http://{self.host}:{self.port + 1}"}
        )
        self.assertEqual(status, 403)

    def test_cross_site_fetch_metadata_is_rejected_without_origin(self):
        status, _ = self.request(
            "GET", "/api/jobs", headers={"Sec-Fetch-Site": "Cross-Site"},
        )
        self.assertEqual(status, 403)

    def test_non_loopback_host_is_rejected(self):
        status, _ = self.request(
            "GET", "/api/jobs", headers={"Host": "scrapeflow.example"},
        )
        self.assertEqual(status, 403)

    def test_originless_loopback_health_check_is_allowed(self):
        with mock.patch.object(server, "health_payload", return_value={"connected": True}):
            status, payload = self.request("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(payload["connected"])

    def test_same_origin_jobs_do_not_require_session_token(self):
        with mock.patch.object(server, "JOBS", {}):
            status, payload = self.request(
                "GET", "/api/jobs", headers=self.same_origin_headers(),
            )
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"jobs": []})

    def test_session_and_cors_preflight_endpoints_are_removed(self):
        status, _ = self.request(
            "GET", "/api/session", headers=self.same_origin_headers(),
        )
        self.assertEqual(status, 404)
        self.assertNotIn("do_OPTIONS", server.Handler.__dict__)

    def test_post_and_delete_apply_same_origin_gate_before_dispatch(self):
        with mock.patch.object(server, "create_job") as create, mock.patch.object(
            server, "clear_local_task_data",
        ) as clear:
            status, _ = self.request(
                "POST", "/api/jobs", body='{"path":"/media/show"}',
                headers={
                    "Origin": "https://evil.example",
                    "Content-Type": "application/json",
                },
            )
            self.assertEqual(status, 403)
            create.assert_not_called()
            status, _ = self.request(
                "DELETE", "/api/jobs", headers={"Origin": "https://evil.example"},
            )
            self.assertEqual(status, 403)
            clear.assert_not_called()

    def test_internal_followup_is_hidden_from_list_but_remains_auditable_by_id(self):
        user_job = server.Job(
            "d" * 12, "/quark/影视/待刮削/User", "/quark/影视/番剧",
            "tv", False, True,
        )
        internal_job = server.Job(
            "e" * 12, "/quark/影视/ScrapeFlow/补源/Internal",
            "/quark/影视/番剧", "tv", False, True, visibility="internal",
        )
        with mock.patch.object(server, "JOBS", {
            user_job.id: user_job, internal_job.id: internal_job,
        }):
            headers = self.same_origin_headers()
            status, payload = self.request("GET", "/api/jobs", headers=headers)
            self.assertEqual(status, 200)
            self.assertEqual([job["id"] for job in payload["jobs"]], [user_job.id])
            self.assertEqual(set(payload), {"jobs"})
            status, payload = self.request(
                "GET", f"/api/jobs/{internal_job.id}", headers=headers,
            )
            self.assertEqual(status, 200)
            self.assertEqual(payload["job"]["id"], internal_job.id)
            self.assertEqual(payload["job"]["visibility"], "internal")
            status, _ = self.request("GET", "/api/history-recovery", headers=headers)
            self.assertEqual(status, 404)

    def test_retired_global_audit_routes_are_absent(self):
        for path in ("/api/audit", "/api/audit?refresh=1"):
            with self.subTest(path=path):
                status, _payload = self.request(
                    "GET", path, headers=self.same_origin_headers(),
                )
                self.assertEqual(status, 404)

    def test_write_rejects_non_json(self):
        status, payload = self.request(
            "POST",
            "/api/jobs",
            body='{"path":"/media/show"}',
            headers={
                "Origin": self.origin,
                "Sec-Fetch-Site": "same-origin",
                "Content-Type": "text/plain",
            },
        )
        self.assertEqual(status, 400)
        self.assertIn("Content-Type", payload["error"])

    def test_write_rejects_duplicate_json_fields(self):
        status, payload = self.request(
            "POST",
            "/api/jobs",
            body='{"path":"/media/one","path":"/media/two"}',
            headers=self.same_origin_headers(json_body=True),
        )
        self.assertEqual(status, 400)
        self.assertIn("重复字段", payload["error"])

    def test_duplicate_job_post_returns_409_with_existing_job(self):
        existing = server.Job(
            "d" * 12, "/quark/影视/待刮削/作品", "/quark/影视/番剧",
            "auto", False, True,
        )
        with mock.patch.object(
            server, "create_job", side_effect=server.ExistingJobConflict(existing)
        ):
            status, payload = self.request(
                "POST", "/api/jobs",
                body=json.dumps({"path": existing.source, "category": "番剧"}),
                headers=self.same_origin_headers(json_body=True),
            )
        self.assertEqual(status, 409)
        self.assertEqual(payload["job"]["id"], existing.id)



    def test_subtitle_executor_runtime_persists_digest_gated_retryable_status(self):
        payload = (
            "[Script Info]\nScriptType: v4.00+\n[Events]\n"
            + "Dialogue: 0,0:00:00.00,0:00:02.00,Default,,0,0,0,,"
            "这是一条简体中文字幕。\n" * 20
        ).encode()
        payload_sha = __import__("hashlib").sha256(payload).hexdigest()
        request_rows = [
            {"request_id": "request-one", "video_path": "/quark/影视/番剧/A/A.mkv", "lane": "ensure_external_zh_CN"},
            {"request_id": "request-two", "video_path": "/quark/影视/番剧/B/B.mkv", "lane": "ensure_external_zh_CN"},
        ]
        request_core = {
            "schema_version": 1, "kind": "subtitle_requests", "requests": request_rows,
        }
        request_sha = server.canonical_digest(request_core)
        candidate_path = "/quark/影视/字幕候选/A.ass"
        core = {
            "schema_version": 1,
            "kind": "subtitle_selection",
            "request_sha256": request_sha,
            "selections": [{
                "request_id": "request-one",
                "lane": "ensure_external_zh_CN",
                "target_path": "/quark/影视/番剧/A/A.zh-CN.ass",
                "candidate_path": candidate_path,
                "candidate_source_kind": "alist",
                "payload_sha256": payload_sha,
            }],
            "acquisition_requests": [],
            "failures": [{
                "request_id": "request-two",
                "video_path": "/quark/影视/番剧/B/B.mkv",
                "status": "no_verified_zh_CN_candidate",
                "isolated": True,
            }],
        }
        selection = {**core, "selection_sha256": server.canonical_digest(core)}
        prepared = {
            "requests": {
                **request_core, "request_sha256": request_sha,
            },
            "selection": selection,
            "inventory_scan_failures": [],
            "inventory_missing_optional_roots": ["/quark/影视/ScrapeFlow/验证"],
        }

        class FakeClient:
            def __init__(self):
                self.uploads = []
                self.files = {candidate_path: payload}
            def try_list(self, *_args, **_kwargs): return []
            def read_file_bytes(self, path, *_args, **_kwargs): return self.files[path]
            def exact_file_info(self, path):
                data = self.files.get(path)
                return None if data is None else {
                    "size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                }
            def open_file_reader(self, path): return io.BytesIO(self.files[path])
            def upload_file(self, path, source, _content_type):
                data = source.read_bytes()
                self.files[path] = data
                self.uploads.append((path, data))
            def mkdir(self, _path): return None
            def remove(self, parent, names):
                for name in names:
                    self.files.pop(posixpath.join(parent, name), None)

        client = FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "title-subtitles"
            with mock.patch.object(server, "_require_subtitle_dispatch_open"), \
                    mock.patch.object(server, "_subtitle_execution_guard", side_effect=lambda: __import__("contextlib").nullcontext()):
                result = server.execute_prepared_title_subtitles(
                    client, prepared, execution_root=root,
                )
                again = server.execute_prepared_title_subtitles(
                    client, prepared, execution_root=root,
                )
                tampered = json.loads(json.dumps(prepared))
                tampered["requests"]["requests"][0]["video_path"] = "/quark/影视/番剧/X/X.mkv"
                with self.assertRaisesRegex(ValueError, "request 摘要无效"):
                    server.execute_prepared_title_subtitles(
                        client, tampered, execution_root=root,
                    )
            self.assertEqual(result["status"], "retryable_unresolved")
            self.assertEqual(result["created_count"], 1)
            self.assertEqual(result["unmatched_retryable_count"], 1)
            self.assertEqual(result["unresolved_action_count"], 1)
            self.assertEqual(result["video_mutations"], 0)
            self.assertEqual(len(client.uploads), 1)
            self.assertEqual(again["created_count"], 1)
            self.assertTrue((root / "execution-result.json").exists())

    def test_subtitle_member_runtime_persists_retryable_search_without_fake_execution(self):
        request_core = {
            "schema_version": 1, "kind": "subtitle_requests", "requests": [{
                "request_id": "request-one", "video_path": "/quark/影视/番剧/A/A - S01E01.mkv",
                "target_root": "/quark/影视/番剧/A", "title": "A Show",
                "media_type": "tv", "season": 1, "episodes": [1],
                "lane": "ensure_external_zh_CN",
            }],
        }
        requests = {**request_core, "request_sha256": server.canonical_digest(request_core)}
        selection = {
            "selection_sha256": "b" * 64,
            "failures": [{
                "request_id": "request-one", "status": "no_verified_zh_CN_candidate",
            }],
        }
        prepared = {"requests": requests, "selection": selection}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            discovery = mock.Mock()
            discovery.enqueue.return_value = {"created": 1, "existing": 0}
            discovery.snapshot.return_value = {"total": 1, "status_counts": {"pending": 1}, "running": False}
            with mock.patch.object(server, "SUBTITLE_SOURCE_MANIFEST_ROOT", root / "sources"), \
                    mock.patch.object(server, "SUBTITLE_MEMBER_CACHE_ROOT", root / "cache"), \
                    mock.patch.object(server, "SUBTITLE_SOURCE_DISCOVERY_RUNTIME", discovery), \
                    mock.patch.object(server, "_require_subtitle_dispatch_open"), \
                    mock.patch.object(server, "_subtitle_execution_guard", side_effect=lambda: __import__("contextlib").nullcontext()), \
                    mock.patch.object(server, "_fetch_background_subtitle_member") as fetch:
                member_prepared = server.prepare_subtitle_member_acquisition(prepared)
                result = server.execute_prepared_subtitle_member_acquisition(
                    object(), prepared, member_prepared, run_root=root / "run",
                )
            self.assertEqual(result["status"], "retryable_search_required")
            self.assertEqual(result["plan_summary"]["unmatched_requests"], 1)
            self.assertEqual(result["plan_summary"]["planned_requests"], 0)
            self.assertEqual(result["resolved_count"], 0)
            self.assertEqual(result["video_members_selected"], 0)
            self.assertEqual(result["quark_ui_operations"], 0)
            fetch.assert_not_called()
            plan_files = list((root / "run/member-acquisition").glob("*/subtitle-member-plan.json"))
            self.assertEqual(len(plan_files), 1)

    def test_subtitle_member_runtime_promotes_verified_cache_once_without_ui(self):
        from engine.scrapeflow.subtitle_member_acquisition import bind_source_manifest

        payload = (
            "[Script Info]\nScriptType: v4.00+\n[Events]\n"
            + "Dialogue: 0,0:00:00.00,0:00:02.00,Default,,0,0,0,,"
            "这是一条简体中文字幕。\n" * 20
        ).encode()
        video_path = "/quark/影视/番剧/My Show/Season 01/My Show - S01E02.mkv"
        request_core = {
            "schema_version": 1, "kind": "subtitle_requests", "requests": [{
                "request_id": "req1", "video_path": video_path,
                "target_root": "/quark/影视/番剧/My Show", "title": "My Show",
                "media_type": "tv", "season": 1, "episodes": [2],
                "lane": "ensure_external_zh_CN",
            }],
        }
        requests = {**request_core, "request_sha256": server.canonical_digest(request_core)}
        selection = {
            "selection_sha256": "c" * 64,
            "failures": [{
                "request_id": "req1", "status": "no_verified_zh_CN_candidate",
            }],
        }
        prepared = {"requests": requests, "selection": selection}
        manifest = bind_source_manifest(
            provider="quark_share", locator="quark_share:share", release_name="My Show S01E02",
            search_request_ids=["req1"], acquisition={"share_id": "share"}, files=[
                {"path": "pack/My Show - S01E02.mkv", "size": 1000, "file_id": "video"},
                {"path": "pack/My Show - S01E02.ass", "size": len(payload), "file_id": "subtitle"},
            ],
        )

        class FakeClient:
            def __init__(self):
                self.uploads = []
                self.files = {}
            def try_list(self, *_args, **_kwargs): return []
            def exact_file_info(self, path):
                data = self.files.get(path)
                return None if data is None else {
                    "size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                }
            def open_file_reader(self, path): return io.BytesIO(self.files[path])
            def upload_file(self, path, source, _content_type):
                data = source.read_bytes()
                self.files[path] = data
                self.uploads.append((path, data))
            def mkdir(self, _path): return None
            def remove(self, parent, names):
                for name in names:
                    self.files.pop(posixpath.join(parent, name), None)

        client = FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); sources = root / "sources"; sources.mkdir()
            server._atomic_json(sources / "share.json", manifest)
            discovery = mock.Mock()
            discovery.enqueue.return_value = {"created": 1, "existing": 0}
            discovery.snapshot.return_value = {"total": 1, "status_counts": {"pending": 1}, "running": False}
            with mock.patch.object(server, "SUBTITLE_SOURCE_MANIFEST_ROOT", sources), \
                    mock.patch.object(server, "SUBTITLE_MEMBER_CACHE_ROOT", root / "cache"), \
                    mock.patch.object(server, "SUBTITLE_SOURCE_DISCOVERY_RUNTIME", discovery), \
                    mock.patch.object(server, "_require_subtitle_dispatch_open"), \
                    mock.patch.object(server, "_subtitle_execution_guard", side_effect=lambda: __import__("contextlib").nullcontext()), \
                    mock.patch.object(server, "_fetch_background_subtitle_member", return_value=payload) as fetch:
                member_prepared = server.prepare_subtitle_member_acquisition(prepared)
                first = server.execute_prepared_subtitle_member_acquisition(
                    client, prepared, member_prepared, run_root=root / "run",
                )
                second = server.execute_prepared_subtitle_member_acquisition(
                    client, prepared, member_prepared, run_root=root / "run",
                )
            self.assertEqual(first["status"], "promoted")
            self.assertEqual(first["resolved_request_ids"], ["req1"])
            self.assertEqual(second["resolved_request_ids"], ["req1"])
            self.assertEqual(len(client.uploads), 1)
            self.assertEqual(client.uploads[0][0], video_path[:-4] + ".zh-CN.ass")
            self.assertEqual(fetch.call_count, 1)
            self.assertEqual(first["video_members_selected"], 0)
            self.assertEqual(first["quark_ui_operations"], 0)

    def test_background_quark_subtitle_fetch_uses_http_bridge_not_native_helper(self):
        payload = ("这是简体中文字幕。\n" * 20).encode()
        item = {
            "request_id": "req", "lane": "ensure_external_zh_CN",
            "provider": "quark_share",
            "release_name": "Example", "source_manifest_sha256": "b" * 64,
            "include_video": False,
            "subtitle_member": {
                "path": "pack/Example - S01E01.ass", "size": len(payload),
                "file_id": "subtitle",
            },
            "transport": {
                "kind": "quark_fast_save", "share_id": "share",
                "share_url": "https://pan.quark.cn/s/share", "passcode": "",
                "selected_file_ids": ["subtitle"],
                "selected_member_paths": ["pack/Example - S01E01.ass"],
                "background_ready_session_required": True,
                "may_launch_or_restart_quark": False, "allow_ui_activation": False,
            },
        }

        class Client:
            def __init__(self): self.mkdirs = []
            def mkdir(self, path): self.mkdirs.append(path)
            def try_list(self, *_args, **_kwargs): return []
            def read_file_bytes(self, *_args, **_kwargs): return payload

        class Bridge:
            def __init__(self, transport): self.transport = transport; self.executed = 0
            def dry_run(self, _selection, destination):
                return {"destination": destination, "expected_files": [{
                    "name": "Example - S01E01.ass", "size": len(payload),
                }]}
            def execute(self, *_args, **_kwargs): self.executed += 1

        client = Client()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            server, "SUBTITLE_MEMBER_ACQUISITION_ROOT", Path(directory),
        ), mock.patch(
            "engine.scrapeflow.quark_fast_save_bridge.QuarkFastSaveBridge", Bridge,
        ), mock.patch(
            "engine.scrapeflow.quark_fast_save_bridge.UrlLibQuarkTransport",
            return_value=object(),
        ) as http_transport, mock.patch(
            "engine.scrapeflow.quark_fast_save_bridge.delegated_quark_session",
            return_value=object(),
        ), mock.patch(
            "engine.scrapeflow.quark_fast_save_bridge.QuarkNativeHelperTransport",
        ) as native, mock.patch.object(
            server, "_await_background_subtitle_arrival",
            return_value="/quark/fixture/Example - S01E01.ass",
        ):
            fetched = server._fetch_background_subtitle_member(
                client, item, plan_sha256="a" * 64,
                member_plan={"plan_sha256": "a" * 64, "acquisitions": []},
            )
        self.assertEqual(fetched, payload)
        self.assertEqual(len(client.mkdirs), 1)
        http_transport.assert_called_once()
        native.assert_not_called()

    def test_background_torrent_subtitle_uses_passive_helper_and_exact_cloud_member(self):
        payload = ("这是简体中文字幕。\n" * 20).encode()
        item = {
            "request_id": "req", "lane": "ensure_external_zh_CN",
            "provider": "torrent",
            "release_name": "Example", "source_manifest_sha256": "b" * 64,
            "include_video": False,
            "subtitle_member": {
                "path": "pack/Example - S01E01.ass", "size": len(payload),
                "torrent_index": 2,
            },
            "transport": {
                "kind": "quark_magnet_subtitle_member",
                "magnet_url": "magnet:?xt=urn:btih:" + "c" * 40,
                "torrent_url": "https://example.invalid/a.torrent",
                "selected_torrent_indices": [2],
                "selected_member_paths": ["pack/Example - S01E01.ass"],
                "background_ready_session_required": True,
                "may_launch_or_restart_quark": False, "allow_ui_activation": False,
            },
        }

        class Client:
            def mkdir(self, _path): return None
            def read_file_bytes(self, *_args, **_kwargs): return payload

        class Bridge:
            def __init__(self, transport): self.transport = transport
            def execute(self, _selection, _destination, _session, **kwargs):
                kwargs["on_submitted"]("task")
                return {"task_id": "task"}

        passive_kwargs = {}

        def transport(_url, _token, **kwargs):
            passive_kwargs.update(kwargs); return object()

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            server, "SUBTITLE_MEMBER_ACQUISITION_ROOT", Path(directory),
        ), mock.patch.object(
            server, "_passive_quark_helper_health",
            side_effect=[{"native_ready": True, "quark_pids": [77]},
                         {"native_ready": True, "quark_pids": [77]}],
        ), mock.patch.object(
            server, "_await_background_subtitle_arrival",
            return_value="/quark/stage/Example - S01E01.ass",
        ), mock.patch.dict(os.environ, {
            "SCRAPEFLOW_QUARK_HELPER_URL": "http://host.docker.internal:18765",
            "SCRAPEFLOW_QUARK_HELPER_TOKEN": "x" * 32,
        }), mock.patch(
            "engine.scrapeflow.quark_fast_save_bridge.QuarkNativeHelperTransport",
            side_effect=transport,
        ), mock.patch(
            "engine.scrapeflow.quark_fast_save_bridge.QuarkMagnetOfflineBridge", Bridge,
        ), mock.patch(
            "engine.scrapeflow.quark_fast_save_bridge.delegated_quark_session",
            return_value=object(),
        ), mock.patch(
            "engine.scrapeflow.subtitle_member_acquisition.fetch_torrent_subtitle_member_after_cloud_exhaustion",
        ) as local_fallback:
            fetched = server._fetch_background_subtitle_member(
                Client(), item, plan_sha256="a" * 64,
                member_plan={"plan_sha256": "a" * 64, "acquisitions": []},
            )
            journal = next(Path(directory).glob("cloud-provider-journals/**/*.json"))
            state = json.loads(journal.read_text())
        self.assertEqual(fetched, payload)
        self.assertTrue(passive_kwargs["passive_only"])
        self.assertEqual(state["status"], "complete")
        self.assertEqual(state["records"][-1]["quark_pids_before"], [77])
        local_fallback.assert_not_called()

    def test_local_torrent_unlock_requires_both_cloud_lane_journals(self):
        plan_sha, request_id = "a" * 64, "req"
        share_digest, torrent_digest = "b" * 64, "c" * 64
        item = {
            "request_id": request_id,
            "source_manifest_sha256": torrent_digest,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            member_plan = {
                "plan_sha256": plan_sha,
                "acquisitions": [
                    {"request_id": request_id, "provider": "quark_share", "source_manifest_sha256": share_digest},
                    {"request_id": request_id, "provider": "torrent", "source_manifest_sha256": torrent_digest},
                ],
            }
            journals = root / "members/cloud-provider-journals" / plan_sha / request_id
            journals.mkdir(parents=True)
            share = {
                "provider": "quark_share", "source_manifest_sha256": share_digest,
                "resource_failures": 30, "infrastructure_failures": 0,
            }
            magnet = {
                "provider": "quark_magnet", "source_manifest_sha256": torrent_digest,
                "plan_sha256": plan_sha,
                "resource_failures": 30, "infrastructure_failures": 0,
            }
            (journals / f"{share_digest}.json").write_text(json.dumps(share))
            (journals / f"{torrent_digest}.json").write_text(json.dumps(magnet))
            queue = root / "members/discovery-queue"; queue.mkdir(parents=True)
            (queue / "task.json").write_text(json.dumps({
                "status": "completed", "batch": {"request_ids": [request_id]},
                "provider_telemetry": {"quark_share": {"search_complete": True}},
            }))
            with mock.patch.object(server, "SUBTITLE_MEMBER_ACQUISITION_ROOT", root / "members"):
                proof = server._local_subtitle_cloud_exhaustion_proof(item, magnet, member_plan)
                self.assertIsNotNone(proof)
                self.assertEqual(proof["quark_share_candidates_resource_failed"], 1)
                share["infrastructure_failures"] = 1
                (journals / f"{share_digest}.json").write_text(json.dumps(share))
                self.assertIsNone(server._local_subtitle_cloud_exhaustion_proof(item, magnet, member_plan))

                # A historical infrastructure outage never counts toward the
                # floor, but it must not permanently poison a provider that
                # later yields a full consecutive window of resource failures.
                share["records"] = [
                    {"status": "infrastructure_failed"},
                    *({"status": "resource_failed"} for _ in range(30)),
                ]
                magnet["records"] = [
                    {"status": "infrastructure_failed"},
                    *({"status": "resource_failed"} for _ in range(30)),
                ]
                magnet["infrastructure_failures"] = 1
                (journals / f"{share_digest}.json").write_text(json.dumps(share))
                (journals / f"{torrent_digest}.json").write_text(json.dumps(magnet))
                self.assertIsNotNone(
                    server._local_subtitle_cloud_exhaustion_proof(item, magnet, member_plan),
                )
                share["records"].append({"status": "infrastructure_failed"})
                share["infrastructure_failures"] = 2
                (journals / f"{share_digest}.json").write_text(json.dumps(share))
                self.assertIsNone(
                    server._local_subtitle_cloud_exhaustion_proof(item, magnet, member_plan),
                )




    def test_successful_journal_survives_two_restarts_without_round_or_log_pollution(self):
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        try:
            with tempfile.TemporaryDirectory() as directory:
                server.JOBS_ROOT = Path(directory)
                server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
                server.JOBS = {}
                job = server.Job(
                    "a1" * 6, "/quark/影视/待刮削/Committed",
                    "/quark/影视/番剧", "tv", False, True,
                    phase="executing_media", replenishment_round=17,
                )
                job.directory.mkdir()
                plan = {
                    "mode": "tv", "source_root": job.source,
                    "target_root": "/quark/影视/番剧/Committed",
                    "metadata": {"tmdb_id": 123, "title": "Committed"},
                }
                server._atomic_json(job.directory / "media-plan.json", {
                    "plan": plan, "plan_sha256": server.canonical_digest(plan),
                })
                server._atomic_json(job.directory / "media-journal.json", {
                    "success": True, "plan_sha256": server.canonical_digest(plan),
                    "records": [],
                })
                server.persist_job(job)

                with mock.patch.object(
                    server, "scrape_first_gate_evidence",
                    return_value={
                        "ready": True, "status": "ready", "blocker_count": 0,
                        "blockers": [], "snapshot_sha256": "a" * 64,
                    },
                ), mock.patch.object(server, "start_thread") as starter, mock.patch.object(
                    server, "prepare_recovery", side_effect=AssertionError("must not recover success"),
                ):
                    server.restore_jobs()
                    server.resume_jobs()
                    first = server.JOBS[job.id]
                    self.assertEqual(first.phase, "replenishing")
                    self.assertEqual(first.replenishment_round, 17)
                    first_log = first.log_path.read_text(encoding="utf-8")
                    self.assertEqual(first_log.count("journal 已完整成功"), 1)

                    server.restore_jobs()
                    server.resume_jobs()
                    second = server.JOBS[job.id]
                    second_log = second.log_path.read_text(encoding="utf-8")
                    self.assertEqual(second.phase, "replenishing")
                    self.assertEqual(second.replenishment_round, 17)
                    self.assertEqual(second_log, first_log)
                    self.assertEqual(starter.call_count, 2)
        finally:
            server.JOBS = previous_jobs
            server.JOBS_ROOT = previous_root
            server.Job.root_provider = staticmethod(previous_provider)

    def test_global_pause_is_persistent_and_gates_without_cancelling_jobs(self):
        previous_control = server.GLOBAL_CONTROL
        previous_scheduler = server.SCHEDULER
        try:
            with tempfile.TemporaryDirectory() as directory:
                control_path = Path(directory) / "global-control.json"
                server.GLOBAL_CONTROL = server.PersistentGlobalControl(control_path)
                scheduler = server.FifoScheduler(
                    analysis_workers=1, execution_workers=1,
                    pause_reader=lambda: server.GLOBAL_CONTROL.paused,
                )
                server.SCHEDULER = scheduler
                job = server.Job(
                    "d1" * 6, "/source", "/target", "tv", False, True,
                )
                ran = threading.Event()
                scheduler.start(lambda target, queued_job, *args: target(queued_job, *args))
                status = server.set_global_pause(True, reason="operator hold")
                scheduler.submit("analysis", job, lambda _job: ran.set())
                self.assertTrue(status["paused"])
                self.assertFalse(job.cancel_requested)
                self.assertEqual(job.phase, "queued")
                self.assertFalse(ran.wait(0.05))

                reloaded = server.PersistentGlobalControl(control_path)
                self.assertTrue(reloaded.paused)
                self.assertEqual(reloaded.snapshot()["reason"], "operator hold")
                resumed = server.set_global_pause(False)
                self.assertFalse(resumed["paused"])
                self.assertTrue(ran.wait(1))
                self.assertFalse(server.PersistentGlobalControl(control_path).paused)
                bootstrap_path = Path(directory) / "bootstrap-control.json"
                bootstrap = server.PersistentGlobalControl(
                    bootstrap_path, default_paused=True,
                )
                self.assertTrue(bootstrap.paused)
                bootstrap.set_paused(False)
                self.assertFalse(server.PersistentGlobalControl(
                    bootstrap_path, default_paused=True,
                ).paused)
                scheduler.stop()
        finally:
            server.GLOBAL_CONTROL = previous_control
            server.SCHEDULER = previous_scheduler

    def test_global_pause_and_resume_http_api_require_confirmation_and_persist(self):
        headers = self.same_origin_headers(json_body=True)
        previous_control = server.GLOBAL_CONTROL
        previous_scheduler = server.SCHEDULER
        try:
            with tempfile.TemporaryDirectory() as directory:
                server.GLOBAL_CONTROL = server.PersistentGlobalControl(
                    Path(directory) / "global-control.json"
                )
                server.SCHEDULER = server.FifoScheduler(
                    analysis_workers=1, execution_workers=1,
                    pause_reader=lambda: server.GLOBAL_CONTROL.paused,
                )
                status, payload = self.request(
                    "POST", "/api/control/pause", body="{}", headers=headers,
                )
                self.assertEqual(status, 400)
                self.assertIn("明确确认", payload["error"])

                status, payload = self.request(
                    "POST", "/api/control/pause",
                    body=json.dumps({"confirm": True, "reason": "maintenance"}),
                    headers=headers,
                )
                self.assertEqual(status, 200)
                self.assertTrue(payload["paused"])
                self.assertNotIn("scheduler_paused", payload)

                status, payload = self.request("GET", "/api/control", headers=headers)
                self.assertEqual(status, 200)
                self.assertTrue(payload["paused"])

                status, payload = self.request(
                    "POST", "/api/control/resume",
                    body=json.dumps({"confirm": True}), headers=headers,
                )
                self.assertEqual(status, 200)
                self.assertFalse(payload["paused"])
                self.assertNotIn("scheduler_paused", payload)
        finally:
            server.GLOBAL_CONTROL = previous_control
            server.SCHEDULER = previous_scheduler

    def test_shutdown_gate_is_process_local_and_never_reported_as_pause(self):
        previous_control = server.GLOBAL_CONTROL
        previous_shutdown = server.SHUTDOWN_EVENT.is_set()
        try:
            with tempfile.TemporaryDirectory() as directory:
                control_path = Path(directory) / "global-control.json"
                server.GLOBAL_CONTROL = server.PersistentGlobalControl(control_path)
                server.SHUTDOWN_EVENT.set()

                status = server.global_control_status()

                self.assertFalse(status["paused"])
                self.assertNotIn("scheduler_paused", status)
                self.assertTrue(server._remote_dispatch_closed())
                self.assertFalse(server.PersistentGlobalControl(control_path).paused)
        finally:
            server.GLOBAL_CONTROL = previous_control
            if previous_shutdown:
                server.SHUTDOWN_EVENT.set()
            else:
                server.SHUTDOWN_EVENT.clear()

    def test_replenishment_stage_boundary_waits_for_global_resume(self):
        job = server.Job(
            id="0123456789ab", source="/quark/影视/番剧/例子",
            parent="/quark/影视/番剧", media_type="tv",
            absolute=False, prefer_simplified=True,
        )
        control = mock.Mock(paused=True)
        previous_control = server.GLOBAL_CONTROL
        finished = threading.Event()
        try:
            server.GLOBAL_CONTROL = control

            thread = threading.Thread(
                target=lambda: (
                    server._raise_if_replenishment_maintenance_stopped(job),
                    finished.set(),
                ),
                daemon=True,
            )
            thread.start()
            time.sleep(0.15)
            self.assertFalse(finished.is_set())
            control.paused = False
            thread.join(timeout=1)
            self.assertTrue(finished.is_set())
        finally:
            server.GLOBAL_CONTROL = previous_control

    def test_replenishment_stage_boundary_parks_on_shutdown_while_paused(self):
        job = server.Job(
            id="0123456789ab", source="/quark/影视/番剧/例子",
            parent="/quark/影视/番剧", media_type="tv",
            absolute=False, prefer_simplified=True,
        )
        control = mock.Mock(paused=True)
        previous_control = server.GLOBAL_CONTROL
        errors: list[BaseException] = []
        try:
            server.GLOBAL_CONTROL = control

            def wait_boundary() -> None:
                try:
                    server._raise_if_replenishment_maintenance_stopped(job)
                except BaseException as exc:  # test captures worker outcome
                    errors.append(exc)

            thread = threading.Thread(target=wait_boundary, daemon=True)
            thread.start()
            time.sleep(0.15)
            job.maintenance_stop_requested = True
            thread.join(timeout=1)
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], server.ReplenishmentMaintenanceStop)
        finally:
            server.GLOBAL_CONTROL = previous_control

    def test_retired_global_convergence_and_sweep_routes_are_absent(self):
        headers = self.same_origin_headers(json_body=True)
        for path in ("/api/convergence", "/api/replenishment/sweep"):
            with self.subTest(method="GET", path=path):
                status, _payload = self.request("GET", path, headers=headers)
                self.assertEqual(status, 404)
            with self.subTest(method="POST", path=path):
                status, _payload = self.request(
                    "POST", path, body=json.dumps({"confirm": True}), headers=headers,
                )
                self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
