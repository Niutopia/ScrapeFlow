"""Tests for the small Engine planner/executor bridge."""

from __future__ import annotations

from dataclasses import replace
import json
import posixpath
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.scrapeflow.core import build_tv_plan, parse_ep_files, validate_plan
from engine.scrapeflow.current_plan import finalize_plan, plan_from_dict, plan_to_dict
from engine.scrapeflow.errors import PlanError
from engine.scrapeflow.media_quality import (
    ABSOLUTE_MINIMUM_VIDEO_BYTES,
    is_production_test_media_path,
    minimum_video_bytes,
)
from engine.scrapeflow.models import Plan, PlannedCleanup, PlannedFile, PlannedProblem
from engine.scrapeflow.residual_policy import (
    REBUILDABLE_STAGING_TEMP_CLEANUP_REASON,
    classify_residual,
    cleanup_allowlist_reason,
)
from engine.scraper import planned_artwork, planned_nfos
from engine.scrapeflow.serialization import atomic_write_json
from local.scrapeflow_api.simple_engine_runner import (
    AutomaticIdentity,
    EngineExecutionError,
    EngineRequest,
    EngineRequestError,
    EngineWorkerBusyError,
    SimpleEngineRunner,
    SimplePlanExecutor,
)
from engine.tools._replenishment_local_adapter_impl import (
    ReplenishmentCandidateError,
    _verify_video_payload,
)


# The production executor deliberately refuses tiny ``.mkv`` fixtures. Keep
# ordinary move/recovery tests representative of an admissible media object;
# dedicated tests below exercise the rejection boundary.
FAKE_VIDEO_BYTES = b"v" * (1024 * 1024)
FAKE_VIDEO_SIZE = len(FAKE_VIDEO_BYTES)


class FakeAList:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.moves: list[tuple[str, str, list[str]]] = []
        self.renames: list[tuple[str, str]] = []
        self.uploads: list[str] = []

    def exact_file_info(self, path: str) -> dict[str, object] | None:
        value = self.files.get(path)
        return None if value is None else {"size": len(value)}

    def mkdir(self, _path: str) -> None:
        return

    def ensure_directory(self, _path: str) -> None:
        return

    def move(self, source_dir: str, target_dir: str, names: list[str]) -> None:
        self.moves.append((source_dir, target_dir, list(names)))
        for name in names:
            source = f"{source_dir.rstrip('/')}/{name}"
            target = f"{target_dir.rstrip('/')}/{name}"
            self.files[target] = self.files.pop(source)

    def rename(self, full_path: str, new_name: str) -> None:
        self.renames.append((full_path, new_name))
        parent = full_path.rsplit("/", 1)[0]
        self.files[f"{parent}/{new_name}"] = self.files.pop(full_path)

    def upload_bytes(self, target: str, data: bytes, _content_type: str, *, overwrite: bool = False) -> None:
        if target in self.files and not overwrite:
            return
        self.uploads.append(target)
        self.files[target] = bytes(data)

    def remove(self, source_dir: str, names: list[str]) -> None:
        for name in names:
            self.files.pop(f"{source_dir.rstrip('/')}/{name}", None)


class ValidationAList:
    """Only the read-only listing surface used by ``validate_plan``."""

    def try_list(self, _path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return []


class DelayedRenameAList(FakeAList):
    """Model a provider that exposes a moved object one retry later."""

    def __init__(self) -> None:
        super().__init__()
        self.rename_attempts = 0

    def rename(self, full_path: str, new_name: str) -> None:
        self.rename_attempts += 1
        if self.rename_attempts == 1:
            raise RuntimeError("object not found during eventual-consistency window")
        super().rename(full_path, new_name)


class DelayedMoveAList(FakeAList):
    def __init__(self) -> None:
        super().__init__()
        self.move_attempts = 0

    def move(self, source_dir: str, target_dir: str, names: list[str]) -> None:
        self.move_attempts += 1
        if self.move_attempts == 1:
            raise RuntimeError("transient provider move error")
        super().move(source_dir, target_dir, names)


class ListingVisibleMoveAList(FakeAList):
    """AList can list a just-moved object before exact-file lookup sees it."""

    def __init__(self) -> None:
        super().__init__()
        self.hidden_exact: set[str] = set()
        self.refresh_calls: list[str] = []

    def exact_file_info(self, path: str) -> dict[str, object] | None:
        if path in self.hidden_exact:
            return None
        return super().exact_file_info(path)

    def move(self, source_dir: str, target_dir: str, names: list[str]) -> None:
        super().move(source_dir, target_dir, names)
        for name in names:
            self.hidden_exact.add(f"{target_dir.rstrip('/')}/{name}")

    def rename(self, full_path: str, new_name: str) -> None:
        super().rename(full_path, new_name)
        self.hidden_exact.discard(full_path)

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        if refresh:
            self.refresh_calls.append(path)
        prefix = path.rstrip("/") + "/"
        rows: list[dict[str, object]] = []
        for full_path, value in self.files.items():
            if full_path.startswith(prefix) and "/" not in full_path[len(prefix):]:
                rows.append({
                    "name": full_path[len(prefix):],
                    "is_dir": False,
                    "size": len(value),
                })
        return rows


class DelayedFinalRenameVisibilityAList(FakeAList):
    """A rename commits, but both AList read paths lag for several reads."""

    def __init__(self) -> None:
        super().__init__()
        self.rename_attempts = 0
        self.hidden_final_reads = 4

    def rename(self, full_path: str, new_name: str) -> None:
        self.rename_attempts += 1
        super().rename(full_path, new_name)

    def _hide_final(self, path: str) -> bool:
        if path.endswith("/Movie (2020).mkv") and self.hidden_final_reads > 0:
            self.hidden_final_reads -= 1
            return True
        return False

    def exact_file_info(self, path: str) -> dict[str, object] | None:
        if self._hide_final(path):
            return None
        return super().exact_file_info(path)

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        final = "/library/Movie (2020)/Movie (2020).mkv"
        if path == "/library/Movie (2020)" and self._hide_final(final):
            return []
        prefix = path.rstrip("/") + "/"
        return [
            {"name": full_path[len(prefix):], "is_dir": False, "size": len(value)}
            for full_path, value in self.files.items()
            if full_path.startswith(prefix) and "/" not in full_path[len(prefix):]
        ]


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


def cleanup_plan(request: EngineRequest, alist: object, tmdb: object) -> Plan:
    plan = fake_plan(request, alist, tmdb)
    plan.cleanup_files = [
        PlannedCleanup(
            source_path="/incoming/movie/._sample.mkv",
            source_dir="/incoming/movie",
            original_name="._sample.mkv",
            reason="macOS AppleDouble 隐藏文件",
            source_size=1,
        )
    ]
    return plan


def provider_media_plan(request: EngineRequest, alist: object, tmdb: object) -> Plan:
    """A child-shaped plan with deliberately visible artifact requests."""
    plan = fake_plan(request, alist, tmdb)
    plan.metadata.update({
        "poster_path": "/tmdb/poster.jpg",
        "backdrop_path": "/tmdb/backdrop.jpg",
    })
    return plan


def provider_media_plan_with_subtitle(request: EngineRequest, alist: object, tmdb: object) -> Plan:
    """Child fixture containing a subtitle companion that must stay staged."""
    plan = provider_media_plan(request, alist, tmdb)
    plan.files.append(PlannedFile(
        source_path="/incoming/movie/source.zh.srt",
        source_dir="/incoming/movie",
        original_name="source.zh.srt",
        final_name="Movie (2020).zh.srt",
        target_dir="/library/Movie (2020)",
        media_kind="subtitle",
        source_size=3,
    ))
    return plan


def provider_tv_child_plan(*, duplicate: bool = False, bonus: bool = False) -> Plan:
    """Make a provider child shape without invoking the ordinary TV planner."""
    source_root = "/incoming/tv"
    target_root = "/library/Example Show/Season 01"
    first_source = "S01E01 - Example.Show.S01E01.1080p.mkv"
    first_final = "Example.Show.S01E01.mkv"
    files = [PlannedFile(
        source_path=f"{source_root}/{first_source}",
        source_dir=source_root,
        original_name=first_source,
        final_name=first_final,
        target_dir=target_root,
        media_kind="video",
        source_size=FAKE_VIDEO_SIZE,
    )]
    if duplicate:
        second_source = "S01E01 - Example.Show.S01E01.alt.1080p.mkv"
        second_final = "Example.Show.S01E01.alt.mkv"
    elif bonus:
        second_source = "S01E02 - Bonus - Example.Show.S01E02.mkv"
        second_final = "Example.Show.S01E02.mkv"
    else:
        second_source = "S01E02 - Example.Show.S01E02.1080p.mkv"
        second_final = "Example.Show.S01E02.mkv"
    files.append(PlannedFile(
        source_path=f"{source_root}/{second_source}",
        source_dir=source_root,
        original_name=second_source,
        final_name=second_final,
        target_dir=target_root,
        media_kind="video",
        source_size=FAKE_VIDEO_SIZE,
    ))
    return Plan(
        mode="tv",
        source_root=source_root,
        target_root="/library/Example Show",
        files=files,
        warnings=[],
        metadata={"tmdb_id": 7, "title": "Example Show"},
    )


class RecordingTMDB:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def download_poster(self, image_path: str) -> bytes:
        self.calls.append(image_path)
        return b"new-artwork"


class SimpleEngineRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.alist = FakeAList()
        self.alist.files["/incoming/movie/source.mkv"] = FAKE_VIDEO_BYTES
        self.request = EngineRequest.from_mapping(
            {
                "source_path": "/incoming/movie",
                "parent_path": "/library",
                "media_type": "movie",
                "tmdb_id": 1,
            }
        )

    def test_plan_is_dry_run_and_execution_is_automatic(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"ok": True},
        )
        job = runner.plan_job(self.request, job_id="engine-test")
        self.assertEqual(job.phase, "planned")
        self.assertEqual(self.alist.files, {"/incoming/movie/source.mkv": FAKE_VIDEO_BYTES})
        raw = (runner.jobs_root / "engine-test.json").read_text(encoding="utf-8")
        self.assertNotIn("sha256", raw.casefold())
        done = runner.execute_job("engine-test")
        self.assertEqual(done.phase, "executed")
        self.assertEqual(done.execution, {"ok": True})

    def test_runner_finalizes_an_injected_plan_before_persisting(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        plan.warnings.append("injected planner warning")
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=lambda *_args: plan,
            validate=False,
        )

        job = runner.plan_job(self.request, job_id="engine-current-finalizer")

        self.assertEqual(job.plan["scan_report"]["total_files"], 1)
        self.assertEqual(job.plan["scan_report"]["matched_files"], 1)
        self.assertEqual(
            job.plan["notices"],
            [{
                "code": "planning_warning",
                "severity": "warning",
                "message": "injected planner warning",
                "details": {},
            }],
        )

    def test_current_finalizer_keeps_one_exact_video_subtitle_track_idempotently(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        simplified = PlannedFile(
            source_path="/incoming/movie/source.zh-CN.ass",
            source_dir="/incoming/movie",
            original_name="source.zh-CN.ass",
            final_name="Movie (2020).zh-CN.ass",
            target_dir="/library/Movie (2020)",
            media_kind="subtitle",
            source_size=100,
        )
        english = PlannedFile(
            source_path="/incoming/movie/source.en.ass",
            source_dir="/incoming/movie",
            original_name="source.en.ass",
            final_name="Movie (2020).en.ass",
            target_dir="/library/Movie (2020)",
            media_kind="subtitle",
            source_size=100,
        )
        plan.files.extend([simplified, english])

        finalized = finalize_plan(plan)

        self.assertEqual(
            {item.source_path for item in finalized.files},
            {
                "/incoming/movie/source.mkv",
                simplified.source_path,
            },
        )
        self.assertEqual(finalized.problem_files, [])
        self.assertEqual(
            finalized.scan_report["deferred_subtitles"],
            [{
                "source_path": english.source_path,
                "planned_target_path": "/library/Movie (2020)/Movie (2020).en.ass",
                "action": "defer_until_exact_video_subtitle_closure",
                "reason": "alternate_subtitle_track",
                "preferred_source_path": simplified.source_path,
            }],
        )

        once = plan_to_dict(finalized)
        twice = plan_to_dict(finalize_plan(plan_from_dict(once)))
        self.assertEqual(twice, once)

    def test_current_finalizer_prefers_normal_release_over_subset_copy(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        normal = PlannedFile(
            source_path="/incoming/movie/source.zh-CN.ass",
            source_dir="/incoming/movie",
            original_name="source.zh-CN.ass",
            final_name="Movie (2020).zh-CN.ass",
            target_dir="/library/Movie (2020)",
            media_kind="subtitle",
            source_size=100,
        )
        subset = PlannedFile(
            source_path="/incoming/movie/子集化字幕/source.zh-CN.ass",
            source_dir="/incoming/movie/子集化字幕",
            original_name="source.zh-CN.ass",
            final_name="Movie (2020).zh-CN.2.ass",
            target_dir="/library/Movie (2020)",
            media_kind="subtitle",
            source_size=100,
        )
        plan.files.extend([normal, subset])

        finalized = finalize_plan(plan)

        self.assertIn(normal.source_path, {item.source_path for item in finalized.files})
        self.assertNotIn(subset.source_path, {item.source_path for item in finalized.files})
        self.assertNotIn(subset.source_path, {item.source_path for item in finalized.problem_files})
        self.assertEqual(
            finalized.scan_report["deferred_subtitles"][0]["source_path"],
            subset.source_path,
        )

    def test_runner_blocks_problem_files_before_injected_executor_or_executing(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        plan.problem_files.append(PlannedProblem(
            source_path="/incoming/movie/unresolved.bin",
            reason="无法唯一识别",
        ))
        calls: list[Plan] = []
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=lambda *_args: plan,
            validate=False,
            executor=lambda received: calls.append(received) or {"unexpected": True},
        )
        job = runner.plan_job(self.request, job_id="engine-problem-gate")

        with self.assertRaisesRegex(EngineExecutionError, "未闭合问题文件"):
            runner.execute_job(job.id)

        self.assertEqual(calls, [])
        blocked = runner.get_job(job.id)
        self.assertEqual(blocked.phase, "failed")
        self.assertIn("未闭合问题文件", blocked.error or "")
        self.assertEqual(self.alist.moves, [])

    def test_runner_redacts_error_fields_at_execute_gate_and_recovery_writes(self) -> None:
        """Durable runner errors never retain a configured runtime secret."""
        secret = "runner-secret-not-for-job-json"

        def persisted_error(runner: SimpleEngineRunner, job_id: str) -> str:
            raw = json.loads(
                (runner.jobs_root / f"{job_id}.json").read_text(encoding="utf-8")
            )
            error = str(raw.get("error") or "")
            self.assertNotIn(secret, error)
            self.assertIn("<redacted>", error)
            self.assertEqual(runner.get_job(job_id).error, error)
            return error

        with patch.dict(
            "os.environ",
            {"SCRAPEFLOW_RUNNER_TEST_SECRET": secret},
            clear=False,
        ):
            def failing_executor(_plan: object) -> object:
                raise RuntimeError(f"provider token={secret}")

            execution_runner = SimpleEngineRunner(
                self.root,
                alist=self.alist,
                tmdb=object(),
                planner=fake_plan,
                validate=False,
                executor=failing_executor,
            )
            execution_job = execution_runner.plan_job(
                self.request,
                job_id="engine-redacted-execution",
            )
            with self.assertRaisesRegex(RuntimeError, secret):
                execution_runner.execute_job(execution_job.id)
            persisted_error(execution_runner, execution_job.id)

            problem_plan = fake_plan(self.request, self.alist, object())
            problem_plan.problem_files.append(PlannedProblem(
                source_path="/incoming/movie/unresolved.bin",
                reason=f"archive password={secret}",
            ))
            gate_runner = SimpleEngineRunner(
                self.root,
                alist=self.alist,
                tmdb=object(),
                planner=lambda *_args: problem_plan,
                validate=False,
                executor=failing_executor,
            )
            gate_job = gate_runner.plan_job(
                self.request,
                job_id="engine-redacted-problem-gate",
            )
            with self.assertRaisesRegex(EngineExecutionError, "未闭合问题文件"):
                gate_runner.execute_job(gate_job.id)
            persisted_error(gate_runner, gate_job.id)

            recovery_runner = SimpleEngineRunner(
                self.root,
                alist=self.alist,
                tmdb=object(),
                planner=fake_plan,
                validate=False,
            )
            recovery_job = recovery_runner.plan_job(
                self.request,
                job_id="engine-redacted-recovery",
            )
            atomic_write_json(
                recovery_runner._job_path(recovery_job.id),  # noqa: SLF001 - failure fixture
                replace(recovery_job, phase="failed").as_dict(),
                allow_nan=False,
            )
            with patch.object(
                recovery_runner,
                "_readback_plan",
                side_effect=RuntimeError(f"upstream api_key={secret}"),
            ):
                retry = recovery_runner.recover_job(recovery_job.id)
            self.assertEqual(retry.phase, "retry_wait")
            self.assertNotIn(secret, retry.error or "")
            persisted_error(recovery_runner, recovery_job.id)

    def test_residual_policy_retains_user_attachments_and_allows_only_os_litter(self) -> None:
        cases = {
            "/incoming/movie/guide.pdf": "document_or_comic",
            "/incoming/movie/bonus.flac": "detached_audio",
            "/incoming/movie/release.sfv": "manifest",
            "/incoming/movie/cover.png": "unknown_image",
            "/incoming/movie/Show.NCOP.mkv": "theme_video",
            "/incoming/movie/setup.exe": "unknown_executable",
        }
        for path, expected_kind in cases.items():
            with self.subTest(path=path):
                decision = classify_residual(path)
                self.assertEqual(decision.kind, expected_kind)
                self.assertFalse(decision.can_cleanup)

        apple_double = classify_residual("/incoming/movie/._metadata")
        ds_store = classify_residual("/incoming/movie/.DS_Store")
        self.assertTrue(apple_double.can_cleanup)
        self.assertTrue(ds_store.can_cleanup)

    def test_executor_rejects_non_whitelisted_cleanup_before_any_move(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        plan.cleanup_files = [PlannedCleanup(
            source_path="/incoming/movie/guide.pdf",
            source_dir="/incoming/movie",
            original_name="guide.pdf",
            reason="小说、漫画或文档附件",
            source_size=1,
        )]
        self.alist.files["/incoming/movie/guide.pdf"] = b"p"

        with self.assertRaisesRegex(EngineExecutionError, "非白名单或非任务自有清理项"):
            SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(self.alist.moves, [])
        self.assertIn("/incoming/movie/source.mkv", self.alist.files)
        self.assertIn("/incoming/movie/guide.pdf", self.alist.files)

    def test_executor_keeps_appledouble_as_the_tiny_unattended_cleanup_exception(self) -> None:
        plan = cleanup_plan(self.request, self.alist, object())
        self.alist.files["/incoming/movie/._sample.mkv"] = b"x"

        result = SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(result["cleanup"], ["/incoming/movie/._sample.mkv"])
        self.assertNotIn("/incoming/movie/._sample.mkv", self.alist.files)

    def test_rebuildable_download_temp_needs_the_current_task_staging_root(self) -> None:
        staging_root = "/quark/影视/ScrapeFlow/补源/engine-77/attempt-1"
        temporary = f"{staging_root}/payload.mkv.aria2"

        self.assertEqual(
            cleanup_allowlist_reason(temporary, task_root=staging_root),
            REBUILDABLE_STAGING_TEMP_CLEANUP_REASON,
        )
        self.assertIsNone(
            cleanup_allowlist_reason(temporary, task_root="/incoming/movie"),
        )

    def test_restart_loads_a_persisted_failed_cleanup_phase(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        job = runner.plan_job(self.request, job_id="engine-failed-cleanup")
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - persisted restart fixture
            replace(job, phase="failed_cleanup", error="cleanup ownership check failed").as_dict(),
            allow_nan=False,
        )

        restarted = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )

        recovered = restarted.get_job(job.id)
        self.assertEqual(recovered.phase, "failed_cleanup")
        self.assertEqual(recovered.error, "cleanup ownership check failed")

    def test_internal_child_marker_is_durable_and_keeps_root_link(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        child = runner.plan_job(self.request, job_id="engine-child-marker")
        marked = runner.mark_internal_child(child.id, root_job_id="engine-root-marker")

        self.assertTrue(marked.summary["internal_child"])
        self.assertEqual(marked.summary["root_job_id"], "engine-root-marker")
        reloaded = runner.get_job(child.id)
        self.assertTrue(reloaded.summary["internal_child"])
        self.assertEqual(reloaded.summary["root_job_id"], "engine-root-marker")
        self.assertTrue(reloaded.summary["provider_media_only"])
        self.assertTrue(reloaded.plan["metadata"]["provider_media_only"])
        self.assertIsNone(runner.find_by_source(self.request.source_path))

    def test_internal_child_marker_is_written_with_initial_plan(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        child = runner.plan_job(
            self.request,
            job_id="engine-child-atomic",
            internal_child_of="engine-root-atomic",
        )
        self.assertTrue(child.summary["internal_child"])
        self.assertEqual(child.summary["root_job_id"], "engine-root-atomic")
        persisted = runner.get_job(child.id)
        self.assertTrue(persisted.summary["internal_child"])
        self.assertTrue(persisted.summary["provider_media_only"])
        self.assertTrue(persisted.plan["metadata"]["provider_media_only"])

    def test_audit_episode_title_evidence_survives_projection(self) -> None:
        projected = SimpleEngineRunner._audit_media_mapping(
            {
                "kind": "missing_episode",
                "label": "Show S00E05",
                "reason": "missing",
                "season": 0,
                "episode": 5,
                "title": "Dawn OVA",
                "title_aliases": ["黎明特别篇", "Dawn OVA"],
                "source_episode_aliases": [{
                    "season": 1,
                    "episode": 5,
                    "series_titles": ["Show", "Show (source)"],
                }],
                "media": {
                    "tmdb_id": 7,
                    "target_root": "/library/番剧/Show",
                    "media_type": "tv",
                },
            },
            tmdb_id=7,
            target_root="/library/番剧/Show",
            media_type="tv",
            title="Show",
            original_title="Show",
            year="2020",
            media_format="",
        )
        self.assertIsNotNone(projected)
        assert projected is not None
        self.assertEqual(projected["title"], "Dawn OVA")
        self.assertEqual(projected["title_aliases"], ["黎明特别篇"])
        self.assertEqual(
            projected["source_episode_aliases"],
            [{"season": 1, "episode": 5,
              "series_titles": ["Show", "Show (source)"]}],
        )

    def test_video_admission_configuration_has_an_immutable_floor(self) -> None:
        with patch.dict("os.environ", {"SCRAPEFLOW_MIN_VIDEO_BYTES": "1"}):
            self.assertEqual(minimum_video_bytes(), ABSOLUTE_MINIMUM_VIDEO_BYTES)

    def test_legacy_production_e2e_path_match_is_casefolded_and_narrow(self) -> None:
        self.assertTrue(is_production_test_media_path(
            "/quark/影视/待刮削/sCrApEfLoW-e2e-fight/source.mkv",
        ))
        self.assertTrue(is_production_test_media_path(
            "/quark/影视/scrapeflow/补源/E2E-run-1/attempt/source.mkv",
        ))
        self.assertFalse(is_production_test_media_path(
            "/quark/影视/ScrapeFlow/补源/audit-engine-1/attempt/source.mkv",
        ))

    def test_plan_validation_refuses_a_large_video_from_legacy_e2e_intake(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        plan.source_root = "/quark/影视/待刮削/ScrapeFlow-E2E-Fight"
        plan.files[0].source_path = f"{plan.source_root}/source.mkv"
        plan.files[0].source_dir = plan.source_root
        plan.target_root = "/quark/影视/电影/Movie (2020)"
        plan.files[0].target_dir = plan.target_root

        with self.assertRaisesRegex(PlanError, "生产 E2E 测试来源路径"):
            validate_plan(ValidationAList(), plan)

    def test_executor_refuses_a_large_video_from_legacy_e2e_staging(self) -> None:
        source = "/quark/影视/ScrapeFlow/补源/e2e-fight/attempt/source.mkv"
        plan = fake_plan(self.request, self.alist, object())
        plan.source_root = "/quark/影视/ScrapeFlow/补源/e2e-fight/attempt"
        plan.files[0].source_path = source
        plan.files[0].source_dir = posixpath.dirname(source)
        self.alist.files.pop("/incoming/movie/source.mkv")
        self.alist.files[source] = FAKE_VIDEO_BYTES

        with self.assertRaisesRegex(EngineExecutionError, "生产 E2E 测试来源路径"):
            SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(self.alist.moves, [])
        self.assertIn(source, self.alist.files)

    def test_normal_audit_staging_path_is_not_mistaken_for_legacy_e2e(self) -> None:
        source = "/quark/影视/ScrapeFlow/补源/audit-engine-1/attempt/source.mkv"
        plan = fake_plan(self.request, self.alist, object())
        plan.source_root = "/quark/影视/ScrapeFlow/补源/audit-engine-1/attempt"
        plan.files[0].source_path = source
        plan.files[0].source_dir = posixpath.dirname(source)
        self.alist.files.pop("/incoming/movie/source.mkv")
        self.alist.files[source] = FAKE_VIDEO_BYTES

        result = SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(result["file_count"], 1)
        self.assertIn("/library/Movie (2020)/Movie (2020).mkv", self.alist.files)

    def test_plan_validation_refuses_a_known_tiny_video(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        plan.files[0].source_size = 1885

        with self.assertRaisesRegex(PlanError, "视频文件小于正式库准入下限"):
            validate_plan(ValidationAList(), plan)

    def test_executor_refuses_tiny_video_before_any_formal_move(self) -> None:
        tiny = b"x" * 1885
        self.alist.files["/incoming/movie/source.mkv"] = tiny
        plan = fake_plan(self.request, self.alist, object())
        plan.files[0].source_size = len(tiny)

        with self.assertRaisesRegex(EngineExecutionError, "正式库准入下限"):
            SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(self.alist.moves, [])
        self.assertEqual(self.alist.files["/incoming/movie/source.mkv"], tiny)
        self.assertNotIn("/library/Movie (2020)/Movie (2020).mkv", self.alist.files)

    def test_executor_does_not_trust_a_mislabelled_mkv_plan_item(self) -> None:
        tiny = b"x" * 1885
        self.alist.files["/incoming/movie/source.mkv"] = tiny
        plan = fake_plan(self.request, self.alist, object())
        plan.files[0].media_kind = "subtitle"
        plan.files[0].source_size = len(tiny)

        with self.assertRaisesRegex(EngineExecutionError, "正式库准入下限"):
            SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(self.alist.moves, [])

    def test_recovery_refuses_a_tiny_target_from_a_pre_guard_plan(self) -> None:
        tiny = b"x" * 1622
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        plan = fake_plan(self.request, self.alist, object())
        plan.files[0].source_size = len(tiny)
        runner.planner = lambda *_args: plan
        job = runner.plan_job(self.request, job_id="engine-tiny-recovery")
        source = "/incoming/movie/source.mkv"
        target = "/library/Movie (2020)/Movie (2020).mkv"
        self.alist.files.pop(source)
        self.alist.files[target] = tiny
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - stale-plan fixture
            replace(job, phase="failed", error="interrupted before guard").as_dict(),
            allow_nan=False,
        )

        recovered = runner.recover_job(job.id)

        self.assertEqual(recovered.phase, "retry_wait")
        self.assertIn("正式库准入下限", recovered.error or "")

    def test_provider_payload_admission_rejects_tiny_file_before_ffprobe(self) -> None:
        selection = {"provider": "test", "locator": "magnet:?xt=urn:btih:test"}
        with patch(
            "engine.tools._replenishment_local_adapter_impl._ffprobe_archive_video"
        ) as probe, self.assertRaisesRegex(
            ReplenishmentCandidateError, "正式库准入下限",
        ):
            _verify_video_payload(Path("/tmp/tiny.mkv"), 1885, selection)
        probe.assert_not_called()

    def test_provider_payload_admission_requires_a_video_stream(self) -> None:
        selection = {"provider": "test", "locator": "magnet:?xt=urn:btih:test"}
        with patch.dict("os.environ", {"SCRAPEFLOW_MIN_VIDEO_BYTES": "1048576"}), patch(
            "engine.tools._replenishment_local_adapter_impl._ffprobe_archive_video"
        ) as probe:
            _verify_video_payload(
                Path("/tmp/admissible-size.mkv"), minimum_video_bytes(), selection,
            )
        probe.assert_called_once_with(Path("/tmp/admissible-size.mkv"))

    def test_default_executor_moves_and_reads_back_by_size(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        result = SimplePlanExecutor(self.alist).execute(plan)
        self.assertEqual(result["file_count"], 1)
        self.assertIn("/library/Movie (2020)/Movie (2020).mkv", self.alist.files)
        self.assertEqual(result["artifact_count"], 1)
        self.assertEqual(self.alist.moves, [("/incoming/movie", "/library/Movie (2020)", ["source.mkv"])])
        self.assertEqual(self.alist.renames, [
            ("/library/Movie (2020)/source.mkv", "Movie (2020).mkv")
        ])

    def test_provider_internal_child_moves_media_without_touching_artifacts(self) -> None:
        plan = provider_media_plan(self.request, self.alist, object())
        plan.metadata["provider_media_only"] = True
        nfos = planned_nfos(plan)
        artwork = planned_artwork(plan)
        self.assertTrue(nfos)
        self.assertTrue(artwork)
        # Existing files intentionally have different sizes.  Ordinary roots
        # reject these conflicts; an internal provider child must leave them
        # byte-for-byte untouched and must not even enter the artifact lane.
        existing: dict[str, bytes] = {}
        for target, _content in nfos:
            existing[target] = b"existing-nfo"
        # Keep one existing artwork target with a conflicting size; leave the
        # remaining artwork absent to prove a media-only child does not fill
        # genuinely missing sidecars either.
        existing_artwork = artwork[0][0]
        existing[existing_artwork] = b"existing-artwork"
        self.alist.files.update(existing)
        tmdb = RecordingTMDB()

        result = SimplePlanExecutor(self.alist, tmdb).execute(plan)

        self.assertEqual(result["file_count"], 1)
        self.assertEqual(result["artifact_count"], 0)
        self.assertTrue(result["media_only"])
        self.assertEqual(tmdb.calls, [])
        for target, content in existing.items():
            self.assertEqual(self.alist.files[target], content)
        self.assertEqual(
            self.alist.files[existing_artwork], b"existing-artwork",
        )
        self.assertTrue(
            set(target for target, _path, _role in artwork if target != existing_artwork)
            .isdisjoint(self.alist.files)
        )

    def test_provider_internal_child_never_creates_episode_nfo_or_artwork(self) -> None:
        """A replenishment child moves only media into an established tree."""
        plan = provider_media_plan(self.request, self.alist, object())
        plan.metadata["provider_media_only"] = True
        targets = {
            target for target, _content in planned_nfos(plan)
        } | {
            target for target, _path, _role in planned_artwork(plan)
        }
        result = SimplePlanExecutor(self.alist, RecordingTMDB()).execute(plan)

        self.assertEqual(result["file_count"], 1)
        self.assertEqual(result["artifact_count"], 0)
        self.assertTrue(result["media_only"])
        self.assertTrue(targets.isdisjoint(self.alist.files))

    def test_provider_internal_child_does_not_move_subtitle_companion(self) -> None:
        plan = provider_media_plan_with_subtitle(self.request, self.alist, object())
        plan.metadata["provider_media_only"] = True
        self.alist.files["/incoming/movie/source.zh.srt"] = b"zh\n"

        result = SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(result["file_count"], 1)
        self.assertNotIn("/library/Movie (2020)/Movie (2020).zh.srt", self.alist.files)
        self.assertIn("/incoming/movie/source.zh.srt", self.alist.files)

    def test_provider_tv_child_plan_rejects_duplicate_episode_before_persisting(self) -> None:
        unsafe = provider_tv_child_plan(duplicate=True)
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=lambda *_args: unsafe,
            validate=False,
        )

        with self.assertRaisesRegex(EngineRequestError, "多个视频映射到 S01E01"):
            runner.plan_job(
                self.request,
                job_id="engine-duplicate-provider-child",
                internal_child_of="engine-provider-root",
            )

        self.assertFalse((runner.jobs_root / "engine-duplicate-provider-child.json").exists())

    def test_provider_tv_child_execution_rejects_bonus_before_any_move(self) -> None:
        plan = provider_tv_child_plan(bonus=True)
        plan.metadata["provider_media_only"] = True
        for item in plan.files:
            self.alist.files[item.source_path] = FAKE_VIDEO_BYTES

        with self.assertRaisesRegex(EngineExecutionError, "附加内容"):
            SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(self.alist.moves, [])
        self.assertTrue(all(item.source_path in self.alist.files for item in plan.files))

    def test_serialized_provider_tv_child_cannot_bypass_unique_episode_guard(self) -> None:
        unsafe = provider_tv_child_plan(duplicate=True)
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=lambda *_args: unsafe,
            validate=False,
            executor=lambda _plan: {"unexpected": True},
        )
        job = runner.plan_job(self.request, job_id="engine-serialized-provider-child")
        tampered_plan = dict(job.plan)
        tampered_metadata = dict(tampered_plan["metadata"])
        tampered_metadata["provider_media_only"] = True
        tampered_plan["metadata"] = tampered_metadata
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - persisted-plan guard fixture
            replace(job, plan=tampered_plan).as_dict(),
            allow_nan=False,
        )

        with self.assertRaisesRegex(EngineExecutionError, "多个视频映射到 S01E01"):
            runner.execute_job(job.id)

        self.assertEqual(runner.get_job(job.id).phase, "planned")

    def test_provider_tv_child_allows_two_distinct_unique_episode_videos(self) -> None:
        plan = provider_tv_child_plan()
        plan.metadata["provider_media_only"] = True
        for item in plan.files:
            self.alist.files[item.source_path] = FAKE_VIDEO_BYTES

        result = SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(result["file_count"], 2)
        self.assertEqual(len(self.alist.moves), 2)
        self.assertIn(
            "/library/Example Show/Season 01/Example.Show.S01E01.mkv",
            self.alist.files,
        )
        self.assertIn(
            "/library/Example Show/Season 01/Example.Show.S01E02.mkv",
            self.alist.files,
        )

    def test_ordinary_root_still_writes_planned_artifacts(self) -> None:
        plan = provider_media_plan(self.request, self.alist, object())
        tmdb = RecordingTMDB()

        result = SimplePlanExecutor(self.alist, tmdb).execute(plan)

        self.assertEqual(
            result["artifact_count"], len(planned_nfos(plan)) + len(planned_artwork(plan)),
        )
        self.assertTrue(tmdb.calls)

    def test_nfo_planning_requires_current_identity_metadata(self) -> None:
        plan = Plan(
            mode="movie",
            source_root="/incoming/movie",
            target_root="/library/Movie (2020)",
            files=[PlannedFile(
                source_path="/incoming/movie/source.mkv",
                source_dir="/incoming/movie",
                original_name="source.mkv",
                final_name="Movie {tmdb-1} (2020).mkv",
                target_dir="/library/Movie (2020)",
                media_kind="video",
                source_size=FAKE_VIDEO_SIZE,
            )],
            warnings=[],
            metadata={},
        )
        self.assertEqual(planned_nfos(plan), [])

    def test_tv_planner_does_not_turn_eighty_six_title_into_range_endpoint(self) -> None:
        """`S00E01 - 86 Eighty Six` is one special, never E01-E86."""
        class PlannerAList:
            def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
                del refresh
                return []

        class PlannerTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/100565":
                    return {
                        "name": "86 - Eighty Six",
                        "original_name": "86 - Eighty Six",
                        "first_air_date": "2021-04-01",
                        "seasons": [],
                    }
                if path == "/tv/100565/season/1":
                    return {"episodes": []}
                if path == "/tv/100565/season/0":
                    return {"episodes": [
                        {"episode_number": 1, "name": "Special One"},
                        {"episode_number": 3, "name": "Special Three"},
                    ]}
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_files = [
            {
                "name": f"86 - Eighty Six - S00E{episode:02d} - 86 - Eighty Six.mkv",
                "full_path": f"/staging/86-eighty-six-{episode}.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            }
            for episode in (1, 3)
        ]

        plan = build_tv_plan(
            PlannerAList(),
            PlannerTMDB(),
            src_path="/staging",
            parent_path="/library/番剧",
            tmdb_id=100565,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
            source_files=source_files,
        )

        self.assertEqual([item.episode_key for item in plan.files], ["SP01", "SP03"])
        self.assertFalse(any("E86" in item.final_name for item in plan.files))
        # Explicit endpoint notation remains a real multi-episode form.
        ranged = parse_ep_files([{
            "name": "86 - Eighty Six - S00E01-E02.mkv",
            "full_path": "/staging/range.mkv",
            "size": FAKE_VIDEO_SIZE,
            "is_dir": False,
        }])
        self.assertEqual(
            [(key.kind, key.number, key.end_number) for key in ranged],
            [("regular", 1, 2)],
        )

    def test_preferred_traditional_subtitle_is_deferred_not_a_problem_file(self) -> None:
        """Keep the planner's simplified-preference result non-blocking.

        The generic current-finalizer selector has its own direct fixture
        above. This one protects the planner's earlier
        ``preferred_excluded_subtitles`` branch: a traditional peer is
        reported as deferred, not promoted into ``problem_files`` (which
        would block the whole write).
        """
        class PlannerAList:
            def try_list(self, _path: str, refresh: bool = False) -> list[dict[str, object]]:
                del refresh
                return []

        class PlannerTMDB:
            def get(self, path: str, language: str | None = None) -> dict[str, object]:
                del language
                if path == "/tv/77":
                    return {
                        "name": "Example Show",
                        "original_name": "Example Show",
                        "first_air_date": "2020-01-01",
                        "seasons": [],
                    }
                if path == "/tv/77/season/1":
                    return {"episodes": [{"episode_number": 1, "name": "Pilot"}]}
                if path == "/tv/77/season/0":
                    return {"episodes": []}
                raise AssertionError(f"unexpected TMDB path: {path}")

        simplified = "/incoming/show/Example.Show.S01E01.zh-CN.ass"
        traditional = "/incoming/show/Example.Show.S01E01.zh-TW.ass"
        plan = build_tv_plan(
            PlannerAList(),
            PlannerTMDB(),
            src_path="/incoming/show",
            parent_path="/library/番剧",
            tmdb_id=77,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            source_files=[
                {
                    "name": "Example.Show.S01E01.1080p.mkv",
                    "full_path": "/incoming/show/Example.Show.S01E01.1080p.mkv",
                    "size": FAKE_VIDEO_SIZE,
                    "is_dir": False,
                },
                {
                    "name": "Example.Show.S01E01.zh-CN.ass",
                    "full_path": simplified,
                    "size": 100,
                    "is_dir": False,
                },
                {
                    "name": "Example.Show.S01E01.zh-TW.ass",
                    "full_path": traditional,
                    "size": 100,
                    "is_dir": False,
                },
            ],
        )

        self.assertIn(simplified, {item.source_path for item in plan.files})
        self.assertNotIn(traditional, {item.source_path for item in plan.files})
        self.assertNotIn(traditional, {item.source_path for item in plan.problem_files})
        self.assertEqual(
            plan.scan_report["deferred_subtitles"],
            [{
                "source_path": traditional,
                "action": "preserve_at_source",
                "reason": "preferred_simplified_subtitle",
            }],
        )

    def test_default_executor_retries_a_delayed_alist_rename(self) -> None:
        alist = DelayedRenameAList()
        alist.files["/incoming/movie/source.mkv"] = FAKE_VIDEO_BYTES
        result = SimplePlanExecutor(alist).execute(fake_plan(self.request, alist, object()))
        self.assertEqual(result["file_count"], 1)
        self.assertEqual(alist.rename_attempts, 2)
        self.assertIn("/library/Movie (2020)/Movie (2020).mkv", alist.files)

    def test_default_executor_waits_for_a_committed_rename_before_replaying_it(self) -> None:
        alist = DelayedFinalRenameVisibilityAList()
        alist.files["/incoming/movie/source.mkv"] = FAKE_VIDEO_BYTES
        # The test simulates visibility time without turning the suite into a
        # real-time backoff test.  The executor still visits its same bounded
        # readback branches.
        with patch("local.scrapeflow_api.simple_engine_runner.time.sleep"):
            result = SimplePlanExecutor(alist).execute(fake_plan(self.request, alist, object()))
        self.assertEqual(result["file_count"], 1)
        self.assertEqual(alist.rename_attempts, 1)
        self.assertIn("/library/Movie (2020)/Movie (2020).mkv", alist.files)

    def test_default_executor_retries_a_transient_alist_move(self) -> None:
        alist = DelayedMoveAList()
        alist.files["/incoming/movie/source.mkv"] = FAKE_VIDEO_BYTES
        result = SimplePlanExecutor(alist).execute(fake_plan(self.request, alist, object()))
        self.assertEqual(result["file_count"], 1)
        self.assertEqual(alist.move_attempts, 2)
        self.assertIn("/library/Movie (2020)/Movie (2020).mkv", alist.files)

    def test_default_executor_refreshes_parent_listing_for_delayed_move_visibility(self) -> None:
        alist = ListingVisibleMoveAList()
        alist.files["/incoming/movie/source.mkv"] = FAKE_VIDEO_BYTES

        result = SimplePlanExecutor(alist).execute(fake_plan(self.request, alist, object()))

        self.assertEqual(result["file_count"], 1)
        self.assertIn("/library/Movie (2020)", alist.refresh_calls)
        self.assertIn("/library/Movie (2020)/Movie (2020).mkv", alist.files)

    def test_subtitle_sidecar_moves_to_the_exact_existing_video_path(self) -> None:
        video = "/library/Show/Season 01/Show.S01E01.mkv"
        source = "/quark/影视/ScrapeFlow/补源/root/attempt-1/Show.S01E01.zh.srt"
        target = "/library/Show/Season 01/Show.S01E01.zh.srt"
        self.alist.files[video] = b"video"
        self.alist.files[source] = b"subtitle"

        result = SimplePlanExecutor(self.alist).install_subtitle_sidecar(
            source, target, expected_size=len(b"subtitle"), video_path=video,
        )

        self.assertEqual(result["status"], "moved")
        self.assertEqual(result["target"], target)
        self.assertEqual(result["size"], len(b"subtitle"))
        self.assertNotIn(source, self.alist.files)
        self.assertEqual(self.alist.files[target], b"subtitle")
        self.assertEqual(self.alist.files[video], b"video")

    def test_subtitle_sidecar_refuses_to_write_without_its_audited_video(self) -> None:
        source = "/quark/影视/ScrapeFlow/补源/root/attempt-1/Show.S01E01.zh.srt"
        self.alist.files[source] = b"subtitle"

        # Exercise the same bounded eventual-consistency retry branch without
        # making a deterministic rejection consume its real backoff time.
        with patch("local.scrapeflow_api.simple_engine_runner.time.sleep"), self.assertRaisesRegex(
            Exception, "正式视频不可见",
        ):
            SimplePlanExecutor(self.alist).install_subtitle_sidecar(
                source,
                "/library/Show/Season 01/Show.S01E01.zh.srt",
                expected_size=len(b"subtitle"),
                video_path="/library/Show/Season 01/Show.S01E01.mkv",
            )
        self.assertIn(source, self.alist.files)

    def test_cleanup_plan_runs_as_one_automatic_execution(self) -> None:
        calls: list[object] = []
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=cleanup_plan,
            validate=False,
            executor=lambda plan: calls.append(plan) or {"ok": True},
        )
        job = runner.plan_job(self.request)
        done = runner.execute_job(job.id)
        self.assertEqual(done.phase, "executed")
        self.assertEqual(len(calls), 1)

    def test_restart_queues_executing_job_for_readback_and_cancel_uses_cancelled_phase(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"ok": True},
        )
        job = runner.plan_job(self.request, job_id="engine-restart")
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - persisted restart fixture
            replace(job, phase="executing").as_dict(),
            allow_nan=False,
        )
        restarted = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"ok": True},
        )
        recovered = restarted.get_job(job.id)
        self.assertEqual(recovered.phase, "retry_wait")
        self.assertIn("重启", recovered.error or "")

        next_job = restarted.plan_job(self.request, job_id="engine-cancel")
        cancelled = restarted.cancel_job(next_job.id)
        self.assertEqual(cancelled.phase, "cancelled")

    def test_recovery_readback_marks_fully_written_engine_job_completed(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        job = runner.plan_job(self.request)
        done = runner.execute_job(job.id)
        atomic_write_json(
            runner._job_path(done.id),  # noqa: SLF001 - restart fixture
            replace(done, phase="failed", execution=None, error="simulated interrupted response").as_dict(),
            allow_nan=False,
        )

        recovered = runner.recover_job(done.id)

        self.assertEqual(recovered.phase, "executed")
        self.assertTrue(recovered.execution and recovered.execution["recovered"])

    def test_provider_child_recovery_reads_back_media_without_artifacts(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=RecordingTMDB(),
            planner=provider_media_plan,
            validate=False,
        )
        child = runner.plan_job(
            self.request,
            job_id="engine-provider-media-only",
            internal_child_of="engine-provider-root",
        )
        target = "/library/Movie (2020)/Movie (2020).mkv"
        self.alist.files[target] = self.alist.files.pop("/incoming/movie/source.mkv")
        atomic_write_json(
            runner._job_path(child.id),  # noqa: SLF001 - interrupted child fixture
            replace(child, phase="failed", error="simulated artifact conflict").as_dict(),
            allow_nan=False,
        )

        recovered = runner.recover_job(child.id)

        self.assertEqual(recovered.phase, "executed")
        self.assertTrue(recovered.execution and recovered.execution["recovered"])
        self.assertTrue(recovered.execution and recovered.execution["media_only"])
        self.assertEqual(recovered.execution and recovered.execution["artifact_count"], 0)
        artifact_plan = provider_media_plan(self.request, self.alist, object())
        expected_artifact_targets = {
            target
            for target, _content in planned_nfos(artifact_plan)
        }
        expected_artifact_targets.update(
            target for target, _image_path, _role in planned_artwork(artifact_plan)
        )
        self.assertTrue(expected_artifact_targets.isdisjoint(self.alist.files))

    def test_recovery_keeps_unverified_cleanup_pending(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=cleanup_plan,
            validate=False,
        )
        job = runner.plan_job(self.request)
        done = runner.execute_job(job.id)
        self.alist.files["/incoming/movie/._sample.mkv"] = b"x"
        atomic_write_json(
            runner._job_path(done.id),  # noqa: SLF001 - recovery fixture
            replace(done, phase="failed", execution=None, error="simulated interrupted response").as_dict(),
            allow_nan=False,
        )

        recovered = runner.recover_job(done.id)

        self.assertEqual(recovered.phase, "retry_wait")
        self.assertIn("清理项", recovered.error or "")

    def test_queued_auto_job_survives_identity_outage_then_plans(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"ok": True},
        )
        queued = runner.create_automatic_job("/incoming/movie")
        self.assertEqual(queued.phase, "queued")
        identity = AutomaticIdentity(
            media_type="movie", tmdb_id=1, title="Movie", year="2020",
            confidence=0.99, target_parent="/library/电影", season=None, trace={},
        )
        original = runner.resolve_automatic_request
        runner.resolve_automatic_request = lambda _source: (self.request, identity)  # type: ignore[method-assign]
        try:
            planned = runner.plan_automatic_job(queued.id)
        finally:
            runner.resolve_automatic_request = original  # type: ignore[method-assign]
        self.assertEqual(planned.phase, "planned")
        self.assertEqual(planned.summary["identity"]["tmdb_id"], 1)

    def test_automatic_job_rejects_retired_production_e2e_source(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        with self.assertRaisesRegex(Exception, "E2E"):
            runner.create_automatic_job("/library/待刮削/ScrapeFlow-E2E-Fight-Club-1999")

    def test_engine_execution_refuses_another_process_holding_the_worker_lock(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"ok": True},
        )
        job = runner.plan_job(self.request)
        script = (
            "import sys, time\n"
            "from pathlib import Path\n"
            "from local.scrapeflow_api.simple_engine_runner import _engine_worker_lock\n"
            "with _engine_worker_lock(Path(sys.argv[1])):\n"
            "    print('locked', flush=True)\n"
            "    time.sleep(20)\n"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", script, str(self.root)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def stop_process() -> None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()

        self.addCleanup(stop_process)
        self.assertEqual(process.stdout.readline().strip(), "locked")
        with self.assertRaises(EngineWorkerBusyError):
            runner.execute_job(job.id)

    def test_engine_execution_contends_with_the_one_file_runtime_lock(self) -> None:
        """Both write paths must use the one lock advertised by the rebuild."""
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"ok": True},
        )
        job = runner.plan_job(self.request)
        script = (
            "import sys, time\n"
            "from pathlib import Path\n"
            "from local.scrapeflow_api.simple_engine_runner import _engine_worker_lock\n"
            "with _engine_worker_lock(Path(sys.argv[1])):\n"
            "    print('locked', flush=True)\n"
            "    time.sleep(20)\n"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", script, str(self.root)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def stop_process() -> None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()

        self.addCleanup(stop_process)
        self.assertEqual(process.stdout.readline().strip(), "locked")
        with self.assertRaises(EngineWorkerBusyError):
            runner.execute_job(job.id)


if __name__ == "__main__":
    unittest.main()
