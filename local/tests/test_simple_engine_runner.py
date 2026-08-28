"""Tests for the small Engine planner/executor bridge."""

from __future__ import annotations

from dataclasses import replace
import json
import posixpath
import subprocess
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from engine.scrapeflow.archive import ArchivePasswordError, ArchiveToolError
from engine.scrapeflow.core import (
    build_tv_plan,
    parse_ep_files,
    validate_plan,
)
from engine.scrapeflow.current_plan import finalize_plan, plan_from_dict, plan_to_dict
from engine.scrapeflow.errors import FormalTargetConflictError, PlanError
from engine.scrapeflow.media_quality import (
    ABSOLUTE_MINIMUM_VIDEO_BYTES,
    minimum_video_bytes,
)
from engine.scrapeflow.replenishment_matching import (
    release_dash_regular_episode,
    release_title_ordinal_regular_episode,
)
from engine.scrapeflow.models import Plan, PlannedCleanup, PlannedFile, PlannedProblem
from engine.scrapeflow.residual_policy import (
    REBUILDABLE_STAGING_TEMP_CLEANUP_REASON,
    classify_residual,
    cleanup_allowlist_reason,
)
from engine.scraper import (
    build_movie_plan,
    build_tv_plan_smart,
    planned_artwork,
    planned_nfos,
)
from engine.scrapeflow.planning.tv.smart import _preclassify_theme_residuals
from engine.scrapeflow.serialization import atomic_write_json
from local.scrapeflow_api.simple_engine_runner import (
    AutomaticIdentity,
    EngineExecutionError,
    EngineJob,
    EngineJobConflictError,
    EnginePauseRequested,
    EngineRequest,
    EngineRequestError,
    EngineWorkerBusyError,
    SimpleEngineRunner,
    SimplePlanExecutor,
)
from local.scrapeflow_api.unit_execution import (
    ContainerMetadataAttention,
    ensure_container_artifacts,
)
from engine.scrapeflow.work_units import WorkUnitRecord, save_work_unit_records
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
        self.directories: set[str] = set()
        self.moves: list[tuple[str, str, list[str]]] = []
        self.renames: list[tuple[str, str]] = []
        self.uploads: list[str] = []

    def exact_file_info(self, path: str) -> dict[str, object] | None:
        value = self.files.get(path)
        return None if value is None else {"size": len(value)}

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        normalized = path.rstrip("/") or "/"
        prefix = normalized.rstrip("/") + "/"
        directory_paths = set(self.directories)
        for full_path in self.files:
            parts = full_path.strip("/").split("/")[:-1]
            current = ""
            for part in parts:
                current += "/" + part
                directory_paths.add(current)
        rows: dict[str, dict[str, object]] = {}
        for directory in directory_paths:
            if not directory.startswith(prefix):
                continue
            remainder = directory[len(prefix):]
            if remainder and "/" not in remainder:
                rows[remainder] = {"name": remainder, "is_dir": True}
        for full_path, value in self.files.items():
            if not full_path.startswith(prefix):
                continue
            remainder = full_path[len(prefix):]
            if remainder and "/" not in remainder:
                rows[remainder] = {
                    "name": remainder,
                    "is_dir": False,
                    "size": len(value),
                }
        return [rows[name] for name in sorted(rows)]

    def read_file_prefix(self, path: str, *, max_bytes: int) -> bytes:
        value = self.files.get(path)
        if value is None:
            raise FileNotFoundError(path)
        return value[:max_bytes]

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


class TargetConflictAList(FakeAList):
    """Expose the same deterministic listings to runner and plan validation."""

    def try_list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        return self.list(path, refresh=refresh)


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


class ArchiveLifecycleAList(FakeAList):
    def __init__(self) -> None:
        super().__init__()
        self.directories: set[str] = set()

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        if path in self.directories:
            return [{"name": "payload.zip", "is_dir": False}]
        return []

    def ensure_directory(self, path: str) -> None:
        self.directories.add(path)

    def move(self, source_dir: str, target_dir: str, names: list[str]) -> None:
        self.moves.append((source_dir, target_dir, list(names)))
        for name in names:
            source = f"{source_dir.rstrip('/')}/{name}"
            target = f"{target_dir.rstrip('/')}/{name}"
            if source in self.directories:
                self.directories.remove(source)
                self.directories.add(target)
            elif source in self.files:
                self.files[target] = self.files.pop(source)


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
    selected_parent = _request.parent_path
    target_root = (
        f"{selected_parent.rstrip('/')}/Movie (2020)"
        if _request.target_shelf is not None
        else "/library/Movie (2020)"
    )
    return Plan(
        mode="movie",
        source_root="/incoming/movie",
        target_root=target_root,
        files=[
            PlannedFile(
                source_path="/incoming/movie/source.mkv",
                source_dir="/incoming/movie",
                original_name="source.mkv",
                final_name="Movie (2020).mkv",
                target_dir=target_root,
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
        metadata={
            "tmdb_id": 7,
            "title": "Example Show",
            "year": "2020",
            "poster_path": None,
            "backdrop_path": None,
        },
    )


class RecordingTMDB:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def download_poster(self, image_path: str) -> bytes:
        self.calls.append(image_path)
        return b"new-artwork"


class RecordingArchivePreprocessor:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def prepare_ordinary_request(self, request, *, alist=None):
        self.calls.append({"request": dict(request), "alist": alist})
        return {**request, "source_path": "/task-staging/archive"}


class OrderedArchivePreprocessor(RecordingArchivePreprocessor):
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        super().__init__()
        self.events = events
        self.fail = fail

    def prepare_ordinary_request(self, request, *, alist=None):
        self.events.append("archive_preprocess")
        if self.fail:
            raise RuntimeError("archive fixture rejected")
        return {**request, "source_path": "/task-staging/archive"}


class FailingArchivePreprocessor:
    def __init__(self, error: Exception, events: list[str]) -> None:
        self.error = error
        self.events = events

    def prepare_ordinary_request(self, request, **_kwargs):
        del request
        self.events.append("archive_preprocess")
        raise self.error


class SimpleEngineRunnerTests(unittest.TestCase):
    def test_movie_plan_quality_dedup_does_not_depend_on_removed_hash_field(self) -> None:
        class MovieTMDB:
            def get(self, path: str) -> dict[str, object]:
                if path != "/movie/42":
                    raise AssertionError(path)
                return {
                    "title": "Example Film",
                    "release_date": "2020-01-01",
                    "poster_path": None,
                    "backdrop_path": None,
                }

        source_files = [
            {
                "name": "Example.Film.1080p.mkv",
                "full_path": "/incoming/Example Film/Example.Film.1080p.mkv",
                "size": 100,
            },
            {
                "name": "Example.Film.2160p.mkv",
                "full_path": "/incoming/Example Film/Example.Film.2160p.mkv",
                "size": 200,
            },
        ]

        plan = build_movie_plan(
            ValidationAList(),
            MovieTMDB(),
            src_path="/incoming/Example Film",
            parent_path="/library",
            tmdb_id=42,
            source_files=source_files,
            defer_validation=True,
        )

        self.assertEqual([item.original_name for item in plan.files], ["Example.Film.2160p.mkv"])
        self.assertEqual(len(plan.cleanup_files), 1)
        self.assertEqual(plan.cleanup_files[0].original_name, "Example.Film.1080p.mkv")
        self.assertEqual(plan.cleanup_files[0].source_size, 100)

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

    @staticmethod
    def _new_work_waiting(
        runner: SimpleEngineRunner,
        source: str,
        *,
        job_id: str | None = None,
    ):
        """Construct a legacy downstream fixture for plan-boundary tests.

        The explicit ``automatic_stage`` key models a pre-retirement legacy
        record; the runner no longer writes the mirror field, and the start
        transition drops it.
        """
        pending = runner.create_pending_job(source, job_id=job_id)
        summary = dict(pending.summary)
        summary.pop("reconciliation", None)
        summary.pop("reconciliation_outcome", None)
        waiting = replace(
            pending,
            phase="awaiting_target_shelf",
            summary={
                **summary,
                "automatic_stage": "awaiting_target_shelf",
            },
        )
        atomic_write_json(
            runner.jobs_root / f"{waiting.id}.json",
            waiting.as_dict(),
            allow_nan=False,
        )
        return waiting

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

    def test_container_metadata_carrier_writes_root_nfo_and_artwork_only(self) -> None:
        """A pure series container gets root metadata through the one writer."""
        source = "/library/待刮削/Container"
        self.alist.files[f"{source}/child.mkv"] = FAKE_VIDEO_BYTES
        tmdb = RecordingTMDB()
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=tmdb,
            library_root="/library",
        )
        root = runner.create_pending_job(source, job_id="engine-root-container")
        root = runner.start_automatic_job(root.id, target_shelf="anime")
        carrier = runner.plan_container_artifacts(
            root_job_id=root.id,
            source_path=source,
            target_root="/library/番剧/Container",
            target_shelf="anime",
            container_title="Container",
            poster_path="/poster.jpg",
            backdrop_path="/backdrop.jpg",
            representative_tmdb_id=7,
            job_id="container-artifacts-engine-root-container",
        )
        self.assertEqual(carrier.phase, "planned")
        self.assertEqual(carrier.plan["files"], [])
        done = runner.execute_job(carrier.id)
        self.assertEqual(done.phase, "executed")
        self.assertIn("/library/番剧/Container/poster.jpg", self.alist.files)
        self.assertIn("/library/番剧/Container/folder.jpg", self.alist.files)
        self.assertIn("/library/番剧/Container/fanart.jpg", self.alist.files)
        nfo = self.alist.files["/library/番剧/Container/tvshow.nfo"].decode()
        self.assertIn("<title>Container</title>", nfo)
        self.assertNotIn("<tmdbid>", nfo)
        self.assertEqual(self.alist.moves, [])
        uploads_before = list(self.alist.uploads)
        repaired = runner.repair_automatic_artifacts(carrier.id)
        self.assertEqual(repaired.phase, "executed")
        self.assertEqual(self.alist.moves, [])
        self.assertEqual(self.alist.uploads, uploads_before)

    @staticmethod
    def _persist_unit_carrier(
        runner: SimpleEngineRunner,
        *,
        job_id: str,
        root_job_id: str,
        target_root: str,
        metadata: dict[str, object],
    ) -> None:
        job = EngineJob(
            id=job_id,
            phase="executed",
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-01T00:00:00Z",
            request={},
            plan={"target_root": target_root, "metadata": metadata},
            summary={"root_job_id": root_job_id, "internal_child": True},
            target_shelf="anime",
            target_root=target_root,
            selected_at="2026-01-01T00:00:00Z",
        )
        atomic_write_json(
            runner.jobs_root / f"{job_id}.json",
            job.as_dict(),
            allow_nan=False,
        )

    def test_container_pass_yields_to_a_written_work_at_the_container_path(self) -> None:
        """A work carrier owning the container path preempts the marker pass.

        A layout re-derivation can demote a root from the dominant-TV form to
        the pure container form after a late sibling executes, while the
        already written main unit keeps the container path as its own target
        root.  The directory-only marker must not compete with that real
        identity NFO, and a stale artifact carrier must not raise drift.
        """
        source = "/library/待刮削/Container"
        self.alist.files[f"{source}/child.mkv"] = FAKE_VIDEO_BYTES
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=RecordingTMDB(),
            library_root="/library",
        )
        root = runner.create_pending_job(source, job_id="engine-root-owned")
        root = runner.start_automatic_job(root.id, target_shelf="anime")
        stale = runner.plan_container_artifacts(
            root_job_id=root.id,
            source_path=source,
            target_root="/library/番剧/Container",
            target_shelf="anime",
            container_title="Container",
            poster_path="/old.jpg",
            backdrop_path=None,
            representative_tmdb_id=8,
            job_id="container-artifacts-engine-root-owned",
        )
        runner.execute_job(stale.id)
        # The main work's own executed carrier now owns the container path
        # exactly; a late sibling demoted the root to the container form.
        self._persist_unit_carrier(
            runner,
            job_id="unit-main-carrier",
            root_job_id=root.id,
            target_root="/library/番剧/Container",
            metadata={"poster_path": "/main.jpg", "tmdb_id": 7},
        )
        save_work_unit_records(self.root, root.id, [
            WorkUnitRecord(
                work_unit_id="unit-a",
                root_task_id=root.id,
                boundary_key=f"{source}/A",
                source_paths=(f"{source}/A",),
                source_revision=1,
                role="single_work",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 7, "title": "A"},
                claimed_seasons=(1,),
                writer_job_id="unit-main-carrier",
            ),
            WorkUnitRecord(
                work_unit_id="unit-b",
                root_task_id=root.id,
                boundary_key=f"{source}/B",
                source_paths=(f"{source}/B",),
                source_revision=1,
                role="single_work",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 9, "title": "B"},
                claimed_seasons=(1,),
            ),
        ])
        uploads_before = list(self.alist.uploads)
        self.assertIsNone(ensure_container_artifacts(runner, self.root, root.id))
        self.assertEqual(self.alist.uploads, uploads_before)

    def test_container_representative_artwork_is_sticky_across_late_siblings(self) -> None:
        """A persisted container artwork identity does not drift on rerun.

        The representative is chosen once from the first proved child; a
        sibling that executes later (and sorts earlier) must not turn every
        following root run into an unconfirmable drift attention.  Only the
        loss of the provenance child reopens the choice.
        """
        source = "/library/待刮削/Container"
        self.alist.files[f"{source}/child.mkv"] = FAKE_VIDEO_BYTES
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=RecordingTMDB(),
            library_root="/library",
        )
        root = runner.create_pending_job(source, job_id="engine-root-sticky")
        root = runner.start_automatic_job(root.id, target_shelf="anime")
        self._persist_unit_carrier(
            runner,
            job_id="unit-carrier-b",
            root_job_id=root.id,
            target_root="/library/番剧/Container/B Work",
            metadata={"poster_path": "/first.jpg", "tmdb_id": 8},
        )
        records = [
            WorkUnitRecord(
                work_unit_id="unit-a",
                root_task_id=root.id,
                boundary_key=f"{source}/A",
                source_paths=(f"{source}/A",),
                source_revision=1,
                role="single_work",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 7, "title": "A"},
                claimed_seasons=(1,),
            ),
            WorkUnitRecord(
                work_unit_id="unit-b",
                root_task_id=root.id,
                boundary_key=f"{source}/B",
                source_paths=(f"{source}/B",),
                source_revision=1,
                role="single_work",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 8, "title": "B"},
                claimed_seasons=(1,),
                writer_job_id="unit-carrier-b",
            ),
        ]
        save_work_unit_records(self.root, root.id, records)
        carrier = ensure_container_artifacts(runner, self.root, root.id)
        self.assertIsNotNone(carrier)
        self.assertEqual(carrier.phase, "executed")
        # The late sibling executes and sorts before the representative.
        self._persist_unit_carrier(
            runner,
            job_id="unit-carrier-a",
            root_job_id=root.id,
            target_root="/library/番剧/Container/A Work",
            metadata={"poster_path": "/late.jpg", "tmdb_id": 7},
        )
        records[0] = replace(
            records[0], writer_job_id="unit-carrier-a",
        )
        save_work_unit_records(self.root, root.id, records)
        uploads_before = list(self.alist.uploads)
        repaired = ensure_container_artifacts(runner, self.root, root.id)
        self.assertIsNotNone(repaired)
        self.assertEqual(repaired.phase, "executed")
        self.assertEqual(self.alist.uploads, uploads_before)
        # Every proved child was rolled back: no executed sibling is left,
        # so the borrowed artwork has no owner and the pass fails closed.
        records[0] = replace(records[0], writer_job_id=None)
        records[1] = replace(records[1], writer_job_id=None)
        save_work_unit_records(self.root, root.id, records)
        with self.assertRaisesRegex(ContainerMetadataAttention, "代表单元已失效"):
            ensure_container_artifacts(runner, self.root, root.id)

    def test_rolled_back_representative_rebinds_to_a_proved_sibling(self) -> None:
        """Losing the provenance child reopens the artwork choice, not a park.

        A rolled-back representative parks the root only when no proved
        sibling is left.  When another executed child exists, the persisted
        carrier must rebind its artwork provenance under the same
        deterministic id instead of raising an attention that no surface can
        confirm.
        """
        source = "/library/待刮削/Container"
        self.alist.files[f"{source}/child.mkv"] = FAKE_VIDEO_BYTES
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=RecordingTMDB(),
            library_root="/library",
        )
        root = runner.create_pending_job(source, job_id="engine-root-rebind")
        root = runner.start_automatic_job(root.id, target_shelf="anime")
        self._persist_unit_carrier(
            runner,
            job_id="unit-carrier-b",
            root_job_id=root.id,
            target_root="/library/番剧/Container/B Work",
            metadata={"poster_path": "/first.jpg", "tmdb_id": 8},
        )
        records = [
            WorkUnitRecord(
                work_unit_id="unit-a",
                root_task_id=root.id,
                boundary_key=f"{source}/A",
                source_paths=(f"{source}/A",),
                source_revision=1,
                role="single_work",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 7, "title": "A"},
                claimed_seasons=(1,),
            ),
            WorkUnitRecord(
                work_unit_id="unit-b",
                root_task_id=root.id,
                boundary_key=f"{source}/B",
                source_paths=(f"{source}/B",),
                source_revision=1,
                role="single_work",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 8, "title": "B"},
                claimed_seasons=(1,),
                writer_job_id="unit-carrier-b",
            ),
        ]
        save_work_unit_records(self.root, root.id, records)
        carrier = ensure_container_artifacts(runner, self.root, root.id)
        self.assertIsNotNone(carrier)
        self.assertEqual(carrier.phase, "executed")
        self.assertEqual(
            carrier.plan["metadata"]["representative_tmdb_id"], 8,
        )
        # The representative's own write was rolled back, but its sibling
        # proved itself in the meantime.
        self._persist_unit_carrier(
            runner,
            job_id="unit-carrier-a",
            root_job_id=root.id,
            target_root="/library/番剧/Container/A Work",
            metadata={"poster_path": "/late.jpg", "tmdb_id": 7},
        )
        records[0] = replace(records[0], writer_job_id="unit-carrier-a")
        records[1] = replace(records[1], writer_job_id=None)
        save_work_unit_records(self.root, root.id, records)
        uploads_before = list(self.alist.uploads)
        rebound = ensure_container_artifacts(runner, self.root, root.id)
        self.assertIsNotNone(rebound)
        self.assertEqual(rebound.phase, "executed")
        self.assertEqual(rebound.id, carrier.id)
        metadata = rebound.plan["metadata"]
        self.assertEqual(metadata["representative_tmdb_id"], 7)
        self.assertEqual(metadata["container_poster_path"], "/late.jpg")
        self.assertEqual(self.alist.uploads, uploads_before)
        persisted = runner.get_job(carrier.id)
        self.assertEqual(
            persisted.plan["metadata"]["representative_tmdb_id"], 7,
        )

    def test_public_request_cannot_nominate_internal_workunit_target_scope(self) -> None:
        """Only the RootJob composition layer may set a WorkUnit scope."""
        with self.assertRaisesRegex(EngineRequestError, "内部范围"):
            EngineRequest.from_mapping({
                "source_path": "/incoming/movie",
                "parent_path": "/library/番剧",
                "media_type": "movie",
                "tmdb_id": 1,
                "target_scope_root": "/library/欧美剧/Foreign Work",
            })
        with self.assertRaisesRegex(EngineRequestError, "内部范围"):
            EngineRequest.from_mapping({
                "source_path": "/incoming/show",
                "parent_path": "/library/番剧",
                "media_type": "tv",
                "tmdb_id": 1,
                "allow_release_dash_ordinal": True,
            })
        with self.assertRaisesRegex(EngineRequestError, "内部范围"):
            EngineRequest.from_mapping({
                "source_path": "/incoming/show",
                "parent_path": "/library/番剧",
                "media_type": "tv",
                "tmdb_id": 1,
                "allow_release_title_ordinal": True,
            })

    def test_new_workunit_scope_requires_a_child_below_its_authorized_scope(self) -> None:
        """A selected shelf is not itself a valid new WorkUnit target."""
        scope = "/library/番剧"
        request = replace(
            self.request,
            parent_path=scope,
            target_scope_root=scope,
        )
        executor_calls: list[object] = []

        def plan_at_scope(_request: EngineRequest, _alist: object, _tmdb: object) -> Plan:
            return Plan(
                mode="movie",
                source_root="/incoming/movie",
                target_root=scope,
                files=[PlannedFile(
                    source_path="/incoming/movie/source.mkv",
                    source_dir="/incoming/movie",
                    original_name="source.mkv",
                    final_name="Scope Test.mkv",
                    target_dir=scope,
                    media_kind="video",
                    source_size=FAKE_VIDEO_SIZE,
                )],
                warnings=[],
                metadata={
                    "tmdb_id": 1,
                    "title": "Scope Test",
                    "year": "2020",
                    "poster_path": None,
                    "backdrop_path": None,
                },
            )

        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=plan_at_scope,
            validate=False,
            library_root="/library",
            executor=lambda plan: executor_calls.append(plan) or {"unexpected": True},
        )

        with self.assertRaisesRegex(EngineRequestError, "允许目标范围外"):
            runner.plan_job(request, job_id="engine-workunit-at-shelf")

        self.assertEqual(executor_calls, [])
        self.assertFalse((runner.jobs_root / "engine-workunit-at-shelf.json").exists())

    def test_d_locked_scope_cannot_treat_a_formal_shelf_as_a_work_root(self) -> None:
        """Malformed D evidence cannot authorize writing at a shelf root."""
        scope = "/library/番剧"
        request = replace(
            self.request,
            parent_path="/library",
            target_scope_root=scope,
        )
        executor_calls: list[object] = []

        def plan_at_scope(_request: EngineRequest, _alist: object, _tmdb: object) -> Plan:
            return Plan(
                mode="movie",
                source_root="/incoming/movie",
                target_root=scope,
                files=[PlannedFile(
                    source_path="/incoming/movie/source.mkv",
                    source_dir="/incoming/movie",
                    original_name="source.mkv",
                    final_name="Malformed D Scope.mkv",
                    target_dir=scope,
                    media_kind="video",
                    source_size=FAKE_VIDEO_SIZE,
                )],
                warnings=[],
                metadata={
                    "tmdb_id": 1,
                    "title": "Malformed D Scope",
                    "year": "2020",
                    "poster_path": None,
                    "backdrop_path": None,
                },
            )

        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=plan_at_scope,
            validate=False,
            library_root="/library",
            executor=lambda plan: executor_calls.append(plan) or {"unexpected": True},
        )

        with self.assertRaisesRegex(EngineRequestError, "允许目标范围外"):
            runner.plan_job(request, job_id="engine-workunit-malformed-d-root")

        self.assertEqual(executor_calls, [])
        self.assertFalse(
            (runner.jobs_root / "engine-workunit-malformed-d-root.json").exists()
        )

    def test_persisted_workunit_scope_blocks_escape_before_execution_and_recovery(self) -> None:
        """A tampered WorkUnit carrier cannot escape its original shelf."""
        scope = "/library/番剧"
        request = replace(
            self.request,
            parent_path=scope,
            target_scope_root=scope,
        )
        executor_calls: list[object] = []

        def scoped_plan(engine_request: EngineRequest, _alist: object, _tmdb: object) -> Plan:
            target = f"{engine_request.parent_path}/Scoped Work (1)"
            return Plan(
                mode="movie",
                source_root="/incoming/movie",
                target_root=target,
                files=[PlannedFile(
                    source_path="/incoming/movie/source.mkv",
                    source_dir="/incoming/movie",
                    original_name="source.mkv",
                    final_name="Scoped Work.mkv",
                    target_dir=target,
                    media_kind="video",
                    source_size=FAKE_VIDEO_SIZE,
                )],
                warnings=[],
                metadata={
                    "tmdb_id": 1,
                    "title": "Scoped Work",
                    "year": "2020",
                    "poster_path": None,
                    "backdrop_path": None,
                },
            )

        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=scoped_plan,
            validate=False,
            library_root="/library",
            executor=lambda plan: executor_calls.append(plan) or {"unexpected": True},
        )
        job = runner.plan_job(request, job_id="engine-workunit-persisted-scope")
        escaped_root = "/library/欧美剧/Escaped Work (1)"
        tampered_plan = dict(job.plan)
        tampered_plan["target_root"] = escaped_root
        tampered_plan["files"] = [
            {**item, "target_dir": escaped_root}
            for item in (job.plan.get("files") or [])
        ]
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - persisted-plan guard fixture
            replace(job, plan=tampered_plan).as_dict(),
            allow_nan=False,
        )

        with self.assertRaisesRegex(EngineRequestError, "允许目标范围外"):
            runner.execute_job(job.id)
        self.assertEqual(executor_calls, [])

        active = runner.get_job(job.id)
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - persisted recovery fixture
            replace(active, phase="failed").as_dict(),
            allow_nan=False,
        )
        recovered = runner.recover_job(job.id)

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(
            recovered.summary.get("recovery", {}).get("reason"),
            "target_shelf_policy_violation",
        )
        self.assertEqual(executor_calls, [])

    def test_pause_boundary_preserves_executing_job_without_writer_replay(self) -> None:
        events: list[str] = []
        paused = {"value": False}

        def executor(_plan):
            events.append("writer-start")
            paused["value"] = True
            # The concrete executor's next checkpoint observes the pause;
            # this injected stand-in models the same boundary explicitly.
            raise EnginePauseRequested("paused")

        runner = SimpleEngineRunner(
            self.root, alist=self.alist, tmdb=object(), planner=fake_plan,
            validate=False, executor=executor,
        )
        planned = runner.plan_job(self.request, job_id="pause-preserve")
        done = runner.execute_job(
            planned.id,
            pause_requested=lambda: paused["value"],
        )
        self.assertEqual(done.phase, "executing")
        self.assertEqual(events, ["writer-start"])
        self.assertIn("active_operation", runner.get_job(done.id).summary)

    def test_optional_archive_preprocessor_replaces_ordinary_source_before_plan(self) -> None:
        adapter = RecordingArchivePreprocessor()
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"ok": True},
            archive_preprocessor=adapter,
        )
        job = runner.plan_job(self.request, job_id="engine-archive-hook")
        self.assertEqual(job.request["source_path"], "/task-staging/archive")
        self.assertEqual(job.plan["source_root"], "/incoming/movie")
        self.assertEqual(len(adapter.calls), 1)

    def test_plan_job_fences_legacy_adapter_remote_call_when_scope_closes(self) -> None:
        """The runner proxy protects adapters that ignore the new callback."""
        paused = {"value": False}

        class RecordingAList(FakeAList):
            def __init__(self):
                super().__init__()
                self.mkdir_calls: list[str] = []

            def mkdir(self, path: str) -> None:
                self.mkdir_calls.append(path)

        class ScopeClosingAdapter:
            def prepare_ordinary_request(self, request, *, alist, **_kwargs):
                paused["value"] = True
                # This adapter intentionally ignores `pause_requested` to
                # exercise the runner-owned AList proxy.
                alist.mkdir("/quark/影视/ScrapeFlow/归档/should-not-exist")
                return request

        alist = RecordingAList()
        alist.files["/incoming/movie/source.mkv"] = FAKE_VIDEO_BYTES
        runner = SimpleEngineRunner(
            self.root,
            alist=alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            archive_preprocessor=ScopeClosingAdapter(),
        )

        with self.assertRaises(EnginePauseRequested):
            runner.plan_job(
                self.request,
                job_id="archive-scope-fence",
                pause_requested=lambda: paused["value"],
            )

        self.assertEqual(alist.mkdir_calls, [])
        self.assertFalse((runner.jobs_root / "archive-scope-fence.json").exists())

    def test_automatic_archive_precedes_identity_and_planning(self) -> None:
        events: list[str] = []
        adapter = OrderedArchivePreprocessor(events)
        runner = SimpleEngineRunner(
            self.root, alist=self.alist, tmdb=object(), planner=fake_plan,
            validate=False, executor=lambda _plan: events.append("writer") or {"ok": True},
            archive_preprocessor=adapter,
        )
        match = SimpleNamespace(
            media_type="movie", tmdb_id=1, title="Movie", year="2020",
            confidence=0.99, decision_trace={},
        )
        with patch("engine.scraper.auto_match_tmdb", side_effect=lambda *args, **kwargs: events.append("resolve_identity") or (match, [])):
            job = runner.plan_automatic(
                "/incoming/archive",
                job_id="auto-archive-order",
                target_shelf="movie",
            )
        self.assertEqual(events, ["archive_preprocess", "resolve_identity"])
        self.assertEqual(job.request["source_path"], "/task-staging/archive")
        self.assertEqual(job.summary["ingress_source_path"], "/incoming/archive")
        self.assertEqual(job.phase, "planned")
        self.assertNotIn("writer", events)

    def test_automatic_archive_failure_never_calls_identity_or_writer(self) -> None:
        events: list[str] = []
        runner = SimpleEngineRunner(
            self.root, alist=self.alist, tmdb=object(), planner=fake_plan,
            validate=False, executor=lambda _plan: events.append("writer") or {"ok": True},
            archive_preprocessor=OrderedArchivePreprocessor(events, fail=True),
        )
        self.alist.directories.add("/incoming/archive")
        waiting = self._new_work_waiting(runner, "/incoming/archive", job_id="auto-archive-fail")
        queued = runner.start_automatic_job(waiting.id, target_shelf="movie")
        with self.assertRaises(RuntimeError):
            runner.plan_automatic_job(queued.id)
        self.assertEqual(events, ["archive_preprocess"])
        self.assertNotIn("resolve_identity", events)
        self.assertNotIn("writer", events)
        self.assertEqual(runner._read(queued.id).phase, "archive_preprocessing")

    def test_boundary_analysis_runs_before_identity_and_persists_work_units(self) -> None:
        """B/W must complete before any TMDB identity work (contract rule 3)."""
        events: list[str] = []
        runner = SimpleEngineRunner(
            self.root, alist=self.alist, tmdb=object(), planner=fake_plan,
            validate=False, executor=lambda _plan: {"ok": True},
        )
        # Fate-style container: two titled children each carrying videos.
        self.alist.files["/incoming/fate/Fate Zero/01.mkv"] = FAKE_VIDEO_BYTES
        self.alist.files["/incoming/fate/Fate Zero/02.mkv"] = FAKE_VIDEO_BYTES
        self.alist.files["/incoming/fate/Fate Zero/03.mkv"] = FAKE_VIDEO_BYTES
        self.alist.files["/incoming/fate/Fate Zero/04.mkv"] = FAKE_VIDEO_BYTES
        self.alist.files["/incoming/fate/Fate Stay Night UBW/Season 01/S01E01.mkv"] = FAKE_VIDEO_BYTES
        match = SimpleNamespace(
            media_type="tv", tmdb_id=35507, title="Fate/Zero", year="2011",
            confidence=0.99, decision_trace={},
        )
        waiting = self._new_work_waiting(runner, "/incoming/fate", job_id="auto-boundary")
        queued = runner.start_automatic_job(waiting.id, target_shelf="anime")
        with patch(
            "engine.scraper.auto_match_tmdb",
            side_effect=lambda *args, **kwargs: events.append("resolve_identity") or (match, []),
        ):
            job = runner.plan_automatic_job(queued.id)
        self.assertEqual(job.phase, "planned")
        self.assertIn("resolve_identity", events)
        from engine.scrapeflow.work_units import load_work_unit_records

        records = load_work_unit_records(self.root, "auto-boundary")
        self.assertEqual(len(records), 2)
        self.assertEqual(
            {record.boundary_key for record in records},
            {
                "/incoming/fate/Fate Zero",
                "/incoming/fate/Fate Stay Night UBW",
            },
        )
        self.assertTrue(all(record.identity_status == "pending" for record in records))

    def test_existing_formal_target_is_terminal_planning_conflict_without_retry(self) -> None:
        alist = TargetConflictAList()
        source_root = "/library/待刮削/movie"
        source = f"{source_root}/source.mkv"
        target = "/library/电影/Movie (2020)/Movie (2020).mkv"
        alist.files[source] = FAKE_VIDEO_BYTES
        alist.files[target] = b"existing formal object"
        writer_calls: list[str] = []

        def selected_shelf_plan(request: EngineRequest, *_args: object) -> Plan:
            target_root = f"{request.parent_path.rstrip('/')}/Movie (2020)"
            return Plan(
                mode="movie",
                source_root=source_root,
                target_root=target_root,
                files=[PlannedFile(
                    source_path=source,
                    source_dir=source_root,
                    original_name="source.mkv",
                    final_name="Movie (2020).mkv",
                    target_dir=target_root,
                    media_kind="video",
                    source_size=FAKE_VIDEO_SIZE,
                )],
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

        runner = SimpleEngineRunner(
            self.root,
            alist=alist,
            tmdb=object(),
            planner=selected_shelf_plan,
            validate=True,
            executor=lambda _plan: writer_calls.append("writer") or {"ok": True},
            library_root="/library",
        )
        identity = AutomaticIdentity(
            media_type="movie",
            tmdb_id=1,
            title="Movie",
            year="2020",
            confidence=0.99,
            target_parent="/library/电影",
            season=None,
            trace={},
            target_shelf="movie",
            target_shelf_root="/library/电影",
        )
        runner.resolve_automatic_request = lambda _source, **_kwargs: (  # type: ignore[method-assign]
            replace(
                self.request,
                source_path=source_root,
                parent_path="/library/电影",
                target_shelf="movie",
            ),
            identity,
        )
        waiting = self._new_work_waiting(
            runner, source_root, job_id="auto-existing-formal-target",
        )
        queued = runner.start_automatic_job(waiting.id, target_shelf="movie")

        failed = runner.plan_automatic_job(queued.id)

        self.assertEqual(failed.phase, "failed_planning")
        # 存量清退：automatic_stage 镜像已不再由 runner 续写，phase 为唯一权威。
        self.assertNotIn("automatic_stage", failed.summary)
        self.assertNotIn("automatic_terminal", failed.summary)
        self.assertNotIn("automatic_attempts", failed.summary)
        self.assertNotIn("next_retry_seconds", failed.summary)
        self.assertEqual(failed.plan, {})
        self.assertIsNone(failed.execution)
        self.assertNotIn("active_operation", failed.summary)
        self.assertIn("目标目录已存在同名文件", failed.error or "")
        self.assertEqual(writer_calls, [])
        self.assertIn(source, alist.files)
        self.assertEqual(alist.files[target], b"existing formal object")
        self.assertEqual(alist.moves, [])

    def test_non_target_planning_error_remains_retryable(self) -> None:
        alist = TargetConflictAList()
        alist.files["/incoming/movie/source.mkv"] = FAKE_VIDEO_BYTES
        runner = SimpleEngineRunner(
            self.root,
            alist=alist,
            tmdb=object(),
            planner=fake_plan,
            validate=True,
        )
        identity = AutomaticIdentity(
            media_type="movie",
            tmdb_id=1,
            title="Movie",
            year="2020",
            confidence=0.99,
            target_parent="/quark/影视/电影",
            season=None,
            trace={},
            target_shelf="movie",
            target_shelf_root="/quark/影视/电影",
        )
        runner.resolve_automatic_request = lambda _source, **_kwargs: (  # type: ignore[method-assign]
            replace(
                self.request,
                parent_path="/quark/影视/电影",
                target_shelf="movie",
            ),
            identity,
        )
        waiting = self._new_work_waiting(
            runner, "/incoming/movie", job_id="auto-retryable-planning-error",
        )
        queued = runner.start_automatic_job(waiting.id, target_shelf="movie")

        with patch(
            "engine.scraper.validate_plan",
            side_effect=PlanError("目标目录暂时无法读取"),
        ), self.assertRaisesRegex(PlanError, "暂时无法读取"):
            runner.plan_automatic_job(queued.id)

        retryable = runner.get_job(queued.id)
        self.assertEqual(retryable.phase, "planning")
        self.assertNotIn("automatic_terminal", retryable.summary)
        self.assertNotIn("automatic_attempts", retryable.summary)
        self.assertEqual(retryable.plan, {})
        self.assertIsNone(retryable.execution)

    def test_validate_plan_uses_typed_error_only_for_existing_formal_target(self) -> None:
        alist = TargetConflictAList()
        plan = fake_plan(self.request, alist, object())
        alist.files["/incoming/movie/source.mkv"] = FAKE_VIDEO_BYTES
        alist.files["/library/Movie (2020)/Movie (2020).mkv"] = b"existing"

        with self.assertRaises(FormalTargetConflictError):
            validate_plan(alist, plan)

    def test_wrong_archive_password_is_immediately_terminal_and_preserves_source(self) -> None:
        events: list[str] = []
        source_file = "/incoming/archive/payload.7z"
        self.alist.files[source_file] = b"7z\xbc\xaf'\x1c"

        def planner(*_args):
            events.append("planning")
            return fake_plan(self.request, self.alist, object())

        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=planner,
            validate=False,
            executor=lambda _plan: events.append("writer") or {"ok": True},
            archive_preprocessor=FailingArchivePreprocessor(
                ArchivePasswordError(
                    "archive extraction failed; password candidates exhausted"
                ),
                events,
            ),
        )
        waiting = self._new_work_waiting(
            runner, "/incoming/archive", job_id="auto-archive-wrong-password"
        )
        queued = runner.start_automatic_job(waiting.id, target_shelf="movie")

        with patch("engine.scraper.auto_match_tmdb") as identity, self.assertRaises(
            ArchivePasswordError
        ):
            runner.plan_automatic_job(queued.id, retry_password="wrong")

        failed = runner.get_job(queued.id)
        self.assertEqual(failed.phase, "failed_archive")
        # 存量清退：automatic_stage 镜像已不再由 runner 续写，phase 为唯一权威。
        self.assertNotIn("automatic_stage", failed.summary)
        self.assertNotIn("automatic_terminal", failed.summary)
        self.assertNotIn("automatic_attempts", failed.summary)
        self.assertEqual(events, ["archive_preprocess"])
        identity.assert_not_called()
        self.assertIn(source_file, self.alist.files)

    def test_archive_tool_failure_keeps_phase_for_explicit_retry(self) -> None:
        events: list[str] = []
        source_file = "/incoming/archive/payload.7z"
        self.alist.files[source_file] = b"7z\xbc\xaf'\x1c"
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: events.append("writer") or {"ok": True},
            archive_preprocessor=FailingArchivePreprocessor(
                ArchiveToolError("7-Zip executable is unavailable"), events
            ),
        )
        waiting = self._new_work_waiting(
            runner, "/incoming/archive", job_id="auto-archive-tool-unavailable"
        )
        queued = runner.start_automatic_job(waiting.id, target_shelf="movie")

        with self.assertRaises(ArchiveToolError):
            runner.plan_automatic_job(queued.id)

        retryable = runner.get_job(queued.id)
        self.assertEqual(retryable.phase, "archive_preprocessing")
        self.assertNotIn("automatic_terminal", retryable.summary)
        self.assertEqual(events, ["archive_preprocess"])
        self.assertIn(source_file, self.alist.files)

    def test_successful_archive_source_is_moved_to_task_owned_processed_area(self) -> None:
        alist = ArchiveLifecycleAList()
        alist.directories.add("/incoming/archive")
        runner = SimpleEngineRunner(
            self.root, alist=alist, tmdb=object(), planner=fake_plan,
            validate=False, executor=lambda _plan: {"ok": True}, library_root="/library",
        )
        job = runner.create_automatic_job("/incoming/archive", job_id="archive-consume")
        prepared = replace(
            job,
            phase="planned",
            request={"source_path": "/task-staging/archive"},
            summary={
                **job.summary,
                "ingress_source_path": "/incoming/archive",
                "archive_preprocessed": {"changed": True, "ingress": "archive"},
            },
        )
        atomic_write_json(runner._job_path(job.id), prepared.as_dict(), allow_nan=False)
        consumed = runner._consume_archive_source(prepared)  # noqa: SLF001 - lifecycle boundary
        self.assertEqual(consumed["status"], "moved_to_processed")
        self.assertTrue(str(consumed["target"]).startswith("/library/ScrapeFlow/归档/archive-consume/processed/"))

    def test_successful_normal_video_uses_the_same_archive_processed_lane(self) -> None:
        alist = ArchiveLifecycleAList()
        alist.directories.add("/incoming/normal")
        runner = SimpleEngineRunner(
            self.root, alist=alist, tmdb=object(), planner=fake_plan,
            validate=False, executor=lambda _plan: {"ok": True}, library_root="/library",
        )
        job = runner.create_automatic_job("/incoming/normal", job_id="normal-consume")
        prepared = replace(
            job,
            phase="executed",
            summary={**job.summary, "automatic": True, "ingress_source_path": "/incoming/normal"},
        )
        atomic_write_json(runner._job_path(job.id), prepared.as_dict(), allow_nan=False)

        consumed = runner._consume_archive_source(prepared)  # noqa: SLF001 - lifecycle boundary

        self.assertEqual(consumed["status"], "moved_to_processed")
        self.assertTrue(str(consumed["target"]).startswith("/library/ScrapeFlow/归档/normal-consume/processed/"))

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

    def test_current_finalizer_uses_content_proof_priority_for_managed_tracks(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        bilingual = PlannedFile(
            source_path="/incoming/movie/source.zh-CN-bilingual-ja.srt",
            source_dir="/incoming/movie",
            original_name="source.zh-CN-bilingual-ja.srt",
            final_name="Movie (2020).zh-CN-bilingual-ja.srt",
            target_dir="/library/Movie (2020)",
            media_kind="subtitle",
            source_size=100,
            subtitle_validation={
                "status": "satisfied", "selection": "bilingual", "preference": 0,
                "source_path": "/incoming/movie/source.zh-CN-bilingual-ja.srt",
                "source_name": "source.zh-CN-bilingual-ja.srt", "size": 100,
            },
        )
        simplified = PlannedFile(
            source_path="/incoming/movie/source.zh-CN.srt",
            source_dir="/incoming/movie",
            original_name="source.zh-CN.srt",
            final_name="Movie (2020).zh-CN.srt",
            target_dir="/library/Movie (2020)",
            media_kind="subtitle",
            source_size=100,
            subtitle_validation={
                "status": "satisfied", "selection": "simplified_chinese", "preference": 1,
                "source_path": "/incoming/movie/source.zh-CN.srt",
                "source_name": "source.zh-CN.srt", "size": 100,
            },
        )
        traditional = PlannedFile(
            source_path="/incoming/movie/source.zh-TW.srt",
            source_dir="/incoming/movie",
            original_name="source.zh-TW.srt",
            final_name="Movie (2020).zh-TW.srt",
            target_dir="/library/Movie (2020)",
            media_kind="subtitle",
            source_size=100,
            subtitle_validation={
                "status": "satisfied", "selection": "traditional_chinese", "preference": 2,
                "source_path": "/incoming/movie/source.zh-TW.srt",
                "source_name": "source.zh-TW.srt", "size": 100,
            },
        )
        plan.files.extend([bilingual, simplified, traditional])

        finalized = finalize_plan(plan)

        self.assertIn(bilingual.source_path, {item.source_path for item in finalized.files})
        self.assertNotIn(simplified.source_path, {item.source_path for item in finalized.files})
        self.assertNotIn(traditional.source_path, {item.source_path for item in finalized.files})
        deferred = finalized.scan_report["deferred_subtitles"]
        self.assertEqual(
            {row["source_path"] for row in deferred},
            {simplified.source_path, traditional.source_path},
        )
        self.assertTrue(all(row["reason"] == "managed_subtitle_lower_priority" for row in deferred))

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

    def test_smart_preclassifies_theme_and_mixed_menu_without_episode_false_positive(self) -> None:
        # The directory is deliberately NOT named like a bonus folder: the
        # files' own labels drive the classification here.  A real
        # ``Menu/``/``EXTRA/`` directory is covered by the shared bonus
        # vocabulary and removes everything inside it.
        source = "/incoming/Release"
        files = [
            {"name": "[OP].mkv", "full_path": source + "/Plain/[OP].mkv"},
            {"name": "[ED].mkv", "full_path": source + "/Plain/[ED].mkv"},
            {"name": "Show Menu - 01.mkv", "full_path": source + "/Show Menu - 01.mkv"},
            {"name": "Show Menu - [NCOP].mkv", "full_path": source + "/Show Menu - [NCOP].mkv"},
        ]
        retained, residuals, withheld_bonus = _preclassify_theme_residuals(files)
        self.assertEqual({item["name"] for item in retained}, {"Show Menu - 01.mkv"})
        self.assertEqual(len(residuals), 3)
        self.assertEqual(withheld_bonus, [])
        self.assertTrue(all(item["action"] == "preserve_at_source" for item in residuals))
        self.assertTrue(all(item["reason"] == "no_write_source_residual" for item in residuals))

    def test_preclassifier_withholds_bonus_directory_videos_for_official_evidence(self) -> None:
        # A named mini-series under ``SPs/`` is removed from the episode
        # parser exactly as before, but is returned in the third bucket so
        # the smart planner can retry it against official Season 00
        # evidence; the residual row keeps the member fail-closed until an
        # evidence mapper re-admits it.
        source = "/incoming/Release"
        files = [
            {
                "name": "Mini Anime - 01.mkv",
                "full_path": source + "/SPs/Mini Anime - 01.mkv",
            },
            {
                "name": "Mini Anime - 02.mkv",
                "full_path": source + "/SPs/Mini Anime - 02.mkv",
            },
            {
                "name": "Show - 01.mkv",
                "full_path": source + "/Show - 01.mkv",
            },
        ]
        retained, residuals, withheld_bonus = _preclassify_theme_residuals(files)
        self.assertEqual(
            {item["name"] for item in retained}, {"Show - 01.mkv"}
        )
        self.assertEqual(
            {item["name"] for item in withheld_bonus},
            {"Mini Anime - 01.mkv", "Mini Anime - 02.mkv"},
        )
        self.assertEqual(
            {row["source_path"] for row in residuals},
            {
                source + "/SPs/Mini Anime - 01.mkv",
                source + "/SPs/Mini Anime - 02.mkv",
            },
        )

    def test_sole_season_bare_run_merges_via_official_alternative_title(self) -> None:
        """A romaji bare run merges through the official alias list.

        A release titled only with an official alternative title (romaji
        transliteration) matches neither the zh-CN ``name`` nor the ja-JP
        ``original_name``.  Without the alias the bare run stays unknown,
        the long season loses its base season group, and a reset-numbered
        second release folder can no longer map onto the post-gap segment.
        """

        class AliasAList:
            def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
                del refresh
                return []

            def walk(self, _path: str, **_kwargs: object) -> list[dict[str, object]]:
                return []

        class AliasTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/211":
                    return {
                        "name": "北风物语",
                        "original_name": "北風物語",
                        "first_air_date": "2020-01-01",
                        "seasons": [{"season_number": 1, "episode_count": 6}],
                    }
                if path == "/tv/211/season/1":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "name": f"第{number}话",
                                "air_date": (
                                    f"2020-01-{1 + 2 * number:02d}"
                                    if number <= 3
                                    else f"2021-01-{2 * number:02d}"
                                ),
                                "runtime": 24,
                            }
                            for number in range(1, 7)
                        ],
                    }
                if path == "/tv/211/season/0":
                    return {"episodes": []}
                if path == "/tv/211/alternative_titles":
                    return {"results": [{"title": "Kita Kaze Monogatari"}]}
                if path.startswith("/search/"):
                    return {"results": []}
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_root = "/quark/影视/待刮削/KitaKaze"
        source_files = [
            *(
                {
                    "name": f"[Grp] Kita Kaze Monogatari [{number:02d}].mkv",
                    "full_path": source_root
                    + f"/[Grp] Kita Kaze Monogatari [{number:02d}].mkv",
                    "size": FAKE_VIDEO_SIZE,
                    "is_dir": False,
                }
                for number in (1, 2, 3)
            ),
            *(
                {
                    "name": f"[Grp] Kita Kaze Monogatari 2nd Season - {number:02d}.mkv",
                    "full_path": source_root
                    + f"/第二季/[Grp] Kita Kaze Monogatari 2nd Season - {number:02d}.mkv",
                    "size": FAKE_VIDEO_SIZE,
                    "is_dir": False,
                }
                for number in (1, 2, 3)
            ),
        ]
        plan = build_tv_plan_smart(
            auto_episode_mode=True,
            alist=AliasAList(),
            tmdb_client=AliasTMDB(),
            src_path=source_root,
            parent_path="/quark/影视/番剧",
            tmdb_id=211,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            source_files=source_files,
            source_declared_seasons=(1, 2),
            media_root="/quark/影视",
        )
        self.assertEqual(
            sorted(item.episode_key for item in plan.files),
            ["E01", "E02", "E03", "E04", "E05", "E06"],
        )
        season_two_sources = {
            item.source_path
            for item in plan.files
            if "/第二季/" in item.source_path
        }
        self.assertEqual(len(season_two_sources), 3)
        self.assertTrue(
            all(
                item.episode_key in {"E04", "E05", "E06"}
                for item in plan.files
                if "/第二季/" in item.source_path
            )
        )

    def test_smart_plan_reclaims_only_officially_proven_sp_directory_mini_series(self) -> None:
        """``SPs/`` members reach Season 00 only with official ordinal proof.

        The preclassifier withholds bonus-directory videos from the episode
        parser, but once the multilingual Season 00 rows are loaded a
        reset-numbered mini-series whose official titles embed their own
        ``第N话`` ordinals must be re-admitted as specials.  Without that
        official numbering the same directory stays fail-closed residuals.
        """

        class ReclaimAList:
            def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
                del refresh
                return []

            def walk(self, _path: str, **_kwargs: object) -> list[dict[str, object]]:
                return []

        def make_tmdb(special_names: dict[int, str]) -> object:
            class PlannerTMDB:
                def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                    if path == "/tv/210":
                        return {
                            "name": "Northwind Show",
                            "original_name": "Northwind Show",
                            "first_air_date": "2020-01-01",
                            "seasons": [{"season_number": 1, "episode_count": 2}],
                        }
                    if path == "/tv/210/season/1":
                        return {
                            "episodes": [
                                {
                                    "episode_number": 1,
                                    "name": "启程",
                                    "air_date": "2020-01-01",
                                    "runtime": 24,
                                },
                                {
                                    "episode_number": 2,
                                    "name": "山道",
                                    "air_date": "2020-01-08",
                                    "runtime": 24,
                                },
                            ],
                        }
                    if path == "/tv/210/season/0":
                        return {
                            "episodes": [
                                {
                                    "episode_number": number,
                                    "name": name,
                                    "air_date": f"2020-02-{2 * number:02d}",
                                    "runtime": 3,
                                }
                                for number, name in sorted(special_names.items())
                            ],
                        }
                    if path == "/tv/210/alternative_titles":
                        return {"results": []}
                    raise AssertionError(f"unexpected TMDB path: {path}")

            return PlannerTMDB()

        source_root = "/quark/影视/待刮削/Northwind"
        source_files = [
            {
                "name": "Northwind.Show.S01E01.mkv",
                "full_path": source_root + "/Northwind.Show.S01E01.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            },
            {
                "name": "Northwind.Show.S01E02.mkv",
                "full_path": source_root + "/Northwind.Show.S01E02.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            },
            *(
                {
                    "name": f"Mini Anime - {number:02d}.mkv",
                    "full_path": source_root + f"/SPs/Mini Anime - {number:02d}.mkv",
                    "size": FAKE_VIDEO_SIZE,
                    "is_dir": False,
                }
                for number in (1, 2, 3)
            ),
        ]
        kwargs = {
            "auto_episode_mode": True,
            "alist": ReclaimAList(),
            "tmdb_client": make_tmdb({
                1: "第1话 迷你动画 启程",
                2: "第2话 迷你动画 山道",
                3: "第3话 迷你动画 归途",
            }),
            "src_path": source_root,
            "parent_path": "/quark/影视/番剧",
            "tmdb_id": 210,
            "season": 1,
            "absolute": False,
            "prefer_simplified": True,
            "allow_unmapped": False,
            "ignore_orphan_temp": False,
            "source_files": source_files,
            "source_declared_seasons": (1,),
            "media_root": "/quark/影视",
        }

        plan = build_tv_plan_smart(**kwargs)
        self.assertEqual(
            sorted(item.episode_key for item in plan.files),
            ["E01", "E02", "SP01", "SP02", "SP03"],
        )
        special_names = sorted(
            item.final_name
            for item in plan.files
            if item.episode_key.startswith("SP")
        )
        self.assertTrue(all("S00E" in name for name in special_names))
        self.assertEqual(len(special_names), 3)
        residuals = plan.scan_report.get("preserved_source_residuals", [])
        self.assertFalse(
            any("/SPs/" in str(row.get("source_path", "")) for row in residuals)
        )

        # No embedded official ordinals: the same SPs directory must stay a
        # fail-closed preserved-at-source residual instead of being guessed.
        unnumbered = build_tv_plan_smart(
            **{
                **kwargs,
                "tmdb_client": make_tmdb({
                    1: "迷你动画 启程",
                    2: "迷你动画 山道",
                    3: "迷你动画 归途",
                }),
            }
        )
        self.assertEqual(
            sorted(item.episode_key for item in unnumbered.files),
            ["E01", "E02"],
        )
        self.assertEqual(
            {
                str(row.get("source_path", ""))
                for row in unnumbered.scan_report.get(
                    "preserved_source_residuals", []
                )
            },
            {
                source_root + f"/SPs/Mini Anime - {number:02d}.mkv"
                for number in (1, 2, 3)
            },
        )

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
        self.assertNotIn("provider_media_only", reloaded.summary)
        self.assertNotIn("provider_media_only", reloaded.plan["metadata"])
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
        self.assertNotIn("provider_media_only", persisted.summary)
        self.assertNotIn("provider_media_only", persisted.plan["metadata"])

    def test_video_admission_configuration_has_an_immutable_floor(self) -> None:
        with patch.dict("os.environ", {"SCRAPEFLOW_MIN_VIDEO_BYTES": "1"}):
            self.assertEqual(minimum_video_bytes(), ABSOLUTE_MINIMUM_VIDEO_BYTES)

    def test_executor_accepts_task_staging_path(self) -> None:
        source = "/quark/影视/ScrapeFlow/补源/root-1/attempt-1/source.mkv"
        plan = fake_plan(self.request, self.alist, object())
        plan.source_root = "/quark/影视/ScrapeFlow/补源/root-1/attempt-1"
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

    def test_plan_validation_refuses_disc_image_even_if_serialized_as_video(self) -> None:
        """A forged/legacy plan cannot bypass B/W by relabelling an ISO."""
        plan = fake_plan(self.request, self.alist, object())
        item = plan.files[0]
        item.source_path = "/incoming/movie/disc.iso"
        item.source_dir = "/incoming/movie"
        item.original_name = "disc.iso"
        item.final_name = "Movie (2020).iso"
        item.media_kind = "video"
        item.source_size = 45 * 1024**3

        with self.assertRaisesRegex(PlanError, "光盘镜像容器"):
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

    def test_executor_rejects_a_stale_unsafe_final_before_any_move(self) -> None:
        plan = fake_plan(self.request, self.alist, object())
        plan.files[0].final_name = "Movie...Legacy.mkv"

        with self.assertRaisesRegex(EngineExecutionError, "AList 不兼容"):
            SimplePlanExecutor(self.alist).execute(plan)

        self.assertEqual(self.alist.moves, [])
        self.assertEqual(self.alist.renames, [])
        self.assertIn("/incoming/movie/source.mkv", self.alist.files)

    def test_failed_legacy_plan_migrates_and_renames_exact_intermediate(self) -> None:
        """A rejected post-move rename remains one durable continuation.

        The old plan has a provider-incompatible title.  Its source has
        already moved to the target under the original basename, so retrying
        must atomically repair the plan and issue only that same-directory
        rename—never re-plan or re-move a missing source.
        """
        def legacy_plan(request, alist, tmdb):  # type: ignore[no-untyped-def]
            plan = fake_plan(request, alist, tmdb)
            plan.files[0].final_name = "Movie...Legacy.mkv"
            return plan

        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=legacy_plan,
            validate=False,
        )
        job = runner.plan_job(self.request, job_id="engine-legacy-basename")
        intermediate = "/library/Movie (2020)/source.mkv"
        self.alist.files[intermediate] = self.alist.files.pop(
            "/incoming/movie/source.mkv",
        )
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - durable failure fixture
            replace(job, phase="failed", error="rename rejected").as_dict(),
            allow_nan=False,
        )

        recovered = runner.recover_job(job.id)

        self.assertEqual(recovered.phase, "retry_wait")
        migrated_final = "Movie-Legacy.mkv"
        self.assertEqual(recovered.plan["files"][0]["final_name"], migrated_final)
        migration = recovered.summary["remote_basename_migration"]
        self.assertEqual(migration["status"], "prepared")
        self.assertEqual(migration["files"][0]["state"], "rename_pending")
        self.assertIn(intermediate, self.alist.files)
        self.assertEqual(self.alist.moves, [])

        resumed = runner.execute_job(recovered.id)

        target = f"/library/Movie (2020)/{migrated_final}"
        self.assertEqual(resumed.phase, "executed")
        self.assertIn(target, self.alist.files)
        self.assertNotIn(intermediate, self.alist.files)
        self.assertNotIn("/incoming/movie/source.mkv", self.alist.files)
        self.assertEqual(self.alist.moves, [])
        self.assertEqual(self.alist.renames, [(intermediate, migrated_final)])
        execution = resumed.execution or {}
        self.assertEqual(
            execution["files"][0]["status"],
            "renamed_after_interrupted_move",
        )

    def test_legacy_plan_refuses_a_preexisting_canonical_target(self) -> None:
        def legacy_plan(request, alist, tmdb):  # type: ignore[no-untyped-def]
            plan = fake_plan(request, alist, tmdb)
            plan.files[0].final_name = "Movie...Legacy.mkv"
            return plan

        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=legacy_plan,
            validate=False,
        )
        job = runner.plan_job(self.request, job_id="engine-legacy-collision")
        intermediate = "/library/Movie (2020)/source.mkv"
        canonical = "/library/Movie (2020)/Movie-Legacy.mkv"
        self.alist.files[intermediate] = self.alist.files.pop(
            "/incoming/movie/source.mkv",
        )
        self.alist.files[canonical] = FAKE_VIDEO_BYTES
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - durable failure fixture
            replace(job, phase="failed", error="rename rejected").as_dict(),
            allow_nan=False,
        )

        recovered = runner.recover_job(job.id)

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(
            recovered.summary["recovery"]["reason"],
            "canonical_target_collision",
        )
        self.assertIn(intermediate, self.alist.files)
        self.assertIn(canonical, self.alist.files)
        self.assertEqual(self.alist.moves, [])
        self.assertEqual(self.alist.renames, [])

    def test_internal_child_uses_normal_artifacts_and_preserves_existing_root_metadata(self) -> None:
        plan = provider_media_plan(self.request, self.alist, object())
        nfos = planned_nfos(plan)
        artwork = planned_artwork(plan)
        self.assertTrue(nfos)
        self.assertTrue(artwork)
        # Existing root metadata remains authoritative. Missing episode-side
        # artifacts still use the ordinary projection; no child-only NFO path.
        existing: dict[str, bytes] = {nfos[0][0]: b"user-root-nfo"}
        existing_artwork = artwork[0][0]
        existing[existing_artwork] = b"existing-artwork"
        self.alist.files.update(existing)
        tmdb = RecordingTMDB()

        result = SimplePlanExecutor(self.alist, tmdb).execute(plan)

        self.assertEqual(result["file_count"], 1)
        self.assertEqual(result["artifact_count"], len(nfos) + len(artwork))
        self.assertEqual(self.alist.files[nfos[0][0]], b"user-root-nfo")
        for target, content in existing.items():
            self.assertEqual(self.alist.files[target], content)
        self.assertTrue(tmdb.calls)
        self.assertTrue(all(target in self.alist.files for target, _ in nfos))
        self.assertTrue(all(target in self.alist.files for target, _path, _role in artwork))

    def test_internal_tv_child_uses_full_normal_nfo_projection(self) -> None:
        """An internal TV child gets root and episode NFOs from the normal planner."""
        plan = provider_tv_child_plan()
        for item in plan.files:
            self.alist.files[item.source_path] = FAKE_VIDEO_BYTES
        expected_nfos = dict(planned_nfos(plan))
        result = SimplePlanExecutor(self.alist, RecordingTMDB()).execute(plan)

        self.assertEqual(result["file_count"], 2)
        self.assertEqual(result["artifact_count"], len(expected_nfos))
        self.assertEqual(
            {row["target"] for row in result["artifacts"]},
            set(expected_nfos),
        )
        self.assertTrue(all(path in self.alist.files for path in expected_nfos))
        self.assertIn("/library/Example Show/tvshow.nfo", self.alist.files)

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
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - persisted-plan guard fixture
            replace(
                job,
                plan=tampered_plan,
                summary={**job.summary, "internal_child": True, "root_job_id": "engine-provider-root"},
            ).as_dict(),
            allow_nan=False,
        )

        with self.assertRaisesRegex(EngineExecutionError, "多个视频映射到 S01E01"):
            runner.execute_job(job.id)

        self.assertEqual(runner.get_job(job.id).phase, "failed")

    def test_provider_tv_child_allows_two_distinct_unique_episode_videos(self) -> None:
        plan = provider_tv_child_plan()
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

    def test_title_ordinal_parser_is_explicitly_gated_and_leading_tags_fail_closed(self) -> None:
        """The bounded ``Title 01`` lane never becomes a global parser rule."""
        source = [
            {
                "name": (
                    f"[4K_EA] One Season Show {episode:02d} "
                    "[简体内嵌][WebRip].mkv"
                ),
                "full_path": f"/incoming/one/{episode:02d}.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            }
            for episode in range(1, 4)
        ]
        gated = parse_ep_files(source, allow_release_title_ordinal=True)
        self.assertEqual([key.display for key in gated], ["E01", "E02", "E03"])

        # The ordinary parser may still have legacy ways to read a trailing
        # ordinal; the D/F gate is what makes this grammar *authoritative*.
        # It is never enabled by a public EngineRequest.
        self.assertEqual(
            [key.display for key in parse_ep_files(source)],
            ["E01", "E02", "E03"],
        )
        unsafe = [
            {
                "name": f"[01] One Season Show {episode:02d}.mkv",
                "full_path": f"/incoming/unsafe/{episode:02d}.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            }
            for episode in (1, 2)
        ]
        self.assertIsNone(
            release_title_ordinal_regular_episode(unsafe[0]["name"]),
        )
        self.assertIsNone(
            release_title_ordinal_regular_episode(unsafe[1]["name"]),
        )
        for name in (
            "One Season Show 01 [01.5].mkv",
            "One Season Show 01 (24.5).mkv",
            "One Season Show 01 [01到02].mkv",
            "One Season Show 01 [2024到2025].mkv",
        ):
            with self.subTest(name=name):
                self.assertIsNone(release_title_ordinal_regular_episode(name))
        self.assertIsNone(
            release_dash_regular_episode("One Season Show - 01 [01.5].mkv"),
        )

    def test_tv_planner_consumes_a_proven_physical_oad_sp_map(self) -> None:
        """The D/F-only SP map turns no unproven OAD into Season 00."""
        class PlannerAList:
            def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
                del refresh
                return []

        class PlannerTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/99100":
                    return {
                        "name": "Example OAD",
                        "original_name": "Example OAD",
                        "first_air_date": "2020-01-01",
                        "seasons": [{"season_number": 1, "episode_count": 5, "name": "OAD"}],
                    }
                if path == "/tv/99100/season/1":
                    return {"episodes": [{
                        "episode_number": number,
                        "name": f"OAD #{number}",
                        "air_date": "2020-01-01",
                    } for number in range(1, 6)]}
                if path == "/tv/99100/season/0":
                    return {"episodes": []}
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_files = [{
            "name": f"Example [OAD{number:02d}].mkv",
            "full_path": f"/incoming/Example OAD/Example [OAD{number:02d}].mkv",
            "size": 2 * 1024 * 1024,
            "is_dir": False,
        } for number in range(1, 6)]
        with tempfile.TemporaryDirectory() as directory:
            mapping_path = Path(directory) / "physical-oad-map.json"
            mapping_path.write_text(
                json.dumps({
                    f"SP{number:02d}": f"S01E{number:02d}"
                    for number in range(1, 6)
                }),
                encoding="utf-8",
            )
            plan = build_tv_plan(
                PlannerAList(), PlannerTMDB(),
                src_path="/incoming/Example OAD", parent_path="/library/番剧",
                tmdb_id=99100, season=1, absolute=False,
                prefer_simplified=True, allow_unmapped=False,
                episode_map_path=mapping_path, source_files=source_files,
            )
        self.assertEqual([item.episode_key for item in plan.files], [
            f"SP{number:02d}" for number in range(1, 6)
        ])
        self.assertEqual([item.final_name for item in plan.files], [
            f"Example OAD - S01E{number:02d} - OAD #{number}.mkv"
            for number in range(1, 6)
        ])
        self.assertEqual(plan.problem_files, [])

    def test_declared_subtitle_only_season_is_retained_without_blocking_tv_plan(self) -> None:
        """B/W-proven empty seasons are gaps, never subtitle-only writes.

        This exercises the real smart multi-season split.  Without the B/W
        declaration it must preserve the existing fail-closed no-video error;
        with the declaration it plans only the two playable seasons and leaves
        the Season 02 SUP file outside files, cleanup, and problem rows.
        """
        class PlannerAList:
            def try_list(self, _path: str, refresh: bool = False) -> list[dict[str, object]]:
                del refresh
                return []

            def walk(self, _path: str, **_kwargs: object) -> list[dict[str, object]]:
                return []

        class PlannerTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/99":
                    return {
                        "name": "Northwind Show",
                        "original_name": "Northwind Show",
                        "first_air_date": "2020-01-01",
                        "seasons": [
                            {"season_number": 0, "episode_count": 0},
                            {"season_number": 1, "episode_count": 1},
                            {"season_number": 2, "episode_count": 1},
                            {"season_number": 3, "episode_count": 1},
                        ],
                    }
                if path.startswith("/tv/99/season/"):
                    season = int(path.rsplit("/", 1)[1])
                    return {
                        "episodes": [] if season == 0 else [{
                            "episode_number": 1,
                            "name": f"Episode {season}",
                            "air_date": "2020-01-01",
                        }],
                    }
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_root = "/quark/影视/待刮削/Northwind"
        subtitle_path = source_root + "/S02/Northwind.Show.S02E01.sup"
        source_files = [
            {
                "name": "Northwind.Show.S01E01.mkv",
                "full_path": source_root + "/S01/Northwind.Show.S01E01.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            },
            {
                "name": "Northwind.Show.S02E01.sup",
                "full_path": subtitle_path,
                "size": 1_024,
                "is_dir": False,
            },
            {
                "name": "Northwind.Show.S03E01.mkv",
                "full_path": source_root + "/S03/Northwind.Show.S03E01.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            },
        ]
        kwargs = {
            "auto_episode_mode": True,
            "alist": PlannerAList(),
            "tmdb_client": PlannerTMDB(),
            "src_path": source_root,
            "parent_path": "/quark/影视/番剧",
            "tmdb_id": 99,
            "season": 1,
            "absolute": False,
            "prefer_simplified": True,
            "allow_unmapped": False,
            "ignore_orphan_temp": False,
            "source_files": source_files,
            "media_root": "/quark/影视",
        }

        with self.assertRaisesRegex(PlanError, "未找到剧集视频文件"):
            build_tv_plan_smart(**kwargs)

        plan = build_tv_plan_smart(
            **kwargs,
            source_declared_seasons=(1, 2, 3),
        )

        self.assertEqual(
            {item.source_path for item in plan.files},
            {
                source_root + "/S01/Northwind.Show.S01E01.mkv",
                source_root + "/S03/Northwind.Show.S03E01.mkv",
            },
        )
        self.assertNotIn(subtitle_path, {item.source_path for item in plan.cleanup_files})
        self.assertNotIn(subtitle_path, {item.source_path for item in plan.problem_files})
        self.assertEqual(
            plan.scan_report["deferred_subtitle_only_seasons"],
            [{
                "kind": "subtitle_only_declared_season",
                "season": 2,
                "source_paths": [subtitle_path],
                "reason": "该季未发现视频；外挂字幕保留在来源，不作为无视频正式库写入",
            }],
        )

        # A declaration is not permission to place subtitles without their
        # video.  The narrow deferral only applies when another executable
        # season remains in the same owned WorkUnit.
        with self.assertRaisesRegex(PlanError, "未找到剧集视频文件"):
            build_tv_plan_smart(
                **{
                    **kwargs,
                    "source_files": [source_files[1]],
                    "source_declared_seasons": (2,),
                },
            )

    def test_explicit_episode_coordinate_overrides_movie_word_in_episode_title(self) -> None:
        """A season episode titled ``大电影`` remains a TV file.

        Release filenames commonly include words such as ``电影`` in an
        episode title.  The enclosing season plus an agreeing SxxEyy token is
        stronger structural evidence than the generic movie-context
        heuristic; the planner must not even query the movie namespace.
        """
        class PlannerAList:
            def try_list(self, _path: str, refresh: bool = False) -> list[dict[str, object]]:
                del refresh
                return []

            def walk(self, _path: str, **_kwargs: object) -> list[dict[str, object]]:
                return []

        class PlannerTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/60625":
                    return {
                        "name": "Example Show",
                        "original_name": "Example Show",
                        "first_air_date": "2020-01-01",
                        "seasons": [{"season_number": 7, "episode_count": 1}],
                    }
                if path == "/tv/60625/season/7":
                    return {
                        "episodes": [{
                            "episode_number": 8,
                            "name": "数字者的崛起：大电影",
                            "air_date": "2020-01-01",
                            "runtime": 22,
                        }],
                    }
                if path == "/tv/60625/season/0":
                    return {"episodes": []}
                if path.startswith("/search/movie"):
                    raise AssertionError("a structured S07E08 episode must not search movies")
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_root = "/incoming/Example Show"
        source_path = (
            source_root
            + "/第七季（2020）全1集/Example Show - S07E08 - 数字者的崛起-大电影.mkv"
        )
        plan = build_tv_plan_smart(
            auto_episode_mode=True,
            alist=PlannerAList(),
            tmdb_client=PlannerTMDB(),
            src_path=source_root,
            parent_path="/library/欧美剧",
            tmdb_id=60625,
            season=7,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            source_files=[{
                "name": Path(source_path).name,
                "full_path": source_path,
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            }],
            media_root="/quark/影视",
            source_declared_seasons=(7,),
        )

        self.assertEqual(plan.mode, "tv")
        self.assertNotIn("member_movies", plan.metadata)
        self.assertEqual(len(plan.files), 1)
        item = plan.files[0]
        self.assertEqual(item.episode_key, "E08")
        self.assertEqual(item.target_dir, "/library/欧美剧/Example Show/Season 07")
        self.assertIn("S07E08", item.final_name)

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

    def test_tv_planner_uses_verified_tc_when_sc_srt_is_invalid(self) -> None:
        """Filename-preferred SC cannot preempt a content-proven TC fallback."""
        class PlannerAList:
            def __init__(self, contents: dict[str, bytes]) -> None:
                self.contents = contents

            def try_list(self, _path: str, refresh: bool = False) -> list[dict[str, object]]:
                del refresh
                return []

            def read_file_prefix(self, path: str, *, max_bytes: int) -> bytes:
                return self.contents[path][:max_bytes]

        class PlannerTMDB:
            def get(self, path: str, language: str | None = None) -> dict[str, object]:
                del language
                if path == "/tv/78":
                    return {
                        "name": "Fallback Show",
                        "original_name": "Fallback Show",
                        "original_language": "ja",
                        "first_air_date": "2020-01-01",
                        "seasons": [],
                    }
                if path == "/tv/78/season/1":
                    return {"episodes": [{"episode_number": 1, "name": "Pilot"}]}
                if path == "/tv/78/season/0":
                    return {"episodes": []}
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_root = "/incoming/fallback"
        invalid_sc = source_root + "/Fallback.Show.S01E01.zh-CN.srt"
        valid_tc = source_root + "/Fallback.Show.S01E01.zh-TW.srt"
        tc_bytes = (
            "1\n00:00:01,000 --> 00:00:03,000\n"
            "這是一個繁體中文字幕測試內容。\n"
        ).encode("utf-8")
        plan = build_tv_plan(
            PlannerAList({invalid_sc: b"not an srt", valid_tc: tc_bytes}),
            PlannerTMDB(),
            src_path=source_root,
            parent_path="/library/番剧",
            tmdb_id=78,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            source_files=[
                {
                    "name": "Fallback.Show.S01E01.mkv",
                    "full_path": source_root + "/Fallback.Show.S01E01.mkv",
                    "size": FAKE_VIDEO_SIZE,
                    "is_dir": False,
                },
                {
                    "name": "Fallback.Show.S01E01.zh-CN.srt",
                    "full_path": invalid_sc,
                    "size": len(b"not an srt"),
                    "is_dir": False,
                },
                {
                    "name": "Fallback.Show.S01E01.zh-TW.srt",
                    "full_path": valid_tc,
                    "size": len(tc_bytes),
                    "is_dir": False,
                },
            ],
        )
        self.assertIn(valid_tc, {item.source_path for item in plan.files})
        self.assertNotIn(invalid_sc, {item.source_path for item in plan.files})
        self.assertNotIn(invalid_sc, {item.source_path for item in plan.problem_files})

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

    def test_subtitle_sidecar_scope_flip_after_validation_starts_no_formal_write(self) -> None:
        """The sidecar writer rechecks scope immediately before mkdir/move."""
        video = "/library/Show/Season 01/Show.S01E01.mkv"
        source = "/quark/影视/ScrapeFlow/补源/root/attempt-1/Show.S01E01.zh.srt"
        target = "/library/Show/Season 01/Show.S01E01.zh.srt"
        paused = {"value": False}

        class ScopeFlipAList(FakeAList):
            def exact_file_info(self, path: str) -> dict[str, object] | None:
                result = super().exact_file_info(path)
                if path == source and result is not None:
                    paused["value"] = True
                return result

        alist = ScopeFlipAList()
        alist.files[video] = b"video"
        alist.files[source] = b"subtitle"
        runner = SimpleEngineRunner(
            self.root / "sidecar-scope",
            alist=alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )

        with self.assertRaises(EnginePauseRequested):
            runner.install_subtitle_sidecar(
                source,
                target,
                expected_size=len(b"subtitle"),
                video_path=video,
                pause_requested=lambda: paused["value"],
            )

        self.assertEqual(alist.moves, [])
        self.assertEqual(alist.renames, [])
        self.assertNotIn(target, alist.files)
        self.assertIn(source, alist.files)

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

    def test_subtitle_sidecar_content_must_match_declared_language(self) -> None:
        video = "/library/Show/Season 01/Show.S01E01.mkv"
        source = "/quark/影视/ScrapeFlow/补源/root/attempt-1/Show.S01E01.zh.srt"
        target = "/library/Show/Season 01/Show.S01E01.zh.srt"
        content = "1\n00:00:01,000 --> 00:00:02,000\n這是一個測試\n".encode("utf-8")
        self.alist.files[video] = b"video"
        self.alist.files[source] = content
        with self.assertRaisesRegex(EngineExecutionError, "语言校验"):
            SimplePlanExecutor(self.alist).install_subtitle_sidecar(
                source, target, expected_size=len(content), video_path=video,
                subtitle_language="zh",
            )
        self.assertIn(source, self.alist.files)

        self.alist.files[source] = (
            "1\n00:00:01,000 --> 00:00:02,000\n这是一个测试\n".encode("utf-8")
        )
        result = SimplePlanExecutor(self.alist).install_subtitle_sidecar(
            source, target, expected_size=len(self.alist.files[source]),
            video_path=video, subtitle_language="zh",
        )
        self.assertEqual(result["status"], "moved")

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

    def test_processing_cancel_waits_for_current_executor_boundary(self) -> None:
        started = threading.Event()
        release = threading.Event()
        calls: list[object] = []

        def blocking_executor(plan: object) -> dict[str, object]:
            calls.append(plan)
            started.set()
            self.assertTrue(release.wait(timeout=3))
            return {"ok": True}

        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=blocking_executor,
        )
        job = runner.plan_job(self.request, job_id="engine-processing-cancel")
        result: list[object] = []

        def run() -> None:
            result.append(runner.execute_job(job.id))

        worker = threading.Thread(target=run)
        worker.start()
        self.assertTrue(started.wait(timeout=3))
        requested = runner.cancel_job(job.id, reason="safe stop")
        self.assertEqual(requested.phase, "executing")
        release.set()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(runner.get_job(job.id).phase, "cancelled")
        self.assertEqual(len(calls), 1)
        self.assertFalse(runner._cancel_request_path(job.id).exists())  # noqa: SLF001

    def test_planning_cancel_stops_after_archive_preprocessor_boundary(self) -> None:
        self.alist.directories.add("/incoming/archive")
        started = threading.Event()
        release = threading.Event()

        def blocking_preprocessor(
            request,
            *,
            job_id=None,
            retry_password=None,
            pause_requested=None,
        ):
            del job_id, retry_password, pause_requested
            started.set()
            self.assertTrue(release.wait(timeout=3))
            return request, None

        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            archive_preprocessor=object(),
        )
        waiting = self._new_work_waiting(runner, "/incoming/archive", job_id="engine-planning-cancel")
        queued = runner.start_automatic_job(waiting.id, target_shelf="movie")
        result: list[object] = []

        with patch.object(
            runner,
            "_preprocess_ordinary_request_details",
            side_effect=blocking_preprocessor,
        ):
            worker = threading.Thread(
                target=lambda: result.append(runner.plan_automatic_job(queued.id))
            )
            worker.start()
            self.assertTrue(started.wait(timeout=3))
            requested = runner.cancel_job(queued.id, reason="stop planning")
            self.assertEqual(requested.phase, "archive_preprocessing")
            release.set()
            worker.join(timeout=3)

        self.assertFalse(worker.is_alive())
        self.assertEqual(runner.get_job(queued.id).phase, "cancelled")
        self.assertEqual(getattr(result[0], "phase", None), "cancelled")

    def test_queued_cancel_is_not_blocked_by_another_job_worker_lock(self) -> None:
        self.alist.directories.add("/incoming/queued")
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        waiting = self._new_work_waiting(runner, "/incoming/queued", job_id="engine-queued-cancel")
        queued = runner.start_automatic_job(waiting.id, target_shelf="movie")
        entered = threading.Event()
        release = threading.Event()

        def hold_other_job_lock() -> None:
            with runner.worker_lock():
                entered.set()
                release.wait(timeout=3)

        holder = threading.Thread(target=hold_other_job_lock)
        holder.start()
        self.assertTrue(entered.wait(timeout=3))
        cancelled = runner.cancel_job(queued.id, reason="cancel while another job runs")
        self.assertEqual(cancelled.phase, "queued")
        self.assertTrue(runner._cancel_request_path(queued.id).exists())  # noqa: SLF001
        release.set()
        holder.join(timeout=3)

        self.assertFalse(holder.is_alive())
        cancelled = runner.cancel_job(queued.id, reason="cancel while another job runs")
        self.assertEqual(cancelled.phase, "cancelled")
        self.assertEqual(runner.get_job(queued.id).phase, "cancelled")
        self.assertEqual(self.alist.moves, [])

    def test_restart_consumes_a_durable_processing_cancel_request(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"ok": True},
        )
        job = runner.plan_job(self.request, job_id="engine-restart-cancel")
        active_summary = runner._with_active_operation(job.summary, kind="formal_write")  # noqa: SLF001
        active = replace(job, phase="executing", summary=active_summary)
        atomic_write_json(runner._job_path(job.id), active.as_dict(), allow_nan=False)  # noqa: SLF001
        atomic_write_json(
            runner._cancel_request_path(job.id),  # noqa: SLF001
            {
                "operation_id": active_summary["active_operation"]["id"],
                "requested_at": "2026-08-09T00:00:00Z",
                "reason": "restart stop",
            },
            allow_nan=False,
        )

        restarted = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"unexpected": True},
        )
        recovered = restarted.get_job(job.id)
        self.assertEqual(recovered.phase, "cancelled")
        self.assertTrue(recovered.summary["cancellation"]["recovered_after_restart"])
        self.assertFalse(restarted._cancel_request_path(job.id).exists())  # noqa: SLF001

    def test_restart_cancel_marks_an_interrupted_cleanup_as_cancelled(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        job = runner.plan_job(self.request, job_id="engine-cleanup-restart-cancel")
        active_summary = runner._with_active_operation(job.summary, kind="final_cleanup")  # noqa: SLF001
        active_summary["lifecycle"] = {
            "cleanup": {"status": "running"},
        }
        active = replace(job, phase="cleaning", summary=active_summary)
        atomic_write_json(runner._job_path(job.id), active.as_dict(), allow_nan=False)  # noqa: SLF001
        atomic_write_json(
            runner._cancel_request_path(job.id),  # noqa: SLF001
            {
                "operation_id": active_summary["active_operation"]["id"],
                "requested_at": "2026-08-09T00:00:00Z",
                "reason": "stop cleanup",
            },
            allow_nan=False,
        )

        restarted = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        cancelled = restarted.get_job(job.id)
        self.assertEqual(cancelled.phase, "cancelled")
        self.assertEqual(cancelled.summary["lifecycle"]["cleanup"]["status"], "cancelled")

    def test_restart_consumes_completed_root_cancel_marker(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"ok": True},
        )
        job = runner.plan_job(self.request, job_id="engine-completed-root-cancel")
        completed = replace(job, phase="completed")
        atomic_write_json(
            runner._job_path(job.id),  # noqa: SLF001 - persisted restart fixture
            completed.as_dict(),
            allow_nan=False,
        )
        runner._request_inactive_cancellation(  # noqa: SLF001 - durable marker fixture
            completed,
            reason="stop active replenishment",
        )

        restarted = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
            executor=lambda _plan: {"unexpected": True},
        )

        self.assertEqual(restarted.get_job(job.id).phase, "cancelled")
        self.assertFalse(restarted._cancel_request_path(job.id).exists())  # noqa: SLF001

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

    def test_historic_internal_child_artifact_repair_backfills_nfos_without_media_move(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=RecordingTMDB(),
            planner=provider_media_plan,
            validate=False,
        )
        child = runner.plan_job(
            self.request,
            job_id="engine-provider-artifact-repair",
            internal_child_of="engine-provider-root",
        )
        target = "/library/Movie (2020)/Movie (2020).mkv"
        self.alist.files[target] = self.alist.files.pop("/incoming/movie/source.mkv")
        atomic_write_json(
            runner._job_path(child.id),  # noqa: SLF001 - interrupted child fixture
            replace(child, phase="executed", error=None).as_dict(),
            allow_nan=False,
        )

        repaired = runner.repair_automatic_artifacts(child.id)

        self.assertEqual(repaired.phase, "executed")
        self.assertEqual(self.alist.moves, [])
        self.assertEqual(self.alist.files[target], FAKE_VIDEO_BYTES)
        artifact_plan = provider_media_plan(self.request, self.alist, object())
        expected_artifact_targets = {
            target
            for target, _content in planned_nfos(artifact_plan)
        }
        expected_artifact_targets.update(
            target for target, _image_path, _role in planned_artwork(artifact_plan)
        )
        self.assertTrue(expected_artifact_targets.issubset(self.alist.files))

    def test_artifact_repair_refuses_visible_source_and_never_replays_media(self) -> None:
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=RecordingTMDB(),
            planner=provider_media_plan,
            validate=False,
        )
        child = runner.plan_job(
            self.request,
            job_id="engine-provider-repair-conflict",
            internal_child_of="engine-provider-root",
        )
        target = "/library/Movie (2020)/Movie (2020).mkv"
        self.alist.files[target] = FAKE_VIDEO_BYTES
        atomic_write_json(
            runner._job_path(child.id),  # noqa: SLF001 - historic repair fixture
            replace(child, phase="executed").as_dict(),
            allow_nan=False,
        )

        with self.assertRaisesRegex(EngineExecutionError, "仍可见的媒体来源"):
            runner.repair_automatic_artifacts(child.id)

        self.assertEqual(self.alist.moves, [])
        self.assertEqual(self.alist.files[target], FAKE_VIDEO_BYTES)
        self.assertIn("/incoming/movie/source.mkv", self.alist.files)

    def test_completed_root_repairs_only_its_executed_replenishment_children(self) -> None:
        tmdb = RecordingTMDB()
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=tmdb,
            planner=fake_plan,
            validate=False,
            library_root="/library",
        )
        root = runner.plan_job(self.request, job_id="engine-root-artifact-completed")
        root = runner.execute_job(root.id)
        atomic_write_json(
            runner._job_path(root.id),  # noqa: SLF001 - aggregate-phase fixture
            replace(root, phase="completed").as_dict(),
            allow_nan=False,
        )

        child_source_root = (
            f"/library/ScrapeFlow/补源/{root.id}/attempt-1/"
            "__scrapeflow_media__"
        )
        child_source = f"{child_source_root}/child.mkv"
        child_target_root = "/library/Child Movie (2020)"
        self.alist.files[child_source] = FAKE_VIDEO_BYTES

        def staged_child_plan(_request: EngineRequest, _alist: object, _tmdb: object) -> Plan:
            plan = provider_media_plan(self.request, _alist, _tmdb)
            plan.source_root = child_source_root
            plan.target_root = child_target_root
            plan.files[0].source_path = child_source
            plan.files[0].source_dir = child_source_root
            plan.files[0].original_name = "child.mkv"
            plan.files[0].final_name = "Child Movie (2020).mkv"
            plan.files[0].target_dir = child_target_root
            plan.metadata["title"] = "Child Movie"
            return plan

        runner.planner = staged_child_plan
        child_request = replace(self.request, source_path=child_source_root)
        child = runner.plan_job(
            child_request,
            job_id="replenishment-attempt-1",
            internal_child_of=root.id,
        )
        child = runner.execute_job(child.id)
        child_plan = runner._plan_from_job(child)  # noqa: SLF001 - durable repair fixture
        for path, _content in planned_nfos(child_plan):
            self.alist.files.pop(path, None)
        for path, _image_path, _role in planned_artwork(child_plan):
            self.alist.files.pop(path, None)
        moves_before = list(self.alist.moves)

        repaired_root, repaired_children = runner.repair_root_artifacts(root.id)

        self.assertEqual(repaired_root.phase, "completed")
        self.assertEqual([row.id for row in repaired_children], [child.id])
        self.assertEqual(self.alist.moves, moves_before)
        self.assertIn(f"{child_target_root}/Child Movie (2020).mkv", self.alist.files)
        self.assertTrue(all(
            path in self.alist.files for path, _content in planned_nfos(child_plan)
        ))
        self.assertTrue(all(
            path in self.alist.files
            for path, _image_path, _role in planned_artwork(child_plan)
        ))

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
        waiting = self._new_work_waiting(runner, "/incoming/movie")
        queued = runner.start_automatic_job(waiting.id, target_shelf="movie")
        self.assertEqual(queued.phase, "queued")
        identity = AutomaticIdentity(
            media_type="movie", tmdb_id=1, title="Movie", year="2020",
            confidence=0.99, target_parent="/quark/影视/电影", season=None, trace={},
            target_shelf="movie", target_shelf_root="/quark/影视/电影",
        )
        original = runner.resolve_automatic_request
        runner.resolve_automatic_request = lambda _source, **_kwargs: (  # type: ignore[method-assign]
            replace(self.request, parent_path="/quark/影视/电影", target_shelf="movie"),
            identity,
        )
        try:
            planned = runner.plan_automatic_job(queued.id)
        finally:
            runner.resolve_automatic_request = original  # type: ignore[method-assign]
        self.assertEqual(planned.phase, "planned")
        self.assertEqual(planned.summary["identity"]["tmdb_id"], 1)

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


    def test_explicit_episode_map_path_still_preclassifies_bonus_residuals(self) -> None:
        """An explicit episode map must not skip bonus preclassification.

        The release-dash D proof hands F a source-key episode map; that path
        bypasses the smart season grouping, but provided source files must
        still be preclassified so ``EXTRA/[SP00] Menu - 01`` never reaches
        the episode parser (轮回七次 F-stage shape).
        """
        class PlannerAList:
            def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
                del refresh
                return []

        class PlannerTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/99101":
                    return {
                        "name": "Example Dash",
                        "original_name": "Example Dash",
                        "first_air_date": "2020-01-01",
                        "seasons": [{"season_number": 1, "episode_count": 2, "name": "S1"}],
                    }
                if path == "/tv/99101/season/1":
                    return {"episodes": [{
                        "episode_number": number,
                        "name": f"Episode {number}",
                        "air_date": "2020-01-01",
                    } for number in (1, 2)]}
                if path == "/tv/99101/season/0":
                    return {"episodes": []}
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_files = [
            {
                "name": f"Example Dash - {number:02d} (BD 1080p).mkv",
                "full_path": f"/incoming/Example Dash/Example Dash - {number:02d} (BD 1080p).mkv",
                "size": 2 * 1024 * 1024,
                "is_dir": False,
            }
            for number in (1, 2)
        ] + [
            {
                "name": "Example Dash [SP00] Menu - 01 (BD 1080p).mkv",
                "full_path": "/incoming/Example Dash/EXTRA/Example Dash [SP00] Menu - 01 (BD 1080p).mkv",
                "size": 2 * 1024 * 1024,
                "is_dir": False,
            },
            {
                "name": "Example Dash [SP05] Picture Drama - 01 (BD 1080p).mkv",
                "full_path": "/incoming/Example Dash/EXTRA/Example Dash [SP05] Picture Drama - 01 (BD 1080p).mkv",
                "size": 2 * 1024 * 1024,
                "is_dir": False,
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            mapping_path = Path(directory) / "dash-map.json"
            mapping_path.write_text(
                json.dumps({"01": "S01E01", "02": "S01E02"}),
                encoding="utf-8",
            )
            plan = build_tv_plan_smart(
                auto_episode_mode=True,
                alist=PlannerAList(), tmdb_client=PlannerTMDB(),
                src_path="/incoming/Example Dash", parent_path="/library/番剧",
                tmdb_id=99101, season=1, absolute=False,
                prefer_simplified=True, allow_unmapped=False,
                episode_map_path=mapping_path, source_files=source_files,
            )
        # The episode keys carry the map's explicit SxxEyy coordinates for
        # the two planned episodes (EpisodeKey.display form).
        self.assertEqual(
            sorted(item.episode_key for item in plan.files),
            ["E01", "E02"],
        )
        self.assertEqual(
            sorted(item.final_name for item in plan.files),
            [
                "Example Dash - S01E01 - Episode 1.mkv",
                "Example Dash - S01E02 - Episode 2.mkv",
            ],
        )
        residuals = plan.scan_report.get("preserved_source_residuals") or []
        self.assertEqual(len(residuals), 2)
        self.assertTrue(
            all(item["action"] == "preserve_at_source" for item in residuals)
        )

if __name__ == "__main__":
    unittest.main()
