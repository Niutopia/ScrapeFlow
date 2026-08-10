#!/usr/bin/env python3
"""Single-user HTTP service for ScrapeFlow's automatic media workflow."""

from __future__ import annotations

import contextlib
import ipaddress
import json
import os
import posixpath
import re
import signal
import sys
import threading
import urllib.parse
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.media_quality import (
    is_production_test_media_path,
    is_video_filename,
)
from engine.scrapeflow.provider_capabilities import provider_capability_snapshot
from engine.scrapeflow.archive_preprocessing import ArchivePreprocessingAdapter
from engine.scrapeflow.target_shelf import target_shelf_values
from local.scrapeflow_api.simple_engine_runner import (
    EngineExecutionError,
    EngineJob,
    EngineJobConflictError,
    EngineJobNotFoundError,
    EngineRequestError,
    EngineWorkerBusyError,
    SimpleEngineError,
    SimpleEngineRunner,
    recover_persisted_engine_jobs,
)
from local.scrapeflow_api.simple_library_audit import (
    audit_and_persist,
    latest_audit_path,
    make_alist_subtitle_checker,
    run_automatic_library_audit,
)
from local.scrapeflow_api.automatic_replenishment import (
    AutomaticReplenishmentRuntime,
    LocalTorrentAutomaticMaterializer,
    reconcile_interrupted_gap_states,
)
from local.scrapeflow_api.control_state import PersistentControlState
from local.scrapeflow_api.redaction import redact_error, redact_value


# These findings enter the automatic provider queue. Media gaps become Engine
# child stages; subtitle gaps use a separate sidecar writer bound to the exact
# already-audited final video.
_AUTOMATIC_PROVIDER_GAP_KINDS = frozenset({
    "missing_media", "missing_episode", "missing_season", "missing_subtitle",
})
_AUDIT_ROOT_PROVIDER_GAP_KINDS = frozenset({
    "missing_media", "missing_episode", "missing_season",
})
_AUTOMATIC_REPAIR_GAP_KINDS = frozenset({"missing_nfo", "missing_poster"})
_AUTOMATIC_UNSUPPORTED_GAP_KINDS = frozenset()
# These findings are deliberately fail-closed evidence gaps rather than
# actionable provider work.  Re-running the same full-library snapshot on a
# heartbeat cannot resolve a bounded TMDB timeout or an identity that the
# catalog could not prove.  A newly committed source and an explicit/manual
# audit remain the retry boundaries.
_AUDIT_DEFERRED_UNKNOWN_KINDS = frozenset({
    "unknown_episode_catalog",
    "unknown_library_work",
    "unknown_subtitle_evidence",
})
# The subtitle evidence ledger intentionally probes a bounded, persisted slice
# of the library on each scan.  This one exact unknown is progress: the next
# serialized pass advances the ledger cursor.  All other unknown evidence is
# fail-closed and must wait for a media commit or an explicit audit instead.
_AUDIT_SUBTITLE_BATCH_DEFERRED_KIND = "unknown_subtitle_evidence"
_AUDIT_SUBTITLE_BATCH_DEFERRED_REASON = "subtitle_probe_batch_deferred"
_ORPHANED_PROVIDER_PROGRESS_PHASES = frozenset({
    "provider_searching", "acquiring", "staging_verifying",
    "subtitle_installing", "child_planning", "child_executing",
    "final_verifying", "cleaning", "child_failed",
})
_PROVIDER_PROJECTION_PHASES = _ORPHANED_PROVIDER_PROGRESS_PHASES | frozenset({
    "gap_discovering", "retry_wait", "failed", "failed_provider",
})
_PROVIDER_PILOT_GAP_RE = re.compile(r"^S\d{2}E\d{2}$")


class DuplicateEngineTask(SimpleEngineError):
    """A source already has a pending Engine plan."""

    def __init__(self, job: EngineJob) -> None:
        super().__init__(f"该源目录已有 Engine 任务: {job.id}")
        self.job = job


class ApplicationError(RuntimeError):
    """The automatic application cannot complete the requested operation."""


def _redacted_job_payload(job: EngineJob) -> dict[str, object]:
    """Return a safe persisted root-job document without mutating the job."""
    redacted = redact_value(job.as_dict())
    return dict(redacted) if isinstance(redacted, Mapping) else job.as_dict()


class SimpleApplication:
    """HTTP-facing composition root for automatic intake and delivery.

    ``remote`` is injectable for smoke tests and local dry runs.  In a real
    container the default is an authenticated AList client created lazily;
    the health endpoint does not make a network call merely to report that the
    application is alive.
    """

    def __init__(
        self,
        *,
        state_root: Path | None = None,
        remote_root: str | None = None,
        remote: object | None = None,
        engine_runner: SimpleEngineRunner | None = None,
        enforce_engine_roots: bool | None = None,
        archive_preprocessor: object | None = None,
    ) -> None:
        self.state_root = Path(state_root or os.getenv("SCRAPEFLOW_STATE_DIR", "/data")).resolve()
        self.state_root.mkdir(parents=True, exist_ok=True)
        configured_remote_root = remote_root or os.getenv("SCRAPEFLOW_MEDIA_ROOT", "/quark/影视")
        self.remote_root = _safe_remote_root(configured_remote_root)
        self.enforce_engine_roots = (
            configured_remote_root.rstrip("/") == "/quark/影视"
            if enforce_engine_roots is None
            else bool(enforce_engine_roots)
        )
        candidate = remote if remote is not None else self._build_remote()
        self._alist_client = candidate
        self._engine_runner = engine_runner
        staging_prefix = f"{self.remote_root.rstrip('/')}/ScrapeFlow"
        local_staging_prefix = (self.state_root / "archive-staging").resolve()
        self._archive_preprocessor = archive_preprocessor or ArchivePreprocessingAdapter(
            staging_root_validator=lambda path: (
                path == staging_prefix or path.startswith(staging_prefix + "/")
            ),
            local_staging_root_validator=lambda path: (
                Path(path).resolve() == local_staging_prefix
                or local_staging_prefix in Path(path).resolve().parents
            ),
        )
        self._engine_runner_lock = threading.Lock()
        self._automatic_replenishment: AutomaticReplenishmentRuntime | None = None
        self._automatic_replenishment_lock = threading.Lock()
        self._control_path = self.state_root / "global-control.json"
        self._control_state = PersistentControlState(self._control_path)
        self._control_lock = threading.Lock()
        self._automatic_lock = threading.RLock()
        self._automatic_executor: ThreadPoolExecutor | None = None
        self._automatic_futures: dict[str, Future[object]] = {}
        self._provider_executor: ThreadPoolExecutor | None = None
        self._provider_futures: dict[str, Future[object]] = {}
        # Delayed retries are real scheduler state, not fire-and-forget
        # ``threading.Timer`` instances.  Each lane/root key has at most one
        # pending callback so manual retry, terminal completion and shutdown
        # can cancel it before it creates another attempt.
        self._scheduled_timers: dict[tuple[str, str], threading.Timer] = {}
        # A terminal cleanup fences callbacks for the root while its durable
        # JSON/staging ownership check and deletion run.  Keeping the fence
        # through the runner's inter-process lock closes the small window in
        # which a timer could fire after the HTTP guard but before cleanup.
        self._cleanup_fences: set[str] = set()
        # A manually supplied archive password is deliberately memory-only.
        # It is consumed by the next planning attempt and never enters a job,
        # gap record, retry timer or public projection.
        self._retry_archive_passwords: dict[str, str] = {}
        self._audit_lock = threading.RLock()
        self._audit_executor: ThreadPoolExecutor | None = None
        self._audit_future: Future[object] | None = None
        self._pending_audit_roots: set[str] = set()
        # A provider child can finish while a read-only full-library audit is
        # still traversing the old inventory.  Keep one coalesced follow-up
        # request so the newly committed video is audited (and can acquire a
        # missing configured-language subtitle) instead of relying on a later
        # scheduler heartbeat.
        self._audit_rerun_requested = False
        self._closed = threading.Event()
        self._startup_threads: list[threading.Thread] = []
        self._intake_stop = threading.Event()
        self._intake_wake = threading.Event()
        self._intake_thread: threading.Thread | None = None
        self._intake_status: dict[str, object] = {
            "last_scan_at": None,
            "last_error": None,
            "last_scheduled_count": 0,
            "last_registered_count": 0,
        }
        self._recover_persisted_engine_jobs()
        # Existing automatic jobs are resumed in the background.
        self._start_startup_thread(self._resume_automatic_jobs, name="scrapeflow-resume")
        if self._automatic_audit_enabled():
            self._start_startup_thread(
                self._queue_library_audit,
                kwargs={"delay": 1.0},
                name="scrapeflow-startup-audit",
            )
        self._start_intake_monitor()

    def _start_startup_thread(
        self,
        target: Callable[..., object],
        *,
        kwargs: Mapping[str, object] | None = None,
        name: str,
    ) -> None:
        if self._closed.is_set():
            return
        thread = threading.Thread(
            target=target,
            kwargs=dict(kwargs or {}),
            daemon=True,
            name=name,
        )
        self._startup_threads.append(thread)
        thread.start()

    def _recover_persisted_engine_jobs(self) -> None:
        """Recover Engine records without changing the operator control state."""
        try:
            recover_persisted_engine_jobs(self.state_root)
        except EngineWorkerBusyError:
            # Keep a second/read-only API process available while the original
            # worker finishes; mutation endpoints will return 409 from the
            # runner's lock instead of starting a concurrent transfer.
            pass

    @staticmethod
    def _build_remote() -> object | None:
        password = os.getenv("ALIST_PASSWORD", "")
        if not password:
            return None
        try:
            # Import lazily so a health-only process stays lightweight.
            from engine.scraper import AListClient
        except (ImportError, ModuleNotFoundError):
            return None
        base_url = os.getenv("ALIST_URL", "http://alist:5244")
        allow_http = base_url.startswith(("http://alist:", "http://127.0.0.1", "http://localhost"))
        client = AListClient(
            base_url,
            os.getenv("ALIST_USERNAME", "admin"),
            password,
            allow_insecure_http=allow_http,
        )
        return client

    @property
    def remote_configured(self) -> bool:
        return self._alist_client is not None

    @property
    def engine_configured(self) -> bool:
        """Whether the Engine bridge can be constructed without network I/O."""
        return self._engine_runner is not None or bool(
            os.getenv("ALIST_PASSWORD", "").strip()
            and os.getenv("TMDB_API_KEY", "").strip()
        )

    def health(self) -> dict[str, object]:
        operations = self._operations_summary()
        with self._automatic_lock:
            intake = dict(self._intake_status)
        return {
            "ok": True,
            "mode": "automatic",
            "connected": self.remote_configured,
            "tmdb_configured": bool(os.getenv("TMDB_API_KEY", "").strip()),
            "engine_configured": self.engine_configured,
            "build_version": os.getenv("SCRAPEFLOW_BUILD_VERSION", "").strip() or "target-shelf-rc1",
            "build_commit": os.getenv("SCRAPEFLOW_BUILD_COMMIT", "").strip() or None,
            "build_time": os.getenv("SCRAPEFLOW_BUILD_TIME", "").strip() or None,
            "provider_capabilities": provider_capability_snapshot(),
            "lane_gates": {
                "provider_auto_repair_enabled": self._provider_auto_repair_enabled(),
                "audit_auto_repair_enabled": self._audit_auto_repair_enabled(),
            },
            "intake_monitoring": self._intake_monitor_enabled(),
            "intake": {
                "enabled": self._intake_monitor_enabled(),
                "root": f"{self.remote_root.rstrip('/')}/待刮削",
                "scan_seconds": self._intake_scan_seconds(),
                **intake,
            },
            "operations": operations,
            "message": (
                "全自动单用户运行时已启动"
                if self.remote_configured
                else "全自动运行时已启动；设置 ALIST_PASSWORD 后才能交付远端文件"
            ),
        }

    def _operations_summary(self) -> dict[str, object]:
        """Small read-only counters for the Web operations home page."""
        try:
            engine_jobs = [
                job for job in self.engine_jobs()
                if not self._is_internal_child(job)
            ]
        except Exception:
            engine_jobs = []
        active_engine_phases = {
            "queued", "analyzing", "archive_preprocessing", "identity_matching", "planning", "planned",
            "executing", "verifying", "cleaning", "retry_wait",
        }
        failed_engine_phases = {
            "failed", "failed_archive", "failed_identity", "failed_planning", "failed_provider", "failed_write",
            "failed_verification", "failed_cleanup",
        }
        provider_active = {
            "gap_discovering", "provider_searching", "acquiring",
            "staging_verifying", "subtitle_installing", "child_planning", "child_executing",
            "final_verifying", "cleaning", "child_failed", "retry_wait",
        }
        with self._automatic_lock:
            formal_writes = sum(
                1 for future in self._automatic_futures.values() if not future.done()
            )
            provider_workers = sum(
                1 for future in self._provider_futures.values() if not future.done()
            )
        with self._audit_lock:
            audit_running = bool(self._audit_future is not None and not self._audit_future.done())
        public_phases = []
        for job in engine_jobs:
            try:
                public_phases.append(str(self.public_engine_job(job).get("phase") or job.phase))
            except Exception:
                public_phases.append(job.phase)
        active_public_phases = active_engine_phases | provider_active
        return {
            "jobs_total": len(engine_jobs),
            "jobs_awaiting_target_shelf": sum(
                1 for phase in public_phases if phase == "awaiting_target_shelf"
            ),
            "jobs_active": sum(1 for phase in public_phases if phase in active_public_phases),
            "jobs_failed": sum(1 for phase in public_phases if phase in failed_engine_phases),
            # Count terminal public root states, not merely the formal Engine
            # move fact. Deferred Provider gaps are complete work with a
            # visible attention state, rather than active background work.
            "jobs_completed": sum(
                1 for phase in public_phases
                if phase in {"completed", "completed_with_gaps"}
            ),
            "provider_active": sum(
                1
                for job in engine_jobs
                if isinstance(job.summary.get("replenishment"), Mapping)
                and str(job.summary["replenishment"].get("status") or "") in provider_active
            ),
            "formal_write_workers": formal_writes,
            "provider_workers": provider_workers,
            "audit_running": audit_running,
        }

    @staticmethod
    def _intake_scan_seconds() -> float:
        """Return the ordinary inbound-directory polling interval.

        This is intentionally a small single-machine poller rather than a
        filesystem watcher, because the source of truth is AList.  Setting it
        to ``0`` is useful for tests and for an installation that submits all
        sources through the API; it never changes the automatic path once a
        source job exists.
        """
        raw = os.getenv("SCRAPEFLOW_INTAKE_SCAN_SECONDS", "30").strip()
        try:
            value = float(raw)
        except ValueError:
            value = 30.0
        if value <= 0:
            return 0.0
        return max(5.0, min(3600.0, value))

    def _intake_monitor_enabled(self) -> bool:
        # Test/local alternate roots normally submit a path explicitly.  The
        # real compose root is monitored by default, while a custom mount can
        # opt in with SCRAPEFLOW_INTAKE_MONITOR=1.
        return self._intake_scan_seconds() > 0 and _env_bool(
            "SCRAPEFLOW_INTAKE_MONITOR", self.enforce_engine_roots,
        )

    def _automatic_audit_enabled(self) -> bool:
        """Run the startup audit by default only for the real media root."""
        return self._audit_auto_repair_enabled() and _env_bool(
            "SCRAPEFLOW_AUTOMATIC_AUDIT", self.enforce_engine_roots,
        )

    def _provider_auto_repair_enabled(self) -> bool:
        """Explicit production gate for Provider workers and retry timers."""
        return _env_bool(
            "SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED",
            not self.enforce_engine_roots,
        )

    def _audit_auto_repair_enabled(self) -> bool:
        """Explicit production gate for background audit/repair scheduling."""
        return _env_bool(
            "SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED",
            not self.enforce_engine_roots,
        )

    def _start_intake_monitor(self) -> None:
        if self._closed.is_set() or not self._intake_monitor_enabled():
            return
        thread = threading.Thread(
            target=self._intake_monitor_loop,
            daemon=True,
            name="scrapeflow-intake-monitor",
        )
        self._intake_thread = thread
        thread.start()

    @staticmethod
    def _safe_inbound_name(value: object) -> str | None:
        if not isinstance(value, str) or not value or value in {".", ".."}:
            return None
        if "/" in value or "\\" in value or "\x00" in value:
            return None
        return value

    @staticmethod
    def _automatic_job_needs_dispatch(job: EngineJob) -> bool:
        """Whether an existing intake item should be scheduled now.

        A terminal failure remains visible instead of being recreated on every
        30-second scan.  The scheduler already performs bounded retries for
        transient failures; a genuinely exhausted source can be retried after
        its naming/configuration problem is fixed, without turning the intake
        monitor into an endless duplicate-job generator.
        """
        return job.phase in {
            "queued", "analyzing", "archive_preprocessing", "identity_matching", "planning", "planned",
            "executing", "verifying", "cleaning", "retry_wait", "failed",
        }

    def _scan_inbound_once(self) -> list[str]:
        """Register each direct child of ``/待刮削`` as a waiting root.

        Only directories are accepted.  Treating loose files at the intake
        root as one job could accidentally combine unrelated titles, so they
        are left untouched until placed in their own source directory.  This
        method reads AList with ``refresh=True`` and only writes a small local
        ownership record.  It intentionally runs while globally paused: pause
        blocks formal work, not passive discovery of a user-visible choice.
        """
        if not self.engine_configured:
            return []
        runner = self._get_engine_runner()
        client = getattr(runner, "alist", None) or self._alist_client
        listing = getattr(client, "list", None)
        if not callable(listing):
            return []
        login = getattr(client, "login", None)
        if callable(login) and not getattr(client, "token", None):
            login()
        root = f"{self.remote_root.rstrip('/')}/待刮削"
        try:
            rows = listing(root, refresh=True)
        except TypeError:
            rows = listing(root)
        if not isinstance(rows, list):
            raise ApplicationError("AList 待刮削目录响应格式无效")
        try:
            existing = {}
            for job in runner.list_jobs():
                if not isinstance(job.request.get("source_path"), str):
                    continue
                # Archive preprocessing deliberately changes the planner's
                # source to task staging.  Intake de-duplication must retain
                # the original ingress directory or a successful archive job
                # would be recreated on every monitor pass.
                original = (
                    job.summary.get("ingress_source_path")
                    if isinstance(job.summary, Mapping)
                    else None
                )
                key = original if isinstance(original, str) else job.request.get("source_path")
                existing[str(key)] = job
        except Exception:
            existing = {}
        registered: list[str] = []
        seen_sources: set[str] = set()
        create = getattr(runner, "create_pending_job", None)
        if not callable(create):
            # Keep a small compatibility fallback for injected focused
            # runners. The real runner exposes create_pending_job.
            create = getattr(runner, "create_automatic_job", None)
        if not callable(create):
            return registered
        for row in rows:
            if not isinstance(row, Mapping) or row.get("is_dir") is not True:
                continue
            name = self._safe_inbound_name(row.get("name"))
            if name is None:
                continue
            source = f"{root}/{name}"
            # Production intake must never turn the retired E2E fixture
            # directories into even a queued task.  The Engine and formal
            # writer repeat this guard, but filtering at discovery keeps a
            # future smoke test from polluting the dashboard or consuming a
            # TMDB/provider retry slot in the first place.
            if is_production_test_media_path(source):
                continue
            seen_sources.add(source)
            job = existing.get(source)
            if job is None:
                job = create(source)
                registered.append(job.id)
        # A missing waiting source is an observation, not an instruction to
        # delete/retry/recreate it. Persist a clear error only after a
        # successful narrow listing of the intake root.
        marker = getattr(runner, "mark_waiting_source_missing", None)
        if callable(marker):
            for source, job in existing.items():
                if (
                    job.phase == "awaiting_target_shelf"
                    and source.startswith(root.rstrip("/") + "/")
                    and source not in seen_sources
                ):
                    try:
                        marker(job.id)
                    except Exception:
                        pass
        with self._automatic_lock:
            self._intake_status.update({
                "last_scan_at": _now(),
                "last_error": None,
                "last_scheduled_count": 0,
                "last_registered_count": len(registered),
            })
        return registered

    def _intake_monitor_loop(self) -> None:
        while not self._intake_stop.is_set():
            try:
                self._scan_inbound_once()
            except Exception:
                # A transient AList/TMDB problem is handled by the ordinary
                # job retry paths once a source is queued.  Before that, the
                # monitor simply tries again on the next read-only poll.
                with self._automatic_lock:
                    self._intake_status.update({
                        "last_scan_at": _now(),
                        "last_error": "待刮削目录暂时不可读取，将自动重试",
                        "last_scheduled_count": 0,
                        "last_registered_count": 0,
                    })
            self._intake_wake.wait(self._intake_scan_seconds())
            self._intake_wake.clear()

    def _get_engine_runner(self) -> SimpleEngineRunner:
        runner = self._engine_runner
        if runner is not None:
            return runner
        with self._engine_runner_lock:
            runner = self._engine_runner
            if runner is not None:
                return runner
            password = os.getenv("ALIST_PASSWORD", "").strip()
            tmdb_key = os.getenv("TMDB_API_KEY", "").strip()
            if not password or not tmdb_key:
                raise ApplicationError(
                    "Engine 计划需要同时配置 ALIST_PASSWORD 和 TMDB_API_KEY"
                )
            try:
                from engine.scraper import AListClient, TMDBClient
            except (ImportError, ModuleNotFoundError) as exc:
                raise ApplicationError("Engine 依赖不可用") from exc
            base_url = os.getenv("ALIST_URL", "http://alist:5244")
            allow_http = base_url.startswith(
                ("http://alist:", "http://127.0.0.1", "http://localhost")
            )
            alist = AListClient(
                base_url,
                os.getenv("ALIST_USERNAME", "admin"),
                password,
                allow_insecure_http=allow_http,
            )
            tmdb = TMDBClient(
                tmdb_key,
                language=os.getenv("TMDB_LANGUAGE", "zh-CN"),
            )
            runner = SimpleEngineRunner(
                self.state_root,
                alist=alist,
                tmdb=tmdb,
                library_root=self.remote_root,
                archive_preprocessor=self._archive_preprocessor,
            )
            self._engine_runner = runner
            return runner

    def _automatic_pool(self) -> ThreadPoolExecutor:
        with self._automatic_lock:
            if self._automatic_executor is None:
                self._automatic_executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="scrapeflow-formal-write",
                )
            return self._automatic_executor

    def _cancel_scheduled_timer(self, lane: str, owner: str) -> bool:
        """Cancel one pending timer without touching a running worker."""
        key = (lane, owner)
        with self._automatic_lock:
            timer = self._scheduled_timers.pop(key, None)
        if timer is None:
            return False
        try:
            timer.cancel()
        except Exception:
            pass
        return True

    def _cancel_job_timers(self, job_id: str) -> bool:
        """Cancel the two delayed queues that can own one public root."""
        automatic = self._cancel_scheduled_timer("automatic", job_id)
        provider = self._cancel_scheduled_timer("provider", job_id)
        return automatic or provider

    @staticmethod
    def _retry_archive_password(value: object) -> str:
        if not isinstance(value, str):
            raise EngineRequestError("归档密码必须是字符串")
        password = value.strip()
        if not password or len(password) > 128 or any(ord(char) < 32 for char in password):
            raise EngineRequestError("归档密码无效")
        return password

    @staticmethod
    def _manual_identity_correction(payload: Mapping[str, object]) -> dict[str, object] | None:
        """Validate the intentionally small failed-identity correction form."""
        # Identity correction is deliberately a four-field contract.  Shelf
        # naming, title/year metadata and the formal parent are derived by the
        # existing planner/policy; accepting them from Web would let a client
        # steer writes outside that policy.
        identity_fields = {"tmdb_id", "media_type", "season"}
        if not any(field in payload for field in identity_fields):
            return None
        raw_id = payload.get("tmdb_id")
        if isinstance(raw_id, str) and raw_id.isascii() and raw_id.isdecimal():
            raw_id = int(raw_id)
        if isinstance(raw_id, bool) or not isinstance(raw_id, int) or raw_id <= 0:
            raise EngineRequestError("手工修正需要正整数 tmdb_id")
        media_type = payload.get("media_type")
        if not isinstance(media_type, str) or media_type.strip().casefold() not in {"movie", "tv"}:
            raise EngineRequestError("手工修正需要 media_type=movie 或 tv")
        correction: dict[str, object] = {
            "tmdb_id": raw_id,
            "media_type": media_type.strip().casefold(),
        }
        raw_season = payload.get("season", 1)
        if isinstance(raw_season, str) and raw_season.isascii() and raw_season.isdecimal():
            raw_season = int(raw_season)
        if isinstance(raw_season, bool) or not isinstance(raw_season, int) or not 0 <= raw_season <= 999:
            raise EngineRequestError("手工修正 season 必须是 0–999 的整数")
        correction["season"] = raw_season
        return correction

    def _schedule_timer(
        self,
        lane: str,
        owner: str,
        delay: float,
        callback: Callable[[], None],
    ) -> None:
        """Replace a delayed callback atomically and run immediate work now.

        The wrapper removes itself before invoking the queue callback.  A
        callback which schedules a new retry therefore cannot accidentally
        cancel its own replacement, while a stale cancelled timer becomes a
        no-op after checking identity under the same lock.
        """
        key = (lane, owner)
        if delay <= 0:
            with self._automatic_lock:
                if owner in self._cleanup_fences:
                    return
                self._cancel_scheduled_timer(lane, owner)
                callback()
            return
        with self._automatic_lock:
            old = self._scheduled_timers.pop(key, None)
            if old is not None:
                try:
                    old.cancel()
                except Exception:
                    pass

            timer: threading.Timer

            def fire() -> None:
                with self._automatic_lock:
                    if owner in self._cleanup_fences:
                        self._scheduled_timers.pop(key, None)
                        return
                    if self._scheduled_timers.get(key) is not timer:
                        return
                    self._scheduled_timers.pop(key, None)
                    # Run the submit/reconciliation callback under the same
                    # re-entrant lock.  Cleanup therefore cannot observe an
                    # empty timer/future pair between this pop and callback's
                    # future insertion.
                    callback()

            timer = threading.Timer(float(delay), fire)
            timer.daemon = True
            self._scheduled_timers[key] = timer
            timer.start()

    @staticmethod
    def _automatic_retry_limit() -> int:
        raw = os.getenv("SCRAPEFLOW_AUTOMATIC_RETRY_LIMIT", "3").strip()
        try:
            return max(0, min(12, int(raw)))
        except ValueError:
            return 3

    @staticmethod
    def _provider_pilot_tmdb() -> int | None:
        """Return the optional TMDB identity allowlist for provider pilots.

        A pilot must fail closed when its selector is malformed.  Treating a
        typo as an absent selector would silently widen a deliberately bounded
        rollout back to every audited root.
        """
        raw = os.getenv("SCRAPEFLOW_PROVIDER_PILOT_TMDB", "").strip()
        if not raw:
            return None
        if not raw.isascii() or not raw.isdecimal():
            raise ApplicationError(
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB 必须是正整数"
            )
        value = int(raw)
        if value <= 0:
            raise ApplicationError(
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB 必须是正整数"
            )
        return value

    @staticmethod
    def _provider_pilot_gap() -> str | None:
        """Return an optional single-gap allowlist for a bounded pilot.

        The gap selector is deliberately narrower than a root selector.  It
        lets a multi-gap audit root prove one exact child closure without
        asking the provider to guess across unrelated specials.
        """
        raw = os.getenv("SCRAPEFLOW_PROVIDER_PILOT_GAP", "").strip().upper()
        if not raw:
            return None
        if not raw.isascii() or _PROVIDER_PILOT_GAP_RE.fullmatch(raw) is None:
            raise ApplicationError(
                "SCRAPEFLOW_PROVIDER_PILOT_GAP 必须是 SxxEyy 格式"
            )
        return raw

    @staticmethod
    def _provider_job_gap_ids(job: EngineJob) -> set[str]:
        """Extract exact episode coordinates from a persisted root plan."""
        plan = job.plan if isinstance(job.plan, Mapping) else {}
        scan = plan.get("scan_report") if isinstance(plan.get("scan_report"), Mapping) else {}
        rows = scan.get("resource_gaps") if isinstance(scan.get("resource_gaps"), list) else []
        output: set[str] = set()
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            raw_id = str(row.get("id") or "").upper()
            match = re.search(r"S\d{2}E\d{2}", raw_id)
            if match:
                output.add(match.group(0))
                continue
            season, episode = row.get("season"), row.get("episode")
            if (
                type(season) is int and 0 <= season <= 99
                and type(episode) is int and 0 < episode <= 99
            ):
                output.add(f"S{season:02d}E{episode:02d}")
        return output

    @classmethod
    def _provider_pilot_job(cls, job: EngineJob) -> EngineJob:
        """Project only the selected gap into a bounded provider pilot."""
        pilot_gap = cls._provider_pilot_gap()
        if pilot_gap is None:
            return job
        plan = dict(job.plan) if isinstance(job.plan, Mapping) else {}
        scan = plan.get("scan_report") if isinstance(plan.get("scan_report"), Mapping) else {}
        rows = scan.get("resource_gaps") if isinstance(scan.get("resource_gaps"), list) else []
        selected = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            row_id = str(row.get("id") or "").upper()
            coordinates = re.findall(r"S\d{2}E\d{2}", row_id)
            if not coordinates:
                season, episode = row.get("season"), row.get("episode")
                if type(season) is int and type(episode) is int:
                    coordinates = [f"S{season:02d}E{episode:02d}"]
            if pilot_gap in coordinates:
                selected.append(dict(row))
        filtered_scan = dict(scan)
        filtered_scan["resource_gaps"] = selected
        plan["scan_report"] = filtered_scan
        return replace(job, plan=plan)

    @staticmethod
    def _provider_job_tmdb(job: EngineJob) -> int | None:
        """Read a persisted root's trusted TMDB identity without guessing."""
        summary = job.summary if isinstance(job.summary, Mapping) else {}
        identity = summary.get("identity") if isinstance(summary.get("identity"), Mapping) else {}
        metadata = job.plan.get("metadata") if isinstance(job.plan.get("metadata"), Mapping) else {}
        raw = identity.get("tmdb_id") or metadata.get("tmdb_id") or summary.get("tmdb_id")
        if isinstance(raw, bool):
            return None
        if isinstance(raw, int) and raw > 0:
            return raw
        if isinstance(raw, str) and raw.isascii() and raw.isdecimal():
            value = int(raw)
            return value if value > 0 else None
        return None

    @classmethod
    def _provider_job_allowed(cls, job: EngineJob) -> bool:
        """Return whether a root may enter the current provider dispatch lane."""
        # A terminal provider attempt is a deliberate per-gap stop boundary.
        # Timers/futures that raced with exhaustion must not re-enter the lane;
        # only ``retry_public_job`` clears this projection for an explicit
        # operator retry (a fresh audit with a different fingerprint clears
        # the old projection before it is queued).
        replenishment = job.summary.get("replenishment") if isinstance(job.summary, Mapping) else None
        if isinstance(replenishment, Mapping) and replenishment.get("terminal") is True:
            return False
        pilot_tmdb = cls._provider_pilot_tmdb()
        if pilot_tmdb is not None and cls._provider_job_tmdb(job) != pilot_tmdb:
            return False
        pilot_gap = cls._provider_pilot_gap()
        return pilot_gap is None or pilot_gap in cls._provider_job_gap_ids(job)

    def _provider_runtime_cancel_requested(self, job: EngineJob) -> bool:
        """Return whether an already-running provider root must stop safely.

        The runtime invokes this only at cooperative boundaries, never in the
        middle of an external operation.  Any unreadable/malformed control or
        pilot state fails closed so a live worker cannot outrun an operator
        pause or accidentally widen a bounded rollout.
        """
        try:
            control = self.control()
            if self._closed.is_set() or control.get("paused") is not False:
                return True
            return not self._provider_job_allowed(job)
        except Exception:
            return True

    def _reconcile_paused_provider_root(self, job: EngineJob) -> EngineJob:
        """Mark a restart-orphaned provider projection retryable while paused.

        A process recreation cannot preserve an in-memory provider future. If
        persisted JSON still says ``acquiring`` there is no worker to make it
        true, so change only local durable state to ``retry_wait``.  Never do
        this while unpaused or while this process owns a live future: both
        cases may describe real work rather than an orphan.
        """
        try:
            with self._automatic_lock:
                # The startup thread may race an operator resume. Re-check
                # under the same projection lock used by provider queueing so
                # a stale paused recovery cannot overwrite a newly queued
                # ``gap_discovering``/live stage.
                if self.control().get("paused") is not True:
                    return job
                future = self._provider_futures.get(job.id)
                if future is not None and not future.done():
                    return job
                runner = self._get_engine_runner()
                current = runner.get_job(job.id)
                prior = current.summary.get("replenishment")
                if not isinstance(prior, Mapping):
                    return current
                if str(prior.get("status") or "") not in _ORPHANED_PROVIDER_PROGRESS_PHASES:
                    return current
                message = "服务重启或暂停后已安全停止在飞补源，等待恢复后重试"
                summary = dict(current.summary)
                replenishment = dict(prior)
                replenishment.update({
                    "status": "retry_wait",
                    "terminal": False,
                    "next_retry_seconds": None,
                    "error": message,
                    "updated_at": _now(),
                })
                summary["replenishment"] = replenishment
                summary["automatic_stage"] = "retry_wait"
                updated = replace(current, summary=summary, updated_at=_now())
                atomic_write_json(
                    runner.jobs_root / f"{current.id}.json",
                    _redacted_job_payload(updated),
                    allow_nan=False,
                )
                reconcile_interrupted_gap_states(self.state_root, current.id, error=message)
                return updated
        except Exception:
            # Recovery/reconciliation is advisory; a corrupted local record
            # must not make a paused control plane start a provider worker.
            return job

    @staticmethod
    def _is_internal_child(job: EngineJob) -> bool:
        """Return whether a persisted Engine record belongs to a root job.

        Provider children are restartable implementation records, not a second
        intake item.  Keep this check based on the explicit durable marker;
        inferring it from a staging path would make an old or renamed source
        look like a child by accident.
        """
        return isinstance(job.summary, Mapping) and job.summary.get("internal_child") is True

    @staticmethod
    def _is_audit_owned_root(job: EngineJob) -> bool:
        """Return whether a root was created from a full-library gap only."""
        return (
            isinstance(job.summary, Mapping)
            and job.summary.get("audit_owned") is True
            and job.summary.get("audit_work_key")
        )

    @classmethod
    def _ordinary_job_has_confirmed_selection(cls, job: EngineJob) -> bool:
        """Allow the scheduler to touch only a shelf-selected ordinary root.

        Audit-owned roots are a separate, explicitly scoped lifecycle lane and
        do not carry a user shelf.  Every other job must have all three
        durable selection fields before a worker can be queued or resumed;
        this single predicate keeps legacy records fail-closed across startup,
        retry and timer races.
        """
        if cls._is_audit_owned_root(job):
            return True
        return (
            isinstance(job.target_shelf, str)
            and bool(job.target_shelf)
            and isinstance(job.target_root, str)
            and bool(job.target_root)
            and isinstance(job.selected_at, str)
            and bool(job.selected_at)
        )

    @staticmethod
    def _audit_root_gap_is_safe(row: Mapping[str, object], job: EngineJob) -> bool:
        """Keep an audit-created root limited to its validated media work.

        Audit roots deliberately do not claim subtitle/metadata findings or
        rows whose embedded identity changed between scans.  Those findings
        remain visible in the audit report but cannot trigger a video child.
        """
        kind = SimpleApplication._audit_row_kind(row)
        if kind not in _AUDIT_ROOT_PROVIDER_GAP_KINDS and kind != "missing_subtitle":
            return False
        media = row.get("media") if isinstance(row.get("media"), Mapping) else {}
        summary = job.summary if isinstance(job.summary, Mapping) else {}
        identity = summary.get("identity") if isinstance(summary.get("identity"), Mapping) else {}
        metadata = job.plan.get("metadata") if isinstance(job.plan.get("metadata"), Mapping) else {}
        expected_id = identity.get("tmdb_id") or metadata.get("tmdb_id") or summary.get("tmdb_id")
        row_id = media.get("tmdb_id") if isinstance(media, Mapping) else None
        if row_id is None:
            row_id = row.get("tmdb_id")
        if expected_id is None or row_id is None or str(expected_id) != str(row_id):
            return False
        expected_target = (
            identity.get("target_root") or metadata.get("series_root")
            or metadata.get("target_root") or job.plan.get("target_root")
            or summary.get("target_root")
        )
        row_target = media.get("target_root") if isinstance(media, Mapping) else None
        if not isinstance(row_target, str) or not isinstance(expected_target, str):
            return False
        if row_target != expected_target:
            return False
        expected_type = "movie" if kind == "missing_media" else "tv"
        if kind == "missing_subtitle":
            # Subtitle gaps can belong to either a movie or a TV work.  Use
            # the already-validated job identity when available instead of
            # assuming every sidecar lives on the TV shelf.
            expected_type = str(
                identity.get("media_type")
                or metadata.get("media_type")
                or job.plan.get("mode")
                or ""
            ).casefold()
        row_type = str(media.get("media_type") or media.get("type") or "").casefold()
        if kind == "missing_subtitle":
            if row_type not in {"movie", "tv", "mixed"}:
                return False
            if expected_type in {"movie", "tv"} and row_type not in {expected_type, "mixed"}:
                return False
        elif row_type not in {expected_type, "mixed" if expected_type == "tv" else expected_type}:
            return False
        if not isinstance(row.get("id"), str) or not str(row.get("id")):
            return False
        if not isinstance(row.get("label"), str) or not str(row.get("label")).strip():
            return False
        if kind == "missing_media":
            return True
        if kind == "missing_subtitle":
            # Subtitle roots are still audit-owned implementation roots, but
            # the sidecar lane must be bound to one exact, already-audited
            # video under this target.  Never let a free-form label/path turn
            # into a whole-tree provider write.
            path = row.get("path")
            if (
                not isinstance(path, str)
                or not path.startswith(expected_target.rstrip("/") + "/")
                or not is_video_filename(path)
                or not isinstance(row.get("subtitle_language"), str)
                or not row.get("subtitle_language", "").strip()
            ):
                return False
            return True
        season = row.get("season")
        if isinstance(season, bool) or not isinstance(season, int) or season < 0 or season > 999:
            return False
        if kind == "missing_season":
            return True
        episode = row.get("episode")
        return (
            isinstance(episode, int)
            and not isinstance(episode, bool)
            and 0 < episode <= 9999
        )

    @staticmethod
    def _is_terminal_automatic_failure(job: EngineJob) -> bool:
        """Whether bounded automatic retries have deliberately stopped.

        Intake polling must never turn a final failure into an unbounded
        write/recovery loop just because its source directory is still under
        ``/待刮削``.  An operator retry explicitly reopens this state after a
        configuration or source-name correction.
        """
        if not isinstance(job.summary, Mapping):
            return job.phase in {
                "failed_archive", "failed_identity", "failed_planning", "failed_write",
                "failed_verification", "failed_cleanup",
            }
        return job.summary.get("automatic_terminal") is True or job.phase in {
            "failed_archive", "failed_identity", "failed_planning", "failed_write",
            "failed_verification", "failed_cleanup",
        }

    @staticmethod
    def _has_provider_gaps(job: EngineJob) -> bool:
        """Return only gaps the active provider chain can actually process.

        Movie ``missing_media`` gaps share the verified video/child-plan path
        with episode and season gaps. ``missing_subtitle`` uses the
        subtitle-only staging/sidecar path, so it cannot be turned into a
        whole-video download or a user-interaction follow-up task.
        """
        if SimpleApplication._is_internal_child(job):
            return False
        prior = job.summary.get("replenishment") if isinstance(job.summary.get("replenishment"), Mapping) else {}
        if isinstance(prior, Mapping) and prior.get("terminal") is True:
            return False
        scan = job.plan.get("scan_report") if isinstance(job.plan.get("scan_report"), Mapping) else {}
        rows = scan.get("resource_gaps") if isinstance(scan, Mapping) else []
        return any(
            isinstance(row, Mapping)
            and str(row.get("kind") or "") in _AUTOMATIC_PROVIDER_GAP_KINDS
            for row in (rows if isinstance(rows, list) else [])
        )

    @staticmethod
    def _job_resource_gaps(job: EngineJob) -> list[dict[str, object]]:
        """Return the persisted plan gaps without treating an empty/malformed
        scan report as success.

        The Engine's formal write fact and the root workflow's completion fact
        are intentionally separate.  This helper is used by the public
        projection as a second line of defence when an audit has not yet
        rewritten the summary.
        """
        scan = job.plan.get("scan_report") if isinstance(job.plan.get("scan_report"), Mapping) else {}
        rows = scan.get("resource_gaps") if isinstance(scan, Mapping) else []
        return [dict(row) for row in rows if isinstance(row, Mapping)] if isinstance(rows, list) else []

    @staticmethod
    def _audit_row_kind(row: Mapping[str, object]) -> str:
        return str(row.get("kind") or "").strip().casefold()

    @staticmethod
    def _provider_gap_signature(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
        """Stable semantic identity for one fresh provider-gap observation."""
        canonical: list[dict[str, object]] = []
        for row in rows:
            media = row.get("media") if isinstance(row.get("media"), Mapping) else {}
            episodes = row.get("episodes")
            episode_range: list[int] | None = None
            if isinstance(episodes, list):
                episode_numbers = [
                    value for value in episodes
                    if type(value) is int and value > 0
                ]
                if len(episode_numbers) > 1:
                    episode_range = [min(episode_numbers), max(episode_numbers)]
            canonical.append({
                "kind": SimpleApplication._audit_row_kind(row),
                "tmdb_id": media.get("tmdb_id") if isinstance(media, Mapping) else row.get("tmdb_id"),
                "media_type": media.get("media_type") if isinstance(media, Mapping) else row.get("media_type"),
                "target_root": media.get("target_root") if isinstance(media, Mapping) else row.get("target_root"),
                "path": row.get("path"),
                "season": row.get("season"),
                "episode": row.get("episode"),
                "episode_range": episode_range,
                "subtitle_language": row.get("subtitle_language"),
            })
        return sorted(
            canonical,
            key=lambda item: json.dumps(
                item, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ),
        )

    @staticmethod
    def _lifecycle_allows_completed_with_gaps(job: EngineJob) -> bool:
        """Whether known gaps are terminally deferred rather than in flight.

        ``completed_with_gaps`` is deliberately a public projection, not a
        new Engine phase.  The durable Engine record remains ``executed`` so
        recovery and cleanup keep their small existing state machine.  This
        predicate requires final cleanup to have completed and both optional
        lanes to have recorded an explicit deferred/skipped outcome; it never
        treats a missing, active, failed, or unknown decision as completion.
        """
        if job.phase not in {"executed", "completed"}:
            return False
        lifecycle_raw = job.summary.get("lifecycle")
        lifecycle = lifecycle_raw if isinstance(lifecycle_raw, Mapping) else {}
        cleanup = lifecycle.get("cleanup") if isinstance(lifecycle.get("cleanup"), Mapping) else {}
        if cleanup.get("status") != "completed":
            return False
        audit = lifecycle.get("audit") if isinstance(lifecycle.get("audit"), Mapping) else {}
        provider = lifecycle.get("provider") if isinstance(lifecycle.get("provider"), Mapping) else {}
        return (
            str(audit.get("status") or "").casefold() in {"trusted", "deferred", "skipped"}
            and str(provider.get("status") or "").casefold() in {"deferred", "skipped"}
        )

    @staticmethod
    def _audit_phase_for_job(job: EngineJob) -> tuple[str | None, str | None]:
        """Map audit state to a truthful public root-workflow phase."""
        summary_audit = job.summary.get("audit")
        audit = dict(summary_audit) if isinstance(summary_audit, Mapping) else {}
        status = str(audit.get("status") or "").casefold()
        if status in {"unknown", "blocked", "failed", "failed_provider"}:
            phase = "failed_provider" if status == "failed_provider" else "failed_verification"
            reason = str(audit.get("message") or audit.get("error") or "媒体库审计仍有未收口问题")
            return phase, reason
        if status in {"pending", "repairing", "retry_wait"}:
            return "retry_wait", str(audit.get("message") or "媒体库审计发现问题，等待自动重试")

        gaps = SimpleApplication._job_resource_gaps(job)
        provider = [row for row in gaps if SimpleApplication._audit_row_kind(row) in _AUTOMATIC_PROVIDER_GAP_KINDS]
        unsupported = [
            row for row in gaps
            if SimpleApplication._audit_row_kind(row) in (
                _AUTOMATIC_UNSUPPORTED_GAP_KINDS | _AUTOMATIC_REPAIR_GAP_KINDS
            )
        ]
        if unsupported:
            # A persisted plan can predate the first full audit.  Do not show
            # it as completed while a known unsupported/repairable finding is
            # still attached to the plan.
            repair = any(SimpleApplication._audit_row_kind(row) in _AUTOMATIC_REPAIR_GAP_KINDS for row in unsupported)
            return (
                "failed_verification" if repair else "failed_provider",
                "正式库仍缺少元数据/媒体/字幕，系统正在等待自动收口"
                if repair else "正式库存在当前补源器无法处理的媒体或字幕缺口",
            )
        if provider:
            replenishment = job.summary.get("replenishment")
            if isinstance(replenishment, Mapping):
                provider_status = str(replenishment.get("status") or "").casefold()
                if provider_status == "child_failed":
                    # The child has a durable failed record, but the provider
                    # round may still select another candidate.  Keep the
                    # root visibly non-green while that retry decision runs.
                    return "retry_wait", str(
                        replenishment.get("error") or "补源 child 失败，等待自动重试"
                    )
                if provider_status == "failed" and replenishment.get("terminal") is True:
                    return "failed_provider", str(replenishment.get("error") or "自动补源未能收口媒体缺口")
                if provider_status == "retry_wait":
                    return "retry_wait", str(
                        replenishment.get("error") or "补源已排队，等待下一次自动重试"
                    )
                if provider_status in {
                    "gap_discovering", "provider_searching", "acquiring",
                    "staging_verifying", "subtitle_installing", "child_planning", "child_executing",
                    "final_verifying", "cleaning",
                }:
                    return provider_status, str(
                        replenishment.get("message") or "系统正在自动处理媒体库缺口"
                    )
                if provider_status in {"completed", "resolved"}:
                    # The provider result is not itself a fresh full-library
                    # audit.  Keep the task pending until the next scan proves
                    # that the resource gap disappeared.
                    return "retry_wait", "补源已返回，等待全库审计确认缺口已消失"
            if SimpleApplication._lifecycle_allows_completed_with_gaps(job):
                return "completed_with_gaps", f"已完成正式整理；保留 {len(gaps)} 项资源缺口（自动补源已跳过）"
            return "gap_discovering", "系统正在自动处理媒体库缺口"
        if gaps and SimpleApplication._lifecycle_allows_completed_with_gaps(job):
            return "completed_with_gaps", f"已完成正式整理；保留 {len(gaps)} 项资源缺口（自动补源已跳过）"
        return None, None

    @staticmethod
    def _audit_unknowns_defer_automatic_retry(unknowns: object) -> bool:
        """Return whether unknown evidence should wait for a fresh trigger.

        TMDB catalog/identity uncertainty and bounded subtitle-probe
        uncertainty are intentionally non-green, but an identical periodic
        inventory cannot make them conclusive.  Keep this helper tolerant of
        old/malformed JSON so startup recovery itself never becomes a retry
        storm.
        """
        if not isinstance(unknowns, (list, tuple)) or not unknowns:
            return False
        return all(
            isinstance(row, Mapping)
            and SimpleApplication._audit_row_kind(row) in _AUDIT_DEFERRED_UNKNOWN_KINDS
            for row in unknowns
        )

    @staticmethod
    def _audit_needs_retry(job: EngineJob) -> bool:
        audit = job.summary.get("audit")
        if not isinstance(audit, Mapping):
            return False
        status = str(audit.get("status") or "").casefold()
        if audit.get("automatic_retry") is False:
            return False
        # Bounded TMDB/subtitle evidence intentionally remains fail-closed
        # ``unknown``. Re-running the same inventory every 30 seconds cannot
        # turn it into a verdict; a fresh media commit or explicit audit is
        # the retry boundary.
        if status == "unknown":
            unknowns = audit.get("unknowns")
            if audit.get("automatic_retry") is False or SimpleApplication._audit_unknowns_defer_automatic_retry(unknowns):
                return False
        return status in {
            "unknown", "blocked", "failed_provider", "failed", "retry_wait", "repairing", "pending",
        }

    def _settle_disabled_automatic_lifecycle(self, job: EngineJob) -> bool:
        """Close a verified ordinary root when both optional lanes are off.

        Audit and Provider are intentionally opt-in in the production root.
        A successful formal write must not remain forever in ``cleaning``
        merely because neither optional lane was scheduled.  This is a narrow
        lifecycle decision, not a substitute audit: it runs only for a
        verified, ordinary automatic root, records both lanes as deferred,
        and then delegates all source/staging work to the existing finalizer.

        ``True`` means the caller is in the disabled-lane regime and must not
        queue an audit as a fallback.  Existing unknown, pending, failed, or
        live Provider decisions deliberately remain untouched; disabling a
        lane must never turn known unresolved evidence into permission to
        consume the source.
        """
        if self._audit_auto_repair_enabled() or self._provider_auto_repair_enabled():
            return False
        if self._closed.is_set() or self.control().get("paused") is True:
            return True
        if (
            self._is_internal_child(job)
            or self._is_audit_owned_root(job)
            or job.summary.get("automatic") is not True
            or job.phase not in {"executed", "completed"}
        ):
            return True

        lifecycle_raw = job.summary.get("lifecycle")
        lifecycle = dict(lifecycle_raw) if isinstance(lifecycle_raw, Mapping) else {}
        cleanup = lifecycle.get("cleanup") if isinstance(lifecycle.get("cleanup"), Mapping) else {}
        if cleanup.get("status") in {"completed", "running"}:
            return True
        formal_write = lifecycle.get("formal_write")
        if not isinstance(formal_write, Mapping) or formal_write.get("status") != "verified":
            return True

        audit = lifecycle.get("audit") if isinstance(lifecycle.get("audit"), Mapping) else {}
        provider = lifecycle.get("provider") if isinstance(lifecycle.get("provider"), Mapping) else {}
        audit_status = str(audit.get("status") or "").casefold()
        provider_status = str(provider.get("status") or "").casefold()
        # ``trusted`` is the only prior audit conclusion safe to retain; a
        # fresh empty decision is also safe because the lanes are explicitly
        # disabled. Any other existing decision contains unresolved evidence.
        if audit_status and audit_status not in {"trusted", "deferred", "skipped", "no_gap"}:
            return True
        if provider_status and provider_status not in {"deferred", "skipped", "no_gap"}:
            return True
        with self._automatic_lock:
            future = self._provider_futures.get(job.id)
            if future is not None and not future.done():
                return True
        try:
            runner = self._get_engine_runner()
            decided = runner.record_automatic_lifecycle_decision(
                job.id,
                audit_status="deferred",
                provider_status="deferred",
                cleanup_ready=True,
                reason="audit_and_provider_auto_repair_disabled",
            )
            runner.finalize_automatic_lifecycle(decided.id)
        except (EngineWorkerBusyError, EngineExecutionError, EngineRequestError):
            # The finalizer persists its own failed_cleanup record.  A busy
            # worker/restart will revisit the same idempotent decision; never
            # route it back into plan/write or an optional disabled lane.
            pass
        return True

    @staticmethod
    def _audit_row_matches_job(
        row: Mapping[str, object], *, job_tmdb: object, job_target: object,
    ) -> bool:
        """Match semantic gap/unknown rows to a persisted automatic work."""
        media = row.get("media") if isinstance(row.get("media"), Mapping) else {}
        row_tmdb = media.get("tmdb_id") if isinstance(media, Mapping) else None
        if row_tmdb is None:
            row_tmdb = row.get("tmdb_id")
        row_target = media.get("target_root") if isinstance(media, Mapping) else None
        if not isinstance(row_target, str) or not row_target:
            row_target = row.get("target_root")
        row_path = row.get("path")
        if job_tmdb is not None and row_tmdb is not None and str(job_tmdb) != str(row_tmdb):
            return False
        if isinstance(job_target, str):
            target = str(row_target) if isinstance(row_target, str) else ""
            path = str(row_path) if isinstance(row_path, str) else ""
            if target:
                return target == job_target or target.startswith(job_target.rstrip("/") + "/") or job_target.startswith(target.rstrip("/") + "/")
            if path:
                return path == job_target or path.startswith(job_target.rstrip("/") + "/")
        work = str(row.get("work") or "")
        if job_tmdb is not None and work in {f"tmdb:{job_tmdb}", f"tmdb:tv:{job_tmdb}", f"tmdb:movie:{job_tmdb}"}:
            return True
        return False

    def _queue_automatic_job(self, job_id: str, *, delay: float = 0.0) -> None:
        """Run one persisted plan from the automatic scheduler."""
        if self._closed.is_set() or self.control().get("paused") is True:
            return
        try:
            queued_job = self._get_engine_runner().get_job(job_id)
        except (EngineJobNotFoundError, SimpleEngineError):
            return
        if not self._ordinary_job_has_confirmed_selection(queued_job):
            return

        def submit() -> None:
            if self._closed.is_set() or self.control().get("paused") is True:
                return
            with self._automatic_lock:
                existing = self._automatic_futures.get(job_id)
                if existing is not None and not existing.done():
                    return
                self._automatic_futures[job_id] = self._automatic_pool().submit(
                    self._run_automatic_job, job_id,
                )

        self._schedule_timer("automatic", job_id, delay, submit)

    @staticmethod
    def _automatic_failure_stage(error: Exception, job: EngineJob | None = None) -> str:
        text = str(error).casefold()
        if job is not None:
            if job.phase == "archive_preprocessing":
                return "archive"
            if job.phase == "planning":
                return "planning"
        if job is not None and not job.plan:
            return "identity"
        if any(token in text for token in ("tmdb", "匹配", "identity", "作品身份", "confidence")):
            return "identity"
        if any(token in text for token in ("readback", "不可见", "大小", "核对", "源文件仍", "海报")):
            return "verification"
        return "write"

    def _record_automatic_retry(
        self,
        job_id: str,
        error: Exception,
        *,
        stage: str | None = None,
    ) -> None:
        """Persist bounded retry state for the automatic scheduler."""
        runner = self._get_engine_runner()
        phase: str | None = None
        retry_delay: float | None = None
        summary: dict[str, object] = {}
        try:
            # Retry projection and cancellation share the same durable worker
            # fence. A stale exception handler must never resurrect a job that
            # an operator just cancelled or arm a timer from an old snapshot.
            with runner.worker_lock():
                job = runner.get_job(job_id)
                cancelled = runner._consume_cancel_request(job)  # noqa: SLF001 - fenced transition
                if cancelled is not None:
                    self._cancel_job_timers(job_id)
                    return
                if job.phase in {"executed", "cancelled"} or self._is_terminal_automatic_failure(job):
                    self._cancel_job_timers(job_id)
                    return
                summary = dict(job.summary)
                # A stale planning/execute operation id must not survive a retry and
                # accidentally match a later cancellation request.
                summary.pop("active_operation", None)
                stage = stage or self._automatic_failure_stage(error, job)
                if stage == "cleanup" or job.phase == "failed_cleanup":
                    # Keep this lane out of the ordinary retry scheduler.  A later
                    # explicit retry may invoke only the idempotent lifecycle
                    # finalizer.
                    summary["cleanup_only_retry"] = True
                attempts = int(summary.get("automatic_attempts") or 0) + 1
                summary["automatic_attempts"] = attempts
                summary[f"{stage}_attempts"] = int(summary.get(f"{stage}_attempts") or 0) + 1
                summary["automatic_stage"] = stage
                summary["next_retry_seconds"] = None
                phase = {
                    "archive": "failed_archive",
                    "identity": "failed_identity",
                    "planning": "failed_planning",
                    "provider": "failed_provider",
                    "verification": "failed_verification",
                    "cleanup": "failed_cleanup",
                }.get(stage, "failed_write")
                if stage == "cleanup" or job.phase == "failed_cleanup":
                    # Cleanup is a separate idempotent finalizer boundary. Never turn
                    # its failure into retry_wait, which would send a second formal
                    # writer through the ordinary scheduler.
                    phase = "failed_cleanup"
                    summary["automatic_terminal"] = True
                elif attempts <= self._automatic_retry_limit():
                    retry_delay = min(60.0, float(2 ** max(0, attempts - 1)))
                    summary["next_retry_seconds"] = retry_delay
                    phase = "retry_wait"
                    summary["automatic_terminal"] = False
                else:
                    summary["automatic_terminal"] = True
                updated = replace(
                    job,
                    phase=phase,
                    updated_at=_now(),
                    summary=summary,
                    error=redact_error(error),
                )
                atomic_write_json(
                    runner.jobs_root / f"{job_id}.json",
                    _redacted_job_payload(updated),
                    allow_nan=False,
                )
        except EngineJobNotFoundError:
            return
        except EngineWorkerBusyError:
            # The current worker will either persist its own terminal state or
            # be reconciled by the next scheduler pass; do not overwrite it
            # from this stale exception handler.
            return
        if phase != "retry_wait":
            self._cancel_job_timers(job_id)
        if phase == "retry_wait":
            self._queue_automatic_job(job_id, delay=float(retry_delay or 1))
        elif phase == "failed_identity" and summary.get("automatic_terminal") is True:
            # A terminal identity failure has no trusted work root.  Do not
            # widen it into a full-library scan; only an explicit operator
            # audit (or a later retry that supplies identity) may establish a
            # bounded work scope.
            return

    def _sync_replenishment_child(self, root: EngineJob) -> EngineJob:
        """Project durable provider-child state back onto one public root.

        Provider children are implementation records and are intentionally not
        listed as public tasks.  A process restart can nevertheless leave the
        root's last progress callback one phase behind the child JSON (for
        example ``child_executing`` after the child was already read back as
        ``executed``).  Refresh the child rows from durable records before the
        scheduler makes a new provider decision.  The method is local-state
        reconciliation only; it never invokes a provider or writes the media
        library.
        """
        runner = self._engine_runner
        if runner is None:
            try:
                runner = self._get_engine_runner()
            except Exception:
                return root
        try:
            current = runner.get_job(root.id)
            if self._is_internal_child(current):
                return current
            persisted = runner.list_jobs()
        except Exception:
            return root

        children: list[EngineJob] = []
        for candidate in persisted:
            if not self._is_internal_child(candidate):
                continue
            child_summary = candidate.summary if isinstance(candidate.summary, Mapping) else {}
            if child_summary.get("root_job_id") != current.id:
                continue
            reconciled = candidate
            # A child can be left in-flight when the API process disappears.
            # ``recover_job`` performs exact readback and is deliberately
            # read-only; use it when available, but keep a malformed/temporary
            # fixture visible rather than dropping the child projection.
            recover = getattr(runner, "recover_job", None)
            if callable(recover) and candidate.phase in {
                "executing", "verifying", "cleaning", "retry_wait",
            }:
                try:
                    maybe = recover(candidate.id)
                    if isinstance(maybe, EngineJob):
                        reconciled = maybe
                except Exception:
                    pass
            children.append(reconciled)

        if not children:
            return current

        prior_replenishment = current.summary.get("replenishment")
        replenishment = (
            dict(prior_replenishment)
            if isinstance(prior_replenishment, Mapping)
            else {}
        )
        prior_rows_raw = replenishment.get("child_jobs")
        prior_rows: dict[str, dict[str, object]] = {}
        if isinstance(prior_rows_raw, list):
            for raw in prior_rows_raw:
                if not isinstance(raw, Mapping):
                    continue
                child_id = raw.get("id")
                if isinstance(child_id, str) and child_id:
                    prior_rows[child_id] = dict(raw)

        successful_phases = {"executed", "completed"}
        failed_phases = {
            "failed", "failed_identity", "failed_provider", "failed_write",
            "failed_verification", "failed_cleanup",
        }
        active_phases = {
            "queued", "analyzing", "archive_preprocessing", "identity_matching", "planning", "planned",
            "executing", "verifying", "cleaning", "retry_wait",
        }
        public_phase = {
            "queued": "child_planning", "analyzing": "child_planning",
            "identity_matching": "child_planning", "planning": "child_planning",
            "planned": "child_planning", "executing": "child_executing",
            "verifying": "final_verifying", "cleaning": "cleaning",
            "retry_wait": "retry_wait",
        }
        child_rows: list[dict[str, object]] = []
        for child in sorted(children, key=lambda item: (item.updated_at, item.id)):
            phase = child.phase
            row = dict(prior_rows.pop(child.id, {}))
            row.update({
                "id": child.id,
                "phase": phase,
                "engine_phase": phase,
                "public_phase": (
                    "completed" if phase in successful_phases
                    else "child_failed" if phase in failed_phases
                    else "cancelled" if phase == "cancelled"
                    else public_phase.get(phase, phase)
                ),
                "updated_at": child.updated_at,
                "terminal": phase in successful_phases or phase in failed_phases or phase == "cancelled",
                "success": phase in successful_phases,
            })
            if child.error:
                row["error"] = redact_error(child.error)
            elif phase in successful_phases:
                row.pop("error", None)
            child_rows.append(row)
        # Preserve historical child attempts after their JSON is manually
        # archived; current rows above always win for an existing id.
        child_rows.extend(prior_rows.values())
        child_rows.sort(key=lambda row: (str(row.get("updated_at") or ""), str(row.get("id") or "")))

        active_children = [child for child in children if child.phase in active_phases]
        failed_children = [child for child in children if child.phase in failed_phases]
        successful_children = [child for child in children if child.phase in successful_phases]
        cancelled_children = [child for child in children if child.phase == "cancelled"]
        prior_status = str(replenishment.get("status") or "").casefold()
        prior_terminal = replenishment.get("terminal") is True
        if active_children:
            current_child = max(active_children, key=lambda item: (item.updated_at, item.id))
            status = public_phase.get(current_child.phase, "retry_wait")
            terminal = False
            error = current_child.error
        elif failed_children:
            # Preserve an explicit exhausted provider budget.  A child failure
            # discovered during restart is otherwise retryable by design.
            status = prior_status if prior_terminal and prior_status in {
                "failed", "failed_provider",
            } else "child_failed"
            terminal = prior_terminal and status in {"failed", "failed_provider"}
            error = next(
                (child.error for child in reversed(sorted(failed_children, key=lambda item: item.updated_at)) if child.error),
                None,
            )
        elif successful_children:
            status = (
                prior_status if prior_terminal and prior_status in {"failed", "failed_provider"}
                else "completed"
            )
            terminal = (
                (prior_terminal and status in {"failed", "failed_provider"})
                or status == "completed"
            )
            error = None
        elif cancelled_children:
            status = "cancelled"
            terminal = True
            error = None
        else:
            status, terminal, error = prior_status or "retry_wait", prior_terminal, None

        prior_core = {
            key: value
            for key, value in replenishment.items()
            if key != "updated_at"
        }
        next_core = {
            **prior_core,
            "status": status,
            "terminal": terminal,
            "child_jobs": child_rows,
        }
        if error:
            next_core["error"] = redact_error(error)
        elif status == "completed":
            next_core.pop("error", None)
        current_core = (
            {
                key: value
                for key, value in prior_replenishment.items()
                if key != "updated_at"
            }
            if isinstance(prior_replenishment, Mapping)
            else {}
        )
        if (
            next_core == current_core
            and str(current.summary.get("automatic_stage") or "") == status
        ):
            return current
        replenishment.update({**next_core, "updated_at": _now()})
        summary = dict(current.summary)
        summary["replenishment"] = replenishment
        summary["automatic_stage"] = status
        updated = replace(current, summary=summary, updated_at=_now())
        lock_factory = getattr(runner, "worker_lock", None)
        if callable(lock_factory):
            try:
                with lock_factory():
                    # Cleanup and formal execution share this lock. Re-read
                    # before writing so a concurrent terminal cleanup cannot
                    # be undone by this projection pass.
                    latest = runner.get_job(current.id)
                    if latest.as_dict() != current.as_dict():
                        return latest
                    atomic_write_json(
                        runner.jobs_root / f"{current.id}.json",
                        _redacted_job_payload(updated),
                        allow_nan=False,
                    )
            except EngineJobNotFoundError:
                return current
        else:
            atomic_write_json(
                runner.jobs_root / f"{current.id}.json",
                _redacted_job_payload(updated),
                allow_nan=False,
            )
        return updated

    def _run_automatic_job(self, job_id: str) -> None:
        """Reconcile first, then execute only the still-missing plan work."""
        if self.control().get("paused") is True:
            return
        try:
            runner = self._get_engine_runner()
            job = runner.get_job(job_id)
            if self._is_internal_child(job):
                return
            if job.phase in {"awaiting_target_shelf", "target_policy_conflict"}:
                return
            # A pre-gate legacy record must never become a formal operation
            # merely because a timer or stale retry scheduled it.
            if not self._ordinary_job_has_confirmed_selection(job):
                return
            if job.phase in {"executed", "completed"}:
                self._settle_disabled_automatic_lifecycle(job)
                return
            if job.phase == "cancelled":
                return
            if self._is_terminal_automatic_failure(job):
                return
            if job.summary.get("cleanup_only_retry") is True:
                # Never route a cleanup-only record through recovery or the
                # formal writer. The public retry endpoint owns finalizer
                # dispatch for this lane.
                return
            if job.phase == "failed_cleanup":
                # Only an explicit public retry may invoke the lifecycle
                # finalizer; this worker must never replay the formal plan.
                return
            # A queued/retry identity job has no plan yet.  Resolve it inside
            # the same scheduler; a transient TMDB error is persisted and
            # retried rather than returned as a transient HTTP error.
            if job.phase in {"queued", "archive_preprocessing", "identity_matching", "planning", "failed_identity"} or (
                job.phase == "retry_wait" and not job.plan
            ):
                try:
                    with self._automatic_lock:
                        retry_password = self._retry_archive_passwords.get(job_id)
                    job = (
                        runner.plan_automatic_job(job_id, retry_password=retry_password)
                        if retry_password is not None
                        else runner.plan_automatic_job(job_id)
                    )
                    if retry_password is not None:
                        with self._automatic_lock:
                            # Consume only after the planner persisted its
                            # result. A wrong password remains available for
                            # this in-memory retry until the caller replaces
                            # it or the process exits.
                            self._retry_archive_passwords.pop(job_id, None)
                except Exception as exc:
                    self._record_automatic_retry(job_id, exc)
                    return
            if job.phase in {"executing", "retry_wait", "failed", "failed_write", "failed_verification"}:
                job = runner.recover_job(job_id)
                # The restart matrix may have converted an ambiguous write
                # into a durable terminal verification failure.  Never fall
                # through to execute_automatic: that would turn a confirmed
                # target/source conflict back into another write retry.
                if self._is_terminal_automatic_failure(job):
                    self._cancel_job_timers(job_id)
                    return
                if job.phase == "executed":
                    job = self._sync_replenishment_child(job)
                    if self._settle_disabled_automatic_lifecycle(job):
                        return
                    # Audit-owned roots are created by a trusted audit
                    # projection and already sit on the provider lane; they
                    # retain their historical dispatch path. Ordinary roots
                    # must wait for the post-write scoped audit below.
                    if self._is_audit_owned_root(job) and self._has_provider_gaps(job):
                        self._queue_provider_job(job.id)
                    target = self._job_audit_target(job)
                    if isinstance(target, str):
                        self._queue_scoped_library_audit([target], delay=0.5)
                    return
            if job.phase not in {"planned", "retry_wait", "failed", "failed_write", "failed_verification"}:
                return
            done = runner.execute_automatic(job_id)
            done = self._sync_replenishment_child(done)
            if self._settle_disabled_automatic_lifecycle(done):
                return
            # The child may have committed a video while another audit was
            # already running.  Ask the audit coordinator for one fresh pass
            # after that run settles; this is still read-only and does not
            # widen the provider/root identity.
            target = self._job_audit_target(done)
            if isinstance(target, str):
                self._queue_scoped_library_audit(
                    [target], delay=0.5, rerun_if_busy=True,
                )
            # A committed provider child must always have the durable root
            # coordinates above.  Missing coordinates are not permission to
            # scan the entire formal library.
        except EngineWorkerBusyError:
            # Another process is already proving the same remote state.  A
            # short requeue is enough; no second writer is started.
            self._queue_automatic_job(job_id, delay=1.0)
        except Exception as exc:
            self._record_automatic_retry(job_id, exc)

    def _resume_automatic_jobs(self) -> None:
        if not self.engine_configured:
            return
        try:
            runner = self._get_engine_runner()
            paused = self.control().get("paused") is True
            for job in runner.list_jobs():
                if self._is_internal_child(job):
                    continue
                if job.phase in {"awaiting_target_shelf", "target_policy_conflict"}:
                    continue
                if not self._ordinary_job_has_confirmed_selection(job):
                    # Legacy pre-gate roots remain discoverable/read-only but
                    # cannot be recovered into archive/TMDB/planning/writing.
                    continue
                if job.summary.get("cleanup_only_retry") is True:
                    # A cleanup-only retry is operator-driven and must not be
                    # requeued as an ordinary plan/write job on restart.
                    continue
                if paused:
                    if job.phase in {"executed", "completed"}:
                        self._reconcile_paused_provider_root(job)
                    # A paused startup is strictly a local-state recovery
                    # pass. It must not requeue providers, formal writes, or
                    # an audit that could change remote state.
                    continue
                if self._is_terminal_automatic_failure(job):
                    continue
                if job.phase in {
                    "queued", "archive_preprocessing", "identity_matching", "planning", "planned", "executing",
                    "retry_wait", "failed", "failed_write", "failed_verification", "failed_cleanup",
                }:
                    self._queue_automatic_job(job.id)
                elif job.phase in {"executed", "completed"}:
                    job = self._sync_replenishment_child(job)
                    if self._settle_disabled_automatic_lifecycle(job):
                        continue
                    if self._is_audit_owned_root(job) and self._has_provider_gaps(job):
                        self._queue_provider_job(job.id)
                    replenishment = job.summary.get("replenishment")
                    if (
                        isinstance(replenishment, Mapping)
                        and replenishment.get("status") == "completed"
                        and isinstance(replenishment.get("child_jobs"), list)
                    ):
                        # A recovered child proves its own move, not a global
                        # inventory. Re-audit only this root's work scope.
                        target = self._job_audit_target(job)
                        if isinstance(target, str):
                            self._queue_scoped_library_audit([target], delay=0.5)
                    if self._audit_needs_retry(job):
                        target = self._job_audit_target(job)
                        if isinstance(target, str):
                            self._queue_scoped_library_audit([target], delay=1.0)
        except Exception:
            # Health/status endpoints remain available while a network or
            # credential issue is repaired; an explicit resume/retry will
            # invoke the same automatic path later.
            return

    def _provider_pool(self) -> ThreadPoolExecutor:
        with self._automatic_lock:
            if self._provider_executor is None:
                workers_raw = os.getenv("SCRAPEFLOW_PROVIDER_WORKERS", "1").strip()
                try:
                    workers = max(1, min(8, int(workers_raw)))
                except ValueError:
                    workers = 1
                self._provider_executor = ThreadPoolExecutor(
                    max_workers=workers,
                    thread_name_prefix="scrapeflow-provider",
                )
            return self._provider_executor

    def _get_automatic_replenishment(self) -> AutomaticReplenishmentRuntime:
        runtime = self._automatic_replenishment
        if runtime is not None:
            return runtime
        with self._automatic_replenishment_lock:
            runtime = self._automatic_replenishment
            if runtime is not None:
                return runtime
            runner = self._get_engine_runner()
            client = self._alist_client or getattr(runner, "alist", None)
            if client is None:
                raise ApplicationError("未配置 AList 客户端，无法自动补源")
            from engine.tools.replenishment_adapter.search import ReplenishmentSearchService
            runtime = AutomaticReplenishmentRuntime(
                self.state_root,
                engine_runner=runner,
                alist=client,
                search=ReplenishmentSearchService(),
                materializer=LocalTorrentAutomaticMaterializer(
                    archive_preprocessor=self._archive_preprocessor,
                ),
                staging_root=f"{self.remote_root.rstrip('/')}/ScrapeFlow/补源",
                progress=self._record_replenishment_progress,
                cancel_requested=self._provider_runtime_cancel_requested,
            )
            self._automatic_replenishment = runtime
            return runtime

    def _queue_provider_job(self, job_id: str, *, delay: float = 0.0) -> None:
        if (
            self._closed.is_set()
            or self.control().get("paused") is True
            or not self._provider_auto_repair_enabled()
        ):
            return

        # A fresh full-library audit may rediscover the same gap while its
        # provider future is acquiring or verifying a candidate. Do not
        # republish ``gap_discovering`` over that live projection: the future
        # already owns this root and will persist its next real stage.
        with self._automatic_lock:
            existing = self._provider_futures.get(job_id)
            has_live_future = existing is not None and not existing.done()
            if has_live_future and delay <= 0:
                return

        # Parse the optional pilot selector before scheduling any timer or
        # changing a root's projection.  A malformed selector must fail
        # closed rather than making an intended one-work pilot broad again.
        pilot_tmdb = self._provider_pilot_tmdb()

        # Do not even arm a timer for an exhausted gap.  This check is
        # intentionally durable (rather than relying on the in-memory future
        # map), so a restart or a stale audit callback cannot resurrect the
        # same fingerprint.  ``retry_public_job`` clears ``terminal`` before
        # explicitly queueing a new attempt.
        try:
            current = self._get_engine_runner().get_job(job_id)
        except Exception:
            return
        if self._is_internal_child(current) or not self._provider_job_allowed(current):
            return

        # Publish the non-green provider stage before submitting the worker.
        # ThreadPoolExecutor submission is asynchronous; without this small
        # durable projection a just-audited root could be rendered
        # ``completed`` for one or more HTTP polls while the provider worker
        # was still waiting to start.
        if not has_live_future:
            try:
                # Serialize the projection with startup orphan reconciliation
                # and re-check the future after the initial fast-path check.
                # A provider can become live between those two points.
                with self._automatic_lock:
                    if (
                        self._closed.is_set()
                        or self.control().get("paused") is True
                        or not self._provider_auto_repair_enabled()
                    ):
                        return
                    existing = self._provider_futures.get(job_id)
                    if existing is not None and not existing.done():
                        if delay <= 0:
                            return
                        has_live_future = True
                    if not has_live_future:
                        runner = self._get_engine_runner()
                        current = runner.get_job(job_id)
                        if self._is_internal_child(current):
                            return
                        if pilot_tmdb is not None and self._provider_job_tmdb(current) != pilot_tmdb:
                            # Leave the audited root as ``gap_discovering``. It remains
                            # visibly incomplete and will enter a later rollout without
                            # requiring an artificial retry or mutating its gap state.
                            return
                        if current.phase == "executed" and self._has_provider_gaps(current):
                            summary = dict(current.summary)
                            prior = summary.get("replenishment")
                            replenishment = dict(prior) if isinstance(prior, Mapping) else {}
                            replenishment.update({
                                "status": "gap_discovering",
                                "terminal": False,
                                "next_retry_seconds": delay if delay > 0 else None,
                                "updated_at": _now(),
                            })
                            summary["replenishment"] = replenishment
                            summary["automatic_stage"] = "gap_discovering"
                            atomic_write_json(
                                runner.jobs_root / f"{job_id}.json",
                                _redacted_job_payload(
                                    replace(current, summary=summary, updated_at=_now()),
                                ),
                                allow_nan=False,
                            )
            except Exception:
                # Queueing is best-effort here; the worker will still persist a
                # failure if the job record cannot be read or written.
                pass

        def submit() -> None:
            if (
                self._closed.is_set()
                or self.control().get("paused") is True
                or not self._provider_auto_repair_enabled()
            ):
                return
            with self._automatic_lock:
                # The pilot environment can change while a delayed retry is
                # waiting. Re-read the persisted root immediately before
                # submitting so a stale timer cannot widen the rollout.
                try:
                    current = self._get_engine_runner().get_job(job_id)
                except Exception:
                    return
                if self._is_internal_child(current) or not self._provider_job_allowed(current):
                    return
                existing = self._provider_futures.get(job_id)
                if existing is not None and not existing.done():
                    return
                self._provider_futures[job_id] = self._provider_pool().submit(
                    self._run_automatic_replenishment, job_id,
                )

        self._schedule_timer("provider", job_id, delay, submit)

    def _record_replenishment_summary(self, job: EngineJob, outcome: Mapping[str, object]) -> None:
        runner = self._get_engine_runner()
        current = runner.get_job(job.id)
        summary = dict(current.summary)
        prior = current.summary.get("replenishment")
        merged = dict(prior) if isinstance(prior, Mapping) else {}
        safe_outcome = redact_value(dict(outcome))
        merged.update(dict(safe_outcome) if isinstance(safe_outcome, Mapping) else dict(outcome))
        summary.pop("provider_gap_fingerprint", None)
        merged.pop("gap_fingerprint", None)
        signature = current.summary.get("provider_gap_signature")
        if isinstance(signature, list) and signature:
            merged["gap_signature"] = signature
        # A successful retry can follow a cooperative cancellation in the
        # same persisted root.  Remove transient failure/cancellation fields
        # that are absent from the new outcome; otherwise the dashboard and
        # next audit see a contradictory ``completed + cancelled`` projection.
        if str(outcome.get("status") or "").casefold() in {"completed", "ready"}:
            for key in (
                "error", "cancelled", "cancellation_boundary",
                "attempt_failure_stage", "next_retry_seconds",
            ):
                merged.pop(key, None)
        summary["replenishment"] = merged
        if isinstance(outcome.get("attempts"), int):
            summary["replenishment_attempts"] = outcome["attempts"]
        updated = replace(current, summary=summary, updated_at=_now())
        atomic_write_json(
            runner.jobs_root / f"{current.id}.json",
            _redacted_job_payload(updated),
            allow_nan=False,
        )

    def _record_replenishment_progress(
        self,
        job: EngineJob,
        phase: str,
        details: Mapping[str, object],
    ) -> None:
        """Persist provider phase for the operations dashboard.

        This is a short projection update only.  It does not change the
        Engine's durable ``executed`` fact and therefore cannot make a
        partially written child look complete.
        """
        try:
            runner = self._get_engine_runner()
            current = runner.get_job(job.id)
            summary = dict(current.summary)
            prior = current.summary.get("replenishment")
            replenishment = dict(prior) if isinstance(prior, Mapping) else {}
            now = _now()
            redacted_details = redact_value(dict(details))
            safe_details = (
                dict(redacted_details)
                if isinstance(redacted_details, Mapping)
                else dict(details)
            )
            replenishment.update({
                "status": phase,
                "updated_at": now,
                **safe_details,
            })

            # A provider attempt owns a real, restartable Engine child, but
            # that child is an implementation detail rather than a second
            # public task.  Keep its latest phase on the root projection so
            # the operations page can explain what is happening without
            # enumerating (or accidentally scheduling) the child record.
            child_id = safe_details.get("child_job_id")
            if isinstance(child_id, str) and child_id:
                raw_children = replenishment.get("child_jobs")
                children: list[dict[str, object]] = []
                if isinstance(raw_children, list):
                    children = [
                        dict(row)
                        for row in raw_children
                        if isinstance(row, Mapping)
                        and isinstance(row.get("id"), str)
                        and row.get("id")
                    ]
                child_phase = safe_details.get("child_phase")
                child_row: dict[str, object] = {
                    "id": child_id,
                    "phase": child_phase if isinstance(child_phase, str) and child_phase else phase,
                    "updated_at": now,
                }
                round_number = safe_details.get("round")
                if isinstance(round_number, int):
                    child_row["round"] = round_number
                error = safe_details.get("error")
                if isinstance(error, str) and error:
                    child_row["error"] = error
                replaced = False
                for index, existing in enumerate(children):
                    if existing.get("id") == child_id:
                        children[index] = {**existing, **child_row}
                        replaced = True
                        break
                if not replaced:
                    children.append(child_row)
                replenishment["child_jobs"] = children
            summary["replenishment"] = replenishment
            summary["automatic_stage"] = phase
            updated = replace(current, summary=summary, updated_at=now)
            atomic_write_json(
                runner.jobs_root / f"{current.id}.json",
                _redacted_job_payload(updated),
                allow_nan=False,
            )
        except Exception:
            return

    def _run_automatic_replenishment(self, job_id: str) -> None:
        # A resume can submit more futures than the provider worker count. A
        # queued future may therefore start after a pause or after the pilot
        # selector changes; re-check both immediately before doing any provider
        # work so the global pause remains a real dispatch boundary.
        self._provider_pilot_tmdb()
        if (
            self._closed.is_set()
            or self.control().get("paused") is True
            or not self._provider_auto_repair_enabled()
        ):
            return
        try:
            runner = self._get_engine_runner()
            job = runner.get_job(job_id)
            if self._is_internal_child(job):
                return
            # Re-check the durable terminal boundary after the future starts;
            # a provider timer may have been queued just before another worker
            # exhausted the same gap.
            if not self._provider_job_allowed(job):
                return
            if job.phase != "executed":
                return
            if not self._provider_job_allowed(job):
                return
            provider_job = self._provider_pilot_job(job)
            if self._closed.is_set() or self.control().get("paused") is True:
                return
            runtime = self._get_automatic_replenishment()
            # Runtime construction can perform lazy dependency setup. Check
            # once more before publishing a searching phase or invoking the
            # provider, so a pause during that setup cannot start a queued root.
            if self._closed.is_set() or self.control().get("paused") is True:
                return
            if not self._provider_job_allowed(job):
                return
            self._record_replenishment_progress(job, "provider_searching", {})
            if self._closed.is_set() or self.control().get("paused") is True:
                return
            if not self._provider_job_allowed(job):
                return
            outcome = runtime.run_for_job(provider_job)
            summary_before = dict(job.summary)
            outcome = dict(outcome)
            cancelled = outcome.get("cancelled") is True or any(
                isinstance(row, Mapping) and row.get("cancelled") is True
                for row in outcome.get("outcomes", [])
            )
            # An operator pause/pilot narrowing is not a failed provider
            # attempt.  It must remain retryable and must not consume the
            # bounded candidate budget merely because a live worker observed
            # the control change at its next cooperative boundary.
            provider_attempts = int(summary_before.get("replenishment_attempts") or 0)
            if not cancelled:
                provider_attempts += 1
            outcome["attempts"] = provider_attempts
            provider_limit_raw = os.getenv("SCRAPEFLOW_PROVIDER_RETRY_LIMIT", "5").strip()
            try:
                provider_limit = max(0, min(30, int(provider_limit_raw)))
            except ValueError:
                provider_limit = 5
            has_error = (
                any(isinstance(row, Mapping) and row.get("error") for row in outcome.get("outcomes", []))
                or bool(outcome.get("unresolved_gaps"))
            )
            if cancelled:
                outcome["terminal"] = False
                outcome["status"] = "retry_wait"
                outcome["next_retry_seconds"] = None
            elif not has_error:
                outcome["terminal"] = True
                outcome["status"] = "completed"
            elif provider_attempts >= provider_limit:
                outcome["terminal"] = True
                outcome["status"] = "failed"
            else:
                outcome["status"] = "retry_wait"
                outcome["next_retry_seconds"] = 30
            self._record_replenishment_summary(job, outcome)
            if outcome.get("terminal") is True:
                self._cancel_job_timers(job_id)
            if cancelled:
                # The runtime has already persisted gap-level retry_wait. Do
                # not trigger a fresh audit or delayed provider timer while a
                # global pause/allowlist boundary is in effect.
                return
            # A provider child may have committed a video while another
            # read-only audit was still traversing the previous inventory.
            # Coalesce one follow-up so that fresh media gets its configured
            # subtitle probe and, if absent, the pure sidecar lane.
            target = self._job_audit_target(job)
            if isinstance(target, str):
                self._queue_scoped_library_audit(
                    [target], delay=0.5, rerun_if_busy=True,
                )
            # Missing root coordinates fail closed; never widen a provider
            # completion callback into a formal-library scan.
            if has_error and outcome.get("terminal") is not True:
                # Provider failures are isolated.  Requeue the gap work after a
                # bounded delay while ordinary jobs continue flowing.
                self._queue_provider_job(job_id, delay=30.0)
        except Exception as exc:
            try:
                runner = self._get_engine_runner()
                job = runner.get_job(job_id)
                provider_attempts = int(job.summary.get("replenishment_attempts") or 0) + 1
                provider_limit_raw = os.getenv("SCRAPEFLOW_PROVIDER_RETRY_LIMIT", "5").strip()
                try:
                    provider_limit = max(0, min(30, int(provider_limit_raw)))
                except ValueError:
                    provider_limit = 5
                terminal = provider_attempts >= provider_limit
                self._record_replenishment_summary(job, {
                    "status": "failed" if terminal else "retry_wait",
                    "terminal": terminal,
                    "attempts": provider_attempts,
                    "error": redact_error(exc),
                    "next_retry_seconds": None if terminal else 30,
                })
                if terminal:
                    self._cancel_job_timers(job_id)
                if not terminal:
                    self._queue_provider_job(job_id, delay=30.0)
            except Exception:
                return

    def _audit_pool(self) -> ThreadPoolExecutor:
        with self._audit_lock:
            if self._audit_executor is None:
                self._audit_executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="scrapeflow-library-audit",
                )
            return self._audit_executor

    @staticmethod
    def _audit_result_needs_subtitle_batch_continuation(result: object) -> bool:
        """Return whether a completed audit can safely advance its ledger.

        ``subtitle_probe_batch_deferred`` is deliberately distinct from a
        probe error, an ambiguous track, and every identity/TMDB unknown.  It
        means the persisted fair cursor still has a bounded next slice to
        inspect.  Checking the completed semantic report here, rather than a
        durable job projection, prevents one job's old unknown state from
        repeatedly rearming the scanner.
        """
        if not isinstance(result, Mapping):
            return False
        report = result.get("audit")
        if not isinstance(report, Mapping):
            return False
        semantic = report.get("semantic")
        if not isinstance(semantic, Mapping):
            return False
        unknowns = semantic.get("unknowns")
        if not isinstance(unknowns, (list, tuple)):
            return False
        return any(
            isinstance(row, Mapping)
            and row.get("kind") == _AUDIT_SUBTITLE_BATCH_DEFERRED_KIND
            and row.get("reason") == _AUDIT_SUBTITLE_BATCH_DEFERRED_REASON
            for row in unknowns
        )

    def _start_library_audit_locked(self) -> Future[object]:
        """Start one audit worker while holding ``_audit_lock``.

        Every caller, including the synchronous public endpoint, goes through
        this one worker lane.  Apart from preventing two inventory scans from
        racing, that also prevents concurrent subtitle-evidence ledger writes.
        """
        scope = tuple(sorted(self._pending_audit_roots)) or None
        self._pending_audit_roots.clear()
        if scope is None:
            future = self._audit_pool().submit(self._run_library_audit_background)
        else:
            future = self._audit_pool().submit(
                self._run_library_audit_background,
                scope_roots=scope,
            )
        self._audit_future = future

        def clear(done: Future[object]) -> None:
            try:
                completed_result = done.result()
            except Exception:
                completed_result = None
            # Do not inspect a persisted job's older audit projection here:
            # only the semantic findings produced by *this* completed audit
            # can advance the bounded subtitle-evidence ledger.
            subtitle_batch_continuation = (
                self._audit_result_needs_subtitle_batch_continuation(completed_result)
            )
            rerun = False
            with self._audit_lock:
                if self._audit_future is done:
                    self._audit_future = None
                    # Provider commits and a bounded subtitle batch share one
                    # coalesced successor.  The executor has one worker, and
                    # this callback only queues after ``done`` has settled,
                    # so neither cause can create concurrent scans.
                    rerun = self._audit_rerun_requested or subtitle_batch_continuation
                    self._audit_rerun_requested = False
            if rerun and not self._closed.is_set() and self.control().get("paused") is not True:
                # Keep the follow-up off the executor callback stack; the
                # short delay also lets AList listings settle.
                self._queue_library_audit(delay=0.5, rerun_if_busy=True)

        future.add_done_callback(clear)
        return future

    def _queue_library_audit(
        self, *, delay: float = 0.0, rerun_if_busy: bool = False,
    ) -> None:
        """Schedule a read-only full-library scan after ordinary mutations.

        ``rerun_if_busy`` is used at a provider-child commit boundary.  If an
        older scan is in flight, replacing it would race its projection; a
        single coalesced follow-up is safer and guarantees that newly visible
        media gets a subtitle probe on the next pass.
        """
        if (
            self._closed.is_set()
            or self.control().get("paused") is True
            or not self._audit_auto_repair_enabled()
        ):
            return

        def submit() -> None:
            if (
                self._closed.is_set()
                or self.control().get("paused") is True
                or not self._audit_auto_repair_enabled()
            ):
                return
            with self._audit_lock:
                if self._audit_future is not None and not self._audit_future.done():
                    if rerun_if_busy:
                        self._audit_rerun_requested = True
                    return
                self._start_library_audit_locked()

        self._schedule_timer("audit", "library", delay, submit)

    def _queue_scoped_library_audit(
        self,
        target_roots: Sequence[str],
        *,
        delay: float = 0.0,
        rerun_if_busy: bool = False,
    ) -> None:
        """Coalesce a bounded audit to the affected work roots."""
        normalized: set[str] = set()
        for raw in target_roots:
            if not isinstance(raw, str) or not raw.startswith("/"):
                continue
            value = posixpath.normpath(raw)
            if value != "/" and all(part not in {"", ".", ".."} for part in value.split("/")[1:]):
                normalized.add(value)
        if not normalized:
            # Scope is mandatory for automatic audit/repair.  An empty or
            # malformed scope is fail-closed rather than a global scan.
            return
        with self._audit_lock:
            self._pending_audit_roots.update(normalized)
        self._queue_library_audit(delay=delay, rerun_if_busy=rerun_if_busy)

    def _run_library_audit_background(
        self, *, scope_roots: Sequence[str] | None = None,
    ) -> dict[str, object] | None:
        try:
            # Keep the no-scope call shape compatible with small injected
            # audit fakes and older test/application adapters.  The scoped
            # keyword is only part of the new bounded-audit contract.
            if scope_roots is None:
                return self._run_library_audit_once()
            return self._run_library_audit_once(scope_roots=scope_roots)
        except Exception:
            # An unavailable TMDB/AList scan is represented by the next
            # explicit/automatic attempt; it must not terminate the write or
            # provider pools.
            return

    @staticmethod
    def _audit_required_subtitle_language() -> str | None:
        value = os.getenv("SCRAPEFLOW_REQUIRED_SUBTITLE_LANGUAGE", "").strip()
        return value or None

    @staticmethod
    def _audit_subtitle_row_key(row: Mapping[str, object]) -> tuple[str, ...]:
        media = row.get("media") if isinstance(row.get("media"), Mapping) else {}
        return (
            str(media.get("tmdb_id") or ""),
            str(media.get("target_root") or row.get("target_root") or ""),
            str(row.get("path") or ""),
            str(row.get("subtitle_language") or "").casefold(),
            str(row.get("id") or ""),
        )

    @staticmethod
    def _audit_subtitle_row_basic_safe(row: Mapping[str, object]) -> bool:
        """Validate one semantic subtitle row before it can create an owner."""
        if str(row.get("kind") or "").casefold() != "missing_subtitle":
            return False
        if row.get("source") != "automatic_library_audit":
            return False
        media = row.get("media") if isinstance(row.get("media"), Mapping) else {}
        raw_id = media.get("tmdb_id")
        if isinstance(raw_id, str) and raw_id.isdecimal():
            raw_id = int(raw_id)
        if isinstance(raw_id, bool) or not isinstance(raw_id, int) or raw_id <= 0:
            return False
        target = media.get("target_root")
        if not isinstance(target, str) or not target.startswith("/"):
            return False
        target = posixpath.normpath(target)
        if target == "/" or any(part in {"", ".", ".."} for part in target.split("/")[1:]):
            return False
        media_type = str(media.get("media_type") or media.get("type") or "").casefold()
        if media_type not in {"movie", "tv", "mixed"}:
            return False
        path = row.get("path")
        if (
            not isinstance(path, str)
            or not path.startswith(target.rstrip("/") + "/")
            or posixpath.normpath(path) != path
            or not is_video_filename(path)
            or is_production_test_media_path(path)
        ):
            return False
        language = row.get("subtitle_language")
        if (
            not isinstance(language, str)
            or not language.strip()
            or len(language.strip()) > 64
            or any(char in language for char in ("/", "\\", "\x00", "\n", "\r"))
        ):
            return False
        gap_id = row.get("id")
        return (
            isinstance(gap_id, str)
            and bool(gap_id.strip())
            and all(char not in gap_id for char in ("/", "\\", "\x00", "\n", "\r"))
        )

    @staticmethod
    def _audit_work_matches_subtitle_row(
        work: Mapping[str, object], row: Mapping[str, object],
    ) -> bool:
        """Require the exact row to be present in the trusted work scope."""
        if not isinstance(work, Mapping) or not SimpleApplication._audit_subtitle_row_basic_safe(row):
            return False
        media = row.get("media") if isinstance(row.get("media"), Mapping) else {}
        work_key = str(work.get("work") or "")
        work_id = work_key.rsplit(":", 1)[-1] if work_key.startswith("tmdb:") else ""
        raw_id = media.get("tmdb_id")
        target = media.get("target_root")
        if not work_id or str(raw_id) != work_id or target != work.get("target_root"):
            return False
        work_gaps = work.get("gaps")
        if not isinstance(work_gaps, list):
            return False
        key = SimpleApplication._audit_subtitle_row_key(row)
        return any(
            isinstance(candidate, Mapping)
            and SimpleApplication._audit_subtitle_row_key(candidate) == key
            for candidate in work_gaps
        )

    @staticmethod
    def _audit_job_coordinates(job: EngineJob) -> tuple[object, object, str]:
        summary = job.summary if isinstance(job.summary, Mapping) else {}
        identity = summary.get("identity") if isinstance(summary.get("identity"), Mapping) else {}
        metadata = job.plan.get("metadata") if isinstance(job.plan.get("metadata"), Mapping) else {}
        tmdb = identity.get("tmdb_id") or metadata.get("tmdb_id") or summary.get("tmdb_id")
        target = (
            identity.get("target_root") or metadata.get("series_root")
            or metadata.get("target_root") or job.plan.get("target_root")
            or summary.get("target_root")
        )
        media_type = str(
            identity.get("media_type") or metadata.get("media_type")
            or job.plan.get("mode") or summary.get("mode") or ""
        ).casefold()
        return tmdb, target, media_type

    @classmethod
    def _audit_existing_subtitle_owner(
        cls, row: Mapping[str, object], jobs: Mapping[str, EngineJob],
    ) -> bool:
        """Return whether an existing executed root can safely own this row."""
        for job in jobs.values():
            if cls._is_internal_child(job) or job.phase != "executed":
                continue
            tmdb, target, _media_type = cls._audit_job_coordinates(job)
            media = row.get("media") if isinstance(row.get("media"), Mapping) else {}
            if str(tmdb) != str(media.get("tmdb_id")) or target != media.get("target_root"):
                continue
            if not cls._audit_root_gap_is_safe(row, job):
                continue
            # Do not attach a subtitle-only observation to a historical root
            # that still carries media gaps; that would re-open a media child
            # under a sidecar request. Audit-owned mixed roots are already
            # task-scoped and are intentionally allowed to handle both lanes.
            if not cls._is_audit_owned_root(job):
                scan = job.plan.get("scan_report") if isinstance(job.plan.get("scan_report"), Mapping) else {}
                old_rows = scan.get("resource_gaps") if isinstance(scan, Mapping) else []
                if any(
                    isinstance(item, Mapping)
                    and cls._audit_row_kind(item) in _AUDIT_ROOT_PROVIDER_GAP_KINDS
                    and item.get("source") != "automatic_library_audit"
                    for item in (old_rows if isinstance(old_rows, list) else [])
                ):
                    continue
            return True
        return False

    def _trusted_subtitle_projects(
        self,
        semantic: Mapping[str, object],
        gaps: Sequence[Mapping[str, object]],
        jobs: Mapping[str, EngineJob],
    ) -> tuple[list[dict[str, object]], set[tuple[str, ...]]]:
        """Build ownerless subtitle projects only from NFO-scoped work facts."""
        raw_works = semantic.get("works")
        works = [row for row in raw_works if isinstance(row, Mapping)] if isinstance(raw_works, list) else []
        grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
        safe_rows: set[tuple[str, ...]] = set()
        for raw_gap in gaps:
            row = dict(raw_gap)
            if not self._audit_subtitle_row_basic_safe(row):
                continue
            matching = [work for work in works if self._audit_work_matches_subtitle_row(work, row)]
            if len(matching) != 1:
                continue
            work = matching[0]
            sources = work.get("identity_sources")
            if not isinstance(sources, (list, tuple, set, frozenset)):
                source = work.get("identity_source")
                sources = [source] if isinstance(source, str) else []
            if not any(str(source).casefold() == "library_nfo" for source in sources):
                # Engine-owned roots are handled by the normal owner matching
                # path; never bootstrap a second root from an untrusted label.
                continue
            safe_rows.add(self._audit_subtitle_row_key(row))
            if self._audit_existing_subtitle_owner(row, jobs):
                continue
            media = row.get("media") if isinstance(row.get("media"), Mapping) else {}
            mode = str(media.get("media_type") or "").casefold()
            if mode == "mixed":
                mode = "tv"
            language = str(row.get("subtitle_language") or "").strip().casefold()
            if mode not in {"movie", "tv"} or not language:
                continue
            key = (
                str(media.get("tmdb_id")),
                str(media.get("target_root")),
                mode,
                language,
            )
            grouped.setdefault(key, []).append(row)
        projects: list[dict[str, object]] = []
        for (tmdb_text, target, mode, language), rows in sorted(grouped.items()):
            try:
                tmdb_id = int(tmdb_text)
            except ValueError:
                continue
            media = rows[0].get("media") if isinstance(rows[0].get("media"), Mapping) else {}
            metadata = {
                "tmdb_id": tmdb_id,
                "title": media.get("title"),
                "original_title": media.get("original_title"),
                "year": media.get("year"),
                "media_type": mode,
                "target_root": target,
                "series_root": target,
                "subtitle_only": True,
                "subtitle_language": language,
            }
            projects.append({
                "project_key": f"tmdb:{mode}:{tmdb_id}:{target}:subtitle:{language}",
                "subtitle_only": True,
                "tmdb_id": tmdb_id,
                "target_root": target,
                "gaps": rows,
                "plan": {
                    "mode": mode,
                    "target_root": target,
                    "metadata": metadata,
                    "scan_report": {"resource_gaps": rows},
                },
            })
        return projects, safe_rows

    def _apply_audit_gaps(
        self,
        report: Mapping[str, object],
        runner: SimpleEngineRunner | None,
        *,
        scope_roots: Sequence[str] | None = None,
    ) -> None:
        """Attach fresh semantic gaps to their owning automatic jobs.

        The audit is read-only with respect to AList.  Updating a JSON job
        summary only tells the provider worker what the latest observation is;
        the worker still performs its own fresh staging/readback checks before
        any remote write.
        """
        if runner is None:
            return
        semantic = report.get("semantic") if isinstance(report.get("semantic"), Mapping) else {}
        raw_gaps = semantic.get("gaps") if isinstance(semantic, Mapping) else []
        raw_unknowns = semantic.get("unknowns") if isinstance(semantic, Mapping) else []
        gaps = [dict(row) for row in raw_gaps if isinstance(row, Mapping)] if isinstance(raw_gaps, list) else []
        unknowns = [dict(row) for row in raw_unknowns if isinstance(row, Mapping)] if isinstance(raw_unknowns, list) else []
        try:
            jobs_by_id = {
                job.id: job
                for job in runner.list_jobs()
                if not self._is_internal_child(job)
                and self._audit_scope_matches(
                    self._job_audit_target(job), scope_roots,
                )
            }
        except Exception:
            return

        # A semantic project can come from an identity bootstrap for an
        # existing library that predates ScrapeFlow's persisted jobs.  Build a
        # local, audit-owned root only after the runner has validated its TMDB
        # identity/target/gaps.  This does not plan or execute media; it gives
        # the existing provider runtime a durable parent for its hidden child.
        projects = semantic.get("acquisition_projects") if isinstance(semantic, Mapping) else []
        creator = getattr(runner, "create_audit_owned_root", None)
        if isinstance(projects, list) and callable(creator):
            for project in projects:
                if not isinstance(project, Mapping):
                    continue
                try:
                    owner = creator(project)
                except (EngineRequestError, SimpleEngineError):
                    # Malformed, ambiguous, unsupported, or out-of-library
                    # observations stay in the read-only audit report. They
                    # must never become a provider task by best effort.
                    continue
                if not isinstance(owner, EngineJob):
                    continue
                jobs_by_id[owner.id] = owner

        # NFO bootstrap is intentionally report-only in the scanner.  A
        # subtitle gap is the one safe exception: when the report carries an
        # exact scoped video and a single library-NFO/TMDB identity, create a
        # local subtitle-only owner so the existing pure sidecar provider lane
        # can handle it.  Unknown identities, forged paths, and engine-only
        # labels stay visible but never become provider work.
        subtitle_projects, trusted_subtitle_rows = self._trusted_subtitle_projects(
            semantic, gaps, jobs_by_id,
        )
        subtitle_creator = getattr(runner, "create_audit_owned_subtitle_root", None)
        if callable(subtitle_creator):
            for project in subtitle_projects:
                try:
                    owner = subtitle_creator(project)
                except (EngineRequestError, SimpleEngineError):
                    continue
                if isinstance(owner, EngineJob):
                    jobs_by_id[owner.id] = owner

        jobs = [
            job for job in jobs_by_id.values()
            if job.phase in {"executed", "completed"}
        ]

        def identity_for(job: EngineJob) -> tuple[object, object]:
            metadata = job.plan.get("metadata") if isinstance(job.plan.get("metadata"), Mapping) else {}
            identity = job.summary.get("identity") if isinstance(job.summary.get("identity"), Mapping) else {}
            return (
                identity.get("tmdb_id") or metadata.get("tmdb_id") or job.summary.get("tmdb_id"),
                metadata.get("series_root")
                or metadata.get("target_root")
                or job.plan.get("target_root")
                or identity.get("target_parent"),
            )

        audit_retry_needed = False
        # Several historical/root records can describe the same TMDB work.
        # They may all receive the fresh audit projection, but one semantic
        # gap must produce only one provider queue entry.  Keep the first
        # deterministic owner (runner.list_jobs is filename-sorted).
        provider_gap_owners: dict[tuple[str, ...], str] = {}
        for job in jobs:
            job_tmdb, job_target = identity_for(job)
            relevant = [
                dict(gap) for gap in gaps
                if self._audit_row_matches_job(gap, job_tmdb=job_tmdb, job_target=job_target)
            ]
            relevant_unknowns = [
                dict(unknown) for unknown in unknowns
                if self._audit_row_matches_job(unknown, job_tmdb=job_tmdb, job_target=job_target)
            ]
            if self._is_audit_owned_root(job):
                # This root exists solely for safe provider gaps.  Always use
                # the *fresh* semantic rows matched above, including a
                # subtitle gap discovered after an earlier media child
                # resolved.  Filtering only by the validated identity,
                # target, exact video path and configured language prevents a
                # stale/raw row from becoming a whole-tree provider write.
                relevant = [
                    dict(row) for row in relevant
                    if self._audit_root_gap_is_safe(row, job)
                ]
                if job.summary.get("audit_subtitle_only") is True:
                    expected_language = str(
                        job.summary.get("audit_subtitle_language") or ""
                    ).casefold()
                    # A subtitle-only owner never absorbs a media gap, a
                    # sibling language, or a path introduced by another
                    # work.  This keeps its provider request permanently in
                    # the pure sidecar lane.
                    relevant = [
                        row for row in relevant
                        if self._audit_row_kind(row) == "missing_subtitle"
                        and (
                            not expected_language
                            or str(row.get("subtitle_language") or "").casefold()
                            == expected_language
                        )
                    ]
                relevant_unknowns = []
            else:
                # A historical Engine root may safely receive a subtitle row
                # only when that row was proven by the same NFO-scoped work.
                # Do not let an identity/target coincidence attach an
                # arbitrary sidecar write to an old task.
                relevant = [
                    row for row in relevant
                    if self._audit_row_kind(row) != "missing_subtitle"
                    or self._audit_subtitle_row_key(row) in trusted_subtitle_rows
                ]

            plan = dict(job.plan)
            scan = dict(plan.get("scan_report") or {}) if isinstance(plan.get("scan_report"), Mapping) else {}
            old_resource_gaps = [
                dict(row) for row in scan.get("resource_gaps", []) if isinstance(row, Mapping)
            ]
            # A full scan is a fresh observation. Remove only audit-owned
            # rows, retaining a gap discovered by the Engine itself.
            existing = [
                dict(row)
                for row in old_resource_gaps
                if row.get("source") != "automatic_library_audit"
            ]
            merged: dict[str, dict[str, object]] = {
                str(row.get("id") or f"engine-{index}"): row
                for index, row in enumerate(existing)
            }
            for index, gap in enumerate(relevant):
                gap.setdefault("source", "automatic_library_audit")
                merged[str(gap.get("id") or f"audit-{index}")] = gap
            new_resource_gaps = list(merged.values())
            scan["resource_gaps"] = new_resource_gaps
            plan["scan_report"] = scan

            summary = dict(job.summary)
            previous_audit = summary.get("audit")
            previous_audit = dict(previous_audit) if isinstance(previous_audit, Mapping) else {}
            provider_relevant = [
                row for row in new_resource_gaps
                if self._audit_row_kind(row) in _AUTOMATIC_PROVIDER_GAP_KINDS
            ]
            provider_gap_signature = (
                self._provider_gap_signature(provider_relevant)
                if provider_relevant else None
            )
            summary.pop("provider_gap_fingerprint", None)
            if provider_gap_signature is not None:
                summary["provider_gap_signature"] = provider_gap_signature
            else:
                summary.pop("provider_gap_signature", None)
            repair_relevant = [
                row for row in new_resource_gaps
                if self._audit_row_kind(row) in _AUTOMATIC_REPAIR_GAP_KINDS
            ]
            unsupported_relevant = [
                row for row in new_resource_gaps
                if self._audit_row_kind(row) in _AUTOMATIC_UNSUPPORTED_GAP_KINDS
            ]

            # A terminal provider attempt belongs to the previous audit
            # round. A newly observed provider gap must reopen it.
            prior_replenishment = summary.get("replenishment")
            same_terminal_gap = bool(
                provider_relevant
                and isinstance(prior_replenishment, Mapping)
                and prior_replenishment.get("terminal") is True
                and prior_replenishment.get("gap_signature") == provider_gap_signature
            )
            if (
                provider_relevant
                and isinstance(prior_replenishment, Mapping)
                and prior_replenishment.get("terminal") is True
                and not same_terminal_gap
            ):
                child_history = prior_replenishment.get("child_jobs")
                summary.pop("replenishment", None)
                if isinstance(child_history, list):
                    # Keep the child lineage visible when a later audit finds
                    # a new gap; only the retry counters/status are reopened.
                    summary["replenishment"] = {"child_jobs": child_history}
                summary.pop("replenishment_attempts", None)

            # A fresh audit with no remaining provider gap closes an old
            # provider projection only when this process has no live future
            # for the root.  That includes a retry_wait/child_failed status
            # left by an earlier child attempt: once the media is visible,
            # retaining it would keep the root permanently non-green.  A
            # live future still owns its final readback/write boundary, so it
            # must retain the projection until it settles.
            provider_projection_cleared = False
            prior_provider_status = (
                str(prior_replenishment.get("status") or "").casefold()
                if isinstance(prior_replenishment, Mapping) else ""
            )
            with self._automatic_lock:
                future = self._provider_futures.get(job.id)
                provider_future_live = future is not None and not future.done()
            if (
                not provider_relevant
                and not provider_future_live
                and isinstance(prior_replenishment, Mapping)
                and (
                    (
                        prior_replenishment.get("terminal") is True
                        and prior_provider_status in {"failed", "failed_provider"}
                    )
                    or (
                        prior_replenishment.get("terminal") is not True
                        and prior_provider_status in _PROVIDER_PROJECTION_PHASES
                    )
                )
            ):
                child_history = prior_replenishment.get("child_jobs")
                summary.pop("replenishment", None)
                if isinstance(child_history, list):
                    summary["replenishment"] = {"child_jobs": child_history}
                summary.pop("replenishment_attempts", None)
                provider_projection_cleared = True

            audit_state: dict[str, object] | None = None
            if relevant_unknowns:
                deferred_unknown_only = self._audit_unknowns_defer_automatic_retry(
                    relevant_unknowns,
                )
                has_tmdb_unknown = any(
                    self._audit_row_kind(row) in {
                        "unknown_episode_catalog", "unknown_library_work",
                    }
                    for row in relevant_unknowns
                )
                has_subtitle_unknown = any(
                    self._audit_row_kind(row) == "unknown_subtitle_evidence"
                    for row in relevant_unknowns
                )
                audit_state = {
                    "status": "unknown",
                    "message": (
                        "TMDB 媒体库证据不足，保持未收口，等待新媒体提交或手动审计后再核对"
                        if deferred_unknown_only and has_tmdb_unknown
                        else (
                            "字幕证据不足，等待新媒体提交或手动审计后再核对"
                            if deferred_unknown_only and has_subtitle_unknown
                            else (
                                "正式库作品身份/范围证据不足，保持未收口，等待新媒体提交或手动审计后再核对"
                                if deferred_unknown_only
                                else "媒体库审计证据不足，系统会自动重新核对"
                            )
                        )
                    ),
                    "gaps": relevant,
                    "unknowns": relevant_unknowns,
                    "gap_count": len(relevant),
                    "unknown_count": len(relevant_unknowns),
                    "retryable": True,
                    # Bounded TMDB/subtitle probes are expected to leave
                    # unknown evidence. Keep the task visibly unknown (never
                    # green), but do not schedule an identical scan every 30
                    # seconds. New media commits and explicit audit requests
                    # remain the retry boundary.
                    "automatic_retry": not deferred_unknown_only,
                }
                if not deferred_unknown_only:
                    audit_retry_needed = True
            elif unsupported_relevant:
                audit_state = {
                    "status": "failed_provider",
                    "message": "存在当前补源器无法处理的缺口",
                    "gaps": unsupported_relevant,
                    "unknowns": [],
                    "gap_count": len(unsupported_relevant),
                    "unknown_count": 0,
                    "retryable": True,
                }
                audit_retry_needed = True
            elif repair_relevant:
                audit_state = {
                    "status": "blocked",
                    "message": "NFO/海报缺口已记录，等待独立 repair gate，不由审计扫描自动写入",
                    "gaps": repair_relevant,
                    "unknowns": [],
                    "gap_count": len(repair_relevant),
                    "unknown_count": 0,
                    "retryable": False,
                    "automatic_retry": False,
                    "repair_attempts": int(previous_audit.get("repair_attempts") or 0),
                }

            if audit_state is None:
                # Clearing this field is important: a prior unknown/repair
                # result must not keep a permanently resolved job red.
                summary.pop("audit", None)
            else:
                # Do not churn the durable record on every identical scan.
                old_core = {key: value for key, value in previous_audit.items() if key != "updated_at"}
                new_core = {key: value for key, value in audit_state.items() if key != "updated_at"}
                if old_core == new_core and previous_audit.get("updated_at"):
                    audit_state["updated_at"] = previous_audit["updated_at"]
                else:
                    audit_state["updated_at"] = _now()
                summary["audit"] = audit_state

            summary["resource_gaps"] = new_resource_gaps
            summary["last_audit_at"] = _now()
            old_audit = job.summary.get("audit")
            persisted_error = job.error
            if audit_state is not None:
                persisted_error = redact_error(
                    audit_state.get("error")
                    or audit_state.get("message")
                    or "媒体库审计仍有未收口问题"
                )
            elif old_audit is not None or provider_projection_cleared or (
                provider_relevant
                and isinstance(prior_replenishment, Mapping)
                and prior_replenishment.get("terminal") is True
            ):
                # Clear an old audit/provider error only after the fresh scan
                # has removed its owning finding.
                persisted_error = None
            changed = (
                new_resource_gaps != old_resource_gaps
                or summary.get("audit") != old_audit
                or summary.get("replenishment") != job.summary.get("replenishment")
                or persisted_error != job.error
            )
            if changed:
                updated = replace(
                    job, plan=plan, summary=summary, error=persisted_error, updated_at=_now(),
                )
                atomic_write_json(
                    runner.jobs_root / f"{job.id}.json",
                    _redacted_job_payload(updated),
                    allow_nan=False,
                )

            # Ordinary roots keep ingress/archive cleanup pending until this
            # fresh scoped audit has produced a durable provider decision.
            # Provider-owned audit roots have no user ingress and therefore do
            # not enter this lifecycle lane.
            if (
                job.summary.get("automatic") is True
                and not self._is_audit_owned_root(job)
                and callable(getattr(runner, "record_automatic_lifecycle_decision", None))
            ):
                prior_provider = (
                    summary.get("replenishment")
                    if isinstance(summary.get("replenishment"), Mapping)
                    else {}
                )
                if relevant_unknowns:
                    lifecycle_audit = "unknown"
                    lifecycle_provider = "deferred"
                    lifecycle_ready = False
                    lifecycle_reason = "audit_evidence_unknown"
                elif unsupported_relevant:
                    lifecycle_audit = "trusted"
                    lifecycle_provider = "unsupported"
                    lifecycle_ready = False
                    lifecycle_reason = "unsupported_provider_gap"
                elif provider_relevant:
                    lifecycle_audit = "trusted"
                    if isinstance(prior_provider, Mapping) and prior_provider.get("terminal") is True:
                        lifecycle_provider = "terminal"
                        lifecycle_ready = True
                        lifecycle_reason = "provider_terminal_decision"
                    elif not self._provider_auto_repair_enabled():
                        lifecycle_provider = "deferred"
                        lifecycle_ready = True
                        lifecycle_reason = "provider_auto_repair_disabled"
                    else:
                        lifecycle_provider = "pending"
                        lifecycle_ready = False
                        lifecycle_reason = "provider_pending"
                elif repair_relevant:
                    lifecycle_audit = "trusted"
                    lifecycle_provider = "deferred"
                    lifecycle_ready = False
                    lifecycle_reason = "metadata_repair_pending"
                else:
                    lifecycle_audit = "trusted"
                    lifecycle_provider = "no_gap"
                    lifecycle_ready = True
                    lifecycle_reason = "no_provider_gap"
                try:
                    lifecycle_job = runner.record_automatic_lifecycle_decision(
                        job.id,
                        audit_status=lifecycle_audit,
                        provider_status=lifecycle_provider,
                        cleanup_ready=lifecycle_ready,
                        reason=lifecycle_reason,
                    )
                    if lifecycle_ready:
                        runner.finalize_automatic_lifecycle(lifecycle_job.id)
                except (EngineWorkerBusyError, EngineExecutionError, EngineRequestError):
                    # A cleanup race or a remote transient remains durable as
                    # pending/failed state; it must never cause a writer or
                    # provider replay from this audit callback.
                    pass

            if provider_relevant:
                for row in provider_relevant:
                    kind = self._audit_row_kind(row)
                    # The stable semantic coordinates are preferable to the
                    # human label/row id: duplicate roots can have slightly
                    # different labels while still representing one episode.
                    key = (
                        str(job_tmdb or ""),
                        str(job_target or ""),
                        kind,
                        str(row.get("season") or ""),
                        str(row.get("episode") or ""),
                        str(row.get("path") or ""),
                    )
                    if not any(key):
                        key = (str(row.get("id") or ""),)
                    # Ownership is expressed by the persisted root/gap state
                    # and this process' single provider queue.  Do not create
                    # a second durable claim registry or lock protocol.
                    if not same_terminal_gap:
                        provider_gap_owners.setdefault(key, job.id)

        # A paused control plane may persist the owner/projection but must not
        # even submit a provider future.  Resume/startup will perform the
        # bounded dispatch once the operator opens the lane.
        if self.control().get("paused") is not True:
            for job_id in dict.fromkeys(provider_gap_owners.values()):
                self._queue_provider_job(job_id)
        if audit_retry_needed:
            # A new read-only audit is the automatic retry for unknown,
            # unsupported, or failed-sidecar evidence. It is deliberately
            # delayed so one bad work cannot spin the audit thread.
            if scope_roots:
                self._queue_scoped_library_audit(scope_roots, delay=30.0)
            else:
                self._queue_library_audit(delay=30.0)

    def _validate_automatic_source(self, source: str) -> str:
        normalized = source.strip().rstrip("/")
        if not normalized or not normalized.startswith("/"):
            raise EngineRequestError("来源必须是绝对远端目录")
        if is_production_test_media_path(normalized):
            raise EngineRequestError("生产 E2E 测试目录不能创建自动任务")
        inbound = f"{self.remote_root.rstrip('/')}/待刮削/"
        if not normalized.startswith(inbound):
            raise EngineRequestError("来源只能来自待刮削目录的直接子目录")
        relative = normalized[len(inbound):]
        if not relative or "/" in relative or "\\" in relative or relative in {".", ".."}:
            raise EngineRequestError("来源必须是待刮削目录的直接子目录")
        return normalized

    def create_task(self, payload: Mapping[str, object]) -> EngineJob:
        """Register one source directory without entering the worker queue."""
        unknown = set(payload) - {"path", "source_path"}
        if unknown:
            raise EngineRequestError("任务入口只接受 path 或 source_path")
        source = payload.get("path", payload.get("source_path"))
        if not isinstance(source, str) or not source.strip():
            raise EngineRequestError("请输入媒体源目录 path")
        normalized_source = self._validate_automatic_source(source)
        existing = None
        runner = self._get_engine_runner()
        try:
            existing = runner.find_by_source(normalized_source)
        except SimpleEngineError:
            existing = None
        if existing is not None:
            return existing
        if not runner.source_directory_exists(normalized_source):
            raise EngineRequestError("来源目录不存在或不是可读取的目录")
        create = getattr(runner, "create_pending_job", None)
        if not callable(create):
            create = getattr(runner, "create_automatic_job", None)
        if not callable(create):
            raise EngineRequestError("Engine 缺少待处理任务登记入口")
        return create(normalized_source)

    def engine_jobs(self) -> list[EngineJob]:
        if self._engine_runner is not None or self.engine_configured:
            return self._get_engine_runner().list_jobs()
        jobs: list[EngineJob] = []
        root = self.state_root / "jobs"
        for path in sorted(root.glob("*.json")):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SimpleEngineError(f"Engine job 无法读取: {path.stem}") from exc
            if not isinstance(raw, Mapping):
                raise SimpleEngineError(f"Engine job 记录无效: {path.stem}")
            jobs.append(EngineJob.from_dict(raw))
        return jobs

    def engine_job(self, job_id: str) -> EngineJob:
        if self._engine_runner is not None:
            return self._engine_runner.get_job(job_id)
        path = self.state_root / "jobs" / f"{job_id}.json"
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}") from exc
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApplicationError(f"Engine job 无法读取: {job_id}") from exc
        if not isinstance(raw, Mapping):
            raise ApplicationError(f"Engine job 记录无效: {job_id}")
        return EngineJob.from_dict(raw)

    def maybe_engine_job(self, job_id: str) -> EngineJob | None:
        path = self.state_root / "jobs" / f"{job_id}.json"
        if not path.exists() and self._engine_runner is None:
            return None
        try:
            job = self.engine_job(job_id)
        except EngineJobNotFoundError:
            return None
        return None if self._is_internal_child(job) else job

    def browse(self, path: str, *, refresh: bool = False) -> dict[str, object]:
        """Return a small read-only AList directory projection for the Web."""
        if not isinstance(path, str) or not path.startswith("/") or "\x00" in path:
            raise ValueError("浏览路径必须是绝对路径")
        parts = path.split("/")
        if any(part in {"", ".", ".."} for part in parts[1:]):
            raise ValueError("浏览路径含有不安全的路径段")
        normalized = path.rstrip("/") or "/"
        root = self.remote_root.rstrip("/") or "/"
        if root != "/" and normalized != root and not normalized.startswith(root + "/"):
            raise ValueError("浏览路径超出轻量运行时远端根")
        client = self._alist_client
        listing = getattr(client, "list", None) if client is not None else None
        if not callable(listing):
            raise ApplicationError("当前 AList 客户端不支持目录浏览")
        login = getattr(client, "login", None)
        if callable(login) and not getattr(client, "token", None):
            login()
        raw_entries = listing(normalized, refresh=bool(refresh))
        if not isinstance(raw_entries, list):
            raise ApplicationError("AList 目录响应格式无效")
        directories: list[dict[str, object]] = []
        files: list[dict[str, object]] = []
        for raw in raw_entries:
            if not isinstance(raw, Mapping):
                continue
            name = raw.get("name")
            if not isinstance(name, str) or not name or name in {".", ".."}:
                continue
            full_path = f"{normalized.rstrip('/')}/{name}" if normalized != "/" else f"/{name}"
            row = {
                "name": name,
                "path": full_path,
                "is_dir": bool(raw.get("is_dir")),
                "size": raw.get("size"),
                "selectable": True,
            }
            (directories if row["is_dir"] else files).append(row)
        parent = normalized.rsplit("/", 1)[0] or "/"
        return {
            "path": normalized,
            "parent": None if normalized == root else parent,
            "directories": sorted(directories, key=lambda row: str(row["name"]).casefold()),
            "files": sorted(files, key=lambda row: str(row["name"]).casefold()),
        }

    def latest_library_audit(self) -> dict[str, object]:
        """Return the latest explicit read-only audit, if one was run."""
        path = latest_audit_path(self.state_root)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"audit": None}
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApplicationError("最近媒体库审计不可读") from exc
        if not isinstance(raw, Mapping):
            raise ApplicationError("最近媒体库审计格式错误")
        return {"audit": dict(raw)}

    def run_library_audit(self) -> dict[str, object]:
        """Run or join the one serialized full-library audit worker.

        The public ``POST /api/library-audit/run`` endpoint remains
        synchronous, but it must not execute a second scan beside a queued
        background audit.  Joining the shared future keeps ``audit_running``
        truthful for direct requests and leaves a queued coalesced rerun for
        the worker completion callback to honour.
        """
        with self._audit_lock:
            future = self._audit_future
            if future is None or future.done():
                # A user-triggered audit is intentionally global; it is the
                # low-frequency escape hatch for identities outside a recent
                # task's affected root.
                self._pending_audit_roots.clear()
                future = self._start_library_audit_locked()
        result = future.result()
        if not isinstance(result, Mapping):
            raise ApplicationError("媒体库审计未返回报告")
        return dict(result)

    @staticmethod
    def _audit_scope_matches(target: object, scope_roots: Sequence[str] | None) -> bool:
        if scope_roots is None:
            return True
        if not isinstance(target, str) or not target.startswith("/"):
            return False
        return any(
            target == root
            or target.startswith(root.rstrip("/") + "/")
            or root.startswith(target.rstrip("/") + "/")
            for root in scope_roots
        )

    @classmethod
    def _job_audit_target(cls, job: EngineJob) -> object:
        _tmdb, target, _kind = cls._audit_job_coordinates(job)
        return target

    def _run_library_audit_once(
        self, *, scope_roots: Sequence[str] | None = None,
    ) -> dict[str, object]:
        """Run one read-only full or affected-work audit and project its gaps."""
        all_roots = tuple(
            f"{self.remote_root.rstrip('/')}/{category}"
            for category in ("电影", "番剧", "美剧")
        )
        roots = tuple(scope_roots) if scope_roots else all_roots
        runner: SimpleEngineRunner | None = None
        jobs: list[EngineJob] = []
        if self._engine_runner is not None or self.engine_configured:
            try:
                runner = self._get_engine_runner()
                jobs = [
                    job for job in runner.list_jobs()
                    if not self._is_internal_child(job)
                    and self._audit_scope_matches(
                        self._job_audit_target(job), scope_roots,
                    )
                ]
            except Exception:
                # Structural evidence is still valuable when TMDB is briefly
                # unavailable.  The next scheduler heartbeat will enrich it.
                runner = None
        required_subtitle_language = self._audit_required_subtitle_language()
        subtitle_checker = (
            make_alist_subtitle_checker(
                self._alist_client,
                required_subtitle_language,
                state_root=self.state_root,
            )
            if required_subtitle_language is not None else None
        )
        if runner is not None:
            # This is the one composition-root entry point for full-library
            # automation: persisted jobs supply identity, TMDB supplies only
            # already-aired episodes, and the API client supplies no gap list.
            report = run_automatic_library_audit(
                self._alist_client,
                self.state_root,
                jobs,
                tmdb_client=runner.tmdb,
                formal_roots=roots,
                required_subtitle_language=required_subtitle_language,
                subtitle_checker=subtitle_checker,
            )
        else:
            report = audit_and_persist(
                self._alist_client,
                self.state_root,
                formal_roots=roots,
                required_subtitle_language=required_subtitle_language,
                subtitle_checker=subtitle_checker,
            )
        self._apply_audit_gaps(report, runner, scope_roots=scope_roots)
        return {"audit": report}

    def _engine_job_or_none(self, job_id: str) -> EngineJob | None:
        try:
            job = self.engine_job(job_id)
        except EngineJobNotFoundError:
            return None
        return None if self._is_internal_child(job) else job

    def list_public_jobs(self) -> list[dict[str, object]]:
        jobs = [
            self.public_engine_job(job)
            for job in self.engine_jobs()
            if not self._is_internal_child(job)
        ]
        return sorted(jobs, key=lambda row: str(row.get("updated_at") or ""), reverse=True)

    def cancel_public_job(self, job_id: str, reason: str | None = None) -> dict[str, object]:
        engine_job = self._engine_job_or_none(job_id)
        if engine_job is None:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
        result = self._get_engine_runner().cancel_job(job_id, reason=reason or "cancelled")
        self._cancel_job_timers(job_id)
        return self.public_engine_job(result)

    def start_public_job(
        self,
        job_id: str,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        """Persist one user shelf choice, then conditionally submit the root."""
        if not isinstance(payload, Mapping):
            raise EngineRequestError("启动请求必须是 JSON 对象")
        if set(payload) != {"target_shelf"}:
            raise EngineRequestError("启动请求只接受 target_shelf")
        runner = self._get_engine_runner()
        selected = runner.start_automatic_job(
            job_id,
            target_shelf=payload.get("target_shelf"),
        )
        # The durable selection is authoritative even while paused. The
        # scheduler is deliberately a second, conditional step so saving a
        # choice never opens archive/TMDB/writer/provider work by itself.
        if self.control().get("paused") is not True:
            self._queue_automatic_job(selected.id)
        return self.public_engine_job(selected)

    def cleanup_public_job(self, job_id: str) -> dict[str, object]:
        """Discard one terminal root's owned local state, never media files.

        This endpoint deliberately has no path arguments.  The runner accepts
        only the durable root id and removes at most its JSON record, local
        ``gaps/<id>`` and local ``staging/<id>`` after proving that no root or
        child worker remains active.  A remote AList/formal-library delete is
        not part of this operation.
        """
        engine_job = self._engine_job_or_none(job_id)
        if engine_job is None:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
        public_phase = str(self.public_engine_job(engine_job).get("phase") or "")
        active_phases = {
            "queued", "analyzing", "archive_preprocessing", "identity_matching", "planning", "executing_media",
            "verifying", "cleaning", "retry_wait", "gap_discovering", "provider_searching",
            "acquiring", "staging_verifying", "subtitle_installing", "child_planning",
            "child_executing", "final_verifying",
        }
        if public_phase in active_phases:
            raise EngineWorkerBusyError(
                f"任务仍在运行或等待重试，不能清理记录: {public_phase}"
            )
        with self._automatic_lock:
            # Fence the root before inspecting timers.  Timer callbacks execute
            # under this same re-entrant lock, so a callback cannot pop its
            # timer and insert a future after this check has passed.
            self._cleanup_fences.add(engine_job.id)
            try:
                self._cancel_job_timers(engine_job.id)
                pending_timer = any(
                    key[1] == engine_job.id
                    for key in self._scheduled_timers
                )
                if pending_timer:
                    raise EngineWorkerBusyError("任务仍有排队中的重试回调，不能清理记录")
                for future in (
                    self._automatic_futures.get(engine_job.id),
                    self._provider_futures.get(engine_job.id),
                ):
                    if future is not None and not future.done():
                        raise EngineWorkerBusyError("任务仍有活动 worker，不能清理记录")
                runner = self._engine_runner
                if runner is None:
                    # Terminal cleanup is a local-state operation and remains
                    # useful while AList/TMDB credentials are unavailable.
                    # Construct a read-only runner without publishing it as a
                    # configured writer.
                    runner = SimpleEngineRunner(
                        self.state_root,
                        alist=self._alist_client or object(),
                        tmdb=object(),
                        validate=False,
                        library_root=self.remote_root,
                    )
                result = runner.cleanup_terminal_job(engine_job.id)
                return result
            except Exception:
                # A rejected/failed cleanup must remain retryable by the user;
                # remove only the in-memory fence, while the durable runner
                # checks continue to protect the state on the next request.
                raise
            finally:
                # A successful cleanup must not retain an id for a job whose
                # durable record no longer exists.  The fence protects only
                # the critical section above.
                self._cleanup_fences.discard(engine_job.id)

    def retry_public_job(self, job_id: str, payload: Mapping[str, object]) -> dict[str, object]:
        engine_job = self._engine_job_or_none(job_id)
        if engine_job is None:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
        if not isinstance(payload, Mapping):
            raise EngineRequestError("重试请求必须是 JSON 对象")
        if engine_job.phase in {"awaiting_target_shelf", "target_policy_conflict"}:
            raise EngineRequestError("任务尚未通过目标货架启动门；请使用 /start 选择目标货架")
        if not self._ordinary_job_has_confirmed_selection(engine_job):
            raise EngineRequestError(
                "旧 automatic 任务缺少已确认的目标货架；保持只读，不允许重试或自动恢复"
            )
        allowed = {"tmdb_id", "media_type", "season", "archive_password"}
        unknown = set(payload) - allowed
        if unknown:
            raise EngineRequestError("重试请求包含不支持的修正字段")
        correction = self._manual_identity_correction(payload)
        raw_password = payload.get("archive_password")
        password = None
        if raw_password is not None:
            password = self._retry_archive_password(raw_password)
        if engine_job.phase == "failed_cleanup":
            # Cleanup retry is intentionally not a second plan/write retry:
            # the formal target is already durable, and only the idempotent
            # finalizer may resume its recorded cleanup steps.
            if correction is not None or password is not None:
                raise EngineRequestError("failed_cleanup 只允许空请求重试最终清理")
            retry_summary = dict(engine_job.summary)
            retry_summary["cleanup_only_retry"] = True
            retry_summary["automatic_terminal"] = True
            marked = replace(
                engine_job,
                phase="failed_cleanup",
                summary=retry_summary,
                updated_at=_now(),
                error=None,
            )
            marked = self._persist_retry_transition(engine_job, marked)
            retried = self._get_engine_runner().finalize_automatic_lifecycle(job_id)
            return self.public_engine_job(retried)
        error_text = str(engine_job.error or "").casefold()
        archive_failure = (
            isinstance(engine_job.summary.get("archive_preprocessed"), Mapping)
            or any(token in error_text for token in ("archive", "归档", "7z", "密码", "password"))
        )
        if correction is not None:
            if engine_job.phase != "failed_identity" and engine_job.summary.get("automatic_stage") != "identity":
                raise EngineRequestError("身份修正只允许用于 failed_identity 任务")
            if password is not None and not archive_failure:
                raise EngineRequestError("当前任务没有可重试的归档失败")
            summary = dict(engine_job.summary)
            summary.update({
                "manual_identity": correction,
                "automatic_terminal": False,
                "automatic_attempts": 0,
                "next_retry_seconds": 0,
                "requested_retry_count": int(summary.get("requested_retry_count") or 0) + 1,
            })
            source = summary.get("ingress_source_path") or engine_job.request.get("source_path")
            if not isinstance(source, str) or not source.startswith("/"):
                raise EngineRequestError("身份修正任务缺少来源路径")
            retried = replace(
                engine_job,
                phase="queued",
                request={"source_path": source},
                plan={},
                summary=summary,
                updated_at=_now(),
                error=None,
                execution=None,
            )
            retried = self._persist_retry_transition(engine_job, retried)
            if password is not None:
                with self._automatic_lock:
                    self._retry_archive_passwords[engine_job.id] = password
            self._queue_automatic_job(job_id)
            return self.public_engine_job(retried)
        if password is not None:
            if not archive_failure:
                raise EngineRequestError("当前任务没有可重试的归档失败")
            if engine_job.plan:
                raise EngineRequestError("归档密码只能在重新规划前提供")
            with self._automatic_lock:
                self._retry_archive_passwords[engine_job.id] = password
        if engine_job is not None:
            replenishment = (
                engine_job.summary.get("replenishment")
                if isinstance(engine_job.summary.get("replenishment"), Mapping)
                else {}
            )
            if (
                engine_job.phase in {"executed", "completed"}
                and isinstance(replenishment, Mapping)
                and replenishment.get("status") == "failed"
                and replenishment.get("terminal") is True
            ):
                summary = dict(engine_job.summary)
                summary["replenishment"] = {
                    **dict(replenishment),
                    "status": "retry_wait",
                    "terminal": False,
                    "attempts": 0,
                    "next_retry_seconds": 0,
                    "error": None,
                }
                summary["replenishment_attempts"] = 0
                retried = replace(engine_job, summary=summary, updated_at=_now(), error=None)
                retried = self._persist_retry_transition(engine_job, retried)
                self._queue_provider_job(job_id)
                return self.public_engine_job(retried)
            if self._is_terminal_automatic_failure(engine_job):
                summary = dict(engine_job.summary)
                summary.update({
                    "automatic_terminal": False,
                    "automatic_attempts": 0,
                    "next_retry_seconds": 0,
                    "requested_retry_count": int(summary.get("requested_retry_count") or 0) + 1,
                })
                retried = replace(
                    engine_job,
                    phase="queued" if not engine_job.plan else "retry_wait",
                    summary=summary,
                    updated_at=_now(),
                    error=None,
                )
                retried = self._persist_retry_transition(engine_job, retried)
                self._queue_automatic_job(job_id)
                return self.public_engine_job(retried)
            self._queue_automatic_job(job_id)
        return self.public_engine_job(self._get_engine_runner().get_job(job_id))

    def _persist_retry_transition(
        self,
        expected: EngineJob,
        updated: EngineJob,
    ) -> EngineJob:
        """Persist one retry transition only if its source revision is current."""
        runner = self._get_engine_runner()
        with runner.worker_lock():
            latest = runner.get_job(expected.id)
            if latest.phase == "cancelled":
                raise EngineJobConflictError("任务已取消，不能被重试请求重新打开")
            if latest.phase != expected.phase or latest.updated_at != expected.updated_at:
                raise EngineJobConflictError("任务状态已变化，请刷新后再重试")
            atomic_write_json(
                runner.jobs_root / f"{expected.id}.json",
                _redacted_job_payload(updated),
                allow_nan=False,
            )
            return updated

    @staticmethod
    def public_engine_job(job: EngineJob) -> dict[str, object]:
        """Expose the current automatic task and its progress."""
        payload = job.as_dict()
        summary = dict(job.summary)
        replenishment = (
            dict(summary.get("replenishment"))
            if isinstance(summary.get("replenishment"), Mapping)
            else {}
        )
        display_phase = {
            "awaiting_target_shelf": "awaiting_target_shelf",
            "queued": "queued",
            "analyzing": "analyzing",
            "archive_preprocessing": "archive_preprocessing",
            "identity_matching": "identity_matching",
            "planning": "planning",
            "planned": "queued",
            "executing": "executing_media",
            "verifying": "verifying",
            "cleaning": "cleaning",
            "retry_wait": "retry_wait",
            "executed": "completed",
            "completed": "completed",
            "failed": "failed",
            "failed_archive": "failed_archive",
            "failed_identity": "failed_identity",
            "failed_planning": "failed_planning",
            "failed_provider": "failed_provider",
            "failed_write": "failed_write",
            "failed_verification": "failed_verification",
            "failed_cleanup": "failed_cleanup",
            "cancelled": "cancelled",
            "target_policy_conflict": "target_policy_conflict",
        }.get(job.phase, job.phase)
        lifecycle = summary.get("lifecycle") if isinstance(summary.get("lifecycle"), Mapping) else {}
        cleanup_state = lifecycle.get("cleanup") if isinstance(lifecycle, Mapping) else {}
        if (
            job.summary.get("automatic") is True
            and job.phase in {"executed", "completed"}
            and isinstance(cleanup_state, Mapping)
            and cleanup_state.get("status") in {"pending", "running"}
        ):
            display_phase = "cleaning"
        audit_message: str | None = None
        # The formal write can be verified while a machine-discovered gap is
        # still searching/downloading.  Keep the durable Engine fact as
        # ``executed`` but expose the real root-workflow stage to the Web so
        # it never turns green before automatic replenishment has settled.
        if job.phase in {"executed", "completed"}:
            audit_phase, audit_message = SimpleApplication._audit_phase_for_job(job)
            if audit_phase is not None:
                display_phase = audit_phase
            else:
                provider_phase = str(replenishment.get("status") or "")
                if provider_phase in {
                    "gap_discovering", "provider_searching", "acquiring",
                    "staging_verifying", "subtitle_installing", "child_planning", "child_executing",
                    "final_verifying", "cleaning", "child_failed", "retry_wait",
                }:
                    display_phase = "retry_wait" if provider_phase == "child_failed" else provider_phase
                elif provider_phase == "failed" and replenishment.get("terminal") is True:
                    display_phase = "failed_provider"
                else:
                    audit_message = None
        payload["phase"] = display_phase
        payload["engine_phase"] = job.phase
        # ``EngineJob.target_root`` is the user-confirmed first-level shelf.
        # Existing summary/plan target_root remains the concrete work path for
        # audit/provider compatibility, exposed separately below.
        payload["target_shelf"] = job.target_shelf
        payload["target_root"] = job.target_root
        payload["target_work_path"] = (
            summary.get("target_work_path")
            or summary.get("target_root")
            or None
        )
        payload["selected_at"] = job.selected_at
        payload["allowed_target_shelves"] = list(target_shelf_values())
        payload["lifecycle"] = dict(lifecycle)
        payload["source"] = (
            "全库审计"
            if SimpleApplication._is_audit_owned_root(job)
            else summary.get("ingress_source_path")
            or summary.get("source_root")
        )
        payload["parent"] = summary.get("target_root")
        payload["media_type"] = summary.get("mode")
        plan_body = dict(job.plan)
        resource_gaps = SimpleApplication._job_resource_gaps(job)
        payload["plan"] = {
            "kind": "media",
            "title": summary.get("title"),
            "tmdb_id": summary.get("tmdb_id"),
            "source_root": summary.get("source_root"),
            "target_root": summary.get("target_root"),
            "target_work_path": summary.get("target_work_path") or summary.get("target_root"),
            "file_count": summary.get("file_count"),
            "normal_file_count": summary.get("file_count"),
            "warning_count": summary.get("warning_count"),
            "warnings": list(plan_body.get("warnings") or []),
            "problem_file_count": summary.get("problem_count"),
            "problem_files": list(plan_body.get("problem_files") or []),
            "cleanup_file_count": summary.get("cleanup_count"),
            "cleanup_files": list(plan_body.get("cleanup_files") or []),
            "notices": list(plan_body.get("notices") or []),
            "scan_report": dict(plan_body.get("scan_report") or {}),
            # The scan report remains available as evidence, but the Web
            # contract consumes these stable top-level fields.  Do not force
            # each UI surface to understand the audit's nested schema.
            "resource_gaps": resource_gaps,
            "resource_gap_count": len(resource_gaps),
            "gap_count": len(resource_gaps),
            "replenishment": replenishment or None,
        }
        audit_projection = summary.get("audit")
        payload["audit"] = (
            dict(audit_projection)
            if isinstance(audit_projection, Mapping)
            else None
        )
        # Keep the complete plan available for task details.
        payload["plan_body"] = plan_body
        provider_progress = {
            "gap_discovering": (86, "系统正在重新核对正式媒体库缺口"),
            "provider_searching": (89, "系统正在自动搜索补源候选"),
            "acquiring": (91, "系统正在自动获取补源文件"),
            "staging_verifying": (93, "系统正在核对补源 staging"),
            "subtitle_installing": (94, "系统正在安装精确绑定字幕"),
            "child_planning": (95, "系统正在自动规划补源"),
            "child_executing": (97, "系统正在自动整理补源"),
            "final_verifying": (98, "系统正在核对补源最终结果"),
            "cleaning": (99, "系统正在清理本任务补源 staging"),
            "retry_wait": (90, "系统正在等待下一次自动补源重试"),
        }
        provider_percent, provider_message = provider_progress.get(display_phase, (None, None))
        if display_phase == "failed_verification" and audit_message:
            provider_message = audit_message
        elif display_phase == "failed_provider" and audit_message:
            provider_message = audit_message
        elif display_phase == "retry_wait" and audit_message:
            provider_message = audit_message
        elif display_phase == "gap_discovering" and audit_message:
            provider_message = audit_message
        payload["progress"] = {
            "stage": display_phase,
            "completed": 1 if display_phase in {"completed", "completed_with_gaps"} else 0,
            "total": int(summary.get("file_count") or 0),
            "percent": 100 if display_phase in {"completed", "completed_with_gaps"} else provider_percent or 0,
            "message": (
                provider_message
                if provider_message is not None
                else "系统已生成计划，正在自动排队执行"
                if job.phase == "planned"
                else "等待用户选择电影、番剧或美剧目标货架"
                if job.phase == "awaiting_target_shelf"
                else "TMDB 识别结果与目标货架冲突；请重新选择货架后启动"
                if job.phase == "target_policy_conflict"
                else "系统正在等待下一次自动重试"
                if job.phase == "retry_wait"
                else "Engine 计划已执行并完成远端大小核验"
                if job.phase == "executed" and display_phase == "completed"
                else f"已完成正式整理；保留 {len(resource_gaps)} 项资源缺口（自动补源已跳过）"
                if display_phase == "completed_with_gaps"
                else "媒体库审计仍有未收口问题"
                if job.phase == "executed"
                else job.phase
            ),
        }
        payload["identity"] = (
            dict(summary.get("identity"))
            if isinstance(summary.get("identity"), Mapping)
            else None
        )
        if SimpleApplication._is_audit_owned_root(job):
            payload["readback"] = {
                "status": "verified" if display_phase in {"completed", "completed_with_gaps"} else "pending",
                "checked_at": job.updated_at if display_phase in {"completed", "completed_with_gaps"} else None,
                "message": (
                    "全库审计已确认缺口消失"
                    if display_phase in {"completed", "completed_with_gaps"}
                    else "审计只记录现有库状态；正式写入由补源 child 完成"
                ),
            }
        else:
            payload["readback"] = {
                "status": "verified" if job.phase in {"executed", "completed"} else "pending",
                "checked_at": job.updated_at if job.phase in {"executed", "completed"} else None,
            }
        payload["settings"] = {
            "media_type": summary.get("mode"),
            "tmdb_id": summary.get("tmdb_id"),
        }
        payload["recovery_available"] = False
        payload["automatic_attempts"] = int(summary.get("automatic_attempts") or 0)
        payload["next_retry_seconds"] = (
            summary.get("next_retry_seconds")
            if summary.get("next_retry_seconds") is not None
            else replenishment.get("next_retry_seconds")
        )
        redacted = redact_value(payload)
        return dict(redacted) if isinstance(redacted, Mapping) else payload

    def control(self) -> dict[str, object]:
        with self._control_lock:
            return self._control_state.read()

    def set_paused(self, paused: bool, reason: str | None = None) -> dict[str, object]:
        if not isinstance(paused, bool):
            raise TypeError("paused must be boolean")
        with self._control_lock:
            payload = self._control_state.set_paused(paused, reason)
        if not paused and not self._closed.is_set():
            self._start_startup_thread(
                self._resume_automatic_jobs,
                name="scrapeflow-resume",
            )
            self._intake_wake.set()
        return payload

    def close(self) -> None:
        """Stop local schedulers during a controlled server shutdown.

        Normal Docker restarts use this after the HTTP listener stops.  It is
        also useful for short-lived test applications: no background retry or
        read-only audit should still be writing a JSON file while its temporary
        state directory is being removed.
        """
        if self._closed.is_set():
            return
        self._closed.set()
        self._intake_stop.set()
        self._intake_wake.set()
        with self._automatic_lock:
            timers = list(self._scheduled_timers.values())
            self._scheduled_timers.clear()
        for timer in timers:
            try:
                timer.cancel()
            except Exception:
                pass
        executors = [
            self._automatic_executor,
            self._provider_executor,
            self._audit_executor,
        ]
        for executor in executors:
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
        thread = self._intake_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
        for startup in list(self._startup_threads):
            if startup is not threading.current_thread():
                startup.join(timeout=1.0)

def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _safe_remote_root(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "\x00" in value or "\\" in value:
        raise ApplicationError("SCRAPEFLOW_MEDIA_ROOT 必须是安全的绝对路径")
    normalized = posixpath.normpath(value)
    if normalized == "/" or normalized != value.rstrip("/"):
        raise ApplicationError("SCRAPEFLOW_MEDIA_ROOT 必须是规范化的媒体库路径")
    return normalized


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().casefold()
    if value not in {"0", "1", "false", "true", "no", "yes"}:
        raise ApplicationError(f"{name} 必须是 0/1 或 true/false")
    return value in {"1", "true", "yes"}


class SimpleHandler(BaseHTTPRequestHandler):
    """JSON API for automatic tasks, controls, browsing, and audits."""

    server_version = "ScrapeFlowSimple/1"

    @property
    def application(self) -> SimpleApplication:
        return self.server.application  # type: ignore[attr-defined,no-any-return]

    @staticmethod
    def _loopback_authority_allowed(authority: object) -> bool:
        """Accept only a syntactically valid loopback Host/Origin authority."""
        if not isinstance(authority, str) or not authority or authority != authority.strip():
            return False
        try:
            parsed = urllib.parse.urlsplit(f"//{authority}")
            hostname = parsed.hostname
            port = parsed.port
        except ValueError:
            return False
        if (
            parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or not hostname
            or (port is not None and not 1 <= port <= 65535)
        ):
            return False
        if hostname.casefold() == "localhost":
            return True
        try:
            return ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            return False

    def _origin_matches_loopback_host(self) -> bool:
        """Validate an optional browser Origin against the received Host."""
        host = self.headers.get("Host", "")
        if not self._loopback_authority_allowed(host):
            return False
        origin = self.headers.get("Origin")
        if not origin:
            return True
        if origin != origin.strip():
            return False
        try:
            parsed = urllib.parse.urlsplit(origin)
        except ValueError:
            return False
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or parsed.netloc.casefold() != host.casefold()
        ):
            return False
        return self._loopback_authority_allowed(parsed.netloc)

    def _local_same_origin_allowed(self) -> bool:
        if not self._origin_matches_loopback_host():
            return False
        return self.headers.get("Sec-Fetch-Site", "").strip().casefold() != "cross-site"

    def _reject_nonlocal_request(self) -> bool:
        if self._local_same_origin_allowed():
            return False
        self._send(403, {"error": "仅允许同源本机 ScrapeFlow 页面访问"})
        return True

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        path, query = self._path()
        try:
            if path == "/api/health":
                self._send(200, self.application.health())
            elif path == "/api/control":
                self._send(200, self.application.control())
            elif path == "/api/jobs":
                self._send(200, {"jobs": self.application.list_public_jobs()})
            elif path.startswith("/api/jobs/") and path.count("/") == 3:
                job_id = urllib.parse.unquote(path.rsplit("/", 1)[1])
                engine_job = self.application.maybe_engine_job(job_id)
                if engine_job is None:
                    raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
                self._send(200, {"job": self.application.public_engine_job(engine_job)})
            elif path == "/api/browse":
                browse_path = (query.get("path") or [self.application.remote_root])[0]
                refresh = (query.get("refresh") or ["0"])[0] == "1"
                self._send(200, self.application.browse(browse_path, refresh=refresh))
            elif path == "/api/library-audit/latest":
                self._send(200, self.application.latest_library_audit())
            else:
                self._send(404, {"error": "not found"})
        except Exception as exc:
            self._handle_error(exc)

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        if self._reject_nonlocal_request():
            return
        path, _query = self._path()
        try:
            payload = self._json_body()
            if path == "/api/jobs":
                try:
                    job = self.application.create_task(payload)
                except DuplicateEngineTask as exc:
                    self._send(
                        409,
                        {"error": str(exc), "job": self.application.public_engine_job(exc.job)},
                    )
                    return
                self._send(201, {"job": self.application.public_engine_job(job)})
            elif path == "/api/control/pause":
                self._send(200, self.application.set_paused(True, self._optional_reason(payload)))
            elif path == "/api/control/resume":
                self._send(200, self.application.set_paused(False))
            elif path == "/api/library-audit/run":
                self._send(200, self.application.run_library_audit())
            elif path.startswith("/api/jobs/"):
                pieces = path.split("/")
                if len(pieces) != 5:
                    self._send(404, {"error": "not found"})
                    return
                job_id, operation = urllib.parse.unquote(pieces[3]), pieces[4]
                if operation == "start":
                    self._send(
                        200,
                        {"job": self.application.start_public_job(job_id, payload)},
                    )
                    return
                if operation == "retry":
                    self._send(200, {"job": self.application.retry_public_job(job_id, payload)})
                    return
                if operation == "cancel":
                    self._send(
                        200,
                        {"job": self.application.cancel_public_job(
                            job_id,
                            reason=self._optional_reason(payload),
                        )},
                    )
                    return
                if operation == "cleanup":
                    self._send(
                        200,
                        {"cleanup": self.application.cleanup_public_job(job_id)},
                    )
                    return
                self._send(404, {"error": "not found"})
            else:
                self._send(404, {"error": "not found"})
        except Exception as exc:
            self._handle_error(exc)

    def _path(self) -> tuple[str, dict[str, list[str]]]:
        parsed = urllib.parse.urlsplit(self.path)
        return parsed.path.rstrip("/") or "/", urllib.parse.parse_qs(parsed.query)

    def _json_body(self) -> dict[str, object]:
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().casefold()
        if content_type != "application/json":
            raise ValueError("请求 Content-Type 必须是 application/json")
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length < 0 or length > 1024 * 1024:
            raise ValueError("request body is too large")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    @staticmethod
    def _optional_reason(payload: Mapping[str, object]) -> str | None:
        reason = payload.get("reason")
        if reason is None:
            return None
        if not isinstance(reason, str):
            raise ValueError("reason must be a string")
        return reason[:500]

    def _handle_error(self, exc: Exception) -> None:
        if isinstance(exc, EngineJobNotFoundError):
            status = 404
        elif isinstance(exc, EngineWorkerBusyError):
            status = 409
        elif isinstance(
            exc,
            (
                ValueError,
                TypeError,
                EngineRequestError,
            ),
        ):
            status = 400
        elif isinstance(exc, EngineExecutionError):
            status = 409
        elif isinstance(exc, ApplicationError):
            status = 503
        elif isinstance(exc, SimpleEngineError):
            status = 503
        elif type(exc).__module__.startswith("engine.") or "AList" in str(exc):
            status = 503
        else:
            status = 500
        message = str(exc) or type(exc).__name__
        self._send(status, {"error": redact_error(message)})

    def _send(self, status: int, payload: Mapping[str, object]) -> None:
        body = json.dumps(redact_value(payload), ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class SimpleHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: tuple[str, int], application: SimpleApplication) -> None:
        self.application = application
        super().__init__(address, SimpleHandler)


def make_server(application: SimpleApplication, host: str = "127.0.0.1", port: int = 8765) -> SimpleHTTPServer:
    """Construct (but do not start) a testable simple HTTP server."""
    return SimpleHTTPServer((host, int(port)), application)


def main() -> int:
    host = os.getenv("SCRAPEFLOW_API_HOST", "127.0.0.1")
    port = int(os.getenv("SCRAPEFLOW_API_PORT", "8765"))
    application = SimpleApplication()
    server = make_server(application, host, port)

    def stop(_signum: int, _frame: Any) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, stop)
    try:
        print(f"ScrapeFlow simple API: http://{host}:{port}", flush=True)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        application.close()
        signal.signal(signal.SIGTERM, previous)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["SimpleApplication", "SimpleHTTPServer", "SimpleHandler", "make_server", "main"]
