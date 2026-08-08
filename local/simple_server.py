#!/usr/bin/env python3
"""Single-user HTTP service for ScrapeFlow's automatic media workflow."""

from __future__ import annotations

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
from local.scrapeflow_api.simple_engine_runner import (
    EngineExecutionError,
    EngineJob,
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


_TARGET_CATEGORY_PARENTS = {
    "番剧": "/quark/影视/番剧",
    "美剧": "/quark/影视/美剧",
    "电影": "/quark/影视/电影",
}
_CATEGORY_MEDIA_TYPES = {"电影": "movie", "番剧": "tv", "美剧": "tv"}

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
        self._engine_runner_lock = threading.Lock()
        self._automatic_replenishment: AutomaticReplenishmentRuntime | None = None
        self._automatic_replenishment_lock = threading.Lock()
        self._control_path = self.state_root / "global-control.json"
        self._control_lock = threading.Lock()
        self._automatic_lock = threading.RLock()
        self._automatic_executor: ThreadPoolExecutor | None = None
        self._automatic_futures: dict[str, Future[object]] = {}
        self._provider_executor: ThreadPoolExecutor | None = None
        self._provider_futures: dict[str, Future[object]] = {}
        self._audit_lock = threading.RLock()
        self._audit_executor: ThreadPoolExecutor | None = None
        self._audit_future: Future[object] | None = None
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
        }
        self._ensure_control_document()
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

    def _ensure_control_document(self) -> None:
        """Create the one persisted control document on an unbootstrapped root."""
        if not self._control_path.exists():
            raw_start_paused = os.getenv("SCRAPEFLOW_START_PAUSED", "0").strip().casefold()
            if raw_start_paused not in {"0", "1", "false", "true", "no", "yes"}:
                raise ApplicationError("SCRAPEFLOW_START_PAUSED 必须是 0/1 或 true/false")
            paused = raw_start_paused in {"1", "true", "yes"}
            atomic_write_json(
                self._control_path,
                {
                    "version": 1,
                    "paused": paused,
                    "scheduler_paused": paused,
                    "persistent": True,
                    "updated_at": _now(),
                    "reason": "startup pause" if paused else None,
                },
                allow_nan=False,
            )
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
            "queued", "analyzing", "identity_matching", "planning", "planned",
            "executing", "verifying", "cleaning", "retry_wait",
        }
        failed_engine_phases = {
            "failed", "failed_identity", "failed_provider", "failed_write",
            "failed_verification", "failed_cleanup",
        }
        provider_active = {
            "gap_discovering", "provider_searching", "acquiring",
            "staging_verifying", "child_planning", "child_executing",
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
            "jobs_active": sum(1 for phase in public_phases if phase in active_public_phases),
            "jobs_failed": sum(1 for phase in public_phases if phase in failed_engine_phases),
            # Count the public root state, not merely the formal Engine move
            # fact. An executed media move with unresolved audit findings is
            # deliberately not a completed project.
            "jobs_completed": sum(1 for phase in public_phases if phase == "completed"),
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
        return _env_bool("SCRAPEFLOW_AUTOMATIC_AUDIT", self.enforce_engine_roots)

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
            "queued", "analyzing", "identity_matching", "planning", "planned",
            "executing", "verifying", "cleaning", "retry_wait", "failed",
        }

    def _scan_inbound_once(self) -> list[str]:
        """Turn each direct child of ``/待刮削`` into one automatic job.

        Only directories are accepted.  Treating loose files at the intake
        root as one job could accidentally combine unrelated titles, so they
        are left untouched until placed in their own source directory.  This
        method reads AList with ``refresh=True`` and never moves or deletes
        anything itself.
        """
        if self.control().get("paused") is True or not self.engine_configured:
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
            existing = {
                str(job.request.get("source_path")): job
                for job in runner.list_jobs()
                if isinstance(job.request.get("source_path"), str)
            }
        except Exception:
            existing = {}
        scheduled: list[str] = []
        create = getattr(runner, "create_automatic_job", None)
        if not callable(create):
            return scheduled
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
            job = existing.get(source)
            if job is None:
                job = create(source)
            elif not self._automatic_job_needs_dispatch(job):
                continue
            self._queue_automatic_job(job.id)
            scheduled.append(job.id)
        with self._automatic_lock:
            self._intake_status.update({
                "last_scan_at": _now(),
                "last_error": None,
                "last_scheduled_count": len(scheduled),
            })
        return scheduled

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
                    updated.as_dict(),
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
                or Path(path).suffix.casefold() not in {
                    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm", ".wmv", ".iso",
                }
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
                "failed_identity", "failed_write", "failed_verification", "failed_cleanup",
            }
        return job.summary.get("automatic_terminal") is True or job.phase in {
            "failed_identity", "failed_write", "failed_verification", "failed_cleanup",
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
    def _audit_phase_for_job(job: EngineJob) -> tuple[str | None, str | None]:
        """Map unresolved audit state to an existing public phase.

        The Web already understands ``retry_wait``, ``failed_provider`` and
        ``failed_verification``.  Reusing those states keeps the root task
        visibly incomplete without adding another UI-controlled state.
        """
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
                    "staging_verifying", "child_planning", "child_executing",
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
            return "gap_discovering", "系统正在自动处理媒体库缺口"
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

        if delay > 0:
            timer = threading.Timer(delay, submit)
            timer.daemon = True
            timer.start()
        else:
            submit()

    @staticmethod
    def _automatic_failure_stage(error: Exception, job: EngineJob | None = None) -> str:
        text = str(error).casefold()
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
        try:
            job = runner.get_job(job_id)
        except EngineJobNotFoundError:
            return
        if job.phase in {"executed", "cancelled"}:
            return
        summary = dict(job.summary)
        stage = stage or self._automatic_failure_stage(error, job)
        attempts = int(summary.get("automatic_attempts") or 0) + 1
        summary["automatic_attempts"] = attempts
        summary[f"{stage}_attempts"] = int(summary.get(f"{stage}_attempts") or 0) + 1
        summary["automatic_stage"] = stage
        summary["next_retry_seconds"] = None
        phase = {
            "identity": "failed_identity",
            "provider": "failed_provider",
            "verification": "failed_verification",
            "cleanup": "failed_cleanup",
        }.get(stage, "failed_write")
        if attempts <= self._automatic_retry_limit():
            delay = min(60.0, float(2 ** max(0, attempts - 1)))
            summary["next_retry_seconds"] = delay
            phase = "retry_wait"
            summary["automatic_terminal"] = False
        else:
            summary["automatic_terminal"] = True
        updated = replace(
            job,
            phase=phase,
            updated_at=_now(),
            summary=summary,
            error=str(error) or type(error).__name__,
        )
        atomic_write_json(runner.jobs_root / f"{job_id}.json", updated.as_dict(), allow_nan=False)
        if phase == "retry_wait":
            self._queue_automatic_job(job_id, delay=float(summary["next_retry_seconds"] or 1))
        elif phase == "failed_identity" and summary.get("automatic_terminal") is True:
            # A terminal identity failure may have no plan and therefore is
            # not picked up by the normal execution queue.  Keep the latest
            # full-library snapshot moving so an identity bootstrap or a
            # newly available TMDB response can be observed without a manual
            # action.  The audit itself is read-only and remains single-flight.
            self._queue_library_audit(delay=1.0)

    def _run_automatic_job(self, job_id: str) -> None:
        """Reconcile first, then execute only the still-missing plan work."""
        if self.control().get("paused") is True:
            return
        try:
            runner = self._get_engine_runner()
            job = runner.get_job(job_id)
            if self._is_internal_child(job):
                return
            if job.phase in {"executed", "cancelled"}:
                return
            if self._is_terminal_automatic_failure(job):
                return
            # A queued/retry identity job has no plan yet.  Resolve it inside
            # the same scheduler; a transient TMDB error is persisted and
            # retried rather than returned as a transient HTTP error.
            if job.phase in {"queued", "identity_matching", "planning", "failed_identity"} or (
                job.phase == "retry_wait" and not job.plan
            ):
                try:
                    job = runner.plan_automatic_job(job_id)
                except Exception as exc:
                    self._record_automatic_retry(job_id, exc, stage="identity")
                    return
            if job.phase in {"executing", "retry_wait", "failed", "failed_write", "failed_verification", "failed_cleanup"}:
                job = runner.recover_job(job_id)
                if job.phase == "executed":
                    self._sync_replenishment_child(job)
                    if self._has_provider_gaps(job):
                        self._queue_provider_job(job.id)
                    self._queue_library_audit(delay=0.5)
                    return
            if job.phase not in {"planned", "retry_wait", "failed", "failed_write", "failed_verification", "failed_cleanup"}:
                return
            done = runner.execute_automatic(job_id)
            self._sync_replenishment_child(done)
            if self._has_provider_gaps(done):
                self._queue_provider_job(done.id)
            # The child may have committed a video while another audit was
            # already running.  Ask the audit coordinator for one fresh pass
            # after that run settles; this is still read-only and does not
            # widen the provider/root identity.
            self._queue_library_audit(delay=0.5, rerun_if_busy=True)
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
            provider_gap_keys: set[tuple[str, ...]] = set()
            for job in runner.list_jobs():
                if self._is_internal_child(job):
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
                    "queued", "identity_matching", "planning", "planned", "executing",
                    "retry_wait", "failed", "failed_write", "failed_verification", "failed_cleanup",
                }:
                    self._queue_automatic_job(job.id)
                elif job.phase == "executed":
                    if self._has_provider_gaps(job):
                        scan = job.plan.get("scan_report") if isinstance(job.plan.get("scan_report"), Mapping) else {}
                        rows = scan.get("resource_gaps") if isinstance(scan, Mapping) else []
                        metadata = job.plan.get("metadata") if isinstance(job.plan.get("metadata"), Mapping) else {}
                        identity = job.summary.get("identity") if isinstance(job.summary.get("identity"), Mapping) else {}
                        job_tmdb = identity.get("tmdb_id") or metadata.get("tmdb_id") or job.summary.get("tmdb_id")
                        job_target = (
                            metadata.get("series_root")
                            or metadata.get("target_root")
                            or job.plan.get("target_root")
                            or identity.get("target_root")
                        )
                        queue_owner = False
                        for row in rows if isinstance(rows, list) else []:
                            if not isinstance(row, Mapping):
                                continue
                            if self._audit_row_kind(row) not in _AUTOMATIC_PROVIDER_GAP_KINDS:
                                continue
                            media = row.get("media") if isinstance(row.get("media"), Mapping) else {}
                            row_tmdb = media.get("tmdb_id") if isinstance(media, Mapping) else None
                            row_target = media.get("target_root") if isinstance(media, Mapping) else None
                            key = (
                                str(row_tmdb if row_tmdb is not None else job_tmdb or ""),
                                str(row_target if isinstance(row_target, str) and row_target else job_target or ""),
                                self._audit_row_kind(row),
                                str(row.get("season") or ""),
                                str(row.get("episode") or ""),
                                str(row.get("path") or ""),
                            )
                            if not any(key):
                                key = (str(row.get("id") or ""),)
                            if key not in provider_gap_keys:
                                provider_gap_keys.add(key)
                                queue_owner = True
                        if queue_owner:
                            self._queue_provider_job(job.id)
                    if self._audit_needs_retry(job):
                        self._queue_library_audit(delay=1.0)
        except Exception:
            # Health/status endpoints remain available while a network or
            # credential issue is repaired; an explicit resume/retry will
            # invoke the same automatic path later.
            return

    def _provider_pool(self) -> ThreadPoolExecutor:
        with self._automatic_lock:
            if self._provider_executor is None:
                workers_raw = os.getenv("SCRAPEFLOW_PROVIDER_WORKERS", "3").strip()
                try:
                    workers = max(1, min(8, int(workers_raw)))
                except ValueError:
                    workers = 3
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
                materializer=LocalTorrentAutomaticMaterializer(),
                staging_root=f"{self.remote_root.rstrip('/')}/ScrapeFlow/补源",
                progress=self._record_replenishment_progress,
                cancel_requested=self._provider_runtime_cancel_requested,
            )
            self._automatic_replenishment = runtime
            return runtime

    def _queue_provider_job(self, job_id: str, *, delay: float = 0.0) -> None:
        if self._closed.is_set() or self.control().get("paused") is True:
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
                    if self._closed.is_set() or self.control().get("paused") is True:
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
                                replace(current, summary=summary, updated_at=_now()).as_dict(),
                                allow_nan=False,
                            )
            except Exception:
                # Queueing is best-effort here; the worker will still persist a
                # failure if the job record cannot be read or written.
                pass

        def submit() -> None:
            if self._closed.is_set() or self.control().get("paused") is True:
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

        if delay > 0:
            timer = threading.Timer(delay, submit)
            timer.daemon = True
            timer.start()
        else:
            submit()

    def _record_replenishment_summary(self, job: EngineJob, outcome: Mapping[str, object]) -> None:
        runner = self._get_engine_runner()
        current = runner.get_job(job.id)
        summary = dict(current.summary)
        prior = current.summary.get("replenishment")
        merged = dict(prior) if isinstance(prior, Mapping) else {}
        merged.update(dict(outcome))
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
        atomic_write_json(runner.jobs_root / f"{current.id}.json", updated.as_dict(), allow_nan=False)

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
            replenishment.update({"status": phase, "updated_at": now, **dict(details)})

            # A provider attempt owns a real, restartable Engine child, but
            # that child is an implementation detail rather than a second
            # public task.  Keep its latest phase on the root projection so
            # the operations page can explain what is happening without
            # enumerating (or accidentally scheduling) the child record.
            child_id = details.get("child_job_id")
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
                child_phase = details.get("child_phase")
                child_row: dict[str, object] = {
                    "id": child_id,
                    "phase": child_phase if isinstance(child_phase, str) and child_phase else phase,
                    "updated_at": now,
                }
                round_number = details.get("round")
                if isinstance(round_number, int):
                    child_row["round"] = round_number
                error = details.get("error")
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
            atomic_write_json(runner.jobs_root / f"{current.id}.json", updated.as_dict(), allow_nan=False)
        except Exception:
            return

    def _run_automatic_replenishment(self, job_id: str) -> None:
        # A resume can submit more futures than the provider worker count. A
        # queued future may therefore start after a pause or after the pilot
        # selector changes; re-check both immediately before doing any provider
        # work so the global pause remains a real dispatch boundary.
        self._provider_pilot_tmdb()
        if self._closed.is_set() or self.control().get("paused") is True:
            return
        try:
            runner = self._get_engine_runner()
            job = runner.get_job(job_id)
            if self._is_internal_child(job):
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
            if cancelled:
                # The runtime has already persisted gap-level retry_wait. Do
                # not trigger a fresh audit or delayed provider timer while a
                # global pause/allowlist boundary is in effect.
                return
            # A provider child may have committed a video while another
            # read-only audit was still traversing the previous inventory.
            # Coalesce one follow-up so that fresh media gets its configured
            # subtitle probe and, if absent, the pure sidecar lane.
            self._queue_library_audit(delay=0.5, rerun_if_busy=True)
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
                    "error": str(exc) or type(exc).__name__,
                    "next_retry_seconds": None if terminal else 30,
                })
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
        future = self._audit_pool().submit(self._run_library_audit_background)
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
        if self._closed.is_set() or self.control().get("paused") is True:
            return

        def submit() -> None:
            if self._closed.is_set() or self.control().get("paused") is True:
                return
            with self._audit_lock:
                if self._audit_future is not None and not self._audit_future.done():
                    if rerun_if_busy:
                        self._audit_rerun_requested = True
                    return
                self._start_library_audit_locked()

        if delay > 0:
            timer = threading.Timer(delay, submit)
            timer.daemon = True
            timer.start()
        else:
            submit()

    def _run_library_audit_background(self) -> dict[str, object] | None:
        try:
            return self._run_library_audit_once()
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
            if provider_relevant and isinstance(prior_replenishment, Mapping) and prior_replenishment.get("terminal") is True:
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
                    "status": "retry_wait",
                    "message": "系统正在自动修复 NFO/海报，等待新鲜审计确认",
                    "gaps": repair_relevant,
                    "unknowns": [],
                    "gap_count": len(repair_relevant),
                    "unknown_count": 0,
                    "retryable": True,
                    "repair_attempts": int(previous_audit.get("repair_attempts") or 0),
                }
                audit_retry_needed = True

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
                persisted_error = str(
                    audit_state.get("error") or audit_state.get("message") or "媒体库审计仍有未收口问题"
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
                atomic_write_json(runner.jobs_root / f"{job.id}.json", updated.as_dict(), allow_nan=False)

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
                    provider_gap_owners.setdefault(key, job.id)

            if repair_relevant:
                if self.control().get("paused") is True:
                    # A full-library scan remains read-only while the global
                    # worker pause is active.  Keep the durable retry_wait
                    # projection above; resume will run the repair path.
                    continue
                try:
                    runner.repair_automatic_artifacts(job.id)
                except Exception as exc:
                    # Repair failures are first-class root failures. Keep the
                    # task non-green and retry the audit/repair automatically
                    # until the bounded retry budget is exhausted.
                    current = runner.get_job(job.id)
                    current_summary = dict(current.summary)
                    current_audit = current_summary.get("audit")
                    state = dict(current_audit) if isinstance(current_audit, Mapping) else dict(audit_state or {})
                    attempts = int(state.get("repair_attempts") or 0) + 1
                    state.update({
                        "status": "retry_wait" if attempts <= self._automatic_retry_limit() else "failed",
                        "repair_attempts": attempts,
                        "error": str(exc) or type(exc).__name__,
                        "message": "NFO/海报自动修复失败，将继续自动重试",
                        "retryable": attempts <= self._automatic_retry_limit(),
                        "next_retry_seconds": min(60.0, float(2 ** max(0, attempts - 1)))
                        if attempts <= self._automatic_retry_limit() else None,
                        "updated_at": _now(),
                    })
                    current_summary["audit"] = state
                    atomic_write_json(
                        runner.jobs_root / f"{job.id}.json",
                        replace(
                            current,
                            summary=current_summary,
                            error=str(exc) or type(exc).__name__,
                            updated_at=_now(),
                        ).as_dict(),
                        allow_nan=False,
                    )
                    audit_retry_needed = attempts <= self._automatic_retry_limit()
                else:
                    # The report that triggered repair is intentionally stale;
                    # keep retry_wait until the next fresh scan proves the
                    # sidecars exist. A successful repair must not flash green.
                    current = runner.get_job(job.id)
                    current_summary = dict(current.summary)
                    current_audit = current_summary.get("audit")
                    state = dict(current_audit) if isinstance(current_audit, Mapping) else dict(audit_state or {})
                    state.update({
                        "status": "retry_wait",
                        "message": "元数据已尝试修复，等待新鲜审计确认",
                        "retryable": True,
                        "next_retry_seconds": 1,
                        "updated_at": _now(),
                    })
                    current_summary["audit"] = state
                    atomic_write_json(
                        runner.jobs_root / f"{job.id}.json",
                        replace(current, summary=current_summary, updated_at=_now()).as_dict(),
                        allow_nan=False,
                    )

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
            self._queue_library_audit(delay=30.0)

    def _validate_automatic_source(self, source: str) -> str:
        normalized = source.strip().rstrip("/")
        if not normalized or not normalized.startswith("/"):
            raise EngineRequestError("来源必须是绝对远端目录")
        if is_production_test_media_path(normalized):
            raise EngineRequestError("生产 E2E 测试目录不能创建自动任务")
        if self.enforce_engine_roots:
            inbound = f"{self.remote_root.rstrip('/')}/待刮削/"
            replenishment = f"{self.remote_root.rstrip('/')}/ScrapeFlow/补源/"
            if not (normalized.startswith(inbound) or normalized.startswith(replenishment)):
                raise EngineRequestError("来源只能来自待刮削目录或系统补源目录")
        return normalized

    def create_task(self, payload: Mapping[str, object]) -> EngineJob:
        """Submit one source directory and immediately queue automatic execution."""
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
            self._queue_automatic_job(existing.id)
            return existing
        job = runner.create_automatic_job(normalized_source)
        self._queue_automatic_job(job.id)
        return job

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
                future = self._start_library_audit_locked()
        result = future.result()
        if not isinstance(result, Mapping):
            raise ApplicationError("媒体库审计未返回报告")
        return dict(result)

    def _run_library_audit_once(self) -> dict[str, object]:
        """Run one read-only audit and feed its machine gaps to automation."""
        roots = tuple(
            f"{self.remote_root.rstrip('/')}/{category}"
            for category in ("电影", "番剧", "美剧")
        )
        runner: SimpleEngineRunner | None = None
        jobs: list[EngineJob] = []
        if self._engine_runner is not None or self.engine_configured:
            try:
                runner = self._get_engine_runner()
                jobs = [
                    job for job in runner.list_jobs()
                    if not self._is_internal_child(job)
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
        self._apply_audit_gaps(report, runner)
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
        return self.public_engine_job(result)

    def retry_public_job(self, job_id: str, payload: Mapping[str, object]) -> dict[str, object]:
        del payload
        engine_job = self._engine_job_or_none(job_id)
        if engine_job is None:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
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
                runner = self._get_engine_runner()
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
                atomic_write_json(runner.jobs_root / f"{engine_job.id}.json", retried.as_dict(), allow_nan=False)
                self._queue_provider_job(job_id)
                return self.public_engine_job(retried)
            if self._is_terminal_automatic_failure(engine_job):
                runner = self._get_engine_runner()
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
                atomic_write_json(
                    runner.jobs_root / f"{engine_job.id}.json",
                    retried.as_dict(),
                    allow_nan=False,
                )
                self._queue_automatic_job(job_id)
                return self.public_engine_job(retried)
            self._queue_automatic_job(job_id)
            return self.public_engine_job(self._get_engine_runner().get_job(job_id))

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
            "queued": "queued",
            "analyzing": "analyzing",
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
            "failed_identity": "failed_identity",
            "failed_provider": "failed_provider",
            "failed_write": "failed_write",
            "failed_verification": "failed_verification",
            "cancelled": "cancelled",
        }.get(job.phase, job.phase)
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
                    "staging_verifying", "child_planning", "child_executing",
                    "final_verifying", "cleaning", "child_failed", "retry_wait",
                }:
                    display_phase = "retry_wait" if provider_phase == "child_failed" else provider_phase
                elif provider_phase == "failed" and replenishment.get("terminal") is True:
                    display_phase = "failed_provider"
                else:
                    audit_message = None
        payload["phase"] = display_phase
        payload["engine_phase"] = job.phase
        payload["source"] = (
            "全库审计"
            if SimpleApplication._is_audit_owned_root(job)
            else summary.get("source_root")
        )
        payload["parent"] = summary.get("target_root")
        payload["media_type"] = summary.get("mode")
        plan_body = dict(job.plan)
        payload["plan"] = {
            "kind": "media",
            "title": summary.get("title"),
            "tmdb_id": summary.get("tmdb_id"),
            "source_root": summary.get("source_root"),
            "target_root": summary.get("target_root"),
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
            "completed": 1 if display_phase == "completed" else 0,
            "total": int(summary.get("file_count") or 0),
            "percent": 100 if display_phase == "completed" else provider_percent or 0,
            "message": (
                provider_message
                if provider_message is not None
                else "系统已生成计划，正在自动排队执行"
                if job.phase == "planned"
                else "系统正在等待下一次自动重试"
                if job.phase == "retry_wait"
                else "Engine 计划已执行并完成远端大小核验"
                if job.phase == "executed" and display_phase == "completed"
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
                "status": "verified" if display_phase == "completed" else "pending",
                "checked_at": job.updated_at if display_phase == "completed" else None,
                "message": (
                    "全库审计已确认缺口消失"
                    if display_phase == "completed"
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
        return payload

    def control(self) -> dict[str, object]:
        with self._control_lock:
            return self._read_control()

    def set_paused(self, paused: bool, reason: str | None = None) -> dict[str, object]:
        if not isinstance(paused, bool):
            raise TypeError("paused must be boolean")
        with self._control_lock:
            now = _now()
            payload = {
                "version": 1,
                "paused": paused,
                "scheduler_paused": paused,
                "persistent": True,
                "updated_at": now,
                "reason": (reason or "operator pause") if paused else None,
            }
            atomic_write_json(self._control_path, payload, allow_nan=False)
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
        try:
            self.set_paused(True, "shutdown")
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

    def _read_control(self) -> dict[str, object]:
        try:
            payload = json.loads(self._control_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {
                "paused": False,
                "scheduler_paused": False,
                "persistent": True,
                "updated_at": None,
                "reason": None,
            }
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApplicationError("控制状态不可读") from exc
        if not isinstance(payload, dict):
            raise ApplicationError("控制状态格式错误")
        return payload


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
                self._send(404, {"error": "not found"})
            else:
                self._send(404, {"error": "not found"})
        except Exception as exc:
            self._handle_error(exc)

    def _path(self) -> tuple[str, dict[str, list[str]]]:
        parsed = urllib.parse.urlsplit(self.path)
        return parsed.path.rstrip("/") or "/", urllib.parse.parse_qs(parsed.query)

    def _json_body(self) -> dict[str, object]:
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
        self._send(status, {"error": str(exc) or type(exc).__name__})

    def _send(self, status: int, payload: Mapping[str, object]) -> None:
        body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
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
