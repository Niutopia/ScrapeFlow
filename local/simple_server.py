#!/usr/bin/env python3
"""Single-user HTTP service for ScrapeFlow's automatic media workflow."""

from __future__ import annotations

import ipaddress
import json
import os
import posixpath
import re
import signal
import sys
import threading
import urllib.parse
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.archive import ArchiveLimits
from engine.scrapeflow.archive_preprocessing import ArchivePreprocessingAdapter
from engine.scrapeflow.target_shelf import (
    parse_target_shelf,
    target_root_for_shelf,
    target_shelf_values,
)
from local.web_dashboard import dashboard_html
from local.scrapeflow_api.root_aggregation import (
    aggregate_root_job,
    public_work_unit_row,
    public_work_unit_rows,
)
from local.scrapeflow_api.simple_engine_runner import (
    EngineCancellationRequested,
    EngineExecutionError,
    EnginePauseRequested,
    EngineJob,
    EngineJobConflictError,
    EngineJobNotFoundError,
    EngineRequestError,
    EngineWorkerBusyError,
    SimpleEngineError,
    SimpleEngineRunner,
    recover_persisted_engine_jobs,
)
from local.scrapeflow_api.control_state import PersistentControlState
from local.scrapeflow_api.batch_manifest import (
    BatchManifest,
    BatchManifestItem,
    BatchManifestValidationError,
    MAX_BATCH_ITEMS,
    load_batch_manifest,
    save_batch_manifest,
)
from local.scrapeflow_api.provider_staging import (
    PRODUCTION_MEDIA_ROOT,
    ProviderStagingPathError,
    validate_provider_media_root,
)
from local.scrapeflow_api.redaction import redact_error, redact_value

class ApplicationError(RuntimeError):
    """The automatic application cannot complete the requested operation."""


def _redacted_job_payload(job: EngineJob) -> dict[str, object]:
    """Return a safe persisted root-job document without mutating the job."""
    redacted = redact_value(job.as_dict())
    return dict(redacted) if isinstance(redacted, Mapping) else job.as_dict()


class SimpleApplication:
    """HTTP-facing composition root for automatic intake and delivery.

    ``remote`` is injectable for focused tests and local dry runs.  In a real
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
        archive_preprocessor: object | None = None,
    ) -> None:
        self.state_root = Path(state_root or os.getenv("SCRAPEFLOW_STATE_DIR", "/data")).resolve()
        self.state_root.mkdir(parents=True, exist_ok=True)
        if remote_root is None:
            configured_remote_root = os.getenv("SCRAPEFLOW_MEDIA_ROOT", PRODUCTION_MEDIA_ROOT)
            try:
                self.remote_root = validate_provider_media_root(configured_remote_root)
            except ProviderStagingPathError as exc:
                raise ApplicationError("SCRAPEFLOW_MEDIA_ROOT 必须是 /quark/影视") from exc
        else:
            # Explicit injection is only for focused tests and local dry runs;
            # the deployed application always uses the fixed production root.
            self.remote_root = _safe_remote_root(remote_root)
        candidate = remote if remote is not None else self._build_remote()
        self._alist_client = candidate
        self._engine_runner = engine_runner
        staging_prefix = f"{self.remote_root.rstrip('/')}/ScrapeFlow"
        local_staging_prefix = (self.state_root / "archive-staging").resolve()
        if archive_preprocessor is not None:
            self._archive_preprocessor = archive_preprocessor
        else:
            try:
                archive_limits = ArchiveLimits.from_environment()
            except ValueError as exc:
                raise ApplicationError("归档/光盘镜像安全限制配置无效") from exc
            self._archive_preprocessor = ArchivePreprocessingAdapter(
                limits=archive_limits,
                staging_root_validator=lambda path: (
                    path == staging_prefix or path.startswith(staging_prefix + "/")
                ),
                local_staging_root_validator=lambda path: (
                    Path(path).resolve() == local_staging_prefix
                    or local_staging_prefix in Path(path).resolve().parents
                ),
            )
        self._engine_runner_lock = threading.Lock()
        self._control_path = self.state_root / "global-control.json"
        self._control_state = PersistentControlState(self._control_path)
        # Each process starts paused. The only durable control state is the
        # two-field JSON record; no second-process hand-off is supported.
        self._control_state.set(paused=True)
        self._automatic_lock = threading.RLock()
        # One fixed local worker serializes both intake and replenishment.
        # There is deliberately one active slot, not per-lane futures, retry
        # timers, or a second control plane.
        self._worker_executor: ThreadPoolExecutor | None = None
        self._worker_future: Future[object] | None = None
        self._worker_root_job_id: str | None = None
        self._closed = threading.Event()
        self._intake_status: dict[str, object] = {
            "last_scan_at": None,
            "last_error": None,
            "last_registered_count": 0,
            "last_scan_empty": False,
        }
        self._recover_persisted_engine_jobs()
        # Crash-orphaned atomic-write temporaries are unreachable garbage;
        # at startup no writer can be mid-flight, so sweep them once.
        try:
            from engine.scrapeflow.serialization import sweep_stale_temporaries
            swept = sweep_stale_temporaries(self.state_root)
            if swept:
                print(f"[startup] 清理了 {swept} 个崩溃残留的临时文件", flush=True)
        except Exception:  # noqa: BLE001 - hygiene only, never blocks start
            pass

    def _recover_persisted_engine_jobs(self) -> None:
        """Recover Engine records without changing the operator control state."""
        try:
            recover_persisted_engine_jobs(self.state_root)
        except EngineWorkerBusyError:
            # The writer's local lock is still held by a previous operation.
            # Do not start another one; the user can resume after it settles.
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

    def _automatic_root_allowed(self, root_job_id: object) -> bool:
        """Allow effects only for the selected, unpaused RootJob."""
        control = self.control()
        return (
            control.get("paused") is False
            and isinstance(root_job_id, str)
            and root_job_id == control.get("root_job_id")
        )

    def _validate_selected_root_job(self, root_job_id: object) -> str:
        """Require one public RootJob that belongs to an intake source."""
        if (
            not isinstance(root_job_id, str)
            or re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}", root_job_id) is None
        ):
            raise EngineRequestError("root_job_id 无效")
        runner = self._get_engine_runner()
        job = runner.get_job(root_job_id)
        if self._is_internal_child(job):
            raise EngineRequestError("只能选择公开 RootJob，不能选择内部 child")
        if job.phase == "cancelled":
            raise EngineRequestError("任务已取消；请创建新的 RootJob 后再运行")
        from local.scrapeflow_api.root_pipeline import is_intake_bound_root
        if not is_intake_bound_root(self.state_root, root_job_id):
            raise EngineRequestError("只能选择 IntakeSource 绑定的 RootJob")
        # Ownership isolation (AGENTS.md §3): a catalog row that merely names
        # this id is not proof of ownership.  ``select`` is the only
        # authorization entry, so it must also prove the row describes the very
        # intake child this root will read.  Without the agreement check a
        # drifted or forged binding could point a selectable root at a formal
        # shelf, and B/W would then split the live library as if it were a
        # source.  Same rule the boundary rebuild already enforces.
        from engine.scrapeflow.intake_source import load_intake_catalog
        try:
            ingress = runner._job_ingress_source(job)  # noqa: SLF001 - exact ingress
            ingress = self._validate_automatic_source(ingress)
            owners = [
                item
                for item in load_intake_catalog(self.state_root)
                if item.root_task_id == root_job_id
                and item.canonical_path == ingress
            ]
        except EngineRequestError:
            raise
        except Exception as exc:
            raise EngineRequestError("RootJob 的来源归属无法验证") from exc
        if len(owners) != 1:
            raise EngineRequestError("RootJob 与待刮削来源绑定不一致，不能选择")
        return root_job_id

    def select_root_job(self, root_job_id: object) -> dict[str, object]:
        """Select one RootJob and keep the scheduler paused."""
        # Keep control mutations serialized with narrow paused-only recovery
        # actions.  There is one API process by contract, so this RLock makes
        # the endpoint's final paused/selected check and its local B/W commit
        # indivisible relative to select/resume/pause/cancel requests.
        with self._automatic_lock:
            selected = self._validate_selected_root_job(root_job_id)
            return self._control_state.set(paused=True, root_job_id=selected)

    def clear_orphan_selection(self) -> dict[str, object]:
        """Clear only a paused selection whose RootJob record is gone."""
        with self._automatic_lock:
            control = self.control()
            selected = control.get("root_job_id")
            if control.get("paused") is not True or not isinstance(selected, str):
                raise EngineRequestError("只能清除 paused 状态下的孤儿选择")
            if self._worker_future is not None and not self._worker_future.done():
                raise EngineWorkerBusyError("仍有活动 worker，不能清除孤儿选择")
            runner = self._get_engine_runner()
            try:
                runner.get_job(selected)
            except EngineJobNotFoundError:
                return self._control_state.set(paused=True, root_job_id=None)
            raise EngineJobConflictError("当前选择仍指向存在的 RootJob，拒绝清除")

    def _resume_after_control_open(self) -> None:
        if self._closed.is_set():
            return
        self._resume_automatic_jobs()

    def resume_selected_root_job(self, root_job_id: object | None = None) -> dict[str, object]:
        """Resume the selected RootJob, optionally selecting it in this call.

        The response reports whether dispatch actually happened: the lone
        worker slot may still be finishing the previous root, in which case
        ``dispatched`` is false with a reason instead of a silent no-op the
        operator only discovers when nothing runs.
        """
        with self._automatic_lock:
            control = self.control()
            selected = control.get("root_job_id") if root_job_id is None else root_job_id
            selected = self._validate_selected_root_job(selected)
            payload = self._control_state.set(paused=False, root_job_id=selected)
            active = self._worker_future
            busy = (
                self._worker_root_job_id is not None
                and active is not None
                and not active.done()
            )
        if busy and self._worker_root_job_id != selected:
            payload = {
                **payload,
                "dispatched": False,
                "reason": (
                    f"worker 仍在收尾上一任务 {self._worker_root_job_id}；"
                    "当前任务已选中未暂停，将在其结束后由下一次恢复派工"
                ),
            }
            return payload
        self._resume_after_control_open()
        return {**payload, "dispatched": True}

    # ------------------------------------------------------------------
    # Phase 1: Intake catalog
    # ------------------------------------------------------------------

    def intake_catalog(self) -> list[dict[str, object]]:
        """Return the persisted IntakeSource catalog as a list of plain dicts.

        This is the read-only view exposed by ``GET /api/intake``.  It never
        triggers a network scan; the operator refreshes the catalog explicitly.
        """
        from engine.scrapeflow.intake_source import load_intake_catalog
        sources = load_intake_catalog(self.state_root)
        result: list[dict[str, object]] = []
        for src in sources:
            d = src.as_dict()
            # Enrich with the target_shelf from the linked EngineJob when
            # available so the Web UI can show the selection state.
            if src.root_task_id is not None and self.engine_configured:
                try:
                    runner = self._get_engine_runner()
                    job = runner._read(src.root_task_id)  # noqa: SLF001
                    # Intake is a read-only catalog, but its linked phase must
                    # reflect durable Gap/WorkUnit reconciliation rather than
                    # the stale EngineJob terminal phase alone.
                    public = self.public_engine_job(job)
                    d["root_job_phase"] = public.get("phase", job.phase)
                    d["root_job_target_shelf"] = public.get(
                        "target_shelf", job.target_shelf,
                    )
                except Exception:
                    pass
            result.append(d)
        return result

    def refresh_intake_catalog(self) -> dict[str, object]:
        """Freshly read ``/待刮削`` from AList for an operator-requested refresh.

        This is deliberately narrower than an ordinary monitor heartbeat: it
        updates only the local IntakeSource catalog and returns the new view.
        It never creates a RootJob, performs identity matching, or starts a
        download.
        """
        registered = self._scan_inbound_once()
        with self._automatic_lock:
            # ``last_scan_at`` is written only after the fresh AList root
            # listing completes.  Returning it lets the Web UI distinguish a
            # real catalog refresh from its own local render time.
            refreshed_at = self._intake_status.get("last_scan_at")
        return {
            "sources": self.intake_catalog(),
            "registered": registered,
            "refreshed_at": refreshed_at,
        }

    # ------------------------------------------------------------------
    # Authorized batch queue
    # ------------------------------------------------------------------

    @staticmethod
    def _batch_now() -> str:
        """Return the queue's deliberately bounded fresh-inspection time."""
        return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _load_batch_manifest(self) -> BatchManifest:
        """Read the passive authorization queue without changing control state."""
        try:
            return load_batch_manifest(self.state_root)
        except BatchManifestValidationError as exc:
            # A malformed authorization file must stop before any external
            # effect.  Do not replace it with an empty queue and thereby lose
            # the operator's original intended order.
            raise ApplicationError("批次清单无效，已安全停止") from exc

    def _save_batch_manifest(self, manifest: BatchManifest) -> BatchManifest:
        try:
            save_batch_manifest(self.state_root, manifest)
        except (OSError, BatchManifestValidationError) as exc:
            raise ApplicationError("批次清单无法安全保存") from exc
        return manifest

    def batch_manifest_view(self) -> dict[str, object]:
        """Return the queue only; it is not a second scheduler/control plane."""
        return {"batch": self._load_batch_manifest().as_dict()}

    def persist_replacement_manifest(self, manifest: object) -> dict[str, object]:
        """Persist one already server-generated replacement proof.

        The HTTP surface does not accept this object.  Composition code may
        call the hook after fresh source/library/TMDB validation; this final
        boundary proves the RootJob and WorkUnit ownership again before local
        persistence and never starts a second writer.
        """
        from engine.scrapeflow.replacement import (
            ReplacementManifest,
            ReplacementValidationError,
            derive_replacement_archive_root,
            save_replacement_manifest,
        )
        from engine.scrapeflow.work_units import load_work_unit_records
        if not isinstance(manifest, ReplacementManifest):
            raise EngineRequestError("replacement manifest 必须由服务端生成")
        with self._automatic_lock:
            if self.control().get("paused") is not True:
                raise EngineRequestError("replacement manifest 只能在暂停边界持久化")
            if self.control().get("root_job_id") != manifest.root_job_id:
                raise EngineRequestError("replacement manifest 只能绑定当前选中的 RootJob")
            # A completed Future has not necessarily run its callback and
            # released the application slot yet.  Keep this boundary strict
            # so a manifest cannot be persisted against a root that is still
            # being reconciled by the lone writer.
            if self._worker_future is not None:
                raise EngineWorkerBusyError("writer slot 尚未释放，不能持久化 replacement manifest")
            runner = self._get_engine_runner()
            job = runner.get_job(manifest.root_job_id)
            from local.scrapeflow_api.root_pipeline import is_intake_bound_root
            if not is_intake_bound_root(self.state_root, manifest.root_job_id):
                raise EngineRequestError("replacement RootJob 未绑定 IntakeSource")
            from engine.scrapeflow.intake_source import find_by_path, load_intake_catalog

            bound_source = find_by_path(
                load_intake_catalog(self.state_root), manifest.source_root,
            )
            if (
                bound_source is None
                or not bound_source.present
                or bound_source.root_task_id != manifest.root_job_id
            ):
                raise EngineJobConflictError("replacement 来源未精确绑定到该 RootJob")
            try:
                ingress_source = str(runner._job_ingress_source(job))  # noqa: SLF001 - final ownership proof
            except Exception as exc:
                raise EngineJobConflictError("replacement RootJob ingress 无法验证") from exc
            if ingress_source != manifest.source_root:
                raise EngineJobConflictError("replacement 来源与 RootJob ingress 不一致")
            try:
                owned_job = runner.find_by_source(manifest.source_root)
            except Exception as exc:
                raise EngineJobConflictError("replacement 来源所有权无法验证") from exc
            if owned_job is None or owned_job.id != manifest.root_job_id:
                raise EngineJobConflictError("replacement 来源被其他 RootJob 占用")
            records = load_work_unit_records(self.state_root, manifest.root_job_id)
            record = next((row for row in records if row.work_unit_id == manifest.work_unit_id), None)
            if record is None:
                raise EngineRequestError("replacement WorkUnit 不属于该 RootJob")
            if record.root_task_id != manifest.root_job_id:
                raise EngineJobConflictError("replacement WorkUnit 根任务不一致")
            if record.reconciliation_outcome != "merge_existing":
                raise EngineJobConflictError("replacement 只能叠加到已对账锁定的既有作品根")
            identity = record.identity
            if record.identity_status != "confirmed" or not isinstance(identity, Mapping):
                raise EngineJobConflictError("replacement WorkUnit 尚未确认 TMDB 身份")
            identity_tmdb = identity.get("tmdb_id")
            identity_media_type = str(identity.get("media_type") or "").casefold()
            if (
                isinstance(identity_tmdb, bool)
                or identity_tmdb != manifest.tmdb_id
                or identity_media_type != manifest.media_type
            ):
                raise EngineJobConflictError("replacement manifest 与 WorkUnit 确认身份不一致")
            if not record.matched_work_root or record.matched_work_root != manifest.target_work_root:
                raise EngineJobConflictError("replacement 目标必须锁定既有 work_root")
            try:
                expected_archive_root = derive_replacement_archive_root(
                    runner.library_root,
                    manifest.target_work_root,
                    manifest.root_job_id,
                )
            except ReplacementValidationError as exc:
                raise EngineJobConflictError("replacement archive 根无法从正式库配置派生") from exc
            if (
                manifest.library_root != runner.library_root
                or manifest.archive_root != expected_archive_root
            ):
                raise EngineJobConflictError("replacement archive 根不是正式库派生路径")
            try:
                save_replacement_manifest(self.state_root, manifest)
            except ReplacementValidationError as exc:
                raise EngineRequestError("replacement manifest 校验失败") from exc
        return {"replacement": manifest.as_dict()}

    def replacement_manifest_view(self, manifest_id: str) -> dict[str, object]:
        from engine.scrapeflow.replacement import load_replacement_manifest
        try:
            manifest = load_replacement_manifest(self.state_root, manifest_id)
        except Exception as exc:
            raise EngineRequestError("replacement manifest 不可读取") from exc
        return {"replacement": manifest.as_dict()}

    @staticmethod
    def _batch_item_payload(
        raw: object,
        *,
        default_sort: int | None = None,
    ) -> tuple[str, str | None, int]:
        """Accept just source identity, shelf and order from the local UI.

        In particular the browser cannot supply a source path, RootJob id,
        arbitrary target, current state, or fresh result.  All of those are
        server-derived to preserve the batch authorization's narrow scope.
        """
        if not isinstance(raw, Mapping):
            raise EngineRequestError("批次项必须是对象")
        allowed = {"source_id", "shelf", "target_shelf", "sort"}
        if set(raw) - allowed or "source_id" not in raw:
            raise EngineRequestError("批次项只接受 source_id、shelf、target_shelf 与 sort")
        source_id = raw.get("source_id")
        # ``shelf`` is intentionally optional.  A missing target is a valid
        # authorization record, but it can only become ``needs_attention``
        # during fresh inspection; it must never be guessed from a path.
        has_shelf = "shelf" in raw
        has_target_shelf = "target_shelf" in raw
        if has_shelf and has_target_shelf and raw.get("shelf") != raw.get("target_shelf"):
            raise EngineRequestError("shelf 与 target_shelf 不能同时指定不同值")
        shelf = raw.get("shelf") if has_shelf else raw.get("target_shelf")
        sort = raw.get("sort")
        if not isinstance(source_id, str) or not source_id:
            raise EngineRequestError("source_id 必须是非空字符串")
        if shelf is None:
            parsed_shelf = None
        else:
            try:
                parsed_shelf = parse_target_shelf(shelf).value
            except ValueError as exc:
                raise EngineRequestError(str(exc)) from exc
        if sort is None and default_sort is not None:
            sort = default_sort
        if isinstance(sort, bool) or not isinstance(sort, int) or sort < 0:
            raise EngineRequestError("sort 必须是非负整数")
        return source_id, parsed_shelf, sort

    def authorize_batch_items(self, payload: Mapping[str, object]) -> dict[str, object]:
        """Append user-authorized catalog sources to the durable queue.

        This only records authorization.  It first refreshes the passive
        IntakeSource catalog so a stale ID/path cannot be enqueued, but does
        not create a RootJob, match metadata or schedule a writer.
        """
        if not isinstance(payload, Mapping) or set(payload) != {"items"} or not isinstance(payload.get("items"), list):
            raise EngineRequestError("批次授权只接受 items 数组")
        raw_items = payload["items"]
        if len(raw_items) > MAX_BATCH_ITEMS:
            raise EngineRequestError("批次项数量超过安全上限")
        # Discover before accepting an ID.  The scan is read-only except for
        # the existing IntakeSource observation record.
        try:
            self._scan_inbound_once()
        except Exception as exc:
            raise ApplicationError("无法 fresh 枚举待刮削目录") from exc
        from engine.scrapeflow.intake_source import find_by_source_id, load_intake_catalog

        catalog = load_intake_catalog(self.state_root)
        existing_manifest = self._load_batch_manifest()
        next_sort = max((item.sort for item in existing_manifest.items), default=-1) + 1
        parsed = []
        for raw in raw_items:
            parsed.append(self._batch_item_payload(raw, default_sort=next_sort))
            if isinstance(raw, Mapping) and raw.get("sort") is None:
                next_sort += 1
        if len({source_id for source_id, _shelf, _sort in parsed}) != len(parsed):
            raise EngineRequestError("同一来源不能在一次批次授权中重复出现")
        if len({sort for _source_id, _shelf, sort in parsed}) != len(parsed):
            raise EngineRequestError("批次排序不能重复")
        with self._automatic_lock:
            manifest = self._load_batch_manifest()
            # Re-read the passive catalog under the same local lock as the
            # manifest append so a concurrent refresh cannot authorize a stale
            # path/binding observation.
            catalog = load_intake_catalog(self.state_root)
            for source_id, shelf, sort in parsed:
                source = find_by_source_id(catalog, source_id)
                if source is None or not source.present:
                    raise EngineRequestError("来源不在当前待刮削目录，拒绝授权")
                if source.root_task_id is not None:
                    # The catalog binding is a durable consumption claim.  A
                    # finished historical root must not become batch evidence
                    # or get silently reopened by a new item.
                    raise EngineJobConflictError("来源已被历史 RootJob 占用，不能加入新批次")
                try:
                    manifest = manifest.add(BatchManifestItem(
                        source_id=source.source_id,
                        source_path_snapshot=source.canonical_path,
                        shelf=shelf,
                        sort=sort,
                        state="scheduled",
                    ))
                except BatchManifestValidationError as exc:
                    raise EngineRequestError("批次来源或排序已存在，不能重复授权") from exc
            self._save_batch_manifest(manifest)
        return {"batch": manifest.as_dict()}

    def retry_batch_item(self, payload: Mapping[str, object]) -> dict[str, object]:
        """Explicitly re-inspect one parked batch item.

        Retry is deliberately source-id based.  An unbound item returns to a
        fresh inspection normally.  A bound item may return only when it can
        prove it still owns the same paused, selected RootJob; it is never
        re-created as a second RootJob.
        """
        if not isinstance(payload, Mapping):
            raise EngineRequestError("批次重试请求必须是对象")
        allowed = {"source_id", "shelf", "target_shelf"}
        if set(payload) - allowed or "source_id" not in payload:
            raise EngineRequestError("批次重试只接受 source_id 与可选 shelf")
        source_id = payload.get("source_id")
        if not isinstance(source_id, str) or not source_id:
            raise EngineRequestError("source_id 必须是非空字符串")
        shelf_supplied = "shelf" in payload or "target_shelf" in payload
        if "shelf" in payload and "target_shelf" in payload and payload.get("shelf") != payload.get("target_shelf"):
            raise EngineRequestError("shelf 与 target_shelf 不能同时指定不同值")
        raw_shelf = payload.get("shelf") if "shelf" in payload else payload.get("target_shelf")
        if shelf_supplied and raw_shelf is not None:
            try:
                parsed_shelf: str | None = parse_target_shelf(raw_shelf).value
            except ValueError as exc:
                raise EngineRequestError(str(exc)) from exc
        else:
            parsed_shelf = None
        from engine.scrapeflow.intake_source import find_by_source_id, load_intake_catalog

        with self._automatic_lock:
            manifest = self._load_batch_manifest()
            item = manifest.find(source_id)
            if item is None:
                raise EngineRequestError("批次来源不存在")
            if item.state not in {"needs_attention", "technical_failure", "skipped_currently_nonmedia"}:
                raise EngineRequestError("当前批次状态不允许重试")
            source = find_by_source_id(load_intake_catalog(self.state_root), source_id)
            if source is None or not source.present or source.canonical_path != item.source_path_snapshot:
                raise EngineRequestError("来源已不存在或路径发生变化")
            next_shelf = parsed_shelf if shelf_supplied else item.shelf
            if source.root_task_id is not None:
                if next_shelf != item.shelf:
                    raise EngineJobConflictError("已绑定 RootJob 的批次重试不能更改货架")
                # This checks all ownership/control/phase evidence before we
                # erase the old attention result.  Re-running the source is a
                # separate explicit S-step authorization; this call only proves
                # the queue record may be reset.
                self._bound_batch_retry_job(item, expected_root_id=source.root_task_id)
            if item.state == "skipped_currently_nonmedia":
                # This state has no direct scheduled edge; pass through the
                # mandated fresh-inspection boundary and then clear evidence.
                manifest = manifest.transition(source_id, "inspecting")
                manifest = manifest.transition(source_id, "scheduled")
            else:
                manifest = manifest.transition(source_id, "scheduled")
            if next_shelf != item.shelf:
                current = manifest.find(source_id)
                assert current is not None
                manifest = manifest.replace(replace(current, shelf=next_shelf))
            self._save_batch_manifest(manifest)
        return {"batch": manifest.as_dict()}

    def _bound_batch_retry_job(
        self,
        item: BatchManifestItem,
        *,
        expected_root_id: str | None = None,
    ) -> EngineJob:
        """Prove that a parked item can resume its *existing* RootJob.

        This deliberately accepts only the two raw root states whose public
        retry path is designed to reopen durable unit records.  It is a
        proof helper, not a scheduler: callers still fresh-inspect the
        source and persist ``ready -> active`` before invoking the ordinary
        retry/queue chain.
        """
        if item.state not in {"needs_attention", "technical_failure", "ready", "active"}:
            raise EngineJobConflictError("当前批次状态不能复用已绑定 RootJob")
        if item.shelf is None:
            raise EngineJobConflictError("已绑定 RootJob 的批次项缺少货架")
        if self._worker_future is not None:
            raise EngineWorkerBusyError("worker slot 尚未释放，不能重试已绑定 RootJob")
        control = self.control()
        if control.get("paused") is not True:
            raise EngineJobConflictError("已绑定 RootJob 的批次重试必须在暂停边界执行")

        from engine.scrapeflow.intake_source import find_by_source_id, load_intake_catalog
        from local.scrapeflow_api.root_pipeline import is_intake_bound_root

        source = find_by_source_id(load_intake_catalog(self.state_root), item.source_id)
        root_id = source.root_task_id if source is not None else None
        if (
            source is None
            or not source.present
            or source.canonical_path != item.source_path_snapshot
            or not isinstance(root_id, str)
            or (expected_root_id is not None and root_id != expected_root_id)
        ):
            raise EngineJobConflictError("批次来源与 RootJob 绑定已变化")
        if control.get("root_job_id") != root_id:
            raise EngineJobConflictError("请先选择该批次来源绑定的 RootJob")
        if not is_intake_bound_root(self.state_root, root_id):
            raise EngineJobConflictError("批次来源绑定的任务不是 Intake RootJob")

        runner = self._get_engine_runner()
        try:
            job = runner.get_job(root_id)
            ingress = str(runner._job_ingress_source(job))  # noqa: SLF001 - ownership proof
            owner = runner.find_by_source(item.source_path_snapshot)
        except Exception as exc:
            raise EngineJobConflictError("批次 RootJob 所有权无法验证") from exc
        if ingress != item.source_path_snapshot or owner is None or owner.id != root_id:
            raise EngineJobConflictError("批次来源不再精确归属该 RootJob")
        try:
            expected_target_root = target_root_for_shelf(runner.library_root, item.shelf)
        except (TypeError, ValueError) as exc:
            raise EngineJobConflictError("批次 RootJob 的正式库货架无效") from exc
        if job.target_shelf != item.shelf or job.target_root != expected_target_root:
            raise EngineJobConflictError("批次 RootJob 的货架或目标根与授权不一致")
        if runner.cancellation_pending(root_id):
            raise EngineJobConflictError("RootJob 已被取消，不能由批次重试复用")
        if job.phase not in {"reconciliation_uncertain", "failed", "queued"}:
            raise EngineJobConflictError("RootJob 当前阶段不允许批次复用重试")
        if job.phase == "queued" and item.state != "active":
            # A queued root may be resumed only from the durable active
            # crash-recovery record.  A parked technical item cannot claim a
            # historical queued root just because it shares a source path.
            raise EngineJobConflictError("非 active 批次项不能复用已排队 RootJob")
        self._require_prewrite_batch_retry_evidence(job, root_id)
        # Media count alone cannot prove a retry still refers to the same
        # B/W object set.  Rebuild a bounded fresh SourceManifest directly
        # against the existing root proof; any added, removed, renamed,
        # resized or version-changed source object stops this source rather
        # than letting an old WorkUnit record write a new release tree.
        try:
            from engine.scrapeflow.root_boundaries import load_source_manifest, walk_source_rows
            from engine.scrapeflow.source_objects import SourceManifest

            expected_manifest = load_source_manifest(self.state_root, root_id)
            if expected_manifest is not None and expected_manifest.root_path != item.source_path_snapshot:
                raise EngineJobConflictError("RootJob 精确来源对象清单的根路径不一致")
            runner._ensure_authenticated(runner.alist)  # noqa: SLF001 - guarded read-only freshness
            fresh_manifest = SourceManifest.from_listing_rows(
                walk_source_rows(runner.alist, item.source_path_snapshot),
                root_path=item.source_path_snapshot,
                snapshot_id=f"batch-retry:{root_id}:{uuid.uuid4().hex}",
            )
            if expected_manifest is None:
                # A process can stop between the S-step RootJob creation and
                # its first B/W snapshot.  That pre-B/W boundary has no old
                # object set to compare, but it is safe only when there is
                # likewise no WorkUnit, plan, H/J, provider or writer fact.
                from engine.scrapeflow.gap_ledger import load_gap_ledger
                from engine.scrapeflow.work_units import load_work_unit_records
                from local.scrapeflow_api.unit_execution import load_work_acceptance

                if (
                    load_work_unit_records(self.state_root, root_id)
                    or bool(job.plan)
                    or job.execution is not None
                    or load_work_acceptance(self.state_root, root_id)
                    or load_gap_ledger(self.state_root, root_id)
                    or (self.state_root / f"replenishment_{root_id}.json").exists()
                ):
                    raise EngineJobConflictError("RootJob 缺少精确来源清单且已越过首次 B/W 边界")
            else:
                expected_manifest.require_fresh_match(fresh_manifest)
        except EngineJobConflictError:
            raise
        except Exception as exc:
            raise EngineJobConflictError("RootJob 精确来源对象清单已漂移或不可读取") from exc
        return job

    def _require_prewrite_batch_retry_evidence(
        self,
        job: EngineJob,
        root_id: str,
    ) -> None:
        """Reject a batch replay once any writer-facing fact exists.

        A root may be ``reconciliation_uncertain`` after C/U, not just before
        it.  It may also be left ``queued`` in the crash window following a
        retry transition.  Neither raw phase proves that no E/F/G/H/J/N side
        effect was begun.  This coordinator is intentionally narrower than a
        root-level recovery: it reopens only a demonstrably pre-write root and
        otherwise leaves the exact carrier for the normal recovery surface.
        """
        from engine.scrapeflow.gap_ledger import _load_gap_ledger_strict
        from engine.scrapeflow.work_units import load_work_unit_records
        from local.scrapeflow_api.unit_execution import WorkAcceptanceResult

        if bool(job.plan) or job.execution is not None:
            raise EngineJobConflictError("RootJob 已有计划或执行载体，不能由批次自动重开")
        records = load_work_unit_records(self.state_root, root_id)
        for record in records:
            # ``uncertain`` is a C/D attention fact and can be retried by the
            # normal root pipeline.  Any resolved D outcome, lane receipt,
            # writer carrier, J status, target lock, or layout repair can
            # already imply a formal-library effect and must stay outside the
            # batch coordinator's replay authority.
            if (
                record.writer_job_id is not None
                or record.lane_status is not None
                or record.lane_detail is not None
                or record.gap_status is not None
                or record.gap_detail is not None
                or record.reconciliation_outcome not in {None, "uncertain"}
                or record.matched_work_root is not None
                or record.reconciliation_evidence is not None
                or bool(record.uncovered_tokens)
                or record.layout_repair is not None
            ):
                raise EngineJobConflictError("RootJob 已有对账、写入或缺口证据，不能由批次自动重开")

        # The public acceptance loader deliberately tolerates legacy/corrupt
        # dashboard state.  A retry decision cannot: a present but malformed
        # receipt is evidence of uncertainty, never an empty receipt.
        acceptance_path = self.state_root / f"work_acceptance_{root_id}.json"
        acceptance: list[WorkAcceptanceResult] = []
        if acceptance_path.exists() or acceptance_path.is_symlink():
            if acceptance_path.is_symlink() or not acceptance_path.is_file():
                raise EngineJobConflictError("RootJob 验收记录不安全或不可读取")
            try:
                raw_acceptance = json.loads(acceptance_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise EngineJobConflictError("RootJob 验收记录无法验证") from exc
            if not isinstance(raw_acceptance, list):
                raise EngineJobConflictError("RootJob 验收记录格式无效")
            seen_units: set[str] = set()
            for raw_row in raw_acceptance:
                if not isinstance(raw_row, Mapping):
                    raise EngineJobConflictError("RootJob 验收记录包含无效条目")
                try:
                    row = WorkAcceptanceResult.from_dict(raw_row)
                except (KeyError, TypeError, ValueError) as exc:
                    raise EngineJobConflictError("RootJob 验收记录包含无法验证的条目") from exc
                if not row.work_unit_id or row.work_unit_id in seen_units:
                    raise EngineJobConflictError("RootJob 验收记录包含重复条目")
                seen_units.add(row.work_unit_id)
                acceptance.append(row)
        if job.phase == "reconciliation_uncertain":
            if acceptance:
                raise EngineJobConflictError("不确定 RootJob 已有验收记录，不能由批次自动重开")
        elif any(
            row.writer_job_id is not None
            or row.planned_files > 0
            or row.phase not in {"failed", "pending"}
            or row.target_root
            for row in acceptance
        ):
            raise EngineJobConflictError("失败或排队 RootJob 已有写入验收证据，不能由批次自动重开")

        try:
            # Unlike the tolerant aggregation loader, the strict reader
            # makes a damaged present ledger a retry barrier.
            if _load_gap_ledger_strict(self.state_root, root_id):
                raise EngineJobConflictError("RootJob 已有缺口账本，不能由批次自动重开")
        except EngineJobConflictError:
            raise
        except Exception as exc:
            raise EngineJobConflictError("RootJob 缺口账本无法验证") from exc

        replenishment_path = self.state_root / f"replenishment_{root_id}.json"
        if replenishment_path.exists() or replenishment_path.is_symlink():
            raise EngineJobConflictError("RootJob 已有补源状态，不能由批次自动重开")

        # A child carrier can exist in the plan→record crash window before a
        # WorkUnit receives its writer_job_id.  List the runner's durable
        # internal carriers rather than trusting the WorkUnit projection.
        try:
            carriers = self._get_engine_runner().list_jobs()
        except Exception as exc:
            raise EngineJobConflictError("RootJob 内部载体无法验证") from exc
        for carrier in carriers:
            if carrier.id == root_id:
                continue
            summary = carrier.summary if isinstance(carrier.summary, Mapping) else {}
            if summary.get("internal_child") is True and summary.get("root_job_id") == root_id:
                raise EngineJobConflictError("RootJob 已有内部写入载体，不能由批次自动重开")

    def health(self) -> dict[str, object]:
        """Return local API state without probing external services."""
        control = self.control()
        with self._automatic_lock:
            intake = dict(self._intake_status)
        return {
            "ok": True,
            "status": "ok",
            "connected": self.remote_configured,
            "tmdb_configured": bool(os.getenv("TMDB_API_KEY", "").strip()),
            "engine_configured": self.engine_configured,
            "control": {
                "paused": control.get("paused") is True,
                "root_job_id": control.get("root_job_id"),
            },
            "intake": {
                "root": f"{self.remote_root.rstrip('/')}/待刮削",
                "last_scan_at": intake.get("last_scan_at"),
                "last_error": intake.get("last_error"),
            },
            "operations": self._operations_summary(),
            "message": "API 可响应；外部操作失败会记录在对应任务中。",
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
            "reconciling", "queued", "analyzing", "archive_preprocessing", "identity_matching", "planning", "planned",
            "executing", "verifying", "cleaning", "retry_wait", "gaps_pending",
        }
        failed_engine_phases = {
            "failed", "failed_archive", "failed_identity", "failed_planning", "failed_provider", "failed_write",
            "failed_verification", "failed_cleanup",
        }
        with self._automatic_lock:
            worker_busy = int(
                self._worker_future is not None and not self._worker_future.done()
            )
        public_phases = []
        for job in engine_jobs:
            try:
                public_phases.append(str(self.public_engine_job(job).get("phase") or job.phase))
            except Exception:
                public_phases.append(job.phase)
        active_public_phases = active_engine_phases
        return {
            "jobs_total": len(engine_jobs),
            "jobs_awaiting_target_shelf": sum(
                1 for phase in public_phases if phase == "awaiting_target_shelf"
            ),
            "jobs_active": sum(1 for phase in public_phases if phase in active_public_phases),
            "jobs_failed": sum(1 for phase in public_phases if phase in failed_engine_phases),
            # Count roots only after both formal media acceptance and all J/N
            # gaps have closed.  A gap-pending root is deliberately not a
            # completed task even when its initial media is already verified.
            "jobs_completed": sum(
                1 for phase in public_phases
                if phase == "completed"
            ),
            "jobs_gaps_pending": sum(
                1 for phase in public_phases if phase == "gaps_pending"
            ),
            "worker_busy": min(1, worker_busy),
        }

    @staticmethod
    def _safe_inbound_name(value: object) -> str | None:
        if not isinstance(value, str) or not value or value in {".", ".."}:
            return None
        if "/" in value or "\\" in value or "\x00" in value:
            return None
        return value

    def _scan_inbound_once(self) -> list[str]:
        """Update the IntakeSource catalog for each direct child of ``/待刮削``.

        Discovery is a passive observation: it records the source with real
        child/file counts and never creates an EngineJob, never matches TMDB,
        and never schedules reconciliation.  Only a user-created RootJob may
        move a source past discovery (P2).  Loose files at the intake root are
        left untouched so they cannot be combined into one accidental work.
        This method reads AList with ``refresh=True`` and only writes the small
        local catalog record.  It intentionally runs while globally paused:
        pause blocks automatic work, not passive discovery.
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
        # ``seen_sources`` intentionally contains only admissible direct
        # directories.  Loose files remain visible to the operator but never
        # become an accidental RootJob.
        intake_objects_present = False
        for row in rows:
            if not isinstance(row, Mapping):
                intake_objects_present = True
                continue
            raw_name = row.get("name")
            if not isinstance(raw_name, str) or not raw_name.strip():
                intake_objects_present = True
                continue
            name = self._safe_inbound_name(raw_name)
            if name is None:
                intake_objects_present = True
                continue
            intake_objects_present = True
            if row.get("is_dir") is not True:
                continue
            source = f"{root}/{name}"
            seen_sources.add(source)

            # A step (discovery): update the IntakeSource catalog with real
            # child/file counts.  No EngineJob is created and nothing is
            # scheduled; the catalog is the only truth source before the
            # user creates a RootJob.
            try:
                from engine.scrapeflow.intake_source import (
                    load_intake_catalog,
                    save_intake_catalog,
                    upsert_intake_source,
                )
                try:
                    child_rows = listing(source, refresh=True)
                except TypeError:
                    child_rows = listing(source)
                child_count: int | None = None
                file_count: int | None = None
                if isinstance(child_rows, list):
                    child_count = sum(
                        1 for item in child_rows
                        if isinstance(item, Mapping) and item.get("is_dir") is True
                    )
                    file_count = sum(
                        1 for item in child_rows
                        if isinstance(item, Mapping) and item.get("is_dir") is not True
                    )
                _catalog = load_intake_catalog(self.state_root)
                _catalog, record = upsert_intake_source(
                    _catalog, source, present=True,
                    child_count=child_count, file_count=file_count,
                )
                save_intake_catalog(self.state_root, _catalog)
                if record.snapshot_revision == 0:
                    registered.append(source)
            except Exception:
                pass  # Catalog update is best-effort; the next scan retries it.
        # Generic staleness pass (A step): a catalog entry under the intake
        # root that this fresh listing no longer contains is a vanished
        # source.  Mark it missing without deleting the record, its history
        # or the root_task_id binding — a re-created same-path source revives
        # the entry.  Only run after a successful fresh listing.
        try:
            from engine.scrapeflow.intake_source import (
                load_intake_catalog,
                mark_source_missing,
                save_intake_catalog,
            )
            _cat = load_intake_catalog(self.state_root)
            stale_paths = [
                src.canonical_path
                for src in _cat
                if src.present
                and src.canonical_path.startswith(root.rstrip("/") + "/")
                and src.canonical_path not in seen_sources
            ]
            if stale_paths:
                for stale_path in stale_paths:
                    _cat, _ = mark_source_missing(_cat, stale_path)
                save_intake_catalog(self.state_root, _cat)
        except Exception:
            pass  # Best-effort; the next scan retries it.
        # A missing waiting source is an observation, not an instruction to
        # delete/retry/recreate it. Persist a clear error only after a
        # successful narrow listing of the intake root.
        marker = getattr(runner, "mark_waiting_source_missing", None)
        if callable(marker):
            for source, job in existing.items():
                if (
                    job.phase in {"awaiting_target_shelf", "reconciling"}
                    and source.startswith(root.rstrip("/") + "/")
                    and source not in seen_sources
                ):
                    try:
                        marker(job.id)
                    except Exception:
                        pass
                    # Phase 1: also mark the intake catalog entry missing.
                    try:
                        from engine.scrapeflow.intake_source import (
                            load_intake_catalog,
                            mark_source_missing,
                            save_intake_catalog,
                        )
                        _cat = load_intake_catalog(self.state_root)
                        _cat, _ = mark_source_missing(_cat, source)
                        save_intake_catalog(self.state_root, _cat)
                    except Exception:
                        pass
        with self._automatic_lock:
            self._intake_status.update({
                "last_scan_at": _now(),
                "last_error": None,
                "last_registered_count": len(registered),
                "last_scan_empty": not intake_objects_present,
            })
        return registered

    def _get_engine_runner(self) -> SimpleEngineRunner:
        runner = self._engine_runner
        if runner is not None:
            bind_pause = getattr(runner, "set_pause_requested", None)
            if callable(bind_pause):
                bind_pause(self._pause_requested)
            return runner
        with self._engine_runner_lock:
            runner = self._engine_runner
            if runner is not None:
                bind_pause = getattr(runner, "set_pause_requested", None)
                if callable(bind_pause):
                    bind_pause(self._pause_requested)
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
            bind_pause = getattr(runner, "set_pause_requested", None)
            if callable(bind_pause):
                bind_pause(self._pause_requested)
            return runner

    def _pause_requested(self) -> bool:
        """Fail closed unless this process is explicitly and currently resumed."""
        try:
            return self.control().get("paused") is not False
        except Exception:
            return True

    def _root_pause_requested(self, root_job_id: object) -> bool:
        """Pause every effect outside the selected, running RootJob."""
        if self._pause_requested() or not self._automatic_root_allowed(root_job_id):
            return True
        if not isinstance(root_job_id, str):
            return True
        runner = self._engine_runner
        if runner is None:
            return True
        try:
            return runner.cancellation_pending(root_job_id)
        except (EngineJobNotFoundError, SimpleEngineError):
            return True

    @staticmethod
    def _consume_root_cancellation(
        runner: SimpleEngineRunner,
        root_job_id: str,
    ) -> EngineJob | None:
        """Commit a root's pending cancel marker after its worker stops."""
        try:
            return runner.consume_cancellation(root_job_id)
        except (EngineJobNotFoundError, SimpleEngineError):
            return None

    def _worker_pool(self) -> ThreadPoolExecutor:
        with self._automatic_lock:
            if self._worker_executor is None:
                self._worker_executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="scrapeflow-worker",
                )
            return self._worker_executor

    def _clear_worker_slot(self, future: Future[object]) -> None:
        """Release the one local worker slot after its task has stopped."""
        finished_root: str | None = None
        with self._automatic_lock:
            if self._worker_future is future:
                finished_root = self._worker_root_job_id
                self._worker_future = None
                self._worker_root_job_id = None
        # The worker slot is released above.  A finished RootJob never starts
        # another one: the next source is authorized explicitly through the S
        # step (``/api/control/select``), so this callback performs no queue
        # advance of its own.
        del finished_root

    def _queue_selected_work(
        self,
        root_job_id: str,
        worker: Callable[[str], None],
    ) -> str:
        """Run one selected RootJob now, or report that the lone worker is busy."""
        if self._closed.is_set():
            return "closed"
        if not self._automatic_root_allowed(root_job_id):
            return "paused-or-unselected"
        with self._automatic_lock:
            active = self._worker_future
            # A completed Future still owns the slot until its callback has
            # cleared the paired root id and reconciled the active batch
            # item.  Submitting over that narrow window would overwrite the
            # Future reference and allow two completion paths to race.
            if active is not None:
                return "already-running"
            future = self._worker_pool().submit(worker, root_job_id)
            self._worker_future = future
            self._worker_root_job_id = root_job_id
            future.add_done_callback(self._clear_worker_slot)
        return "queued"

    @staticmethod
    def _is_internal_child(job: EngineJob) -> bool:
        """Return whether a persisted Engine record belongs to a root job.

        Provider children are restartable implementation records, not a second
        intake item.  Keep this check based on the explicit durable marker;
        inferring it from a staging path would make an old or renamed source
        look like a child by accident.
        """
        return isinstance(job.summary, Mapping) and job.summary.get("internal_child") is True

    def _queue_automatic_job(self, job_id: str) -> str:
        """Queue the selected RootJob's normal intake pipeline once."""
        if self._closed.is_set() or not self._automatic_root_allowed(job_id):
            return "paused-or-unselected"
        try:
            from local.scrapeflow_api.root_pipeline import is_intake_bound_root

            job = self._get_engine_runner().get_job(job_id)
            if self._is_internal_child(job) or not is_intake_bound_root(self.state_root, job_id):
                return "not-a-root-job"
        except (EngineJobNotFoundError, SimpleEngineError):
            return "not-found"
        return self._queue_selected_work(job_id, self._run_automatic_job)

    def _completed_root_needs_planner_gap_rereview(self, root_task_id: str) -> bool:
        """Read whether an old executed carrier has a strict unledgered season.

        This check is local-state only.  It deliberately does not turn a
        broad historical display label into authority to reopen a completed
        RootJob.
        """
        try:
            from local.scrapeflow_api.unit_execution import (
                has_unmaterialized_planner_season_gaps,
            )

            return has_unmaterialized_planner_season_gaps(
                self._get_engine_runner(),
                self.state_root,
                root_task_id,
            )
        except Exception:
            return False

    def _queue_completed_root_j_rereview(self, root_task_id: str) -> str:
        """Queue the narrow J-only repair for one selected completed root."""
        if self._closed.is_set():
            return "closed"
        if not self._automatic_root_allowed(root_task_id):
            return "paused-or-unselected"
        if not self._completed_root_needs_planner_gap_rereview(root_task_id):
            return "no-planner-gap-rereview"
        return self._queue_selected_work(root_task_id, self._run_completed_root_j_rereview)

    def _record_automatic_failure(self, job_id: str, error: Exception) -> None:
        """Make an unexpected intake failure visible for an explicit retry."""
        if not self._automatic_root_allowed(job_id):
            return
        try:
            runner = self._get_engine_runner()
            with runner.worker_lock():
                job = runner.get_job(job_id)
                if job.phase in {"completed", "cancelled", "failed"}:
                    return
                updated = replace(
                    job,
                    phase="failed",
                    updated_at=_now(),
                    error=redact_error(error),
                )
                atomic_write_json(
                    runner.jobs_root / f"{job_id}.json",
                    _redacted_job_payload(updated),
                    allow_nan=False,
                )
        except (EngineJobNotFoundError, EngineWorkerBusyError):
            return

    def _run_automatic_job(self, job_id: str) -> None:
        """Run the one selected intake-bound RootJob through B→R."""
        if self._closed.is_set() or not self._automatic_root_allowed(job_id):
            return
        runner: SimpleEngineRunner | None = None
        try:
            from local.scrapeflow_api.root_pipeline import (
                RUNNABLE_PHASES,
                is_intake_bound_root,
                run_root_pipeline,
            )

            runner = self._get_engine_runner()
            job = runner.get_job(job_id)
            if self._is_internal_child(job) or not is_intake_bound_root(self.state_root, job_id):
                return
            if job.phase in RUNNABLE_PHASES:
                final = run_root_pipeline(
                    runner,
                    self.state_root,
                    job_id,
                    pause_requested=lambda: self._root_pause_requested(job_id),
                )
                if (
                    final.phase == "gaps_pending"
                    and aggregate_root_job(self.state_root, job_id).open_gaps > 0
                ):
                    # The selected RootJob owns its gaps directly.  Keep the
                    # two provider tiers in this same one-worker turn instead
                    # of scheduling a separate lane or retry callback.
                    self._run_root_replenishment(job_id)
        except (EnginePauseRequested, EngineCancellationRequested):
            return
        except Exception as exc:
            self._record_automatic_failure(job_id, exc)
        finally:
            if runner is not None:
                self._consume_root_cancellation(runner, job_id)

    def _run_completed_root_j_rereview(self, root_task_id: str) -> None:
        """Repair a strict J omission without re-entering F/G/H.

        An explicit retry reaches this worker only after the local detection
        proved an executed internal carrier has a canonical planner
        ``missing_season`` row not yet represented by exact ledger episodes.
        ``rereview_executed_unit_gaps`` therefore never creates a plan or
        executes the writer.  If J opens precise gaps, the ordinary selected
        root replenishment turn follows in this same single-worker slot.
        """
        if self._closed.is_set() or not self._automatic_root_allowed(root_task_id):
            return
        runner: SimpleEngineRunner | None = None
        try:
            from local.scrapeflow_api.root_pipeline import refresh_root_after_j_rereview
            from local.scrapeflow_api.unit_execution import rereview_executed_unit_gaps

            runner = self._get_engine_runner()
            rereview_executed_unit_gaps(
                runner,
                self.state_root,
                root_task_id,
                pause_requested=lambda: self._root_pause_requested(root_task_id),
            )
            final = refresh_root_after_j_rereview(runner, self.state_root, root_task_id)
            if (
                final.phase == "gaps_pending"
                and aggregate_root_job(self.state_root, root_task_id).open_gaps > 0
            ):
                self._run_root_replenishment(root_task_id)
        except (EnginePauseRequested, EngineCancellationRequested):
            return
        except Exception:
            # An untrusted legacy carrier cannot be repaired by guessing.  A
            # later normal retry remains available; this narrow path never
            # changes media or invents an alternate lifecycle state.
            return
        finally:
            if runner is not None:
                self._consume_root_cancellation(runner, root_task_id)

    def _resume_automatic_jobs(self) -> None:
        """Resume only the currently selected RootJob after an explicit resume."""
        if self._closed.is_set() or self.control().get("paused") is not False:
            return
        root_job_id = self.control().get("root_job_id")
        if not isinstance(root_job_id, str) or not self._automatic_root_allowed(root_job_id):
            return
        try:
            from local.scrapeflow_api.root_pipeline import RUNNABLE_PHASES, is_intake_bound_root

            job = self._get_engine_runner().get_job(root_job_id)
            if not is_intake_bound_root(self.state_root, root_job_id):
                return
            aggregate = aggregate_root_job(self.state_root, root_job_id)
            if job.phase in RUNNABLE_PHASES:
                self._queue_automatic_job(root_job_id)
            elif aggregate.open_gaps > 0 and job.phase in {"completed", "gaps_pending"}:
                # A completed record from before the explicit J/N root state
                # still owns its durable gaps.  Resume only its selected
                # replenishment lane; never re-run B/W/C/D or create work.
                self._queue_root_replenishment(root_job_id)
        except Exception:
            # A configuration or remote failure remains visible on the task;
            # it never changes the local pause/selection state.
            return

    def _queue_root_replenishment(self, root_task_id: str) -> str:
        """Queue the selected RootJob's gap work directly."""
        if self._closed.is_set():
            return "closed"
        if not self._automatic_root_allowed(root_task_id):
            return "paused-or-unselected"
        try:
            if aggregate_root_job(self.state_root, root_task_id).open_gaps <= 0:
                return "no-open-gaps"
        except Exception:
            return "aggregate-error"
        return self._queue_selected_work(root_task_id, self._run_root_replenishment)

    @staticmethod
    def _root_replenishment_search_runner(
        runner: SimpleEngineRunner,
    ) -> Callable[[Mapping[str, Any]], Mapping[str, Any]]:
        """Build the one read-only PanSou discovery boundary for a root.

        PanSou only returns untrusted share URLs.  It needs an inspector bound
        to the same authenticated AList Quark mount as the root writer before
        a row can become a selectable candidate.  Keeping that wiring here
        gives the discovery layer no write capability: the inspector performs
        only share metadata reads, while the materializer remains the sole
        later staging writer.
        """
        from engine.tools.replenishment_adapter.pansou import (
            PanSouDiscovery,
            quark_share_inspector,
        )
        from engine.tools.replenishment_adapter.search import (
            ReplenishmentSearchService,
        )

        inspector = quark_share_inspector(runner.alist, runner.library_root)
        discovery = PanSouDiscovery.from_env(inspector=inspector)
        return ReplenishmentSearchService(pansou=discovery).run

    def _run_root_replenishment(self, root_task_id: str) -> None:
        """Execute one selected-root replenishment round."""
        if self._closed.is_set() or not self._automatic_root_allowed(root_task_id):
            return
        runner: SimpleEngineRunner | None = None
        try:
            runner = self._get_engine_runner()
            from local.scrapeflow_api.root_replenishment import run_root_replenishment

            search_runner = self._root_replenishment_search_runner(runner)

            # There are exactly two video tiers.  If the first one reaches a
            # proven terminal candidate result, continue straight into the
            # second one while this same selected-root worker is active.  A
            # retry_wait or waiting_reconcile state is durable and requires a
            # later explicit user retry; no timer re-submits it.
            for _ in range(2):
                result = run_root_replenishment(
                    runner,
                    self.state_root,
                    root_task_id,
                    search_runner=search_runner,
                    pause_requested=lambda: self._root_pause_requested(root_task_id),
                )
                if (
                    result.get("waiting") is not None
                    or result.get("tier") == result.get("tier_before")
                    or self._closed.is_set()
                    or not self._automatic_root_allowed(root_task_id)
                ):
                    break
            # The materializer closes each Gap only after its child write and
            # fresh targeted readback.  Re-read the ledger once at the end of
            # this one-worker round so the root cannot remain "completed"
            # merely because H passed before J/N did.
            from local.scrapeflow_api.root_pipeline import finalize_root_gap_closure
            finalize_root_gap_closure(runner, self.state_root, root_task_id)
        except (EnginePauseRequested, EngineCancellationRequested):
            return
        except Exception as exc:
            # The replenishment runner persists retry/wait state for provider
            # failures.  Never dump a provider exception here: it can contain
            # an opaque share URL or token.  An unexpected local failure
            # (bug, disk error) still leaves a durable, redacted note on the
            # job instead of a silent stall the operator cannot see.
            from engine.scrapeflow.serialization import atomic_write_json

            note_path = self.state_root / f"replenishment_crash_{root_task_id}.json"
            try:
                atomic_write_json(
                    note_path,
                    {
                        "root_task_id": root_task_id,
                        "note": "补源轮异常退出（详见日志时间戳），可重试 resume",
                        "at": _now(),
                    },
                    allow_nan=False,
                )
            except Exception:  # noqa: BLE001 - best-effort note only
                pass
            print(f"[replenishment] root {root_task_id} 补源轮异常退出（已落崩溃标记）", flush=True)
            return
        finally:
            if runner is not None:
                self._consume_root_cancellation(runner, root_task_id)

    def replenishment_view(self, job_id: str) -> dict[str, object]:
        """Read-only P14 preview: aggregate, tier state and bridged requests."""
        runner = self._get_engine_runner()
        job = runner.get_job(job_id)
        from local.scrapeflow_api.replenishment_bridge import gap_ledger_requests
        from local.scrapeflow_api.root_replenishment import (
            load_root_replenishment_state,
        )
        return {
            "root_task_id": job_id,
            "phase": self.public_engine_job(job).get("phase", job.phase),
            "aggregate": aggregate_root_job(self.state_root, job_id).as_dict(),
            "tier_state": load_root_replenishment_state(self.state_root, job_id),
            "requests": gap_ledger_requests(self.state_root, job_id),
        }

    def trigger_root_replenishment(self, job_id: str) -> dict[str, object]:
        """Queue missing media for the selected, running RootJob."""
        from local.scrapeflow_api.root_pipeline import is_intake_bound_root
        runner = self._get_engine_runner()
        job = runner.get_job(job_id)
        if not self._automatic_root_allowed(job.id):
            raise EngineRequestError("请先选择并恢复此 RootJob")
        if not is_intake_bound_root(self.state_root, job_id):
            raise EngineRequestError("补源触发只允许 intake 绑定的根任务")
        if aggregate_root_job(self.state_root, job_id).open_gaps <= 0:
            raise EngineRequestError("根任务当前没有待闭环缺口")
        status = self._queue_root_replenishment(job_id)
        return {"queued": status == "queued", "status": status, "root_task_id": job_id}

    def _validate_automatic_source(self, source: str) -> str:
        normalized = source.strip().rstrip("/")
        if not normalized or not normalized.startswith("/"):
            raise EngineRequestError("来源必须是绝对远端目录")
        inbound = f"{self.remote_root.rstrip('/')}/待刮削/"
        if not normalized.startswith(inbound):
            raise EngineRequestError("来源只能来自待刮削目录的直接子目录")
        relative = normalized[len(inbound):]
        if not relative or "/" in relative or "\\" in relative or relative in {".", ".."}:
            raise EngineRequestError("来源必须是待刮削目录的直接子目录")
        return normalized

    def create_root_task(
        self,
        payload: Mapping[str, object],
        *,
        allow_existing: bool = True,
    ) -> EngineJob:
        """Create the user-authorized RootJob for one intake source (S step).

        The user picks the source and its first-level shelf in one action.
        The new root becomes the selected RootJob and remains paused until the
        user explicitly resumes it.
        """
        unknown = set(payload) - {"path", "source_path", "target_shelf"}
        if unknown:
            raise EngineRequestError("创建任务只接受 path/source_path 与 target_shelf")
        source = payload.get("path", payload.get("source_path"))
        if not isinstance(source, str) or not source.strip():
            raise EngineRequestError("请输入媒体源目录 path")
        normalized_source = self._validate_automatic_source(source)
        try:
            shelf = parse_target_shelf(payload.get("target_shelf"))
        except ValueError as exc:
            raise EngineRequestError(str(exc)) from exc
        runner = self._get_engine_runner()
        try:
            existing = runner.find_by_source(normalized_source)
        except SimpleEngineError as exc:
            if not allow_existing:
                raise ApplicationError("无法确认来源是否已有 RootJob，已安全停止") from exc
            existing = None
        if existing is not None:
            if not allow_existing:
                raise EngineJobConflictError("批次来源已有 RootJob，拒绝复用历史任务")
            if existing.target_shelf is not None and existing.target_shelf != shelf.value:
                raise EngineJobConflictError("同一来源已选择其他货架，不能更改")
            if existing.target_shelf is None:
                selected = runner.start_automatic_job(existing.id, target_shelf=shelf)
            else:
                selected = existing
            self.select_root_job(selected.id)
            return selected
        if not runner.source_directory_exists(normalized_source):
            raise EngineRequestError("来源目录不存在或不是可读取的目录")
        create_root = getattr(runner, "create_root_job", None)
        if not callable(create_root):
            raise EngineRequestError("Engine 缺少根任务创建入口")
        from engine.scrapeflow.intake_source import intake_source_id

        job = create_root(
            intake_source_id(normalized_source),
            source_path=normalized_source,
            target_shelf=shelf,
        )
        selected = runner.start_automatic_job(job.id, target_shelf=shelf)
        self.select_root_job(selected.id)
        return selected

    def reopen_orphan_root_task(
        self,
        job_id: str,
        payload: Mapping[str, object],
    ) -> EngineJob:
        """Recover one dangling IntakeSource binding under its original id.

        This is deliberately narrower than create/retry: the service must be
        paused and unselected, the caller supplies the deleted job document as
        identity evidence (legacy cleanups have no tombstone), and every F+
        artifact must be absent.  Fresh B/W is discovered before the queued
        job becomes visible; the job JSON is the commit marker.  A crash
        before that marker remains an orphan and the same request can safely
        replace the incomplete B/W generation.
        """
        if not isinstance(payload, Mapping) or set(payload) != {"backup_job"}:
            raise EngineRequestError("orphan reopen 只接受 backup_job 证据")
        raw_backup = payload.get("backup_job")
        if not isinstance(raw_backup, Mapping):
            raise EngineRequestError("backup_job 必须是完整 RootJob 对象")
        try:
            backup = EngineJob.from_dict(raw_backup)
        except Exception as exc:
            raise EngineRequestError("backup_job 格式或字段无法验证") from exc
        if backup.id != job_id:
            raise EngineJobConflictError("backup_job ID 与孤儿绑定 ID 不一致")
        if backup.phase not in {"cancelled", "failed", "completed"}:
            raise EngineJobConflictError("backup_job 不是可审计的终态记录")
        if backup.plan or backup.execution is not None:
            raise EngineJobConflictError("backup_job 已包含 F/G/H 计划或执行证据")
        source = backup.request.get("source_path")
        if not isinstance(source, str):
            raise EngineJobConflictError("backup_job 缺少来源路径")
        source = self._validate_automatic_source(source)
        if backup.target_shelf is None or backup.target_root is None:
            raise EngineJobConflictError("backup_job 缺少已授权货架")
        expected_root = target_root_for_shelf(self.remote_root, backup.target_shelf)
        if backup.target_root != expected_root:
            raise EngineJobConflictError("backup_job 货架与正式库根不一致")

        from engine.scrapeflow.intake_source import load_intake_catalog
        from engine.scrapeflow.root_boundaries import (
            build_root_boundary_analysis,
            load_source_snapshot,
            persist_root_boundary_analysis,
        )
        from engine.scrapeflow.work_units import load_work_unit_records
        reopen_revision: list[int] = []

        def assert_quiescent(runner: SimpleEngineRunner) -> None:
            control = self.control()
            if control != {"paused": True, "root_job_id": None}:
                raise EngineJobConflictError("orphan reopen 要求 paused=true 且无 selected root")
            active = self._worker_future
            if active is not None and not active.done():
                raise EngineWorkerBusyError("orphan reopen 时仍有活动 worker")
            bindings = [
                item for item in load_intake_catalog(self.state_root)
                if item.root_task_id == job_id
            ]
            if len(bindings) != 1 or bindings[0].canonical_path != source:
                raise EngineJobConflictError("IntakeSource 必须只绑定该 ID 与该来源")
            if runner._job_path(job_id).exists() or runner._job_path(job_id).is_symlink():  # noqa: SLF001
                raise EngineJobConflictError("RootJob 记录已存在，不是孤儿绑定")
            forbidden = [
                self.state_root / f"work_acceptance_{job_id}.json",
                self.state_root / f"gap_ledger_{job_id}.json",
                self.state_root / f"replenishment_{job_id}.json",
                self.state_root / "gaps" / job_id,
                self.state_root / "staging" / job_id,
                self.state_root / "archive-staging" / job_id,
                self.state_root / "replenishment_workspace" / job_id,
                self.state_root / "subtitle_replenishment_workspace" / job_id,
            ]
            if any(path.exists() or path.is_symlink() for path in forbidden):
                raise EngineJobConflictError("RootJob 存在 writer/acceptance/gap/staging 副作用证据")
            for candidate in runner.list_jobs():
                summary = candidate.summary if isinstance(candidate.summary, Mapping) else {}
                if summary.get("root_job_id") == job_id:
                    raise EngineJobConflictError("RootJob 仍存在内部 carrier/child")
            tombstone = self.state_root / "root-job-tombstones" / f"{job_id}.json"
            if tombstone.exists():
                try:
                    marker = json.loads(tombstone.read_text(encoding="utf-8"))
                except Exception as exc:
                    raise EngineJobConflictError("RootJob tombstone 无法读取") from exc
                expected = {
                    "job_id": job_id,
                    "source_path": source,
                    "target_shelf": backup.target_shelf,
                    "target_root": backup.target_root,
                }
                if any(marker.get(key) != value for key, value in expected.items()):
                    raise EngineJobConflictError("RootJob tombstone 与 backup_job 身份不一致")
            # Read every old pre-F ledger before allowing replacement.  Missing
            # or malformed ledgers are uncertainty, never proof of no write.
            units_path = self.state_root / f"work_units_{job_id}.json"
            if not units_path.is_file() or units_path.is_symlink():
                raise EngineJobConflictError("旧 WorkUnit ledger 缺失，无法证明停在写入前")
            try:
                raw_units = json.loads(units_path.read_text(encoding="utf-8"))
                if not isinstance(raw_units, list):
                    raise ValueError
                records = load_work_unit_records(self.state_root, job_id)
                if len(records) != len(raw_units) or not records:
                    raise ValueError
            except Exception as exc:
                raise EngineJobConflictError("旧 WorkUnit ledger malformed，拒绝覆盖") from exc
            max_revision = 0
            for raw, record in zip(raw_units, records):
                if not isinstance(raw, Mapping) or record.root_task_id != job_id:
                    raise EngineJobConflictError("旧 WorkUnit 所有权或格式无法验证")
                max_revision = max(max_revision, int(record.source_revision))
                if record.writer_job_id is not None:
                    raise EngineJobConflictError("旧 WorkUnit 已有 writer_job_id，拒绝 reopen")
                map_path = self.state_root / f"episode_map_{record.work_unit_id}.json"
                if map_path.exists() or map_path.is_symlink():
                    raise EngineJobConflictError("旧 WorkUnit 存在 episode map，拒绝 reopen")
                for key in ("writer_job_id", "acceptance", "execution", "plan", "target_root"):
                    if raw.get(key) not in (None, {}, ""):
                        raise EngineJobConflictError(f"旧 WorkUnit 含 F+ 字段 {key}，拒绝 reopen")
            if backup.phase in {"completed", "failed"}:
                if backup.phase != "failed" or backup.summary.get("prewrite_failure_proven") is not True:
                    raise EngineJobConflictError("completed/failed 缺少明确写入前证明，拒绝 reopen")
            # Snapshot and manifest are required inputs, but their revision is
            # evidence; never reset the fresh generation to a magic 1.
            snapshot_path = self.state_root / f"work_snapshot_{job_id}.json"
            manifest_path = self.state_root / f"source_manifest_{job_id}.json"
            if not snapshot_path.is_file() or not manifest_path.is_file():
                raise EngineJobConflictError("旧 B/W 证据不完整，拒绝 reopen")
            reopen_revision[:] = [max_revision + 1]

        with self._automatic_lock:
            runner = self._get_engine_runner()
            assert_quiescent(runner)
            runner._ensure_authenticated(runner.alist)  # noqa: SLF001
            if not runner.source_directory_exists(source):
                raise EngineJobConflictError("待刮削来源不存在，不能 reopen")
            snapshot, records = build_root_boundary_analysis(
                runner.alist,
                source,
                root_task_id=job_id,
                source_revision=(reopen_revision[0] if reopen_revision else 1),
            )
            if not records:
                raise EngineJobConflictError("fresh B/W 未发现作品单元")
            with runner.worker_lock():
                assert_quiescent(runner)
                persist_root_boundary_analysis(
                    self.state_root, job_id, snapshot, records,
                )
                summary = {
                    "automatic": True,
                    "source_root": source,
                    "ingress_source_path": source,
                    "mode": "auto",
                    "target_shelf": backup.target_shelf,
                    "selected_target_root": backup.target_root,
                    "orphan_reopened": True,
                }
                reopened = replace(
                    backup,
                    phase="queued",
                    updated_at=_now(),
                    request={"source_path": source},
                    plan={},
                    summary=summary,
                    execution=None,
                    error=None,
                )
                atomic_write_json(
                    runner._job_path(job_id),  # noqa: SLF001
                    _redacted_job_payload(reopened),
                    allow_nan=False,
                )
            self._control_state.set(paused=True, root_job_id=job_id)
            # Prove the committed B/W generation is readable before returning.
            if load_source_snapshot(self.state_root, job_id) is None:
                raise EngineJobConflictError("fresh B/W 提交后回读失败")
            return reopened

    def work_units_view(self, job_id: str) -> dict[str, object]:
        """Read-only R-node projection of one root task's work units."""
        runner = self._get_engine_runner()
        job = runner.get_job(job_id)
        aggregate = aggregate_root_job(self.state_root, job_id)
        return {
            "root_task_id": job_id,
            "source": job.request.get("source_path"),
            # Keep the unit-detail endpoint aligned with the public job card:
            # a legacy Engine ``completed`` phase with open J Gaps is exposed
            # as ``gaps_pending`` while retaining the H readback evidence.
            "phase": self.public_engine_job(job).get("phase", job.phase),
            "aggregate": aggregate.as_dict(),
            "units": public_work_unit_rows(self.state_root, job_id),
        }

    def confirm_work_unit(
        self,
        job_id: str,
        work_unit_id: str,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        """Persist one durable operator identity confirmation (U node).

        Only ``media_type + tmdb_id`` (and an optional season) are accepted.
        The override lives on the WorkUnit ledger, so retrying the same root
        task never asks the same question again.
        """
        if not isinstance(payload, Mapping):
            raise EngineRequestError("确认请求必须是 JSON 对象")
        # The confirmation is a lock-free ledger write; the pipeline's own
        # saves race it when this root's worker is live (the operator's
        # "confirmed" could be silently overwritten back to uncertain).
        # Require the worker to be stopped for this root — pausing first is
        # the documented operator flow.
        with self._automatic_lock:
            active = self._worker_future
            if (
                self._worker_root_job_id == job_id
                and active is not None
                and not active.done()
            ):
                raise EngineWorkerBusyError(
                    "任务运行中不能确认作品单元；请先暂停再确认"
                )
        unknown = set(payload) - {"tmdb_id", "media_type", "season"}
        if unknown:
            raise EngineRequestError("确认只接受 tmdb_id、media_type 与可选 season")
        media_type = payload.get("media_type")
        if media_type not in {"movie", "tv"}:
            raise EngineRequestError("media_type 必须是 movie 或 tv")
        tmdb_id = payload.get("tmdb_id")
        if isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0:
            raise EngineRequestError("tmdb_id 必须是正整数")
        season = payload.get("season")
        if season is not None and (
            isinstance(season, bool) or not isinstance(season, int) or season <= 0
        ):
            raise EngineRequestError("season 必须是正整数或省略")
        from engine.scrapeflow.unit_identity import apply_work_unit_override

        try:
            unit = apply_work_unit_override(
                self.state_root, job_id, work_unit_id,
                media_type=media_type, tmdb_id=tmdb_id, season=season,
            )
            # The override writes the minimal identity (title=None), but the
            # layout layer's sub-series grouping reads the zh-CN title.  Fill
            # the official title/year from TMDB the same way the automatic
            # matcher's identity projection would have.
            try:
                detail = self._get_engine_runner().tmdb.get(
                    f"/{media_type}/{tmdb_id}"
                )
                if isinstance(detail, dict):
                    title = detail.get("name" if media_type == "tv" else "title")
                    date_value = detail.get(
                        "first_air_date" if media_type == "tv" else "release_date"
                    )
                    year = str(date_value or "")[:4] or None
                    if title:
                        from engine.scrapeflow.work_units import (
                            load_work_unit_records,
                            save_work_unit_records,
                        )
                        from dataclasses import replace as _replace
                        records_now = load_work_unit_records(
                            self.state_root, job_id,
                        )
                        for _index, item in enumerate(records_now):
                            if item.work_unit_id != work_unit_id:
                                continue
                            identity_now = dict(item.identity or {})
                            identity_now["title"] = str(title)
                            if year:
                                identity_now["year"] = year
                            records_now[_index] = _replace(
                                item, identity=identity_now,
                            )
                            unit = records_now[_index]
                            break
                        save_work_unit_records(
                            self.state_root, job_id, records_now,
                        )
            except Exception:
                pass  # The durable override stands; the title is enrichments.
        except KeyError as exc:
            raise EngineJobNotFoundError(f"work unit 不存在: {work_unit_id}") from exc
        except ValueError as exc:
            raise EngineRequestError(str(exc)) from exc
        # Resume the same root task: refresh the read-only reconciliation for
        # the confirmed identity (D step).  Never create a new task.
        try:
            runner = self._get_engine_runner()
            from local.scrapeflow_api.library_index import (
                reconcile_root_work_units,
            )
            from local.scrapeflow_api.tmdb_episode_catalog import (
                TmdbEpisodeCatalog,
            )
            from local.scrapeflow_api.unit_e_lanes import compute_known_gap_tokens
            reconcile_root_work_units(
                runner.alist, runner.library_root, self.state_root, job_id,
                known_gap_tokens_by_identity=compute_known_gap_tokens(
                    self.state_root
                ),
                # Mirror the pipeline's own D pass: without the catalog and
                # client the planner dry-run evidence source stays dark and a
                # specials-only confirmed unit parks as "缺少可证明的季集坐标"
                # even though F could prove its coordinates.
                episode_catalog=TmdbEpisodeCatalog(runner.tmdb),
                tmdb_client=runner.tmdb,
            )
        except Exception:
            pass  # Read-only refinement; the durable override already stands.
        # Re-dispatch the root through the unit pipeline when running; the
        # durable override alone does not trigger execution while paused.
        # Immediate dispatch (no timer hop) so the override is never left
        # parked behind a lost scheduling edge.
        if self.control().get("paused") is not True:
            self._queue_automatic_job(job_id)
        return public_work_unit_row(unit, self.state_root, job_id)

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
        with self._automatic_lock:
            engine_job = self._engine_job_or_none(job_id)
            if engine_job is None:
                raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
            # First persist the cancellation (or its durable in-flight marker).
            # Only then close the selected-root gate, so a crash cannot leave a
            # merely-paused task that later resumes provider work.
            result = self._get_engine_runner().cancel_job(job_id, reason=reason or "cancelled")
            if self.control().get("root_job_id") == engine_job.id:
                self._control_state.set(paused=True, root_job_id=None)
            return self.public_engine_job(result)

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
            "queued", "analyzing", "archive_preprocessing", "identity_matching",
            "planning", "executing_media", "verifying", "cleaning", "retry_wait",
            "gaps_pending",
        }
        if public_phase in active_phases:
            raise EngineWorkerBusyError(
                f"任务仍在运行或等待人工重试，不能清理记录: {public_phase}"
            )
        with self._automatic_lock:
            active = self._worker_future
            if (
                self._worker_root_job_id == engine_job.id
                and active is not None
                and not active.done()
            ):
                raise EngineWorkerBusyError("任务仍有活动 worker，不能清理记录")
            runner = self._engine_runner
            if runner is None:
                # Terminal cleanup is a local-state operation and remains
                # useful while AList/TMDB credentials are unavailable.
                runner = SimpleEngineRunner(
                    self.state_root,
                    alist=self._alist_client or object(),
                    tmdb=object(),
                    validate=False,
                    library_root=self.remote_root,
                )
            return runner.cleanup_terminal_job(engine_job.id)

    def consume_source_public_job(self, job_id: str, payload: Mapping[str, object]) -> dict[str, object]:
        """Consume one terminal root's intake source tree (operator ruling).

        The engine cleanup the pipeline runs automatically, exposed as an
        explicit idempotent operator action for historical terminal roots
        and for re-running a consumption that previously left residuals.
        The whole intake tree — losing versions, unmapped specials,
        non-media — is deleted after intake ownership is proven; the gap
        ledger stays the durable record.  Only ``completed`` and
        ``gaps_pending`` roots qualify: attention/parked roots keep their
        source until their reconciliation is resolved.
        """
        if not isinstance(payload, Mapping) or payload:
            raise EngineRequestError("清源请求必须是空 JSON 对象")
        engine_job = self._engine_job_or_none(job_id)
        if engine_job is None:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
        public_phase = str(self.public_engine_job(engine_job).get("phase") or "")
        if public_phase not in {"completed", "gaps_pending"}:
            raise EngineRequestError(
                f"只有终态根（completed/gaps_pending）才能清源，当前: {public_phase}"
            )
        with self._automatic_lock:
            active = self._worker_future
            if (
                self._worker_root_job_id == engine_job.id
                and active is not None
                and not active.done()
            ):
                raise EngineWorkerBusyError("任务仍有活动 worker，不能清源")
        from local.scrapeflow_api.root_pipeline import consume_terminal_source_root

        runner = self._get_engine_runner()

        def _explicit_consume_pause_requested() -> bool:
            """Only a live cancellation stops an explicit terminal cleanup.

            The standing pause is the engine's RESTING state after a run
            completes — it gates the automatic worker, not the operator's
            explicit action on an already-terminal root.  Routing it through
            ``_root_pause_requested`` silently no-opped every consume-source
            re-run (the walk aborted at its first pause check with an empty
            failure list, leaving the residual note unchanged).  A
            cancellation request raised during the walk still stops it.
            """
            try:
                return runner.cancellation_pending(job_id)
            except (EngineJobNotFoundError, SimpleEngineError):
                return True

        receipt = consume_terminal_source_root(
            runner,
            self.state_root,
            engine_job,
            pause_requested=_explicit_consume_pause_requested,
        )
        return {"consume_source": receipt}

    def file_disc_ruling_public_job(
        self,
        job_id: str,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        """File one operator playlist→episode ruling for a parked scope.

        This is the data-level operator channel for scopes whose discs
        declare no readable episode order: the ruling is stored beside the
        root's other durable state and the next expansion pass validates it
        against the same TMDB roster and duration tolerance as the engine's
        own proof.  It only lands on a scope that is currently parked behind
        the disc-image inspection requirement, so a wrong-scope filing fails
        closed instead of silently lying in the store.
        """
        from engine.scrapeflow.disc_expansion import ScopeMappingRuling
        from engine.scrapeflow.disc_expansion_bridge import save_disc_ruling
        from engine.scrapeflow.work_units import load_work_unit_records

        if not isinstance(payload, Mapping) or not payload:
            raise EngineRequestError("光盘裁决请求必须是非空 JSON 对象")
        engine_job = self._engine_job_or_none(job_id)
        if engine_job is None:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
        with self._automatic_lock:
            active = self._worker_future
            if (
                self._worker_root_job_id == engine_job.id
                and active is not None
                and not active.done()
            ):
                raise EngineWorkerBusyError("任务仍有活动 worker，不能登记光盘裁决")
        data = dict(payload)
        if not str(data.get("filed_at") or "").strip():
            data["filed_at"] = datetime.now(UTC).isoformat()
        try:
            ruling = ScopeMappingRuling.from_mapping(data)
        except (TypeError, ValueError) as exc:
            raise EngineRequestError(f"光盘裁决格式无效: {exc}") from exc
        records = load_work_unit_records(self.state_root, engine_job.id)
        parked = [
            record
            for record in records
            if record.requires_content_expansion
            and record.disc_expansion is None
            and tuple(record.source_paths) == (ruling.scope_path,)
        ]
        if not parked:
            raise EngineRequestError(
                "来源范围没有待展开的光盘镜像单元（或已按裁决展开），无法登记: "
                f"{ruling.scope_path}"
            )
        save_disc_ruling(self.state_root, engine_job.id, ruling)
        return {
            "filed": {
                "scope_path": ruling.scope_path,
                "season": ruling.season,
                "assignments": len(ruling.assignments),
                "operator": ruling.operator,
            }
        }

    def repair_public_job_artifacts(
        self,
        job_id: str,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        """Explicitly repair deterministic NFO/poster artifacts for one root.

        This is intentionally a narrowly scoped operator action, not a
        background side effect: the persisted plan remains the source of truth and
        no discovery, provider search, or media cleanup is started here.
        """
        if not isinstance(payload, Mapping) or payload:
            raise EngineRequestError("元数据修复请求必须是空 JSON 对象")
        engine_job = self._engine_job_or_none(job_id)
        if engine_job is None:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
        if not self._automatic_root_allowed(engine_job.id):
            raise EngineRequestError("请先选择并恢复此 RootJob")
        repaired, children = self._get_engine_runner().repair_root_artifacts(
            job_id,
            pause_requested=lambda: self._root_pause_requested(job_id),
        )
        public = self.public_engine_job(repaired)
        public["artifact_repair"] = {
            "replenishment_children": [
                {
                    "id": child.id,
                    "updated_at": child.updated_at,
                    "artifact_count": int(
                        (child.execution or {}).get("artifact_count", 0)
                    ),
                }
                for child in children
            ],
        }
        return public

    def retry_public_job(self, job_id: str, payload: Mapping[str, object]) -> dict[str, object]:
        if not isinstance(payload, Mapping) or payload:
            raise EngineRequestError("重试请求只接受空 JSON 对象；身份确认请使用作品单元确认接口")
        engine_job = self._engine_job_or_none(job_id)
        if engine_job is None:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
        selected = self._validate_selected_root_job(job_id)
        if self.control().get("root_job_id") != selected:
            raise EngineRequestError("请先选择此 RootJob")
        # A retry rewrites the job to queued and requeues uncertain units in
        # the shared work-unit ledger while the worker may still be running
        # this same root — two lock-free ledger writers racing each other.
        # Refuse while the root's worker is live (same guard as cleanup /
        # consume-source); the operator retries after it stops or pauses.
        with self._automatic_lock:
            active = self._worker_future
            if (
                self._worker_root_job_id == engine_job.id
                and active is not None
                and not active.done()
            ):
                raise EngineWorkerBusyError(
                    "任务仍在运行，不能重试；请先暂停或等它收口"
                )
        from local.scrapeflow_api.root_pipeline import is_intake_bound_root

        if not is_intake_bound_root(self.state_root, job_id):
            raise EngineRequestError("重试只支持 IntakeSource 创建的 RootJob")
        if engine_job.phase in {"completed", "gaps_pending"}:
            if self._completed_root_needs_planner_gap_rereview(job_id):
                if self.control().get("paused") is False:
                    self._queue_completed_root_j_rereview(job_id)
            elif self.control().get("paused") is False:
                self._queue_root_replenishment(job_id)
            return self.public_engine_job(engine_job)
        summary = dict(engine_job.summary)
        # Discard state left by the retired automatic retry scheduler.  Retry
        # is now one explicit operator action, not a background loop.
        summary.pop("automatic_attempts", None)
        summary.pop("automatic_terminal", None)
        summary.pop("next_retry_seconds", None)
        retried = replace(
            engine_job,
            phase="queued",
            summary=summary,
            updated_at=_now(),
            error=None,
        )
        retried = self._persist_retry_transition(engine_job, retried)
        # C/U and D/U are deliberately durable while a task is parked.  A
        # user-issued retry is the only normal route that reopens those
        # specific records after a generic matcher/index/configuration fix;
        # confirmed identities and completed work remain untouched.
        from engine.scrapeflow.unit_identity import requeue_uncertain_work_units

        requeue_uncertain_work_units(self.state_root, job_id)
        if self.control().get("paused") is False:
            self._queue_automatic_job(job_id)
        return self.public_engine_job(retried)

    def rebuild_uncertain_unit_boundaries(
        self,
        job_id: str,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        """Re-derive ONLY the parked C-uncertain units' boundaries for one root.

        The whole-root ``rebuild-boundaries`` surface must fail closed once any
        sibling unit carries write-side facts, because re-analysing a
        partially consumed source tree cannot reproduce those siblings'
        scopes.  A generic B/W rule deployed after such a partial write still
        needs a recovery point: this narrower surface re-runs the boundary
        analysis on each never-written C-uncertain unit's own single scope,
        keeps every other record byte-identical, and leaves the root paused
        and queued for the operator to resume.  It neither performs shelf
        selection nor starts a worker.
        """
        if not isinstance(payload, Mapping) or payload:
            raise EngineRequestError("未决单元边界重建请求必须是空 JSON 对象")
        selected = self._validate_selected_root_job(job_id)
        control = self.control()
        if control.get("paused") is not True or control.get("root_job_id") != selected:
            raise EngineRequestError("未决单元边界重建必须在当前已选 RootJob 的暂停态执行")
        from engine.scrapeflow.root_boundaries import (
            load_source_manifest,
            rederive_uncertain_unit_boundary,
            source_object_claims_for_records,
        )
        from engine.scrapeflow.source_objects import (
            validate_unique_source_object_ownership,
        )
        from engine.scrapeflow.work_units import save_work_unit_records
        from local.scrapeflow_api.root_pipeline import is_intake_bound_root

        with self._automatic_lock:
            active = self._worker_future
            if active is not None and not active.done():
                raise EngineWorkerBusyError("任务仍有活动 worker，不能重建未决单元边界")
            runner = self._get_engine_runner()
            before = runner.get_job(selected)
            if before.phase != "reconciliation_uncertain":
                raise EngineJobConflictError(
                    f"当前任务阶段不能重建未决单元边界: {before.phase}"
                )
            summary = before.summary if isinstance(before.summary, Mapping) else {}
            if "active_operation" in summary or "replenishment" in summary:
                raise EngineJobConflictError("任务已有活动或补源状态，不能重建未决单元边界")
            cancel_marker = runner._cancel_request_path(selected)  # noqa: SLF001 - exact local marker
            if cancel_marker.exists() or cancel_marker.is_symlink():
                raise EngineJobConflictError("任务已有取消请求，不能重建未决单元边界")
            if not is_intake_bound_root(self.state_root, selected):
                raise EngineRequestError("未决单元边界重建只支持 IntakeSource 创建的 RootJob")
            records = self._rebuild_boundary_records(selected)
            uncertain = [
                record for record in records
                if record.identity_status == "uncertain"
            ]
            if not uncertain:
                raise EngineRequestError("没有 C 未决单元，无需重建未决边界")
            ingress = str(runner._job_ingress_source(before)).rstrip("/")  # noqa: SLF001 - root composition
            for record in uncertain:
                if record.requires_content_expansion or len(record.source_paths) != 1:
                    raise EngineJobConflictError(
                        f"未决单元 {record.work_unit_id} 的边界形状不能安全重建，请人工处理"
                    )
                scope = str(record.source_paths[0]).rstrip("/")
                if scope == ingress:
                    raise EngineJobConflictError("整根边界请使用目录边界重建接口")
                if not runner.source_directory_exists(scope):
                    raise EngineJobConflictError("未决单元来源目录已不存在，不能重建边界")
            source_revision = max(
                record.source_revision for record in records
            ) + 1
            # The fresh AList walk of each uncertain scope runs outside the
            # writer lock; the final lock and re-check below rejects any
            # concurrent change to the durable root or ledger.
            replacements = [
                rederive_uncertain_unit_boundary(
                    runner.alist,
                    record,
                    root_task_id=selected,
                    source_revision=source_revision,
                )
                for record in uncertain
            ]
            with runner.worker_lock():
                latest = runner.get_job(selected)
                if (
                    latest.phase != before.phase
                    or latest.updated_at != before.updated_at
                ):
                    raise EngineJobConflictError("RootJob 状态已变化，请刷新后再重试")
                current = self._rebuild_boundary_records(selected)
                if [item.work_unit_id for item in current] != [
                    item.work_unit_id for item in records
                ]:
                    raise EngineJobConflictError("WorkUnit 账本已变化，请刷新后再重试")
                control = self.control()
                if (
                    control.get("paused") is not True
                    or control.get("root_job_id") != selected
                ):
                    raise EngineJobConflictError("控制状态已变化，不能重建未决单元边界")
                combined = [
                    record
                    for record in records
                    if record.identity_status != "uncertain"
                ]
                for group in replacements:
                    combined.extend(group)
                save_work_unit_records(self.state_root, selected, combined)
                manifest = load_source_manifest(self.state_root, selected)
                if manifest is not None:
                    validate_unique_source_object_ownership(
                        source_object_claims_for_records(manifest, combined)
                    )
                rebuilt = replace(
                    latest,
                    phase="queued",
                    updated_at=_now(),
                    error=None,
                )
                atomic_write_json(
                    runner._job_path(selected),  # noqa: SLF001 - root state boundary
                    _redacted_job_payload(rebuilt),
                    allow_nan=False,
                )
        return self.public_engine_job(rebuilt)

    def _rebuild_boundary_records(self, root_job_id: str) -> list[object]:
        """Read one existing B/W ledger strictly enough to replace it safely.

        ``load_work_unit_records`` intentionally treats a missing or malformed
        local file as an empty list for ordinary pipeline recovery.  That is
        useful there, but is too permissive for an operator action which is
        about to discard the old ledger: an unreadable record must stop this
        narrow recovery path rather than being mistaken for an empty B/W run.
        """
        from engine.scrapeflow.work_units import WorkUnitRecord

        path = self.state_root / f"work_units_{root_job_id}.json"
        if path.is_symlink() or not path.is_file():
            raise EngineJobConflictError("缺少可验证的 WorkUnit 台账，不能重建目录边界")
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EngineJobConflictError("WorkUnit 台账无法验证，不能重建目录边界") from exc
        if not isinstance(raw, list) or not raw:
            raise EngineJobConflictError("WorkUnit 台账为空，不能重建目录边界")
        records: list[WorkUnitRecord] = []
        seen_ids: set[str] = set()
        for item in raw:
            if not isinstance(item, Mapping):
                raise EngineJobConflictError("WorkUnit 台账包含无效记录，不能重建目录边界")
            identity = item.get("identity")
            if identity is not None and not isinstance(identity, Mapping):
                raise EngineJobConflictError("WorkUnit 身份记录格式无效，不能重建目录边界")
            candidates = item.get("candidate_identities", ())
            if not isinstance(candidates, (list, tuple)) or any(
                not isinstance(candidate, Mapping) for candidate in candidates
            ):
                raise EngineJobConflictError("WorkUnit 候选身份记录格式无效，不能重建目录边界")
            try:
                record = WorkUnitRecord.from_dict(item)
            except (KeyError, TypeError, ValueError) as exc:
                raise EngineJobConflictError("WorkUnit 台账无法验证，不能重建目录边界") from exc
            if (
                record.root_task_id != root_job_id
                or not record.work_unit_id
                or record.work_unit_id in seen_ids
                or not record.source_paths
            ):
                raise EngineJobConflictError("WorkUnit 台账归属不一致，不能重建目录边界")
            seen_ids.add(record.work_unit_id)
            records.append(record)
        return records

    def _discardable_prewrite_failure_acceptance(
        self,
        root_job_id: str,
        records: Sequence[object],
    ) -> tuple[Path, frozenset[str]] | None:
        """Return one proved pre-write failure receipt that B/W may replace.

        A failed planner can leave a typed acceptance row even though no
        internal carrier, plan, target path, or formal write ever existed.
        That row must not permanently prevent a boundary rebuild after a
        generic B/F repair.  This deliberately accepts only a non-empty,
        well-formed set of *failed before plan* rows; accepted, skipped,
        planned, malformed, or empty receipts remain hard blockers.
        """
        path = self.state_root / f"work_acceptance_{root_job_id}.json"
        if not (path.exists() or path.is_symlink()):
            return None
        if path.is_symlink() or not path.is_file():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(raw, list) or not raw:
            return None
        try:
            from local.scrapeflow_api.unit_execution import WorkAcceptanceResult

            accepted = [
                WorkAcceptanceResult.from_dict(item)
                for item in raw
                if isinstance(item, Mapping)
            ]
        except (KeyError, TypeError, ValueError):
            return None
        if len(accepted) != len(raw):
            return None
        record_ids = {
            str(getattr(record, "work_unit_id", ""))
            for record in records
        }
        receipt_ids = [result.work_unit_id for result in accepted]
        if (
            not record_ids
            or len(set(receipt_ids)) != len(receipt_ids)
            or any(result.work_unit_id not in record_ids for result in accepted)
        ):
            return None
        records_by_id = {
            str(getattr(record, "work_unit_id", "")): record
            for record in records
        }
        if all(
            result.outcome == "failed"
            and result.phase == "failed"
            and result.writer_job_id is None
            and result.target_root == ""
            and result.planned_files == 0
            and bool(result.error)
            and getattr(
                records_by_id.get(result.work_unit_id),
                "reconciliation_outcome",
                None,
            ) == "new_work"
            for result in accepted
        ):
            return path, frozenset(receipt_ids)
        return None

    def _has_proven_empty_work_acceptance(self, root_job_id: str) -> bool:
        """Return true only for the harmless pre-write ``[]`` receipt.

        Boundary analysis persists an empty acceptance list before any
        planner, carrier, writer, gap or staging action exists.  Treating that
        marker as post-write evidence made a parked C/U-only root impossible
        to rebuild after a generic B/W/C repair.  The proof is deliberately
        exact: missing is harmless too; a symlink, non-file, malformed JSON,
        non-list value, or even one empty-looking object stays a hard block.
        """
        path = self.state_root / f"work_acceptance_{root_job_id}.json"
        if not path.exists() or path.is_symlink() or not path.is_file():
            return False
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return False
        return raw == []

    def _assert_boundary_rebuild_safe(
        self,
        runner: SimpleEngineRunner,
        job: EngineJob,
        *,
        expected_updated_at: str | None = None,
    ) -> tuple[list[object], tuple[Path, frozenset[str]] | None]:
        """Prove that only non-effect B/W/C/D state would be replaced.

        This is deliberately narrower than ``retry``.  It may discard
        automatically-derived C/U evidence because a changed source boundary
        invalidates that evidence.  It normally refuses D results too; the
        sole exception is a root that failed before a plan/carrier was ever
        created and has an exact zero-file failure receipt.  The caller holds
        the application lock; the final invocation also holds the runner's
        process-wide writer lock.
        """
        if self._is_internal_child(job):
            raise EngineJobConflictError("内部 child 不能重建目录边界")
        if expected_updated_at is not None and job.updated_at != expected_updated_at:
            raise EngineJobConflictError("RootJob 状态已变化，请刷新后再重试")
        if job.phase not in {"queued", "reconciliation_uncertain", "failed"}:
            raise EngineJobConflictError(f"当前任务阶段不能重建目录边界: {job.phase}")
        if job.target_shelf is None or job.target_root is None or job.selected_at is None:
            raise EngineJobConflictError("RootJob 尚未完成货架选择，不能重建目录边界")
        if job.execution is not None or dict(job.plan):
            raise EngineJobConflictError("任务已有计划或执行记录，不能重建目录边界")
        summary = job.summary if isinstance(job.summary, Mapping) else {}
        if "active_operation" in summary or "replenishment" in summary:
            raise EngineJobConflictError("任务已有活动或补源状态，不能重建目录边界")
        cancel_marker = runner._cancel_request_path(job.id)  # noqa: SLF001 - exact local marker
        if cancel_marker.exists() or cancel_marker.is_symlink():
            raise EngineJobConflictError("任务已有取消请求，不能重建目录边界")

        records = self._rebuild_boundary_records(job.id)
        prewrite_failure = self._discardable_prewrite_failure_acceptance(
            job.id, records,
        )
        if job.phase == "failed" and prewrite_failure is None:
            raise EngineJobConflictError(
                "失败任务缺少可验证的写前失败记录，不能重建目录边界"
            )
        allow_automatic_reconciliation_reset = job.phase == "failed" and prewrite_failure is not None
        for record in records:
            identity = record.identity or {}
            if identity.get("source") == "operator_override":
                raise EngineJobConflictError("存在人工身份确认，不能重建目录边界")
            # A failed root may discard only automatic D=new_work decisions
            # when the zero-file receipt above proves F never persisted a
            # carrier or reached a formal target.  Any other D/lane fact is
            # still outside this recovery surface.  An automatic
            # D=uncertain is different: it is a parked state ("证据不足"),
            # never a write-side decision, and every write-side fact is
            # still blocked independently below.  An operator rebuilding
            # boundaries after changing the source (for example deleting
            # release folders) must be able to discard those stale parks so
            # C/D re-derive from the fresh snapshot.
            if (
                (
                    record.reconciliation_outcome is not None
                    and not (
                        (
                            allow_automatic_reconciliation_reset
                            and record.reconciliation_outcome == "new_work"
                        )
                        or record.reconciliation_outcome == "uncertain"
                    )
                )
                or record.matched_work_root is not None
                or record.writer_job_id is not None
                or record.lane_status is not None
                or record.lane_detail is not None
                or record.uncovered_tokens
                or record.gap_status is not None
                or record.gap_detail is not None
            ):
                raise EngineJobConflictError("WorkUnit 已进入对账或写入阶段，不能重建目录边界")

        blockers = [
            self.state_root / f"gap_ledger_{job.id}.json",
            self.state_root / f"replenishment_{job.id}.json",
            self.state_root / "gaps" / job.id,
            self.state_root / "staging" / job.id,
            self.state_root / "archive-staging" / job.id,
            self.state_root / "replenishment_workspace" / job.id,
            self.state_root / "subtitle_replenishment_workspace" / job.id,
        ]
        acceptance_path = self.state_root / f"work_acceptance_{job.id}.json"
        if prewrite_failure is None and not self._has_proven_empty_work_acceptance(job.id):
            blockers.append(acceptance_path)
        blockers.extend(
            self.state_root / f"episode_map_{record.work_unit_id}.json"
            for record in records
        )
        if any(path.exists() or path.is_symlink() for path in blockers):
            raise EngineJobConflictError("任务已有写后、缺口或 staging 证据，不能重建目录边界")

        # ``plan_job`` performs archive preprocessing before it persists the
        # internal carrier.  A crash in that narrow window must not look like
        # a harmless zero-file failure and let B/W forget task-owned staging.
        # Probe both deterministic local state and the exact remote staging
        # root; unknown remote state is a blocker, never evidence of absence.
        for record in records:
            try:
                carrier_id = f"unit-{record.work_unit_id}"
                carrier_path = runner._job_path(carrier_id)  # noqa: SLF001 - exact carrier key
                local_archive, remote_archive = runner._archive_task_roots(carrier_id)  # noqa: SLF001 - exact archive key
                remote_kind = runner._remote_entry_kind(remote_archive)  # noqa: SLF001 - exact remote readback
            except Exception as exc:
                raise EngineJobConflictError("无法验证 WorkUnit 写前载体或 staging，不能重建目录边界") from exc
            if carrier_path.exists() or carrier_path.is_symlink():
                raise EngineJobConflictError("任务已有 WorkUnit 内部载体，不能重建目录边界")
            if local_archive.exists() or local_archive.is_symlink():
                raise EngineJobConflictError("任务已有 WorkUnit 归档 staging，不能重建目录边界")
            if remote_kind != "missing":
                raise EngineJobConflictError("WorkUnit 远端归档 staging 未证实为空，不能重建目录边界")

        for candidate in runner.list_jobs():
            candidate_summary = candidate.summary if isinstance(candidate.summary, Mapping) else {}
            if candidate_summary.get("root_job_id") == job.id:
                raise EngineJobConflictError("任务已有内部 child，不能重建目录边界")
        return records, prewrite_failure

    def _assert_boundary_rebuild_binding(self, root_job_id: str, source: str) -> str:
        """Prove the selected root still owns this exact direct intake child."""
        normalized = self._validate_automatic_source(source)
        from engine.scrapeflow.intake_source import load_intake_catalog

        try:
            bindings = [
                item
                for item in load_intake_catalog(self.state_root)
                if item.root_task_id == root_job_id
                and item.canonical_path == normalized
            ]
        except Exception as exc:
            raise EngineJobConflictError("IntakeSource 绑定无法验证，不能重建目录边界") from exc
        if len(bindings) != 1:
            raise EngineJobConflictError("RootJob 与待刮削来源绑定不一致，不能重建目录边界")
        return normalized

    def rebuild_public_job_boundaries(
        self,
        job_id: str,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        """Re-run B/W for a selected paused root without touching media.

        This is a one-off recovery surface for a demonstrably wrong directory
        boundary.  It neither changes the S-step shelf selection nor resumes
        the root.  Fresh AList discovery is completed before any local state is
        replaced, and the ledger is persisted through its empty safety marker
        so an interrupted operation cannot pair old WorkUnits with a new
        snapshot.
        """
        if not isinstance(payload, Mapping) or payload:
            raise EngineRequestError("目录边界重建请求必须是空 JSON 对象")
        selected = self._validate_selected_root_job(job_id)
        control = self.control()
        if control.get("paused") is not True or control.get("root_job_id") != selected:
            raise EngineRequestError("目录边界重建必须在当前已选 RootJob 的暂停态执行")

        from engine.scrapeflow.root_boundaries import (
            build_root_boundary_analysis,
            persist_root_boundary_analysis,
        )
        from local.scrapeflow_api.root_pipeline import is_intake_bound_root

        with self._automatic_lock:
            active = self._worker_future
            if active is not None and not active.done():
                raise EngineWorkerBusyError("任务仍有活动 worker，不能重建目录边界")
            runner = self._get_engine_runner()
            before = runner.get_job(selected)
            if not is_intake_bound_root(self.state_root, selected):
                raise EngineRequestError("目录边界重建只支持 IntakeSource 创建的 RootJob")
            old_records, _prewrite_failure = self._assert_boundary_rebuild_safe(
                runner, before,
            )
            source = self._assert_boundary_rebuild_binding(
                selected,
                runner._job_ingress_source(before),  # noqa: SLF001 - root composition
            )
            if not runner.source_directory_exists(source):
                raise EngineJobConflictError("待刮削来源目录已不存在，不能重建目录边界")

            # The following read-only pass is deliberately outside the writer
            # lock.  The application lock prevents this process from starting
            # a root worker, and the final lock/check below catches another
            # process changing the durable root while AList is being read.
            snapshot, records = build_root_boundary_analysis(
                runner.alist,
                source,
                root_task_id=selected,
                source_revision=max(record.source_revision for record in old_records) + 1,
            )
            if not records:
                raise EngineJobConflictError("新的目录分析未发现可验证作品单元，已保留原状态")

            control = self.control()
            if control.get("paused") is not True or control.get("root_job_id") != selected:
                raise EngineJobConflictError("控制状态已变化，不能重建目录边界")
            with runner.worker_lock():
                latest = runner.get_job(selected)
                _final_records, final_prewrite_failure = self._assert_boundary_rebuild_safe(
                    runner,
                    latest,
                    expected_updated_at=before.updated_at,
                )
                self._assert_boundary_rebuild_binding(
                    selected,
                    runner._job_ingress_source(latest),  # noqa: SLF001 - root composition
                )
                control = self.control()
                if control.get("paused") is not True or control.get("root_job_id") != selected:
                    raise EngineJobConflictError("控制状态已变化，不能重建目录边界")
                persist_root_boundary_analysis(
                    self.state_root,
                    selected,
                    snapshot,
                    records,
                )
                if final_prewrite_failure is not None:
                    # This exact receipt was freshly proved to represent a
                    # no-carrier, zero-file failure.  It belongs to the old
                    # B/W generation and would otherwise make the new pending
                    # WorkUnit look failed.  Do not write ``[]``: a missing
                    # receipt is the normal pre-D state and keeps later
                    # rebuild gates meaningful.
                    final_prewrite_failure[0].unlink()
                rebuilt = replace(
                    latest,
                    phase="queued",
                    updated_at=_now(),
                    plan={},
                    execution=None,
                    error=None,
                )
                atomic_write_json(
                    runner._job_path(selected),  # noqa: SLF001 - root state boundary
                    _redacted_job_payload(rebuilt),
                    allow_nan=False,
                )
        return self.public_engine_job(rebuilt)

    def _persist_retry_transition(
        self,
        expected: EngineJob,
        updated: EngineJob,
    ) -> EngineJob:
        """Persist one retry transition only if its source state is current."""
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

    def public_engine_job(self, job: EngineJob) -> dict[str, object]:
        """Return the small public view for one local RootJob."""
        summary = dict(job.summary) if isinstance(job.summary, Mapping) else {}
        plan_body = dict(job.plan) if isinstance(job.plan, Mapping) else {}
        aggregate: dict[str, object] | None = None
        # Older roots can have an Engine ``completed`` phase from a time when
        # J's open ledger was not part of the public phase.  Project the
        # authoritative RootJob aggregate here rather than editing historical
        # job JSON just to make a dashboard accurate.  This is local-only and
        # never triggers an AList, TMDB, provider, or writer operation.
        if job.phase in {"executed", "completed", "gaps_pending"}:
            try:
                from local.scrapeflow_api.root_pipeline import is_intake_bound_root

                if is_intake_bound_root(self.state_root, job.id):
                    aggregate = aggregate_root_job(self.state_root, job.id).as_dict()
            except Exception:
                # A malformed historical ledger must not make an ordinary job
                # response fail.  Its Engine phase remains visible and the
                # next explicit RootJob run will fail closed if necessary.
                aggregate = None
        phase = {
            "reconciliation_uncertain": "needs_attention",
            "planned": "queued",
            "executing": "executing_media",
            "executed": "completed",
        }.get(job.phase, job.phase)
        if aggregate is not None:
            aggregate_status = str(aggregate.get("status") or "")
            if aggregate_status == "gaps_pending":
                phase = "gaps_pending"
            elif aggregate_status == "needs_attention":
                phase = "needs_attention"
            elif aggregate_status == "failed":
                phase = "failed"
            elif aggregate_status == "in_progress":
                phase = "needs_attention"
        media_readback_verified = job.phase in {"executed", "completed", "gaps_pending"}
        open_gaps = int(aggregate.get("open_gaps") or 0) if aggregate is not None else 0
        payload = job.as_dict()
        payload.update({
            "phase": phase,
            "engine_phase": job.phase,
            "aggregate": aggregate,
            "source": summary.get("ingress_source_path") or summary.get("source_root"),
            "target_shelf": job.target_shelf,
            "target_root": job.target_root,
            "target_work_path": summary.get("target_root"),
            "selected_at": job.selected_at,
            "allowed_target_shelves": (
                list(target_shelf_values())
                if job.phase == "awaiting_target_shelf"
                else []
            ),
            "plan": {
                "kind": "media",
                "title": summary.get("title"),
                "tmdb_id": summary.get("tmdb_id"),
                "source_root": summary.get("source_root"),
                "target_root": summary.get("target_root"),
                "file_count": summary.get("file_count"),
                "warnings": list(plan_body.get("warnings") or []),
                "problem_files": list(plan_body.get("problem_files") or []),
                "notices": list(plan_body.get("notices") or []),
            },
            "plan_body": plan_body,
            "readback": {
                # A gap-pending root has a successful initial H readback;
                # do not misrepresent that evidence as pending just because
                # its J/N closure remains open.
                "status": "verified" if media_readback_verified else "pending",
                "checked_at": job.updated_at if media_readback_verified else None,
            },
            "progress": {
                "stage": phase,
                "completed": 1 if media_readback_verified else 0,
                "total": int(summary.get("file_count") or 0),
                "percent": 100 if media_readback_verified else 0,
                "message": (
                    "已完成，正式库路径和大小已回读"
                    if phase == "completed"
                    else f"正式库已回读；仍有 {open_gaps} 个缺口待补源"
                    if phase == "gaps_pending"
                    else "等待人工确认"
                    if phase == "needs_attention"
                    else "等待人工重试"
                    if phase == "retry_wait"
                    else job.error or phase
                ),
            },
            "settings": {
                "media_type": summary.get("mode"),
                "tmdb_id": summary.get("tmdb_id"),
            },
        })

        redacted = redact_value(payload)
        return dict(redacted) if isinstance(redacted, Mapping) else payload

    def control(self) -> dict[str, object]:
        """Return the exact two-field local control record."""
        return self._control_state.read()

    def set_paused(
        self,
        paused: bool,
        reason: str | None = None,
    ) -> dict[str, object]:
        """Change only the pause bit; the selected RootJob is preserved."""
        del reason
        if type(paused) is not bool:
            raise TypeError("paused must be boolean")
        with self._automatic_lock:
            payload = self._control_state.set(paused=paused)
        if not paused:
            self._resume_after_control_open()
        return payload

    def close(self) -> None:
        """Stop this process's local worker threads."""
        if self._closed.is_set():
            return
        self._closed.set()
        if self._worker_executor is not None:
            self._worker_executor.shutdown(wait=True, cancel_futures=True)

def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _safe_remote_root(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "\x00" in value or "\\" in value:
        raise ApplicationError("SCRAPEFLOW_MEDIA_ROOT 必须是安全的绝对路径")
    normalized = posixpath.normpath(value)
    if normalized == "/" or normalized != value.rstrip("/"):
        raise ApplicationError("SCRAPEFLOW_MEDIA_ROOT 必须是规范化的媒体库路径")
    return normalized


class SimpleHandler(BaseHTTPRequestHandler):
    """JSON API for local RootJobs, controls, and browsing."""

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
        # The read face used to skip the local/origin gate entirely: a DNS
        # rebinding page (or any client with a foreign Host) could read the
        # full job/intake/browse metadata although every POST was blocked.
        # Reads now enforce the same loopback-authority rule as writes; the
        # Docker healthcheck is a bare TCP connect and is unaffected.
        if self._reject_nonlocal_request():
            return
        try:
            if path in {"/", "/index.html"}:
                self._send_html(200, dashboard_html())
            elif path == "/api/health":
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
            elif path.startswith("/api/jobs/") and path.count("/") == 4 and path.endswith("/work-units"):
                pieces = path.split("/")
                job_id = urllib.parse.unquote(pieces[3])
                if self.application.maybe_engine_job(job_id) is None:
                    raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
                self._send(200, self.application.work_units_view(job_id))
            elif path == "/api/browse":
                browse_path = (query.get("path") or [self.application.remote_root])[0]
                refresh = (query.get("refresh") or ["0"])[0] == "1"
                self._send(200, self.application.browse(browse_path, refresh=refresh))
            elif path.startswith("/api/jobs/") and path.endswith("/replenishment"):
                pieces = path.split("/")
                if len(pieces) != 5:
                    self._send(404, {"error": "not found"})
                    return
                job_id = urllib.parse.unquote(pieces[3])
                if self.application.maybe_engine_job(job_id) is None:
                    raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
                self._send(200, self.application.replenishment_view(job_id))
            elif path == "/api/intake":
                self._send(200, {"sources": self.application.intake_catalog()})
            elif path in {"/api/batch", "/api/batch/status"}:
                self._send(200, self.application.batch_manifest_view())
            elif path.startswith("/api/replacements/") and path.count("/") == 3:
                manifest_id = urllib.parse.unquote(path.rsplit("/", 1)[1])
                self._send(200, self.application.replacement_manifest_view(manifest_id))
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
            if path == "/api/root-jobs":
                job = self.application.create_root_task(payload)
                self._send(201, {"job": self.application.public_engine_job(job)})
            elif path == "/api/intake/refresh":
                self._send(200, self.application.refresh_intake_catalog())
            elif path in {"/api/batch", "/api/batch/authorize"}:
                self._send(201, self.application.authorize_batch_items(payload))
            elif path == "/api/batch/retry":
                self._send(200, self.application.retry_batch_item(payload))
            elif path == "/api/replacements":
                raise EngineRequestError("replacement manifest 只能由服务端 fresh 盘点生成")
            elif path == "/api/control/pause":
                self._send(200, self.application.set_paused(True, self._optional_reason(payload)))
            elif path == "/api/control/select":
                self._send(
                    200,
                    self.application.select_root_job(
                        self._root_job_id(payload, required=True),
                    ),
                )
            elif path == "/api/control/clear-orphan-selection":
                if payload:
                    raise EngineRequestError("清除孤儿选择不接受参数")
                self._send(200, self.application.clear_orphan_selection())
            elif path == "/api/control/resume":
                self._send(
                    200,
                    self.application.resume_selected_root_job(
                        self._root_job_id(payload, required=False),
                    ),
                )
            elif path.startswith("/api/jobs/") and path.endswith("/confirm"):
                pieces = path.split("/")
                if len(pieces) != 7:
                    self._send(404, {"error": "not found"})
                    return
                job_id = urllib.parse.unquote(pieces[3])
                unit_id = urllib.parse.unquote(pieces[5])
                if self.application.maybe_engine_job(job_id) is None:
                    raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}")
                self._send(
                    200,
                    {"unit": self.application.confirm_work_unit(job_id, unit_id, payload)},
                )
                return
            elif path.startswith("/api/jobs/"):
                pieces = path.split("/")
                if len(pieces) != 5:
                    self._send(404, {"error": "not found"})
                    return
                job_id, operation = urllib.parse.unquote(pieces[3]), pieces[4]
                if operation == "retry":
                    self._send(200, {"job": self.application.retry_public_job(job_id, payload)})
                    return
                if operation == "reopen-orphan":
                    self._send(
                        200,
                        {"job": self.application.public_engine_job(
                            self.application.reopen_orphan_root_task(job_id, payload),
                        )},
                    )
                    return
                if operation == "rebuild-boundaries":
                    self._send(
                        200,
                        {"job": self.application.rebuild_public_job_boundaries(job_id, payload)},
                    )
                    return
                if operation == "rebuild-uncertain-units":
                    self._send(
                        200,
                        {"job": self.application.rebuild_uncertain_unit_boundaries(job_id, payload)},
                    )
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
                if operation == "consume-source":
                    self._send(
                        200,
                        self.application.consume_source_public_job(job_id, payload),
                    )
                    return
                if operation == "file-disc-ruling":
                    self._send(
                        200,
                        self.application.file_disc_ruling_public_job(job_id, payload),
                    )
                    return
                if operation == "repair-artifacts":
                    self._send(
                        200,
                        {"job": self.application.repair_public_job_artifacts(job_id, payload)},
                    )
                    return
                if operation == "replenish":
                    self._send(
                        200,
                        self.application.trigger_root_replenishment(job_id),
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

    @staticmethod
    def _root_job_id(
        payload: Mapping[str, object],
        *,
        required: bool,
    ) -> str | None:
        """Parse the selected-RootJob request surface."""
        unknown = set(payload) - {"root_job_id"}
        if unknown:
            raise ValueError("控制请求只接受 root_job_id")
        value = payload.get("root_job_id")
        if value is None:
            if required:
                raise ValueError("必须提供 root_job_id")
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("root_job_id 必须是非空字符串")
        return value

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

    def _send_html(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'")
        self.end_headers()
        self.wfile.write(body)

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
