"""Persistent background queue for subtitle source-manifest discovery."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import threading
import uuid
from typing import Any, Callable, ContextManager, Mapping

from engine.scrapeflow.subtitle_member_acquisition import validate_source_manifest
from engine.tools.subtitle_executor import canonical_digest


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime | None = None) -> str:
    return (value or _now()).isoformat()


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.chmod(temporary, 0o600)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _dispatch_gate(control: Mapping[str, Any]) -> dict[str, Any]:
    """Read only the sole durable pause flag; reject malformed state closed."""
    paused = control.get("paused")
    if type(paused) is not bool:
        return {"allowed": False, "reason": "global_pause_state_invalid"}
    return {
        "allowed": not paused,
        "reason": None if not paused else "global_pause_active",
    }


class SubtitleSourceDiscoveryRuntime:
    """Crash-safe discovery workers backed by one atomic durable queue."""

    # Version 3 reserves source-manifest capacity across every compacted query
    # and keeps capped optional-source pages moving in the background.  Older
    # completed tasks may have stopped after one broad feed page, so the same
    # request refreshes them once under the current semantics.
    VERSION = 3

    def __init__(
        self, queue_root: Path, manifest_root: Path, *, poll_seconds: float = 30.0,
        retry_seconds: int = 300, refresh_seconds: int = 21_600,
        worker_count: int = 1, allow_legacy_tasks: bool = True,
    ) -> None:
        if (
            poll_seconds <= 0
            or retry_seconds < 1
            or refresh_seconds < 60
            or type(worker_count) is not int
            or not 1 <= worker_count <= 16
        ):
            raise ValueError("subtitle discovery timing is invalid")
        self.queue_root = queue_root
        self.manifest_root = manifest_root
        self.poll_seconds = float(poll_seconds)
        self.retry_seconds = retry_seconds
        self.refresh_seconds = refresh_seconds
        self.worker_count = worker_count
        self.allow_legacy_tasks = allow_legacy_tasks
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._threads: list[threading.Thread] = []
        self._active_worker_count = 0
        self._recovery_complete = False

    def _task_path(self, owner_job_id: str, batch_id: str) -> Path:
        task_id = canonical_digest({
            "owner_job_id": owner_job_id, "search_batch_id": batch_id,
        })[:24]
        return self.queue_root / f"{task_id}.json"

    def _recover_interrupted(self) -> None:
        if not self.queue_root.exists():
            return
        for path in self.queue_root.glob("*.json"):
            try:
                row = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(row, dict) and row.get("status") == "running":
                    row["status"] = "retryable"
                    row["last_error"] = "worker_interrupted_before_terminal_commit"
                    row["next_attempt_at"] = _stamp()
                    row["updated_at"] = _stamp()
                    _atomic_json(path, row)
            except (OSError, ValueError, json.JSONDecodeError):
                continue

    @staticmethod
    def _validate_search(search: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        core = {key: search.get(key) for key in (
            "schema_version", "kind", "batches", "ambiguity_cases",
        )}
        if (
            search.get("schema_version") != 1
            or search.get("kind") != "subtitle_search_batches"
            or canonical_digest(core) != search.get("search_batches_sha256")
            or not isinstance(search.get("batches"), list)
        ):
            raise ValueError("subtitle search batch digest is invalid")
        return [row for row in search["batches"] if isinstance(row, Mapping)]

    def enqueue(
        self, search: Mapping[str, Any], *, owner_job_id: str = "standalone",
    ) -> dict[str, int]:
        if (
            not isinstance(owner_job_id, str)
            or not owner_job_id
            or len(owner_job_id) > 64
        ):
            raise ValueError("subtitle discovery owner is invalid")
        created = existing = reopened = archived = 0
        batches = self._validate_search(search)
        with self._lock:
            for batch in batches:
                batch_id = str(batch.get("search_batch_id") or "")
                core = {key: batch.get(key) for key in (
                    "target_root", "title", "aliases", "request_ids", "query_terms", "providers",
                    "manifest_requirement", "member_policy",
                )}
                if len(batch_id) != 24 or canonical_digest(core)[:24] != batch_id:
                    raise ValueError("subtitle search batch id is invalid")
                path = self._task_path(owner_job_id, batch_id)
                if path.exists():
                    current = json.loads(path.read_text(encoding="utf-8"))
                    if (
                        current.get("batch") != dict(batch)
                        or current.get("owner_job_id") != owner_job_id
                    ):
                        raise ValueError("subtitle search batch id collision")
                    if current.get("status") == "archived_superseded":
                        # A regressed request set reintroduced this batch.
                        # Restore it to pending in place so _due_task selects
                        # it again: manifests, telemetry and the durable
                        # archive evidence stay auditable, only the marker
                        # moves aside for a fresh search.
                        if "archived_at" not in current:
                            current["archived_at"] = str(
                                current.get("updated_at") or _stamp()
                            )
                        if "archived_reason" not in current:
                            current["archived_reason"] = str(
                                current.get("last_error") or "request_set_no_longer_current"
                            )
                        current["status"] = "pending"
                        current["version"] = self.VERSION
                        current["last_error"] = "request_set_returned_to_current_set"
                        current["next_attempt_at"] = _stamp()
                        current["updated_at"] = _stamp()
                        _atomic_json(path, current)
                        reopened += 1
                    elif (
                        current.get("status") == "completed"
                        and (
                            current.get("version")
                            if type(current.get("version")) is int else 0
                        ) < self.VERSION
                    ):
                        # Discovery semantics changed after this durable result
                        # was sealed.  Preserve its manifests as pre-exclusion
                        # evidence, but run the exact same signed batch once
                        # with the current algorithm before treating it as
                        # exhausted again.
                        current["version"] = self.VERSION
                        current["status"] = "retryable"
                        current["last_error"] = (
                            f"discovery_algorithm_upgraded_v{self.VERSION}"
                        )
                        current["next_attempt_at"] = _stamp()
                        current["updated_at"] = _stamp()
                        _atomic_json(path, current)
                        reopened += 1
                    elif current.get("status") == "completed" and not (
                        current.get("manifest_sha256s") or []
                    ):
                        # A complete provider pass with no manifest is valid
                        # evidence for that moment, not permanent exhaustion.
                        # Only the same title/request enqueue may reopen it,
                        # and only after the durable refresh deadline.
                        try:
                            refresh_due = datetime.fromisoformat(
                                str(current.get("next_attempt_at") or "")
                            ) <= _now()
                        except ValueError:
                            refresh_due = True
                        if refresh_due:
                            current["status"] = "retryable"
                            current["last_error"] = "periodic_no_manifest_refresh"
                            current["next_attempt_at"] = _stamp()
                            current["updated_at"] = _stamp()
                            _atomic_json(path, current)
                            reopened += 1
                        else:
                            existing += 1
                    else:
                        existing += 1
                    continue
                now = _stamp()
                _atomic_json(path, {
                    "version": self.VERSION, "kind": "subtitle_source_discovery_task",
                    "batch_id": batch_id, "owner_job_id": owner_job_id,
                    "batch": dict(batch), "status": "pending",
                    "attempts": 0, "manifest_sha256s": [], "provider_telemetry": {},
                    "last_error": None, "next_attempt_at": now,
                    "created_at": now, "updated_at": now,
                })
                created += 1
            ambiguity_root = self.queue_root / "ambiguities"
            for case in search.get("ambiguity_cases", []):
                if not isinstance(case, Mapping):
                    raise ValueError("subtitle ambiguity case is invalid")
                core = {key: case.get(key) for key in (
                    "request_id", "candidate_ids", "candidate_evidence", "resolution",
                )}
                case_id = str(case.get("ambiguity_case_id") or "")
                if len(case_id) != 24 or canonical_digest(core)[:24] != case_id:
                    raise ValueError("subtitle ambiguity case digest is invalid")
                path = ambiguity_root / f"{case_id}.json"
                if path.exists():
                    continue
                now = _stamp()
                _atomic_json(path, {
                    "version": self.VERSION,
                    "kind": "subtitle_ambiguity_evidence_task",
                    "ambiguity_case_id": case_id,
                    **core,
                    "status": "awaiting_deterministic_evidence",
                    "created_at": now, "updated_at": now,
                })
        if created or reopened:
            self._wake.set()
        return {
            "created": created, "existing": existing, "reopened": reopened,
            "archived": archived,
        }

    def _due_task(self) -> tuple[Path, dict[str, Any]] | None:
        now = _now()
        candidates: list[tuple[tuple[Any, ...], Path, dict[str, Any]]] = []
        for path in self.queue_root.glob("*.json") if self.queue_root.exists() else []:
            try:
                row = json.loads(path.read_text(encoding="utf-8"))
                due = datetime.fromisoformat(str(row.get("next_attempt_at") or ""))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            status = row.get("status")
            if not self.allow_legacy_tasks and not isinstance(
                row.get("owner_job_id"), str,
            ):
                continue
            if status not in {"pending", "retryable"} or due > now:
                continue
            # A persistently unavailable provider must not let a small batch ID
            # retry forever ahead of batches that have never been searched.
            # Drain first attempts first, then rotate retries by their durable
            # due time instead of the filename chosen by a content hash.
            try:
                created = datetime.fromisoformat(str(row.get("created_at") or ""))
            except ValueError:
                created = due
            candidates.append(((
                0 if status == "pending" else 1,
                created if status == "pending" else due,
                int(row.get("attempts") or 0),
                path.name,
            ), path, row))
        if candidates:
            _, path, row = min(candidates, key=lambda item: item[0])
            return path, row
        return None

    def reopen_for_request(self, request_id: str, *, reason: str) -> int:
        """Immediately refresh completed searches after a candidate expires."""
        if not request_id or not reason:
            raise ValueError("subtitle discovery reopen identity is invalid")
        reopened = 0
        with self._lock:
            for path in sorted(self.queue_root.glob("*.json")) if self.queue_root.exists() else []:
                try:
                    task = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError, json.JSONDecodeError):
                    continue
                batch = task.get("batch")
                if (
                    task.get("status") != "completed" or not isinstance(batch, Mapping)
                    or request_id not in (batch.get("request_ids") or [])
                ):
                    continue
                task["status"] = "retryable"
                task["last_error"] = reason
                task["next_attempt_at"] = _stamp()
                task["updated_at"] = _stamp()
                _atomic_json(path, task)
                reopened += 1
        if reopened:
            self._wake.set()
        return reopened

    def run_once(
        self, discover_batch: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        read_control: Callable[[], Mapping[str, Any]],
        *, item_guard: Callable[[], ContextManager[Any]] | None = None,
    ) -> dict[str, Any]:
        gate = _dispatch_gate(dict(read_control()))
        if not gate["allowed"]:
            return {"ran": False, "reason": gate["reason"]}
        guard = item_guard or nullcontext
        try:
            with guard():
                if not _dispatch_gate(dict(read_control()))["allowed"]:
                    return {"ran": False, "reason": "global_pause_activated_before_discovery_start"}
                with self._lock:
                    # Recovery is intentionally deferred until dispatch is open
                    # and protected by the same pause-transition guard.  Merely
                    # importing or starting the API while paused is zero-write.
                    if not self._recovery_complete:
                        self._recover_interrupted()
                        self._recovery_complete = True
                    selected = self._due_task()
                    if selected is None:
                        return {"ran": False, "reason": "queue_empty_or_not_due"}
                    path, task = selected
                    task["version"] = self.VERSION
                    task["status"] = "running"
                    task["attempts"] = int(task.get("attempts") or 0) + 1
                    task["updated_at"] = _stamp()
                    _atomic_json(path, task)
        except Exception as exc:
            return {"ran": False, "reason": f"dispatch_start_blocked:{type(exc).__name__}"}
        with self._lock:
            self._active_worker_count += 1
        try:
            result = discover_batch(task["batch"])
            if not isinstance(result, Mapping) or not isinstance(result.get("manifests"), list):
                raise ValueError("subtitle discovery runner returned invalid result")
            manifests = [validate_source_manifest(row) for row in result["manifests"]]
            with guard():
                if not _dispatch_gate(dict(read_control()))["allowed"]:
                    raise RuntimeError("global_pause_activated_before_discovery_commit")
                for manifest in manifests:
                    _atomic_json(self.manifest_root / f"{manifest['manifest_sha256']}.json", manifest)
                complete = result.get("search_complete") is True
                task["manifest_sha256s"] = sorted(set([
                    *task.get("manifest_sha256s", []),
                    *(str(row["manifest_sha256"]) for row in manifests),
                ]))
                provider_telemetry = dict(result.get("provider_telemetry") or {})
                previous_telemetry = task.get("provider_telemetry")
                if isinstance(previous_telemetry, Mapping):
                    # Candidate failures are durable pre-exclusion evidence.
                    # Replacing this list on every retry makes capped provider
                    # pages alternate forever: page A excludes page B, then B
                    # excludes A.  Preserve the union while leaving all other
                    # counters as current-attempt telemetry.
                    for provider, current in list(provider_telemetry.items()):
                        previous = previous_telemetry.get(provider)
                        if not isinstance(current, Mapping) or not isinstance(previous, Mapping):
                            continue
                        merged = dict(current)
                        merged["resource_failed_locators"] = sorted(set([
                            *(str(value) for value in previous.get("resource_failed_locators", [])
                              if isinstance(value, str) and value),
                            *(str(value) for value in current.get("resource_failed_locators", [])
                              if isinstance(value, str) and value),
                        ]))
                        provider_telemetry[provider] = merged
                task["provider_telemetry"] = provider_telemetry
                torrent_telemetry = provider_telemetry.get("torrent")
                capped_optional_sources = (
                    torrent_telemetry.get("optional_sources_capped")
                    if isinstance(torrent_telemetry, Mapping) else None
                )
                optional_pages_remaining = bool(
                    complete and isinstance(capped_optional_sources, list)
                    and all(
                        isinstance(value, str) and value
                        for value in capped_optional_sources
                    )
                    and capped_optional_sources
                )
                task["status"] = (
                    "retryable" if optional_pages_remaining or not complete
                    else "completed"
                )
                task["last_error"] = (
                    "optional_source_pages_remaining" if optional_pages_remaining
                    else None if complete
                    else "provider_search_incomplete_retryable"
                )
                task["next_attempt_at"] = _stamp(_now() + timedelta(
                    seconds=(
                        self.refresh_seconds
                        if complete and not optional_pages_remaining
                        else self.retry_seconds
                    ),
                ))
                task["updated_at"] = _stamp()
                with self._lock:
                    _atomic_json(path, task)
            return {"ran": True, "batch_id": task["batch_id"], "status": task["status"], "manifest_count": len(manifests)}
        except Exception as exc:
            try:
                with guard():
                    if not _dispatch_gate(dict(read_control()))["allowed"]:
                        return {
                            "ran": True, "batch_id": task["batch_id"],
                            "status": "paused_without_terminal_commit",
                        }
                    with self._lock:
                        task["status"] = "retryable"
                        task["last_error"] = f"{type(exc).__name__}: {exc}"
                        task["next_attempt_at"] = _stamp(_now() + timedelta(seconds=self.retry_seconds))
                        task["updated_at"] = _stamp()
                        _atomic_json(path, task)
                return {"ran": True, "batch_id": task["batch_id"], "status": "retryable", "error": task["last_error"]}
            except Exception:
                # The pause transition won the race.  Leave the durable row in
                # running state; the next open dispatch recovers it under the
                # guarded startup path above.
                return {
                    "ran": True, "batch_id": task["batch_id"],
                    "status": "paused_without_terminal_commit",
                }
        finally:
            with self._lock:
                self._active_worker_count -= 1

    def snapshot(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        ambiguity_counts: dict[str, int] = {}
        total = 0
        if self.queue_root.exists():
            for path in self.queue_root.glob("*.json"):
                try:
                    row = json.loads(path.read_text(encoding="utf-8"))
                    status = str(row.get("status") or "invalid")
                except (OSError, ValueError, json.JSONDecodeError):
                    status = "invalid"
                counts[status] = counts.get(status, 0) + 1
                total += 1
        ambiguity_root = self.queue_root / "ambiguities"
        if ambiguity_root.exists():
            for path in ambiguity_root.glob("*.json"):
                try:
                    row = json.loads(path.read_text(encoding="utf-8"))
                    status = str(row.get("status") or "invalid")
                except (OSError, ValueError, json.JSONDecodeError):
                    status = "invalid"
                ambiguity_counts[status] = ambiguity_counts.get(status, 0) + 1
        return {
            "total": total, "status_counts": counts,
            "ambiguity_total": sum(ambiguity_counts.values()),
            "ambiguity_status_counts": ambiguity_counts,
            "running": self._active_worker_count > 0,
            "worker_count": self.worker_count,
            "active_worker_count": self._active_worker_count,
            "worker_threads_alive": sum(
                1 for thread in self._threads if thread.is_alive()
            ),
        }

    def start(
        self, discover_batch: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        read_control: Callable[[], Mapping[str, Any]], *,
        item_guard: Callable[[], ContextManager[Any]] | None = None,
        on_result: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> None:
        with self._lock:
            if any(thread.is_alive() for thread in self._threads):
                return
            self._stop.clear()
            self._wake.clear()
            self._recovery_complete = False
        def loop() -> None:
            while not self._stop.is_set():
                result = self.run_once(discover_batch, read_control, item_guard=item_guard)
                if on_result is not None and result.get("ran") is True:
                    try:
                        on_result(result)
                    except Exception:
                        # Queue state is already durable; a coordinator wake-up
                        # failure must not corrupt or replay provider discovery.
                        pass
                self._wake.wait(self.poll_seconds)
                self._wake.clear()
        threads = [
            threading.Thread(
                target=loop,
                name=f"subtitle-source-discovery-{index + 1}",
                daemon=True,
            )
            for index in range(self.worker_count)
        ]
        with self._lock:
            self._threads = threads
        for thread in threads:
            thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        self._wake.set()
        with self._lock:
            threads = list(self._threads)
        per_thread_timeout = timeout / max(len(threads), 1)
        for thread in threads:
            thread.join(timeout=per_thread_timeout)
