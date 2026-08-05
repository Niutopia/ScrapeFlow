"""Small in-process FIFO scheduler for analysis and remote mutations."""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal


PoolName = Literal["analysis", "execution"]
PauseReader = Callable[[], bool]


@dataclass
class ScheduledWork:
    sequence: int
    pool: PoolName
    job: Any
    target: Callable[..., None]
    args: tuple[Any, ...]
    resources: frozenset[str]


class FifoScheduler:
    """Run bounded analysis and conflict-aware execution pools in FIFO order."""

    def __init__(
        self, *, analysis_workers: int = 4, execution_workers: int = 1,
        pause_reader: PauseReader | None = None,
    ) -> None:
        if (
            isinstance(analysis_workers, bool)
            or not isinstance(analysis_workers, int)
            or not 1 <= analysis_workers <= 8
        ):
            raise ValueError("分析并发数必须是 1–8 之间的整数")
        if (
            isinstance(execution_workers, bool)
            or not isinstance(execution_workers, int)
            or not 1 <= execution_workers <= 4
        ):
            raise ValueError("执行并发数必须是 1–4 之间的整数")
        if pause_reader is not None and not callable(pause_reader):
            raise ValueError("pause_reader must be callable")
        self.analysis_workers = analysis_workers
        self.execution_workers = execution_workers
        self._pause_reader = pause_reader or (lambda: False)
        self._condition = threading.Condition()
        self._queues: dict[PoolName, deque[ScheduledWork]] = {
            "analysis": deque(),
            "execution": deque(),
        }
        self._sequence = 0
        self._started = False
        self._stopping = False
        self._threads: list[threading.Thread] = []
        self._active: dict[int, ScheduledWork] = {}

    def submit(
        self,
        pool: PoolName,
        job: Any,
        target: Callable[..., None],
        *args: Any,
        resources: Iterable[str] = (),
    ) -> int:
        with self._condition:
            if self._stopping:
                raise RuntimeError("任务调度器正在停止")
            if any(item.job.id == job.id for item in self._queues[pool]) or any(
                item.pool == pool and item.job.id == job.id
                for item in self._active.values()
            ):
                raise ValueError("任务已经在队列中")
            self._sequence += 1
            work = ScheduledWork(
                self._sequence,
                pool,
                job,
                target,
                args,
                frozenset(str(resource) for resource in resources if str(resource)),
            )
            self._queues[pool].append(work)
            self._refresh_positions_locked(pool)
            self._condition.notify_all()
            return int(job.queue_position)

    def start(self, runner: Callable[..., None]) -> None:
        with self._condition:
            if self._started:
                return
            self._started = True
            self._stopping = False
            specs = [("analysis", index + 1) for index in range(self.analysis_workers)]
            specs.extend(("execution", index + 1) for index in range(self.execution_workers))
            for pool, index in specs:
                thread = threading.Thread(
                    target=self._worker,
                    args=(pool, runner),
                    name=f"scrapeflow-{pool}-{index}",
                    daemon=True,
                )
                self._threads.append(thread)
                thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        for thread in self._threads:
            thread.join(timeout=timeout)
        self._threads = [thread for thread in self._threads if thread.is_alive()]
        if not self._threads:
            self._started = False

    def pending(self, pool: PoolName) -> list[str]:
        with self._condition:
            return [item.job.id for item in self._ordered_queue_locked(pool)]

    def active_jobs(self) -> list[Any]:
        """Return a stable snapshot of work currently owned by workers.

        A scheduled job can be active while it is between subprocesses, so a
        service shutdown cannot safely infer active work from ``job.process``
        alone.  Callers receive only the job objects and cannot mutate the
        scheduler's internal work registry.
        """
        with self._condition:
            return [item.job for item in self._active.values()]

    def wake(self) -> None:
        """Recheck externally owned dispatch state without storing a copy."""
        with self._condition:
            self._condition.notify_all()

    def cancel_pending(self, job_id: str) -> bool:
        """Remove queued work without interrupting an already active worker."""
        with self._condition:
            for pool, queue in self._queues.items():
                for index, work in enumerate(queue):
                    if work.job.id != job_id:
                        continue
                    del queue[index]
                    work.job.queue_position = None
                    work.job.queue_kind = None
                    self._refresh_positions_locked(pool)
                    self._condition.notify_all()
                    return True
        return False

    def clear(self) -> None:
        """Clear pending work before tests or before rebuilding restored queues."""
        with self._condition:
            if self._started:
                raise RuntimeError("运行中的调度器不能清空")
            for pool, queue in self._queues.items():
                for item in queue:
                    item.job.queue_position = None
                    item.job.queue_kind = None
                queue.clear()
                self._refresh_positions_locked(pool)
            self._stopping = False

    @staticmethod
    def _resources_overlap(left: frozenset[str], right: frozenset[str]) -> bool:
        for left_path in left:
            normalized_left = left_path.rstrip("/").casefold() or "/"
            for right_path in right:
                normalized_right = right_path.rstrip("/").casefold() or "/"
                left_prefix = "/" if normalized_left == "/" else normalized_left + "/"
                right_prefix = "/" if normalized_right == "/" else normalized_right + "/"
                if (
                    normalized_left == normalized_right
                    or normalized_left.startswith(right_prefix)
                    or normalized_right.startswith(left_prefix)
                ):
                    return True
        return False

    @staticmethod
    def _dispatch_priority(work: ScheduledWork) -> int:
        """Keep new media analysis ahead of a title's delayed source review.

        Replenishment work can legitimately block on slow provider searches.
        Letting restored title retries fill every analysis worker makes newly
        submitted media wait behind background source checks.
        FIFO remains strict within each class and execution work is unchanged.
        """
        if work.pool == "analysis" and getattr(work.job, "phase", None) in {
            "queued", "starting_archive_execution",
        }:
            return 0
        return 1

    def _ordered_queue_locked(self, pool: PoolName) -> list[ScheduledWork]:
        return sorted(
            self._queues[pool],
            key=lambda work: (self._dispatch_priority(work), work.sequence),
        )

    def _take_runnable_locked(self, pool: PoolName) -> ScheduledWork | None:
        if self._pause_reader():
            return None
        # Mutation-capable title finalization runs in the analysis pool, while
        # ordinary media writes run in execution.  Path leases therefore span
        # both pools; pool-local comparison allowed overlapping writes.
        active_resources = [item.resources for item in self._active.values()]
        earlier_resources: list[frozenset[str]] = []
        for work in self._ordered_queue_locked(pool):
            active_conflict = any(
                self._resources_overlap(work.resources, resources)
                for resources in active_resources
            )
            fifo_conflict = any(
                self._resources_overlap(work.resources, resources)
                for resources in earlier_resources
            )
            if not active_conflict and not fifo_conflict:
                self._queues[pool].remove(work)
                self._active[work.sequence] = work
                work.job.queue_position = None
                work.job.queue_kind = None
                self._refresh_positions_locked(pool)
                return work
            earlier_resources.append(work.resources)
        return None

    def _refresh_positions_locked(self, pool: PoolName) -> None:
        for position, work in enumerate(self._ordered_queue_locked(pool), start=1):
            work.job.queue_position = position
            work.job.queue_kind = pool

    def _worker(self, pool: PoolName, runner: Callable[..., None]) -> None:
        while True:
            with self._condition:
                work = self._take_runnable_locked(pool)
                while work is None and not self._stopping:
                    self._condition.wait()
                    work = self._take_runnable_locked(pool)
                if self._stopping:
                    return
            try:
                runner(work.target, work.job, *work.args)
            finally:
                with self._condition:
                    self._active.pop(work.sequence, None)
                    self._condition.notify_all()
