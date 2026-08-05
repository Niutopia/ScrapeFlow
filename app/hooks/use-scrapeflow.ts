"use client";

import { useCallback, useEffect, useState } from "react";
import { ApiRequestError, scrapeFlowApi } from "../core/api-client";
import type {
  BrowseResult, GlobalControl, Health, Job, JobRetryOptions, TargetCategory,
} from "../core/contracts";
import { ACTIVE_PHASES, APPROVAL_PHASES, UNSCRAPED_MEDIA_ROOT } from "../core/job-state";

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

function normalizeJob(job: Job): Job {
  return { ...job, plan: job.plan ?? null };
}

export function useScrapeFlow() {
  const [health, setHealth] = useState<Health | null>(null);
  const [healthError, setHealthError] = useState("");
  const [jobs, setJobs] = useState<Job[]>([]);
  const [control, setControl] = useState<GlobalControl | null>(null);
  const [controlError, setControlError] = useState("");
  const [selected, setSelected] = useState<Job | null>(null);
  const [initializing, setInitializing] = useState(true);
  const [pending, setPending] = useState(false);
  const [queueError, setQueueError] = useState("");
  const [operationError, setOperationError] = useState("");
  const [browser, setBrowser] = useState<BrowseResult | null>(null);
  const [browserPending, setBrowserPending] = useState(false);
  const [createConflict, setCreateConflict] = useState<Job | null>(null);
  const mergeJob = useCallback((incoming: Job) => {
    const next = normalizeJob(incoming);
    setSelected(next);
    setJobs(previous => {
      const existing = previous.findIndex(job => job.id === next.id);
      if (existing < 0) return [next, ...previous];
      return previous.map(job => job.id === next.id ? next : job);
    });
  }, []);

  const refreshHealth = useCallback(async () => {
    try {
      setHealth(await scrapeFlowApi.health());
      setHealthError("");
    } catch (cause) {
      setHealth(null);
      setHealthError(errorMessage(cause, "无法连接本地服务"));
    }
  }, []);

  const refreshJobs = useCallback(async () => {
    try {
      const response = await scrapeFlowApi.jobs();
      const next = response.jobs.map(normalizeJob);
      setJobs(next);
      setQueueError("");
      return next;
    } catch (cause) {
      setQueueError(errorMessage(cause, "无法读取任务队列"));
      return [];
    }
  }, []);

  const refreshControl = useCallback(async () => {
    try {
      setControl(await scrapeFlowApi.control());
      setControlError("");
    } catch (cause) {
      setControlError(errorMessage(cause, "无法读取自动流程控制"));
    }
  }, []);

  const setGlobalPause = useCallback(async (paused: boolean, reason?: string) => {
    try {
      setControl(await scrapeFlowApi[paused ? "pause" : "resume"](reason));
      setControlError("");
      return true;
    } catch (cause) {
      setControlError(errorMessage(cause, paused ? "自动流程暂停失败" : "自动流程恢复失败"));
      return false;
    }
  }, []);

  useEffect(() => {
    let disposed = false;
    const restore = async () => {
      const [, queue] = await Promise.all([
        refreshHealth(), refreshJobs(), refreshControl(),
      ]);
      if (disposed) return;
      const active = queue.find(job => APPROVAL_PHASES.has(job.phase) || job.phase === "recovery_required")
        ?? queue.find(job => ACTIVE_PHASES.has(job.phase))
        ?? queue[0];
      if (active) {
        try {
          const response = await scrapeFlowApi.job(active.id);
          if (!disposed) mergeJob(response.job);
        } catch {
          // The queue still renders; health/error surfaces handle diagnostics.
        }
      }
      if (!disposed) setInitializing(false);
    };
    void restore();
    // Pause/resume initiated elsewhere (or restored after a process restart)
    // reaches the dashboard within one heartbeat.
    const healthTimer = window.setInterval(() => {
      void refreshHealth();
      void refreshControl();
    }, 12000);
    return () => {
      disposed = true;
      window.clearInterval(healthTimer);
    };
  }, [mergeJob, refreshControl, refreshHealth, refreshJobs]);

  const hasActiveJobs = jobs.some(job => ACTIVE_PHASES.has(job.phase));
  const selectedActiveId = selected && ACTIVE_PHASES.has(selected.phase) ? selected.id : null;
  useEffect(() => {
    if (!hasActiveJobs) return;
    let disposed = false;
    let timer = 0;
    const poll = async () => {
      if (!disposed) await refreshJobs();
      // The queue endpoint intentionally omits plans. Refresh the selected live
      // task as well so its stage, candidate and resource details do not freeze
      // at the version that was first expanded.
      if (!disposed && selectedActiveId) {
        try {
          const response = await scrapeFlowApi.job(selectedActiveId);
          if (!disposed) mergeJob(response.job);
        } catch {
          // Queue polling remains authoritative for the row. A later tick will
          // retry the detail without replacing a visible operation error.
        }
      }
      if (!disposed) timer = window.setTimeout(poll, 1000);
    };
    timer = window.setTimeout(poll, 700);
    return () => {
      disposed = true;
      window.clearTimeout(timer);
    };
  }, [hasActiveJobs, mergeJob, refreshJobs, selectedActiveId]);

  const loadJob = async (id: string) => {
    setPending(true);
    setOperationError("");
    try {
      const response = await scrapeFlowApi.job(id);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "无法读取任务详情"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const createJob = async (
    source: string,
    category: TargetCategory | "",
    tmdbId?: number,
    mediaType?: "tv" | "movie",
  ) => {
    const path = source.trim().replace(/\/+$/, "");
    if (!path || path === UNSCRAPED_MEDIA_ROOT) {
      setOperationError(`请选择 ${UNSCRAPED_MEDIA_ROOT} 下的具体媒体目录`);
      return false;
    }
    if (!category) {
      setOperationError("请选择目标分类：番剧、美剧或电影");
      return false;
    }
    if (!health?.connected || !health.tmdb_configured) {
      setOperationError("AList 与 TMDB 就绪后才能创建任务");
      return false;
    }
    setPending(true);
    setOperationError("");
    setCreateConflict(null);
    try {
      const response = await scrapeFlowApi.create(path, category, tmdbId, mediaType);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      if (cause instanceof ApiRequestError && cause.status === 409) {
        const candidate = cause.payload.job;
        if (candidate && typeof candidate === "object" && typeof (candidate as Record<string, unknown>).id === "string") {
          const existing = normalizeJob(candidate as Job);
          mergeJob(existing);
          setCreateConflict(existing);
          setOperationError(`这个目录已有任务 #${existing.id}，未重复创建；已在新建页显示原任务。`);
          return false;
        }
      }
      setOperationError(errorMessage(cause, "无法创建任务"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const browse = async (path: string, refresh = false) => {
    setBrowserPending(true);
    setOperationError("");
    try {
      const result = await scrapeFlowApi.browse(path || UNSCRAPED_MEDIA_ROOT, refresh);
      setBrowser(result);
      return result;
    } catch (cause) {
      setOperationError(errorMessage(cause, "无法读取 AList 目录"));
      return null;
    } finally {
      setBrowserPending(false);
    }
  };

  const approve = async () => {
    if (!selected?.digest) return false;
    setPending(true);
    setOperationError("");
    try {
      const response = await scrapeFlowApi.approve(selected.id, selected.digest);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "计划批准失败"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const recover = async (job = selected) => {
    if (!job) return false;
    setPending(true);
    setOperationError("");
    try {
      const response = await scrapeFlowApi.recover(job.id);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "恢复检查启动失败"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const retry = async (job: Job, options: JobRetryOptions = {}) => {
    setPending(true);
    setOperationError("");
    try {
      const response = await scrapeFlowApi.retry(job.id, options);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "任务重试失败"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const cancel = async (job: Job) => {
    setPending(true);
    setOperationError("");
    try {
      const response = await scrapeFlowApi.cancel(job.id);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "无法安全停止任务"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const keepExisting = async (job: Job) => {
    setPending(true);
    setOperationError("");
    try {
      const response = await scrapeFlowApi.keepExisting(job.id);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "无法保留现有版本"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const remove = async (job: Job) => {
    setPending(true);
    setOperationError("");
    try {
      await scrapeFlowApi.remove(job.id);
      setJobs(previous => previous.filter(item => item.id !== job.id));
      setSelected(previous => previous?.id === job.id ? null : previous);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "无法删除任务记录"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const clearData = async () => {
    setPending(true);
    setOperationError("");
    try {
      await scrapeFlowApi.clearData();
      setJobs([]);
      setSelected(null);
      setBrowser(previous => previous ? {
        ...previous,
        directories: previous.directories.map(directory => ({
          name: directory.name,
          path: directory.path,
        })),
      } : null);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "无法清空本地任务与处理历史"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const error = operationError || queueError;
  const errorSource = operationError ? "operation" as const : queueError ? "queue" as const : null;

  return {
    health, healthError, jobs, selected, initializing, pending, error, errorSource, createConflict,
    browser, browserPending, refreshHealth, refreshJobs, loadJob,
    createJob, browse, approve, recover, retry, cancel, keepExisting, remove, clearData,
    control, controlError, refreshControl, setGlobalPause,
    dismissError: () => setOperationError(""),
  };
}
