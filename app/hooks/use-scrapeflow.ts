"use client";

import { useCallback, useEffect, useState } from "react";
import { ApiRequestError, scrapeFlowApi } from "../core/api-client";
import type { RetryCorrection } from "../core/api-client";
import type {
  BrowseResult, GlobalControl, Health, Job, LibraryAudit, TargetShelf,
} from "../core/contracts";
import { ACTIVE_PHASES, UNSCRAPED_MEDIA_ROOT, isFailed } from "../core/job-state";

function errorMessage(error: unknown, fallback: string) {
  if (error instanceof ApiRequestError && error.status === 504) {
    return "请求超时；系统会根据 AList 实际状态继续核对，请刷新任务查看进度。";
  }
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
  const [libraryAudit, setLibraryAudit] = useState<LibraryAudit | null>(null);
  const [auditPending, setAuditPending] = useState(false);
  const [auditError, setAuditError] = useState("");

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

  const refreshLibraryAudit = useCallback(async () => {
    try {
      const response = await scrapeFlowApi.latestAudit();
      setLibraryAudit(response.audit ?? null);
      setAuditError("");
      return response.audit ?? null;
    } catch (cause) {
      setAuditError(errorMessage(cause, "无法读取最近媒体库审计"));
      return null;
    }
  }, []);

  const runLibraryAudit = useCallback(async () => {
    setAuditPending(true);
    setAuditError("");
    try {
      const response = await scrapeFlowApi.runAudit();
      setLibraryAudit(response.audit ?? null);
      return response.audit ?? null;
    } catch (cause) {
      setAuditError(errorMessage(cause, "媒体库审计失败"));
      return null;
    } finally {
      setAuditPending(false);
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
        refreshHealth(), refreshJobs(), refreshControl(), refreshLibraryAudit(),
      ]);
      if (disposed) return;
      const active = queue.find(job => ACTIVE_PHASES.has(job.phase))
        ?? queue.find(isFailed)
        ?? queue[0];
      if (active) {
        try {
          const response = await scrapeFlowApi.job(active.id);
          if (!disposed) mergeJob(response.job);
        } catch {
          // The compact queue remains useful if loading one detail fails.
        }
      }
      if (!disposed) setInitializing(false);
    };
    void restore();
    const healthTimer = window.setInterval(() => {
      void refreshHealth();
      void refreshJobs();
      void refreshControl();
      void refreshLibraryAudit();
    }, 12000);
    return () => {
      disposed = true;
      window.clearInterval(healthTimer);
    };
  }, [mergeJob, refreshControl, refreshHealth, refreshJobs, refreshLibraryAudit]);

  const hasActiveJobs = jobs.some(job => ACTIVE_PHASES.has(job.phase));
  const selectedActiveId = selected && ACTIVE_PHASES.has(selected.phase) ? selected.id : null;
  useEffect(() => {
    if (!hasActiveJobs) return;
    let disposed = false;
    let timer = 0;
    const poll = async () => {
      if (!disposed) await refreshJobs();
      if (!disposed && selectedActiveId) {
        try {
          const response = await scrapeFlowApi.job(selectedActiveId);
          if (!disposed) mergeJob(response.job);
        } catch {
          // The next heartbeat repeats the read-only refresh.
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

  const createJob = async (source: string) => {
    const path = source.trim().replace(/\/+$/, "");
    if (!path) {
      setOperationError("请输入或选择来源目录");
      return false;
    }
    if (!health?.connected || !health.tmdb_configured) {
      setOperationError("AList 与 TMDB 就绪后才能开始自动整理");
      return false;
    }
    setPending(true);
    setOperationError("");
    setCreateConflict(null);
    try {
      const response = await scrapeFlowApi.create(path);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      if (cause instanceof ApiRequestError && cause.status === 409) {
        const candidate = cause.payload.job;
        if (candidate && typeof candidate === "object" && typeof (candidate as Record<string, unknown>).id === "string") {
          const existing = normalizeJob(candidate as Job);
          mergeJob(existing);
          setCreateConflict(existing);
          setOperationError(`这个目录已有自动任务 #${existing.id}，没有重复创建。`);
          return false;
        }
      }
      setOperationError(errorMessage(cause, "无法创建自动任务"));
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

  const retry = async (job: Job, correction: RetryCorrection = {}) => {
    setPending(true);
    setOperationError("");
    try {
      const response = await scrapeFlowApi.retry(job.id, correction);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "无法请求自动重试"));
      return false;
    } finally {
      setPending(false);
    }
  };

  const startJob = async (job: Job, targetShelf: TargetShelf) => {
    setPending(true);
    setOperationError("");
    try {
      const response = await scrapeFlowApi.start(job.id, targetShelf);
      mergeJob(response.job);
      return true;
    } catch (cause) {
      setOperationError(errorMessage(cause, "无法启动自动任务"));
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

  const cleanup = async (job: Job) => {
    setPending(true);
    setOperationError("");
    try {
      const response = await scrapeFlowApi.cleanup(job.id);
      if (response.cleanup.removed) {
        setJobs(previous => previous.filter(item => item.id !== job.id));
        setSelected(previous => previous?.id === job.id ? null : previous);
      }
      return response.cleanup.removed;
    } catch (cause) {
      setOperationError(errorMessage(cause, "无法安全清理任务记录"));
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
    createJob, browse, startJob, retry, cancel, cleanup,
    control, controlError, refreshControl, setGlobalPause,
    libraryAudit, auditPending, auditError, refreshLibraryAudit, runLibraryAudit,
    dismissError: () => setOperationError(""),
  };
}
