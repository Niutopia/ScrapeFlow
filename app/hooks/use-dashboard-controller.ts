"use client";

import { useEffect, useMemo, useState } from "react";
import type { Job } from "../core/contracts";
import {
  ACTIVE_PHASES,
  MEDIA_ROOT,
  TERMINAL_PHASES,
  isFailed,
} from "../core/job-state";
import { useScrapeFlow } from "./use-scrapeflow";

export type TaskFilter = "all" | "attention" | "running" | "finished";
export const DASHBOARD_FILTERS: Array<{ value: TaskFilter; label: string }> = [
  { value: "all", label: "全部" },
  { value: "running", label: "自动处理中" },
  { value: "attention", label: "异常" },
  { value: "finished", label: "已结束" },
];

export function useDashboardController() {
  const [filter, setFilter] = useState<TaskFilter>("all");
  const [source, setSource] = useState("");
  const [browserOpen, setBrowserOpen] = useState(false);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshFeedback, setRefreshFeedback] = useState<"" | "已同步" | "同步失败">("");
  const [bulkRetrying, setBulkRetrying] = useState(false);
  const flow = useScrapeFlow();

  useEffect(() => {
    if (!refreshFeedback) return;
    const timer = window.setTimeout(() => setRefreshFeedback(""), 1600);
    return () => window.clearTimeout(timer);
  }, [refreshFeedback]);

  const groups = useMemo(() => {
    const all = flow.jobs;
    const attention = flow.jobs.filter(isFailed);
    const running = flow.jobs.filter(job => ACTIVE_PHASES.has(job.phase));
    const finished = flow.jobs.filter(job => TERMINAL_PHASES.has(job.phase) && !isFailed(job));
    return { all, attention, running, finished };
  }, [flow.jobs]);

  const visibleJobs = groups[filter];
  const ready = !!flow.health?.connected && !!flow.health.tmdb_configured && !!flow.health.engine_configured;
  const current = flow.selected;

  const keepTaskAtViewportPosition = (jobId: string, previousTop: number) => {
    window.requestAnimationFrame(() => {
      const row = document.querySelector<HTMLElement>(`[data-job-id="${jobId}"]`);
      if (!row) return;
      const delta = row.getBoundingClientRect().top - previousTop;
      if (Math.abs(delta) > 1) window.scrollBy(0, delta);
    });
  };

  const toggleTaskPanel = async (job: Job) => {
    const expandable = ACTIVE_PHASES.has(job.phase) || isFailed(job) || job.phase === "completed" || job.phase === "cancelled";
    if (!expandable) return;
    const row = document.querySelector<HTMLElement>(`[data-job-id="${job.id}"]`);
    const previousTop = row?.getBoundingClientRect().top;
    if (expandedId === job.id) {
      setExpandedId(null);
      return;
    }
    const selectedJob = flow.selected;
    const needsFreshDetail = selectedJob?.id !== job.id
      || !selectedJob.plan
      || selectedJob.phase !== job.phase
      || selectedJob.updated_at !== job.updated_at;
    if (needsFreshDetail && !(await flow.loadJob(job.id))) return;
    setExpandedId(job.id);
    if (typeof previousTop === "number") keepTaskAtViewportPosition(job.id, previousTop);
  };

  const browse = async (path: string) => {
    const result = await flow.browse(path);
    setBrowserOpen(!!result);
  };

  const selectBrowserPath = (path: string) => {
    setSource(path);
    setBrowserOpen(false);
  };

  const createJob = async () => {
    if (await flow.createJob(source)) {
      setBrowserOpen(false);
      setSource("");
      setExpandedId(null);
      setFilter("running");
      return true;
    }
    return false;
  };

  const openIssueJob = async (job: Job) => {
    setFilter("attention");
    if (flow.selected?.id !== job.id || !flow.selected.plan) {
      if (!(await flow.loadJob(job.id))) return;
    }
    setExpandedId(job.id);
    window.requestAnimationFrame(() => document.querySelector(`[data-job-id="${job.id}"]`)?.scrollIntoView({ behavior: "smooth", block: "center" }));
  };

  const retryMany = async (retryableJobs: Job[]) => {
    if (bulkRetrying || flow.pending) return;
    setBulkRetrying(true);
    for (const job of retryableJobs) await flow.retry(job);
    await flow.refreshJobs();
    setFilter("running");
    setBulkRetrying(false);
  };

  const refreshDashboard = async () => {
    if (refreshing) return;
    setRefreshing(true);
    setRefreshFeedback("");
    try {
      const enteredPath = source.trim().replace(/\/+$/, "");
      const refreshPath = enteredPath && enteredPath !== MEDIA_ROOT ? enteredPath : current?.source || MEDIA_ROOT;
      const refreshed = await flow.browse(refreshPath, true);
      if (!refreshed) {
        setRefreshFeedback("同步失败");
        return;
      }
      const selectedId = flow.selected?.id;
      const [, jobs] = await Promise.all([
        flow.refreshHealth(), flow.refreshJobs(),
      ]);
      const next = jobs.find(job => job.id === selectedId) ?? jobs[0];
      if (next) await flow.loadJob(next.id);
      setRefreshFeedback("已同步");
    } catch {
      setRefreshFeedback("同步失败");
    } finally {
      setRefreshing(false);
    }
  };

  return {
    filter, setFilter, source, setSource, browserOpen, setBrowserOpen,
    expandedId, setExpandedId, refreshing, refreshFeedback,
    bulkRetrying, flow, groups, visibleJobs, ready,
    current, toggleTaskPanel, browse, selectBrowserPath, createJob,
    openIssueJob, retryMany, refreshDashboard,
  };
}
