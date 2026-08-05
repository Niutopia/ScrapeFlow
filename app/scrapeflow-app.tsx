"use client";

import { useEffect, useMemo, useState } from "react";
import { IssueWorkbench } from "./components/issue-workbench";
import { ReviewPanel } from "./components/review-dialog";
import type {
  BrowseResult, GlobalControl, Job, JobRetryOptions, TargetCategory,
} from "./core/contracts";
import {
  ACTIVE_PHASES,
  APPROVAL_PHASES,
  MEDIA_ROOT,
  PHASE,
  TERMINAL_PHASES,
  UNSCRAPED_MEDIA_ROOT,
  canDelete,
  canRetry,
  formatDate,
  isRecoverable,
} from "./core/job-state";
import { useScrapeFlow } from "./hooks/use-scrapeflow";

type TaskFilter = "attention" | "running" | "finished";
type WorkspaceTab = "tasks" | "create";
type SupplementContext = {
  title: string;
  tmdbId: number;
  targetRoot: string;
  category: TargetCategory;
  mediaType: "tv" | "movie";
};
const TARGET_CATEGORY_STORAGE_KEY = "scrapeflow.target-category";
const TARGET_CATEGORIES = ["番剧", "美剧", "电影"] as const satisfies readonly TargetCategory[];
const FILTERS: Array<{ value: TaskFilter; label: string }> = [
  { value: "attention", label: "需处理" },
  { value: "running", label: "运行中" },
  { value: "finished", label: "已结束" },
];

export function ScrapeFlowApp() {
  const [filter, setFilter] = useState<TaskFilter>("attention");
  const [source, setSource] = useState(UNSCRAPED_MEDIA_ROOT);
  const [category, setCategory] = useState<TargetCategory | "">("");
  const [browserOpen, setBrowserOpen] = useState(false);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [managing, setManaging] = useState(false);
  const [confirmClear, setConfirmClear] = useState(false);
  const [confirmRemoveId, setConfirmRemoveId] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshFeedback, setRefreshFeedback] = useState<"" | "已同步" | "同步失败">("");
  const [supplementContext, setSupplementContext] = useState<SupplementContext | null>(null);
  const [workspaceTab, setWorkspaceTab] = useState<WorkspaceTab>("tasks");
  const [bulkRetrying, setBulkRetrying] = useState(false);
  const flow = useScrapeFlow();

  useEffect(() => {
    try {
      const stored = window.localStorage.getItem(TARGET_CATEGORY_STORAGE_KEY);
      if (TARGET_CATEGORIES.some(value => value === stored)) setCategory(stored as TargetCategory);
    } catch {
      // Private browsing and disabled storage must not block task creation.
    }
  }, []);

  useEffect(() => {
    if (!refreshFeedback) return;
    const timer = window.setTimeout(() => setRefreshFeedback(""), 1600);
    return () => window.clearTimeout(timer);
  }, [refreshFeedback]);

  const groups = useMemo(() => {
    const attention = flow.jobs.filter(job => APPROVAL_PHASES.has(job.phase) || job.phase === "failed" || job.phase === "recovery_required" || job.phase === "planning_recovery");
    const running = flow.jobs.filter(job => ACTIVE_PHASES.has(job.phase) && job.phase !== "planning_recovery");
    const finished = flow.jobs.filter(job => TERMINAL_PHASES.has(job.phase) && job.phase !== "failed");
    return { attention, running, finished };
  }, [flow.jobs]);

  const visibleJobs = groups[filter];
  const ready = !!flow.health?.connected && !!flow.health.tmdb_configured;
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
    const expandable = ACTIVE_PHASES.has(job.phase) || APPROVAL_PHASES.has(job.phase) || job.phase === "failed" || job.phase === "completed" || job.phase === "recovered";
    if (!expandable) return;
    const row = document.querySelector<HTMLElement>(`[data-job-id="${job.id}"]`);
    const previousTop = row?.getBoundingClientRect().top;
    if (expandedId === job.id) {
      setExpandedId(null);
      return;
    }
    // Queue polling intentionally returns compact jobs.  A task may advance
    // from media approval to recovery approval while the previously selected
    // detail still contains the old digest/plan.  Always reload when the row
    // version differs, otherwise the expanded panel can be blank or show the
    // plan from the task's previous phase.
    const selectedJob = flow.selected;
    const needsFreshDetail = selectedJob?.id !== job.id
      || !selectedJob.plan
      || selectedJob.phase !== job.phase
      || selectedJob.digest !== job.digest
      || selectedJob.updated_at !== job.updated_at;
    if (needsFreshDetail && !(await flow.loadJob(job.id))) return;
    setExpandedId(job.id);
    if (typeof previousTop === "number") keepTaskAtViewportPosition(job.id, previousTop);
  };

  const browse = async (path: string) => {
    const result = await flow.browse(path);
    if (result) {
      setBrowserOpen(true);
    } else {
      setBrowserOpen(false);
    }
  };

  const selectBrowserPath = (path: string) => {
    setSource(path);
    setBrowserOpen(false);
  };

  const createJob = async () => {
    if (await flow.createJob(
      source,
      category,
      supplementContext?.tmdbId,
      supplementContext?.mediaType,
    )) {
      setBrowserOpen(false);
      setSource(UNSCRAPED_MEDIA_ROOT);
      setExpandedId(null);
      setSupplementContext(null);
    }
  };

  const selectCategory = (value: TargetCategory) => {
    setCategory(value);
    try {
      window.localStorage.setItem(TARGET_CATEGORY_STORAGE_KEY, value);
    } catch {
      // Keep the in-memory selection usable when persistence is unavailable.
    }
  };

  const supplementResources = async (job: Job) => {
    const plan = job.plan;
    if (!plan?.tmdb_id) return;
    const targetRoot = plan.target_root || job.parent;
    const category: TargetCategory = targetRoot.includes("/电影/") || targetRoot.endsWith("/电影")
      ? "电影"
      : targetRoot.includes("/美剧/") || targetRoot.endsWith("/美剧") ? "美剧" : "番剧";
    const matchedType = plan.review?.match?.media_type;
    const mediaType = matchedType === "movie" || matchedType === "tv"
      ? matchedType
      : job.settings?.media_type === "movie" ? "movie" : "tv";
    setSupplementContext({
      title: plan.title || job.source.split("/").at(-1) || "当前作品",
      tmdbId: plan.tmdb_id,
      targetRoot,
      category,
      mediaType,
    });
    selectCategory(category);
    setExpandedId(null);
    setWorkspaceTab("create");
    await browse(UNSCRAPED_MEDIA_ROOT);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const removeJob = async (job: Job) => {
    if (!canDelete(job) || flow.pending) return;
    if (confirmRemoveId !== job.id) {
      setConfirmRemoveId(job.id);
      return;
    }
    if (await flow.remove(job)) {
      if (expandedId === job.id) setExpandedId(null);
      setConfirmRemoveId(null);
    }
  };

  const clearData = async () => {
    if (flow.pending) return;
    if (!confirmClear) {
      setConfirmClear(true);
      return;
    }
    if (await flow.clearData()) {
      setExpandedId(null);
      setManaging(false);
      setConfirmClear(false);
    }
  };

  const approveJob = async () => {
    const approved = await flow.approve();
    if (approved) {
      setExpandedId(null);
      setFilter("running");
    }
    return approved;
  };

  const openIssueJob = async (job: Job) => {
    setWorkspaceTab("tasks");
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
      const [, jobs] = await Promise.all([flow.refreshHealth(), flow.refreshJobs()]);
      const next = jobs.find(job => job.id === selectedId) ?? jobs[0];
      if (next) await flow.loadJob(next.id);
      setRefreshFeedback("已同步");
    } catch {
      setRefreshFeedback("同步失败");
    } finally {
      setRefreshing(false);
    }
  };

  return <main className="taskdesk-shell dashboard-shell">
    <header className="taskdesk-topbar taskdesk-topbar-single">
      <div className="brand" aria-label="ScrapeFlow">
        <span className="brand-mark" aria-hidden="true" />
        <b>SCRAPEFLOW</b>
      </div>
      <div className="taskdesk-top-actions">
        <span className={`taskdesk-health ${ready ? "ready" : "offline"}`} title={flow.healthError || flow.health?.message || "本地连接状态"}><i aria-hidden="true" />{flow.health === null ? "连接中" : ready ? "任务在线" : "服务需要配置"}</span>
        <button className={`taskdesk-refresh-square ${refreshing ? "refreshing" : ""} ${refreshFeedback === "同步失败" ? "failed" : refreshFeedback ? "success" : ""}`} onClick={() => void refreshDashboard()} disabled={refreshing} aria-live="polite" title="强制刷新当前媒体目录的 AList 缓存"><i aria-hidden="true">↻</i><span>{refreshing ? "同步中" : refreshFeedback || "刷新 AList"}</span></button>
      </div>
    </header>

    {flow.initializing ? <section className="boot-screen" aria-label="正在恢复本地任务"><div className="boot-glyph"><i /><i /><i /></div><b>正在接入本地任务队列</b><span>恢复状态与审核计划</span></section> : <section className="dashboard-page">
      <nav className="workspace-tabs" role="tablist" aria-label="工作区">
        <button type="button" role="tab" id="workspace-tab-create" aria-controls="workspace-panel-create" aria-selected={workspaceTab === "create"} className={workspaceTab === "create" ? "active" : ""} onClick={() => { setWorkspaceTab("create"); setExpandedId(null); }}><span>NEW JOB</span><b>新建整理</b></button>
        <button type="button" role="tab" id="workspace-tab-tasks" aria-controls="workspace-panel-tasks" aria-selected={workspaceTab === "tasks"} className={workspaceTab === "tasks" ? "active" : ""} onClick={() => setWorkspaceTab("tasks")}><span>TASK CENTER</span><b>任务中心</b></button>
      </nav>

      {flow.error && <div className="taskdesk-error dashboard-error" role="alert"><b>!</b><span>{flow.error}</span><button onClick={() => flow.errorSource === "queue" ? void flow.refreshJobs() : flow.dismissError()}>{flow.errorSource === "queue" ? "重试" : "关闭"}</button></div>}

      {workspaceTab === "tasks" ? <section className="workspace-panel task-center-panel" id="workspace-panel-tasks" role="tabpanel" aria-labelledby="workspace-tab-tasks">
        <SystemControlBar
          control={flow.control}
          controlError={flow.controlError}
          pending={flow.pending}
          onPause={() => void flow.setGlobalPause(true)}
          onResume={() => void flow.setGlobalPause(false)}
        />
        <IssueWorkbench
        jobs={flow.jobs}
        pending={flow.pending || bulkRetrying}
        onOpen={job => void openIssueJob(job)}
        onRecover={job => void flow.recover(job)}
        onRetry={job => flow.retry(job)}
        onRetryMany={retryMany}
        onNewTask={() => { setWorkspaceTab("create"); setExpandedId(null); }}
      />

      <div className="dashboard-layout">
        <div className="dashboard-primary">
          <section className="dashboard-queue">
            <header><div><span>JOB QUEUE</span><h2>任务队列</h2></div><div className="queue-controls"><div className="taskdesk-filters" role="tablist" aria-label="筛选任务">{FILTERS.map(item => <button key={item.value} role="tab" aria-selected={filter === item.value} className={filter === item.value ? "active" : ""} onClick={() => { setFilter(item.value); setConfirmRemoveId(null); }}>{item.label}<span>{groups[item.value].length}</span></button>)}</div>{managing && <button className={`queue-clear ${confirmClear ? "confirming" : ""}`} type="button" onClick={() => void clearData()} disabled={flow.pending} title="同时清空本地任务记录和已处理历史；不会删除 AList 媒体">{confirmClear ? "确认清空" : "清空数据"}</button>}<button className={`queue-manage ${managing ? "active" : ""}`} type="button" aria-pressed={managing} onClick={() => { setManaging(value => !value); setConfirmClear(false); setConfirmRemoveId(null); setExpandedId(null); }}>{managing ? "完成" : "管理"}</button></div></header>
            <div className="dashboard-table-head"><span>任务</span><span>状态</span><span>进度</span><span>更新时间</span><span>操作</span></div>
            <div className="dashboard-task-list">
              {visibleJobs.map(job => {
                const expanded = expandedId === job.id;
                const detailJob = expanded && current?.id === job.id ? current : job;
                return <div className={`taskdesk-job ${expanded ? "expanded" : ""}`} key={job.id} data-job-id={job.id}>
                  <TaskRow
                    job={job}
                    selected={expanded}
                    expanded={expanded}
                    managing={managing}
                    removeConfirming={confirmRemoveId === job.id}
                    pending={flow.pending}
                    onToggle={() => void toggleTaskPanel(job)}
                    onRecover={() => void flow.recover(job)}
                    onRetry={() => void flow.retry(job)}
                    onRemove={() => void removeJob(job)}
                  />
                  {expanded && <TaskExpansion
                    job={detailJob}
                    pending={flow.pending}
                    onClose={() => setExpandedId(null)}
                    onApprove={approveJob}
                    onRetry={options => flow.retry(detailJob, options)}
                    onCancel={() => flow.cancel(detailJob)}
                    onSupplement={() => void supplementResources(detailJob)}
                    onKeepExisting={() => flow.keepExisting(detailJob)}
                  />}
                </div>;
              })}
              {!visibleJobs.length && <div className="taskdesk-empty"><i>✓</i><b>这个分类暂时为空</b><span>切换到其他分类查看任务。</span></div>}
            </div>
          </section>
        </div>

      </div>
      </section> : <section className="workspace-panel create-workspace-panel" id="workspace-panel-create" role="tabpanel" aria-labelledby="workspace-tab-create">
        {supplementContext ? <aside className="supplement-context" aria-label="补充资源上下文"><div><b>正在为《{supplementContext.title}》补充资源</b><span>选择新的待刮削目录后会创建正常新任务；已锁定 TMDB #{supplementContext.tmdbId}，并与目标 {supplementContext.targetRoot} 的现有内容共同去重。</span></div><button type="button" onClick={() => setSupplementContext(null)}>取消补充</button></aside> : null}
        <section className="new-task-workspace open" id="new-task">
          <header><div><span>NEW JOB</span><h2>新建整理</h2><p>提交待刮削目录后，自动整理、到盘复核并补齐当前作品的缺集与中文字幕。</p></div></header>
          <NewTaskPanel source={source} category={category} jobs={flow.jobs} createConflict={flow.createConflict} ready={ready} pending={flow.pending} browser={flow.browser} browserOpen={browserOpen} browserPending={flow.browserPending} onSourceChange={setSource} onCategoryChange={selectCategory} onBrowse={browse} onCloseBrowser={() => setBrowserOpen(false)} onSelectBrowserPath={selectBrowserPath} onCreate={() => void createJob()} />
        </section>
      </section>}
    </section>}
  </main>;
}

function SystemControlBar({ control, controlError, pending, onPause, onResume }: {
  control: GlobalControl | null;
  controlError: string;
  pending: boolean;
  onPause: () => void;
  onResume: () => void;
}) {
  const [confirmPause, setConfirmPause] = useState(false);
  const paused = !!control?.paused;
  const gateOpen = !paused;
  const gateLabel = paused
    ? `已暂停${control.reason ? ` · ${control.reason}` : ""}`
    : "运行中";
  return <section className="system-control-bar" aria-label="自动流程控制">
    <div className="system-control-group">
      <header><div><span>FLOW CONTROL</span><b>自动流程</b></div><i className={gateOpen ? "open" : "paused"} aria-hidden="true" /></header>
      <p>{gateLabel}{paused ? "；新建与现有作品任务都会排队等待，不会开始新的文件操作。" : "提交的作品会依次完成刮削、到盘复核和缺项补源。"}</p>
      <div className="system-control-actions">
        {paused ? <button type="button" className={confirmPause ? "confirming" : ""} disabled={pending} onClick={() => {
          if (!confirmPause) { setConfirmPause(true); return; }
          setConfirmPause(false);
          onResume();
        }}>{pending ? "正在处理…" : confirmPause ? "确认恢复" : "恢复自动流程"}</button>
          : <button type="button" className={confirmPause ? "confirming" : ""} disabled={pending} onClick={() => {
            if (!confirmPause) { setConfirmPause(true); return; }
            setConfirmPause(false);
            onPause();
          }}>{pending ? "正在处理…" : confirmPause ? "确认暂停" : "暂停自动流程"}</button>}
        <span>{paused ? "恢复后才会继续派发排队任务。" : "暂停不会中断当前操作，只停止后续派发。"}</span>
      </div>
      {controlError ? <p className="error">流程控制读取失败：{controlError}</p> : null}
    </div>
  </section>;
}

function TaskRow({ job, selected, expanded, managing, removeConfirming, pending, onToggle, onRecover, onRetry, onRemove }: { job: Job; selected: boolean; expanded: boolean; managing: boolean; removeConfirming: boolean; pending: boolean; onToggle: () => void; onRecover: () => void; onRetry: () => void; onRemove: () => void }) {
  const isQueued = typeof job.queue_position === "number" && job.queue_position > 0;
  const queueName = job.queue_kind === "execution" ? "文件操作" : "分析";
  const pendingDelete = job.progress?.stage === "source_excluded"
    || /[（(]\s*待删\d*\s*[）)]\s*(?:\/|$)/.test(job.source);
  const meta = pendingDelete
    ? { ...PHASE.cancelled, label: "待删除", detail: "源目录已标记待删除并自动排除" }
    : isQueued
    ? { ...PHASE.queued, detail: `${queueName}队列第 ${job.queue_position} 位` }
    : PHASE[job.phase];
  const liveProgress = !isQueued && ACTIVE_PHASES.has(job.phase) ? job.progress : null;
  const progress = liveProgress?.percent ?? meta.progress;
  const detail = liveProgress?.message || meta.detail;
  const progressCount = liveProgress && liveProgress.total > 1
    ? `${liveProgress.completed}/${liveProgress.total}`
    : null;
  const title = job.source.split("/").at(-1) || job.source;
  const needsApproval = APPROVAL_PHASES.has(job.phase);
  const resultAvailable = job.phase === "completed" || job.phase === "recovered";
  const active = ACTIVE_PHASES.has(job.phase);
  const recoverable = isRecoverable(job);
  const retryable = canRetry(job);
  const removable = canDelete(job);
  const failed = job.phase === "failed";
  const action = managing ? removable ? { label: removeConfirming ? "确认删除" : "删除", run: onRemove, danger: true } : null : expanded ? { label: "收起", run: onToggle } : needsApproval ? { label: "审核", run: onToggle } : resultAvailable ? { label: job.phase === "recovered" ? "查看恢复" : "查看结果", run: onToggle } : failed ? { label: "查看原因", run: onToggle } : recoverable ? { label: "检查恢复", run: onRecover } : retryable ? { label: "重新尝试", run: onRetry } : active ? { label: "查看进度", run: onToggle } : null;
  const titleContent = <><i className={`tone-${meta.tone}`} /><span><b>{title}</b><small>{job.source}</small></span></>;
  return <article className={`dashboard-task-row ${selected ? "selected" : ""}`}>
    {!managing && (active || needsApproval || resultAvailable || failed) ? <button className="taskdesk-task" onClick={onToggle} aria-expanded={expanded}>{titleContent}</button> : <div className="taskdesk-task">{titleContent}</div>}
    <div className={`taskdesk-state tone-${meta.tone}`}><i /><span><b>{meta.label}</b><small title={failed ? job.error || undefined : undefined}>{failed && job.error ? job.error : detail}</small></span></div>
    <div
      className="taskdesk-progress"
      role="progressbar"
      aria-label={`${title}进度`}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(progress)}
      aria-valuetext={detail}
    >
      <span aria-hidden="true"><i style={{ width: `${progress}%` }} /></span>
      <b>{Math.round(progress)}%{progressCount && <small>{progressCount}</small>}</b>
    </div>
    <time>{formatDate(job.updated_at)}</time>
      <div className="taskdesk-actions">{action && <button className={action.danger ? "row-danger" : needsApproval && !expanded ? "row-primary" : ""} onClick={action.run} disabled={pending} aria-expanded={!managing && (active || needsApproval || resultAvailable || failed) ? expanded : undefined}>{action.label}</button>}</div>
  </article>;
}

function TaskExpansion({ job, pending, onClose, onApprove, onRetry, onCancel, onSupplement, onKeepExisting }: {
  job: Job;
  pending: boolean;
  onClose: () => void;
  onApprove: () => Promise<boolean>;
  onRetry: (options?: JobRetryOptions) => Promise<boolean>;
  onCancel: () => Promise<boolean>;
  onSupplement: () => void;
  onKeepExisting?: () => Promise<boolean>;
}) {
  if (ACTIVE_PHASES.has(job.phase)) {
    return <LiveTaskPanel job={job} pending={pending} onClose={onClose} onCancel={onCancel} />;
  }
  if (APPROVAL_PHASES.has(job.phase)) {
    return <div className="taskdesk-inline-review">
      <ReviewPanel key={`${job.id}-${job.digest}`} job={job} open pending={pending} onClose={onClose} onApprove={onApprove} onSupplement={onSupplement} />
      {job.phase === "awaiting_media_approval" ? <PlanCorrectionPanel key={`${job.id}-${job.digest}-correction`} job={job} pending={pending} onRetry={onRetry} /> : null}
    </div>;
  }
  if (job.phase === "failed" && job.plan?.replenishment) {
    return <ReplenishmentFailurePanel job={job} pending={pending} onClose={onClose} onRetry={onRetry} />;
  }
  if (job.phase === "failed") {
    return <FailureRepairPanel job={job} pending={pending} onClose={onClose} onRetry={onRetry} onKeepExisting={onKeepExisting} />;
  }
  if (job.phase !== "completed" && job.phase !== "recovered") return null;
  const title = job.source.split("/").at(-1) || job.source;
  const count = job.plan?.file_count ?? 0;
  const cleanupCount = job.plan?.cleanup_file_count ?? 0;
  const resourceGaps = job.plan?.resource_gaps ?? [];
  const replenishment = job.plan?.replenishment;
  const selectedReleases = replenishment?.selections
    ?? (replenishment?.selection ? [replenishment.selection] : []);
  const awaitingSources = replenishment?.status === "awaiting_sources";
  const replenishmentText = replenishment?.status === "acquired" && replenishment.followup_job_id
    ? replenishment.uncovered_gap_count
      ? `已覆盖 ${replenishment.covered_gap_count ?? 0} 项缺口并创建后续任务，另有 ${replenishment.uncovered_gap_count} 项进入下一轮搜索。`
      : `检测到 ${replenishment.gap_count ?? resourceGaps.length} 项正片缺口，已组合 ${selectedReleases.length} 组候选并创建后续整理任务。`
    : awaitingSources
      ? `当前作品仍有 ${replenishment.gap_count ?? resourceGaps.length} 项缺口，本轮没有找到合格来源；缺口记录已保留，可从当前作品继续查补。`
    : replenishment?.status === "no_match"
      ? `检测到 ${replenishment.gap_count ?? resourceGaps.length} 项正片缺口；${replenishment.candidate_count ?? 0} 个原始候选中有 ${replenishment.eligible_candidate_count ?? 0} 个通过身份、可用性与覆盖门槛。`
      : replenishment?.status === "adapter_not_configured"
        ? `检测到 ${replenishment.gap_count ?? resourceGaps.length} 项正片缺口，等待查补适配器接管。`
        : replenishment?.status === "no_regular_gaps"
          ? "文件已经完成整理，完成后缺项检测也已通过。"
          : resourceGaps.length
            ? `文件整理已通过校验，完成后检测到 ${resourceGaps.length} 项资源缺口。`
            : "文件已经完成整理并通过校验。";
  if (job.phase === "recovered") return <section className="taskdesk-success-panel taskdesk-recovered-panel" aria-label={`${title}恢复结果`}>
    <i aria-hidden="true">↶</i>
    <div><span>回滚已完成</span><h3>{title}</h3><p>文件已恢复到整理前状态；该媒体仍属于未处理，可修正参数后重新创建计划。</p></div>
    <dl><div><dt>恢复文件</dt><dd>{count || "—"}</dd></div><div><dt>原始目录</dt><dd>{job.source}</dd></div><div><dt>恢复时间</dt><dd>{formatDate(job.updated_at)}</dd></div></dl>
    <button onClick={onClose}>收起</button>
  </section>;
  return <section className={`taskdesk-success-panel${awaitingSources ? " taskdesk-awaiting-sources-panel" : ""}`} aria-label={`${title}${awaitingSources ? "外部来源等待" : "完成结果"}`}>
    <i aria-hidden="true">{awaitingSources ? "◷" : "✓"}</i>
    <div><span>{awaitingSources ? "等待外部来源" : "任务已完成"}</span><h3>{title}</h3><p>{replenishmentText}</p></div>
    <dl><div><dt>文件</dt><dd>{count}</dd></div>{cleanupCount ? <div><dt>自动清理</dt><dd>{cleanupCount}</dd></div> : null}<div><dt>目标目录</dt><dd>{job.plan?.target_root || job.parent}</dd></div>{awaitingSources ? <div><dt>补齐状态</dt><dd>等待合格来源</dd></div> : <div><dt>完成时间</dt><dd>{formatDate(job.updated_at)}</dd></div>}</dl>
    <button onClick={onClose}>收起</button>
    {resourceGaps.length ? <ul className="taskdesk-success-gaps">
      {selectedReleases.map((selection, index) => selection.release_name ? <li key={`${selection.release_name}-${index}`}><b>已选候选 {index + 1}</b><span>{selection.release_name} · {selection.resolution || "清晰度待核验"}{selection.selected_gap_ids?.length ? ` · 覆盖 ${selection.selected_gap_ids.join("、")}` : ""}</span></li> : null)}
      {resourceGaps.map(gap => <li key={`${gap.kind}-${gap.label}`}><b>{gap.label}</b><span>{gap.reason}</span></li>)}
    </ul> : null}
  </section>;
}

function LiveTaskPanel({ job, pending, onClose, onCancel }: { job: Job; pending: boolean; onClose: () => void; onCancel: () => Promise<boolean> }) {
  const title = job.plan?.title || job.source.split("/").at(-1) || job.source;
  const progress = job.progress;
  const meta = PHASE[job.phase];
  const rawPercent = progress?.percent ?? meta.progress;
  const percent = Math.max(0, Math.min(100, Number.isFinite(rawPercent) ? rawPercent : meta.progress));
  const replenishment = job.plan?.replenishment;
  const projects = replenishment?.projects ?? [];
  const candidatePool = [
    ...(replenishment?.selections ?? (replenishment?.selection ? [replenishment.selection] : [])),
    ...projects.flatMap(project => project.selections ?? (project.selection ? [project.selection] : [])),
  ];
  const candidates = Array.from(new Map(candidatePool.map(candidate => [
    `${candidate.provider || "unknown"}:${candidate.release_name || "unknown"}:${(candidate.selected_gap_ids ?? []).join(",")}`,
    candidate,
  ])).values());
  const replenishmentGaps = replenishment?.gaps ?? projects.flatMap(project => project.gaps ?? []);
  const resourceGaps = job.plan?.resource_gaps ?? [];
  const gapRows = replenishmentGaps.length
    ? replenishmentGaps.map(gap => ({ key: gap.id || gap.label || "gap", label: gap.label || gap.id || "未命名缺口", reason: "" }))
    : resourceGaps.map(gap => ({ key: `${gap.kind}-${gap.label}`, label: gap.label, reason: gap.reason }));
  const previousFailure = projects.find(project => /failed|invalid/.test(project.status ?? ""));
  const stageLabel: Record<string, string> = {
    replenishment_search: "搜索候选",
    replenishment_acquire: "获取候选",
    replenishment_complete: "查补收尾",
    execution_locked: "锁定路径",
    execution_apply: "写入文件",
    execution_verify: "校验结果",
  };
  const stage = progress?.stage ? stageLabel[progress.stage] || progress.stage : meta.label;
  const message = progress?.message || meta.detail;

  return <section className="live-task-panel" aria-label={`${title}实时进度`}>
    <header>
      <div><span>LIVE TASK</span><h3>{title}</h3><p aria-live="polite">{message}</p></div>
      <button type="button" onClick={onClose}>收起</button>
    </header>
    <div className="live-task-progress" role="progressbar" aria-label={`${title}实时进度`} aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(percent)} aria-valuetext={message}>
      <span><i style={{ width: `${percent}%` }} /></span><b>{Math.round(percent)}%</b>
    </div>
    <dl>
      <div><dt>当前阶段</dt><dd>{stage}</dd></div>
      <div><dt>任务状态</dt><dd>{meta.label}</dd></div>
      {replenishment?.round ? <div><dt>查补轮次</dt><dd>第 {replenishment.round} 轮</dd></div> : null}
      <div><dt>最后更新</dt><dd>{formatDate(job.updated_at)}</dd></div>
    </dl>
    {candidates.length ? <section className="live-task-candidates" aria-label="最近持久化的查补候选">
      <header><b>候选与覆盖</b><span>最近持久化 {candidates.length} 组</span></header>
      <ul>{candidates.map((candidate, index) => <li key={`${candidate.release_name || "candidate"}-${index}`}>
        <span>{candidate.provider === "quark_share" ? "夸克快转" : candidate.provider === "quark_magnet" ? "夸克离线" : candidate.provider === "cloud_share" ? "云分享" : candidate.provider === "magnet" ? "本地种子" : "来源待核验"}</span>
        <strong>{candidate.release_name || "候选名称未记录"}</strong>
        <small>{candidate.resolution || "清晰度待核验"}{candidate.selected_gap_ids?.length ? ` · 覆盖 ${candidate.selected_gap_ids.length} 项` : ""}</small>
      </li>)}</ul>
    </section> : null}
    {gapRows.length ? <details className="live-task-resources">
      <summary><span>待补资源清单</span><b>{gapRows.length} 项</b></summary>
      <ol>{gapRows.map((gap, index) => <li key={`${gap.key}-${index}`}><b>{gap.label}</b>{gap.reason ? <span>{gap.reason}</span> : null}</li>)}</ol>
    </details> : null}
    {previousFailure?.message ? <aside className="live-task-previous-error" role="note"><b>上轮候选记录</b><p>{previousFailure.message}</p><small>当前任务仍在运行；此记录用于说明换源原因，不是当前终态。</small></aside> : null}
    <footer><span title={job.source}>{job.source}</span><button className="danger" type="button" onClick={() => void onCancel()} disabled={pending || job.phase === "cancelling"}>{job.phase === "cancelling" ? "正在安全停止" : "安全停止"}</button></footer>
  </section>;
}

function ReplenishmentFailurePanel({ job, pending, onClose, onRetry }: { job: Job; pending: boolean; onClose: () => void; onRetry: () => Promise<boolean> }) {
  const title = job.plan?.title || job.source.split("/").at(-1) || job.source;
  const replenishment = job.plan?.replenishment;
  const detail = replenishment?.failure_detail;
  const projects = replenishment?.projects ?? [];
  const selections = replenishment?.selections
    ?? projects.flatMap(project => project.selections ?? (project.selection ? [project.selection] : []));
  const gaps = replenishment?.gaps ?? projects.flatMap(project => project.gaps ?? []);
  const statusFallback: Record<string, string> = {
    acquire_failed: "已选候选在获取阶段失败。",
    search_failed: "补源候选搜索执行失败。",
    invalid_candidates: "候选返回内容未通过字段校验。",
    no_match: "现有候选均未通过作品身份与缺失季集覆盖门槛。",
    invalid_acquisition: "获取结果未通过目录或文件到盘校验。",
  };
  const projectMessage = projects.find(project => project.message)?.message;
  const technicalReason = detail?.technical_reason || projectMessage || job.error || "未记录";
  // Older task artifacts may retain the previous candidate's friendly
  // summary while the technical reason has already advanced to upload.  In
  // that case, prefer the concrete storage response so the operator does not
  // see a zero-speed diagnosis beside an upload-part rejection.
  const emptyUploadPart = /EntityTooSmall|ProposedSize\D*0|minimum allowed size|MinSizeAllowed/i.test(technicalReason);
  const summary = emptyUploadPart
    ? "候选已进入 AList 上传，但存储端拒绝了 0 字节分片。"
    : detail?.summary
      || projectMessage
      || statusFallback[replenishment?.status ?? ""]
      || job.error
      || "本轮查补没有形成可入库文件。";
  const stage = emptyUploadPart ? "AList 分片上传" : detail?.stage || "补源获取";
  const evidence = emptyUploadPart
    ? "存储端返回 EntityTooSmall，拒绝 ProposedSize=0 的上传分片。"
    : detail?.evidence || projectMessage || job.error || replenishment?.status || "未记录";
  const uploadStatus = emptyUploadPart ? "AList 上传未完成，未形成可校验的到盘目录" : detail?.upload_status || "未生成 AList 待刮削目录";
  const nextAction = emptyUploadPart
    ? "重新发起查补以重做获取与上传；若再次出现空分片，核对本地候选文件与 AList 分片上传状态。"
    : detail?.next_action || "隔离失败候选，按相同覆盖与清晰度规则选择下一条候选。";
  return <section className="replenishment-failure-panel" aria-label={`${title}实际失败原因`}>
    <header>
      <div><span>ACTUAL FAILURE</span><h3>{title}</h3><p>{summary}</p></div>
      <div className="replenishment-failure-actions"><button className="primary" disabled={pending} onClick={() => void onRetry()}>{pending ? "正在切换…" : "换候选重新查补"}</button><button onClick={onClose}>收起</button></div>
    </header>
    <dl>
      <div><dt>失败阶段</dt><dd>{stage}</dd></div>
      <div><dt>直接证据</dt><dd>{evidence}</dd></div>
      <div><dt>技术原文</dt><dd>{technicalReason}</dd></div>
      <div><dt>入库状态</dt><dd>{uploadStatus}</dd></div>
      <div className="next-action"><dt>下一步</dt><dd>{nextAction}</dd></div>
    </dl>
    {selections.length ? <section className="replenishment-failure-candidates" aria-label="本轮已选候选">
      <b>本轮实际尝试的候选</b>
      <ul>{selections.map((selection, index) => <li key={`${selection.release_name || "candidate"}-${index}`}>
        <span>{selection.provider === "quark_share" ? "夸克快转" : selection.provider === "quark_magnet" ? "夸克离线" : selection.provider === "cloud_share" ? "云分享" : selection.provider === "magnet" ? "本地种子" : "来源待核验"}</span>
        <strong>{selection.release_name || "候选名称未记录"}</strong>
        <small>{selection.resolution || "清晰度待核验"}{selection.selected_gap_ids?.length ? ` · 覆盖 ${selection.selected_gap_ids.length} 项` : ""}{selection.updated_at ? ` · 更新 ${selection.updated_at}` : ""}</small>
      </li>)}</ul>
    </section> : null}
    <footer>
      <p><b>缺失项 {job.plan?.gap_count ?? replenishment?.gap_count ?? gaps.length}</b>{gaps.length ? `：${gaps.map(gap => gap.label || gap.id).filter(Boolean).join("、")}` : " · 具体缺口沿用本轮查补请求"}</p>
      <small>来源任务 {job.plan?.origin_job_id || "—"} · 结束 {formatDate(job.updated_at)}</small>
    </footer>
  </section>;
}

function PlanCorrectionPanel({ job, pending, onRetry }: { job: Job; pending: boolean; onRetry: (options?: JobRetryOptions) => Promise<boolean> }) {
  const settings = job.settings;
  const matchedType = job.plan?.review?.match?.media_type;
  const [open, setOpen] = useState(false);
  const [tmdbId, setTmdbId] = useState(settings?.tmdb_id?.toString() ?? job.plan?.tmdb_id?.toString() ?? "");
  const [mediaType, setMediaType] = useState<"" | "tv" | "movie">(
    settings?.media_type === "tv" || settings?.media_type === "movie"
      ? settings.media_type : matchedType === "tv" || matchedType === "movie" ? matchedType : "",
  );
  const [query, setQuery] = useState(settings?.query ?? "");
  const [season, setSeason] = useState(settings?.season?.toString() ?? "");
  const submit = () => {
    const parsedTmdb = tmdbId.trim() ? Number(tmdbId) : null;
    const parsedSeason = season.trim() ? Number(season) : null;
    if ((parsedTmdb !== null && (!Number.isInteger(parsedTmdb) || parsedTmdb <= 0 || !mediaType)) || (parsedSeason !== null && (!Number.isInteger(parsedSeason) || parsedSeason < 0 || parsedSeason > 100))) return;
    void onRetry({ tmdb_id: parsedTmdb, ...(mediaType ? { media_type: mediaType } : {}), query: query.trim() || null, season: parsedSeason });
  };
  return <section className="review-correction" aria-label="修正当前识别计划">
    <header><div><span>PLAN CORRECTION</span><b>匹配不对？先修正，不要批准错计划</b><p>修改后会废弃当前摘要并重新扫描，不会执行媒体写入。</p></div><button type="button" onClick={() => setOpen(value => !value)}>{open ? "收起修正" : "修正识别参数"}</button></header>
    {open ? <form className="failure-repair-form" onSubmit={event => { event.preventDefault(); submit(); }}>
      <label><span>TMDB ID</span><input inputMode="numeric" value={tmdbId} placeholder="自动识别" onChange={event => { setTmdbId(event.target.value); if (event.target.value) setQuery(""); }} /></label>
      <label><span>TMDB 类型</span><select value={mediaType} onChange={event => setMediaType(event.target.value as "" | "tv" | "movie")}><option value="">自动识别</option><option value="tv">剧集</option><option value="movie">电影</option></select></label>
      <label><span>检索标题</span><input value={query} placeholder="沿用原片名" onChange={event => { setQuery(event.target.value); if (event.target.value) setTmdbId(""); }} /></label>
      {mediaType !== "movie" && settings?.media_type !== "collection" ? <label><span>指定季度</span><input inputMode="numeric" value={season} placeholder="自动判断" onChange={event => setSeason(event.target.value)} /></label> : null}
      <button className="primary" type="submit" disabled={pending}>{pending ? "正在重新生成…" : "应用并重新生成计划"}</button>
    </form> : null}
  </section>;
}

function FailureRepairPanel({ job, pending, onClose, onRetry, onKeepExisting }: { job: Job; pending: boolean; onClose: () => void; onRetry: (options?: JobRetryOptions) => Promise<boolean>; onKeepExisting?: () => Promise<boolean> }) {
  const settings = job.settings;
  const matchedType = job.plan?.review?.match?.media_type;
  const [repairOpen, setRepairOpen] = useState(false);
  const [tmdbId, setTmdbId] = useState(settings?.tmdb_id?.toString() ?? "");
  const [mediaType, setMediaType] = useState<"" | "tv" | "movie">(
    settings?.media_type === "tv" || settings?.media_type === "movie"
      ? settings.media_type : matchedType === "tv" || matchedType === "movie" ? matchedType : "",
  );
  const [query, setQuery] = useState(settings?.query ?? "");
  const [season, setSeason] = useState(settings?.season?.toString() ?? "");
  const [archivePassword, setArchivePassword] = useState("");
  const [confirmKeepExisting, setConfirmKeepExisting] = useState(false);
  const title = job.source.split("/").at(-1) || job.source;
  const passwordFailure = /wrong archive password|archive password|压缩包密码|解压密码|密码错误/i.test(job.error ?? "");
  const targetConflict = /目标目录已存在同名文件|目标冲突|拒绝覆盖/i.test(job.error ?? "");
  const submitRepair = () => {
    const parsedTmdb = tmdbId.trim() ? Number(tmdbId) : null;
    const parsedSeason = season.trim() ? Number(season) : null;
    if ((parsedTmdb !== null && (!Number.isInteger(parsedTmdb) || parsedTmdb <= 0 || !mediaType)) || (parsedSeason !== null && (!Number.isInteger(parsedSeason) || parsedSeason < 0 || parsedSeason > 100))) return;
    void onRetry({ tmdb_id: parsedTmdb, ...(mediaType ? { media_type: mediaType } : {}), query: query.trim() || null, season: parsedSeason, archive_password: archivePassword || null });
  };
  return <section className="taskdesk-failure-panel" aria-label={`${title}失败原因`}>
    <header><div><span>FAILURE REPAIR</span><h3>定位原因并原地修复</h3></div><button onClick={onClose}>收起</button></header>
    <dl>
      <div><dt>任务目录</dt><dd>{job.source}</dd></div>
      <div><dt>错误详情</dt><dd>{job.error || "后端未返回具体错误，请查看任务日志。"}</dd></div>
    </dl>
    <div className="failure-options">
      <button type="button" onClick={() => setRepairOpen(value => !value)}>{repairOpen ? "收起识别参数" : "修改识别参数"}</button>
      <p>识别错作品、错季度时无需删除任务，可在这里修正后重新分析。</p>
    </div>
    {targetConflict ? <div className="failure-options conflict-resolution">
      <button type="button" className={confirmKeepExisting ? "confirming" : ""} disabled={pending || !onKeepExisting} onClick={() => {
        if (!confirmKeepExisting) { setConfirmKeepExisting(true); return; }
        void onKeepExisting?.();
      }}>{pending ? "正在处理…" : confirmKeepExisting ? "确认保留现有版本" : "保留现有版本并结束"}</button>
      <p>不会覆盖目标库，也不会删除新上传的目录；任务会记为“已停止”，以后仍可把新资源作为替代版本重新处理。</p>
    </div> : null}
    {(repairOpen || passwordFailure) ? <form className="failure-repair-form" onSubmit={event => { event.preventDefault(); submitRepair(); }}>
      {repairOpen ? <label><span>TMDB ID</span><input inputMode="numeric" value={tmdbId} placeholder="自动识别" onChange={event => { setTmdbId(event.target.value); if (event.target.value) setQuery(""); }} /></label> : null}
      {repairOpen ? <label><span>TMDB 类型</span><select value={mediaType} onChange={event => setMediaType(event.target.value as "" | "tv" | "movie")}><option value="">自动识别</option><option value="tv">剧集</option><option value="movie">电影</option></select></label> : null}
      {repairOpen ? <label><span>检索标题</span><input value={query} placeholder="沿用原片名" onChange={event => { setQuery(event.target.value); if (event.target.value) setTmdbId(""); }} /></label> : null}
      {repairOpen && mediaType !== "movie" && settings?.media_type !== "collection" ? <label><span>指定季度</span><input inputMode="numeric" value={season} placeholder="自动判断" onChange={event => setSeason(event.target.value)} /></label> : null}
      {(passwordFailure || repairOpen) ? <label><span>压缩包密码</span><input type="password" autoComplete="off" value={archivePassword} placeholder="仅保存在本机任务目录" onChange={event => setArchivePassword(event.target.value)} /></label> : null}
      <button className="primary" type="submit" disabled={pending}>{pending ? "正在重新分析…" : "应用参数并重试"}</button>
    </form> : null}
    <footer><p>{passwordFailure ? "输入正确密码后才会重新解压；密码仅写入本机受限文件。" : targetConflict ? "目标冲突不会通过重复运行自行消失，请选择保留现有版本或修改识别参数。" : "按原参数重试适合临时连接、缓存问题；修改参数适合识别边界错误。"}</p>{!passwordFailure && !targetConflict ? <button onClick={() => void onRetry()} disabled={pending}>{pending ? "正在重试…" : "按原参数重新诊断"}</button> : null}</footer>
  </section>;
}

type DirectoryState = { key: "blocked" | "pending_delete" | "unprocessed" | "attention" | "running" | "completed" | "problem"; label: string; rank: number };

function directoryState(path: string, phase: Job["phase"] | undefined, jobs: Job[], backendState?: BrowseResult["directories"][number]["directory_state"]): DirectoryState {
  if (backendState === "pending_delete") return { key: "pending_delete", label: "待删除", rank: 5 };
  const matchingJob = jobs
    .filter(job => job.source === path || job.plan?.target_root === path)
    .sort((left, right) => right.updated_at.localeCompare(left.updated_at))[0];
  const currentPhase = matchingJob?.phase ?? phase;
  if (!currentPhase) return { key: "unprocessed", label: "未处理", rank: 0 };
  if (currentPhase === "completed") return { key: "completed", label: "已处理", rank: 4 };
  if (currentPhase === "recovered" || currentPhase === "cancelled") return { key: "unprocessed", label: "未处理", rank: 0 };
  if (currentPhase === "failed" || currentPhase === "recovery_required") return { key: "problem", label: "需处理", rank: 1 };
  if (APPROVAL_PHASES.has(currentPhase)) return { key: "attention", label: "待审核", rank: 2 };
  return { key: "running", label: "处理中", rank: 3 };
}

function NewTaskPanel({ source, category, jobs, createConflict, ready, pending, browser, browserOpen, browserPending, onSourceChange, onCategoryChange, onBrowse, onCloseBrowser, onSelectBrowserPath, onCreate }: { source: string; category: TargetCategory | ""; jobs: Job[]; createConflict: Job | null; ready: boolean; pending: boolean; browser: BrowseResult | null; browserOpen: boolean; browserPending: boolean; onSourceChange: (value: string) => void; onCategoryChange: (value: TargetCategory) => void; onBrowse: (path: string) => void; onCloseBrowser: () => void; onSelectBrowserPath: (path: string) => void; onCreate: () => void }) {
  const directories = (browser?.directories ?? [])
    .map(directory => ({
      ...directory,
      state: directory.selectable === false
        ? { key: "blocked" as const, label: "备份保留", rank: 5 }
        : directoryState(directory.path, directory.task_phase, jobs, directory.directory_state),
    }))
    .sort((left, right) => left.state.rank - right.state.rank || left.name.localeCompare(right.name, "zh-CN"));
  const counts = directories.reduce<Record<DirectoryState["key"], number>>((result, directory) => {
    result[directory.state.key] += 1;
    return result;
  }, { blocked: 0, pending_delete: 0, unprocessed: 0, attention: 0, running: 0, completed: 0, problem: 0 });

  return <section className="create-dialog create-panel create-panel-persistent">
    <form onSubmit={event => { event.preventDefault(); onCreate(); }}>
      <label htmlFor="source-path">媒体目录</label>
      <div className="create-source"><input id="source-path" value={source} onChange={event => onSourceChange(event.target.value)} /><button type="button" aria-expanded={browserOpen} onClick={() => { if (!browserOpen) onBrowse(source.startsWith(UNSCRAPED_MEDIA_ROOT) ? source : UNSCRAPED_MEDIA_ROOT); }} disabled={browserPending || browserOpen}>{browserPending ? "读取中" : browserOpen ? "目录已展开" : "浏览"}</button></div>
      <fieldset className="create-categories">
        <legend>目标分类</legend>
        {TARGET_CATEGORIES.map(value => <button type="button" key={value} className={category === value ? "active" : ""} aria-pressed={category === value} onClick={() => onCategoryChange(value)}><b>{value}</b><span>/{value}</span></button>)}
      </fieldset>
      {createConflict ? <aside className="create-conflict" role="status"><b>未重复创建</b><span>原任务 #{createConflict.id} · {PHASE[createConflict.phase].label}</span><small>{createConflict.source}</small><p>你仍停留在新建整理，可直接选择其他目录继续添加。</p></aside> : null}
      {browserOpen && browser && <section className="create-browser">
        <header><div><small>正在浏览</small><b>{browser.path}</b></div><button type="button" onClick={onCloseBrowser}>收起目录</button></header>
        <div className="create-browser-summary" aria-label="目录处理状态统计">
          <span className="state-unprocessed">未处理 <b>{counts.unprocessed}</b></span>
          {!!counts.problem && <span className="state-problem">需处理 <b>{counts.problem}</b></span>}
          {!!counts.attention && <span className="state-attention">待审核 <b>{counts.attention}</b></span>}
          {!!counts.running && <span className="state-running">处理中 <b>{counts.running}</b></span>}
          <span className="state-completed">已处理 <b>{counts.completed}</b></span>
          {!!counts.pending_delete && <span className="state-pending-delete">待删除 <b>{counts.pending_delete}</b></span>}
          {!!counts.blocked && <span className="state-blocked">备份保留 <b>{counts.blocked}</b></span>}
        </div>
        {browser.parent && browser.path !== UNSCRAPED_MEDIA_ROOT && <button type="button" className="create-parent" onClick={() => onBrowse(browser.parent!)}>← 返回上一级</button>}
        <div className="create-folders">{directories.length ? directories.map(directory => <button type="button" className={`directory-${directory.state.key}`} key={directory.path} onClick={() => onBrowse(directory.path)} disabled={directory.selectable === false} title={directory.disabled_reason}><i>DIR</i><span>{directory.name}</span><em>{directory.state.label}</em><b>{directory.selectable === false ? "—" : "→"}</b></button>) : <p>这个目录下没有子目录</p>}</div>
        <footer><span>确认后才会更改上方媒体目录</span><button type="button" className="button-primary" onClick={() => onSelectBrowserPath(browser.path)}>选择当前目录</button></footer>
      </section>}
      <footer><span className={ready ? "ready" : "offline"}><i />{ready ? "本地服务已就绪" : "等待服务就绪"}</span><div><button className="button-primary" type="submit" disabled={pending || !ready || !category || source.trim().replace(/\/+$/, "") === UNSCRAPED_MEDIA_ROOT}>{pending ? "正在创建…" : "创建并分析"}</button></div></footer>
    </form>
  </section>;
}
