"use client";

import { useState } from "react";
import { IssueWorkbench } from "./components/issue-workbench";
import { LibraryAuditPanel } from "./components/library-audit-panel";
import { NewTaskPanel } from "./components/new-task-panel";
import { OperationsOverview } from "./components/operations-overview";
import { SystemControlBar } from "./components/system-control-bar";
import { TaskExpansion } from "./components/task-expansion";
import { TaskRow } from "./components/task-row";
import { DASHBOARD_FILTERS, useDashboardController } from "./hooks/use-dashboard-controller";

export function ScrapeFlowApp() {
  const {
    filter, setFilter, source, setSource, browserOpen, setBrowserOpen,
    expandedId, setExpandedId, refreshing, refreshFeedback,
    bulkRetrying, flow, groups, visibleJobs, ready, current, toggleTaskPanel,
    browse, selectBrowserPath, createJob, openIssueJob,
    retryMany, refreshDashboard,
  } = useDashboardController();
  const [fallbackOpen, setFallbackOpen] = useState(false);

  const openFallback = () => {
    setFallbackOpen(true);
    window.requestAnimationFrame(() => document.querySelector("#fallback-source")?.scrollIntoView({ behavior: "smooth", block: "nearest" }));
  };

  const submitFallback = async () => {
    if (await createJob()) setFallbackOpen(false);
  };

  return <main className="taskdesk-shell dashboard-shell">
    <header className="taskdesk-topbar taskdesk-topbar-single">
      <div className="brand" aria-label="ScrapeFlow"><span className="brand-mark" aria-hidden="true" /><b>SCRAPEFLOW</b></div>
      <div className="taskdesk-top-actions">
        <span className={`taskdesk-health ${ready ? "ready" : "offline"}`} title={flow.healthError || flow.health?.message || "本地连接状态"}><i aria-hidden="true" />{flow.health === null ? "连接中" : ready ? "自动流程在线" : "服务需要配置"}</span>
        <button className={`taskdesk-refresh-square ${refreshing ? "refreshing" : ""} ${refreshFeedback === "同步失败" ? "failed" : refreshFeedback ? "success" : ""}`} onClick={() => void refreshDashboard()} disabled={refreshing} aria-live="polite" title="刷新 AList 与自动任务状态"><i aria-hidden="true">↻</i><span>{refreshing ? "同步中" : refreshFeedback || "刷新状态"}</span></button>
      </div>
    </header>

    {flow.initializing ? <section className="boot-screen" aria-label="正在恢复自动任务"><div className="boot-glyph"><i /><i /><i /></div><b>正在接入自动运维平台</b><span>恢复待刮削监听、任务队列与自动重试</span></section> : <section className="dashboard-page automation-dashboard-page">
      <OperationsOverview health={flow.health} control={flow.control} jobs={flow.jobs} audit={flow.libraryAudit} />

      {flow.error ? <div className="taskdesk-error dashboard-error" role="alert"><b>!</b><span>{flow.error}</span><button onClick={() => flow.errorSource === "queue" ? void flow.refreshJobs() : flow.dismissError()}>{flow.errorSource === "queue" ? "刷新队列" : "关闭"}</button></div> : null}

      <SystemControlBar
        control={flow.control}
        controlError={flow.controlError}
        pending={flow.pending}
        onPause={() => void flow.setGlobalPause(true)}
        onResume={() => void flow.setGlobalPause(false)}
      />

      <details className="fallback-source" id="fallback-source" open={fallbackOpen} onToggle={event => setFallbackOpen(event.currentTarget.open)}>
        <summary><span><b>兜底：补录已有来源目录</b><small>待刮削目录会自动监听；仅在来源已经位于其他位置时使用。</small></span><i aria-hidden="true" /></summary>
        <NewTaskPanel source={source} jobs={flow.jobs} createConflict={flow.createConflict} ready={ready} pending={flow.pending} browser={flow.browser} browserOpen={browserOpen} browserPending={flow.browserPending} onSourceChange={setSource} onBrowse={browse} onCloseBrowser={() => setBrowserOpen(false)} onSelectBrowserPath={selectBrowserPath} onCreate={() => void submitFallback()} />
      </details>

      <section className="dashboard-layout" aria-label="自动任务列表">
        <div className="dashboard-primary">
          <section className="dashboard-queue">
            <header><div><span>AUTOMATIC JOBS</span><h2>任务列表</h2></div><div className="queue-controls"><div className="taskdesk-filters" role="tablist" aria-label="筛选任务">{DASHBOARD_FILTERS.map(item => <button key={item.value} role="tab" aria-selected={filter === item.value} className={filter === item.value ? "active" : ""} onClick={() => setFilter(item.value)}>{item.label}<span>{groups[item.value].length}</span></button>)}</div></div></header>
            <div className="dashboard-table-head"><span>任务</span><span>自动阶段</span><span>进度</span><span>更新时间</span><span>操作</span></div>
            <div className="dashboard-task-list">
              {visibleJobs.map(job => {
                const expanded = expandedId === job.id;
                const detailJob = expanded && current?.id === job.id ? current : job;
                return <div className={`taskdesk-job ${expanded ? "expanded" : ""}`} key={job.id} data-job-id={job.id}>
                  <TaskRow job={job} selected={expanded} expanded={expanded} pending={flow.pending} onToggle={() => void toggleTaskPanel(job)} onRetry={() => void flow.retry(job)} />
                  {expanded ? <TaskExpansion job={detailJob} pending={flow.pending} onClose={() => setExpandedId(null)} onRetry={(correction) => flow.retry(detailJob, correction)} onCancel={() => flow.cancel(detailJob)} onCleanup={async () => {
                    const removed = await flow.cleanup(detailJob);
                    if (removed) setExpandedId(null);
                    return removed;
                  }} /> : null}
                </div>;
              })}
              {!visibleJobs.length ? <div className="taskdesk-empty"><i>✓</i><b>这个列表暂时为空</b><span>待刮削监听发现来源后会自动加入任务队列。</span></div> : null}
            </div>
          </section>
        </div>
      </section>

      <IssueWorkbench jobs={flow.jobs} pending={flow.pending || bulkRetrying} onOpen={job => void openIssueJob(job)} onRetry={job => flow.retry(job)} onRetryMany={retryMany} onOpenFallback={openFallback} />

      <LibraryAuditPanel audit={flow.libraryAudit} auditPending={flow.auditPending} auditError={flow.auditError} onRefreshAudit={() => void flow.refreshLibraryAudit()} onRunAudit={() => void flow.runLibraryAudit()} jobs={flow.jobs} />
    </section>}
  </main>;
}
