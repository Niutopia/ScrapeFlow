"use client";

import { useMemo, useState } from "react";
import type { Job } from "../core/contracts";
import { ACTIVE_PHASES, canRetry, isFailed } from "../core/job-state";

type IssueWorkbenchProps = {
  jobs: Job[];
  pending: boolean;
  onOpen: (job: Job) => void;
  onRetry: (job: Job) => Promise<boolean>;
  onRetryMany: (jobs: Job[]) => Promise<void>;
  onOpenFallback: () => void;
};

const PAGE_SIZE = 5;

function failureDiagnosis(job: Job) {
  const value = job.error ?? "";
  if (job.phase === "failed_identity" || /TMDB|匹配|识别|季度|集数|episode/i.test(value)) return {
    label: "自动识别失败",
    note: "身份匹配策略已经用尽。修正来源命名或 TMDB 配置后，可以主动重试。",
  };
  if (job.phase === "failed_provider" || /补源|provider|候选|download|下载/i.test(value)) return {
    label: "自动补源失败",
    note: "当前候选和重试次数已经用尽。检查来源配置后，可以主动重试。",
  };
  if (job.phase === "failed_verification" || /AList|回读|readback|可见|timeout|timed out|HTTP/i.test(value)) return {
    label: "远端核对失败",
    note: "AList 回读未能确认结果，任务已经停止。检查连接和远端目录后再重试。",
  };
  if (job.phase === "failed_cleanup" || /清理|cleanup|删除/i.test(value)) return {
    label: "自动清理失败",
    note: "任务拥有的临时内容没有清理完，任务已经停止；正式媒体不会因此被删除。",
  };
  return { label: "自动任务失败", note: "自动尝试已经结束。查看失败详情，处理原因后可主动重试。" };
}

function attemptCount(job: Job) {
  const attempts = job.automatic_attempts;
  if (typeof attempts === "number") return attempts;
  if (!attempts) return 0;
  return attempts.total ?? Math.max(attempts.identity ?? 0, attempts.write ?? 0, attempts.acquisition ?? 0);
}

export function IssueWorkbench({ jobs, pending, onOpen, onRetry, onRetryMany, onOpenFallback }: IssueWorkbenchProps) {
  const retryJobs = useMemo(() => jobs.filter(job => isFailed(job) && canRetry(job)), [jobs]);
  const issueJobs = useMemo(() => jobs.filter(isFailed), [jobs]);
  const runningCount = jobs.filter(job => ACTIVE_PHASES.has(job.phase)).length;
  const [expanded, setExpanded] = useState(false);
  const [page, setPage] = useState(0);
  const pages = Math.max(1, Math.ceil(issueJobs.length / PAGE_SIZE));
  const safePage = Math.min(page, pages - 1);
  const visibleJobs = issueJobs.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

  return <section className="task-status-bar" aria-labelledby="task-status-title">
    <div className="task-status-overview">
      <div className="task-status-title">
        <i className={issueJobs.length ? "attention" : "clear"} aria-hidden="true" />
        <div><span>TASK STATUS</span><h1 id="task-status-title">自动任务状态</h1><p>{runningCount} 个自动处理中 · {issueJobs.length} 项失败需查看</p></div>
      </div>
      <button type="button" className={`task-status-metric${expanded ? " active" : ""}`} disabled={issueJobs.length === 0} onClick={() => setExpanded(true)}><span>异常</span><b>{issueJobs.length}</b></button>
      <div className="task-status-actions"><button type="button" onClick={onOpenFallback}>补录已有目录</button></div>
    </div>

    <details className="task-status-details" open={expanded} onToggle={event => setExpanded(event.currentTarget.open)}>
      <summary><span>{expanded ? "收起明细" : "查看异常明细"}</span><b>{issueJobs.length}</b></summary>
      <div className="task-status-drawer">
        <header>
          <div><b>自动处理异常</b><p>这里只显示自动调度尚未收口的任务；打开任务可查看阶段、尝试次数和机器错误。</p></div>
          {retryJobs.length > 1 ? <div className="task-status-tools"><button type="button" onClick={() => void onRetryMany(retryJobs)} disabled={pending}>立即重试全部 {retryJobs.length} 项</button></div> : null}
        </header>

        <div className="task-status-list">
          {visibleJobs.map(job => {
            const title = job.source.split("/").at(-1) || job.source;
            const diagnosis = failureDiagnosis(job);
            return <article key={job.id}>
              <i data-tone="retry" aria-hidden="true" />
              <div><small>{diagnosis.label}</small><b title={job.source}>{title}</b><p>{diagnosis.note} · 已尝试 {attemptCount(job)} 次</p></div>
              <div className="task-status-item-actions">
                <button type="button" onClick={() => onOpen(job)}>查看自动详情</button>
                {canRetry(job) ? <button className="primary" type="button" onClick={() => void onRetry(job)} disabled={pending}>立即重试</button> : null}
              </div>
            </article>;
          })}
          {!visibleJobs.length ? <EmptyState label="没有异常任务" /> : null}
        </div>

        {issueJobs.length > PAGE_SIZE ? <footer className="task-status-pagination"><span>第 {safePage + 1}/{pages} 页 · 共 {issueJobs.length} 项</span><div><button type="button" disabled={safePage === 0} onClick={() => setPage(value => Math.max(0, value - 1))}>上一页</button><button type="button" disabled={safePage >= pages - 1} onClick={() => setPage(value => Math.min(pages - 1, value + 1))}>下一页</button></div></footer> : null}
      </div>
    </details>
  </section>;
}

function EmptyState({ label }: { label: string }) {
  return <div className="task-status-empty"><b>{label}</b><span>状态变化后会自动更新。</span></div>;
}
