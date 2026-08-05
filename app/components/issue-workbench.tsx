"use client";

import { useMemo, useState } from "react";
import type { Job } from "../core/contracts";
import { ACTIVE_PHASES, APPROVAL_PHASES, canRetry, isRecoverable } from "../core/job-state";

type IssueWorkbenchProps = {
  jobs: Job[];
  pending: boolean;
  onOpen: (job: Job) => void;
  onRecover: (job: Job) => void;
  onRetry: (job: Job) => Promise<boolean>;
  onRetryMany: (jobs: Job[]) => Promise<void>;
  onNewTask: () => void;
};

const PAGE_SIZE = 5;

function failureDiagnosis(error: string | null) {
  const value = error ?? "";
  if (/wrong archive password|archive password|压缩包密码|解压密码|密码错误/i.test(value)) return {
    label: "压缩包密码错误",
    note: "打开详情输入正确密码，系统会通过本地受限密码文件重新解压。",
  };
  if (/压缩|解压|archive|zip|rar|7z/i.test(value)) return {
    label: "压缩包异常",
    note: "检查分卷完整性或输入密码后，可沿用当前任务重新诊断。",
  };
  if (/TMDB|匹配|识别|季度|集数|episode/i.test(value)) return {
    label: "识别边界",
    note: "可修改 TMDB、检索标题或季度后原地重新分析。",
  };
  if (/AList|连接|网络|timeout|timed out|HTTP/i.test(value)) return {
    label: "连接异常",
    note: "刷新 AList 后重新诊断，不需要重建任务。",
  };
  if (/存在|冲突|覆盖|重复/i.test(value)) return {
    label: "目标冲突",
    note: "不会自动覆盖；打开后可保留现有版本并安全结束。",
  };
  return { label: "执行异常", note: "打开详情可查看具体原因并按原参数重试。" };
}

function safeForBulkRetry(error: string | null) {
  const value = error ?? "";
  if (/wrong archive password|archive password|压缩包密码|解压密码|密码错误|目标目录已存在同名文件|目标冲突|拒绝覆盖/i.test(value)) return false;
  return /AList|连接|网络|timeout|timed out|HTTP|TMDB 服务暂时不可用|请求过于频繁/i.test(value);
}

export function IssueWorkbench({
  jobs,
  pending,
  onOpen,
  onRecover,
  onRetry,
  onRetryMany,
  onNewTask,
}: IssueWorkbenchProps) {
  const recoveryJobs = useMemo(() => jobs.filter(job => isRecoverable(job)), [jobs]);
  const retryJobs = useMemo(() => jobs.filter(job => canRetry(job)), [jobs]);
  const reviewJobs = useMemo(() => jobs.filter(job => APPROVAL_PHASES.has(job.phase)), [jobs]);
  const issueJobs = useMemo(() => Array.from(new Map(
    [...recoveryJobs, ...retryJobs, ...reviewJobs].map(job => [job.id, job]),
  ).values()), [recoveryJobs, retryJobs, reviewJobs]);
  const runningCount = jobs.filter(job => ACTIVE_PHASES.has(job.phase)).length;
  const safeRetryJobs = retryJobs.filter(job => safeForBulkRetry(job.error));
  const [expanded, setExpanded] = useState(false);
  const [page, setPage] = useState(0);
  const pages = Math.max(1, Math.ceil(issueJobs.length / PAGE_SIZE));
  const safePage = Math.min(page, pages - 1);
  const visibleJobs = issueJobs.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

  return <section className="task-status-bar" aria-labelledby="task-status-title">
    <div className="task-status-overview">
      <div className="task-status-title">
        <i className={issueJobs.length ? "attention" : "clear"} aria-hidden="true" />
        <div><span>TASK STATUS</span><h1 id="task-status-title">任务状态</h1><p>{runningCount} 个运行中 · {issueJobs.length} 项待处理 · 缺集、中文字幕和补源只随当前作品自动检查</p></div>
      </div>
      <button type="button" className={`task-status-metric${expanded ? " active" : ""}`} disabled={issueJobs.length === 0} onClick={() => setExpanded(true)}><span>需处理</span><b>{issueJobs.length}</b></button>
      <div className="task-status-actions">
        <button type="button" className="primary" onClick={onNewTask}>＋ 新建整理</button>
      </div>
    </div>

    <details className="task-status-details" open={expanded} onToggle={event => setExpanded(event.currentTarget.open)}>
      <summary><span>{expanded ? "收起明细" : "查看待处理明细"}</span><b>{issueJobs.length}</b></summary>
      <div className="task-status-drawer">
        <header>
          <div><b>任务异常与确认</b><p>这里只显示当前作品任务需要介入的确认、失败和恢复；作品补齐流程在任务内继续。</p></div>
          {safeRetryJobs.length > 1 ? <div className="task-status-tools"><button type="button" onClick={() => void onRetryMany(safeRetryJobs)} disabled={pending}>批量重试临时故障 {safeRetryJobs.length} 项</button></div> : null}
        </header>

        <div className="task-status-list">
          {visibleJobs.map(job => {
            const title = job.source.split("/").at(-1) || job.source;
            const recoverable = isRecoverable(job);
            const retryable = canRetry(job);
            const reviewable = APPROVAL_PHASES.has(job.phase);
            const diagnosis = retryable ? failureDiagnosis(job.error) : null;
            const label = recoverable ? "写入状态需恢复" : reviewable ? "计划等待确认" : diagnosis?.label;
            const note = recoverable ? "将先读取事务日志，不会直接覆盖现有文件。" : reviewable ? "进入后只确认识别与必要风险，其余步骤由当前作品流程继续。" : diagnosis?.note;
            return <article key={job.id}>
              <i data-tone={recoverable ? "recovery" : retryable ? "retry" : "review"} aria-hidden="true" />
              <div><small>{label}</small><b title={job.source}>{title}</b><p>{note}</p></div>
              <div className="task-status-item-actions">
                {recoverable ? <button type="button" onClick={() => onRecover(job)} disabled={pending}>生成恢复计划</button> : null}
                {retryable ? <><button type="button" onClick={() => onOpen(job)}>查看并修正</button>{safeForBulkRetry(job.error) ? <button className="primary" type="button" onClick={() => void onRetry(job)} disabled={pending}>重试临时故障</button> : null}</> : null}
                {reviewable ? <button className="primary" type="button" onClick={() => onOpen(job)}>只审异常</button> : null}
              </div>
            </article>;
          })}
          {!visibleJobs.length ? <EmptyState label="没有需要处理的任务" /> : null}
        </div>

        {issueJobs.length > PAGE_SIZE ? <footer className="task-status-pagination"><span>第 {safePage + 1}/{pages} 页 · 共 {issueJobs.length} 项</span><div><button type="button" disabled={safePage === 0} onClick={() => setPage(value => Math.max(0, value - 1))}>上一页</button><button type="button" disabled={safePage >= pages - 1} onClick={() => setPage(value => Math.min(pages - 1, value + 1))}>下一页</button></div></footer> : null}
      </div>
    </details>
  </section>;
}

function EmptyState({ label }: { label: string }) {
  return <div className="task-status-empty"><b>{label}</b><span>状态变化后会自动更新。</span></div>;
}
