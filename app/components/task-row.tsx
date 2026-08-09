import type { Job } from "../core/contracts";
import {
  ACTIVE_PHASES,
  PHASE,
  START_GATE_PHASES,
  canRetry,
  formatDate,
  isFailed,
} from "../core/job-state";

function automaticAttemptLabel(job: Job) {
  const attempts = job.automatic_attempts;
  const total = typeof attempts === "number"
    ? attempts
    : attempts?.total ?? Math.max(attempts?.identity ?? 0, attempts?.write ?? 0, attempts?.acquisition ?? 0);
  return total > 0 ? `自动尝试 ${total} 次` : "";
}

export function TaskRow({ job, selected, expanded, pending, onToggle, onRetry }: { job: Job; selected: boolean; expanded: boolean; pending: boolean; onToggle: () => void; onRetry: () => void }) {
  const isQueued = typeof job.queue_position === "number" && job.queue_position > 0;
  const queueName = job.queue_kind === "execution" ? "文件操作" : "分析";
  const meta = isQueued
    ? { ...PHASE.queued, detail: `${queueName}队列第 ${job.queue_position} 位` }
    : PHASE[job.phase] ?? { ...PHASE.failed_write, label: "状态待识别", detail: `未识别状态：${job.phase}` };
  const liveProgress = !isQueued && ACTIVE_PHASES.has(job.phase) ? job.progress : null;
  const progress = liveProgress?.percent ?? meta.progress;
  const detail = liveProgress?.message || meta.detail;
  const progressCount = liveProgress && liveProgress.total > 1
    ? `${liveProgress.completed}/${liveProgress.total}`
    : null;
  const title = job.source.split("/").at(-1) || job.source;
  const resultAvailable = job.phase === "completed" || job.phase === "completed_with_gaps";
  const active = ACTIVE_PHASES.has(job.phase);
  const startable = START_GATE_PHASES.has(job.phase);
  const failed = isFailed(job);
  const retryable = canRetry(job);
  const expandable = startable || active || resultAvailable || failed || job.phase === "cancelled";
  const action = expanded ? { label: "收起", run: onToggle } : startable ? { label: job.phase === "target_policy_conflict" ? "重新选择" : "选择货架", run: onToggle } : resultAvailable ? { label: "查看结果", run: onToggle } : job.phase === "cancelled" ? { label: "清理记录", run: onToggle } : failed && retryable ? { label: "立即重试", run: onRetry } : failed ? { label: "查看异常", run: onToggle } : active ? { label: "查看自动进度", run: onToggle } : null;
  const titleContent = <><i className={`tone-${meta.tone}`} /><span><b>{title}</b><small>{job.source}</small></span></>;
  const retryDelay = job.phase === "retry_wait" && typeof job.next_retry_seconds === "number" && job.next_retry_seconds > 0
    ? `${job.next_retry_seconds} 秒后重试`
    : "";
  const statusDetail = [failed && job.error ? job.error : detail, automaticAttemptLabel(job), retryDelay].filter(Boolean).join(" · ");
  return <article className={`dashboard-task-row ${selected ? "selected" : ""}`}>
    {expandable ? <button className="taskdesk-task" onClick={onToggle} aria-expanded={expanded}>{titleContent}</button> : <div className="taskdesk-task">{titleContent}</div>}
    <div className={`taskdesk-state tone-${meta.tone}`}><i /><span><b>{meta.label}</b><small title={statusDetail}>{statusDetail}</small></span></div>
    <div className="taskdesk-progress" role="progressbar" aria-label={`${title}进度`} aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(progress)} aria-valuetext={detail}>
      <span aria-hidden="true"><i style={{ width: `${progress}%` }} /></span>
      <b>{Math.round(progress)}%{progressCount && <small>{progressCount}</small>}</b>
    </div>
    <time>{formatDate(job.updated_at)}</time>
    <div className="taskdesk-actions">{action && <button className={(failed || startable) && !expanded ? "row-primary" : ""} onClick={action.run} disabled={pending} aria-expanded={expandable ? expanded : undefined}>{action.label}</button>}</div>
  </article>;
}
