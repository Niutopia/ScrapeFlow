"use client";

import {
  isLibraryAuditComplete,
  isLibraryAuditTraversalComplete,
  type GlobalControl,
  type Health,
  type Job,
  type LibraryAudit,
} from "../core/contracts";
import { ACTIVE_PHASES, PHASE, formatDate, isFailed } from "../core/job-state";

const ACQUISITION_PHASES = new Set([
  "gap_discovering", "provider_searching", "acquiring", "staging_verifying",
  "child_planning", "child_executing", "final_verifying",
]);

function jobTitle(job: Job) {
  return job.plan?.title || job.identity?.title || job.source.split("/").at(-1) || job.source;
}

function auditSummary(audit: LibraryAudit | null) {
  if (!audit) return { value: "等待快照", detail: "调度器尚未返回最近审计", tone: "quiet" };
  if (!audit.available || audit.status === "error" || audit.status === "unavailable") return { value: "审计异常", detail: "最近一次媒体库扫描不可用", tone: "danger" };
  const automaticTasks = audit.automatic_tasks?.length ?? 0;
  if (!isLibraryAuditTraversalComplete(audit)) return { value: "扫描中", detail: "正在读取正式媒体库", tone: "live" };
  if (isLibraryAuditComplete(audit)) {
    return { value: "全库完成", detail: "影视库业务状态已收口", tone: "success" };
  }
  const semantic = audit.semantic;
  const gapCount = typeof semantic?.gap_count === "number"
    ? semantic.gap_count
    : Array.isArray(semantic?.gaps) ? semantic.gaps.length : 0;
  const unknownCount = typeof semantic?.unknown_count === "number"
    ? semantic.unknown_count
    : Array.isArray(semantic?.unknowns) ? semantic.unknowns.length : 0;
  const jobGapCount = typeof semantic?.job_gap_count === "number"
    ? semantic.job_gap_count
    : Array.isArray(semantic?.job_gaps) ? semantic.job_gaps.length : 0;
  const errorCount = audit.errors?.length ?? 0;
  const explicitlyIncomplete = audit.library_complete === false || semantic?.library_complete === false;
  // ``clean`` is the audit's explicit truth value. A missing/null value is
  // not a pass, and semantic gaps/unknown evidence remain visible even when
  // the structural task list happens to be empty.
  if (explicitlyIncomplete || audit.clean !== true || automaticTasks || gapCount || unknownCount || jobGapCount || errorCount) {
    const count = automaticTasks + gapCount + unknownCount + jobGapCount + errorCount;
    return {
      value: `${count || 1} 项待处理`,
      detail: unknownCount
        ? "扫描已完成，但存在未知证据，系统会继续核对"
        : jobGapCount
          ? "扫描已完成，但自动任务状态仍待收敛"
          : errorCount
            ? "扫描已完成，但审计报告包含错误证据"
            : "扫描已完成，影视库业务仍未收口",
      tone: "attention",
    };
  }
  // A legacy report may have no explicit ``library_complete`` flag. Reaching
  // this branch means its old clean/semantic evidence is sufficient for the
  // compatibility fallback in ``isLibraryAuditComplete``.
  return { value: "待确认", detail: "扫描完成，但缺少影视库业务完成标记", tone: "attention" };
}

export function OperationsOverview({ health, control, jobs, audit }: {
  health: Health | null;
  control: GlobalControl | null;
  jobs: Job[];
  audit: LibraryAudit | null;
}) {
  const paused = !!control?.paused;
  const connected = !!health?.connected && !!health.tmdb_configured && !!health.engine_configured;
  const queued = jobs.filter(job => job.phase === "queued").length;
  const processing = jobs.filter(job => ACTIVE_PHASES.has(job.phase) && !["queued", "retry_wait"].includes(job.phase)).length;
  const retrying = jobs.filter(job => job.phase === "retry_wait").length;
  const attention = jobs.filter(isFailed).length;
  const acquiring = jobs.filter(job => ACQUISITION_PHASES.has(job.phase)).length;
  const latest = [...jobs].sort((left, right) => right.updated_at.localeCompare(left.updated_at))[0];
  const auditState = auditSummary(audit);
  const intakeError = health?.intake?.last_error;
  const intake = health?.intake_monitoring === false
    ? { value: "未启用", detail: "当前只接受 Web 或 API 提交来源路径", tone: "quiet" }
    : health?.intake_monitoring === true
      ? { value: intakeError ? "读取异常" : "监控中", detail: intakeError || "发现来源后会自动入队", tone: intakeError ? "danger" : "success" }
      : connected
        ? { value: "监听就绪", detail: "待刮削目录已接入自动流程", tone: "live" }
        : { value: "等待服务", detail: "连接 AList 与 TMDB 后启用监听", tone: "quiet" };

  return <section className="operations-overview" aria-labelledby="operations-overview-title">
    <header className="operations-overview-header">
      <div><span>AUTOMATION OVERVIEW</span><h1 id="operations-overview-title">自动运维总览</h1><p>待刮削目录由系统持续监听：识别、整理、补源、AList 回读和清理都在后台自动完成。</p></div>
      <div className={`operations-overview-badge tone-${paused ? "attention" : connected ? "success" : "quiet"}`}><i aria-hidden="true" /><span>{paused ? "调度已暂停" : connected ? "自动流程在线" : "等待服务"}</span></div>
    </header>
    <div className="operations-overview-grid">
      <article className="operations-metric" data-tone={paused ? "attention" : connected ? "success" : "quiet"}><span>调度器</span><b>{paused ? "已暂停" : connected ? "运行中" : "未就绪"}</b><small>{paused ? control?.reason || "新任务会保持排队" : "自动派发和重试已启用"}</small></article>
      <article className="operations-metric" data-tone={intake.tone}><span>待刮削监听</span><b>{intake.value}</b><small>{intake.detail}</small></article>
      <article className="operations-metric" data-tone={queued ? "live" : "quiet"}><span>等待队列</span><b>{queued}</b><small>{queued ? "个来源等待自动处理" : "没有等待来源"}</small></article>
      <article className="operations-metric" data-tone={processing ? "live" : "quiet"}><span>正在处理</span><b>{processing}</b><small>{retrying ? `${retrying} 个任务正在自动退避` : "识别、写入或核对中的任务"}</small></article>
      <article className="operations-metric" data-tone={attention ? "danger" : "success"}><span>需关注</span><b>{attention}</b><small>{attention ? "失败任务已停止，请查看原因或主动重试" : "没有失败任务"}</small></article>
      <article className="operations-metric" data-tone={acquiring ? "live" : auditState.tone}><span>审计 / 补源</span><b>{acquiring ? `${acquiring} 个补源中` : auditState.value}</b><small>{acquiring ? `搜索、获取或修复中 · ${health?.operations?.provider_workers ?? 0} 个获取 worker` : auditState.detail}</small></article>
    </div>
    <footer className="operations-overview-footer">
      {latest ? <div><span>最近活动</span><b>{jobTitle(latest)}</b><small>{PHASE[latest.phase]?.label || latest.phase} · 更新于 {formatDate(latest.updated_at)}</small></div> : <div><span>最近活动</span><b>暂无任务</b><small>新的来源会由待刮削监听自动加入队列。</small></div>}
      <p><b>自动策略：</b>正式媒体库保持单写入；下载、审计和搜索可并发执行。</p>
    </footer>
  </section>;
}
