"use client";

import {
  isLibraryAuditComplete,
  isLibraryAuditTraversalComplete,
  type Job,
  type LibraryAudit,
} from "../core/contracts";
import { PHASE, formatDate, isFailed } from "../core/job-state";

type LibraryAuditPanelProps = {
  audit: LibraryAudit | null;
  auditPending: boolean;
  auditError: string;
  onRefreshAudit: () => void;
  onRunAudit: () => void;
  jobs: Job[];
};

const FINDING_LABELS: Record<string, string> = {
  zero_byte_file: "空文件",
  temporary_entry: "临时残留",
  possible_duplicate: "疑似重复",
  empty_directory: "空目录",
  missing_nfo: "缺 NFO",
  missing_poster: "缺海报",
  missing_media: "缺正片",
  missing_episode: "缺集",
  missing_season: "缺季度",
  missing_subtitle: "缺字幕",
  unknown_subtitle_evidence: "字幕证据未知",
  unknown_episode_catalog: "集数目录未知",
  unknown_inventory: "媒体库证据未知",
  unknown_library_work: "未登记作品",
};

const ACQUISITION_PHASES = new Set([
  "gap_discovering", "provider_searching", "acquiring", "staging_verifying",
  "child_planning", "child_executing", "final_verifying",
]);

function statusLabel(status: string) {
  if (status === "completed") return "已完成扫描";
  if (status === "unavailable") return "暂不可用";
  if (status === "error") return "扫描失败";
  if (status === "pending") return "自动扫描中";
  return status;
}

function findingLabel(kind: string) {
  return FINDING_LABELS[kind] ?? kind.replaceAll("_", " ");
}

function formatNumber(value: number | undefined) {
  return typeof value === "number" ? value.toLocaleString("zh-CN") : "—";
}

function formatTime(value?: string | null) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return value;
  return date.toLocaleString("zh-CN", { dateStyle: "short", timeStyle: "short" });
}

function shortPath(value?: string) {
  if (!value) return "未提供路径";
  return value.length > 86 ? `…${value.slice(-83)}` : value;
}

function auditTone(audit: LibraryAudit | null) {
  if (!audit) return "unknown";
  if (!audit.available || audit.status === "error" || audit.status === "unavailable") return "danger";
  if (!isLibraryAuditTraversalComplete(audit)) return "live";
  if (!isLibraryAuditComplete(audit)) return "attention";
  return "success";
}

function completionLabel(audit: LibraryAudit) {
  if (!audit.available || audit.status === "error" || audit.status === "unavailable") {
    return { value: statusLabel(audit.status), detail: "无法确认影视库业务状态" };
  }
  if (!isLibraryAuditTraversalComplete(audit)) {
    return { value: "扫描未完成", detail: "正在遍历正式媒体库" };
  }
  if (isLibraryAuditComplete(audit)) {
    return { value: "影视库已完成", detail: "扫描遍历和业务核对均已收口" };
  }
  return { value: "影视库待收敛", detail: "扫描遍历已完成，仍有业务缺口或未知证据" };
}

function observationSummary(audit: LibraryAudit | null) {
  const duplicateCount = audit?.duplicates?.length ?? 0;
  const emptyDirectoryCount = audit?.empty_directories?.length ?? 0;
  const archiveCount = audit?.archives?.length ?? 0;
  const attachmentCount = audit?.attachments?.length ?? 0;
  const unknownFileCount = audit?.unknown_files?.length ?? 0;
  const orphanSubtitleCount = audit?.orphan_subtitles?.length ?? 0;
  const categorizedResidualCount = archiveCount + attachmentCount + unknownFileCount;
  const residualCount = audit?.residuals?.length ?? 0;
  const uncategorizedResidualCount = Math.max(0, residualCount - categorizedResidualCount);
  if (!duplicateCount && !emptyDirectoryCount && !residualCount && !orphanSubtitleCount) return null;
  const labels = [
    duplicateCount ? `${duplicateCount} 组疑似重复` : "",
    emptyDirectoryCount ? `${emptyDirectoryCount} 个空目录` : "",
    archiveCount ? `${archiveCount} 个归档包` : "",
    attachmentCount ? `${attachmentCount} 个附件` : "",
    unknownFileCount ? `${unknownFileCount} 个未知文件` : "",
    uncategorizedResidualCount ? `${uncategorizedResidualCount} 个保留残留` : "",
    orphanSubtitleCount ? `${orphanSubtitleCount} 个孤立字幕` : "",
  ].filter(Boolean);
  return `${labels.join("、")}仅作观察，不会自动删除或移动。`;
}

function automaticAttempts(job: Job) {
  const attempts = job.automatic_attempts;
  if (typeof attempts === "number") return attempts;
  if (!attempts) return 0;
  return attempts.total ?? Math.max(attempts.identity ?? 0, attempts.write ?? 0, attempts.acquisition ?? 0);
}

function acquisitionStage(job: Job) {
  const labels: Record<string, string> = {
    gap_discovering: "检查缺口",
    provider_searching: "搜索候选",
    acquiring: "自动获取",
    staging_verifying: "核对暂存",
    child_planning: "生成内部计划",
    child_executing: "写入正式库",
    final_verifying: "最终回读",
  };
  return labels[job.phase] ?? PHASE[job.phase]?.label ?? job.phase;
}

export function LibraryAuditPanel({ audit, auditPending, auditError, onRefreshAudit, onRunAudit, jobs }: LibraryAuditPanelProps) {
  const tasks = audit?.automatic_tasks ?? [];
  const semantic = audit?.semantic;
  const semanticGapCount = semantic?.gap_count ?? semantic?.gaps?.length ?? 0;
  const semanticUnknownCount = semantic?.unknown_count ?? semantic?.unknowns?.length ?? 0;
  const semanticJobGapCount = semantic?.job_gap_count ?? semantic?.job_gaps?.length ?? 0;
  const semanticFindingCount = semanticGapCount + semanticUnknownCount + semanticJobGapCount;
  const visibleTasks = tasks.slice(0, 6);
  const tone = auditTone(audit);
  const completion = audit ? completionLabel(audit) : null;
  const observations = observationSummary(audit);
  const acquisitionJobs = jobs
    .filter(job => ACQUISITION_PHASES.has(job.phase) || !!job.plan?.replenishment)
    .sort((left, right) => right.updated_at.localeCompare(left.updated_at))
    .slice(0, 8);

  return <section className="library-operations-panel" aria-label="媒体库自动审计与补源状态">
    <div className="library-operations-grid">
      <article className="library-audit-card">
        <header><div><span>LIBRARY AUDIT</span><h2>媒体库自动审计</h2><p>系统会定期扫描正式库，自动发现缺集、缺字幕、缺海报和残留。</p></div><i className={`library-audit-dot tone-${tone}`} aria-hidden="true" /></header>
        <div className="library-audit-actions"><button type="button" onClick={onRunAudit} disabled={auditPending}>{auditPending ? "扫描中…" : "立即扫描"}</button><button type="button" onClick={onRefreshAudit} disabled={auditPending}>刷新结果</button>{audit ? <time dateTime={audit.finished_at ?? audit.started_at}>{formatTime(audit.finished_at ?? audit.started_at)}</time> : <span>等待自动扫描</span>}</div>
        {auditError ? <p className="library-operations-error" role="alert">{auditError}</p> : null}
        {!audit ? <div className="library-audit-empty"><b>暂无审计快照</b><span>调度器会自动建立正式媒体库快照。</span></div> : <>
          <div className="library-audit-summary"><div><b>{completion?.value}</b><span>{completion?.detail}</span></div><div><b>{formatNumber(audit.counts.files)}</b><span>文件</span></div><div><b>{formatNumber(audit.counts.videos)}</b><span>视频</span></div><div><b>{formatNumber(tasks.length + semanticFindingCount)}</b><span>自动待处理</span></div></div>
          <div className="library-audit-roots" aria-label="审计根目录状态">{audit.roots.map(root => <span key={root.path} data-tone={root.status === "completed" ? "success" : "danger"} title={root.error || root.path}><i aria-hidden="true" />{root.path.split("/").at(-1) || root.path}</span>)}</div>
          {visibleTasks.length || semanticFindingCount ? <div className="library-audit-worklist"><header><b>AUTOMATIC TASKS</b><span>{tasks.length + semanticFindingCount} 项</span></header><ul>{visibleTasks.map((item, index) => <li key={`${item.kind}-${item.path ?? item.basename ?? index}`}><strong>{findingLabel(item.kind)}</strong><span title={item.path || item.basename}>{shortPath(item.path || item.basename)}</span>{item.task ? <small>{item.task}</small> : null}</li>)}{semanticFindingCount ? <li><strong>语义缺口 / 未知证据</strong><span>详情已写入最近审计报告</span><small>{[
            semanticGapCount ? `语义缺口 ${semanticGapCount} 项` : "",
            semanticUnknownCount ? `未知证据 ${semanticUnknownCount} 项` : "",
            semanticJobGapCount ? `任务状态待收敛 ${semanticJobGapCount} 项` : "",
          ].filter(Boolean).join(" · ")}</small></li> : null}</ul></div> : <p className="library-audit-clean">当前没有需要自动处理的结构性问题。</p>}
          {observations ? <p className="library-audit-observations">{observations}</p> : null}
        </>}
      </article>

      <article className="replenishment-status-card">
        <header><div><span>AUTOMATIC ACQUISITION</span><h2>自动补源状态</h2><p>缺口由系统发现、搜索、获取、回刮、核对和清理；这里仅展示根任务状态。</p></div></header>
        {!acquisitionJobs.length ? <div className="library-audit-empty"><b>暂无自动补源任务</b><span>发现可判定缺口后，系统会自动建立补源阶段。</span></div> : <div className="acquisition-list">
          {acquisitionJobs.map(job => {
            const title = job.plan?.title || job.identity?.title || job.source.split("/").at(-1) || job.source;
            const gapCount = job.plan?.replenishment?.gap_count ?? job.plan?.gap_count ?? job.plan?.resource_gap_count ?? job.plan?.resource_gaps?.length ?? 0;
            const nextRetry = typeof job.next_retry_seconds === "number" && job.next_retry_seconds > 0 ? ` · ${job.next_retry_seconds} 秒后重试` : "";
            return <article key={job.id} data-phase={job.phase}>
              <div className="acquisition-heading"><i aria-hidden="true" /><div><b title={job.source}>{title}</b><small>{acquisitionStage(job)}</small></div><time dateTime={job.updated_at}>{formatDate(job.updated_at)}</time></div>
              <p title={job.source}>{shortPath(job.source)}</p>
              <div className="acquisition-meta"><span>{job.progress?.message || PHASE[job.phase]?.detail || "自动调度中"}</span>{gapCount ? <span>缺口 {gapCount} 项</span> : null}{automaticAttempts(job) ? <span>尝试 {automaticAttempts(job)} 次</span> : null}{nextRetry && job.phase === "retry_wait" ? <span>{nextRetry.slice(3)}</span> : null}</div>
              {isFailed(job) && job.error ? <small className="acquisition-error">{job.error}</small> : null}
            </article>;
          })}
        </div>}
      </article>
    </div>
  </section>;
}
