"use client";

import { useState } from "react";
import type { Job } from "../core/contracts";
import { approvalCopy, humanizeWarning } from "../core/job-state";

type ReviewPanelProps = {
  job: Job | null;
  open: boolean;
  pending: boolean;
  onClose: () => void;
  onApprove: () => Promise<boolean>;
  onSupplement: () => void;
};

const resourceGapKindLabel = (kind: string) => ({
  subtitle_without_video: "缺少对应视频",
  missing_episode: "缺少已发布剧集",
  missing_season: "缺少已发布季度",
  missing_multipart_segment: "缺少电影分段",
}[kind] ?? "资源缺失");

export function ReviewPanel({ job, open, pending, onClose, onApprove, onSupplement }: ReviewPanelProps) {
  const [confirmed, setConfirmed] = useState(false);
  if (!open || !job?.plan) return null;

  const copy = approvalCopy(job);
  const plan = job.plan;
  const warningCount = plan.warning_count ?? plan.warnings?.length ?? 0;
  const count = plan.file_count ?? 0;
  const problemFiles = plan.problem_files ?? [];
  const cleanupFiles = plan.cleanup_files ?? [];
  const cleanupCount = plan.cleanup_file_count ?? cleanupFiles.length;
  const cleanupGroups = plan.cleanup_groups ?? cleanupFiles.map(file => ({
    reason: file.reason,
    count: 1,
    examples: [file.source],
    truncated: false,
  }));
  const resourceGaps = plan.resource_gaps ?? [];
  const resourceGapCount = plan.resource_gap_count ?? resourceGaps.length;
  const problemCount = plan.problem_file_count ?? problemFiles.length;
  const displayedIssueCount = problemFiles.length;
  const unresolvedProblemCount = problemCount;
  const requiresCorrection = unresolvedProblemCount > 0;
  const assessment = plan.review;
  const destructiveCleanupCount = assessment?.destructive_cleanup_count ?? cleanupCount;
  const automaticCleanupCount = Math.max(0, cleanupCount - destructiveCleanupCount);
  const requiresConfirmation = assessment ? !assessment.automation_eligible : unresolvedProblemCount > 0 || destructiveCleanupCount > 0;
  const confirmationCount = requiresConfirmation
    ? Math.max(assessment?.reasons.length ?? 0, unresolvedProblemCount + destructiveCleanupCount)
    : 0;
  const normalCount = plan.normal_file_count
    ?? Math.max(0, count - problemFiles.filter(file => file.target).length);
  const match = assessment?.match;
  const candidates = match?.top_candidates ?? [];
  const confidence = typeof match?.confidence === "number" ? Math.round(match.confidence * 100) : null;

  return <section className="review-panel review-panel-inline" aria-labelledby="review-title">
    <header className="review-header">
      <div><span>{copy.kicker}</span><h2 id="review-title">{copy.title}</h2><p>{copy.note}</p></div>
      <button className="icon-button" onClick={onClose} aria-label="收起审核计划">×</button>
    </header>

    <div className="review-identity">
      <div><small>{plan.kind.toUpperCase()} PLAN</small><b>{plan.title || "安全恢复任务"}</b><span>{[plan.year, plan.tmdb_id ? `TMDB #${plan.tmdb_id}` : null].filter(Boolean).join("  /  ")}</span></div>
      <strong>{count}<small>文件</small></strong>
    </div>

    <div className="review-scope" aria-label="审核范围"><div><strong>{normalCount + automaticCleanupCount}</strong><span>自动核验通过</span></div><div><strong>{confirmationCount}</strong><span>真正需确认</span></div><div><strong>{resourceGapCount}</strong><span>仅供参考</span></div><p>自动通过项与审批项分开；长路径和逐文件清单默认收起，按需展开。</p></div>

    {assessment ? <section className={`review-assessment risk-${assessment.risk_level}`} aria-label="执行风险与匹配依据">
      <header><div><small>DECISION EVIDENCE</small><b>{assessment.automation_eligible ? "可自动执行" : "已暂停，等待确认"}</b></div><em>{assessment.risk_level === "high" ? "高风险" : assessment.risk_level === "medium" ? "需核对" : "低风险"}</em></header>
      {assessment.reasons.length ? <ul>{assessment.reasons.map(reason => <li key={reason}>{reason}</li>)}</ul> : <p>没有发现歧义、资源缺口或用户文件删除操作。</p>}
      {match?.tmdb_id ? <div className="match-evidence">
        <div><span>最终匹配</span><b>{match.title || plan.title} {match.year ? `(${match.year})` : ""}</b><small>{String(match.media_type || plan.kind).toUpperCase()} · TMDB #{match.tmdb_id}</small></div>
        {confidence !== null ? <strong>{confidence}%<small>匹配置信度</small></strong> : null}
      </div> : null}
      {candidates.length > 1 ? <details className="candidate-evidence"><summary>查看其他候选与评分差距</summary><ol>{candidates.map(candidate => <li key={`${candidate.media_type}-${candidate.tmdb_id}`}><span><b>{candidate.title}</b><small>{candidate.year} · {candidate.media_type.toUpperCase()} · TMDB #{candidate.tmdb_id}</small></span><strong>{Math.round(candidate.confidence * 100)}%</strong></li>)}</ol></details> : null}
    </section> : null}

    <details className="route-details">
      <summary><span>查看源目录与最终目标路径</span><b>路径详情</b></summary>
      <div className="route-map"><div><small>FROM</small><b>{plan.source_root || job.source}</b></div><i aria-hidden="true">→</i><div><small>TO</small><b>{plan.target_root || job.parent}</b></div></div>
    </details>

    <p className="mapping-passed">{normalCount} 项文件映射已自动核验通过{automaticCleanupCount ? `；${automaticCleanupCount} 个系统垃圾文件已判定为可自动清理` : ""}。</p>

    <section className={`review-notes ${warningCount ? "has-warning" : ""}`}>
      <header><b>{warningCount ? `${warningCount} 项需要留意` : "自动检查通过"}</b><span>{plan.truncated ? "当前展示关键项目" : "计划内容完整可见"}</span></header>
      {plan.warnings?.length
        ? <ol>{plan.warnings.map((warning, index) => <li key={`${warning}-${index}`}>{humanizeWarning(warning)}</li>)}</ol>
        : <p>没有发现额外风险或需要人工判断的映射。</p>}
      {warningCount > (plan.warnings?.length ?? 0) ? <p>仅展示前 {plan.warnings?.length ?? 0} 项，共 {warningCount} 项；完整证据仍保存在任务工件中。</p> : null}
    </section>

    {resourceGapCount ? <details className="mapping-details problem-files resource-gaps">
      <summary id="resource-gaps-title"><span>参考：资源不完整</span><b>{resourceGaps.length}/{resourceGapCount} 项缺口</b></summary>
      <div className="mapping-table">
        <div className="mapping-head"><span>缺失内容</span><span>具体原因 / 相关文件</span></div>
        {resourceGaps.map((gap, index) => <div key={`${gap.kind}-${gap.label}-${index}`}><span><b>{gap.label}</b><small>{resourceGapKindLabel(gap.kind)}</small></span><span><b>{gap.reason}</b>{gap.files.map(file => <small key={file}>{file}</small>)}</span></div>)}
      </div>
      {plan.truncated && resourceGaps.length < resourceGapCount ? <p className="problem-file-summary">缺口明细过多，当前显示 {resourceGaps.length} 项，共 {resourceGapCount} 项。</p> : null}
      <footer className="resource-gap-actions">
        <p>{plan.tmdb_id
          ? "缺口仅作信息提示，不阻塞当前计划。补充时请选择新的源目录。"
          : "缺口仅作信息提示，不阻塞当前计划。当前计划无法唯一锁定 TMDB，请从新建任务补充。"}</p>
        {plan.tmdb_id ? <button type="button" onClick={onSupplement}>补充资源</button> : null}
      </footer>
    </details> : null}

    {problemCount ? <details className="mapping-details problem-files requires-confirmation">
      <summary id="problem-files-title"><span>需修正：无法安全处理的文件</span><b>{displayedIssueCount}/{problemCount} 项</b></summary>
      <div className="mapping-table">
        <div className="mapping-head"><span>问题文件</span><span>原因 / 处理方式</span></div>
        {problemFiles.map((file, index) => <div key={`${file.source}-${index}`}><span>{file.source}</span><span><b>{file.reason}</b>{file.target ? <small>诊断目标：{file.target}</small> : null}<small>系统不会自动移动或删除；请修正识别，字幕需补齐逐视频闭环证据。</small></span></div>)}
      </div>
      {plan.truncated && problemFiles.length < problemCount ? <p className="problem-file-summary">文件明细过多，当前显示 {displayedIssueCount} 项，共 {problemCount} 项；所有问题项均保持原位并阻止执行。</p> : null}
      <p className="problem-file-summary">问题文件采用失败关闭策略：系统不会创建自动归档路线，也不能用人工确认绕过。请修正后重新生成计划。</p>
      <p className="problem-file-summary">其余 {normalCount} 项映射已通过自动检查，不再逐条展示。</p>
    </details> : null}

    {cleanupCount ? <details className={`mapping-details cleanup-review ${destructiveCleanupCount ? "requires-confirmation" : "automatic-cleanup"}`}>
      <summary id="cleanup-review-title"><span>{destructiveCleanupCount ? "需确认：执行后永久删除" : "自动通过：清理系统垃圾文件"}</span><b>{cleanupCount} 个文件</b></summary>
      <p className="cleanup-warning">{destructiveCleanupCount ? `其中 ${destructiveCleanupCount} 个用户文件不会进入回收站，需要明确确认。` : "这些仅是已识别的系统生成垃圾文件，不作为人工审批项。"}系统按原因合并展示，内层分组可查看代表路径。</p>
      <div className="cleanup-groups">{cleanupGroups.map(group => <details key={group.reason}><summary><span><b>{group.reason}</b><small>{group.truncated ? "显示 3 个示例" : "已列出全部示例"}</small></span><strong>{group.count}</strong></summary><ul>{group.examples.map(path => <li key={path}>{path}</li>)}</ul></details>)}</div>
    </details> : null}

    {requiresCorrection ? <div className="approval-check requires-correction"><span><b>当前计划不能执行</b><small>先修正上方问题文件；字幕状态将按每个视频的闭环证据重新核验。</small></span></div> : requiresConfirmation ? <label className="approval-check">
      <input type="checkbox" checked={confirmed} onChange={event => setConfirmed(event.target.checked)} />
      <span><b>{destructiveCleanupCount ? `我确认永久删除 ${destructiveCleanupCount} 个用户文件，并已核对其他风险` : confirmationCount ? `我已核对上方 ${confirmationCount} 项需确认内容` : "我已确认作品与最终目标路径"}</b><small>自动通过的映射和系统垃圾文件无需逐件审核；计划内容发生变化时需要重新确认。</small></span>
    </label> : <div className="approval-check automatic"><span><b>该计划已满足自动执行门禁</b><small>不需要确认或逐项勾选；服务会自动继续执行。</small></span></div>}

    {requiresConfirmation && !requiresCorrection ? <footer className="review-actions">
      <button className="button-primary" disabled={pending || !confirmed} onClick={() => void onApprove().then(ok => { if (ok) onClose(); })}>{pending ? "正在提交…" : copy.action}</button>
    </footer> : null}
  </section>;
}
