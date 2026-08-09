"use client";

import { useState } from "react";
import type { Job, TargetShelf } from "../core/contracts";
import type { RetryCorrection } from "../core/api-client";
import { ACTIVE_PHASES, PHASE, START_GATE_PHASES, formatDate, isFailed } from "../core/job-state";

const DEFAULT_TARGET_SHELVES: TargetShelf[] = ["movie", "anime", "us_tv"];
const TARGET_SHELF_COPY: Record<TargetShelf, { label: string; detail: string }> = {
  movie: { label: "电影", detail: "Movie" },
  anime: { label: "番剧", detail: "Anime / TV" },
  us_tv: { label: "美剧", detail: "US TV" },
};

function providerLabel(provider?: string) {
  switch (provider) {
    case "magnet": return "磁力 / Torrent 来源";
    case "cloud_share": return "云分享来源（当前不可用）";
    default: return "自动来源";
  }
}

function attemptCount(job: Job) {
  const attempts = job.automatic_attempts;
  if (typeof attempts === "number") return attempts;
  if (!attempts) return 0;
  return attempts.total ?? Math.max(attempts.identity ?? 0, attempts.write ?? 0, attempts.acquisition ?? 0);
}

function phaseCopy(job: Job) {
  if (job.phase === "failed_identity") return "身份识别策略已经用尽，任务已停止。检查来源命名和 TMDB 配置后可以主动重试。";
  if (job.phase === "failed_provider") return "补源候选和自动尝试次数已经用尽，任务已停止。检查来源配置后可以主动重试。";
  if (job.phase === "failed_verification") return "AList 回读未能确认最终结果，任务已停止。请先检查连接和远端目录。";
  if (job.phase === "failed_cleanup") return "任务拥有的临时内容没有清理完，任务已停止；既有正式媒体不会被删除。";
  if (job.phase === "failed_write") return "正式库写入没有完整完成，任务已停止。请根据 AList 实际状态排查后再重试。";
  return "自动尝试已经结束。请查看错误，处理原因后主动重试。";
}

export function TaskExpansion({ job, pending, onClose, onStart, onRetry, onCancel, onCleanup }: {
  job: Job;
  pending: boolean;
  onClose: () => void;
  onStart: (targetShelf: TargetShelf) => Promise<boolean>;
  onRetry: (correction?: RetryCorrection) => Promise<boolean>;
  onCancel: () => Promise<boolean>;
  onCleanup: () => Promise<boolean>;
}) {
  if (START_GATE_PHASES.has(job.phase)) {
    return <TargetShelfStartPanel job={job} pending={pending} onClose={onClose} onStart={onStart} onCancel={onCancel} />;
  }
  if (ACTIVE_PHASES.has(job.phase)) {
    return <LiveTaskPanel job={job} pending={pending} onClose={onClose} onCancel={onCancel} />;
  }
  if (isFailed(job)) {
    return <AutomaticFailurePanel job={job} pending={pending} onClose={onClose} onRetry={onRetry} onCleanup={onCleanup} />;
  }
  if (job.phase === "completed" || job.phase === "completed_with_gaps") return <TaskSuccessPanel job={job} pending={pending} onClose={onClose} onCleanup={onCleanup} />;
  if (job.phase === "cancelled") return <CancelledTaskPanel job={job} pending={pending} onClose={onClose} onCleanup={onCleanup} />;
  return null;
}

function TargetShelfStartPanel({ job, pending, onClose, onStart, onCancel }: {
  job: Job;
  pending: boolean;
  onClose: () => void;
  onStart: (targetShelf: TargetShelf) => Promise<boolean>;
  onCancel: () => Promise<boolean>;
}) {
  const title = job.plan?.title || job.identity?.title || job.source.split("/").at(-1) || job.source;
  const [targetShelf, setTargetShelf] = useState<TargetShelf | null>(null);
  const allowedShelves = job.allowed_target_shelves?.length ? job.allowed_target_shelves : DEFAULT_TARGET_SHELVES;
  const conflict = job.phase === "target_policy_conflict";
  const meta = PHASE[job.phase];
  const submit = () => {
    if (targetShelf) void onStart(targetShelf);
  };

  return <section className={`target-shelf-panel ${conflict ? "conflict" : ""}`} aria-label={`${title}目标货架`}>
    <header><div><span>{conflict ? "TARGET CONFLICT" : "START GATE"}</span><h3>{title}</h3><p>{job.error || meta.detail}</p></div><button type="button" onClick={onClose}>收起</button></header>
    <div className="target-shelf-options" role="radiogroup" aria-label="目标货架">
      {allowedShelves.map(shelf => {
        const copy = TARGET_SHELF_COPY[shelf];
        const selected = targetShelf === shelf;
        return <button key={shelf} type="button" role="radio" aria-checked={selected} className={selected ? "selected" : ""} onClick={() => setTargetShelf(shelf)}><b>{copy.label}</b><small>{copy.detail}</small></button>;
      })}
    </div>
    <dl>
      <div><dt>来源目录</dt><dd>{job.source}</dd></div>
      <div><dt>当前货架</dt><dd>{job.target_shelf ? TARGET_SHELF_COPY[job.target_shelf].label : "尚未选择"}</dd></div>
      <div><dt>最后更新</dt><dd>{formatDate(job.updated_at)}</dd></div>
    </dl>
    <footer><span>{targetShelf ? `将启动到 ${TARGET_SHELF_COPY[targetShelf].label}` : "等待用户确认目标货架"}</span><div><button className="danger" type="button" disabled={pending} onClick={() => void onCancel()}>{pending ? "正在停止..." : "取消任务"}</button><button className="primary" type="button" disabled={pending || !targetShelf} onClick={submit}>{pending ? "正在启动..." : conflict ? "重新启动" : "启动任务"}</button></div></footer>
  </section>;
}

function TaskSuccessPanel({ job, pending, onClose, onCleanup }: {
  job: Job;
  pending: boolean;
  onClose: () => void;
  onCleanup: () => Promise<boolean>;
}) {
  const title = job.plan?.title || job.identity?.title || job.source.split("/").at(-1) || job.source;
  const count = job.plan?.file_count ?? 0;
  const cleanupCount = job.plan?.cleanup_file_count ?? 0;
  const resourceGaps = job.plan?.resource_gaps ?? [];
  const replenishment = job.plan?.replenishment;
  const selectedReleases = replenishment?.selections
    ?? (replenishment?.selection ? [replenishment.selection] : []);
  const readback = job.readback?.status === "verified" ? "已通过" : job.readback?.status || "已完成";
  const hasDeferredGaps = job.phase === "completed_with_gaps";
  return <section className="taskdesk-success-panel" aria-label={`${title}自动整理结果`}>
    <i aria-hidden="true">✓</i>
    <div><span>{hasDeferredGaps ? "自动流程已完成，保留缺口" : "自动流程完成"}</span><h3>{title}</h3><p>{hasDeferredGaps ? `自动识别、媒体整理、AList 回读和任务清理已完成；自动补源已跳过，仍有 ${resourceGaps.length} 项资源缺口。` : "自动识别、媒体整理、AList 回读和任务清理已完成。"}</p></div>
    <dl>
      <div><dt>文件</dt><dd>{count || "—"}</dd></div>
      <div><dt>AList 回读</dt><dd>{readback}</dd></div>
      <div><dt>目标目录</dt><dd>{job.plan?.target_root || job.parent}</dd></div>
      {cleanupCount ? <div><dt>已自动清理</dt><dd>{cleanupCount}</dd></div> : <div><dt>完成时间</dt><dd>{formatDate(job.updated_at)}</dd></div>}
    </dl>
    <div className="replenishment-failure-actions"><button onClick={onClose}>收起</button><button className="danger" type="button" disabled={pending} onClick={() => void onCleanup()}>{pending ? "正在清理…" : "清理任务记录"}</button></div>
    {(resourceGaps.length || selectedReleases.length) ? <ul className="taskdesk-success-gaps">
      {selectedReleases.map((selection, index) => selection.release_name ? <li key={`${selection.release_name}-${index}`}><b>自动补源候选 {index + 1}</b><span>{selection.release_name}{selection.selected_gap_ids?.length ? ` · 覆盖 ${selection.selected_gap_ids.join("、")}` : ""}</span></li> : null)}
      {resourceGaps.map((gap, index) => <li key={`${gap.kind || "gap"}-${gap.label || index}`}><b>{gap.label || gap.kind || "资源缺口"}</b><span>{gap.reason || "等待下次媒体库审计"}</span></li>)}
    </ul> : null}
  </section>;
}

function CancelledTaskPanel({ job, pending, onClose, onCleanup }: {
  job: Job;
  pending: boolean;
  onClose: () => void;
  onCleanup: () => Promise<boolean>;
}) {
  const title = job.plan?.title || job.identity?.title || job.source.split("/").at(-1) || job.source;
  return <section className="replenishment-failure-panel" aria-label={`${title}已停止任务`}>
    <header><div><span>TERMINAL TASK</span><h3>{title}</h3><p>任务已停止。清理只会移除这项任务拥有的本地状态和 staging，不会删除正式媒体库。</p></div><div className="replenishment-failure-actions"><button onClick={onClose}>收起</button><button className="danger" type="button" disabled={pending} onClick={() => void onCleanup()}>{pending ? "正在清理…" : "清理任务记录"}</button></div></header>
    <footer><p>来源目录：{job.source}</p><small>最后更新 {formatDate(job.updated_at)}</small></footer>
  </section>;
}

function LiveTaskPanel({ job, pending, onClose, onCancel }: { job: Job; pending: boolean; onClose: () => void; onCancel: () => Promise<boolean> }) {
  const title = job.plan?.title || job.identity?.title || job.source.split("/").at(-1) || job.source;
  const progress = job.progress;
  const meta = PHASE[job.phase] ?? PHASE.queued;
  const rawPercent = progress?.percent ?? meta.progress;
  const percent = Math.max(0, Math.min(100, Number.isFinite(rawPercent) ? rawPercent : meta.progress));
  const replenishment = job.plan?.replenishment;
  const candidatePool = replenishment?.selections
    ?? (replenishment?.selection ? [replenishment.selection] : []);
  const candidates = Array.from(new Map(candidatePool.map(candidate => [
    `${candidate.provider || "unknown"}:${candidate.release_name || "unknown"}:${(candidate.selected_gap_ids ?? []).join(",")}`,
    candidate,
  ])).values());
  const gaps = (replenishment?.gaps ?? job.plan?.resource_gaps ?? []).map(gap => ({
    id: "id" in gap ? gap.id : undefined,
    label: gap.label,
    reason: "reason" in gap ? gap.reason : undefined,
  }));
  const stageLabel: Record<string, string> = {
    analyzing: "分析来源",
    identity_matching: "自动识别",
    planning: "生成计划",
    executing_media: "正式库写入",
    verifying: "AList 回读",
    cleaning: "自动清理",
    retry_wait: "自动重试等待",
    gap_discovering: "检查补源缺口",
    provider_searching: "自动搜索补源",
    acquiring: "自动获取补源",
    staging_verifying: "核对补源文件",
    child_planning: "自动规划补源",
    child_executing: "自动整理补源",
    final_verifying: "补源最终核对",
    execution_apply: "正式库写入",
    execution_verify: "AList 回读",
  };
  const stage = progress?.stage ? stageLabel[progress.stage] || progress.stage : meta.label;
  const message = progress?.message || meta.detail;
  const retryDelay = job.phase === "retry_wait" && typeof job.next_retry_seconds === "number" && job.next_retry_seconds > 0
    ? `${job.next_retry_seconds} 秒后自动重试`
    : null;

  return <section className="live-task-panel" aria-label={`${title}自动进度`}>
    <header><div><span>AUTOMATIC JOB</span><h3>{title}</h3><p aria-live="polite">{message}</p></div><button type="button" onClick={onClose}>收起</button></header>
    <div className="live-task-progress" role="progressbar" aria-label={`${title}自动进度`} aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(percent)} aria-valuetext={message}>
      <span><i style={{ width: `${percent}%` }} /></span><b>{Math.round(percent)}%</b>
    </div>
    <dl>
      <div><dt>当前阶段</dt><dd>{stage}</dd></div>
      <div><dt>自动尝试</dt><dd>{attemptCount(job)} 次</dd></div>
      <div><dt>AList 回读</dt><dd>{job.readback?.status || "等待阶段完成"}</dd></div>
      <div><dt>最后更新</dt><dd>{formatDate(job.updated_at)}</dd></div>
    </dl>
    {retryDelay ? <aside className="live-task-previous-error" role="status"><b>自动重试已排队</b><p>{retryDelay}。系统会继续刷新 AList、重试当前操作或换补源候选。</p></aside> : null}
    {candidates.length ? <section className="live-task-candidates" aria-label="自动补源候选">
      <header><b>自动补源候选</b><span>系统最近选择 {candidates.length} 组</span></header>
      <ul>{candidates.map((candidate, index) => <li key={`${candidate.release_name || "candidate"}-${index}`}>
        <span>{providerLabel(candidate.provider)}</span>
        <strong>{candidate.release_name || "候选名称未记录"}</strong>
        <small>{candidate.resolution || "清晰度待核验"}{candidate.selected_gap_ids?.length ? ` · 覆盖 ${candidate.selected_gap_ids.length} 项` : ""}</small>
      </li>)}</ul>
    </section> : null}
    {gaps.length ? <details className="live-task-resources">
      <summary><span>系统发现的补源缺口</span><b>{gaps.length} 项</b></summary>
      <ol>{gaps.map((gap, index) => <li key={`${gap.id || gap.label || "gap"}-${index}`}><b>{gap.label || gap.id || "未命名缺口"}</b>{gap.reason ? <span>{gap.reason}</span> : null}</li>)}</ol>
    </details> : null}
    <footer><span title={job.source}>{job.source}</span><button className="danger" type="button" onClick={() => void onCancel()} disabled={pending}>停止此任务</button></footer>
  </section>;
}

function AutomaticFailurePanel({ job, pending, onClose, onRetry, onCleanup }: {
  job: Job;
  pending: boolean;
  onClose: () => void;
  onRetry: (correction?: RetryCorrection) => Promise<boolean>;
  onCleanup: () => Promise<boolean>;
}) {
  const title = job.plan?.title || job.identity?.title || job.source.split("/").at(-1) || job.source;
  const replenishment = job.plan?.replenishment;
  const selections = replenishment?.selections
    ?? (replenishment?.selection ? [replenishment.selection] : []);
  const [tmdbId, setTmdbId] = useState("");
  const [mediaType, setMediaType] = useState<RetryCorrection["media_type"] | "">("");
  const [season, setSeason] = useState("");
  const [archivePassword, setArchivePassword] = useState("");
  const needsIdentityCorrection = job.phase === "failed_identity";
  const hasTmdbId = !!tmdbId.trim();
  const missingMediaType = needsIdentityCorrection && hasTmdbId && !mediaType;
  const submitRetry = () => {
    const correction: RetryCorrection = {};
    if (needsIdentityCorrection && hasTmdbId) {
      if (!mediaType) return;
      correction.tmdb_id = Number(tmdbId);
      correction.media_type = mediaType;
      if (season.trim()) correction.season = Number(season);
    }
    if (archivePassword) correction.archive_password = archivePassword;
    setArchivePassword("");
    void onRetry(correction);
  };
  return <section className="replenishment-failure-panel" aria-label={`${title}自动失败详情`}>
    <header><div><span>AUTOMATIC FAILURE</span><h3>{title}</h3><p>{phaseCopy(job)}</p></div><div className="replenishment-failure-actions"><button className="primary" disabled={pending || missingMediaType} onClick={submitRetry}>{pending ? "正在请求…" : "立即重试"}</button><button className="danger" type="button" disabled={pending} onClick={() => void onCleanup()}>{pending ? "正在清理…" : "清理任务记录"}</button><button onClick={onClose}>收起</button></div></header>
    {needsIdentityCorrection ? <fieldset className="identity-correction"><legend>人工修正身份</legend><label>TMDB ID<input inputMode="numeric" value={tmdbId} onChange={event => setTmdbId(event.target.value)} /></label><label>媒体类型<select value={mediaType} onChange={event => setMediaType(event.target.value as RetryCorrection["media_type"] | "")}><option value="">不指定</option><option value="movie">电影</option><option value="tv">剧集</option></select></label><label>季（可选）<input inputMode="numeric" value={season} onChange={event => setSeason(event.target.value)} /></label></fieldset> : null}
    <label className="identity-correction">归档密码（一次性，可选）<input type="password" autoComplete="off" value={archivePassword} onChange={event => setArchivePassword(event.target.value)} /></label>
    <dl>
      <div><dt>失败阶段</dt><dd>{PHASE[job.phase]?.label || job.phase}</dd></div>
      <div><dt>自动尝试</dt><dd>{attemptCount(job)} 次</dd></div>
      <div><dt>机器错误</dt><dd>{job.error || "后端未返回具体错误"}</dd></div>
      <div><dt>AList 回读</dt><dd>{job.readback?.status || "未记录"}</dd></div>
      <div className="next-action"><dt>调度状态</dt><dd>已停止，等待排查或主动重试</dd></div>
    </dl>
    {selections.length ? <section className="replenishment-failure-candidates" aria-label="本轮自动补源候选">
      <b>本轮自动尝试的候选</b>
      <ul>{selections.map((selection, index) => <li key={`${selection.release_name || "candidate"}-${index}`}><span>{providerLabel(selection.provider)}</span><strong>{selection.release_name || "候选名称未记录"}</strong><small>{selection.resolution || "清晰度待核验"}{selection.selected_gap_ids?.length ? ` · 覆盖 ${selection.selected_gap_ids.length} 项` : ""}</small></li>)}</ul>
    </section> : null}
    <footer><p>来源目录：{job.source}</p><small>最后更新 {formatDate(job.updated_at)}</small></footer>
  </section>;
}
