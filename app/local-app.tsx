"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

const API = "http://127.0.0.1:8765/api";

type Section = "task" | "history" | "connection";
type JobPhase =
  | "queued" | "planning_archives" | "awaiting_archive_approval"
  | "starting_archive_execution" | "extracting_archives" | "planning_media"
  | "awaiting_media_approval" | "starting_media_execution" | "executing_media"
  | "completed" | "failed" | "cancelled";

type PlanSummary = {
  kind: "archive" | "media";
  archive_count?: number;
  part_count?: number;
  video_count?: number;
  title?: string;
  year?: string;
  season?: number;
  tmdb_id?: number;
  file_count?: number;
  source_root?: string;
  target_root?: string;
  warnings?: string[];
  items?: Array<{ archive_path: string; destination: string; parts: number; videos: number; password_source: string }>;
  files?: Array<{ source: string; target: string; name: string }>;
  truncated?: boolean;
};

type Job = {
  id: string;
  source: string;
  parent: string;
  media_type: string;
  absolute: boolean;
  prefer_simplified: boolean;
  created_at: string;
  updated_at: string;
  phase: JobPhase;
  logs: string[];
  error: string | null;
  digest: string | null;
  plan: PlanSummary | null;
};

type Health = {
  local: boolean;
  engine: string;
  alist_url: string;
  alist_configured: boolean;
  tmdb_configured: boolean;
  connected: boolean;
  archive_supported: boolean;
  alist_version?: string;
  message?: string;
};

const phaseCopy: Record<JobPhase, string> = {
  queued: "任务排队中",
  planning_archives: "检查压缩包",
  awaiting_archive_approval: "等待批准解压",
  starting_archive_execution: "准备解压",
  extracting_archives: "AList 正在解压",
  planning_media: "识别媒体并生成计划",
  awaiting_media_approval: "等待批准整理",
  starting_media_execution: "准备执行",
  executing_media: "正在整理媒体",
  completed: "整理完成",
  failed: "任务失败",
  cancelled: "任务已取消",
};

const runningPhases = new Set<JobPhase>([
  "queued", "planning_archives", "starting_archive_execution", "extracting_archives",
  "planning_media", "starting_media_execution", "executing_media",
]);

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `本地服务返回 ${response.status}`);
  return body as T;
}

function formatTime(value: string) {
  try {
    return new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(new Date(value));
  } catch { return "—"; }
}

function phaseProgress(phase?: JobPhase) {
  if (!phase) return 0;
  if (phase === "completed") return 100;
  if (phase === "failed" || phase === "cancelled") return 0;
  if (["planning_archives"].includes(phase)) return 15;
  if (["awaiting_archive_approval"].includes(phase)) return 28;
  if (["starting_archive_execution", "extracting_archives"].includes(phase)) return 42;
  if (["planning_media"].includes(phase)) return 66;
  if (["awaiting_media_approval"].includes(phase)) return 78;
  if (["starting_media_execution", "executing_media"].includes(phase)) return 90;
  return 5;
}

export function LocalApp() {
  const [section, setSection] = useState<Section>("task");
  const [health, setHealth] = useState<Health | null>(null);
  const [healthError, setHealthError] = useState("");
  const [path, setPath] = useState("");
  const [mediaType, setMediaType] = useState("auto");
  const [absolute, setAbsolute] = useState(true);
  const [simplified, setSimplified] = useState(true);
  const [job, setJob] = useState<Job | null>(null);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [busy, setBusy] = useState(false);
  const [uiError, setUiError] = useState("");
  const [showPlan, setShowPlan] = useState(false);

  const refreshHealth = useCallback(async () => {
    try {
      const result = await api<Health>("/health");
      setHealth(result);
      setHealthError("");
    } catch (error) {
      setHealth(null);
      setHealthError(error instanceof Error ? error.message : "无法连接本地服务");
    }
  }, []);

  const refreshJobs = useCallback(async () => {
    try {
      const result = await api<{ jobs: Job[] }>("/jobs");
      setJobs(result.jobs);
    } catch (error) {
      setUiError(error instanceof Error ? error.message : "读取任务记录失败");
    }
  }, []);

  useEffect(() => {
    refreshHealth();
    const timer = window.setInterval(refreshHealth, 10000);
    return () => window.clearInterval(timer);
  }, [refreshHealth]);

  useEffect(() => {
    if (section === "history") refreshJobs();
  }, [section, refreshJobs]);

  useEffect(() => {
    if (!job || !runningPhases.has(job.phase)) return;
    const timer = window.setInterval(async () => {
      try {
        const result = await api<{ job: Job }>(`/jobs/${job.id}`);
        setJob(result.job);
      } catch (error) {
        setUiError(error instanceof Error ? error.message : "任务状态读取失败");
      }
    }, 700);
    return () => window.clearInterval(timer);
  }, [job]);

  const start = async () => {
    if (!path.trim()) { setUiError("请粘贴具体的 AList 媒体目录或完整链接"); return; }
    setBusy(true); setUiError(""); setShowPlan(false);
    try {
      const result = await api<{ job: Job }>("/jobs", {
        method: "POST",
        body: JSON.stringify({ path, type: mediaType, absolute, prefer_simplified: simplified }),
      });
      setJob(result.job);
    } catch (error) {
      setUiError(error instanceof Error ? error.message : "任务启动失败");
    } finally { setBusy(false); }
  };

  const approve = async () => {
    if (!job?.digest) return;
    setBusy(true); setUiError("");
    try {
      const result = await api<{ job: Job }>(`/jobs/${job.id}/approve`, {
        method: "POST", body: JSON.stringify({ digest: job.digest }),
      });
      setJob(result.job); setShowPlan(false);
    } catch (error) {
      setUiError(error instanceof Error ? error.message : "计划批准失败");
    } finally { setBusy(false); }
  };

  const cancel = async () => {
    if (!job) return;
    setBusy(true);
    try {
      const result = await api<{ job: Job }>(`/jobs/${job.id}/cancel`, {
        method: "POST", body: JSON.stringify({ confirm: true }),
      });
      setJob(result.job);
    } catch (error) {
      setUiError(error instanceof Error ? error.message : "取消任务失败");
    } finally { setBusy(false); }
  };

  const waitingApproval = job?.phase === "awaiting_archive_approval" || job?.phase === "awaiting_media_approval";
  const isRunning = !!job && runningPhases.has(job.phase);
  const progress = phaseProgress(job?.phase);
  const currentPlanTitle = job?.plan?.kind === "archive" ? "解压计划" : "媒体整理计划";
  const counts = useMemo(() => ({
    videos: job?.plan?.video_count ?? job?.plan?.file_count ?? "—",
    archives: job?.plan?.archive_count ?? "—",
    operations: job?.plan?.kind === "media" ? job.plan.file_count ?? "—" : "—",
  }), [job]);

  return (
    <main className="local-shell">
      <aside className="local-sidebar">
        <div className="local-logo"><i>▶</i><span><b>ScrapeFlow</b><small>LOCAL MEDIA SERVER</small></span></div>
        <nav>
          <button className={section === "task" ? "active" : ""} onClick={() => setSection("task")}><i>＋</i><span>新建任务</span></button>
          <button className={section === "history" ? "active" : ""} onClick={() => setSection("history")}><i>◷</i><span>任务记录</span></button>
          <button className={section === "connection" ? "active" : ""} onClick={() => setSection("connection")}><i>⌁</i><span>连接状态</span></button>
        </nav>
        <section className={`local-engine ${health?.connected && health?.tmdb_configured ? "online" : "offline"}`}><i /><span><b>{health?.connected ? "本地引擎已连接" : "本地引擎未就绪"}</b><small>{health?.message || healthError || "正在检查…"}</small></span></section>
        <footer><span>N</span><div><b>niutopia</b><small>本机管理员</small></div></footer>
      </aside>

      <section className="local-main">
        <header className="local-topbar"><div><span className={`connection-dot ${health?.connected ? "online" : ""}`} /><b>{health?.connected ? `AList ${health.alist_version || "已连接"}` : "AList 未连接"}</b><small>{health?.alist_url || "http://127.0.0.1:5244"}</small></div><span className="local-only">仅在本机 127.0.0.1 运行</span><button onClick={refreshHealth}>刷新连接</button></header>

        {section === "task" && <>
          <section className="task-heading"><div><span>LOCAL SCRAPE WORKFLOW</span><h1>整理媒体目录</h1><p>粘贴具体的 AList 文件夹路径。先生成计划，审核通过后才会执行。</p></div>{job && <div className={`job-state ${job.phase}`}><i />{phaseCopy[job.phase]}</div>}</section>

          <section className="task-launch">
            {health?.connected && !health.archive_supported && <div className="version-warning"><b>当前 AList {health.alist_version}</b><span>普通媒体目录可以整理；服务器端自动解压需要升级到 v3.57.0 或更高版本。</span></div>}
            <label className="path-field"><span>AList 媒体路径或完整链接</span><div><i>⌁</i><input autoFocus value={path} onChange={event => setPath(event.target.value)} onKeyDown={event => { if (event.key === "Enter" && !isRunning) start(); }} placeholder="例如：/quark/影视/番剧/某个待整理目录" /></div><small>请输入单个电视剧、番剧或电影所在的目录，不要选择整个媒体库根目录。</small></label>
            <div className="task-options">
              <label><span>媒体类型</span><select value={mediaType} onChange={event => setMediaType(event.target.value)}><option value="auto">自动识别</option><option value="tv">电视剧 / 番剧</option><option value="movie">电影</option></select></label>
              <label className="switch-row"><input type="checkbox" checked={absolute} onChange={event => setAbsolute(event.target.checked)} /><span><b>绝对集数转换</b><small>将 13、14… 映射到后续季度</small></span></label>
              <label className="switch-row"><input type="checkbox" checked={simplified} onChange={event => setSimplified(event.target.checked)} /><span><b>优先简体字幕</b><small>同集多字幕时优先选择简体</small></span></label>
            </div>
            {(uiError || job?.error) && <div className="inline-error"><b>!</b><span>{uiError || job?.error}</span><button onClick={() => { setUiError(""); if (job?.error) setJob(null); }}>关闭</button></div>}
            <div className="launch-actions">
              <button className="primary-action" onClick={start} disabled={busy || isRunning}><i>{isRunning ? "•••" : "▶"}</i>{job?.phase === "completed" ? "开始新任务" : isRunning ? phaseCopy[job!.phase] : "分析并生成计划"}</button>
              {isRunning && <button className="cancel-action" onClick={cancel} disabled={busy}>取消任务</button>}
              <span><i>✓</i> 密码只在本机进程内使用</span><span><i>✓</i> 原压缩包默认保留</span>
            </div>
          </section>

          <section className="task-grid">
            <article className="workflow-card">
              <header><div><b>实时处理流程</b><small>{job ? `任务 ${job.id}` : "等待启动"}</small></div><span>{progress}%</span></header>
              <div className="progress-track"><i style={{ width: `${progress}%` }} /></div>
              {[
                ["检查压缩包", ["planning_archives", "awaiting_archive_approval", "starting_archive_execution", "extracting_archives"]],
                ["AList 服务端解压", ["starting_archive_execution", "extracting_archives"]],
                ["TMDB 识别与季集映射", ["planning_media", "awaiting_media_approval"]],
                ["重命名、移动与图稿", ["starting_media_execution", "executing_media", "completed"]],
              ].map(([name, phases], index) => {
                const list = phases as JobPhase[];
                const active = job ? list.includes(job.phase) : false;
                const thresholds = [28, 55, 78, 100];
                const done = progress >= thresholds[index] || job?.phase === "completed";
                return <div className={`workflow-step ${active ? "active" : ""} ${done ? "done" : ""}`} key={name as string}><i>{done ? "✓" : index + 1}</i><span><b>{name as string}</b><small>{["发现分卷、密码标记与目标冲突", "网盘服务器端完成，不下载视频", "匹配官方标题、年份、季度和集数", "生成标准文件名并完成最终校验"][index]}</small></span><em>{done ? "完成" : active ? "进行中" : "等待"}</em></div>;
              })}
            </article>

            <article className="stats-card"><header><div><b>任务统计</b><small>来自实际计划</small></div></header><div><span><b>{counts.videos}</b><small>视频 / 文件</small></span><span><b>{counts.archives}</b><small>压缩包</small></span><span><b>{counts.operations}</b><small>整理操作</small></span></div>{job?.plan?.kind === "media" && <section><span>匹配结果</span><b>{job.plan.title || "已匹配 TMDB"}</b><small>{[job.plan.year, job.plan.season !== undefined ? `第 ${job.plan.season} 季` : null, job.plan.tmdb_id ? `TMDB ${job.plan.tmdb_id}` : null].filter(Boolean).join(" · ")}</small></section>}</article>

            <article className="log-card"><header><div><b>实时日志</b><small>本机引擎输出</small></div><button onClick={() => job && navigator.clipboard?.writeText(job.logs.join("\n"))} disabled={!job?.logs.length}>复制</button></header><pre>{job?.logs.length ? job.logs.join("\n") : "等待任务启动…"}</pre></article>
          </section>

          {waitingApproval && job?.plan && <section className="approval-bar"><div><i>!</i><span><b>{currentPlanTitle}需要你的确认</b><small>{job.plan.kind === "archive" ? `将通过 AList 解压 ${job.plan.archive_count} 个归档；原压缩包不会删除。` : `将执行 ${job.plan.file_count} 项整理操作；请先查看源路径和目标路径。`}</small></span></div><button className="review-button" onClick={() => setShowPlan(true)}>查看完整计划</button><button className="approve-button" onClick={approve} disabled={busy}>确认并执行</button></section>}
        </>}

        {section === "history" && <section className="section-page"><header><div><span>LOCAL HISTORY</span><h1>任务记录</h1><p>当前本机服务启动后创建的任务。</p></div><button onClick={refreshJobs}>刷新</button></header><div className="history-list">{jobs.length ? jobs.map(item => <button key={item.id} onClick={() => { setJob(item); setSection("task"); }}><i className={item.phase} /><span><b>{item.source}</b><small>{item.id} · {formatTime(item.created_at)}</small></span><em>{phaseCopy[item.phase]}</em><strong>›</strong></button>) : <div className="empty-state"><i>◷</i><b>还没有任务记录</b><span>创建第一个刮削任务后会显示在这里。</span></div>}</div></section>}

        {section === "connection" && <section className="section-page"><header><div><span>LOCAL CONNECTION</span><h1>连接状态</h1><p>密钥只从启动 ScrapeFlow 的本机环境变量读取，不发送到网页或云端。</p></div><button onClick={refreshHealth}>重新检测</button></header><div className="connection-grid"><article><i className={health?.connected ? "ok" : "bad"}>{health?.connected ? "✓" : "!"}</i><span><b>AList 服务</b><small>{health?.message || healthError || "检测中"}</small></span><em>{health?.alist_version || "未连接"}</em></article><article><i className={health?.tmdb_configured ? "ok" : "bad"}>{health?.tmdb_configured ? "✓" : "!"}</i><span><b>TMDB API</b><small>{health?.tmdb_configured ? "本机密钥已配置" : "缺少 TMDB_API_KEY"}</small></span><em>{health?.tmdb_configured ? "已配置" : "未配置"}</em></article><article><i className={health?.archive_supported ? "ok" : "bad"}>{health?.archive_supported ? "✓" : "!"}</i><span><b>服务器端解压</b><small>{health?.archive_supported ? "AList 版本满足安全解压要求" : "需要 AList v3.57.0 或更高版本"}</small></span><em>{health?.archive_supported ? "可用" : "不可用"}</em></article><article><i className="ok">✓</i><span><b>本地安全边界</b><small>API 仅监听 127.0.0.1，只允许 localhost 页面访问</small></span><em>本机</em></article></div><div className="config-help"><b>启动时需要的配置</b><code>ALIST_PASSWORD&nbsp;&nbsp;TMDB_API_KEY</code><p>AList 地址默认为 http://127.0.0.1:5244，用户名默认为 admin；需要修改时使用 ALIST_URL 和 ALIST_USERNAME。</p></div></section>}
      </section>

      {showPlan && job?.plan && <div className="plan-overlay" role="dialog" aria-modal="true" aria-label={currentPlanTitle}><section className="plan-modal"><header><div><span>SHA-256</span><code>{job.digest}</code><h2>{currentPlanTitle}</h2></div><button onClick={() => setShowPlan(false)}>×</button></header><div className="plan-summary"><span><b>{job.plan.kind === "archive" ? job.plan.archive_count : job.plan.file_count}</b><small>{job.plan.kind === "archive" ? "归档" : "文件操作"}</small></span><span><b>{job.plan.kind === "archive" ? job.plan.video_count : job.plan.title || "TMDB"}</b><small>{job.plan.kind === "archive" ? "归档内视频" : "匹配结果"}</small></span></div><div className="plan-table"><div className="head"><span>来源</span><span>目标</span></div>{job.plan.kind === "archive" ? job.plan.items?.map(item => <div key={item.archive_path}><span>{item.archive_path}<small>{item.parts} 个分卷 · {item.videos} 个视频</small></span><span>{item.destination}</span></div>) : job.plan.files?.map((item, index) => <div key={`${item.source}-${index}`}><span>{item.source}</span><span>{item.target}</span></div>)}</div>{job.plan.truncated && <p className="plan-warning">文件较多，界面只显示前 250 项；完整计划保存在本机任务目录。</p>}<footer><button onClick={() => setShowPlan(false)}>返回检查</button><button className="approve-button" onClick={approve} disabled={busy}>确认摘要并执行</button></footer></section></div>}
    </main>
  );
}
