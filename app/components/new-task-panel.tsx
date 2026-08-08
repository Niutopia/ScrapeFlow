import type { BrowseResult, Job } from "../core/contracts";
import { PHASE, UNSCRAPED_MEDIA_ROOT } from "../core/job-state";

type DirectoryState = { key: "blocked" | "unprocessed" | "running" | "completed" | "problem"; label: string; rank: number };

function directoryState(path: string, phase: Job["phase"] | undefined, jobs: Job[]): DirectoryState {
  const matchingJob = jobs
    .filter(job => job.source === path || job.plan?.target_root === path)
    .sort((left, right) => right.updated_at.localeCompare(left.updated_at))[0];
  const currentPhase = matchingJob?.phase ?? phase;
  if (!currentPhase) return { key: "unprocessed", label: "未处理", rank: 0 };
  if (currentPhase === "completed") return { key: "completed", label: "已处理", rank: 4 };
  if (currentPhase === "cancelled") return { key: "unprocessed", label: "未处理", rank: 0 };
  if (currentPhase.startsWith("failed")) return { key: "problem", label: "处理失败", rank: 1 };
  return { key: "running", label: "处理中", rank: 3 };
}

export function NewTaskPanel({ source, jobs, createConflict, ready, pending, browser, browserOpen, browserPending, onSourceChange, onBrowse, onCloseBrowser, onSelectBrowserPath, onCreate }: { source: string; jobs: Job[]; createConflict: Job | null; ready: boolean; pending: boolean; browser: BrowseResult | null; browserOpen: boolean; browserPending: boolean; onSourceChange: (value: string) => void; onBrowse: (path: string) => void; onCloseBrowser: () => void; onSelectBrowserPath: (path: string) => void; onCreate: () => void }) {
  const directories = (browser?.directories ?? [])
    .map(directory => ({
      ...directory,
      state: directory.selectable === false
        ? { key: "blocked" as const, label: "不可选择", rank: 5 }
        : directoryState(directory.path, directory.task_phase, jobs),
    }))
    .sort((left, right) => left.state.rank - right.state.rank || left.name.localeCompare(right.name, "zh-CN"));
  const counts = directories.reduce<Record<DirectoryState["key"], number>>((result, directory) => {
    result[directory.state.key] += 1;
    return result;
  }, { blocked: 0, unprocessed: 0, running: 0, completed: 0, problem: 0 });

  return <section className="create-dialog create-panel fallback-source-panel">
    <form onSubmit={event => { event.preventDefault(); onCreate(); }}>
      <p className="fallback-source-note">正常情况下只需把内容放进 <b>{UNSCRAPED_MEDIA_ROOT}</b>；这里用于补录已经存在于其他位置的来源目录。</p>
      <label htmlFor="source-path">已有来源目录</label>
      <div className="create-source"><input id="source-path" value={source} onChange={event => onSourceChange(event.target.value)} /><button type="button" aria-expanded={browserOpen} onClick={() => { if (!browserOpen) onBrowse(source.startsWith(UNSCRAPED_MEDIA_ROOT) ? source : UNSCRAPED_MEDIA_ROOT); }} disabled={browserPending || browserOpen}>{browserPending ? "读取中" : browserOpen ? "目录已展开" : "浏览"}</button></div>
      {createConflict ? <aside className="create-conflict" role="status"><b>未重复补录</b><span>已有任务 #{createConflict.id} · {PHASE[createConflict.phase]?.label || createConflict.phase}</span><small>{createConflict.source}</small><p>该来源已经由自动流程接管。</p></aside> : null}
      {browserOpen && browser && <section className="create-browser">
        <header><div><small>正在浏览</small><b>{browser.path}</b></div><button type="button" onClick={onCloseBrowser}>收起目录</button></header>
        <div className="create-browser-summary" aria-label="目录处理状态统计">
          <span className="state-unprocessed">未处理 <b>{counts.unprocessed}</b></span>
          {!!counts.problem && <span className="state-problem">需处理 <b>{counts.problem}</b></span>}
          {!!counts.running && <span className="state-running">处理中 <b>{counts.running}</b></span>}
          <span className="state-completed">已处理 <b>{counts.completed}</b></span>
          {!!counts.blocked && <span className="state-blocked">不可选择 <b>{counts.blocked}</b></span>}
        </div>
        {browser.parent && browser.path !== UNSCRAPED_MEDIA_ROOT && <button type="button" className="create-parent" onClick={() => onBrowse(browser.parent!)}>← 返回上一级</button>}
        <div className="create-folders">{directories.length ? directories.map(directory => <button type="button" className={`directory-${directory.state.key}`} key={directory.path} onClick={() => onBrowse(directory.path)} disabled={directory.selectable === false} title={directory.disabled_reason}><i>DIR</i><span>{directory.name}</span><em>{directory.state.label}</em><b>{directory.selectable === false ? "—" : "→"}</b></button>) : <p>这个目录下没有子目录</p>}</div>
        <footer><span>选择后会更新上方媒体目录</span><button type="button" className="button-primary" onClick={() => onSelectBrowserPath(browser.path)}>使用当前目录</button></footer>
      </section>}
      <footer><span className={ready ? "ready" : "offline"}><i />{ready ? "自动流程已就绪" : "等待服务就绪"}</span><div><button className="button-primary" type="submit" disabled={pending || !ready || !source.trim()}>{pending ? "正在补录…" : "补录并自动处理"}</button></div></footer>
    </form>
  </section>;
}
