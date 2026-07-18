"use client";

import { useEffect, useState } from "react";

const defaultPath = "/quark/影视/番剧/86不存.ZDZQ 全2季 1080P";
const steps = ["读取目录", "匹配 TMDB", "检查压缩包", "生成整理计划"];

function useDemo() {
  const [path, setPath] = useState(defaultPath);
  const [stage, setStage] = useState(0);
  const [running, setRunning] = useState(false);
  useEffect(() => {
    if (!running) return;
    const timer = window.setInterval(() => setStage((value) => {
      if (value >= 4) { window.clearInterval(timer); setRunning(false); return value; }
      return value + 1;
    }), 650);
    return () => window.clearInterval(timer);
  }, [running]);
  return { path, setPath, stage, running, start: () => { if (path.trim()) { setStage(0); setRunning(true); } } };
}

function Back({ number, name }: { number: string; name: string }) {
  return <a className="ui-back" href="/"><span>←</span><b>{number}</b><small>{name}</small></a>;
}

export function CommandUI() {
  const d = useDemo();
  return (
    <main className="ui-command" data-ui="01-command">
      <aside className="command-side">
        <a className="command-logo" href="/"><i>SF</i></a>
        <nav><button className="active">⌁<small>新任务</small></button><button>▦<small>任务库</small></button><button>◫<small>媒体库</small></button><button>⚙<small>设置</small></button></nav>
        <span className="command-avatar">N</span>
      </aside>
      <section className="command-main">
        <header><Back number="01" name="深海控制台" /><div className="server-pill"><i /> AList 5244 在线</div></header>
        <div className="command-title"><div><span>NEW SCRAPE MISSION</span><h1>启动整理任务</h1><p>从一个路径开始，ScrapeFlow 会处理后面的所有步骤。</p></div><button className="ghost-button">载入历史计划</button></div>
        <div className="command-board">
          <section className="command-launch">
            <label>ALIST SOURCE PATH</label>
            <div className="command-input"><span>↳</span><input value={d.path} onChange={e => d.setPath(e.target.value)} /><button>浏览</button></div>
            <div className="command-options"><button className="active">自动识别</button><button>电视剧</button><button>电影</button><button>合集</button></div>
            <div className="command-safe"><span>◆</span><div><b>安全计划已启用</b><small>执行前必须核对文件数与计划摘要</small></div><i>ON</i></div>
            <button className="command-run" onClick={d.start} disabled={d.running}><span>{d.running ? "正在分析" : "生成刮削计划"}</span><b>→</b></button>
          </section>
          <section className="command-pipeline">
            <header><b>任务流水线</b><span>{d.stage}/4 COMPLETE</span></header>
            {steps.map((step, i) => <div className={`command-step ${d.stage > i ? "done" : ""} ${d.running && d.stage === i ? "live" : ""}`} key={step}><i>{d.stage > i ? "✓" : `0${i + 1}`}</i><span><b>{step}</b><small>{["视频、字幕与分卷", "标题、年份与集数", "密码与解压目标", "重命名与图稿"][i]}</small></span><em>{d.stage > i ? "DONE" : d.running && d.stage === i ? "RUN" : "WAIT"}</em></div>)}
          </section>
        </div>
      </section>
      <aside className="command-inspector">
        <header><span>任务检查器</span><b>•••</b></header>
        <div className="command-poster"><strong>86</strong><span>TMDB · 100565</span></div>
        <h2>{d.stage === 4 ? "86-不存在的战区-" : "等待识别"}</h2><p>{d.stage === 4 ? "2021 · 电视剧 · 全 23 集" : "媒体详情会显示在这里"}</p>
        <div className="command-metrics"><div><b>{d.stage === 4 ? "25" : "—"}</b><small>视频</small></div><div><b>{d.stage === 4 ? "69" : "—"}</b><small>字幕</small></div><div><b>{d.stage === 4 ? "05" : "—"}</b><small>图稿</small></div></div>
        <div className="command-log"><span>LIVE ACTIVITY</span><p><i />连接 AList 服务</p><p><i />等待新的任务</p>{d.stage > 0 && <p><i className="cyan" />目录快照已读取</p>}</div>
      </aside>
    </main>
  );
}

export function PaperUI() {
  const d = useDemo();
  return (
    <main className="ui-paper" data-ui="02-paper">
      <header className="paper-mast"><div><Back number="02" name="纸张编辑部" /></div><h1>SCRAPEFLOW</h1><div><span>第 01 期</span><small>媒体整理专刊</small></div></header>
      <section className="paper-hero"><div className="paper-kicker">THE QUIET WAY TO ORGANIZE MEDIA</div><h2>把混乱的文件，<br /><em>编辑</em>成一座媒体库。</h2><p>输入原始文件所在位置。标题校对、压缩包解开、集数编排与海报装帧，将按编辑流程依次完成。</p></section>
      <section className="paper-sheet">
        <div className="paper-form">
          <span className="paper-section-no">01 / 来源</span>
          <label><small>AList 文件路径</small><input value={d.path} onChange={e => d.setPath(e.target.value)} /></label>
          <div className="paper-two"><label><small>内容类型</small><select defaultValue="auto"><option value="auto">自动判断</option><option>电视剧</option><option>电影</option></select></label><label><small>输出位置</small><input value="与源目录同级" readOnly /></label></div>
          <button className="paper-submit" onClick={d.start} disabled={d.running}>{d.running ? "编辑流程进行中…" : "提交给 ScrapeFlow"}<span>↗</span></button>
        </div>
        <aside className="paper-notes"><span className="paper-section-no">编者注</span><blockquote>“默认先排版计划，确认无误后才会触碰原文件。”</blockquote><div><b>安全计划</b><span>启用</span></div><div><b>原压缩包</b><span>保留</span></div><div><b>海报来源</b><span>TMDB</span></div></aside>
      </section>
      <section className="paper-process"><header><span>02 / 制作流程</span><b>{d.stage === 4 ? "校样完成" : "等待来稿"}</b></header><ol>{steps.map((step, i) => <li className={d.stage > i ? "done" : ""} key={step}><span>{i + 1}</span><b>{step}</b><small>{["盘点素材", "核对资料", "打开归档", "输出校样"][i]}</small></li>)}</ol></section>
      <footer className="paper-footer"><span>ALIST × TMDB × INFUSE</span><span>ScrapeFlow Editorial System</span></footer>
    </main>
  );
}

export function AuroraUI() {
  const d = useDemo();
  const current = Math.min(d.stage + 1, 4);
  return (
    <main className="ui-aurora" data-ui="03-aurora">
      <div className="aurora-orb one" /><div className="aurora-orb two" /><div className="aurora-grid" />
      <header className="aurora-nav"><a href="/" className="aurora-brand"><i>✦</i> ScrapeFlow</a><Back number="03" name="极光向导" /><span className="aurora-online"><i /> 系统就绪</span></header>
      <section className="aurora-wizard">
        <div className="aurora-progress"><span>STEP {current} OF 4</span><div><i style={{ width: `${(current / 4) * 100}%` }} /></div></div>
        <span className="aurora-badge">✦ 智能媒体整理向导</span>
        <h1>{d.stage === 4 ? "计划已经准备好了" : "你的媒体，住在哪里？"}</h1>
        <p>{d.stage === 4 ? "25 个视频与 69 个字幕已经完成映射，等待你的最终确认。" : "粘贴 AList 路径，接下来的识别、解压和命名交给我们。"}</p>
        <div className="aurora-path"><span>⌘</span><div><small>ALIST PATH</small><input value={d.path} onChange={e => d.setPath(e.target.value)} /></div><button onClick={d.start} disabled={d.running}>{d.running ? "···" : "继续"} <i>→</i></button></div>
        <div className="aurora-chips"><span>自动识别类型</span><span>安全审核模式</span><span>保留原压缩包</span></div>
      </section>
      <section className="aurora-floating">
        <article><i>◉</i><span><small>识别结果</small><b>{d.stage === 4 ? "86-不存在的战区-" : "等待路径"}</b></span></article>
        <article><div className="aurora-ring" style={{ "--progress": `${d.stage * 25}%` } as React.CSSProperties}><span>{d.stage * 25}%</span></div><span><small>当前进度</small><b>{d.running ? steps[d.stage] : d.stage === 4 ? "计划就绪" : "尚未开始"}</b></span></article>
        <article><i>⌁</i><span><small>连接状态</small><b>AList · 12ms</b></span></article>
      </section>
      <footer className="aurora-footer"><a href="/">查看其他设计方向</a><span>所有写操作都需要计划确认</span></footer>
    </main>
  );
}

export function TerminalUI() {
  const d = useDemo();
  return (
    <main className="ui-terminal" data-ui="04-terminal">
      <header className="terminal-bar"><div className="terminal-dots"><i /><i /><i /></div><span>SCRAPEFLOW_OPERATOR — zsh — 132×42</span><Back number="04" name="终端机" /></header>
      <div className="terminal-body">
        <aside className="terminal-tree"><header>EXPLORER</header><p>▾ SCRAPEFLOW</p><nav><button className="active">› run.scrape</button><button>› plans/</button><button>› journals/</button><button>› engine/</button><button>› settings.env</button></nav><footer><span>ALIST</span><b>● CONNECTED</b></footer></aside>
        <section className="terminal-console">
          <div className="terminal-tabs"><span className="active">run.scrape ×</span><span>output.log</span></div>
          <div className="terminal-copy"><p><em>01</em><span className="comment"># ScrapeFlow interactive operator</span></p><p><em>02</em><span><b>engine</b> = <i>"scraper.py@3.3.2"</i></span></p><p><em>03</em><span><b>mode</b> = <i>"safe-plan"</i></span></p><p><em>04</em><span><b>source</b> =</span></p></div>
          <label className="terminal-prompt"><span>scrapeflow@alist:~$</span><input value={d.path} onChange={e => d.setPath(e.target.value)} spellCheck={false} /></label>
          <div className="terminal-command"><span>python engine/scraper.py --type auto --plan-json ./plans/latest.json --</span><b>↵</b></div>
          <button className="terminal-execute" onClick={d.start} disabled={d.running}>[{d.running ? " RUNNING " : " EXECUTE DRY-RUN "}]</button>
          <div className="terminal-output"><p><b>INFO</b> AList session authenticated</p><p><b>INFO</b> waiting for operator input</p>{steps.slice(0, d.stage).map((step, i) => <p key={step}><b className="ok">{i === d.stage - 1 && d.running ? "RUN" : " OK "}</b> {step}</p>)}{d.stage === 4 && <p><b className="cyan">PLAN</b> sha256: 95e4c453…b503f1</p>}<span className="terminal-cursor">█</span></div>
        </section>
      </div>
      <footer className="terminal-status"><span>main*</span><span>UTF-8</span><span>Python 3.14</span><b>Ln 12, Col 1</b></footer>
    </main>
  );
}

export function BentoUI() {
  const d = useDemo();
  return (
    <main className="ui-bento" data-ui="05-bento">
      <header className="bento-nav"><a href="/" className="bento-logo"><i>S</i><b>ScrapeFlow</b></a><Back number="05" name="媒体便当" /><div><button>任务历史</button><span>N</span></div></header>
      <section className="bento-head"><div><span>MEDIA AUTOMATION, MADE SIMPLE</span><h1>丢进路径，<br />收获媒体墙。</h1></div><p>五秒配置一次刮削任务。自动处理压缩包、乱序集数、字幕语言和 Infuse 海报。</p></section>
      <section className="bento-grid">
        <article className="bento-path-card"><header><span>01</span><b>媒体在哪里？</b><i>必填</i></header><label><span>↳</span><input value={d.path} onChange={e => d.setPath(e.target.value)} /></label><footer><span>已连接 AList</span><b>路径有效 ✓</b></footer></article>
        <article className="bento-type-card"><header><span>02</span><b>内容类型</b></header><div><button className="active">自动</button><button>剧集</button><button>电影</button><button>合集</button></div><p>推荐保持自动识别</p></article>
        <article className="bento-safe-card"><span>SAFE</span><h2>先计划，<br />后执行。</h2><p>每一个移动动作都有摘要和日志。</p><i>✓ 已开启</i></article>
        <article className="bento-action-card"><span>{d.running ? "分析中" : d.stage === 4 ? "再来一次" : "一切就绪"}</span><button onClick={d.start} disabled={d.running}>{d.running ? "•••" : "开始刮削"}<b>↗</b></button></article>
        <article className="bento-flow-card"><header><b>自动化进度</b><span>{d.stage}/4</span></header><div>{steps.map((step, i) => <span className={d.stage > i ? "done" : ""} key={step}><i>{d.stage > i ? "✓" : i + 1}</i><b>{step}</b></span>)}</div></article>
        <article className="bento-result-card"><div className="bento-cover">86</div><span><small>匹配预览</small><b>{d.stage === 4 ? "86-不存在的战区-" : "等待识别"}</b><p>{d.stage === 4 ? "25 视频 · 69 字幕 · 5 图稿" : "结果会出现在这里"}</p></span></article>
      </section>
      <footer className="bento-footer"><span>ScrapeFlow © 2026</span><a href="/">← 返回五套方案</a></footer>
    </main>
  );
}
