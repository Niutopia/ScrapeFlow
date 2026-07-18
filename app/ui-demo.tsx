"use client";

import { useEffect, useState } from "react";

const defaultPath = "/quark/影视/番剧/待整理目录";
const stages = ["读取目录", "识别媒体", "解析压缩包", "生成计划"];

function useScrapeDemo() {
  const [path, setPath] = useState(defaultPath);
  const [stage, setStage] = useState(0);
  const [running, setRunning] = useState(false);
  useEffect(() => {
    if (!running) return;
    const timer = window.setInterval(() => setStage(value => {
      if (value >= 4) { window.clearInterval(timer); setRunning(false); return value; }
      return value + 1;
    }), 620);
    return () => window.clearInterval(timer);
  }, [running]);
  return { path, setPath, stage, running, start: () => { if (path.trim()) { setStage(0); setRunning(true); } } };
}

function Back({ number, label }: { number: string; label: string }) {
  return <a className="design-back" href="/"><span>‹</span><b>{number}</b><small>{label}</small></a>;
}

function StatusDot() { return <span className="status-online"><i /> AList 在线</span>; }

export function CommandUI() {
  const d = useScrapeDemo();
  return (
    <main className="cinema-ui infuse-ui" data-ui="01-infuse">
      <div className="cinema-bg" />
      <header className="infuse-nav"><a href="/" className="media-brand"><i>SF</i><b>ScrapeFlow</b></a><nav><button className="active">刮削</button><button>任务</button><button>资料库</button></nav><StatusDot /></header>
      <section className="infuse-content">
        <Back number="01" label="INFUSE DIRECTION" />
        <div className="infuse-title"><span>MEDIA AUTOMATION</span><h1>把目录变成媒体库</h1><p>AList&nbsp;&nbsp;·&nbsp;&nbsp;TMDB&nbsp;&nbsp;·&nbsp;&nbsp;Infuse 标准命名</p></div>
        <div className="infuse-command">
          <label><small>ALIST 媒体路径</small><input value={d.path} onChange={e => d.setPath(e.target.value)} /></label>
          <button onClick={d.start} disabled={d.running}><i>{d.running ? "•••" : "▶"}</i><span>{d.running ? stages[Math.min(d.stage, 3)] : d.stage === 4 ? "重新分析" : "开始刮削"}</span></button>
        </div>
        <div className="infuse-progress">{stages.map((stage, i) => <div className={d.stage > i ? "done" : d.running && d.stage === i ? "live" : ""} key={stage}><i>{d.stage > i ? "✓" : i + 1}</i><span>{stage}</span></div>)}</div>
      </section>
      <aside className="infuse-plan"><span>安全计划</span><b>{d.stage === 4 ? "94 项变更待审核" : "不会直接修改文件"}</b><small>原压缩包保留 · 字幕自动配对 · Infuse 命名</small></aside>
    </main>
  );
}

export function PaperUI() {
  const d = useScrapeDemo();
  return (
    <main className="tv-ui" data-ui="02-appletv">
      <header className="tv-nav"><a className="tv-brand" href="/"><i>SF</i> ScrapeFlow</a><nav><button className="active">立即整理</button><button>资料库</button><button>任务记录</button></nav><div><StatusDot /><span className="tv-avatar">N</span></div></header>
      <section className="tv-feature">
        <div className="tv-feature-bg" />
        <div className="tv-feature-copy"><Back number="02" label="TV DIRECTION" /><span className="tv-eyebrow">SCRAPEFLOW ORIGINAL WORKSPACE</span><h1>让杂乱文件，<br />登上你的海报墙。</h1><p>自动识别作品、展开加密分卷、校正季集，并为 Infuse 准备完整元数据。</p><div className="tv-actions"><button className="primary" onClick={d.start} disabled={d.running}>{d.running ? "正在分析…" : "▶  开始新任务"}</button><button className="round">···</button></div></div>
        <div className="tv-path"><span>来源</span><input value={d.path} onChange={e => d.setPath(e.target.value)} /><b>⌘ V</b></div>
      </section>
      <section className="tv-shelf"><header><h2>{d.stage === 4 ? "计划已生成" : "本次任务会做什么"}</h2><span>{d.stage}/4 已完成</span></header><div>{stages.map((stage, i) => <article className={d.stage > i ? "done" : ""} key={stage}><span>0{i + 1}</span><b>{stage}</b><p>{["盘点视频、字幕和分卷", "匹配 TMDB 标题与年份", "读取密码并安全解压", "生成移动与命名清单"][i]}</p><i>{d.stage > i ? "完成" : "等待"}</i></article>)}</div></section>
    </main>
  );
}

export function AuroraUI() {
  const d = useScrapeDemo();
  const rows = [
    ["[字幕组] 番剧合集 [01-24] 1080P", "文件夹", "18.4 GB", "待分析"],
    ["Anime.Collection.7z.001", "分卷压缩包", "2.15 GB", "检测到密码"],
    ["Anime.Collection.7z.002", "分卷压缩包", "2.15 GB", "关联分卷"],
    ["密码说明", "文件夹", "—", "密码候选"],
  ];
  return (
    <main className="nas-ui" data-ui="03-synology">
      <header className="nas-top"><a href="/" className="nas-logo"><i>SF</i><b>ScrapeFlow</b></a><div className="nas-search">⌕&nbsp;&nbsp;搜索任务、路径或媒体</div><StatusDot /><span className="nas-user">Niu</span></header>
      <aside className="nas-side"><Back number="03" label="NAS DIRECTION" /><nav><span>工作区</span><button className="active">▣&nbsp; 新建刮削</button><button>▤&nbsp; 任务记录</button><button>▦&nbsp; 整理计划</button><span>系统</span><button>◫&nbsp; AList 连接</button><button>⚙&nbsp; 偏好设置</button></nav><footer><i /> 引擎 3.3.2 正常</footer></aside>
      <section className="nas-main">
        <header><div><span>媒体整理</span><h1>新建刮削任务</h1></div><button className="nas-history">查看历史</button><button className="nas-run" onClick={d.start} disabled={d.running}>{d.running ? "分析中…" : "分析此路径"}</button></header>
        <section className="nas-form"><label><span>AList 路径</span><div><input value={d.path} onChange={e => d.setPath(e.target.value)} /><button>浏览</button></div></label><label><span>内容类型</span><select defaultValue="auto"><option value="auto">自动识别</option><option>剧集</option><option>电影</option></select></label><label><span>执行策略</span><select defaultValue="plan"><option value="plan">仅生成安全计划</option><option>审核后执行</option></select></label></section>
        <section className="nas-files"><header><b>源目录预览</b><span>{d.stage === 4 ? "已发现 8 个项目" : "路径内容"}</span></header><div className="nas-table"><div className="head"><span>名称</span><span>类型</span><span>大小</span><span>状态</span></div>{rows.map((row, i) => <div className={d.stage > 0 && i < d.stage ? "checked" : ""} key={row[0]}><span><i>{i ? "▱" : "▰"}</i>{row[0]}</span><span>{row[1]}</span><span>{row[2]}</span><span><b>{d.stage > 0 && i < d.stage ? "✓ 已读取" : row[3]}</b></span></div>)}</div></section>
        <footer className="nas-summary"><div><span>视频</span><b>{d.stage === 4 ? "23" : "—"}</b></div><div><span>字幕</span><b>{d.stage === 4 ? "46" : "—"}</b></div><div><span>压缩分卷</span><b>7</b></div><p><i>✓</i><span><b>安全模式已开启</b><small>生成计划前不会写入或移动任何文件</small></span></p></footer>
      </section>
    </main>
  );
}

export function TerminalUI() {
  const d = useScrapeDemo();
  return (
    <main className="emby-ui" data-ui="04-emby">
      <aside className="emby-side">
        <a href="/" className="emby-logo"><i>▶</i><b>ScrapeFlow</b><small>MEDIA SERVER</small></a>
        <Back number="04" label="EMBY DIRECTION" />
        <nav><span>主要功能</span><button className="active">⌂<b>控制台</b></button><button>＋<b>新建刮削</b></button><button>▦<b>媒体库</b><em>1</em></button><span>系统管理</span><button>◷<b>活动记录</b></button><button>☷<b>计划审核</b><em className="review">3</em></button><button>⚙<b>偏好设置</b></button></nav>
        <div className="emby-engine"><i /><span><b>整理引擎</b><small>3.3.2 · 工作正常</small></span></div>
        <footer><span>N</span><div><b>niutopia</b><small>服务器管理员</small></div><i>···</i></footer>
      </aside>
      <section className="emby-main">
        <header className="emby-topbar"><div className="emby-search">⌕<span>搜索媒体、任务或文件路径</span><kbd>⌘ K</kbd></div><button>◷</button><button>♧</button><StatusDot /><span className="emby-top-avatar">N</span></header>
        <div className="emby-welcome"><div><span>SCRAPEFLOW DASHBOARD</span><h1>媒体服务器</h1><p>从一个 AList 路径开始，自动完成识别、解压与标准化整理。</p></div><div className="emby-date"><small>服务器时间</small><b>07:56</b><span>2026 / 07 / 18</span></div></div>
        <section className="emby-hero">
          <div className="emby-art" aria-hidden="true"><i className="reel one" /><i className="reel two" /><i className="play">▶</i><span>SCRAPEFLOW</span></div>
          <div className="emby-hero-copy"><span>NEW SCRAPE JOB</span><h2>开始整理新的媒体目录</h2><p>粘贴 AList 路径。系统会先读取目录并生成审核计划，不会直接移动或删除文件。</p>
            <label className="emby-path"><i>⌁</i><div><small>ALIST SOURCE PATH</small><input aria-label="AList 媒体路径" value={d.path} onChange={e => d.setPath(e.target.value)} /></div><button>浏览</button></label>
            <div className="emby-hero-actions"><button className="emby-start" onClick={d.start} disabled={d.running}><i>{d.running ? "•••" : "▶"}</i>{d.running ? stages[Math.min(d.stage, 3)] : d.stage === 4 ? "重新生成计划" : "分析并生成计划"}</button><span><i>✓</i> 安全计划模式</span><span><i>✓</i> 保留原压缩包</span></div>
          </div>
        </section>
        <section className="emby-stats">
          <article><i className="mint">▦</i><span><small>发现视频</small><b>{d.stage === 4 ? "24" : "—"}</b></span><em>{d.stage === 4 ? "+24" : "等待"}</em></article>
          <article><i className="blue">CC</i><span><small>匹配字幕</small><b>{d.stage === 4 ? "48" : "—"}</b></span><em>{d.stage === 4 ? "100%" : "等待"}</em></article>
          <article><i className="amber">▱</i><span><small>压缩分卷</small><b>{d.stage > 2 ? "7" : "—"}</b></span><em>{d.stage > 2 ? "可展开" : "等待"}</em></article>
          <article><i className="violet">✓</i><span><small>计划操作</small><b>{d.stage === 4 ? "96" : "—"}</b></span><em>{d.stage === 4 ? "待审核" : "等待"}</em></article>
        </section>
        <section className="emby-workgrid">
          <article className="emby-progress-panel"><header><div><b>本次处理流程</b><small>{d.running ? "任务正在运行" : d.stage === 4 ? "计划生成完成" : "尚未启动"}</small></div><span>{d.stage}/4</span></header><div className="emby-progressbar"><i style={{ width: `${d.stage * 25}%` }} /></div>{stages.map((stage, i) => <div className={`emby-step ${d.stage > i ? "done" : d.running && d.stage === i ? "live" : ""}`} key={stage}><i>{d.stage > i ? "✓" : i + 1}</i><span><b>{stage}</b><small>{["盘点视频、字幕与压缩分卷", "匹配 TMDB 标题、年份和季数", "读取密码并校验解压目标", "准备重命名、移动与海报清单"][i]}</small></span><em>{d.stage > i ? "完成" : d.running && d.stage === i ? "进行中" : "等待"}</em></div>)}</article>
          <article className="emby-preview-panel"><header><div><b>识别预览</b><small>TMDB 匹配结果</small></div><button>查看详情</button></header><div className="emby-media-placeholder"><i>▶</i><span>MEDIA</span></div><h3>{d.stage === 4 ? "媒体标题已成功匹配" : "等待识别媒体"}</h3><p>{d.stage === 4 ? "电视剧 · 2 季 · 24 集 · 高置信度" : "开始分析后会显示官方名称与季集映射。"}</p><div><span><b>{d.stage === 4 ? "98%" : "—"}</b><small>匹配度</small></span><span><b>{d.stage === 4 ? "2" : "—"}</b><small>季</small></span><span><b>{d.stage === 4 ? "24" : "—"}</b><small>集</small></span></div></article>
          <article className="emby-activity"><header><div><b>实时活动</b><small>服务器事件</small></div><button>全部记录</button></header><p><i className="green" /><span><b>AList 服务已连接</b><small>刚刚 · 延迟 12ms</small></span></p><p><i className={d.stage > 0 ? "green" : ""} /><span><b>{d.stage > 0 ? "源目录读取完成" : "等待新的整理任务"}</b><small>{d.stage > 0 ? "发现媒体与压缩文件" : "安全计划模式已启用"}</small></span></p><p><i className={d.stage === 4 ? "green" : ""} /><span><b>{d.stage === 4 ? "审核计划已生成" : "写操作保持锁定"}</b><small>{d.stage === 4 ? "96 项操作等待确认" : "确认计划前不会修改文件"}</small></span></p></article>
        </section>
      </section>
    </main>
  );
}

export function BentoUI() {
  const d = useScrapeDemo();
  return (
    <main className="fusion-ui" data-ui="05-fusion">
      <div className="fusion-bg" />
      <header className="fusion-nav"><a href="/" className="fusion-brand"><i>SF</i><b>ScrapeFlow</b></a><nav><button className="active">工作台</button><button>任务</button><button>资料库</button></nav><StatusDot /></header>
      <section className="fusion-layout">
        <div className="fusion-copy"><Back number="05" label="SCRAPEFLOW ORIGINAL" /><span className="fusion-kicker">CINEMATIC MEDIA AUTOMATION</span><h1>从下载完成，<br />到海报墙亮起。</h1><p>一个路径，完成识别、解压、季集映射、标准命名与图稿准备。</p><div className="fusion-trust"><span>TMDB</span><span>ALIST</span><span>INFUSE</span><span>SAFE PLAN</span></div></div>
        <section className="fusion-console"><header><div><span>新建任务</span><b>{d.stage === 4 ? "计划已就绪" : "等待路径"}</b></div><i>{d.stage === 4 ? "94 项" : "01"}</i></header><label><span>媒体路径</span><textarea value={d.path} onChange={e => d.setPath(e.target.value)} rows={2} /></label><div className="fusion-options"><button className="active">自动识别</button><button>剧集</button><button>电影</button><span>仅生成计划&nbsp; ✓</span></div><button className="fusion-run" onClick={d.start} disabled={d.running}><span>{d.running ? stages[Math.min(d.stage, 3)] : d.stage === 4 ? "重新生成计划" : "生成刮削计划"}</span><b>→</b></button><div className="fusion-steps">{stages.map((stage, i) => <div className={d.stage > i ? "done" : d.running && d.stage === i ? "live" : ""} key={stage}><i>{d.stage > i ? "✓" : i + 1}</i><span>{stage}</span></div>)}</div></section>
      </section>
      <section className="fusion-result"><div className="fusion-poster" /><div><span>匹配预览</span><h2>{d.stage === 4 ? "媒体标题已成功匹配" : "等待媒体识别"}</h2><p>{d.stage === 4 ? "电视剧 · 2 季 · 24 集 · 高置信度" : "完成分析后，这里会显示匹配结果与季集映射。"}</p></div><div className="fusion-counts"><span><b>{d.stage === 4 ? "24" : "—"}</b>视频</span><span><b>{d.stage === 4 ? "48" : "—"}</b>字幕</span><span><b>{d.stage === 4 ? "7" : "—"}</b>分卷</span></div><button>查看完整计划</button></section>
    </main>
  );
}
