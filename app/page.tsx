"use client";

import { useEffect, useMemo, useState } from "react";

type ThemeId = "midnight" | "paper" | "aurora" | "terminal" | "bento";

const themes: Array<{
  id: ThemeId;
  number: string;
  name: string;
  english: string;
  description: string;
  swatches: string[];
}> = [
  {
    id: "midnight",
    number: "01",
    name: "深海控制台",
    english: "Midnight Command",
    description: "沉稳、专业，适合长时间运行与查看任务日志。",
    swatches: ["#08111d", "#0f2235", "#35d7ff"],
  },
  {
    id: "paper",
    number: "02",
    name: "纸张编辑部",
    english: "Paper Studio",
    description: "温暖、克制，把复杂刮削流程变成清晰表单。",
    swatches: ["#f4efe5", "#243026", "#c14f37"],
  },
  {
    id: "aurora",
    number: "03",
    name: "极光玻璃",
    english: "Aurora Glass",
    description: "柔和、通透，更像现代影音应用的操作面板。",
    swatches: ["#17142f", "#774eff", "#66e4d3"],
  },
  {
    id: "terminal",
    number: "04",
    name: "终端机",
    english: "Operator Terminal",
    description: "高密度、零装饰，面向熟悉脚本和日志的用户。",
    swatches: ["#060806", "#152015", "#b6ff4a"],
  },
  {
    id: "bento",
    number: "05",
    name: "媒体便当",
    english: "Media Bento",
    description: "轻快、直观，用大色块突出路径和下一步动作。",
    swatches: ["#fffdf7", "#6952e8", "#ff9b59"],
  },
];

const pipeline = [
  ["读取目录", "扫描视频、字幕与压缩分卷"],
  ["匹配 TMDB", "确认标准标题、年份与集数"],
  ["安全解压", "发现密码标记并在服务端解压"],
  ["整理入库", "审核计划后重命名、移动与刮图"],
];

export default function Home() {
  const [theme, setTheme] = useState<ThemeId>("midnight");
  const [path, setPath] = useState("/quark/影视/番剧/86不存.ZDZQ 全2季 1080P");
  const [mediaType, setMediaType] = useState("自动识别");
  const [safeMode, setSafeMode] = useState(true);
  const [phase, setPhase] = useState(0);
  const [running, setRunning] = useState(false);
  const [chosen, setChosen] = useState<ThemeId | null>(null);

  const current = useMemo(
    () => themes.find((item) => item.id === theme) ?? themes[0],
    [theme],
  );

  useEffect(() => {
    if (!running) return;
    const timer = window.setInterval(() => {
      setPhase((value) => {
        if (value >= pipeline.length) {
          setRunning(false);
          window.clearInterval(timer);
          return value;
        }
        return value + 1;
      });
    }, 720);
    return () => window.clearInterval(timer);
  }, [running]);

  function startPreview() {
    if (!path.trim()) return;
    setPhase(0);
    setRunning(true);
  }

  return (
    <main className="app-shell" data-theme={theme}>
      <div className="ambient ambient-one" />
      <div className="ambient ambient-two" />

      <header className="topbar">
        <a className="brand" href="#top" aria-label="ScrapeFlow 首页">
          <span className="brand-mark">SF</span>
          <span>
            <strong>ScrapeFlow</strong>
            <small>ALIST MEDIA AUTOMATION</small>
          </span>
        </a>
        <div className="top-actions">
          <span className="connection"><i /> AList 已连接</span>
          <span className="version">ENGINE 3.3.2</span>
        </div>
      </header>

      <section className="theme-rail" aria-label="五种界面风格">
        <div className="rail-heading">
          <span>SELECT INTERFACE</span>
          <strong>选择一个视觉方向</strong>
        </div>
        <div className="theme-list">
          {themes.map((item) => (
            <button
              className={`theme-option ${theme === item.id ? "active" : ""}`}
              key={item.id}
              onClick={() => setTheme(item.id)}
              aria-pressed={theme === item.id}
            >
              <span className="theme-number">{item.number}</span>
              <span className="theme-copy">
                <strong>{item.name}</strong>
                <small>{item.english}</small>
              </span>
              <span className="swatches" aria-hidden="true">
                {item.swatches.map((color) => (
                  <i key={color} style={{ background: color }} />
                ))}
              </span>
            </button>
          ))}
        </div>
      </section>

      <section className="workspace" id="top">
        <div className="workspace-head">
          <div>
            <span className="eyebrow">{current.number} / {current.english}</span>
            <h1>{current.name}</h1>
            <p>{current.description}</p>
          </div>
          <button className={`choose-button ${chosen === theme ? "chosen" : ""}`} onClick={() => setChosen(theme)}>
            {chosen === theme ? "✓ 已设为候选" : "选择这套风格"}
          </button>
        </div>

        {chosen && (
          <div className="choice-note" role="status">
            已记录：{themes.find((item) => item.id === chosen)?.name}。你仍可继续比较其他方案。
          </div>
        )}

        <div className="dashboard-grid">
          <section className="launch-card panel">
            <div className="panel-title">
              <span className="panel-index">A</span>
              <div><strong>启动一次刮削</strong><small>只需要提供 AList 文件路径</small></div>
            </div>

            <label className="path-field">
              <span>AList 媒体路径</span>
              <div className="input-wrap">
                <i>↳</i>
                <input
                  value={path}
                  onChange={(event) => setPath(event.target.value)}
                  placeholder="/quark/影视/番剧/待整理目录"
                  spellCheck={false}
                />
              </div>
            </label>

            <div className="field-group">
              <span className="field-label">媒体类型</span>
              <div className="segment-control">
                {["自动识别", "电视剧", "电影", "合集"].map((type) => (
                  <button
                    key={type}
                    className={mediaType === type ? "active" : ""}
                    onClick={() => setMediaType(type)}
                  >{type}</button>
                ))}
              </div>
            </div>

            <button className="safe-row" onClick={() => setSafeMode((value) => !value)} aria-pressed={safeMode}>
              <span className={`toggle ${safeMode ? "on" : ""}`}><i /></span>
              <span><strong>安全审核模式</strong><small>先生成不可篡改计划，确认后再整理</small></span>
              <b>{safeMode ? "ON" : "OFF"}</b>
            </button>

            <button className="primary-action" onClick={startPreview} disabled={running || !path.trim()}>
              <span>{running ? "正在分析目录" : phase === pipeline.length ? "重新分析" : "开始刮削"}</span>
              <i>{running ? "•••" : "→"}</i>
            </button>
            <p className="action-hint">演示模式不会修改媒体库 · 选定设计后接入本机引擎</p>
          </section>

          <section className="flow-card panel">
            <div className="panel-title">
              <span className="panel-index">B</span>
              <div><strong>自动化流程</strong><small>从原始文件到 Infuse 媒体墙</small></div>
            </div>
            <div className="pipeline">
              {pipeline.map(([title, detail], index) => {
                const complete = phase > index;
                const active = running && phase === index;
                return (
                  <div className={`pipeline-step ${complete ? "complete" : ""} ${active ? "active" : ""}`} key={title}>
                    <span className="step-dot">{complete ? "✓" : String(index + 1).padStart(2, "0")}</span>
                    <div><strong>{title}</strong><small>{detail}</small></div>
                    <i>{active ? "RUN" : complete ? "OK" : "—"}</i>
                  </div>
                );
              })}
            </div>
          </section>

          <section className="result-card panel">
            <div className="result-status">
              <span className={phase === pipeline.length ? "ready" : "idle"}>
                {phase === pipeline.length ? "计划就绪" : "等待任务"}
              </span>
              <small>LAST SCAN · JUST NOW</small>
            </div>
            <div className="media-title">
              <div className="poster-placeholder"><span>86</span><small>TMDB</small></div>
              <div>
                <small>自动匹配预览</small>
                <strong>{phase === pipeline.length ? "86-不存在的战区-" : "尚未识别媒体"}</strong>
                <p>{phase === pipeline.length ? "2021 · TV · TMDB 100565" : "运行目录分析后显示匹配结果"}</p>
              </div>
            </div>
            <div className="stats-row">
              <div><span>{phase === pipeline.length ? "25" : "—"}</span><small>视频</small></div>
              <div><span>{phase === pipeline.length ? "69" : "—"}</span><small>字幕</small></div>
              <div><span>{phase === pipeline.length ? "05" : "—"}</span><small>图稿</small></div>
            </div>
          </section>

          <section className="activity-card panel">
            <div className="activity-head"><strong>运行动态</strong><span>LIVE</span></div>
            <div className="log-lines" aria-live="polite">
              <p><time>14:58:03</time><span>连接 AList 服务</span><b>200</b></p>
              <p><time>14:58:04</time><span>等待输入媒体路径</span><b>READY</b></p>
              {phase > 0 && <p><time>NOW</time><span>已读取目录快照</span><b>OK</b></p>}
              {phase > 1 && <p><time>NOW</time><span>TMDB 匹配完成</span><b>99.6%</b></p>}
              {phase > 2 && <p><time>NOW</time><span>压缩包与密码检查完成</span><b>SAFE</b></p>}
            </div>
          </section>
        </div>
      </section>

      <footer>
        <span>ScrapeFlow / Designed for AList + TMDB + Infuse</span>
        <span>五套界面 · 一个安全引擎</span>
      </footer>
    </main>
  );
}
