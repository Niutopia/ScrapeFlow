"""Dependency-free same-origin industrial console for the local ScrapeFlow API.

The page is intentionally rebuilt from scratch.  It keeps the existing
same-origin API calls and operator safety boundaries, but does not reuse the
legacy dashboard's visual structure or selectors.
"""


DASHBOARD_HTML = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="dark">
<title>ScrapeFlow · 工业控制台</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='3' fill='%231d1e1c'/%3E%3Cpath d='M4 4h24v24H4z' fill='none' stroke='%23df6d43' stroke-width='4'/%3E%3Cpath d='M4 4h7v24H4z' fill='%23df6d43'/%3E%3C/svg%3E">
<style>
:root{
  --forge-void:#171817;
  --forge-panel:#1d1e1c;
  --forge-raised:#262724;
  --forge-line:#3a3b37;
  --forge-line-soft:#2e2f2c;
  --forge-text:#f1f0eb;
  --forge-muted:#bbb9b0;
  --forge-faint:#88877f;
  --forge-signal:#dc7048;
  --forge-alert:#d5a94b;
  --forge-ready:#9bbd79;
  --forge-danger:#db8064;
  --forge-radius:4px;
}
*{box-sizing:border-box}
html,body{min-height:100%;margin:0;background:var(--forge-void);color:var(--forge-text)}
body{
  font-family:"SF Pro Text","PingFang SC","Microsoft YaHei",sans-serif;
  font-size:14px;line-height:1.45;
}
button,input{font:inherit}
button{color:inherit}
button:disabled{opacity:.45;cursor:wait}
button:focus-visible,[role="button"]:focus-visible{
  outline:2px solid var(--forge-alert);outline-offset:2px;
}
.forge-app{
  min-height:100vh;
  background:
    linear-gradient(rgba(255,255,255,.016) 1px,transparent 1px),
    linear-gradient(90deg,rgba(255,255,255,.016) 1px,transparent 1px),
    var(--forge-void);
  background-size:32px 32px;
}
.forge-topbar{
  min-height:72px;display:flex;align-items:center;justify-content:space-between;gap:20px;
  padding:12px clamp(16px,3vw,38px);border-bottom:1px solid var(--forge-line);
  background:rgba(32,33,31,.96);
}
.forge-brand{display:flex;align-items:center;gap:10px;min-width:0}
.forge-brand__mark{
  width:18px;height:18px;display:block;flex:none;border:2px solid var(--forge-signal);
  border-left-width:6px;
}
.forge-brand__name{font-size:18px;font-weight:700;letter-spacing:0}
.forge-commands{display:flex;align-items:center;gap:12px;justify-content:flex-end}
.forge-status-cluster,.forge-command-actions{display:flex;align-items:center}
.forge-status-cluster{gap:12px;flex-wrap:wrap;justify-content:flex-end}
.forge-status{
  display:inline-flex;align-items:center;gap:8px;color:var(--forge-muted);
  font-size:12px;white-space:nowrap;
}
.forge-status::before{
  width:8px;height:8px;border-radius:50%;background:var(--forge-alert);
  box-shadow:0 0 0 4px rgba(213,169,75,.1);content:"";
}
.forge-status.is-ready::before{
  background:var(--forge-ready);box-shadow:0 0 0 4px rgba(155,189,121,.1);
}
.forge-status.is-bad::before{
  background:var(--forge-danger);box-shadow:0 0 0 4px rgba(219,128,100,.1);
}
.forge-status.is-paused::before{
  background:var(--forge-alert);box-shadow:0 0 0 4px rgba(213,169,75,.1);
}
.forge-button{
  min-height:36px;padding:0 14px;border:1px solid var(--forge-line);border-radius:var(--forge-radius);
  background:#2a2b28;color:var(--forge-text);font-size:13px;font-weight:600;cursor:pointer;
}
.forge-button:hover{border-color:#62635d;background:#343531}
.forge-button--signal{
  border-color:var(--forge-signal);background:var(--forge-signal);color:#20150f;
}
.forge-button--signal:hover{border-color:#ed8659;background:#ed8659}
.forge-button--danger{border-color:#874a38;background:#35201a;color:#f2c0ae}
.forge-button--quiet{background:transparent}
.forge-button--small{min-height:32px;padding:0 11px;font-size:12px}
#refreshButton{min-width:88px}
.forge-shell{width:min(1480px,100%);margin:0 auto;padding:22px clamp(16px,3vw,38px) 42px}
.forge-workbar{
  display:flex;align-items:center;justify-content:space-between;gap:20px;
  padding:0 0 16px;border-bottom:1px solid var(--forge-line);
}
.forge-view-tabs{display:flex;align-items:center;gap:18px}
.forge-view-tab{
  min-height:42px;padding:0;border:0;border-bottom:2px solid transparent;background:transparent;
  color:var(--forge-muted);font-size:18px;font-weight:700;cursor:pointer;
}
.forge-view-tab:hover{color:var(--forge-text)}
.forge-view-tab.is-active{border-color:var(--forge-signal);color:var(--forge-text)}
.forge-summary{
  display:grid;grid-template-columns:repeat(3,minmax(0,1fr));
  gap:10px;margin-top:16px;
}
.forge-summary-card{
  min-height:86px;padding:14px;border:1px solid var(--forge-line);background:var(--forge-panel);
}
.forge-summary-card span{color:var(--forge-faint);font-size:12px}
.forge-summary-card strong{display:block;margin-top:8px;font-size:23px;line-height:1}
.forge-summary-card--run strong{color:var(--forge-signal)}
.forge-summary-card--alert strong{color:var(--forge-alert)}
.forge-summary-card--ready strong{color:var(--forge-ready)}
.forge-section{margin-top:24px}
.forge-section__head{
  display:flex;align-items:center;justify-content:space-between;gap:18px;margin-bottom:10px;
}
.forge-section__head h2{margin:0;font-size:16px;font-weight:700}
.forge-section__head p{margin:4px 0 0;color:var(--forge-faint);font-size:12px}
.forge-source-toolbar{
  display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:start;gap:18px;margin-bottom:10px;
}
.forge-source-toolbar h2{margin:0;font-size:16px;font-weight:700}
.forge-source-toolbar p{margin:4px 0 0;color:var(--forge-faint);font-size:12px}
.forge-source-toolbar .forge-button{align-self:start}
.forge-filters{display:flex;gap:5px;flex-wrap:wrap}
.forge-filter{
  min-height:32px;padding:0 10px;border:1px solid var(--forge-line);border-radius:var(--forge-radius);
  background:transparent;color:var(--forge-muted);font-size:12px;cursor:pointer;
}
.forge-filter:hover{color:var(--forge-text);background:#252622}
.forge-filter.is-active{border-color:#6c6d65;background:#30312e;color:var(--forge-text)}
.forge-attention{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px}
.forge-alert-card{
  min-height:0;padding:13px;border:1px solid #68532f;background:#29271e;
}
.forge-alert-card strong{display:block;font-size:13px}
.forge-alert-card p{margin:6px 0 12px;color:#d2bd8f;font-size:12px}
.forge-table-head,.forge-job-row{
  display:grid;
  grid-template-columns:minmax(260px,1.65fr) minmax(120px,.62fr) minmax(120px,.5fr) minmax(210px,.82fr);
  gap:16px;align-items:center;
}
.forge-table-head{
  min-height:38px;padding:0 10px;border:1px solid var(--forge-line);border-bottom:0;
  background:#252622;color:var(--forge-faint);font-size:11px;
}
.forge-job-board{border:1px solid var(--forge-line)}
.forge-job-row{
  min-height:82px;padding:11px 10px;border-bottom:1px solid var(--forge-line-soft);cursor:pointer;
}
.forge-job-row:last-child{border-bottom:0}
.forge-job-row:hover{background:#262724}
.forge-job-index{display:none}
.forge-job-main{min-width:0}
.forge-job-main strong{
  display:block;overflow:hidden;color:var(--forge-text);font-size:14px;
  text-overflow:ellipsis;white-space:nowrap;
}
.forge-job-main small{
  display:block;overflow:hidden;margin-top:5px;color:var(--forge-faint);font-size:11px;
  text-overflow:ellipsis;white-space:nowrap;
}
.forge-progress{height:3px;max-width:none;margin-top:8px;background:var(--forge-line)}
.forge-progress i{display:block;width:0;height:100%;background:var(--forge-signal)}
.forge-progress i.is-complete{background:var(--forge-ready)}
.forge-job-stage{display:grid;gap:6px;justify-items:start}
.forge-chip{
  display:inline-flex;align-items:center;min-height:25px;padding:0 9px;border:1px solid var(--forge-line);
  border-radius:999px;color:var(--forge-muted);font-size:11px;white-space:nowrap;
}
.forge-chip--ready{border-color:#526343;color:var(--forge-ready)}
.forge-chip--alert{border-color:#66502e;color:var(--forge-alert)}
.forge-chip--danger{border-color:#633629;color:#e29476}
.forge-chip--run{border-color:#6a3d2d;color:#ef9a75}
.forge-job-stage small,.forge-job-metrics{color:var(--forge-faint);font-size:11px;line-height:1.55}
.forge-job-actions{display:flex;flex-wrap:wrap;justify-content:flex-start;gap:7px}
.forge-job-actions .forge-button{white-space:nowrap}
.forge-empty{
  padding:28px 16px;border:1px dashed var(--forge-line);color:var(--forge-faint);
  text-align:center;font-size:13px;
}
.forge-source-list{border:1px solid var(--forge-line);border-radius:var(--forge-radius)}
.forge-source{
  width:100%;display:grid;grid-template-columns:20px minmax(0,1fr) auto;gap:10px;align-items:center;
  min-height:62px;padding:0 13px;border:0;border-bottom:1px solid var(--forge-line-soft);
  background:rgba(29,30,28,.92);color:var(--forge-muted);text-align:left;cursor:pointer;
}
.forge-source:last-child{border-bottom:0}
.forge-source:hover,.forge-source.is-selected{background:var(--forge-raised);color:var(--forge-text)}
.forge-source__icon{width:10px;height:10px;border:1px solid var(--forge-faint)}
.forge-source.is-selected .forge-source__icon{border:3px solid var(--forge-signal)}
.forge-source strong{
  display:block;overflow:hidden;font-size:13px;text-overflow:ellipsis;white-space:nowrap;
}
.forge-source small{display:block;margin-top:4px;color:var(--forge-faint);font-size:11px}
.forge-source em{font-style:normal;color:var(--forge-ready);font-size:11px}
.forge-drawer-backdrop,.forge-modal-backdrop{
  position:fixed;inset:0;z-index:30;display:none;background:rgba(6,8,6,.78);
}
.forge-drawer-backdrop.is-open{display:block}
.forge-modal-backdrop{align-items:center;justify-content:center}
.forge-modal-backdrop.is-open{display:flex}
.forge-drawer{
  position:absolute;top:0;right:0;width:min(680px,100%);height:100%;overflow:auto;
  border-left:1px solid var(--forge-line);background:#20211f;box-shadow:-20px 0 70px rgba(0,0,0,.35);
}
.forge-drawer__head{
  position:sticky;top:0;z-index:1;display:flex;align-items:flex-start;justify-content:space-between;
  gap:14px;padding:18px 20px;border-bottom:1px solid var(--forge-line);background:#20211f;
}
.forge-drawer__head h2{margin:0;font-size:18px}
.forge-drawer__head p{margin:6px 0 0;color:var(--forge-faint);font-size:12px;word-break:break-all}
.forge-close{border:0;background:transparent;color:var(--forge-muted);font-size:23px;line-height:1;cursor:pointer}
.forge-drawer__body{padding:20px}
.forge-drawer__actions{
  display:flex;flex-wrap:wrap;gap:7px;margin:0 0 12px;padding:0 0 12px;border-bottom:1px solid var(--forge-line);
}
.forge-detail-bar{
  display:flex;flex-wrap:wrap;align-items:stretch;margin-bottom:16px;
  border:1px solid var(--forge-line);background:#191a18;
}
.forge-detail-item{
  display:flex;align-items:center;gap:8px;min-height:42px;padding:8px 12px;
  border-right:1px solid var(--forge-line);color:var(--forge-muted);font-size:12px;
}
.forge-detail-item:last-child{border-right:0}
.forge-detail-item span{color:var(--forge-faint)}
.forge-detail-item strong{color:var(--forge-text);font-size:14px}
.forge-detail-item .forge-chip{min-height:24px}
.forge-detail-item--gaps strong{color:var(--forge-alert)}
.forge-label{margin:0 0 8px;color:var(--forge-faint);font-size:12px}
.forge-unit-list{border-top:1px solid var(--forge-line)}
.forge-unit{padding:13px 0;border-bottom:1px solid var(--forge-line-soft)}
.forge-unit__top{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}
.forge-unit__top strong{font-size:14px}
.forge-unit__top small{display:block;margin-top:5px;color:var(--forge-faint);font-size:11px;word-break:break-all}
.forge-candidates{display:flex;flex-wrap:wrap;gap:6px;margin-top:12px}
.forge-candidate{
  min-height:30px;padding:0 9px;border:1px solid var(--forge-line);border-radius:var(--forge-radius);
  background:var(--forge-raised);color:var(--forge-muted);font-size:11px;cursor:pointer;text-align:left;
}
.forge-candidate:hover{border-color:var(--forge-signal);color:var(--forge-text)}
.forge-candidate b{display:block;font-size:11px}
.forge-candidate span{display:block;margin-top:2px;color:var(--forge-faint);font-size:10px}
.forge-modal{
  width:min(560px,calc(100% - 30px));max-height:min(720px,calc(100vh - 30px));overflow:auto;
  border:1px solid var(--forge-line);border-radius:6px;background:#20211f;box-shadow:0 24px 80px rgba(0,0,0,.45);
}
.forge-modal__head,.forge-modal__foot{
  display:flex;align-items:center;justify-content:space-between;gap:15px;padding:15px 18px;border-bottom:1px solid var(--forge-line);
}
.forge-modal__head strong{font-size:14px}
.forge-modal__body{padding:18px}
.forge-modal__foot{justify-content:flex-end;border-top:1px solid var(--forge-line);border-bottom:0}
.forge-confirm-modal__message{margin:0;color:var(--forge-text);font-size:14px;line-height:1.65}
.forge-confirm-modal__hint{margin:10px 0 0;color:var(--forge-muted);font-size:12px;line-height:1.55}
.forge-choice-label{margin:18px 0 8px;color:var(--forge-faint);font-size:12px}
.forge-segment{display:grid;grid-template-columns:repeat(3,1fr);border:1px solid var(--forge-line)}
.forge-segment button{
  height:34px;border:0;border-right:1px solid var(--forge-line);background:var(--forge-void);
  color:var(--forge-muted);font-size:13px;cursor:pointer;
}
.forge-segment button:last-child{border-right:0}
.forge-segment button.is-selected{background:#3a2922;color:#f1b092}
.forge-toast{
  position:fixed;left:50%;bottom:25px;z-index:60;max-width:min(520px,calc(100% - 30px));
  padding:10px 14px;transform:translate(-50%,10px);opacity:0;pointer-events:none;
  border:1px solid var(--forge-line);background:#e7e1d7;color:#171914;font-size:12px;transition:.2s ease;
}
.forge-toast.is-visible{transform:translate(-50%,0);opacity:1}
.forge-toast.is-bad{border-color:#8d4938;background:#5b2c23;color:#f6d8ce}
@media(max-width:980px){
  .forge-summary{grid-template-columns:repeat(2,minmax(0,1fr))}
  .forge-table-head{display:none}
  .forge-job-row{grid-template-columns:minmax(0,1fr) minmax(155px,.7fr)}
  .forge-job-metrics{display:none}
  .forge-job-actions{grid-column:1 / -1}
}
@media(max-width:680px){
  .forge-topbar{align-items:flex-start;flex-direction:column}
  .forge-commands{width:100%;justify-content:space-between;flex-wrap:wrap}
  .forge-status-cluster{justify-content:flex-start;gap:7px}
  .forge-summary{grid-template-columns:repeat(2,minmax(0,1fr))}
  .forge-attention{grid-template-columns:1fr}
  .forge-job-row{display:block}
  .forge-job-stage{display:flex;align-items:center;gap:8px;margin-top:10px}
  .forge-job-actions{margin-top:12px}
  .forge-detail-item{width:50%;border-bottom:1px solid var(--forge-line)}
  .forge-detail-item:nth-last-child(-n+2){border-bottom:0}
  .forge-section__head{display:block}
  .forge-source-toolbar{grid-template-columns:minmax(0,1fr) auto;gap:10px}
  .forge-filters{margin-top:10px}
}
@media(prefers-reduced-motion:reduce){
  *{scroll-behavior:auto!important;transition:none!important}
}
</style>
</head>
<body class="forge-app" data-ui="function-first">
<header class="forge-topbar">
  <div class="forge-brand">
    <span class="forge-brand__mark" aria-hidden="true"></span>
    <div><span class="forge-brand__name">ScrapeFlow</span></div>
  </div>
  <div class="forge-commands">
    <div class="forge-status-cluster">
      <span class="forge-status" id="serviceStatus">正在读取状态</span>
    </div>
    <div class="forge-command-actions">
      <button type="button" class="forge-button forge-button--quiet" id="refreshButton">刷新</button>
    </div>
  </div>
</header>

<main class="forge-shell">
  <nav class="forge-workbar" role="tablist" aria-label="工作区">
    <div class="forge-view-tabs">
      <button type="button" class="forge-view-tab is-active" role="tab" aria-selected="true" aria-controls="sourceView" data-view="sources">待选区</button>
      <button type="button" class="forge-view-tab" role="tab" aria-selected="false" aria-controls="tasksView" data-view="tasks">任务</button>
    </div>
  </nav>

  <div id="tasksView" hidden>
  <section class="forge-summary" id="taskSummary" aria-label="任务状态" hidden>
    <article class="forge-summary-card forge-summary-card--run">
      <span id="activeSummaryLabel">处理中</span><strong id="activeCount">0</strong>
    </article>
    <article class="forge-summary-card forge-summary-card--alert">
      <span>需要处理</span><strong id="attentionCount">0</strong>
    </article>
    <article class="forge-summary-card forge-summary-card--ready">
      <span>已结束</span><strong id="completedCount">0</strong>
    </article>
  </section>

  <section class="forge-section" id="attentionSection" hidden>
    <div class="forge-section__head">
      <div><h2>需要处理</h2><p>需要确认的项目会列在这里。识别不确定，请确认正确身份：</p></div>
    </div>
    <div class="forge-attention" id="attentionList"></div>
  </section>

  <section class="forge-section">
    <div class="forge-section__head">
      <div><h2>任务列表</h2><p id="jobSummary">正在读取任务</p></div>
      <div class="forge-filters" role="tablist" aria-label="任务筛选">
        <button type="button" class="forge-filter is-active" data-filter="all">全部</button>
        <button type="button" class="forge-filter" data-filter="active">处理中</button>
        <button type="button" class="forge-filter" data-filter="attention">需要关注</button>
        <button type="button" class="forge-filter" data-filter="history">已完成</button>
      </div>
    </div>
    <div class="forge-table-head" aria-hidden="true">
      <span>任务与来源</span><span>状态</span><span>进度</span><span>操作</span>
    </div>
    <div class="forge-job-board" id="jobBoard"><div class="forge-empty">正在读取任务</div></div>
  </section>
  </div>

  <section class="forge-section" id="sourceView">
    <div class="forge-source-toolbar">
      <div><h2>待选区</h2><p id="sourceRefreshStamp">正在读取 AList</p></div>
      <button type="button" class="forge-button forge-button--signal" id="createButton">＋ 创建任务</button>
    </div>
    <div class="forge-source-list" id="sourceStrip"><div class="forge-empty">正在读取来源</div></div>
  </section>
</main>

<div class="forge-drawer-backdrop" id="drawerBackdrop" aria-hidden="true">
  <aside class="forge-drawer" role="dialog" aria-modal="true" aria-labelledby="drawerTitle">
    <div class="forge-drawer__head">
      <div><h2 id="drawerTitle">任务详情</h2><p id="drawerSource"></p></div>
      <button type="button" class="forge-close" id="drawerClose" aria-label="关闭详情">×</button>
    </div>
    <div class="forge-drawer__body" id="drawerBody"><div class="forge-empty">正在读取详情</div></div>
  </aside>
</div>

<div class="forge-modal-backdrop" id="createBackdrop" aria-hidden="true">
  <section class="forge-modal" role="dialog" aria-modal="true" aria-labelledby="createTitle">
    <div class="forge-modal__head">
      <strong id="createTitle">创建根任务</strong>
      <button type="button" class="forge-close" data-close-create aria-label="关闭创建窗口">×</button>
    </div>
    <div class="forge-modal__body">
      <p class="forge-label">这个来源整理到哪里？</p>
      <div class="forge-source-list" id="modalSources"><div class="forge-empty">正在读取来源</div></div>
      <p class="forge-choice-label">这个任务应整理到哪里？</p>
      <div class="forge-segment" id="shelfChoices">
        <button type="button" data-shelf="movie">电影</button>
        <button type="button" data-shelf="anime" class="is-selected">番剧</button>
        <button type="button" data-shelf="us_tv">美剧</button>
      </div>
    </div>
    <div class="forge-modal__foot">
      <button type="button" class="forge-button forge-button--quiet" data-close-create>取消</button>
      <button type="button" class="forge-button forge-button--signal" id="submitCreate">建立任务</button>
    </div>
  </section>
</div>

<div class="forge-modal-backdrop" id="confirmBackdrop" aria-hidden="true">
  <section class="forge-modal forge-confirm-modal" role="dialog" aria-modal="true" aria-labelledby="confirmTitle" aria-describedby="confirmMessage">
    <div class="forge-modal__head">
      <strong id="confirmTitle">请确认操作</strong>
      <button type="button" class="forge-close" data-close-confirm aria-label="关闭确认窗口">×</button>
    </div>
    <div class="forge-modal__body">
      <p class="forge-confirm-modal__message" id="confirmMessage"></p>
      <p class="forge-confirm-modal__hint" id="confirmHint">操作会继续遵守系统的暂停和安全规则。</p>
    </div>
    <div class="forge-modal__foot">
      <button type="button" class="forge-button forge-button--quiet" data-close-confirm>取消</button>
      <button type="button" class="forge-button forge-button--signal" id="confirmSubmit">确认继续</button>
    </div>
  </section>
</div>

<div class="forge-toast" id="toast" role="status" aria-live="polite"></div>

<script>
(function(){
  "use strict";
  var $ = function(selector){ return document.querySelector(selector); };
  var $$ = function(selector){ return Array.prototype.slice.call(document.querySelectorAll(selector)); };
  var esc = function(value){
    return String(value == null ? "" : value).replace(/[&<>"']/g,function(char){
      return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[char];
    });
  };
  var viewStorageKey = "scrapeflow.dashboard.active-view";
  function savedView(){
    try {
      var saved = window.sessionStorage.getItem(viewStorageKey);
      return saved === "tasks" || saved === "sources" ? saved : "sources";
    } catch(error) {
      return "sources";
    }
  }
  function saveView(view){
    try { window.sessionStorage.setItem(viewStorageKey,view); } catch(error) {}
  }
  var state = {
    jobs:[], sources:[], units:{}, replenishment:{}, health:null, intakeRefreshedAt:null,
    view:savedView(), filter:"all", busy:false, loading:false, healthError:false, drawerJobId:null, selectedSource:null,
    selectedShelf:"anime", pendingAction:null
  };
  var terminal = new Set(["completed","completed_with_gaps","executed","cancelled"]);
  var attention = new Set([
    "failed","failed_archive","failed_cleanup","failed_identity","failed_planning",
    "failed_provider","failed_verification","failed_write","reconciliation_uncertain",
    "target_policy_conflict","needs_attention","uncertain"
  ]);
  var phaseLabels = {
    queued:"已排队",planned:"已排队",reconciling:"正在对账",reconciled:"已对账",
    needs_attention:"需要关注",archive_preprocessing:"归档预处理",identity_matching:"识别内容",
    analyzing:"分析目录",planning:"生成计划",executing:"正在写入",executing_media:"正在写入",
    verifying:"正在核验",cleaning:"清理暂存",completed:"已完成",completed_with_gaps:"已完成（有缺口）",
    executed:"已完成",cancelled:"已取消",failed:"失败",failed_archive:"归档失败",
    failed_cleanup:"清理失败",failed_identity:"识别失败",failed_planning:"计划失败",
    failed_provider:"补源失败",failed_verification:"验收失败",failed_write:"写入失败",
    reconciliation_uncertain:"对账不确定",target_policy_conflict:"货架冲突",
    retry_wait:"等待重试",awaiting_target_shelf:"等待选择货架",gap_discovering:"核对缺口",
    provider_searching:"搜索补源",acquiring:"获取文件",staging_verifying:"核对暂存",
    subtitle_installing:"处理字幕",child_planning:"规划补源",child_executing:"整理补源",
    final_verifying:"最终核对",uncertain:"需要确认",confirmed:"已确认",complete:"已完成",
    pending:"等待处理",running:"处理中",waiting_reconcile:"等待安全对账",
    paused_waiting:"暂停等待"
  };
  var shelfLabels = {movie:"电影",anime:"番剧",us_tv:"美剧"};
  var tierLabels = {quark_share:"夸克分享",magnet:"本地磁力"};

  function number(value){ return typeof value === "number" && isFinite(value) ? value : 0; }
  function jobTitle(job){
    var raw = job && job.plan && job.plan.title || job && job.source || job && job.id || "未命名任务";
    var bits = String(raw).split("/").filter(Boolean);
    return bits[bits.length - 1] || String(raw);
  }
  function jobSource(job){ return job && job.source === "全库审计" ? job.id : (job && job.source || job && job.id || ""); }
  function phaseLabel(phase){ return phaseLabels[phase] || phase || "未知状态"; }
  function phaseTone(phase){
    if(terminal.has(phase)){ return "ready"; }
    if(attention.has(phase)){ return phase.indexOf("failed") === 0 ? "danger" : "alert"; }
    return "run";
  }
  function isWaiting(job){ return !!(job && Array.isArray(job.allowed_target_shelves) && job.allowed_target_shelves.length); }
  function rootTierState(job){
    var view = job && state.replenishment[job.id];
    return view && view.tier_state && typeof view.tier_state === "object" ? view.tier_state : {};
  }
  function rootReplenishmentWait(job){
    var waiting = rootTierState(job).waiting;
    return waiting === "waiting_reconcile" || waiting === "retry_wait" ? waiting : "";
  }
  function isAttentionJob(job){ return !!(job && attention.has(job.phase)); }
  function isTerminalJob(job){ return !!(job && terminal.has(job.phase) && !rootReplenishmentWait(job)); }
  function systemPaused(){
    return !!(state.health && state.health.control && state.health.control.paused === true);
  }
  function isRecoveryWait(job){
    return !!(job && (rootReplenishmentWait(job) || job.phase === "waiting_reconcile" || job.phase === "retry_wait"));
  }
  function isPausedWaiting(job){
    return systemPaused() && !isTerminalJob(job) && !isAttentionJob(job) && !isWaiting(job) && !isRecoveryWait(job);
  }
  function displayPhase(job){
    return rootReplenishmentWait(job) || (isPausedWaiting(job) ? "paused_waiting" : job.phase);
  }
  function unitData(jobId){ return state.units[jobId] || null; }
  function stats(job){
    var aggregate = unitData(job.id) && unitData(job.id).aggregate || {};
    return {
      total:number(aggregate.unit_count),
      completed:number(aggregate.completed),
      attention:number(aggregate.attention),
      failed:number(aggregate.failed),
      gaps:number(aggregate.open_gaps)
    };
  }
  function percent(value,total){
    if(!total){ return 0; }
    return Math.max(0,Math.min(100,Math.round(value * 100 / total)));
  }
  function shortTime(value){
    if(!value){ return "--:--"; }
    var date = new Date(value);
    if(isNaN(date.getTime())){ return String(value).slice(11,16) || "--:--"; }
    return date.toLocaleTimeString("zh-CN",{hour:"2-digit",minute:"2-digit",hour12:false});
  }
  function tierFor(job){
    var rootView = job && state.replenishment[job.id];
    var rootTier = rootTierState(job).tier;
    if(rootTier && rootView && rootView.aggregate && number(rootView.aggregate.open_gaps) > 0){ return rootTier; }
    var replenishment = job && job.summary && job.summary.replenishment;
    return replenishment && replenishment.tier || "";
  }
  function replenishmentFor(job){
    return job && ((job.plan && job.plan.replenishment) || (job.summary && job.summary.replenishment)) || {};
  }
  function requiresRootJobSubtitleMigration(job){
    var replenishment = replenishmentFor(job);
    return !!(replenishment && replenishment.migration_required === true);
  }
  function api(path,options){
    options = options || {};
    var headers = Object.assign({"Content-Type":"application/json"},options.headers || {});
    return fetch(path,Object.assign({},options,{headers:headers})).then(function(response){
      return response.json().catch(function(){ return {}; }).then(function(data){
        if(!response.ok){ throw new Error(data.error || ("请求失败（" + response.status + "）")); }
        return data;
      });
    });
  }
  function toast(message,bad){
    var node = $("#toast");
    node.textContent = message;
    node.className = "forge-toast is-visible" + (bad ? " is-bad" : "");
    clearTimeout(node._timer);
    node._timer = setTimeout(function(){ node.className = "forge-toast"; },2800);
  }
  function setBusy(value){
    state.busy = value;
    $$("button").forEach(function(button){ button.disabled = value; });
  }
  function setRefreshState(refreshing){
    var button = $("#refreshButton");
    button.textContent = refreshing ? "刷新中…" : "刷新";
    button.setAttribute("aria-busy", refreshing ? "true" : "false");
  }
  function jobById(jobId){
    return state.jobs.find(function(job){ return job.id === jobId; }) || null;
  }
  function filteredJobs(){
    return state.jobs.filter(function(job){
      if(state.filter === "active"){ return !isTerminalJob(job) && !isAttentionJob(job) && !isWaiting(job); }
      if(state.filter === "attention"){ return isAttentionJob(job); }
      if(state.filter === "history"){ return isTerminalJob(job); }
      return true;
    });
  }
  function updateHeader(){
    var service = $("#serviceStatus");
    if(!state.health){
      service.textContent = state.healthError ? "API 无响应" : "正在读取状态";
      service.className = "forge-status" + (state.healthError ? " is-bad" : "");
      return;
    }
    var alive = !!(state.health.liveness && state.health.liveness.alive === true);
    if(!alive){
      service.textContent = "API 无响应";
      service.className = "forge-status is-bad";
    } else if(systemPaused()){
      service.textContent = "系统已暂停";
      service.className = "forge-status is-paused";
    } else {
      service.textContent = "API 可达";
      service.className = "forge-status is-ready";
    }
  }
  function renderCore(){
    var waiting = state.jobs.filter(isWaiting).length;
    var paused = systemPaused() ? state.jobs.filter(isPausedWaiting).length : 0;
    var recovering = state.jobs.filter(isRecoveryWait).length;
    var active = state.jobs.filter(function(job){
      return !isTerminalJob(job) && !isAttentionJob(job) && !isWaiting(job) && !isPausedWaiting(job);
    }).length;
    var issues = attentionItems().length + waiting;
    var completed = state.jobs.filter(isTerminalJob).length;
    var total = state.jobs.length;
    $("#taskSummary").hidden = total === 0;
    $("#activeSummaryLabel").textContent = recovering ? "安全对账 / 重试" : (paused ? "暂停等待" : "处理中");
    $("#activeCount").textContent = paused || active;
    $("#attentionCount").textContent = issues;
    $("#completedCount").textContent = completed;
    $("#sourceRefreshStamp").textContent = state.intakeRefreshedAt
      ? "AList 刷新于 " + shortTime(state.intakeRefreshedAt)
      : "尚未刷新 AList";
  }
  function attentionItems(){
    var items = [];
    state.jobs.forEach(function(job){
      if(isAttentionJob(job)){
        items.push({job:job,title:jobTitle(job),message:job.error || phaseLabel(job.phase)});
      }
      var view = unitData(job.id);
      (view && view.units || []).forEach(function(unit){
        if(unit.identity_status === "uncertain" || unit.reconciliation_outcome === "uncertain"){
          items.push({job:job,unit:unit,title:unit.display_label || unit.boundary_key || "未命名单元",message:"身份需要确认"});
        }
      });
    });
    return items;
  }
  function renderAttention(){
    var items = attentionItems().slice(0,6);
    var section = $("#attentionSection");
    section.hidden = !items.length;
    if(!items.length){ return; }
    $("#attentionList").innerHTML = items.map(function(item){
      var jobId = esc(item.job.id);
      return '<article class="forge-alert-card"><strong>' + esc(item.title) + '</strong><p>' +
        esc(item.message) + '</p><button type="button" class="forge-button forge-button--small forge-button--quiet" data-action="open" data-job="' +
        jobId + '">查看处理</button></article>';
    }).join("");
  }
  function chip(phase){
    return '<span class="forge-chip forge-chip--' + phaseTone(phase) + '">' + esc(phaseLabel(phase)) + "</span>";
  }
  function jobRow(job,index){
    var detail = stats(job);
    var progress = percent(detail.completed,detail.total);
    var waiting = isWaiting(job);
    var paused = isPausedWaiting(job);
    var shownPhase = displayPhase(job);
    var tier = tierFor(job);
    var action = '<button type="button" class="forge-button forge-button--small forge-button--quiet" data-action="open" data-job="' + esc(job.id) + '">查看详情</button>';
    if(waiting){
      action += (job.allowed_target_shelves || []).map(function(shelf){
        return '<button type="button" class="forge-button forge-button--small forge-button--signal" data-action="start" data-job="' +
          esc(job.id) + '" data-shelf="' + esc(shelf) + '">' + esc(shelfLabels[shelf] || shelf) + "</button>";
      }).join("");
    }
    if(detail.gaps > 0 && !waiting && !requiresRootJobSubtitleMigration(job)){
      action += '<button type="button" class="forge-button forge-button--small forge-button--danger" data-action="replenish" data-job="' +
        esc(job.id) + '">手动补源</button>';
    }
    if(isTerminalJob(job)){
      action += '<button type="button" class="forge-button forge-button--small forge-button--quiet" data-action="cleanup" data-job="' +
        esc(job.id) + '">清理记录</button>';
    }
    var progressClass = progress === 100 ? " is-complete" : "";
    var secondary = detail.total ? (detail.completed + " / " + detail.total + " 个作品") : (waiting ? "等待选择货架" : "等待单元数据");
    if(requiresRootJobSubtitleMigration(job)){
      secondary = "旧字幕任务需迁移到 RootJob，不能在此手动补源";
    } else if(paused){
      secondary = "系统已暂停，等待恢复运行";
    } else if(systemPaused() && shownPhase === "waiting_reconcile"){
      secondary = "系统已暂停；保留任务，恢复后安全对账";
    } else if(systemPaused() && shownPhase === "retry_wait"){
      secondary = "系统已暂停；恢复后继续等待重试";
    }
    if(tier){ secondary += " · " + (tierLabels[tier] || tier); }
    return '<article class="forge-job-row" data-open-job="' + esc(job.id) + '">' +
      '<div class="forge-job-index">' + String(index + 1).padStart(2,"0") + "</div>" +
      '<div class="forge-job-main"><strong title="' + esc(jobSource(job)) + '">' + esc(jobTitle(job)) +
      '</strong><small title="' + esc(jobSource(job)) + '">' + esc(jobSource(job)) + "</small>" +
      '<div class="forge-progress"><i class="' + progressClass + '" style="width:' + progress + '%"></i></div></div>' +
      '<div class="forge-job-stage">' + chip(shownPhase) + '<small>' + esc(secondary) + "</small></div>" +
      '<div class="forge-job-metrics">' + (detail.total ? ("完成 " + detail.completed + " / " + detail.total) : "尚未生成单元") +
      "<br>" + (detail.gaps ? (detail.gaps + " 个开放缺口") : "无开放缺口") +
      (detail.attention ? "<br>" + detail.attention + " 个待确认" : "") + "</div>" +
      '<div class="forge-job-actions">' + action + "</div></article>";
  }
  function renderJobs(){
    var rows = filteredJobs();
    $("#jobSummary").textContent = state.jobs.length
      ? (rows.length + " 个任务 · 共 " + state.jobs.length + " 个根任务")
      : "当前没有任务";
    $$(".forge-filter").forEach(function(button){ button.classList.toggle("is-active",button.dataset.filter === state.filter); });
    $("#jobBoard").innerHTML = rows.length ? rows.map(jobRow).join("") : '<div class="forge-empty">这个筛选里暂时没有任务</div>';
  }
  function sourceButton(source){
    var selected = state.selectedSource === source.canonical_path;
    var stateText = "待创建";
    var count = (source.child_count == null ? "—" : source.child_count) + " 个子目录 · " +
      (source.file_count == null ? "—" : source.file_count) + " 个文件";
    return '<button type="button" class="forge-source' + (selected ? " is-selected" : "") +
      '" data-source="' + esc(source.canonical_path) + '"><span class="forge-source__icon"></span><span>' +
      '<strong title="' + esc(source.canonical_path) + '">' + esc(source.display_name || source.canonical_path) +
      '</strong><small>' + esc(count) + '</small></span><em>' + esc(stateText) + "</em></button>";
  }
  function waitingSources(){
    return state.sources.filter(function(source){
      return source.present !== false && !source.root_task_id;
    });
  }
  function renderSources(){
    var sources = waitingSources();
    var html = sources.length ? sources.map(sourceButton).join("") : '<div class="forge-empty">待选区目前没有来源</div>';
    $("#sourceStrip").innerHTML = html;
    $("#modalSources").innerHTML = html;
  }
  function renderView(){
    var showingTasks = state.view === "tasks";
    $("#tasksView").hidden = !showingTasks;
    $("#sourceView").hidden = showingTasks;
    $$(".forge-view-tab").forEach(function(button){
      var active = button.dataset.view === state.view;
      button.classList.toggle("is-active",active);
      button.setAttribute("aria-selected",active ? "true" : "false");
    });
    $("#createButton").disabled = state.busy;
  }
  function renderAll(){
    updateHeader();
    renderCore();
    renderAttention();
    renderJobs();
    renderSources();
    renderView();
    if(state.drawerJobId){ renderDrawer(); }
  }
  function loadReplenishmentView(jobId){
    return api("/api/jobs/" + encodeURIComponent(jobId) + "/replenishment").then(function(view){
      state.replenishment[jobId] = view;
      return view;
    }).catch(function(){
      // A legacy job has no RootJob ledger.  This is a read-only adornment,
      // so an unavailable view must never make the ordinary task list look
      // failed or block its refresh.
      state.replenishment[jobId] = null;
      return null;
    });
  }
  function loadReplenishmentViews(jobs){
    var liveIds = {};
    (jobs || []).forEach(function(job){ liveIds[job.id] = true; });
    Object.keys(state.replenishment).forEach(function(jobId){
      if(!liveIds[jobId]){ delete state.replenishment[jobId]; }
    });
    return Promise.all((jobs || []).map(function(job){
      return loadReplenishmentView(job.id);
    }));
  }
  function load(options){
    options = options || {};
    if(state.loading){ return Promise.resolve(false); }
    state.loading = true;
    var intakeRequest = options.refreshIntake
      ? api("/api/intake/refresh",{method:"POST",body:"{}"})
      : api("/api/intake");
    return Promise.all([api("/api/health"),api("/api/jobs"),intakeRequest]).then(function(result){
      state.health = result[0];
      state.healthError = false;
      var intake = state.health && state.health.intake || {};
      state.intakeRefreshedAt = (options.refreshIntake && result[2].refreshed_at)
        || intake.last_scan_at || state.intakeRefreshedAt;
      state.jobs = result[1].jobs || [];
      state.sources = result[2].sources || [];
      if(!waitingSources().some(function(source){ return source.canonical_path === state.selectedSource; })){
        state.selectedSource = null;
      }
      Object.keys(state.units).forEach(function(jobId){
        if(!state.jobs.some(function(job){ return job.id === jobId; })){ delete state.units[jobId]; }
      });
      return Promise.all(state.jobs.filter(function(job){
        return !isTerminalJob(job) || !state.units[job.id];
      }).map(function(job){
        return api("/api/jobs/" + encodeURIComponent(job.id) + "/work-units").then(function(view){
          state.units[job.id] = view;
        }).catch(function(){ state.units[job.id] = null; });
      })).then(function(){ return loadReplenishmentViews(state.jobs); });
    }).then(function(){
      renderAll();
      return true;
    }).catch(function(error){
      if(!state.health){ state.healthError = true; }
      updateHeader();
      toast((options.refreshIntake ? "AList 刷新失败：" : "") + error.message,true);
      renderAll();
      return false;
    }).finally(function(){ state.loading = false; });
  }
  function send(path,payload){
    if(state.busy){ return Promise.resolve(); }
    setBusy(true);
    return api(path,{method:"POST",body:JSON.stringify(payload || {})}).then(function(){
      return load();
    }).then(function(){
      toast("操作已保存");
      if(state.drawerJobId){ return loadDrawerData(state.drawerJobId); }
      return null;
    }).catch(function(error){
      toast(error.message,true);
    }).finally(function(){ setBusy(false); });
  }
  function openDrawer(jobId){
    state.drawerJobId = jobId;
    $("#drawerBackdrop").classList.add("is-open");
    $("#drawerBackdrop").setAttribute("aria-hidden","false");
    $("#drawerBody").innerHTML = '<div class="forge-empty">正在读取详情</div>';
    loadDrawerData(jobId);
  }
  function closeDrawer(){
    state.drawerJobId = null;
    $("#drawerBackdrop").classList.remove("is-open");
    $("#drawerBackdrop").setAttribute("aria-hidden","true");
  }
  function loadDrawerData(jobId){
    var requests = [
      api("/api/jobs/" + encodeURIComponent(jobId) + "/work-units").then(function(view){ state.units[jobId] = view; }),
      loadReplenishmentView(jobId)
    ];
    return Promise.all(requests).then(function(){ renderDrawer(); }).catch(function(error){
      $("#drawerBody").innerHTML = '<div class="forge-empty">' + esc(error.message) + "</div>";
    });
  }
  function unitRow(unit){
    var status = unit.identity_status || unit.reconciliation_outcome || "未完成";
    var candidates = unit.candidate_identities || [];
    var candidateHtml = "";
    if(status === "uncertain" && candidates.length){
      candidateHtml = '<div class="forge-candidates">' + candidates.map(function(candidate){
        var season = candidate.season == null ? "" : String(candidate.season);
        return '<button type="button" class="forge-candidate" data-action="confirm" data-confirm="1" data-job="' +
          esc(state.drawerJobId) + '" data-unit="' + esc(unit.work_unit_id) + '" data-tmdb="' +
          esc(candidate.tmdb_id) + '" data-type="' + esc(candidate.media_type) + '" data-season="' + esc(season) + '">' +
          '<b>' + esc(candidate.title || (candidate.media_type || "候选")) + '</b><span>' +
          esc((candidate.media_type === "movie" ? "电影" : "剧集") + " · " + candidate.tmdb_id +
            (candidate.year ? " · " + candidate.year : "")) + "</span></button>";
      }).join("") + "</div>";
    }
    return '<div class="forge-unit"><div class="forge-unit__top"><div><strong>' +
      esc(unit.display_label || unit.boundary_key || "未命名单元") + '</strong><small>' +
      esc(unit.boundary_key || unit.work_unit_id || "") + "</small></div>" + chip(status) +
      "</div>" + candidateHtml + "</div>";
  }
  function renderDrawer(){
    if(!state.drawerJobId){ return; }
    var job = jobById(state.drawerJobId);
    if(!job){ closeDrawer(); return; }
    var detail = stats(job);
    var view = unitData(job.id) || {};
    var replenishment = state.replenishment[job.id] || {};
    var tierState = replenishment.tier_state || {};
    var tier = tierState.tier || tierFor(job);
    $("#drawerTitle").textContent = jobTitle(job);
    $("#drawerSource").textContent = jobSource(job);
    var actions = "";
    if(detail.gaps > 0 && !isWaiting(job) && !requiresRootJobSubtitleMigration(job)){
      actions += '<button type="button" class="forge-button forge-button--danger" data-action="replenish" data-job="' + esc(job.id) + '">补源（' + detail.gaps + ' 个缺口）</button>';
    } else if(requiresRootJobSubtitleMigration(job)){
      actions += '<p class="forge-label">旧字幕任务需迁移到 RootJob；此处不会重新下载或写入。</p>';
    }
    if(isTerminalJob(job)){
      actions += '<button type="button" class="forge-button forge-button--quiet" data-action="cleanup" data-job="' + esc(job.id) + '">清理任务记录</button>';
    }
    var units = view.units || [];
    var unitHtml = units.length ? units.map(unitRow).join("") : '<div class="forge-empty">暂时没有可展示的作品单元</div>';
    var unitText = detail.total ? (detail.completed + " / " + detail.total + " 已完成") : "等待单元数据";
    var gapText = detail.gaps ? (detail.gaps + " 个开放缺口") : "没有开放缺口";
    var detailBar =
      '<div class="forge-detail-bar" aria-label="任务概况">' +
      '<div class="forge-detail-item"><span>状态</span>' + chip(displayPhase(job)) + "</div>" +
      '<div class="forge-detail-item"><span>作品单元</span><strong>' + esc(unitText) + "</strong></div>" +
      '<div class="forge-detail-item forge-detail-item--gaps"><span>缺口</span><strong>' + esc(gapText) + "</strong></div>" +
      (tier ? '<div class="forge-detail-item"><span>当前补源方式</span><strong>' + esc(tierLabels[tier] || tier) + "</strong></div>" : "") +
      "</div>";
    $("#drawerBody").innerHTML =
      (actions ? '<div class="forge-drawer__actions">' + actions + "</div>" : "") +
      detailBar +
      '<p class="forge-label">作品单元（' + units.length + '）</p><div class="forge-unit-list">' + unitHtml + "</div>";
  }
  function openCreate(){
    state.selectedSource = null;
    $("#createBackdrop").classList.add("is-open");
    $("#createBackdrop").setAttribute("aria-hidden","false");
    renderSources();
  }
  function closeCreate(){
    $("#createBackdrop").classList.remove("is-open");
    $("#createBackdrop").setAttribute("aria-hidden","true");
  }
  function createRoot(){
    if(!state.selectedSource){ toast("请先选择一个来源",true); return; }
    if(state.busy){ return; }
    setBusy(true);
    api("/api/root-jobs",{method:"POST",body:JSON.stringify({
      path:state.selectedSource,target_shelf:state.selectedShelf
    })}).then(function(){
      closeCreate();
      return load();
    }).then(function(){ toast("根任务已建立"); }).catch(function(error){
      toast(error.message,true);
    }).finally(function(){ setBusy(false); });
  }
  function confirmCandidate(button){
    var payload = {
      media_type:button.dataset.type,
      tmdb_id:Number(button.dataset.tmdb)
    };
    if(button.dataset.season){ payload.season = Number(button.dataset.season); }
    send("/api/jobs/" + encodeURIComponent(button.dataset.job) + "/work-units/" +
      encodeURIComponent(button.dataset.unit) + "/confirm",payload);
  }
  function askForConfirmation(action){
    state.pendingAction = action;
    $("#confirmTitle").textContent = action.title;
    $("#confirmMessage").textContent = action.message;
    $("#confirmHint").textContent = action.hint || "操作会继续遵守系统的暂停和安全规则。";
    var submit = $("#confirmSubmit");
    submit.textContent = action.confirmLabel || "确认继续";
    submit.className = "forge-button " + (action.danger ? "forge-button--danger" : "forge-button--signal");
    $("#confirmBackdrop").classList.add("is-open");
    $("#confirmBackdrop").setAttribute("aria-hidden","false");
    submit.focus();
  }
  function closeConfirmation(){
    state.pendingAction = null;
    $("#confirmBackdrop").classList.remove("is-open");
    $("#confirmBackdrop").setAttribute("aria-hidden","true");
  }
  function submitConfirmation(){
    var action = state.pendingAction;
    if(!action){ return; }
    closeConfirmation();
    send(action.path,action.payload || {});
  }
  function cleanupJob(jobId){
    askForConfirmation({
      title:"清理任务记录",
      message:"只清理这条任务记录，不删除媒体文件。",
      hint:"这项操作不会删除正式库、来源或下载文件。",
      confirmLabel:"清理记录",
      danger:true,
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/cleanup"
    });
  }
  function replenishJob(jobId){
    askForConfirmation({
      title:"手动补源",
      message:"将按当前的补源规则手动触发一次。",
      hint:"系统仍会先检查暂停状态、门禁和每一阶的安全限制。",
      confirmLabel:"开始补源",
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/replenish"
    });
  }
  $("#refreshButton").addEventListener("click",function(){
    if(state.busy || state.loading){ return; }
    setRefreshState(true);
    setBusy(true);
    load({refreshIntake:true}).then(function(refreshed){
      if(refreshed){
        toast(state.intakeRefreshedAt
          ? "AList 已刷新 · " + shortTime(state.intakeRefreshedAt)
          : "AList 已刷新");
      }
    }).finally(function(){
      setRefreshState(false);
      setBusy(false);
    });
  });
  $("#createButton").addEventListener("click",openCreate);
  $("#drawerClose").addEventListener("click",closeDrawer);
  $("#drawerBackdrop").addEventListener("click",function(event){
    if(event.target === $("#drawerBackdrop")){ closeDrawer(); }
  });
  $$("[data-close-create]").forEach(function(button){ button.addEventListener("click",closeCreate); });
  $("#createBackdrop").addEventListener("click",function(event){
    if(event.target === $("#createBackdrop")){ closeCreate(); }
  });
  $$("[data-close-confirm]").forEach(function(button){ button.addEventListener("click",closeConfirmation); });
  $("#confirmBackdrop").addEventListener("click",function(event){
    if(event.target === $("#confirmBackdrop")){ closeConfirmation(); }
  });
  $("#confirmSubmit").addEventListener("click",submitConfirmation);
  $$(".forge-filter").forEach(function(button){
    button.addEventListener("click",function(){ state.filter = button.dataset.filter; renderJobs(); });
  });
  $$(".forge-view-tab").forEach(function(button){
    button.addEventListener("click",function(){
      state.view = button.dataset.view;
      saveView(state.view);
      state.selectedSource = null;
      renderAll();
    });
  });
  function sourceClick(event){
    var button = event.target.closest("[data-source]");
    if(!button){ return; }
    state.selectedSource = button.dataset.source;
    renderSources();
  }
  $("#sourceStrip").addEventListener("click",sourceClick);
  $("#modalSources").addEventListener("click",sourceClick);
  $$("#shelfChoices button").forEach(function(button){
    button.addEventListener("click",function(){
      state.selectedShelf = button.dataset.shelf;
      $$("#shelfChoices button").forEach(function(item){ item.classList.toggle("is-selected",item === button); });
    });
  });
  $("#submitCreate").addEventListener("click",createRoot);
  function actionClick(event){
    var action = event.target.closest("[data-action]");
    if(!action){ return; }
    event.preventDefault();
    event.stopPropagation();
    var name = action.dataset.action;
    if(name === "open"){ openDrawer(action.dataset.job); }
    if(name === "start"){
      var b = action;
      send(`/api/jobs/${encodeURIComponent(b.dataset.job)}/start`,{target_shelf:b.dataset.shelf});
    }
    if(name === "cleanup"){ cleanupJob(action.dataset.job); }
    if(name === "replenish"){ replenishJob(action.dataset.job); }
    if(name === "confirm"){ confirmCandidate(action); }
  }
  $("#jobBoard").addEventListener("click",function(event){
    actionClick(event);
    if(event.defaultPrevented){ return; }
    var row = event.target.closest("[data-open-job]");
    if(row){ openDrawer(row.dataset.openJob); }
  });
  $("#attentionList").addEventListener("click",actionClick);
  $("#drawerBody").addEventListener("click",actionClick);
  document.addEventListener("keydown",function(event){
    if(event.key === "Escape"){
      closeConfirmation();
      closeCreate();
      closeDrawer();
    }
  });
  renderView();
  load();
  setInterval(function(){
    if(document.visibilityState === "visible" && !state.busy){ load(); }
  },10000);
})();
</script>
</body>
</html>'''


def dashboard_html() -> bytes:
    return DASHBOARD_HTML.encode("utf-8")
