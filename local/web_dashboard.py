"""Dependency-free same-origin industrial console for the local ScrapeFlow API.

The page is intentionally rebuilt from scratch.  It keeps the existing
same-origin API calls and operator safety boundaries, but does not reuse the
legacy dashboard's visual structure or selectors.
"""


DASHBOARD_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="dark">
<title>ScrapeFlow · 工业控制台</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='3' fill='%231d1e1c'/%3E%3Cpath d='M4 4h24v24H4z' fill='none' stroke='%23df6d43' stroke-width='4'/%3E%3Cpath d='M4 4h7v24H4z' fill='%23df6d43'/%3E%3C/svg%3E">
<style>
:root{
  --forge-void:#151615;
  --forge-panel:#1c1d1b;
  --forge-raised:#252623;
  --forge-line:#383935;
  --forge-line-soft:#2a2b27;
  --forge-text:#f3f2ed;
  --forge-muted:#b7b5ac;
  --forge-faint:#828178;
  --forge-signal:#dc7048;
  --forge-alert:#d5a94b;
  --forge-ready:#9bbd79;
  --forge-danger:#db8064;
  --forge-radius:4px;
  --forge-font-mono:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
}
*{box-sizing:border-box}
html,body{min-height:100%;margin:0;background:var(--forge-void);color:var(--forge-text)}
body{
  font-family:"SF Pro Text","PingFang SC","Microsoft YaHei",sans-serif;
  font-size:14px;line-height:1.45;
}
button,input,select{font:inherit}
button{color:inherit}
button:disabled{opacity:.45;cursor:wait}
button:focus-visible,[role="button"]:focus-visible,input:focus-visible,select:focus-visible{
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
  background:rgba(28,29,27,.98);
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
.forge-heartbeat{
  width:8px;height:8px;border-radius:50%;background:var(--forge-ready);
  box-shadow:0 0 6px rgba(155,189,121,.4);display:inline-block;flex:none;
  transition:transform .15s ease,background-color .15s ease;
}
.forge-heartbeat.is-active{
  background:var(--forge-signal);
  box-shadow:0 0 8px var(--forge-signal);
  animation:forge-beat .6s infinite alternate ease-in-out;
}
@keyframes forge-beat{
  0%{transform:scale(.75);opacity:.6}
  100%{transform:scale(1.25);opacity:1}
}
.forge-status{
  display:inline-flex;align-items:center;gap:8px;color:var(--forge-muted);
  font-size:12px;white-space:nowrap;font-family:var(--forge-font-mono);
  font-variant-numeric:tabular-nums;
}
.forge-status::before{
  width:8px;height:8px;border-radius:50%;background:var(--forge-alert);
  box-shadow:0 0 0 4px rgba(213,169,75,.1);content:"";
}
.forge-status.is-ready::before{
  background:var(--forge-ready);box-shadow:0 0 0 4px rgba(155,189,121,.15);
  animation:forge-breathe 2.4s infinite ease-in-out;
}
.forge-status.is-bad::before{
  background:var(--forge-danger);box-shadow:0 0 0 4px rgba(219,128,100,.15);
}
.forge-status.is-paused::before{
  background:var(--forge-alert);box-shadow:0 0 0 4px rgba(213,169,75,.12);
}
@keyframes forge-breathe{
  0%,100%{opacity:1}
  50%{opacity:.5}
}
.forge-active-root-badge{
  display:inline-flex;align-items:center;gap:6px;padding:3px 9px;
  border:1px solid var(--forge-line);border-radius:var(--forge-radius);
  background:rgba(21,22,20,.85);font-size:11px;color:var(--forge-muted);font-family:var(--forge-font-mono);
}
.forge-active-root-badge em{font-style:normal;color:var(--forge-signal);font-weight:600}
.forge-button{
  min-height:36px;padding:0 14px;border:1px solid var(--forge-line);border-radius:var(--forge-radius);
  background:#292a27;color:var(--forge-text);font-size:13px;font-weight:600;cursor:pointer;
  transition:border-color .15s ease,background-color .15s ease,opacity .15s ease;
}
.forge-button:hover{border-color:#656660;background:#32332f}
.forge-button--signal{
  border-color:var(--forge-signal);background:var(--forge-signal);color:#19120e;
}
.forge-button--signal:hover{border-color:#ed8659;background:#ed8659}
.forge-button--replenish{
  border-color:#7d5c2d;background:#322718;color:#f1c875;
}
.forge-button--replenish:hover{border-color:#a67c3b;background:#443621}
.forge-button--danger{border-color:#874a38;background:#35201a;color:#f2c0ae}
.forge-button--danger:hover{border-color:#a25944;background:#442820}
.forge-button--quiet{background:transparent}
.forge-button--small{min-height:30px;padding:0 10px;font-size:12px}
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
.forge-summary-card strong{
  display:block;margin-top:8px;font-size:23px;line-height:1;font-family:var(--forge-font-mono);
  font-variant-numeric:tabular-nums;
}
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
  display:inline-flex;align-items:center;
}
.forge-filter:hover{color:var(--forge-text);background:#252622}
.forge-filter.is-active{border-color:#6c6d65;background:#30312e;color:var(--forge-text)}
.forge-filter-count{
  display:inline-block;padding:0 5px;margin-left:5px;border-radius:999px;
  background:rgba(255,255,255,.07);font-size:10px;font-family:var(--forge-font-mono);line-height:1.5;
  font-variant-numeric:tabular-nums;
}
.forge-filter.is-active .forge-filter-count{
  background:var(--forge-signal);color:#19120e;font-weight:700;
}
.forge-attention{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px}
.forge-alert-card{
  min-height:0;padding:13px;border:1px solid #68532f;background:#29271e;
}
.forge-alert-card strong{display:block;font-size:13px}
.forge-alert-card p{margin:6px 0 12px;color:#d2bd8f;font-size:12px}
.forge-table-head,.forge-job-row{
  display:grid;
  grid-template-columns:minmax(260px,1.65fr) minmax(140px,.75fr) minmax(120px,.5fr) minmax(240px,.9fr);
  gap:16px;align-items:center;
}
.forge-table-head{
  min-height:38px;padding:0 12px;border:1px solid var(--forge-line);border-bottom:0;
  background:#232421;color:var(--forge-faint);font-size:11px;
}
.forge-job-board{border:1px solid var(--forge-line)}
.forge-job-row{
  min-height:86px;padding:12px;border-bottom:1px solid var(--forge-line-soft);cursor:pointer;
  background:var(--forge-panel);transition:background-color .15s ease;
}
.forge-job-row:last-child{border-bottom:0}
.forge-job-row:hover{background:var(--forge-raised)}
.forge-job-row:focus-visible{
  outline:2px solid var(--forge-alert);outline-offset:-1px;
}
.forge-job-row.is-active-root{
  border-left:3px solid var(--forge-signal);
  background:rgba(220,112,72,.05);
}
.forge-active-badge{
  display:inline-flex;align-items:center;gap:6px;
  color:var(--forge-signal);font-size:11px;font-weight:700;font-family:var(--forge-font-mono);
  letter-spacing:.5px;
}
.forge-active-badge::before{
  content:"";width:6px;height:6px;border-radius:50%;background:var(--forge-signal);
  box-shadow:0 0 6px var(--forge-signal);
  animation:forge-pulse 2s infinite ease-in-out;
}
@keyframes forge-pulse{
  0%,100%{opacity:1;transform:scale(1)}
  50%{opacity:.35;transform:scale(.8)}
}
.forge-job-index{display:none}
.forge-job-main{min-width:0}
.forge-job-main strong{
  display:block;overflow:hidden;color:var(--forge-text);font-size:14px;
  text-overflow:ellipsis;white-space:nowrap;
}
.forge-job-main small{
  display:block;overflow:hidden;margin-top:4px;color:var(--forge-faint);font-size:11px;
  text-overflow:ellipsis;white-space:nowrap;font-family:var(--forge-font-mono);
}
.forge-progress{height:3px;max-width:none;margin-top:8px;background:var(--forge-line)}
.forge-progress i{display:block;width:0;height:100%;background:var(--forge-signal)}
.forge-progress i.is-complete{background:var(--forge-ready)}
.forge-job-stage{display:grid;gap:5px;justify-items:start}
.forge-chip{
  display:inline-flex;align-items:center;min-height:24px;padding:0 8px;border:1px solid var(--forge-line);
  border-radius:999px;color:var(--forge-muted);font-size:11px;white-space:nowrap;font-family:var(--forge-font-mono);
}
.forge-chip--ready{background:rgba(155,189,121,.1);border-color:#526343;color:var(--forge-ready)}
.forge-chip--alert{background:rgba(213,169,75,.1);border-color:#66502e;color:var(--forge-alert)}
.forge-chip--danger{background:rgba(219,128,100,.1);border-color:#633629;color:#e29476}
.forge-chip--run{background:rgba(220,112,72,.1);border-color:#6a3d2d;color:#ef9a75}
.forge-job-stage small,.forge-job-metrics{color:var(--forge-faint);font-size:11px;line-height:1.55}
.forge-job-metrics{font-family:var(--forge-font-mono);font-variant-numeric:tabular-nums}
.forge-job-actions{display:flex;flex-wrap:wrap;justify-content:flex-start;gap:6px}
.forge-job-actions .forge-button{white-space:nowrap}

/* Direction 1: Mini Stepper in job card */
.forge-mini-stepper{
  display:inline-flex;align-items:center;gap:3px;margin-top:2px;
}
.forge-mini-step{
  display:inline-flex;align-items:center;justify-content:center;
  min-width:18px;height:16px;padding:0 3px;border-radius:2px;
  font-size:9.5px;font-family:var(--forge-font-mono);font-weight:600;
  background:rgba(255,255,255,.04);color:var(--forge-faint);
}
.forge-mini-step.is-done{background:rgba(155,189,121,.15);color:var(--forge-ready)}
.forge-mini-step.is-current{background:rgba(220,112,72,.25);color:#ff986c;font-weight:700}
.forge-mini-step.is-attention{background:rgba(213,169,75,.25);color:var(--forge-alert);font-weight:700}
.forge-mini-step.is-danger{background:rgba(219,128,100,.25);color:var(--forge-danger)}

/* Direction 1: Pipeline Stepper in drawer */
.forge-pipeline{
  display:grid;grid-template-columns:repeat(5,minmax(0,1fr));
  gap:6px;margin-bottom:16px;padding:12px;
  border:1px solid var(--forge-line);background:rgba(19,20,18,.95);
  border-radius:var(--forge-radius);
}
.forge-step{
  position:relative;display:flex;flex-direction:column;gap:4px;
  padding:8px;border-radius:var(--forge-radius);
  background:#171816;border:1px solid var(--forge-line-soft);
  transition:all .15s ease;
}
.forge-step__head{
  display:flex;align-items:center;justify-content:space-between;
}
.forge-step__num{
  font-size:10px;font-weight:700;font-family:var(--forge-font-mono);color:var(--forge-faint);
}
.forge-step__dot{
  width:6px;height:6px;border-radius:50%;background:var(--forge-faint);
}
.forge-step strong{
  font-size:11.5px;color:var(--forge-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
.forge-step small{
  font-size:10px;color:var(--forge-faint);font-family:var(--forge-font-mono);white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis;
}
.forge-step.is-done{border-color:rgba(155,189,121,.3);background:rgba(155,189,121,.04)}
.forge-step.is-done .forge-step__dot{background:var(--forge-ready);box-shadow:0 0 6px rgba(155,189,121,.5)}
.forge-step.is-done strong{color:var(--forge-text)}
.forge-step.is-done small{color:var(--forge-ready)}

.forge-step.is-current{border-color:var(--forge-signal);background:rgba(220,112,72,.07);box-shadow:0 0 10px rgba(220,112,72,.15)}
.forge-step.is-current .forge-step__dot{background:var(--forge-signal);box-shadow:0 0 6px var(--forge-signal);animation:forge-pulse 1.8s infinite ease-in-out}
.forge-step.is-current strong{color:#ff986c}
.forge-step.is-current small{color:#ef9a75}

.forge-step.is-attention{border-color:var(--forge-alert);background:rgba(213,169,75,.07)}
.forge-step.is-attention .forge-step__dot{background:var(--forge-alert);box-shadow:0 0 6px var(--forge-alert);animation:forge-pulse 1.8s infinite ease-in-out}
.forge-step.is-attention strong{color:var(--forge-alert)}
.forge-step.is-attention small{color:var(--forge-alert)}

.forge-step.is-danger{border-color:var(--forge-danger);background:rgba(219,128,100,.08)}
.forge-step.is-danger .forge-step__dot{background:var(--forge-danger);box-shadow:0 0 6px var(--forge-danger)}
.forge-step.is-danger strong{color:var(--forge-danger)}
.forge-step.is-danger small{color:var(--forge-danger)}

/* Direction 2: Replenishment & Gap Ledger Panel */
.forge-replenish-panel{
  margin-bottom:16px;padding:14px;border:1px solid #5a4220;
  border-radius:var(--forge-radius);background:#201c16;
}
.forge-replenish-panel__head{
  display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px;
}
.forge-replenish-panel__head strong{font-size:13px;color:#e8c688}
.forge-replenish-panel__head p{margin:3px 0 0;font-size:11px;color:#a8977a}
.forge-tier-track{
  display:grid;grid-template-columns:1fr 1fr minmax(130px,1fr);gap:8px;align-items:stretch;
  padding:10px;margin-bottom:12px;background:rgba(0,0,0,.28);border-radius:var(--forge-radius);
  border:1px solid #3c2e1a;
}
.forge-tier-node{
  padding:8px 10px;border-radius:var(--forge-radius);
  border:1px solid #4a3c25;background:#282218;font-size:11px;
}
.forge-tier-node strong{display:block;font-size:12px;color:var(--forge-muted)}
.forge-tier-node small{display:block;margin-top:3px;font-size:10px;color:var(--forge-faint);font-family:var(--forge-font-mono)}
.forge-tier-node.is-active{border-color:var(--forge-alert);background:#382d1c}
.forge-tier-node.is-active strong{color:var(--forge-alert)}
.forge-tier-node.is-active small{color:#e2bb6e}
.forge-gap-table{width:100%;border-collapse:collapse;font-size:11px;margin-top:8px}
.forge-gap-table th{text-align:left;padding:6px 8px;border-bottom:1px solid #4a3c25;color:#a8977a;font-weight:600}
.forge-gap-table td{padding:7px 8px;border-bottom:1px solid #332817;color:var(--forge-text);vertical-align:middle}
.forge-gap-table tr:last-child td{border-bottom:0}
.forge-gap-kind{
  display:inline-block;padding:2px 6px;border-radius:3px;font-size:10px;
  font-family:var(--forge-font-mono);font-weight:600;
}
.forge-gap-kind--season{background:rgba(213,169,75,.2);color:#e2bb6e}
.forge-gap-kind--episode{background:rgba(220,112,72,.2);color:#ef9a75}
.forge-gap-kind--media{background:rgba(219,128,100,.2);color:#f2c0ae}
.forge-gap-kind--subtitle{background:rgba(155,189,121,.2);color:var(--forge-ready)}
.forge-details{
  margin-top:10px;padding:8px 10px;border:1px solid #4a3c25;border-radius:var(--forge-radius);
  background:rgba(0,0,0,.18);font-size:11px;
}
.forge-details summary{cursor:pointer;color:#c9b387;font-weight:600;outline:none;user-select:none}
.forge-details summary:hover{color:#f1deb8}
.forge-attempt-list{
  list-style:none;padding:8px 0 0;margin:0;display:flex;flex-direction:column;gap:6px;
}
.forge-attempt-item{
  display:grid;grid-template-columns:55px 60px minmax(100px,1fr) auto;gap:8px;
  padding:6px 8px;border-radius:3px;background:#1e1a14;border:1px solid #382c1b;
  font-family:var(--forge-font-mono);font-size:10.5px;
}

/* Direction 3: WorkUnit filter pills & Confidence display */
.forge-unit-filter-bar{
  display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin:12px 0 10px;
}
.forge-unit-pill{
  min-height:28px;padding:0 10px;border:1px solid var(--forge-line);border-radius:var(--forge-radius);
  background:transparent;color:var(--forge-muted);font-size:12px;cursor:pointer;
  display:inline-flex;align-items:center;gap:6px;transition:all .15s ease;
}
.forge-unit-pill:hover{background:var(--forge-raised);color:var(--forge-text)}
.forge-unit-pill.is-active{
  border-color:#6c6d65;background:#30312e;color:var(--forge-text);
}
.forge-unit-pill.is-active .forge-filter-count{
  background:var(--forge-signal);color:#19120e;font-weight:700;
}
.forge-conf{
  display:inline-flex;align-items:center;gap:3px;padding:1px 5px;border-radius:3px;
  font-size:10px;font-family:var(--forge-font-mono);font-weight:600;
}
.forge-conf--high{background:rgba(155,189,121,.15);color:var(--forge-ready);border:1px solid rgba(155,189,121,.3)}
.forge-conf--mid{background:rgba(213,169,75,.15);color:var(--forge-alert);border:1px solid rgba(213,169,75,.3)}
.forge-conf--low{background:rgba(220,112,72,.15);color:var(--forge-signal);border:1px solid rgba(220,112,72,.3)}
.forge-margin-tag{
  display:inline-block;padding:0 4px;margin-left:4px;border-radius:2px;
  background:rgba(255,255,255,.08);color:var(--forge-faint);font-size:9.5px;
  font-family:var(--forge-font-mono);
}

/* Direction 4: Diagnostics Vault */
.forge-diagnostics-box{
  margin-bottom:16px;padding:14px;border:1px solid var(--forge-line);
  border-radius:var(--forge-radius);background:#1a1b19;
}
.forge-diagnostics-box__head{
  display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px;
}
.forge-diagnostics-box__head strong{font-size:13px}
.forge-diag-error{
  display:flex;align-items:center;justify-content:space-between;gap:10px;
  padding:10px;margin-bottom:10px;border-radius:var(--forge-radius);
  border:1px solid #633629;background:#291b17;color:#f2c0ae;font-size:12px;line-height:1.5;
}
.forge-diag-list{
  list-style:none;padding:0;margin:8px 0 0;display:flex;flex-direction:column;gap:5px;
}
.forge-diag-item{
  display:flex;align-items:flex-start;justify-content:space-between;gap:10px;
  padding:6px 10px;border-radius:var(--forge-radius);background:rgba(0,0,0,.2);
  border:1px solid var(--forge-line-soft);font-size:11.5px;font-family:var(--forge-font-mono);
}
.forge-diag-item--bad{border-color:rgba(219,128,100,.3);color:#f2c0ae}
.forge-diag-item--warn{border-color:rgba(213,169,75,.3);color:#e2bb6e}
.forge-readback-stamp{
  display:inline-flex;align-items:center;gap:6px;padding:3px 8px;
  border-radius:var(--forge-radius);font-size:11px;font-family:var(--forge-font-mono);
  background:rgba(255,255,255,.04);color:var(--forge-faint);border:1px solid var(--forge-line);
}
.forge-readback-stamp--verified{
  border-color:rgba(155,189,121,.4);background:rgba(155,189,121,.08);color:var(--forge-ready);
}

/* Direction 5: One-click copy */
.forge-copy-btn{
  display:inline-flex;align-items:center;justify-content:center;
  min-height:20px;padding:1px 6px;margin-left:6px;
  border:1px solid var(--forge-line);border-radius:var(--forge-radius);
  background:rgba(255,255,255,.04);color:var(--forge-muted);
  font-size:11px;font-family:var(--forge-font-mono);cursor:pointer;
  vertical-align:middle;transition:all .15s ease;
}
.forge-copy-btn:hover{
  border-color:var(--forge-faint);background:rgba(255,255,255,.08);color:var(--forge-text);
}
.forge-copy-btn.is-copied{
  border-color:var(--forge-ready);background:rgba(155,189,121,.15);color:var(--forge-ready);
}

.forge-empty{
  padding:32px 16px;border:1px dashed var(--forge-line);color:var(--forge-faint);
  text-align:center;font-size:13px;line-height:1.6;
}
.forge-empty code{font-family:var(--forge-font-mono);color:var(--forge-muted)}
.forge-empty-action{margin-top:12px}
.forge-source-list{border:1px solid var(--forge-line);border-radius:var(--forge-radius)}
.forge-source-list--modal{max-height:220px;overflow-y:auto}
.forge-source{
  width:100%;display:grid;grid-template-columns:20px minmax(0,1fr) auto auto;gap:12px;align-items:center;
  min-height:64px;padding:0 14px;border:0;border-bottom:1px solid var(--forge-line-soft);
  background:rgba(28,29,27,.94);color:var(--forge-muted);text-align:left;cursor:pointer;
  transition:background-color .15s ease;
}
.forge-source:last-child{border-bottom:0}
.forge-source:hover,.forge-source.is-selected{background:var(--forge-raised);color:var(--forge-text)}
.forge-source__icon{width:10px;height:10px;border:1px solid var(--forge-faint)}
.forge-source.is-selected .forge-source__icon{border:3px solid var(--forge-signal)}
.forge-source strong{
  display:block;overflow:hidden;font-size:13px;text-overflow:ellipsis;white-space:nowrap;
}
.forge-source small{display:block;margin-top:4px;color:var(--forge-faint);font-size:11px;font-family:var(--forge-font-mono)}
.forge-source em{font-style:normal;color:var(--forge-ready);font-size:11px;white-space:nowrap}
.forge-source__action{display:flex;align-items:center}
.forge-source-item{
  width:100%;display:grid;grid-template-columns:20px minmax(0,1fr) auto;gap:12px;align-items:center;
  min-height:52px;padding:0 14px;border:0;border-bottom:1px solid var(--forge-line-soft);
  background:var(--forge-panel);color:var(--forge-muted);text-align:left;cursor:pointer;
}
.forge-source-item:last-child{border-bottom:0}
.forge-source-item:hover,.forge-source-item.is-selected{background:var(--forge-raised);color:var(--forge-text)}
.forge-source-item.is-selected .forge-source__icon{border:3px solid var(--forge-signal)}
.forge-drawer-backdrop,.forge-modal-backdrop{
  position:fixed;inset:0;z-index:30;display:none;background:rgba(6,8,6,.82);
}
.forge-drawer-backdrop.is-open{display:block}
.forge-modal-backdrop{align-items:center;justify-content:center}
.forge-modal-backdrop.is-open{display:flex}
.forge-drawer{
  position:absolute;top:0;right:0;width:min(720px,100%);height:100%;overflow:auto;
  border-left:1px solid var(--forge-line);background:#1f201e;box-shadow:-20px 0 70px rgba(0,0,0,.45);
}
.forge-drawer__head{
  position:sticky;top:0;z-index:1;display:flex;align-items:flex-start;justify-content:space-between;
  gap:14px;padding:18px 20px;border-bottom:1px solid var(--forge-line);background:#1f201e;
}
.forge-drawer__head h2{margin:0;font-size:18px}
.forge-drawer__head p{margin:6px 0 0;color:var(--forge-faint);font-size:12px;word-break:break-all;font-family:var(--forge-font-mono)}
.forge-close{border:0;background:transparent;color:var(--forge-muted);font-size:23px;line-height:1;cursor:pointer;padding:4px}
.forge-drawer__body{padding:20px}
.forge-drawer__actions{
  display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin:0 0 14px;padding:0 0 14px;border-bottom:1px solid var(--forge-line);
}
.forge-detail-bar{
  display:flex;flex-wrap:wrap;align-items:stretch;margin-bottom:16px;
  border:1px solid var(--forge-line);background:#181917;
}
.forge-detail-item{
  display:flex;align-items:center;gap:8px;min-height:42px;padding:8px 12px;
  border-right:1px solid var(--forge-line);color:var(--forge-muted);font-size:12px;
}
.forge-detail-item:last-child{border-right:0}
.forge-detail-item span{color:var(--forge-faint)}
.forge-detail-item strong{color:var(--forge-text);font-size:13px;font-family:var(--forge-font-mono);font-variant-numeric:tabular-nums}
.forge-detail-item code{font-family:var(--forge-font-mono);font-size:11px;color:var(--forge-muted);word-break:break-all}
.forge-detail-item .forge-chip{min-height:22px}
.forge-detail-item--gaps strong{color:var(--forge-alert)}
.forge-detail-item--wide{width:100%;border-top:1px solid var(--forge-line);border-right:0}
.forge-label{margin:0 0 8px;color:var(--forge-faint);font-size:12px}
.forge-unit-list{border-top:1px solid var(--forge-line)}
.forge-unit{padding:14px 0;border-bottom:1px solid var(--forge-line-soft)}
.forge-unit__top{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}
.forge-unit__top strong{font-size:14px}
.forge-unit__top small{display:block;margin-top:5px;color:var(--forge-faint);font-size:11px;word-break:break-all;font-family:var(--forge-font-mono)}
.forge-unit__meta{
  margin-top:8px;font-size:12px;color:var(--forge-muted);line-height:1.6;
  background:rgba(18,19,17,.6);padding:8px 10px;border-left:2px solid var(--forge-line);
}
.forge-unit__meta-item{display:block;margin-bottom:4px}
.forge-unit__meta-item:last-child{margin-bottom:0}
.forge-unit__meta-item strong{color:var(--forge-faint);font-weight:600;font-size:11px;margin-right:4px}
.forge-unit__meta-item--danger{color:var(--forge-danger)}
.forge-unit__meta-item--alert{color:var(--forge-alert)}
.forge-unit__meta-item--ready{color:var(--forge-ready)}
.forge-unit__meta code{font-family:var(--forge-font-mono);color:var(--forge-muted);font-size:11px}
.forge-unit__link{color:var(--forge-signal);text-decoration:none;font-family:var(--forge-font-mono)}
.forge-unit__link:hover{text-decoration:underline}
.forge-candidates{display:flex;flex-wrap:wrap;gap:6px;margin-top:12px}
.forge-candidate{
  min-height:30px;padding:2px 9px;border:1px solid var(--forge-line);border-radius:var(--forge-radius);
  background:var(--forge-raised);color:var(--forge-muted);font-size:11px;cursor:pointer;text-align:left;
}
.forge-candidate:hover{border-color:var(--forge-signal);color:var(--forge-text)}
.forge-candidate b{display:block;font-size:11px}
.forge-candidate span{display:block;margin-top:2px;color:var(--forge-faint);font-size:10px;font-family:var(--forge-font-mono)}
.forge-manual-confirm{
  margin-top:12px;padding:10px 12px;border:1px dashed var(--forge-line);
  background:rgba(20,21,19,.75);border-radius:var(--forge-radius);
}
.forge-manual-confirm__label{
  font-size:11px;color:var(--forge-faint);margin:0 0 8px;display:flex;align-items:center;justify-content:space-between;
}
.forge-manual-confirm__label a{color:var(--forge-signal);text-decoration:none}
.forge-manual-confirm__label a:hover{text-decoration:underline}
.forge-manual-inputs{
  display:flex;flex-wrap:wrap;gap:8px;align-items:center;
}
.forge-input,.forge-select{
  height:32px;padding:0 8px;border:1px solid var(--forge-line);border-radius:var(--forge-radius);
  background:var(--forge-panel);color:var(--forge-text);font-size:12px;
}
.forge-input:focus,.forge-select:focus{
  outline:0;border-color:var(--forge-signal);
}
.forge-input:disabled{opacity:.35;cursor:not-allowed}
.forge-input--mono{font-family:var(--forge-font-mono)}
.forge-input--id{width:110px}
.forge-input--season{width:95px}
.forge-modal{
  width:min(580px,calc(100% - 30px));max-height:min(720px,calc(100vh - 30px));overflow:auto;
  border:1px solid var(--forge-line);border-radius:6px;background:#1f201e;box-shadow:0 24px 80px rgba(0,0,0,.5);
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
  padding:10px 16px;transform:translate(-50%,10px);opacity:0;pointer-events:none;
  border:1px solid var(--forge-ready);border-radius:var(--forge-radius);
  background:#222320;color:var(--forge-text);font-size:12px;font-family:var(--forge-font-mono);
  box-shadow:0 8px 32px rgba(0,0,0,.6);transition:.2s ease;
}
.forge-toast.is-visible{transform:translate(-50%,0);opacity:1}
.forge-toast.is-bad{border-color:var(--forge-danger);background:#2b1c18;color:#f6d8ce}
@media(max-width:980px){
  .forge-summary{grid-template-columns:repeat(2,minmax(0,1fr))}
  .forge-table-head{display:none}
  .forge-job-row{grid-template-columns:minmax(0,1fr) minmax(155px,.7fr)}
  .forge-job-metrics{display:none}
  .forge-job-actions{grid-column:1 / -1}
  .forge-pipeline{grid-template-columns:repeat(2,1fr)}
  .forge-tier-track{grid-template-columns:1fr}
}
@media(max-width:680px){
  .forge-topbar{align-items:flex-start;flex-direction:column;gap:12px}
  .forge-commands{width:100%;justify-content:space-between;flex-wrap:wrap;gap:10px}
  .forge-status-cluster{justify-content:flex-start;gap:7px}
  .forge-summary{grid-template-columns:repeat(2,minmax(0,1fr))}
  .forge-attention{grid-template-columns:1fr}
  .forge-job-row{display:block}
  .forge-job-stage{display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap}
  .forge-job-actions{margin-top:12px}
  .forge-drawer{width:100%}
  .forge-detail-item{width:50%;border-bottom:1px solid var(--forge-line)}
  .forge-detail-item:nth-last-child(-n+2){border-bottom:0}
  .forge-section__head{display:block}
  .forge-source-toolbar{grid-template-columns:minmax(0,1fr) auto;gap:10px}
  .forge-filters{margin-top:10px}
  .forge-pipeline{grid-template-columns:1fr}
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
      <span class="forge-heartbeat" id="heartbeatDot" title="调度心跳 (10s 轮询)"></span>
      <span class="forge-status" id="serviceStatus">正在读取状态</span>
      <span class="forge-active-root-badge" id="topActiveBadge">未选定活跃任务</span>
    </div>
    <div class="forge-command-actions">
      <button type="button" class="forge-button forge-button--quiet" id="controlButton">恢复</button>
      <button type="button" class="forge-button forge-button--quiet" id="refreshButton">刷新</button>
      <button type="button" class="forge-button forge-button--quiet" id="orphanButton" title="清除指向已删除任务的孤儿选择">清除孤儿选择</button>
    </div>
  </div>
</header>

<main class="forge-shell">
  <nav class="forge-workbar" role="tablist" aria-label="工作区">
    <div class="forge-view-tabs">
      <button type="button" class="forge-view-tab is-active" role="tab" aria-selected="true" aria-controls="sourceView" data-view="sources">待选区</button>
      <button type="button" class="forge-view-tab" role="tab" aria-selected="false" aria-controls="tasksView" data-view="tasks">任务</button>
      <button type="button" class="forge-view-tab" role="tab" aria-selected="false" aria-controls="browseView" data-view="browse">库浏览</button>
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
      <div><h2>需要处理</h2><p>需要确认的项目会列在这里。识别或对账不确定，请确认正确身份：</p></div>
    </div>
    <div class="forge-attention" id="attentionList"></div>
  </section>

  <section class="forge-section">
    <div class="forge-section__head">
      <div><h2>任务列表</h2><p id="jobSummary">正在读取任务</p></div>
      <div class="forge-filters" role="tablist" aria-label="任务筛选">
        <button type="button" class="forge-filter is-active" data-filter="all">全部<span class="forge-filter-count" id="countAll">0</span></button>
        <button type="button" class="forge-filter" data-filter="active">处理中<span class="forge-filter-count" id="countActive">0</span></button>
        <button type="button" class="forge-filter" data-filter="attention">需要关注<span class="forge-filter-count" id="countAttention">0</span></button>
        <button type="button" class="forge-filter" data-filter="history">已完成<span class="forge-filter-count" id="countHistory">0</span></button>
      </div>
    </div>
    <div class="forge-table-head" aria-hidden="true">
      <span>任务与来源</span><span>状态与主流程</span><span>进度</span><span>操作</span>
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

  <section class="forge-section" id="browseView" hidden>
    <div class="forge-source-toolbar">
      <div><h2>库浏览</h2><p id="browseStamp">只读视图 · 不提供删除或移动</p></div>
      <div style="display:flex;gap:8px;align-items:center">
        <button type="button" class="forge-button forge-button--quiet" id="browseRefresh" title="夸克列表可能滞后数分钟，强制刷新走新索引">强制刷新</button>
      </div>
    </div>
    <div class="forge-gap-table" style="margin-bottom:10px">
      <table><thead><tr><th>快捷入口</th></tr></thead><tbody><tr><td id="browseQuickLinks"></td></tr></tbody></table>
    </div>
    <div id="browseCrumbs" style="padding:4px 0 10px;font-family:var(--forge-font-mono);font-size:12px;color:var(--forge-muted)"></div>
    <div class="forge-table-head" aria-hidden="true"><span>名称</span><span>类型</span><span>大小</span><span>修改时间</span></div>
    <div class="forge-job-board" id="browseBoard"><div class="forge-empty">选择快捷入口或输入路径</div></div>
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
      <p class="forge-label">选择待整理来源：</p>
      <div class="forge-source-list forge-source-list--modal" id="modalSources"><div class="forge-empty">正在读取来源</div></div>
      <p class="forge-choice-label">这个任务应整理到哪里？</p>
      <div class="forge-segment" id="shelfChoices">
        <button type="button" data-shelf="movie">电影</button>
        <button type="button" data-shelf="anime" class="is-selected">番剧</button>
        <button type="button" data-shelf="us_tv">欧美剧</button>
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
      return saved === "tasks" || saved === "sources" || saved === "browse" ? saved : "sources";
    } catch(error) {
      return "sources";
    }
  }
  function saveView(view){
    try { window.sessionStorage.setItem(viewStorageKey,view); } catch(error) {}
  }
  var state = {
    jobs:[], sources:[], units:{}, replenishment:{}, health:null, intakeRefreshedAt:null,
    view:savedView(), filter:"all", unitFilter:"all", busy:false, loading:false, healthError:false,
    drawerJobId:null, selectedSource:null, selectedShelf:"anime", pendingAction:null,
    browsePath:null, browseData:null, browseBusy:false
  };
  var terminal = new Set(["completed","executed","cancelled"]);
  var attention = new Set([
    "failed","failed_archive","failed_cleanup","failed_identity","failed_planning",
    "failed_provider","failed_verification","failed_write","reconciliation_uncertain",
    "target_policy_conflict","needs_attention","uncertain"
  ]);
  var phaseLabels = {
    queued:"已排队",planned:"已排队",reconciling:"正在对账",reconciled:"已对账",
    needs_attention:"需要关注",archive_preprocessing:"归档预处理",identity_matching:"识别内容",
    analyzing:"分析目录",planning:"生成计划",executing:"正在写入",executing_media:"正在写入",
    verifying:"正在核验",cleaning:"清理暂存",completed:"已完成",gaps_pending:"缺口待闭环",
    executed:"已完成",cancelled:"已取消",failed:"失败",failed_archive:"归档失败",
    failed_cleanup:"清理失败",failed_identity:"识别失败",failed_planning:"计划失败",
    failed_provider:"补源失败",failed_verification:"验收失败",failed_write:"写入失败",
    reconciliation_uncertain:"对账冲突",target_policy_conflict:"货架冲突",
    retry_wait:"等待重试",awaiting_target_shelf:"等待选择货架",gap_discovering:"核对缺口",
    provider_searching:"搜索补源",acquiring:"获取文件",staging_verifying:"核对暂存",
    subtitle_installing:"处理字幕",child_planning:"规划补源",child_executing:"整理补源",
    final_verifying:"最终核对",uncertain:"需要确认",confirmed:"已确认",complete:"已完成",
    pending:"等待处理",running:"处理中",waiting_reconcile:"等待安全对账",
    paused_waiting:"暂停等待",disc_expanding:"展开光盘",disc_expansion:"光盘展开",
    duplicate_complete:"完全重复",existing_gap:"既有作品缺口",merge_existing:"归并入库",
    new_work:"全新作品"
  };
  var shelfLabels = {movie:"电影",anime:"番剧",us_tv:"欧美剧"};
  var tierLabels = {quark_share:"夸克分享",magnet:"本地磁力"};

  function number(value){ return typeof value === "number" && isFinite(value) ? value : 0; }
  function activeRootJobId(){
    return state.health && state.health.control && state.health.control.root_job_id || null;
  }
  function isSelectedRoot(job){
    return !!(job && job.id && activeRootJobId() === job.id);
  }
  function jobTitle(job){
    var raw = job && job.plan && job.plan.title || job && job.source || job && job.id || "未命名任务";
    var bits = String(raw).split("/").filter(Boolean);
    return bits[bits.length - 1] || String(raw);
  }
  function jobSource(job){ return job && job.source || job && job.id || ""; }
  function phaseLabel(phase){ return phaseLabels[phase] || phase || "未知状态"; }
  function phaseTone(phase){
    if(terminal.has(phase)){ return "ready"; }
    if(attention.has(phase) || phase === "reconciliation_uncertain" || phase === "target_policy_conflict"){
      return (String(phase).indexOf("failed") === 0) ? "danger" : "alert";
    }
    if(phase === "gaps_pending"){ return "alert"; }
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
  function isCancellableJob(job){ return !!(job && !terminal.has(job.phase) && job.phase !== "complete"); }
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

  /* Direction 1: Pipeline 5-Stage Step Determination */
  function pipelineSteps(job, units){
    units = units || [];
    var sState = (job.phase === "awaiting_target_shelf") ? "current" : "done";
    var sText = (job.phase === "awaiting_target_shelf") ? "待选货架" : "已授权";

    var bHasUncertain = units.some(function(u){ return u.identity_status === "uncertain"; });
    var bState = "pending";
    var bText = "未开始";
    if(sState === "done"){
      if(bHasUncertain){
        bState = "attention";
        bText = "需确认身份";
      } else if(job.phase === "planning" || job.phase === "queued" || job.phase === "analyzing" || job.phase === "identity_matching"){
        bState = "current";
        bText = "识别中";
      } else if(units.length && units.every(function(u){ return u.identity_status !== "uncertain"; })){
        bState = "done";
        bText = "已识别 (" + units.length + ")";
      } else if(job.plan && (job.plan.title || job.plan.file_count)){
        bState = "done";
        bText = "已完成";
      } else {
        bState = "current";
        bText = "分析中";
      }
    }

    var dHasConflict = (job.engine_phase === "reconciliation_uncertain") || units.some(function(u){ return u.reconciliation_outcome === "uncertain"; });
    var dState = "pending";
    var dText = "待对账";
    if(bState === "done"){
      if(dHasConflict){
        dState = "attention";
        dText = "对账冲突";
      } else if(job.phase === "reconciling"){
        dState = "current";
        dText = "三库查重中";
      } else if(units.some(function(u){ return u.reconciliation_outcome; }) || (job.plan && job.plan.target_root)){
        dState = "done";
        dText = "对账完成";
      } else {
        dState = "current";
        dText = "对账中";
      }
    }

    var gFailed = (job.phase && String(job.phase).indexOf("failed") === 0) || units.some(function(u){ return u.acceptance && u.acceptance.error; });
    var gReadbackOk = (job.readback && job.readback.status === "verified") || (job.phase === "completed") || (job.phase === "gaps_pending");
    var gState = "pending";
    var gText = "待写入";
    if(dState === "done"){
      if(gFailed){
        gState = "danger";
        gText = "写入/回读失败";
      } else if(job.phase === "executing" || job.phase === "executing_media" || job.phase === "verifying"){
        gState = "current";
        gText = "单写器执行中";
      } else if(gReadbackOk){
        gState = "done";
        gText = "回读通过";
      } else {
        gState = "pending";
        gText = "等待执行";
      }
    }

    var detail = stats(job);
    var jState = "pending";
    var jText = "待闭环";
    if(gState === "done"){
      if(detail.gaps > 0 || job.phase === "gaps_pending"){
        if(isWaiting(job)){
          jState = "attention";
          jText = "补源挂起等待";
        } else {
          jState = "current";
          jText = "补源中 (" + detail.gaps + ")";
        }
      } else if(isTerminalJob(job) || detail.gaps === 0){
        jState = "done";
        jText = "完整闭环";
      } else {
        jState = "pending";
        jText = "就绪";
      }
    }

    return [
      {num:"01", id:"S", title:"S 货架授权", desc:"用户选择货架创建并授权 RootJob", state:sState, text:sText},
      {num:"02", id:"BC", title:"B/C 边界与识别", desc:"目录形态分析与 TMDB 身份打分识别", state:bState, text:bText},
      {num:"03", id:"D", title:"D 三库对账", desc:"电影/番剧/欧美剧 LibraryIndex 独立查重与五分类判定", state:dState, text:dText},
      {num:"04", id:"GH", title:"G/H 执行验收", desc:"单写器串行写入、路径字节核验与 Fresh Listing 回读", state:gState, text:gText},
      {num:"05", id:"JN", title:"J/N 缺口闭环", desc:"缺口账本精确管理与两阶严格补源 (Quark → Magnet)", state:jState, text:jText}
    ];
  }

  function miniStepper(job){
    var view = unitData(job.id);
    var steps = pipelineSteps(job, view && view.units);
    return '<div class="forge-mini-stepper" title="主流程 5 阶进度">' + steps.map(function(s){
      return '<span class="forge-mini-step is-' + s.state + '" title="' + esc(s.title + '：' + s.text) + '">' + esc(s.id) + '</span>';
    }).join("") + '</div>';
  }

  /* Direction 5: One-click copy with moment feedback */
  function copyText(text, button){
    if(!text){ return; }
    var done = function(){
      if(!button){ toast("已复制到剪贴板"); return; }
      var old = button.innerHTML;
      button.innerHTML = "✓ 已复制";
      button.classList.add("is-copied");
      setTimeout(function(){
        button.innerHTML = old;
        button.classList.remove("is-copied");
      }, 1500);
    };
    if(navigator.clipboard && navigator.clipboard.writeText){
      navigator.clipboard.writeText(text).then(done).catch(function(){
        fallbackCopy(text);
        done();
      });
    } else {
      fallbackCopy(text);
      done();
    }
  }
  function fallbackCopy(text){
    var t = document.createElement("textarea");
    t.value = text;
    t.style.position = "fixed";
    t.style.opacity = "0";
    document.body.appendChild(t);
    t.select();
    try { document.execCommand("copy"); } catch(e){}
    document.body.removeChild(t);
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
    $$("button, input, select").forEach(function(el){ el.disabled = value; });
    var dot = $("#heartbeatDot");
    if(dot){ dot.classList.toggle("is-active", value || state.loading); }
  }
  function setRefreshState(refreshing){
    var button = $("#refreshButton");
    button.textContent = refreshing ? "刷新中…" : "刷新";
    button.setAttribute("aria-busy", refreshing ? "true" : "false");
    var dot = $("#heartbeatDot");
    if(dot){ dot.classList.toggle("is-active", refreshing); }
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
    var topActive = $("#topActiveBadge");
    if(!state.health){
      service.textContent = state.healthError ? "API 无响应" : "正在读取状态";
      service.className = "forge-status" + (state.healthError ? " is-bad" : "");
      if(topActive){ topActive.textContent = "未选定活跃任务"; }
      return;
    }
    var selected = activeRootJobId();
    var activeJob = selected ? jobById(selected) : null;
    if(topActive){
      if(activeJob){
        topActive.innerHTML = '活跃任务: <em>' + esc(jobTitle(activeJob)) + '</em>';
      } else if(selected){
        topActive.innerHTML = '活跃任务 ID: <em>' + esc(selected).slice(0,12) + '…</em>';
      } else {
        topActive.textContent = "未选定活跃任务";
      }
    }
    if(state.health.ok !== true){
      service.textContent = "API 无响应";
      service.className = "forge-status is-bad";
    } else if(systemPaused()){
      service.textContent = selected ? "系统已暂停" : "已暂停（未选定任务）";
      service.className = "forge-status is-paused";
    } else {
      service.textContent = selected ? "运行中" : "空闲（未选定任务）";
      service.className = "forge-status is-ready";
    }
    var controlButton = $("#controlButton");
    controlButton.disabled = !selected || state.busy;
    controlButton.textContent = systemPaused() ? "恢复" : "暂停";
    if(!selected){
      controlButton.title = "请先在任务列表中选择或创建一个活跃任务";
    } else {
      controlButton.removeAttribute("title");
    }
  }
  function renderCore(){
    var waiting = state.jobs.filter(isWaiting).length;
    var recovering = state.jobs.filter(isRecoveryWait).length;
    var activeJobs = state.jobs.filter(function(job){
      return !isTerminalJob(job) && !isAttentionJob(job) && !isWaiting(job);
    });
    var attentionJobs = state.jobs.filter(isAttentionJob);
    var completedJobs = state.jobs.filter(isTerminalJob);
    var issues = attentionItems().length + waiting;
    var completed = completedJobs.length;
    var total = state.jobs.length;

    $("#taskSummary").hidden = total === 0;
    $("#activeSummaryLabel").textContent = systemPaused()
      ? "暂停等待"
      : (recovering ? "安全对账 / 重试" : "处理中");
    $("#activeCount").textContent = activeJobs.length;
    $("#attentionCount").textContent = issues;
    $("#completedCount").textContent = completed;
    $("#sourceRefreshStamp").textContent = state.intakeRefreshedAt
      ? "AList 刷新于 " + shortTime(state.intakeRefreshedAt)
      : "尚未刷新 AList";

    $("#countAll").textContent = total;
    $("#countActive").textContent = activeJobs.length;
    $("#countAttention").textContent = attentionJobs.length;
    $("#countHistory").textContent = completed;
  }
  function attentionItems(){
    var items = [];
    state.jobs.forEach(function(job){
      if(isAttentionJob(job)){
        items.push({job:job,title:jobTitle(job),message:job.error || phaseLabel(job.phase)});
      }
      var view = unitData(job.id);
      (view && view.units || []).forEach(function(unit){
        if(unit.identity_status === "uncertain"){
          items.push({job:job,unit:unit,title:unit.display_label || unit.boundary_key || "未命名单元",message:"身份需要确认"});
        } else if(unit.reconciliation_outcome === "uncertain"){
          items.push({job:job,unit:unit,title:unit.display_label || unit.boundary_key || "未命名单元",message:"三库对账存在冲突"});
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
    var isCurrentActive = isSelectedRoot(job);
    var action = '<button type="button" class="forge-button forge-button--small forge-button--quiet" data-action="open" data-job="' + esc(job.id) + '">查看详情</button>';
    if(!isCurrentActive && !isTerminalJob(job)){
      action += '<button type="button" class="forge-button forge-button--small forge-button--quiet" data-action="select" data-job="' +
        esc(job.id) + '">设为活跃</button>';
    }
    if(isAttentionJob(job) || (job.phase && String(job.phase).indexOf("failed") === 0)){
      action += '<button type="button" class="forge-button forge-button--small forge-button--signal" data-action="retry" data-job="' +
        esc(job.id) + '">重试任务</button>';
    }
    if(isCancellableJob(job)){
      action += '<button type="button" class="forge-button forge-button--small forge-button--danger" data-action="cancel" data-job="' +
        esc(job.id) + '">取消任务</button>';
    }
    if(detail.gaps > 0 && !waiting && !requiresRootJobSubtitleMigration(job)){
      action += '<button type="button" class="forge-button forge-button--small forge-button--replenish" data-action="replenish" data-job="' +
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
    var rowClass = "forge-job-row" + (isCurrentActive ? " is-active-root" : "");
    var activeTag = isCurrentActive ? '<div style="margin-bottom:4px"><span class="forge-active-badge">活跃任务</span></div>' : "";
    return '<article class="' + rowClass + '" tabindex="0" role="button" aria-label="查看任务详情" data-open-job="' + esc(job.id) + '">' +
      '<div class="forge-job-index">' + String(index + 1).padStart(2,"0") + "</div>" +
      '<div class="forge-job-main">' + activeTag + '<strong title="' + esc(jobSource(job)) + '">' + esc(jobTitle(job)) +
      '</strong><small title="' + esc(jobSource(job)) + '">' + esc(jobSource(job)) + "</small>" +
      '<div class="forge-progress"><i class="' + progressClass + '" style="width:' + progress + '%"></i></div></div>' +
      '<div class="forge-job-stage">' + chip(shownPhase) + miniStepper(job) + '<small>' + esc(secondary) + "</small></div>" +
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
    if(rows.length){
      $("#jobBoard").innerHTML = rows.map(jobRow).join("");
    } else if(state.jobs.length === 0){
      $("#jobBoard").innerHTML = '<div class="forge-empty">当前没有进行中的任务。<br><small>待选区如有媒体文件夹，可切换至【待选区】创建根任务。</small><div class="forge-empty-action"><button type="button" class="forge-button forge-button--small forge-button--signal" data-switch-view="sources">前往待选区 →</button></div></div>';
    } else {
      $("#jobBoard").innerHTML = '<div class="forge-empty">当前筛选下没有任务。<br><small>可切换至其他筛选或查看全部。</small><div class="forge-empty-action"><button type="button" class="forge-button forge-button--small forge-button--quiet" data-reset-filter="all">显示全部任务</button></div></div>';
    }
  }
  function sourceButton(source){
    var selected = state.selectedSource === source.canonical_path;
    var stateText = "待创建";
    var count = (source.child_count == null ? "—" : source.child_count) + " 个子目录 · " +
      (source.file_count == null ? "—" : source.file_count) + " 个文件";
    return '<div class="forge-source' + (selected ? " is-selected" : "") +
      '" data-source="' + esc(source.canonical_path) + '"><span class="forge-source__icon"></span><span>' +
      '<strong title="' + esc(source.canonical_path) + '">' + esc(source.display_name || source.canonical_path) +
      '</strong><small>' + esc(count) + '</small></span><em>' + esc(stateText) +
      '</em><span class="forge-source__action"><button type="button" class="forge-button forge-button--small forge-button--signal" data-create-source="' +
      esc(source.canonical_path) + '">以此建任务 →</button></span></div>';
  }
  function modalSourceItem(source){
    var selected = state.selectedSource === source.canonical_path;
    var count = (source.child_count == null ? "—" : source.child_count) + " 目录 · " +
      (source.file_count == null ? "—" : source.file_count) + " 文件";
    return '<div class="forge-source-item' + (selected ? " is-selected" : "") +
      '" data-source="' + esc(source.canonical_path) + '"><span class="forge-source__icon"></span><span>' +
      '<strong title="' + esc(source.canonical_path) + '">' + esc(source.display_name || source.canonical_path) +
      '</strong><small>' + esc(count) + '</small></span><span style="font-size:11px;color:var(--forge-ready)">待整理</span></div>';
  }
  function waitingSources(){
    return state.sources.filter(function(source){
      return source.present !== false && !source.root_task_id;
    });
  }
  function renderSources(){
    var sources = waitingSources();
    $("#sourceStrip").innerHTML = sources.length
      ? sources.map(sourceButton).join("")
      : '<div class="forge-empty">待选区没有等待刮削的文件夹。<br><small>请将待整理媒体文件夹放入 AList 的“/待刮削”目录。</small></div>';
    $("#modalSources").innerHTML = sources.length
      ? sources.map(modalSourceItem).join("")
      : '<div class="forge-empty">没有可选择的来源目录</div>';
  }
  function renderView(){
    var isTasks = state.view === "tasks";
    var isBrowse = state.view === "browse";
    $("#sourceView").hidden = isTasks || isBrowse;
    $("#tasksView").hidden = !isTasks;
    $("#browseView").hidden = !isBrowse;
    $$(".forge-view-tab").forEach(function(tab){
      var active = tab.dataset.view === state.view;
      tab.classList.toggle("is-active",active);
      tab.setAttribute("aria-selected",active ? "true" : "false");
    });
    if(isBrowse && !state.browseData){ loadBrowse(); }
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
      state.replenishment[jobId] = null;
      return null;
    });
  }
  function loadReplenishmentViews(jobs){
    var liveIds = {};
    jobs.forEach(function(job){ liveIds[job.id] = true; });
    Object.keys(state.replenishment).forEach(function(jobId){
      if(!liveIds[jobId]){ delete state.replenishment[jobId]; }
    });
    return Promise.all(jobs.map(function(job){
      return loadReplenishmentView(job.id);
    }));
  }
  function load(options){
    options = options || {};
    var refreshIntake = options.refreshIntake === true;
    state.loading = true;
    var dot = $("#heartbeatDot");
    if(dot){ dot.classList.add("is-active"); }
    var intakeRequest = refreshIntake
      ? api("/api/intake/refresh",{method:"POST"}).then(function(data){
          state.intakeRefreshedAt = data.refreshed_at || new Date().toISOString();
          return api("/api/intake");
        })
      : api("/api/intake");
    var healthRequest = api("/api/health").catch(function(){ return {ok:false}; });
    var jobsRequest = api("/api/jobs").then(function(data){ return data.jobs || []; });

    return Promise.all([intakeRequest,jobsRequest,healthRequest]).then(function(results){
      state.healthError = false;
      state.sources = results[0].sources || [];
      state.jobs = results[1];
      state.health = results[2];
      var liveIds = {};
      state.jobs.forEach(function(job){ liveIds[job.id] = true; });
      Object.keys(state.units).forEach(function(jobId){
        if(!liveIds[jobId]){ delete state.units[jobId]; }
      });
      return Promise.all(state.jobs.map(function(job){
        return api("/api/jobs/" + encodeURIComponent(job.id) + "/work-units").then(function(view){
          state.units[job.id] = view;
        }).catch(function(){});
      })).then(function(){ return loadReplenishmentViews(state.jobs); });
    }).then(function(){
      renderAll();
      return true;
    }).catch(function(error){
      state.healthError = true;
      renderAll();
      toast(error.message,true);
      return false;
    }).finally(function(){
      state.loading = false;
      if(dot && !state.busy){ dot.classList.remove("is-active"); }
    });
  }
  function openDrawer(jobId){
    state.drawerJobId = jobId;
    $("#drawerBackdrop").classList.add("is-open");
    $("#drawerBackdrop").setAttribute("aria-hidden","false");
    $("#drawerBody").innerHTML = '<div class="forge-empty">正在加载作品单元和缺口账本…</div>';
    renderDrawer();
    var requests = [
      api("/api/jobs/" + encodeURIComponent(jobId) + "/work-units").then(function(view){ state.units[jobId] = view; }),
      loadReplenishmentView(jobId)
    ];
    return Promise.all(requests).then(function(){ renderDrawer(); }).catch(function(error){
      $("#drawerBody").innerHTML = '<div class="forge-empty">' + esc(error.message) + "</div>";
    });
  }
  function unitStatusKey(unit){
    if(unit.reconciliation_outcome === "uncertain"){ return "reconciliation_uncertain"; }
    if(unit.identity_status === "uncertain"){ return "uncertain"; }
    if(unit.identity_status === "failed"){ return "failed"; }
    if(unit.gap_status === "attention"){ return "needs_attention"; }
    if(unit.reconciliation_outcome){ return unit.reconciliation_outcome; }
    return unit.identity_status || "未完成";
  }

  /* Direction 2: Replenishment & Gap Ledger Visualizer */
  function replenishmentPanel(job, detail, replenishment){
    var tierState = (replenishment && replenishment.tier_state) || {};
    var tier = tierState.tier || tierFor(job) || "quark_share";
    var waiting = tierState.waiting || "";
    var requests = (replenishment && replenishment.requests) || [];
    var attemptLog = Array.isArray(tierState.attempt_log) ? tierState.attempt_log : [];
    var hasGaps = detail.gaps > 0 || requests.length > 0 || attemptLog.length > 0;
    if(!hasGaps){ return ""; }

    var tier1Active = (tier === "quark_share");
    var tier2Active = (tier === "magnet");
    var waitingLabel = "";
    if(waiting === "waiting_reconcile"){
      waitingLabel = '<span class="forge-chip forge-chip--alert">存疑等待对账 (锁定中)</span>';
    } else if(waiting === "retry_wait"){
      waitingLabel = '<span class="forge-chip forge-chip--alert">网络/服务故障 (等待重试)</span>';
    }

    var trackHtml =
      '<div class="forge-tier-track">' +
      '<div class="forge-tier-node' + (tier1Active ? " is-active" : "") + '">' +
      '<strong>第 1 阶：Quark 分享</strong><small>' + (tier1Active ? (waiting ? "锁定/等待" : "当前活跃推进阶") : "候选已排除 / 已降阶") + '</small></div>' +
      '<div class="forge-tier-node' + (tier2Active ? " is-active" : "") + '">' +
      '<strong>第 2 阶：本地 Torrent (DHT)</strong><small>' + (tier2Active ? (waiting ? "锁定/等待" : "当前活跃推进阶") : "备用阶梯 (直连无代理)") + '</small></div>' +
      '<div class="forge-tier-node">' +
      '<strong>独立通道：中文字幕</strong><small>简中/双语直搜 · 专属 Staging 闭环</small></div>' +
      '</div>';

    var gapRows = [];
    requests.forEach(function(req){
      var mediaTitle = (req.media && (req.media.title || req.media.original_title)) || "作品";
      var gaps = req.gaps || [];
      gaps.forEach(function(g){
        var kindMap = {
          missing_season: {label:"缺失整季", cls:"season"},
          missing_episode: {label:"缺失单集", cls:"episode"},
          missing_media: {label:"缺失全片", cls:"media"},
          missing_subtitle: {label:"缺失字幕", cls:"subtitle"}
        };
        var kInfo = kindMap[g.kind] || {label:g.kind || "缺口", cls:"media"};
        var coord = "";
        if(g.season != null && g.episodes && g.episodes.length){
          coord = "S" + String(g.season).padStart(2,"0") + "E" + g.episodes.map(function(e){ return String(e).padStart(2,"0"); }).join(",");
        } else if(g.season != null){
          coord = "第 " + g.season + " 季全集";
        } else if(g.subtitle_language){
          coord = "字幕 [" + g.subtitle_language + "]";
        } else {
          coord = g.id || "未指定坐标";
        }
        var targetPath = g.path || "";
        gapRows.push(
          '<tr>' +
          '<td><span class="forge-gap-kind forge-gap-kind--' + kInfo.cls + '">' + esc(kInfo.label) + '</span></td>' +
          '<td><strong>' + esc(mediaTitle) + '</strong></td>' +
          '<td><code>' + esc(coord) + '</code></td>' +
          '<td>' + (targetPath ? '<code title="' + esc(targetPath) + '">' + esc(targetPath).split("/").slice(-2).join("/") + '</code>' : '<span style="color:var(--forge-faint)">-</span>') + '</td>' +
          '</tr>'
        );
      });
    });

    var gapTableHtml = gapRows.length ? (
      '<table class="forge-gap-table"><thead><tr>' +
      '<th>类型</th><th>作品</th><th>季集坐标</th><th>目标路径</th>' +
      '</tr></thead><tbody>' + gapRows.join("") + '</tbody></table>'
    ) : '<div style="color:var(--forge-muted);font-size:12px;padding:6px 0">所有缺口坐标已成功闭环或正在生成账本投影。</div>';

    var attemptHtml = "";
    if(attemptLog.length){
      var logItems = attemptLog.slice(-10).reverse().map(function(item){
        var timeStr = item.recorded_at ? shortTime(item.recorded_at) : "--:--";
        var tName = item.tier === "quark_share" ? "Quark" : (item.tier === "magnet" ? "Torrent" : (item.tier || "补源"));
        var outcomeMap = {
          candidate: '<span style="color:var(--forge-faint)">[排除] 无效资源</span>',
          infrastructure: '<span style="color:var(--forge-alert)">[重试] 网络异常</span>',
          in_doubt: '<span style="color:var(--forge-alert)">[对账] 存疑锁定</span>',
          window_pending: '<span style="color:var(--forge-faint)">[窗口] 搜索预算未翻完，下轮继续</span>',
          closed: '<span style="color:var(--forge-ready)">[闭环] 缺口已核销</span>',
          paused: '<span style="color:var(--forge-faint)">[暂停] 等待恢复</span>',
          waiting_reconcile: '<span style="color:var(--forge-alert)">[对账] 等待安全对账</span>',
          succeeded: '<span style="color:var(--forge-ready)">[成功] 命中入库</span>'
        };
        var outText = outcomeMap[item.outcome] || ('<span>' + esc(item.outcome || "尝试") + '</span>');
        var errText = item.error ? (' · ' + esc(item.error)) : "";
        return '<li class="forge-attempt-item">' +
          '<span>' + esc(timeStr) + '</span>' +
          '<strong>' + esc(tName) + '</strong>' +
          '<span><code>' + esc(item.gap_id || "-") + '</code>' + errText + '</span>' +
          outText + '</li>';
      }).join("");
      attemptHtml =
        '<details class="forge-details"><summary>补源尝试与排除审计日志 (' + attemptLog.length + ' 条记录)</summary>' +
        '<ul class="forge-attempt-list">' + logItems + '</ul></details>';
    }

    return '<div class="forge-replenish-panel">' +
      '<div class="forge-replenish-panel__head">' +
      '<div><strong>两阶补源与缺口账本 (Gap Ledger)</strong>' +
      '<p>遵循 strict two-tier 纪律：Quark 分享 ➔ 本地 Torrent，经 Staging 闭环核销</p></div>' +
      (waitingLabel || ('<span class="forge-chip forge-chip--' + (tier1Active ? "run" : "alert") + '">' + esc(tierLabels[tier] || tier) + '</span>')) +
      '</div>' +
      trackHtml +
      '<p class="forge-label" style="margin-top:10px">待闭环缺口明细（' + gapRows.length + '）</p>' +
      gapTableHtml +
      attemptHtml +
      '<details class="forge-details" style="margin-top:10px"><summary>直供资源登记（操作员通道）</summary>' +
      '<form data-catalog-form="1" data-job="' + esc(state.drawerJobId) + '">' +
      '<div class="forge-manual-inputs" style="flex-wrap:wrap;margin-top:6px">' +
      '<input type="number" class="forge-input forge-input--mono forge-input--id" placeholder="TMDB ID" min="1" required data-catalog-tmdb>' +
      '<input type="text" class="forge-input" placeholder="发布名 release_name" required data-catalog-release style="flex:1;min-width:220px">' +
      '<select class="forge-select" data-catalog-resolution>' +
      '<option value="2160p">2160p</option><option value="1080p" selected>1080p</option>' +
      '<option value="720p">720p</option><option value="unknown">未知</option></select>' +
      '</div>' +
      '<input type="text" class="forge-input forge-input--mono" placeholder="magnet:?xt=urn:btih:…（必填）" required data-catalog-magnet style="width:100%;margin-top:6px">' +
      '<textarea class="forge-input forge-input--mono" data-catalog-acquisition rows="4" placeholder="选填：acquisition 附加 JSON（gap↔文件索引映射等），形如 {&quot;kind&quot;:&quot;torrent&quot;,&quot;url&quot;:&quot;magnet:…&quot;,&quot;file_index_by_gap&quot;:{&quot;S10E01&quot;:[1]}}" style="width:100%;margin-top:6px;font-size:12px"></textarea>' +
      '<div style="display:flex;gap:8px;margin-top:6px;align-items:center">' +
      '<button type="submit" class="forge-button forge-button--small forge-button--signal">登记直供候选</button>' +
      '<button type="button" class="forge-button forge-button--small forge-button--danger" data-catalog-remove="1">按 infohash 移除</button>' +
      '<input type="text" class="forge-input forge-input--mono" placeholder="infohash（移除用）" data-catalog-infohash style="width:220px">' +
      '</div></form></details>' +
      '</div>';
  }

  /* Direction 4: Diagnostics Vault */
  function diagnosticsBox(job, units){
    units = units || [];
    var warnings = (job.plan && job.plan.warnings) || [];
    var problemFiles = (job.plan && job.plan.problem_files) || [];
    var notices = (job.plan && job.plan.notices) || [];
    var error = job.error || "";
    var acceptanceErrors = units.filter(function(u){ return u.acceptance && u.acceptance.error; });
    var readbackVerified = (job.readback && job.readback.status === "verified");

    var hasIssues = error || problemFiles.length || warnings.length || notices.length || acceptanceErrors.length;
    if(!hasIssues && !readbackVerified){ return ""; }

    var errorHtml = "";
    if(error){
      errorHtml = '<div class="forge-diag-error"><span><strong>致命阻断：</strong>' + esc(error) +
        '</span><button type="button" class="forge-copy-btn" data-copy="' + esc(error) + '">⧉ 复制报错</button></div>';
    }

    var items = [];
    problemFiles.forEach(function(pf){
      items.push('<li class="forge-diag-item forge-diag-item--bad"><span><code>' + esc(pf) + '</code></span><span class="forge-chip forge-chip--danger">准入拒绝 (&lt;64KB / 非媒体 / 链接)</span></li>');
    });
    acceptanceErrors.forEach(function(u){
      items.push('<li class="forge-diag-item forge-diag-item--bad"><span><strong>' + esc(u.display_label || u.boundary_key) + '：</strong><code>' + esc(u.acceptance.error) + '</code></span><span class="forge-chip forge-chip--danger">验收失败</span></li>');
    });
    warnings.forEach(function(w){
      items.push('<li class="forge-diag-item forge-diag-item--warn"><span>' + esc(w) + '</span><span class="forge-chip forge-chip--alert">规约预警</span></li>');
    });
    notices.forEach(function(n){
      items.push('<li class="forge-diag-item"><span>' + esc(n) + '</span><span class="forge-chip">处理备注</span></li>');
    });

    var listHtml = items.length ? ('<ul class="forge-diag-list">' + items.join("") + '</ul>') : "";
    var readbackHtml = readbackVerified
      ? '<div class="forge-readback-stamp forge-readback-stamp--verified">✓ AList 远端路径与字节核验通过 (Fresh listing 证明入库生效)</div>'
      : '<div class="forge-readback-stamp">○ AList 远端核验待完成</div>';

    return '<div class="forge-diagnostics-box">' +
      '<div class="forge-diagnostics-box__head">' +
      '<div><strong>系统规约与运行诊断匣 (Diagnostics Vault)</strong>' +
      '<small style="display:block;color:var(--forge-faint);margin-top:2px">展示准入拒收、规范化预警与远端写后回读核验证据</small></div>' +
      readbackHtml +
      '</div>' +
      errorHtml +
      listHtml +
      '</div>';
  }

  /* Direction 3: WorkUnit Row with Confidence Radar and Margin */
  function unitRow(unit){
    var status = unitStatusKey(unit);
    var isUncertain = (unit.identity_status === "uncertain" || unit.reconciliation_outcome === "uncertain");
    var candidates = unit.candidate_identities || [];
    var candidateHtml = "";
    if(isUncertain && candidates.length){
      candidateHtml = '<div class="forge-candidates">' + candidates.map(function(candidate, cIdx){
        var season = (candidate.season != null && Number(candidate.season) > 0) ? String(candidate.season) : "";
        var confStr = (candidate.confidence != null) ? (Math.round(Number(candidate.confidence) * 100) + "%") : "";
        var marginStr = "";
        if(cIdx === 0 && candidates.length > 1 && candidate.confidence != null && candidates[1].confidence != null){
          var diff = Math.round((Number(candidate.confidence) - Number(candidates[1].confidence)) * 100);
          if(diff > 0){ marginStr = '<span class="forge-margin-tag">Δ +' + diff + '%</span>'; }
        }
        return '<button type="button" class="forge-candidate" data-action="confirm" data-confirm="1" data-job="' +
          esc(state.drawerJobId) + '" data-unit="' + esc(unit.work_unit_id) + '" data-tmdb="' +
          esc(candidate.tmdb_id) + '" data-type="' + esc(candidate.media_type) + '" data-season="' + esc(season) + '">' +
          '<b>' + esc(candidate.title || (candidate.media_type || "候选")) +
          (confStr ? (' <span style="color:var(--forge-ready);font-weight:700">[' + confStr + ']</span>') : '') + marginStr + '</b><span>' +
          esc((candidate.media_type === "movie" ? "电影" : "剧集") + " · #" + candidate.tmdb_id +
            (candidate.year ? " · " + candidate.year : "")) + "</span></button>";
      }).join("") + "</div>";
    }

    var manualHtml = "";
    if(isUncertain){
      var queryTitle = unit.display_label || unit.boundary_key || "";
      var tmdbSearchUrl = "https://www.themoviedb.org/search?query=" + encodeURIComponent(queryTitle);
      manualHtml = '<form class="forge-manual-confirm" data-manual-form="1" data-job="' + esc(state.drawerJobId) + '" data-unit="' + esc(unit.work_unit_id) + '">' +
        '<div class="forge-manual-confirm__label"><span>手动指定 TMDB 身份</span>' +
        '<small><a href="' + esc(tmdbSearchUrl) + '" target="_blank" rel="noopener">在 TMDB 检索此标题 ↗</a></small></div>' +
        '<div class="forge-manual-inputs">' +
        '<input type="number" class="forge-input forge-input--mono forge-input--id" placeholder="TMDB ID" min="1" required data-manual-tmdb>' +
        '<select class="forge-select" data-manual-type><option value="tv">剧集 (TV)</option><option value="movie">电影 (Movie)</option></select>' +
        '<input type="number" class="forge-input forge-input--mono forge-input--season" placeholder="季号 (选填)" min="1" data-manual-season>' +
        '<button type="submit" class="forge-button forge-button--small forge-button--signal">确认身份</button>' +
        '</div></form>';
    }

    var rulingHtml = "";
    if(unit.requires_content_expansion){
      var rulingScope = unit.boundary_key || "";
      var assignmentRows = "";
      for(var ri = 0; ri < 6; ri++){
        assignmentRows +=
          '<div style="display:flex;gap:6px;margin-bottom:4px">' +
          '<input type="text" class="forge-input forge-input--mono" placeholder="/BDMV/PLAYLIST/00051.mpls" data-ruling-playlist style="flex:1">' +
          '<input type="number" class="forge-input forge-input--mono" placeholder="集号" min="1" data-ruling-episode style="width:84px">' +
          '</div>';
      }
      var skippedRows = "";
      for(var si = 0; si < 3; si++){
        skippedRows +=
          '<input type="text" class="forge-input forge-input--mono" placeholder="/BDMV/PLAYLIST/00999.mpls（特典，选填）" data-ruling-skipped style="width:100%;margin-bottom:4px">';
      }
      rulingHtml = '<form class="forge-manual-confirm" data-ruling-form="1" data-job="' + esc(state.drawerJobId) + '" data-scope="' + esc(rulingScope) + '">' +
        '<div class="forge-manual-confirm__label"><span>光盘集号裁决（操作员通道）</span>' +
        '<small>镜像自身证不出保序映射时使用；每条裁决都会按 TMDB 集时长容差复核，与盘面时长矛盾的裁决会被拒绝。必须先暂停任务。</small></div>' +
        '<div class="forge-manual-inputs" style="flex-wrap:wrap">' +
        '<input type="number" class="forge-input forge-input--mono forge-input--season" placeholder="季号" min="1" required data-ruling-season>' +
        '</div>' +
        '<div style="margin-top:8px"><strong style="font-size:12px">playlist → 集号（至少一条）</strong>' + assignmentRows + '</div>' +
        '<div style="margin-top:8px"><strong style="font-size:12px">显式跳过的特典 playlist（选填）</strong>' + skippedRows + '</div>' +
        '<div style="margin-top:8px"><input type="text" class="forge-input" placeholder="依据说明（必填，如：分盘序号与官方片单核对）" required data-ruling-note style="width:100%"></div>' +
        '<div style="margin-top:8px"><button type="submit" class="forge-button forge-button--small forge-button--signal">提交裁决</button></div>' +
        '</form>';
    }

    var metaItems = [];
    if(unit.identity && unit.identity.title){
      var tmdbType = unit.identity.media_type === "movie" ? "movie" : "tv";
      var tmdbUrl = "https://www.themoviedb.org/" + tmdbType + "/" + encodeURIComponent(unit.identity.tmdb_id);
      var confBadge = "";
      if(unit.identity.confidence != null){
        var confVal = Math.round(Number(unit.identity.confidence) * 100);
        var confCls = confVal >= 85 ? "forge-conf--high" : (confVal >= 70 ? "forge-conf--mid" : "forge-conf--low");
        confBadge = ' <span class="forge-conf ' + confCls + '">' + confVal + '% 置信度</span>';
      }
      var idText = (unit.identity.media_type === "movie" ? "电影" : "剧集") + " · " +
        esc(unit.identity.title) + (unit.identity.year ? " (" + esc(unit.identity.year) + ")" : "") +
        ' · <a class="forge-unit__link" href="' + esc(tmdbUrl) + '" target="_blank" rel="noopener" title="在 TMDB 查看此条目">#' +
        esc(unit.identity.tmdb_id) + ' ↗</a>' +
        '<button type="button" class="forge-copy-btn" data-copy="' + esc(unit.identity.tmdb_id) + '" title="复制 TMDB ID">⧉</button>' +
        confBadge;
      metaItems.push('<span class="forge-unit__meta-item forge-unit__meta-item--ready"><strong>TMDB:</strong> ' + idText + '</span>');
    }
    if(unit.matched_work_root){
      metaItems.push('<span class="forge-unit__meta-item"><strong>目标根:</strong> <code>' + esc(unit.matched_work_root) + '</code><button type="button" class="forge-copy-btn" data-copy="' + esc(unit.matched_work_root) + '" title="复制路径">⧉</button></span>');
    }
    if(unit.disc_expansion){
      var de = unit.disc_expansion;
      metaItems.push('<span class="forge-unit__meta-item"><strong>光盘镜像展开:</strong> ' + esc(de.members || 0) + ' 个媒体文件' +
        (de.basis ? ' · 依据 ' + esc(de.basis) : '') + (de.season ? ' (S' + de.season + ')' : '') + '</span>');
    }
    if(unit.acceptance && unit.acceptance.error){
      metaItems.push('<span class="forge-unit__meta-item forge-unit__meta-item--danger"><strong>验收失败:</strong> ' + esc(unit.acceptance.error) + '</span>');
    }
    if(unit.gap_detail || (unit.gap_status && unit.gap_status !== "none")){
      metaItems.push('<span class="forge-unit__meta-item forge-unit__meta-item--alert"><strong>缺口账本:</strong> ' + esc(unit.gap_detail || unit.gap_status) + '</span>');
    }
    if(unit.attention){
      metaItems.push('<span class="forge-unit__meta-item forge-unit__meta-item--alert"><strong>需要关注:</strong> ' + esc(unit.attention) + '</span>');
    }

    var metaHtml = metaItems.length ? '<div class="forge-unit__meta">' + metaItems.join("") + '</div>' : "";

    return '<div class="forge-unit" data-unit-card="' + esc(unit.work_unit_id) + '"><div class="forge-unit__top"><div><strong>' +
      esc(unit.display_label || unit.boundary_key || "未命名单元") + '</strong><small>' +
      esc(unit.boundary_key || unit.work_unit_id || "") + "</small></div>" + chip(status) +
      "</div>" + metaHtml + candidateHtml + manualHtml + rulingHtml + "</div>";
  }

  function renderDrawer(){
    if(!state.drawerJobId){ return; }
    var job = jobById(state.drawerJobId);
    if(!job){ closeDrawer(); return; }
    var drawer = $(".forge-drawer");
    var drawerBody = $("#drawerBody");
    var prevScroll = drawer ? drawer.scrollTop : 0;
    if(document.activeElement && drawerBody && drawerBody.contains(document.activeElement)){
      if(document.activeElement.tagName === "INPUT" || document.activeElement.tagName === "SELECT"){
        return;
      }
    }
    var detail = stats(job);
    var view = unitData(job.id) || {};
    var units = view.units || [];
    var replenishment = state.replenishment[job.id] || {};
    var tierState = replenishment.tier_state || {};
    var tier = tierState.tier || tierFor(job);
    var isCurrentActive = isSelectedRoot(job);
    var shelfName = shelfLabels[job.target_shelf] || job.target_shelf || "未指定";
    var targetPath = job.target_work_path || job.target_root || "";

    $("#drawerTitle").textContent = jobTitle(job);
    $("#drawerSource").innerHTML = esc(jobSource(job)) +
      '<button type="button" class="forge-copy-btn" data-copy="' + esc(jobSource(job)) + '" title="复制来源路径">⧉</button>';

    var actions = "";
    if(!isCurrentActive && !isTerminalJob(job)){
      actions += '<button type="button" class="forge-button forge-button--signal" data-action="select" data-job="' + esc(job.id) + '">设为当前活跃任务</button>';
    } else if(isCurrentActive){
      actions += '<span class="forge-active-badge" style="padding:4px 0">● 当前选定的调度活跃根任务</span>';
    }
    if(isAttentionJob(job) || (job.phase && String(job.phase).indexOf("failed") === 0)){
      actions += '<button type="button" class="forge-button forge-button--signal" data-action="retry" data-job="' + esc(job.id) + '">重试任务</button>';
    }
    if(detail.gaps > 0 && !isWaiting(job) && !requiresRootJobSubtitleMigration(job)){
      actions += '<button type="button" class="forge-button forge-button--replenish" data-action="replenish" data-job="' + esc(job.id) + '">手动补源（' + detail.gaps + ' 个缺口）</button>';
    } else if(requiresRootJobSubtitleMigration(job)){
      actions += '<p class="forge-label">旧字幕任务需迁移到 RootJob；此处不会重新下载或写入。</p>';
    }
    if(isTerminalJob(job) || job.phase === "gaps_pending"){
      actions += '<button type="button" class="forge-button forge-button--quiet" data-action="consume" data-job="' + esc(job.id) + '">消费待刮削来源</button>';
    }
    if(isTerminalJob(job)){
      actions += '<button type="button" class="forge-button forge-button--quiet" data-action="cleanup" data-job="' + esc(job.id) + '">清理任务记录</button>';
    }
    if(isAttentionJob(job) || isTerminalJob(job)){
      actions += '<button type="button" class="forge-button forge-button--quiet" data-action="rebuild-uncertain" data-job="' + esc(job.id) + '" title="只重建 C 未决的单元边界">重建未决单元</button>';
      actions += '<button type="button" class="forge-button forge-button--quiet" data-action="rebuild-boundaries" data-job="' + esc(job.id) + '" title="零写入根重新推导全部边界（有写入事实的根会被拒绝）">重建边界</button>';
      actions += '<button type="button" class="forge-button forge-button--quiet" data-action="repair-artifacts" data-job="' + esc(job.id) + '" title="补写缺失的 NFO 与海报">修复元数据</button>';
    }
    if(isAttentionJob(job) || (job.phase && String(job.phase).indexOf("failed") === 0)){
      actions += '<button type="button" class="forge-button forge-button--quiet" data-action="reopen-orphan" data-job="' + esc(job.id) + '" title="IntakeSource 指向已删除任务时恢复绑定">解除孤儿绑定</button>';
    }
    if(isCancellableJob(job)){
      actions += '<button type="button" class="forge-button forge-button--danger" data-action="cancel" data-job="' + esc(job.id) + '">取消任务</button>';
    }

    var unitText = detail.total ? (detail.completed + " / " + detail.total + " 已完成") : "等待单元数据";
    var gapText = detail.gaps ? (detail.gaps + " 个开放缺口") : "没有开放缺口";
    var detailBar =
      '<div class="forge-detail-bar" aria-label="任务概况">' +
      '<div class="forge-detail-item"><span>状态</span>' + chip(displayPhase(job)) + "</div>" +
      '<div class="forge-detail-item"><span>货架</span><strong>' + esc(shelfName) + "</strong></div>" +
      '<div class="forge-detail-item"><span>作品单元</span><strong>' + esc(unitText) + "</strong></div>" +
      '<div class="forge-detail-item forge-detail-item--gaps"><span>缺口</span><strong>' + esc(gapText) + "</strong></div>" +
      (tier ? '<div class="forge-detail-item"><span>补源方式</span><strong>' + esc(tierLabels[tier] || tier) + "</strong></div>" : "") +
      (targetPath ? '<div class="forge-detail-item forge-detail-item--wide"><span>落盘目标根</span><code>' + esc(targetPath) + '</code><button type="button" class="forge-copy-btn" data-copy="' + esc(targetPath) + '" title="复制落盘路径">⧉</button></div>' : "") +
      "</div>";

    /* Direction 1: Pipeline Steps Render */
    var steps = pipelineSteps(job, units);
    var pipelineHtml =
      '<div class="forge-pipeline" aria-label="A→P 主流程阶段导轨">' + steps.map(function(s){
        return '<div class="forge-step is-' + s.state + '" title="' + esc(s.desc) + '">' +
          '<div class="forge-step__head"><span class="forge-step__num">' + esc(s.num) + '</span><span class="forge-step__dot"></span></div>' +
          '<strong>' + esc(s.title) + '</strong><small>' + esc(s.text) + '</small>' +
          '</div>';
      }).join("") + '</div>';

    /* Direction 4: Diagnostics Vault */
    var diagHtml = diagnosticsBox(job, units);

    /* Direction 2: Replenishment Panel */
    var replenishHtml = replenishmentPanel(job, detail, replenishment);

    /* Direction 3: Unit Filter & Render */
    var currentUnitFilter = state.unitFilter || "all";
    var filterCounts = {all:units.length, attention:0, gaps:0, completed:0};
    units.forEach(function(u){
      if(u.identity_status === "uncertain" || u.reconciliation_outcome === "uncertain" || !!u.attention){
        filterCounts.attention++;
      }
      if(u.gap_status === "has_gaps" || (u.gap_detail && String(u.gap_detail).indexOf("0") !== 0 && u.gap_status !== "none")){
        filterCounts.gaps++;
      }
      if(u.lane_status === "completed" || (u.acceptance && u.acceptance.outcome === "verified") || u.reconciliation_outcome === "duplicate_complete"){
        filterCounts.completed++;
      }
    });

    var filteredUnits = units.filter(function(u){
      if(currentUnitFilter === "attention"){
        return u.identity_status === "uncertain" || u.reconciliation_outcome === "uncertain" || !!u.attention;
      }
      if(currentUnitFilter === "gaps"){
        return u.gap_status === "has_gaps" || (u.gap_detail && String(u.gap_detail).indexOf("0") !== 0 && u.gap_status !== "none");
      }
      if(currentUnitFilter === "completed"){
        return u.lane_status === "completed" || (u.acceptance && u.acceptance.outcome === "verified") || u.reconciliation_outcome === "duplicate_complete";
      }
      return true;
    });

    var filterPillsHtml =
      '<div class="forge-unit-filter-bar" role="tablist" aria-label="作品单元筛选">' +
      '<button type="button" class="forge-unit-pill' + (currentUnitFilter === "all" ? " is-active" : "") + '" data-unit-filter="all">全部 <span class="forge-filter-count">' + filterCounts.all + '</span></button>' +
      '<button type="button" class="forge-unit-pill' + (currentUnitFilter === "attention" ? " is-active" : "") + '" data-unit-filter="attention">需人工确认 <span class="forge-filter-count">' + filterCounts.attention + '</span></button>' +
      '<button type="button" class="forge-unit-pill' + (currentUnitFilter === "gaps" ? " is-active" : "") + '" data-unit-filter="gaps">存在缺口 <span class="forge-filter-count">' + filterCounts.gaps + '</span></button>' +
      '<button type="button" class="forge-unit-pill' + (currentUnitFilter === "completed" ? " is-active" : "") + '" data-unit-filter="completed">已入库完成 <span class="forge-filter-count">' + filterCounts.completed + '</span></button>' +
      '</div>';

    var unitHtml = filteredUnits.length
      ? filteredUnits.map(unitRow).join("")
      : '<div class="forge-empty">当前分类下没有作品单元</div>';

    drawerBody.innerHTML =
      (actions ? '<div class="forge-drawer__actions">' + actions + "</div>" : "") +
      detailBar +
      pipelineHtml +
      diagHtml +
      replenishHtml +
      '<p class="forge-label">作品单元（共 ' + units.length + ' 个）</p>' +
      filterPillsHtml +
      '<div class="forge-unit-list">' + unitHtml + "</div>";

    if(drawer && prevScroll > 0){ drawer.scrollTop = prevScroll; }
  }
  function closeDrawer(){
    state.drawerJobId = null;
    $("#drawerBackdrop").classList.remove("is-open");
    $("#drawerBackdrop").setAttribute("aria-hidden","true");
  }
  function openCreate(presetSource){
    if(presetSource){
      state.selectedSource = presetSource;
    } else if(!state.selectedSource){
      var sources = waitingSources();
      if(sources.length === 1){
        state.selectedSource = sources[0].canonical_path;
      }
    }
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
    })}).then(function(){ return api("/api/control/resume",{method:"POST",body:"{}"}); }).then(function(){
      closeCreate();
      return load();
    }).then(function(){ toast("任务已建立并开始"); }).catch(function(error){
      toast(error.message,true);
    }).finally(function(){ setBusy(false); });
  }
  function confirmCandidate(button){
    var payload = {
      media_type:button.dataset.type,
      tmdb_id:Number(button.dataset.tmdb)
    };
    if(button.dataset.season && Number(button.dataset.season) > 0){
      payload.season = Number(button.dataset.season);
    }
    button.disabled = true;
    var originalText = button.innerHTML;
    button.innerHTML = "<b>提交中…</b>";
    send("/api/jobs/" + encodeURIComponent(button.dataset.job) + "/work-units/" +
      encodeURIComponent(button.dataset.unit) + "/confirm",payload,"已确认作品身份").finally(function(){
        button.disabled = false;
        button.innerHTML = originalText;
      });
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
    send(action.path,action.payload || {},action.successMessage);
  }
  function selectJob(jobId){
    send("/api/control/select",{root_job_id:jobId},"已设为当前活跃任务");
  }
  function retryJob(jobId){
    askForConfirmation({
      title:"重试任务",
      message:"确定重新执行此任务吗？",
      hint:"系统将从中断处或当前来源状态重新推进。",
      confirmLabel:"开始重试",
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/retry",
      payload:{},
      successMessage:"已触发任务重试"
    });
  }
  function consumeSource(jobId){
    askForConfirmation({
      title:"消费待刮削来源",
      message:"确定彻底清理此任务的待刮削源目录吗？",
      hint:"正式库文件不受影响；缺口账本仍为终态凭据。将安全删除待刮削目录下的已入库文件及残留。",
      confirmLabel:"清理来源树",
      danger:true,
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/consume-source",
      payload:{},
      successMessage:"已消费并清理待刮削来源树"
    });
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
      hint:"系统会检查暂停状态、任务归属和每一阶的安全限制。",
      confirmLabel:"开始补源",
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/replenish"
    });
  }
  function cancelJob(jobId){
    var job = jobById(jobId);
    if(!isCancellableJob(job)){
      toast("该任务已经结束，不能取消",true);
      return;
    }
    askForConfirmation({
      title:"取消任务",
      message:"确定取消“" + jobTitle(job) + "”吗？",
      hint:"正在执行的步骤会在下一个安全边界停止；不会开始新的写入、移动或提交。",
      confirmLabel:"确认取消",
      danger:true,
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/cancel",
      successMessage:"已请求取消任务"
    });
  }
  function rebuildUncertainUnits(jobId){
    askForConfirmation({
      title:"重建未决单元边界",
      message:"只重新推导身份未决（uncertain）单元的边界。",
      hint:"适用于引擎修复后重跑同一形状的来源；有写入事实的单元不受影响。需要先暂停任务。",
      confirmLabel:"重建未决单元",
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/rebuild-uncertain-units",
      payload:{},
      successMessage:"已重建未决单元边界"
    });
  }
  function rebuildBoundaries(jobId){
    askForConfirmation({
      title:"重建任务边界",
      message:"零写入的根重新推导全部工作单元边界。",
      hint:"已有任何写入事实（carrier/车道/缺口/已决 D）的根会被拒绝——那是 cancel + cleanup + 重建的领地。",
      confirmLabel:"重建边界",
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/rebuild-boundaries",
      payload:{},
      successMessage:"已重建任务边界"
    });
  }
  function reopenOrphan(jobId){
    askForConfirmation({
      title:"解除孤儿绑定",
      message:"此来源的 IntakeSource 指向已删除的任务，恢复其建任务能力。",
      hint:"需要被删任务的原始 JSON 作证据（备份或墓碑里找）；没有证据时服务器会拒绝。",
      confirmLabel:"解除孤儿绑定",
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/reopen-orphan",
      payload:{},
      successMessage:"已解除孤儿绑定"
    });
  }
  function repairArtifacts(jobId){
    askForConfirmation({
      title:"修复元数据",
      message:"为已入库的作品补写缺失的 NFO 与海报。",
      hint:"只补缺失项，不覆盖已有文件；写后走同一回读核验。",
      confirmLabel:"修复元数据",
      path:"/api/jobs/" + encodeURIComponent(jobId) + "/repair-artifacts",
      payload:{},
      successMessage:"已提交元数据修复"
    });
  }
  function clearOrphanSelection(){
    askForConfirmation({
      title:"清除孤儿选择",
      message:"清除指向已删除任务的调度选择？",
      hint:"仅当当前选中根指向已不存在的任务时使用；不清除媒体与账本。",
      confirmLabel:"清除选择",
      path:"/api/control/clear-orphan-selection",
      payload:{},
      successMessage:"已清除孤儿选择"
    });
  }
  function submitRulingForm(event){
    var form = event.target.closest("[data-ruling-form]");
    if(!form){ return; }
    event.preventDefault();
    var jobId = form.dataset.job;
    var scopePath = form.dataset.scope;
    var seasonInput = form.querySelector("[data-ruling-season]");
    var noteInput = form.querySelector("[data-ruling-note]");
    var season = Number(seasonInput ? seasonInput.value : 0);
    if(!season || season <= 0){
      toast("请输入有效的季号",true);
      return;
    }
    var assignments = [];
    var playlistInputs = form.querySelectorAll("[data-ruling-playlist]");
    var episodeInputs = form.querySelectorAll("[data-ruling-episode]");
    for(var i = 0; i < playlistInputs.length; i++){
      var playlist = (playlistInputs[i].value || "").trim();
      var episode = Number(episodeInputs[i] ? episodeInputs[i].value : 0);
      if(!playlist && !episode){ continue; }
      if(!playlist || !episode || episode <= 0){
        toast("裁决行必须同时填 playlist 路径和正整数集号",true);
        return;
      }
      assignments.push({ playlist_inner_path: playlist, episode: episode });
    }
    if(!assignments.length){
      toast("至少填一条 playlist → 集号 裁决",true);
      return;
    }
    var skipped = [];
    form.querySelectorAll("[data-ruling-skipped]").forEach(function(input){
      var value = (input.value || "").trim();
      if(value){ skipped.push(value); }
    });
    var payload = {
      scope_path: scopePath,
      season: season,
      assignments: assignments,
      operator: "web-console",
      note: (noteInput ? noteInput.value : "").trim()
    };
    if(!payload.note){
      toast("请填写依据说明",true);
      return;
    }
    if(skipped.length){ payload.skipped = skipped; }
    var submitBtn = form.querySelector('button[type="submit"]');
    if(submitBtn){
      submitBtn.disabled = true;
      submitBtn.textContent = "提交中…";
    }
    api("/api/jobs/" + encodeURIComponent(jobId) + "/file-disc-ruling",{
      method:"POST", body: JSON.stringify(payload)
    }).then(function(){
      toast("已登记光盘集号裁决");
      return load();
    }).then(function(){ renderDrawer(); }).catch(function(error){
      toast(error.message,true);
    }).finally(function(){
      if(submitBtn){
        submitBtn.disabled = false;
        submitBtn.textContent = "提交裁决";
      }
    });
  }
  function submitCatalogForm(event){
    var form = event.target.closest("[data-catalog-form]");
    if(!form){ return; }
    event.preventDefault();
    var isRemove = event.submitter && event.submitter.getAttribute("data-catalog-remove") === "1";
    var tmdbInput = form.querySelector("[data-catalog-tmdb]");
    var tmdbId = Number(tmdbInput ? tmdbInput.value : 0);
    if(!tmdbId || tmdbId <= 0){
      toast("请输入有效的 TMDB 正整数 ID",true);
      return;
    }
    var payload = { tmdb_id: tmdbId };
    if(isRemove){
      var hashInput = form.querySelector("[data-catalog-infohash]");
      var infohash = (hashInput ? hashInput.value : "").trim();
      if(!infohash){
        toast("移除需要 infohash",true);
        return;
      }
      payload.action = "remove";
      payload.infohash = infohash;
    } else {
      var releaseInput = form.querySelector("[data-catalog-release]");
      var magnetInput = form.querySelector("[data-catalog-magnet]");
      var resolutionSelect = form.querySelector("[data-catalog-resolution]");
      var acquisitionText = form.querySelector("[data-catalog-acquisition]");
      var release = (releaseInput ? releaseInput.value : "").trim();
      var magnet = (magnetInput ? magnetInput.value : "").trim();
      if(!release || !magnet){
        toast("发布名与 magnet 链接必填",true);
        return;
      }
      var candidate = {
        provider: "magnet",
        release_name: release,
        locator: magnet.indexOf("torrent:") === 0 ? magnet : ("torrent:" + magnet),
        resolution: resolutionSelect ? resolutionSelect.value : "unknown",
      };
      var extraText = (acquisitionText ? acquisitionText.value : "").trim();
      if(extraText){
        try {
          candidate.acquisition = JSON.parse(extraText);
        } catch(error){
          toast("acquisition 附加 JSON 无法解析: " + error.message,true);
          return;
        }
      }
      payload.action = "add";
      payload.candidate = candidate;
    }
    var submitBtn = event.submitter || form.querySelector('button[type="submit"]');
    if(submitBtn){ submitBtn.disabled = true; }
    api("/api/replenishment/catalog",{
      method:"POST", body: JSON.stringify(payload)
    }).then(function(result){
      toast(isRemove
        ? (result.removed ? "已移除直供候选" : "未找到该 infohash")
        : "已登记直供候选（" + (result.project_candidates || 1) + " 条在册）");
    }).catch(function(error){
      toast(error.message,true);
    }).finally(function(){
      if(submitBtn){ submitBtn.disabled = false; }
    });
  }
  function browseQuickLinks(){
    var links = [
      { label:"番剧", path:"/quark/影视/番剧" },
      { label:"欧美剧", path:"/quark/影视/欧美剧" },
      { label:"电影", path:"/quark/影视/电影" },
      { label:"待刮削", path:"/quark/影视/待刮削" },
      { label:"展开 staging", path:"/quark/影视/ScrapeFlow/展开" },
      { label:"补源 staging", path:"/quark/影视/ScrapeFlow/补源" },
      { label:"归档", path:"/quark/影视/ScrapeFlow/归档" }
    ];
    return links.map(function(link){
      return '<button type="button" class="forge-button forge-button--small forge-button--quiet" data-browse-path="' +
        esc(link.path) + '">' + esc(link.label) + '</button>';
    }).join(" ");
  }
  function loadBrowse(path, refresh){
    if(state.browseBusy){ return; }
    state.browseBusy = true;
    var target = path || state.browsePath || "/quark/影视";
    var query = "?path=" + encodeURIComponent(target) + (refresh ? "&refresh=1" : "");
    api("/api/browse" + query).then(function(data){
      state.browsePath = String(data.path || target);
      state.browseData = data;
      renderBrowse();
    }).catch(function(error){
      toast(error.message,true);
      $("#browseBoard").innerHTML = '<div class="forge-empty">读取失败：' + esc(error.message) + '</div>';
    }).finally(function(){ state.browseBusy = false; });
  }
  function renderBrowse(){
    var board = $("#browseBoard");
    if(!board){ return; }
    $("#browseQuickLinks").innerHTML = browseQuickLinks();
    var data = state.browseData;
    if(!data){
      board.innerHTML = '<div class="forge-empty">正在读取</div>';
      return;
    }
    var crumbs = '<button type="button" class="forge-button forge-button--small forge-button--quiet" data-browse-path="/">/</button>';
    var cumulative = "";
    String(data.path || "").split("/").forEach(function(part){
      if(!part){ return; }
      cumulative += "/" + part;
      crumbs += ' / <button type="button" class="forge-button forge-button--small forge-button--quiet" data-browse-path="' +
        esc(cumulative) + '">' + esc(part) + '</button>';
    });
    if(data.parent){
      crumbs = '<button type="button" class="forge-button forge-button--small forge-button--quiet" data-browse-path="' +
        esc(data.parent) + '">↑ 上一级</button> · ' + crumbs;
    }
    $("#browseCrumbs").innerHTML = crumbs;
    function sizeText(value){
      var size = Number(value);
      if(!isFinite(size) || size <= 0){ return "-"; }
      if(size >= 1024 * 1024 * 1024){ return (size / (1024 * 1024 * 1024)).toFixed(2) + " GB"; }
      if(size >= 1024 * 1024){ return (size / (1024 * 1024)).toFixed(1) + " MB"; }
      if(size >= 1024){ return (size / 1024).toFixed(0) + " KB"; }
      return size + " B";
    }
    var rows = (data.directories || []).map(function(entry){
      return '<div class="forge-job-row" data-browse-path="' + esc(entry.path) + '" style="cursor:pointer">' +
        '<span><strong>📁 ' + esc(entry.name) + '</strong></span><span>目录</span><span>-</span><span>' +
        esc(entry.modified || "-") + '</span></div>';
    }).concat((data.files || []).map(function(entry){
      return '<div class="forge-job-row" style="cursor:default"><span>' + esc(entry.name) + '</span>' +
        '<span>文件</span><span>' + sizeText(entry.size) + '</span><span>' + esc(entry.modified || "-") + '</span></div>';
    }));
    board.innerHTML = rows.length ? rows.join("") :
      '<div class="forge-empty">此目录为空</div>';
  }
  function handleManualConfirm(event){
    var rulingForm = event.target.closest("[data-ruling-form]");
    if(rulingForm){
      submitRulingForm(event);
      return;
    }
    var catalogForm = event.target.closest("[data-catalog-form]");
    if(catalogForm){
      submitCatalogForm(event);
      return;
    }
    var form = event.target.closest("[data-manual-form]");
    if(!form){ return; }
    event.preventDefault();
    var jobId = form.dataset.job;
    var unitId = form.dataset.unit;
    var tmdbInput = form.querySelector("[data-manual-tmdb]");
    var typeInput = form.querySelector("[data-manual-type]");
    var seasonInput = form.querySelector("[data-manual-season]");
    var tmdbId = Number(tmdbInput ? tmdbInput.value : 0);
    if(!tmdbId || tmdbId <= 0){
      toast("请输入有效的 TMDB 正整数 ID",true);
      return;
    }
    var mediaType = typeInput ? typeInput.value : "tv";
    var payload = {
      media_type: mediaType,
      tmdb_id: tmdbId
    };
    var seasonVal = seasonInput ? seasonInput.value : "";
    if(mediaType === "tv" && seasonVal != null && seasonVal.trim() !== "" && Number(seasonVal) > 0){
      payload.season = Number(seasonVal);
    }
    var submitBtn = form.querySelector('button[type="submit"]');
    if(submitBtn){
      submitBtn.disabled = true;
      submitBtn.textContent = "提交中…";
    }
    send("/api/jobs/" + encodeURIComponent(jobId) + "/work-units/" +
      encodeURIComponent(unitId) + "/confirm", payload, "已确认作品身份").finally(function(){
        if(submitBtn){
          submitBtn.disabled = false;
          submitBtn.textContent = "确认身份";
        }
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
  function send(path,body,successMessage){
    if(state.busy){ return Promise.resolve(); }
    setBusy(true);
    return api(path,{method:"POST",body:JSON.stringify(body || {})}).then(function(){
      return load();
    }).then(function(){
      if(successMessage){ toast(successMessage); }
    }).catch(function(error){
      toast(error.message,true);
    }).finally(function(){ setBusy(false); });
  }
  $("#controlButton").addEventListener("click",function(){
    if(state.busy){ return; }
    send(systemPaused() ? "/api/control/resume" : "/api/control/pause",{});
  });
  $("#createButton").addEventListener("click",function(){ openCreate(); });
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
      renderAll();
    });
  });
  function sourceClick(event){
    var createBtn = event.target.closest("[data-create-source]");
    if(createBtn){
      event.preventDefault();
      event.stopPropagation();
      openCreate(createBtn.dataset.createSource);
      return;
    }
    var item = event.target.closest("[data-source]");
    if(!item){ return; }
    state.selectedSource = item.dataset.source;
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
    var copyBtn = event.target.closest("[data-copy]");
    if(copyBtn){
      event.preventDefault();
      event.stopPropagation();
      copyText(copyBtn.dataset.copy, copyBtn);
      return;
    }
    var unitFilterBtn = event.target.closest("[data-unit-filter]");
    if(unitFilterBtn){
      event.preventDefault();
      event.stopPropagation();
      state.unitFilter = unitFilterBtn.dataset.unitFilter;
      renderDrawer();
      return;
    }
    var action = event.target.closest("[data-action]");
    if(!action){ return; }
    event.preventDefault();
    event.stopPropagation();
    var name = action.dataset.action;
    if(name === "open"){ openDrawer(action.dataset.job); }
    if(name === "select"){ selectJob(action.dataset.job); }
    if(name === "retry"){ retryJob(action.dataset.job); }
    if(name === "consume"){ consumeSource(action.dataset.job); }
    if(name === "cleanup"){ cleanupJob(action.dataset.job); }
    if(name === "replenish"){ replenishJob(action.dataset.job); }
    if(name === "cancel"){ cancelJob(action.dataset.job); }
    if(name === "rebuild-uncertain"){ rebuildUncertainUnits(action.dataset.job); }
    if(name === "rebuild-boundaries"){ rebuildBoundaries(action.dataset.job); }
    if(name === "reopen-orphan"){ reopenOrphan(action.dataset.job); }
    if(name === "repair-artifacts"){ repairArtifacts(action.dataset.job); }
    if(name === "confirm"){ confirmCandidate(action); }
  }
  $("#jobBoard").addEventListener("click",function(event){
    var switchBtn = event.target.closest("[data-switch-view]");
    if(switchBtn){
      state.view = switchBtn.dataset.switchView;
      saveView(state.view);
      renderAll();
      return;
    }
    var resetBtn = event.target.closest("[data-reset-filter]");
    if(resetBtn){
      state.filter = resetBtn.dataset.resetFilter;
      renderJobs();
      return;
    }
    actionClick(event);
    if(event.defaultPrevented){ return; }
    var row = event.target.closest("[data-open-job]");
    if(row){ openDrawer(row.dataset.openJob); }
  });
  $("#jobBoard").addEventListener("keydown",function(event){
    if(event.key === "Enter" || event.key === " "){
      var row = event.target.closest("[data-open-job]");
      if(row && event.target === row){
        event.preventDefault();
        openDrawer(row.dataset.openJob);
      }
    }
  });
  $("#attentionList").addEventListener("click",actionClick);
  $("#drawerBody").addEventListener("click",actionClick);
  $("#drawerBody").addEventListener("submit",handleManualConfirm);
  $("#orphanButton").addEventListener("click",function(){
    if(state.busy){ return; }
    clearOrphanSelection();
  });
  $("#browseRefresh").addEventListener("click",function(){
    loadBrowse(null, true);
  });
  function browseClick(event){
    var target = event.target.closest("[data-browse-path]");
    if(!target){ return; }
    event.preventDefault();
    event.stopPropagation();
    loadBrowse(target.dataset.browsePath, false);
  }
  $("#browseBoard").addEventListener("click",browseClick);
  $("#browseCrumbs").addEventListener("click",browseClick);
  var quickLinksEl = $("#browseQuickLinks");
  if(quickLinksEl){ quickLinksEl.addEventListener("click",browseClick); }
  $("#drawerBody").addEventListener("change",function(event){
    var typeSelect = event.target.closest("[data-manual-type]");
    if(typeSelect){
      var form = typeSelect.closest("[data-manual-form]");
      var seasonInput = form ? form.querySelector("[data-manual-season]") : null;
      if(seasonInput){
        if(typeSelect.value === "movie"){
          seasonInput.disabled = true;
          seasonInput.placeholder = "电影无季号";
          seasonInput.value = "";
        } else {
          seasonInput.disabled = false;
          seasonInput.placeholder = "季号 (选填)";
        }
      }
    }
  });
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
</html>"""


def dashboard_html() -> bytes:
    return DASHBOARD_HTML.encode("utf-8")
