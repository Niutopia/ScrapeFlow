"""Dependency-free same-origin Web dashboard for the local ScrapeFlow API.

Visual language follows the Media Engine v4 mock (dark industrial palette,
sidebar navigation, task rows with progress, modal-based task creation).
All functionality stays inline: no external assets, no new API surface.
"""

DASHBOARD_HTML = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ScrapeFlow</title>
<style>
:root{
  --bg:#171714;--side:#1a1a17;--panel:#1f1f1b;--panel2:#24231f;--line:#35332d;
  --text:#ece8de;--muted:#8a867e;--faint:#5e5b55;--accent:#d36b47;--ok:#8da379;--warn:#d0a05b;
}
*{box-sizing:border-box}
html,body{margin:0;min-height:100%;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
button,input{font:inherit}
button{color:inherit}
body{overflow:hidden}
.app{height:100vh;display:grid;grid-template-columns:190px minmax(0,1fr);grid-template-rows:46px minmax(0,1fr)}
.top{grid-column:1/3;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 14px;background:#191916}
.brand{display:flex;align-items:center;gap:9px}
.mark{width:14px;height:14px;border:2px solid var(--accent);border-left-width:5px}
.brand strong{font-size:12px}.brand span{font-size:10px;color:var(--faint)}
.top-actions{display:flex;align-items:center;gap:8px}
.health{font-size:9px;color:#77736b;display:flex;align-items:center;gap:6px}
.health i{width:6px;height:6px;border-radius:50%;background:#656159}
.health i.ok{background:var(--ok)}
.sidebar{grid-row:2;background:var(--side);border-right:1px solid var(--line);padding:15px 10px;position:relative}
.side-title{font-size:8.5px;color:#5f5b54;text-transform:uppercase;letter-spacing:.12em;padding:0 8px 6px;margin-top:5px}
.nav{height:32px;width:100%;border:0;background:transparent;text-align:left;padding:0 8px;display:grid;grid-template-columns:18px 1fr auto;align-items:center;gap:6px;color:#918d84;font-size:10.5px;cursor:pointer;border-left:2px solid transparent}
.nav:hover{background:#22211e}
.nav.active{background:#27251f;color:#f0ece2;border-left-color:var(--accent)}
.nav em{font-style:normal;font-size:8.5px;color:#66625b}
.spacer{height:14px}
.side-bottom{position:absolute;bottom:14px;left:10px;right:10px}
.main{grid-column:2;grid-row:2;overflow:auto;background:#1c1c19}
.page{max-width:980px;margin:0 auto;padding:34px 34px 60px}
.page-head{display:flex;align-items:flex-end;justify-content:space-between;margin-bottom:26px}
.page-head h1{font-size:25px;margin:0 0 7px;font-weight:650;letter-spacing:-.035em}
.page-head p{margin:0;color:#77736b;font-size:10.5px}
.btn{height:30px;border:1px solid #454138;background:#22211d;color:#b9b4aa;padding:0 11px;font-size:9.5px;cursor:pointer}
.btn:hover{background:#292722}
.btn.primary{background:var(--accent);border-color:var(--accent);color:#1b1714;font-weight:650}
.btn.danger{color:#d89b76;border-color:#5a3b2f}
.btn:disabled{opacity:.5;cursor:wait}
.summary-line{display:flex;gap:22px;margin-bottom:22px;color:#77736b;font-size:9.5px}
.summary-line b{color:#cbc6bb;font-weight:600}
.summary-line .warn{color:var(--warn)}
.section{margin-bottom:26px}
.section-title{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
.section-title h2{font-size:11px;margin:0;color:#8a867e;font-weight:600;letter-spacing:.04em}
.section-title span{font-size:8.5px;color:#5f5b54}
.task-list{border-top:1px solid var(--line)}
.task{display:grid;grid-template-columns:minmax(0,1fr) 130px 90px;gap:16px;align-items:center;min-height:78px;border-bottom:1px solid var(--line);padding:0 4px}
.task:hover{background:#20201c}
.task-main strong{font-size:13px;font-weight:600;display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.task-main p{font-size:9px;color:#66625b;margin:5px 0 0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.task-progress{height:2px;background:#34322c;margin-top:10px;max-width:460px}
.task-progress i{display:block;height:100%;background:var(--accent)}
.task-meta{font-size:9px;color:#77736b;line-height:1.7}
.task-state{text-align:right;font-size:9.5px}
.run{color:#aaa69d}.done{color:var(--ok)}.attention{color:var(--warn)}.row-error{color:#c46a4a;font-size:8.5px;margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.notice{margin-top:14px;border-left:2px solid var(--warn);background:#231f19;padding:11px 13px;display:flex;align-items:center;justify-content:space-between;gap:14px}
.notice strong{font-size:9.5px;color:#caa46d}
.notice p{font-size:8.8px;color:#776958;margin:4px 0 0}
.notice button{border:0;background:transparent;color:#d89b76;font-size:9px;cursor:pointer}
.waiting-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.card{background:var(--panel);border:1px solid var(--line);padding:13px 14px}
.card strong{display:block;font-size:11px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card small{display:block;font-size:8.5px;color:#615e57;margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.choicebox{border:1px solid var(--line);margin-top:12px}
.choice{min-height:48px;border-bottom:1px solid var(--line);display:grid;grid-template-columns:17px 1fr;align-items:center;padding:0 10px;cursor:pointer}
.choice:last-child{border-bottom:0}
.choice.active{background:#29261f}
.choice i{width:9px;height:9px;border-radius:50%;border:1px solid #5d5952}
.choice.active i{border:3px solid var(--accent)}
.choice b{display:block;font-size:9.8px}
.choice small{display:block;font-size:8.2px;color:#66625b;margin-top:3px}
.segment{display:grid;grid-template-columns:repeat(3,1fr);border:1px solid var(--line);margin-top:9px}
.segment button{height:28px;border:0;border-right:1px solid var(--line);background:#1a1a17;color:#77736b;font-size:9px;cursor:pointer}
.segment button:last-child{border:0}
.segment button.active{background:#33251f;color:#efb294}
.choose-label{font-size:8.5px;color:#67635c;margin:12px 0 8px}
.empty{padding:24px;text-align:center;border:1px dashed #35332d;color:#5e5b55;font-size:9px}
.modalback{position:fixed;inset:0;background:rgba(10,10,8,.72);display:none;align-items:center;justify-content:center;z-index:30}
.modalback.show{display:flex}
.modal{width:min(520px,calc(100vw - 32px));background:#1e1e1a;border:1px solid #464239}
.modal-head{height:44px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 13px}
.modal-head strong{font-size:11px}
.close{border:0;background:transparent;color:#747068;font-size:17px;cursor:pointer}
.modal-body{padding:13px;max-height:64vh;overflow:auto}
.label{font-size:8.5px;color:#67635c;margin-bottom:7px}
.dirlist{border:1px solid var(--line)}
.dir{width:100%;min-height:44px;border:0;border-bottom:1px solid var(--line);background:#1b1b18;display:grid;grid-template-columns:18px 1fr auto;align-items:center;text-align:left;padding:0 10px;color:#9a968d;cursor:pointer}
.dir:last-child{border-bottom:0}
.dir.active{background:#29261f}
.dir b{display:block;font-size:9.8px;color:#cac5bb;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.dir small{display:block;font-size:8.2px;color:#5e5a53;margin-top:3px}
.dir em{font-style:normal;color:var(--accent);font-size:9px}
.modal-foot{height:47px;border-top:1px solid var(--line);display:flex;align-items:center;justify-content:flex-end;gap:6px;padding:0 13px}
.toast{position:fixed;left:50%;bottom:22px;transform:translate(-50%,8px);background:#e7e1d7;color:#171714;padding:8px 11px;font-size:9px;opacity:0;transition:.2s;z-index:50;max-width:80vw}
.toast.show{opacity:1;transform:translate(-50%,0)}
.toast.bad{background:#5a2c22;color:#f0d9cf}
@media(max-width:760px){
  body{overflow:auto}
  .app{display:block;height:auto}
  .top{position:sticky;top:0;z-index:10}
  .sidebar{display:none}
  .main{overflow:visible}
  .page{padding:28px 18px 50px}
  .task{grid-template-columns:1fr 90px}
  .task-meta{display:none}
  .waiting-grid{grid-template-columns:1fr}
}
</style>
</head>
<body>
<div class="app">
  <header class="top">
    <div class="brand"><div class="mark"></div><strong>ScrapeFlow</strong><span>影视整理</span></div>
    <div class="top-actions">
      <span class="health"><i id="healthDot"></i><span id="healthText">连接中</span></span>
      <button id="refresh" class="btn">刷新</button>
      <button id="control" class="btn primary">载入中</button>
    </div>
  </header>

  <aside class="sidebar">
    <div class="side-title">任务</div>
    <button class="nav active" data-tab="active"><span>▸</span><span>进行中</span><em id="countActive">0</em></button>
    <button class="nav" data-tab="attention"><span>!</span><span>需要关注</span><em id="countAttention">0</em></button>
    <button class="nav" data-tab="history"><span>✓</span><span>历史记录</span><em id="countHistory">0</em></button>
    <button class="nav" data-tab="all"><span>▤</span><span>全部</span><em id="countAll">0</em></button>

    <div class="spacer"></div>
    <div class="side-title">媒体</div>
    <button class="nav" id="openCreate"><span>＋</span><span>创建任务</span></button>
    <button class="nav" data-tab="waiting"><span>▣</span><span>等待选择货架</span><em id="countWaiting">0</em></button>

    <div class="side-bottom">
      <button class="nav" id="sideControl"><span>⚙</span><span>运行控制</span></button>
    </div>
  </aside>

  <main class="main">
    <section class="page">
      <div class="page-head">
        <div>
          <h1>任务</h1>
          <p>资源放进待刮削，选择来源与货架建立任务，其余交给系统。</p>
        </div>
        <div class="top-actions">
          <button class="btn" id="topRefresh">刷新</button>
          <button class="btn primary" id="topCreate">＋ 创建任务</button>
        </div>
      </div>

      <div class="summary-line" id="summary">
        <span><b id="sumWaiting">0</b> 等待选择</span>
        <span><b id="sumActive">0</b> 处理中</span>
        <span><b class="warn" id="sumIssues">0</b> 需要关注</span>
      </div>

      <div class="section" id="waitingSection">
        <div class="section-title"><h2>等待选择货架</h2><span id="waitingCount">0 个任务</span></div>
        <div id="waiting" class="waiting-grid"><div class="empty">载入中…</div></div>
      </div>

      <div class="section" id="confirmSection">
        <div class="section-title"><h2>需要确认</h2><span id="confirmCount">0 个单元</span></div>
        <div id="confirm" class="waiting-grid"><div class="empty">载入中…</div></div>
      </div>

      <div class="section" id="workspaceSection">
        <div class="section-title"><h2>任务记录</h2><span id="jobCount">0 个任务</span></div>
        <div id="jobList" class="task-list"><div class="empty">正在载入任务…</div></div>
      </div>
    </section>
  </main>
</div>

<div class="modalback" id="createModal">
  <div class="modal">
    <div class="modal-head"><strong>创建任务</strong><button class="close" data-close="createModal">×</button></div>
    <div class="modal-body">
      <div class="label">选择待刮削目录</div>
      <div class="dirlist" id="intakeList"><div class="empty">正在载入来源…</div></div>
      <div class="choose-label">这个来源整理到哪里？</div>
      <div class="segment" id="shelfSegment">
        <button data-shelf-segment="movie">电影</button>
        <button data-shelf-segment="anime" class="active">番剧</button>
        <button data-shelf-segment="us_tv">美剧</button>
      </div>
    </div>
    <div class="modal-foot">
      <button class="btn" data-close="createModal">取消</button>
      <button class="btn primary" id="confirmCreate">建立任务</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const $=s=>document.querySelector(s),$$=s=>Array.from(document.querySelectorAll(s)),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let jobs=[],intakeSources=[],unitViews={},paused=true,busy=false,currentTab='active',selectedSource=null,selectedShelf='anime';
const shelfLabels={movie:'电影',anime:'番剧',us_tv:'美剧'},terminal=new Set(['completed','executed','cancelled']),attention=new Set(['failed','failed_cleanup','failed_identity','reconciliation_uncertain','target_policy_conflict']);
async function api(path,opt){const r=await fetch(path,{headers:{'Content-Type':'application/json'},...opt}),d=await r.json().catch(()=>({}));if(!r.ok)throw new Error(d.error||`请求失败 (${r.status})`);return d}
function toast(msg,bad=false){const n=$('#toast');n.textContent=msg;n.className='toast show'+(bad?' bad':'');clearTimeout(n.timer);n.timer=setTimeout(()=>n.classList.remove('show'),2600)}
function title(j){const raw=j.source||j.summary?.source_root||j.id;return String(raw).split('/').filter(Boolean).pop()||j.id}
function phaseLabel(p){return({queued:'已排队',reconciling:'正在对账',archive_preprocessing:'归档预处理',identity_matching:'识别内容',planning:'生成计划',executing:'正在写入',completed:'已完成',executed:'已执行',cancelled:'已取消',failed:'失败',failed_cleanup:'清理失败',failed_identity:'识别失败',reconciliation_uncertain:'对账不确定',target_policy_conflict:'货架冲突',retry_wait:'等待重试',awaiting_target_shelf:'等待选择货架'}[p]||p)}
function unitProgress(j){const v=unitViews[j.id];if(!v||!v.aggregate||!v.aggregate.unit_count)return null;const a=v.aggregate;return {unit_count:a.unit_count,completed:a.completed,attention:a.attention,failed:a.failed,open_gaps:a.open_gaps}}
function waitingCard(j){const shelves=j.allowed_target_shelves||[];return `<div class="card"><strong title="${esc(j.source||j.id)}">${esc(title(j))}</strong><small>${esc(j.source||j.id)}</small>${j.error?`<small class="row-error">${esc(j.error)}</small>`:''}<div class="choose-label">这个任务应整理到哪里？</div><div class="segment">${shelves.map(s=>`<button data-job="${esc(j.id)}" data-shelf="${esc(s)}">${shelfLabels[s]||esc(s)}</button>`).join('')}</div></div>`}
function confirmCard(job,unit){const cands=(unit.candidate_identities||[]).map(c=>`<div class="choice" data-confirm="1" data-job="${esc(job.id)}" data-unit="${esc(unit.work_unit_id)}" data-tmdb="${esc(c.tmdb_id)}" data-type="${esc(c.media_type)}"><i></i><span><b>${esc(c.title||c.media_type)}</b><small>${esc(c.media_type)} / ${esc(c.tmdb_id)}${c.year?` · ${esc(c.year)}`:''}</small></span></div>`).join('');return `<div class="card"><strong title="${esc(unit.boundary_key||'')}">${esc(unit.display_label||unit.boundary_key)}</strong><small>${esc(job.source||job.id)}</small><div class="choose-label">识别不确定，请确认正确身份：</div><div class="choicebox">${cands||`<div class="choice"><i></i><span><small>无可选候选</small></span></div>`}</div></div>`}
function intakeDir(s){const state=s.root_task_id?`<em>已建立</em>`:`<em></em>`;const counts=`${s.child_count==null?'—':s.child_count} 个子目录 · ${s.file_count==null?'—':s.file_count} 个文件`;return `<button class="dir${selectedSource===s.canonical_path?' active':''}" data-source="${esc(s.canonical_path)}"><span>▣</span><span><b>${esc(s.display_name||s.canonical_path)}</b><small>${counts}</small></span>${state}</button>`}
function row(j){const bad=attention.has(j.phase),run=!terminal.has(j.phase)&&!bad;const prog=unitProgress(j);const pct=prog&&prog.unit_count?Math.round(prog.completed*100/prog.unit_count):0;const meta=prog?`${prog.unit_count} 个作品<br>${prog.completed} 已完成${prog.attention?` · ${prog.attention} 待确认`:''}`:(j.allowed_target_shelves?.length?'等待选择货架':'—');const stateCls=bad?'attention':(terminal.has(j.phase)?'done':'run');return `<div class="task" data-job-row="${esc(j.id)}"><div class="task-main"><strong title="${esc(j.source||j.id)}">${esc(title(j))}</strong><p>${esc(j.source||j.id)}</p>${prog?`<div class="task-progress"><i style="width:${pct}%;${pct===100?'background:var(--ok)':''}"></i></div>`:''}${j.error?`<div class="row-error">${esc(j.error)}</div>`:''}</div><div class="task-meta">${meta}</div><div class="task-state ${stateCls}">${esc(phaseLabel(j.phase))}</div></div>`}
function render(){const waiting=jobs.filter(j=>Array.isArray(j.allowed_target_shelves)&&j.allowed_target_shelves.length);$('#waitingCount').textContent=`${waiting.length} 个任务`;$('#countWaiting').textContent=waiting.length;$('#waitingSection').style.display=waiting.length?'':'none';$('#waiting').innerHTML=waiting.length?waiting.map(waitingCard).join(''):`<div class="empty">没有等待选择的任务</div>`;let rows=jobs.filter(j=>!waiting.includes(j));if(currentTab==='waiting')rows=waiting;if(currentTab==='active')rows=rows.filter(j=>!terminal.has(j.phase)&&!attention.has(j.phase));if(currentTab==='attention')rows=rows.filter(j=>attention.has(j.phase));if(currentTab==='history')rows=rows.filter(j=>terminal.has(j.phase));$('#jobCount').textContent=`${rows.length} 个任务`;$('#jobList').innerHTML=rows.length?rows.map(row).join(''):`<div class="empty">这个分类中暂无任务</div>`;$('#countActive').textContent=jobs.filter(j=>!terminal.has(j.phase)&&!attention.has(j.phase)&&!j.allowed_target_shelves?.length).length;$('#countAttention').textContent=jobs.filter(j=>attention.has(j.phase)).length;$('#countHistory').textContent=jobs.filter(j=>terminal.has(j.phase)).length;$('#countAll').textContent=jobs.length;$$('.nav[data-tab]').forEach(x=>x.classList.toggle('active',x.dataset.tab===currentTab))}
function renderIntake(){const present=intakeSources.filter(s=>s.present!==false);$('#intakeList').innerHTML=present.length?present.map(intakeDir).join(''):`<div class="empty">待刮削目录中没有来源</div>`}
function renderConfirm(){const rows=[];for(const j of jobs){const view=unitViews[j.id];if(!view)continue;for(const u of view.units||[]){if(u.identity_status==='uncertain'&&(u.candidate_identities||[]).length)rows.push(confirmCard(j,u))}}$('#confirmCount').textContent=`${rows.length} 个单元`;$('#confirmSection').style.display=rows.length?'':'none';$('#confirm').innerHTML=rows.length?rows.join(''):`<div class="empty">没有需要确认的单元</div>`}
async function load(){try{const[h,c,j,i]=await Promise.all([api('/api/health'),api('/api/control'),api('/api/jobs'),api('/api/intake')]);paused=c.paused!==false;jobs=j.jobs||[];intakeSources=i.sources||[];unitViews={};$('#healthText').textContent=h.ok?'服务正常':'服务异常';$('#healthDot').className=h.ok?'ok':'';await Promise.all(jobs.filter(j=>!terminal.has(j.phase)).map(async j=>{try{unitViews[j.id]=await api('/api/jobs/'+encodeURIComponent(j.id)+'/work-units')}catch(e){unitViews[j.id]=null}}));const waiting=jobs.filter(x=>x.allowed_target_shelves?.length).length,active=jobs.filter(x=>!terminal.has(x.phase)&&!attention.has(x.phase)&&!x.allowed_target_shelves?.length).length,issues=jobs.filter(x=>attention.has(x.phase)).length;$('#sumWaiting').textContent=waiting;$('#sumActive').textContent=active;$('#sumIssues').textContent=issues;const b=$('#control'),sb=$('#sideControl');b.textContent=paused?'恢复运行':'暂停运行';b.className='btn '+(paused?'primary':'danger');sb.querySelector('span').textContent=paused?'恢复运行':'暂停运行';render();renderIntake();renderConfirm()}catch(e){$('#healthText').textContent='连接失败';toast(e.message,true)}}
async function post(path,payload={}){if(busy)return;busy=true;$$('button').forEach(b=>b.disabled=true);try{await api(path,{method:'POST',body:JSON.stringify(payload)});await load();toast('操作已保存')}catch(e){toast(e.message,true)}finally{busy=false;$$('button').forEach(b=>b.disabled=false)}}
$('#refresh').onclick=load;$('#topRefresh').onclick=load;
$('#control').onclick=()=>post(paused?'/api/control/resume':'/api/control/pause');
$('#sideControl').onclick=()=>post(paused?'/api/control/resume':'/api/control/pause');
$$('.nav[data-tab]').forEach(n=>n.onclick=()=>{currentTab=n.dataset.tab;render()});
$('#waiting').onclick=e=>{const b=e.target.closest('[data-shelf]');if(b)post(`/api/jobs/${encodeURIComponent(b.dataset.job)}/start`,{target_shelf:b.dataset.shelf})};
$('#confirm').onclick=e=>{const b=e.target.closest('[data-confirm]');if(b)post('/api/jobs/'+encodeURIComponent(b.dataset.job)+'/work-units/'+encodeURIComponent(b.dataset.unit)+'/confirm',{media_type:b.dataset.type,tmdb_id:Number(b.dataset.tmdb)})};
function openModal(m){m.classList.add('show')}
function closeModal(m){m.classList.remove('show')}
$('#openCreate').onclick=()=>openModal($('#createModal'));
$('#topCreate').onclick=()=>openModal($('#createModal'));
$$('[data-close]').forEach(b=>b.onclick=()=>closeModal(document.getElementById(b.dataset.close)));
$$('.modalback').forEach(m=>m.addEventListener('click',e=>{if(e.target===m)closeModal(m)}));
$('#intakeList').onclick=e=>{const b=e.target.closest('[data-source]');if(!b||b.dataset.source===selectedSource)return;selectedSource=b.dataset.source;renderIntake()};
$$('#shelfSegment button').forEach(b=>b.onclick=()=>{selectedShelf=b.dataset.shelfSegment;$$('#shelfSegment button').forEach(x=>x.classList.toggle('active',x===b))});
$('#confirmCreate').onclick=async()=>{if(!selectedSource){toast('请先选择待刮削目录',true);return}if(busy)return;busy=true;$$('button').forEach(b=>b.disabled=true);try{await api('/api/root-jobs',{method:'POST',body:JSON.stringify({path:selectedSource,target_shelf:selectedShelf})});closeModal($('#createModal'));selectedSource=null;await load();toast('任务已建立')}catch(e){toast(e.message,true)}finally{busy=false;$$('button').forEach(b=>b.disabled=false)}};
load();setInterval(load,10000);
</script>
</body>
</html>'''


def dashboard_html() -> bytes:
    return DASHBOARD_HTML.encode("utf-8")
