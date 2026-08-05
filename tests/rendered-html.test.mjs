import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import test from "node:test";

async function render(pathname = "/") {
  const page = pathname === "/"
    ? "index.html"
    : `${pathname.replace(/^\/+|\/+$/g, "")}.html`;
  const path = process.env.SCRAPEFLOW_BUILD_ROOT
    ? resolve(process.env.SCRAPEFLOW_BUILD_ROOT, "out", page)
    : new URL(`../out/${page}`, import.meta.url);
  try {
    return new Response(await readFile(path), {
      status: 200,
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  } catch (error) {
    if (error?.code === "ENOENT") return new Response("Not found", { status: 404 });
    throw error;
  }
}

test("server renders the single-page task dashboard at root", async () => {
  const response = await render("/");
  assert.equal(response.status, 200);
  const html = await response.text();
  assert.match(html, /<title>ScrapeFlow — 任务列表<\/title>/);
  assert.match(html, /SCRAPEFLOW/);
  assert.doesNotMatch(html, /LOCAL MEDIA OPERATOR/);
  assert.match(html, /正在接入本地任务队列/);
  assert.doesNotMatch(html, /从下载目录|href="\/workspace"|打开工作台/);
});

test("workspace route was removed", async () => {
  const response = await render("/workspace");
  assert.equal(response.status, 404);
});

test("new web client covers the complete safe job lifecycle", async () => {
  const [app, hook, api, review, jobState, workbench] = await Promise.all([
    readFile(new URL("../app/scrapeflow-app.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/hooks/use-scrapeflow.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/core/api-client.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/components/review-dialog.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/core/job-state.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/components/issue-workbench.tsx", import.meta.url), "utf8"),
  ]);
  assert.match(api, /const API_ROOT = "\/api"/);
  assert.match(api, /export class ApiRequestError extends Error/);
  assert.match(api, /new ApiRequestError\(apiError\(payload,[\s\S]*response\.status, payload\)/);
  assert.doesNotMatch(api, /X-ScrapeFlow-Token|\/session/);
  assert.doesNotMatch(api, /\/audit|\/convergence|runConvergence/);
  assert.match(api, /`\/jobs\/\$\{id\}\/approve`/);
  assert.match(api, /`\/jobs\/\$\{id\}\/cancel`/);
  assert.match(api, /`\/jobs\/\$\{id\}\/resolve`/);
  assert.match(api, /`\/jobs\/\$\{id\}\/recover`/);
  assert.match(api, /`\/jobs\/\$\{id\}\/retry`/);
  assert.match(api, /retry: \(id: string, options: JobRetryOptions = \{\}\)/);
  assert.match(api, /method: "DELETE"/);
  assert.match(api, /clearData: \(\) => request<\{ cleared: number \}>\("\/jobs"/);
  assert.match(hook, /const poll = async/);
  assert.match(hook, /const hasActiveJobs = jobs\.some/);
  assert.match(hook, /await refreshJobs\(\)/);
  assert.match(hook, /previous\.map\(job => job\.id === next\.id \? next : job\)/);
  assert.doesNotMatch(hook, /\[next, \.\.\.previous\.filter/);
  assert.doesNotMatch(api, /log_after|logAfter/);
  assert.match(api, /: "auto"/);
  assert.match(api, /mediaType \|\| \(category === "电影" \? "movie" : "tv"\)/);
  assert.match(api, /category === "电影" \? "movie" : "tv"/);
  assert.match(app, /plan\.review\?\.match\?\.media_type/);
  assert.match(app, /supplementContext\?\.mediaType/);
  assert.match(app, /<span>TMDB 类型<\/span><select value=\{mediaType\}/);
  assert.match(app, /media_type: mediaType/);
  assert.match(api, /prefer_simplified: true/);
  assert.doesNotMatch(app, /CurrentTaskCard|current-task-card|CURRENT TASK|审核当前计划/);
  assert.match(app, /任务队列/);
  assert.match(app, /className="workspace-tabs" role="tablist" aria-label="工作区"/);
  assert.match(app, /workspace-tab-tasks/);
  assert.match(app, /workspace-tab-create/);
  assert.ok(
    app.indexOf('id="workspace-tab-create"') < app.indexOf('id="workspace-tab-tasks"'),
    "新建整理标签应位于任务中心左侧",
  );
  assert.match(app, /workspaceTab === "tasks"/);
  assert.doesNotMatch(app, /creatorOpen|setCreatorOpen|展开新建|收起新建/);
  assert.match(app, /IssueWorkbench/);
  assert.doesNotMatch(app, /MediaAuditPanel|media-audit-panel|CONVERGENCE|立即收敛/);
  assert.match(workbench, /缺集、中文字幕和补源只随当前作品自动检查/);
  assert.doesNotMatch(workbench, /MediaAudit|SubtitleIssuesPanel|审计|全库|收敛/);
  assert.match(app, /job\.queue_position/);
  assert.match(app, /queueName = job\.queue_kind === "execution" \? "文件操作" : "分析"/);
  assert.match(app, /队列第 \$\{job\.queue_position\} 位/);
  assert.match(app, /role="progressbar"/);
  assert.match(app, /保留现有版本并结束/);
  assert.match(app, /passwordFailure/);
  assert.match(app, /aria-valuenow=\{Math\.round\(progress\)\}/);
  assert.match(app, /\$\{liveProgress\.completed\}\/\$\{liveProgress\.total\}/);
  assert.match(jobState, /queued: \{ label: "排队中"/);
  assert.match(app, /className="brand-mark" aria-hidden="true"/);
  assert.doesNotMatch(app, /className="brand-mark">SF/);
  assert.doesNotMatch(app, /value: "all"|label: "全部"|filter === "all"/);
  assert.match(app, /value: "attention", label: "需处理"/);
  assert.match(app, /value: "running", label: "运行中"/);
  assert.match(app, /value: "finished", label: "已结束"/);
  assert.doesNotMatch(app, /TaskSidePanel|dashboard-side/);
  assert.doesNotMatch(app, /TaskInlineDetail|taskdesk-inline-detail|任务详情|执行日志|drawer-logs/);
  assert.match(app, /TaskExpansion|taskdesk-inline-review/);
  assert.match(app, /taskdesk-success-panel|任务已完成|查看结果/);
  assert.match(app, /expandedId === job\.id/);
  assert.match(app, /keepTaskAtViewportPosition/);
  assert.match(app, /data-job-id=\{job\.id\}/);
  assert.match(app, /expanded \? \{ label: "收起"/);
  assert.match(app, /managing \? "完成" : "管理"/);
  assert.match(app, /canDelete\(job\)/);
  assert.doesNotMatch(app, /window\.confirm/);
  assert.doesNotMatch(app, /只删除任务记录，不会修改或删除媒体文件/);
  assert.match(app, /flow\.remove\(job\)/);
  assert.match(app, /定位原因并原地修复/);
  assert.match(app, /ACTUAL FAILURE/);
  assert.match(app, /直接证据/);
  assert.match(app, /本轮实际尝试的候选/);
  assert.match(app, /隔离失败候选/);
  assert.match(app, /EntityTooSmall\|ProposedSize/);
  assert.match(app, /候选已进入 AList 上传/);
  assert.match(app, /AList 分片上传/);
  assert.match(app, /拒绝 ProposedSize=0 的上传分片/);
  assert.match(app, /修改识别参数/);
  assert.match(app, /压缩包密码/);
  assert.match(app, /archive_password: archivePassword \|\| null/);
  assert.match(review, /subtitle_without_video: "缺少对应视频"/);
  assert.match(review, /missing_episode: "缺少已发布剧集"/);
  assert.match(app, /job\.error \|\| "后端未返回具体错误/);
  assert.match(app, /confirmClear \? "确认清空" : "清空数据"/);
  assert.match(app, /confirmRemoveId !== job\.id/);
  assert.match(app, /removeConfirming \? "确认删除" : "删除"/);
  assert.doesNotMatch(app, /新建整理任务/);
  assert.match(app, /create-panel-persistent/);
  assert.match(app, /const TARGET_CATEGORY_STORAGE_KEY = "scrapeflow\.target-category"/);
  assert.match(app, /const TARGET_CATEGORIES = \["番剧", "美剧", "电影"\]/);
  assert.match(app, /window\.localStorage\.getItem\(TARGET_CATEGORY_STORAGE_KEY\)/);
  assert.match(app, /TARGET_CATEGORIES\.some\(value => value === stored\)/);
  assert.match(app, /window\.localStorage\.setItem\(TARGET_CATEGORY_STORAGE_KEY, value\)/);
  assert.match(app, /catch \{\s*\/\/ Private browsing and disabled storage must not block task creation\./);
  assert.match(app, /catch \{\s*\/\/ Keep the in-memory selection usable when persistence is unavailable\./);
  const createSuccess = app.slice(app.indexOf("const createJob = async () =>"), app.indexOf("const selectCategory ="));
  assert.match(createSuccess, /setSource\(UNSCRAPED_MEDIA_ROOT\)/);
  assert.match(createSuccess, /setSupplementContext\(null\)/);
  assert.doesNotMatch(createSuccess, /setWorkspaceTab\("tasks"\)|setFilter\("running"\)|setCategory\(""\)/);
  assert.match(app, /onCategoryChange=\{selectCategory\}/);
  assert.match(app, /TARGET_CATEGORIES\.map/);
  const browseFlow = app.slice(app.indexOf("const browse = async"), app.indexOf("const createJob = async () =>"));
  assert.doesNotMatch(browseFlow, /setSource\(result\.path\)/);
  assert.match(browseFlow, /const selectBrowserPath = \(path: string\)[\s\S]*setSource\(path\)[\s\S]*setBrowserOpen\(false\)/);
  const browserPanel = app.slice(app.indexOf("function NewTaskPanel"));
  assert.match(browserPanel, /aria-expanded=\{browserOpen\}/);
  assert.match(browserPanel, /disabled=\{browserPending \|\| browserOpen\}/);
  assert.match(browserPanel, /browserOpen \? "目录已展开" : "浏览"/);
  assert.match(browserPanel, /onSelectBrowserPath\(browser\.path\)/);
  assert.match(browserPanel, /确认后才会更改上方媒体目录/);
  assert.match(browserPanel, />收起目录</);
  assert.doesNotMatch(app, /taskdesk-new|收起新建/);
  assert.match(app, /refreshDashboard/);
  assert.match(app, /flow\.browse\(refreshPath, true\)/);
  assert.match(app, /刷新 AList/);
  assert.match(app, /同步中|已同步|同步失败/);
  assert.match(app, /setTimeout\(\(\) => setRefreshFeedback\(""\), 1600\)/);
  assert.doesNotMatch(app, /SystemPage|setView\("system"\)|系统状态|LOCAL SYSTEM/);
  assert.doesNotMatch(app, /TaskDrawer|task-drawer|drawer-overlay|review-backdrop/);
  assert.doesNotMatch(app, /setDetailId|href=\{?`?\/task|href="\/workspace"/);
  assert.doesNotMatch(review, /aria-modal|review-backdrop|dialog-open/);
  assert.doesNotMatch(app, /setReviewing|reviewing=/);
  assert.match(app, /if \(APPROVAL_PHASES\.has\(job\.phase\)\)/);
  assert.match(app, /needsApproval \? \{ label: "审核"/);
  assert.match(app, /job\.phase === "planning_recovery"/);
  assert.match(app, /const approveJob = async/);
  assert.match(app, /安全停止/);
  assert.match(app, /PLAN CORRECTION/);
  assert.match(app, /回滚已完成/);
  assert.match(app, /setFilter\("running"\)/);
  assert.doesNotMatch(review, /计划完整性摘要|SHA-256|下载 JSON|返回摘要/);
  assert.match(review, /我已核对上方/);
  assert.match(review, /执行风险与匹配依据/);
  assert.match(review, /匹配置信度/);
  assert.match(review, /查看其他候选与评分差距/);
  assert.match(review, /执行后永久删除/);
  assert.match(review, /不会进入回收站/);
  assert.match(review, /自动通过的映射和系统垃圾文件无需逐件审核/);
  assert.match(review, /该计划已满足自动执行门禁/);
  assert.match(review, /不需要确认或逐项勾选/);
  assert.match(review, /const destructiveCleanupCount = assessment\?\.destructive_cleanup_count \?\? cleanupCount/);
  assert.match(review, /const requiresConfirmation = assessment \? !assessment\.automation_eligible/);
  assert.match(review, /真正需确认/);
  assert.match(review, /仅供参考/);
  assert.match(review, /<details className="route-details">/);
  assert.match(review, /<details className="mapping-details problem-files requires-confirmation">/);
  assert.match(review, /<details className=\{`mapping-details cleanup-review/);
  assert.match(review, /需修正：无法安全处理的文件/);
  assert.match(review, /问题文件采用失败关闭策略/);
  assert.match(review, /const unresolvedProblemCount = problemCount/);
  assert.match(review, /系统不会自动移动或删除/);
  assert.match(review, /不能用人工确认绕过/);
  assert.doesNotMatch(review, /plan\.auto_routed_problem_count/);
  assert.doesNotMatch(review, /file\.automatic_handling/);
  assert.doesNotMatch(review, /ScrapeFlow\/验证\/待核验/);
  assert.doesNotMatch(review, /ScrapeFlow\/备份\/字幕备份/);
  assert.match(review, /assessment \? !assessment\.automation_eligible : unresolvedProblemCount > 0/);
  assert.doesNotMatch(review, /系统自动处理：无法唯一映射的文件/);
  assert.doesNotMatch(review, /<details className="route-details" open/);
  assert.doesNotMatch(review, /mapping-details problem-files[^\n]*open/);
  assert.match(review, /自动通过：清理系统垃圾文件/);
  assert.match(review, /资源不完整/);
  assert.match(review, /resourceGaps/);
  assert.match(review, /gap\.reason/);
  assert.match(review, /gap\.files\.map/);
  assert.match(review, /补充资源/);
  assert.match(review, /plan\.tmdb_id \? <button/);
  assert.match(review, /当前计划无法唯一锁定 TMDB/);
  assert.match(app, /setSupplementContext/);
  assert.match(app, /if \(!plan\?\.tmdb_id\) return/);
  assert.match(app, /browse\(UNSCRAPED_MEDIA_ROOT\)/);
  assert.match(app, /已锁定 TMDB/);
  assert.doesNotMatch(app, /未取得 TMDB 编号，将重新自动识别/);
  assert.match(api, /tmdb_id: tmdbId/);
});

test("one compact task status bar shows only current workflow exceptions", async () => {
  const [app, hook, workbench, review, css, contracts] = await Promise.all([
    readFile(new URL("../app/scrapeflow-app.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/hooks/use-scrapeflow.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/components/issue-workbench.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/review-dialog.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../app/core/contracts.ts", import.meta.url), "utf8"),
  ]);
  assert.match(app, /IssueWorkbench/);
  assert.match(workbench, /任务状态/);
  assert.match(workbench, /task-status-bar/);
  assert.match(workbench, /task-status-details/);
  assert.match(workbench, /查看待处理明细/);
  assert.match(workbench, /PAGE_SIZE = 5/);
  assert.match(workbench, /任务异常与确认/);
  assert.match(workbench, /重新诊断/);
  assert.match(workbench, /批量重试临时故障/);
  assert.match(hook, /Promise\.all\(\[[\s\S]*refreshHealth\(\), refreshJobs\(\), refreshControl\(\),\s*\]\)/);
  assert.doesNotMatch(hook, /refreshAudit|auditRefreshing|refreshConvergence|runConvergence/);
  assert.match(workbench, /作品补齐流程在任务内继续/);
  assert.match(workbench, /wrong archive password[\s\S]*return false/);
  assert.doesNotMatch(workbench, /搜索当前分类|片名、路径、错误或缺失集|normalizedQuery/);
  assert.doesNotMatch(workbench, /审计|MediaAudit|SubtitleIssuesPanel|全库|收敛|laneShows/);
  assert.doesNotMatch(app, /MediaAuditPanel|media-audit-panel|全库|立即收敛/);
  assert.match(review, /其余 \{normalCount\} 项映射已通过自动检查/);
  assert.match(review, /cleanupGroups\.map/);
  assert.match(contracts, /automation_eligible/);
  assert.doesNotMatch(contracts, /MediaAudit|ConvergenceStatus|replenishment_sweep/);
  assert.match(contracts, /JobRetryOptions/);
  assert.match(css, /\.task-status-overview \{[^}]*grid-template-columns/);
  assert.match(css, /\.task-status-metric \{/);
  assert.doesNotMatch(css, /audit-list|task-status-subtitle|task-status-audit-error/);
});

test("review details keep long paths readable on narrow screens", async () => {
  const css = await readFile(new URL("../app/globals.css", import.meta.url), "utf8");
  assert.match(css, /\.mapping-table > div > span \{[^}]*min-width: 0;[^}]*overflow-wrap: anywhere;/);
  assert.match(css, /\.taskdesk-failure-panel dd \{[^}]*overflow-wrap: anywhere;[^}]*white-space: pre-wrap;/);
  assert.match(css, /@media \(max-width: 760px\)[\s\S]*\.mapping-table > div \{ grid-template-columns: 1fr; \}/);
  assert.match(css, /@media \(max-width: 760px\)[\s\S]*\.taskdesk-failure-panel dl > div \{ grid-template-columns: 1fr; \}/);
});

test("live task progress remains visible on narrow screens", async () => {
  const css = await readFile(new URL("../app/globals.css", import.meta.url), "utf8");
  const mobile = css.match(/@media \(max-width: 760px\)([\s\S]*)/)?.[1] ?? "";
  assert.match(mobile, /\.dashboard-task-row \.taskdesk-progress \{[^}]*grid-column: 1 \/ -1;/);
  assert.doesNotMatch(mobile, /\.dashboard-task-row \.taskdesk-progress[^}]*display: none;/);
});

test("live tasks expose replenishment details without losing operation errors", async () => {
  const [app, hook, workbench, css] = await Promise.all([
    readFile(new URL("../app/scrapeflow-app.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/hooks/use-scrapeflow.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/components/issue-workbench.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);
  assert.match(app, /const expandable = ACTIVE_PHASES\.has\(job\.phase\)/);
  assert.match(app, /active \? \{ label: "查看进度"/);
  assert.match(app, /function LiveTaskPanel/);
  assert.match(app, /当前阶段/);
  assert.match(app, /候选与覆盖/);
  assert.match(app, /待补资源清单/);
  assert.match(app, /上轮候选记录/);
  assert.match(app, /不是当前终态/);
  assert.match(hook, /const \[queueError, setQueueError\]/);
  assert.match(hook, /const \[operationError, setOperationError\]/);
  assert.match(hook, /cause instanceof ApiRequestError && cause\.status === 409/);
  assert.match(hook, /setCreateConflict\(existing\)/);
  assert.match(hook, /未重复创建；已在新建页显示原任务/);
  assert.match(app, /createConflict=\{flow\.createConflict\}/);
  assert.match(app, /className="create-conflict" role="status"/);
  assert.match(app, /你仍停留在新建整理/);
  assert.doesNotMatch(hook, /setJobs\(next\);\s*setOperationError\(""\)/);
  assert.match(hook, /selectedActiveId[\s\S]*scrapeFlowApi\.job\(selectedActiveId\)/);
  assert.doesNotMatch(workbench, /task-status-audit-error|资源清单|全库|审计/);
  assert.match(css, /\.live-task-resources ol \{[^}]*overflow-y: auto/);
  assert.match(css, /@media \(max-width: 760px\)[\s\S]*\.live-task-candidates li, \.live-task-resources li \{ grid-template-columns: 1fr;/);
});

test("pause control stays single-page while permanent library controls are absent", async () => {
  const [app, hook, api, workbench, contracts, css] = await Promise.all([
    readFile(new URL("../app/scrapeflow-app.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/hooks/use-scrapeflow.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/core/api-client.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/components/issue-workbench.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/core/contracts.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);
  assert.match(contracts, /export type GlobalControl = \{[^}]*paused: boolean/);
  assert.doesNotMatch(contracts, /scheduler_paused/);
  assert.doesNotMatch(contracts, /ConvergenceStatus|SubtitleConvergenceStatus|MediaAudit|replenishment_sweep|blocks_global_convergence/);

  assert.match(api, /control: \(\) => request<GlobalControl>\("\/control"\)/);
  assert.match(api, /pause: \(reason\?: string\) => post<GlobalControl>\("\/control\/pause", reason \? \{ confirm: true, reason \} : \{ confirm: true \}\)/);
  assert.match(api, /resume: \(\) => post<GlobalControl>\("\/control\/resume", \{ confirm: true \}\)/);
  assert.doesNotMatch(api, /\/convergence|\/audit|runConvergence|recoveryPlan|recovery-plan/);

  assert.match(hook, /window\.setInterval\(\(\) => \{\s*void refreshHealth\(\);\s*void refreshControl\(\);\s*\}, 12000\)/);
  assert.match(hook, /const setGlobalPause = useCallback/);
  assert.match(hook, /scrapeFlowApi\[paused \? "pause" : "resume"\]\(reason\)/);
  assert.doesNotMatch(hook, /refreshConvergence|runConvergence|refreshAudit|buildRecoveryPlan|recoveryPlan/);

  assert.match(workbench, /缺集、中文字幕和补源只随当前作品自动检查/);
  assert.doesNotMatch(workbench, /SubtitleIssuesPanel|missing_external_subtitle|全库|审计|收敛/);

  assert.match(app, /SystemControlBar/);
  assert.match(app, /暂停自动流程/);
  assert.match(app, /恢复自动流程/);
  assert.match(app, /刮削、到盘复核和缺项补源/);
  assert.doesNotMatch(app, /CONVERGENCE|立即收敛|收敛周期|全库扫描/);
  assert.doesNotMatch(app, /HistoryRecoveryBanner|重建丢失任务规划|window\.confirm/);

  assert.match(css, /\.system-control-bar \{/);
  assert.doesNotMatch(css, /task-status-subtitles|audit-list|system-control-group \+ \.system-control-group/);
});

test("web and local API use the same job phase contract", async () => {
  const [contractText, webContract, serverContract] = await Promise.all([
    readFile(new URL("../contracts/job-phases.json", import.meta.url), "utf8"),
    readFile(new URL("../app/core/contracts.ts", import.meta.url), "utf8"),
    readFile(new URL("../local/scrapeflow_api/contracts.py", import.meta.url), "utf8"),
  ]);
  const phases = JSON.parse(contractText);
  const arrayBlock = webContract.match(/export const JOB_PHASES = \[([\s\S]*?)\] as const/)?.[1] ?? "";
  const webPhases = [...arrayBlock.matchAll(/"([a-z_]+)"/g)].map(match => match[1]);
  assert.deepEqual(new Set(webPhases), new Set(phases));
  for (const phase of phases) assert.match(serverContract, new RegExp(`"${phase}"`));
});

test("directory browser uses a large scrollable single-column list", async () => {
  const [app, contracts, css, server] = await Promise.all([
    readFile(new URL("../app/scrapeflow-app.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/core/contracts.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../local/server.py", import.meta.url), "utf8"),
  ]);
  assert.match(css, /\.create-folders \{[^}]*grid-template-columns: 1fr;/);
  assert.doesNotMatch(css, /\.create-folders \{[^}]*repeat\(2/);
  assert.match(css, /\.create-folders \{[^}]*max-height: min\(62vh, 760px\)/);
  assert.match(css, /\.create-folders \{[^}]*overflow-y: auto/);
  assert.match(css, /\.create-folders \{[^}]*scrollbar-gutter: stable/);
  assert.match(contracts, /task_phase\?: JobPhase/);
  assert.match(contracts, /directory_state\?: "pending_delete"/);
  assert.match(app, /backendState === "pending_delete"[\s\S]*label: "待删除"/);
  assert.match(app, /job\.progress\?\.stage === "source_excluded"/);
  assert.match(app, /待删除", detail: "源目录已标记待删除并自动排除"/);
  assert.match(app, /directory\.directory_state/);
  assert.match(app, /state-pending-delete">待删除/);
  assert.match(app, /<dt>自动清理<\/dt><dd>\{cleanupCount\}<\/dd>/);
  assert.match(app, /className=\{`directory-\$\{directory\.state\.key\}`\}/);
  assert.match(css, /directory-pending_delete/);
  assert.match(app, /未处理/);
  assert.match(app, /处理中/);
  assert.match(app, /待审核/);
  assert.match(app, /已处理/);
  assert.match(app, /left\.state\.rank - right\.state\.rank/);
  assert.match(css, /directory-completed/);
  assert.match(server, /remember_completed_job\(job\)/);
  assert.match(server, /directory_task_phase\(child\)/);
});

test("ships the local bridge, engine and launch surfaces", async () => {
  const [server, scheduler, config, engine, pkg, nginx] = await Promise.all([
    readFile(new URL("../local/server.py", import.meta.url), "utf8"),
    readFile(new URL("../local/scrapeflow_api/scheduler.py", import.meta.url), "utf8"),
    readFile(new URL("../local/scrapeflow_api/config.py", import.meta.url), "utf8"),
    readFile(new URL("../engine/scraper.py", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
    readFile(new URL("../docker/nginx.conf", import.meta.url), "utf8"),
  ]);
  assert.match(server, /ThreadingHTTPServer/);
  assert.match(server, /analysis_workers=analysis_worker_count\(\)/);
  assert.match(server, /execution_workers=execution_worker_count\(\)/);
  assert.match(scheduler, /1 <= analysis_workers <= 8/);
  assert.match(scheduler, /range\(self\.execution_workers\)/);
  assert.match(config, /SCRAPEFLOW_ANALYSIS_WORKERS/);
  assert.match(config, /os\.getenv\("SCRAPEFLOW_ANALYSIS_WORKERS", "4"\)/);
  assert.match(config, /os\.getenv\("SCRAPEFLOW_EXECUTION_WORKERS", "1"\)/);
  assert.match(server, /start_execution\(execute_approved_media/);
  assert.match(server, /validate_approved_execution/);
  assert.match(server, /review_reasons = media_plan_review_reasons\(plan\)/);
  assert.doesNotMatch(server, /COMMAND_LOCK/);
  assert.match(server, /127\.0\.0\.1/);
  assert.match(engine, /__version__ = "3\.3\.2"/);
  assert.match(pkg, /"local": "node scripts\/local\.mjs"/);
  assert.match(pkg, /"dev": "next dev"/);
  assert.match(pkg, /"build": "next build"/);
  assert.doesNotMatch(pkg, /vinext|wrangler|cloudflare|@vitejs|react-server-dom-webpack/);
  assert.match(nginx, /try_files \$uri \$uri\.html \$uri\/ =404;/);
  assert.match(nginx, /Cache-Control "no-store, no-cache, must-revalidate"/);
  assert.match(nginx, /location = \/workspace[\s\S]+return 404;/);
  assert.doesNotMatch(nginx, /try_files[^;]+\/index\.html;/);
  await access(new URL("../scripts/local.mjs", import.meta.url));
  await access(new URL("../docker-compose.yml", import.meta.url));
  await access(new URL("../docker/nginx.conf", import.meta.url));
});

test("local source build keeps runtime and destructive workflow invariants", async () => {
  const [server, archives, dockerfile, ocrRequirements, dockerIgnore, webDockerfile, compose, nginx, webBuilder, config] = await Promise.all([
    readFile(new URL("../local/server.py", import.meta.url), "utf8"),
    readFile(new URL("../engine/tools/extract_archives.py", import.meta.url), "utf8"),
    readFile(new URL("../Dockerfile.api", import.meta.url), "utf8"),
    readFile(new URL("../engine/requirements-ocr.txt", import.meta.url), "utf8"),
    readFile(new URL("../.dockerignore", import.meta.url), "utf8"),
    readFile(new URL("../Dockerfile.web", import.meta.url), "utf8"),
    readFile(new URL("../docker-compose.yml", import.meta.url), "utf8"),
    readFile(new URL("../docker/nginx.conf", import.meta.url), "utf8"),
    readFile(new URL("../scripts/build-web-staging.mjs", import.meta.url), "utf8"),
    readFile(new URL("../local/scrapeflow_api/config.py", import.meta.url), "utf8"),
  ]);
  assert.doesNotMatch(server, /if not media_plan_requires_review\(plan\):[\s\S]{0,180}approve_job/);
  assert.doesNotMatch(server, /\u81ea\u52a8\u68c0\u67e5\u901a\u8fc7/);
  assert.doesNotMatch(archives, /def _remove_consumed_archive/);
  assert.match(archives, /"retained_archives": \[\]/);
  assert.match(dockerfile, /^FROM python:3\.12-slim-bookworm/);
  assert.match(dockerfile, /apt-get install -y --no-install-recommends/);
  for (const dependency of ["aria2", "ffmpeg", "p7zip-full", "libgl1", "libglib2.0-0", "libgomp1"]) {
    assert.match(dockerfile, new RegExp(`\\s${dependency.replaceAll("-", "\\-")} \\\\`));
  }
  assert.match(dockerfile, /COPY engine\/requirements-ocr\.txt \/tmp\/requirements-ocr\.txt/);
  assert.match(dockerfile, /pip install --requirement \/tmp\/requirements-ocr\.txt/);
  assert.match(dockerfile, /import cv2, onnxruntime; from rapidocr import RapidOCR/);
  assert.match(dockerfile, /useradd --system --uid 999/);
  assert.match(dockerfile, /USER scrapeflow/);
  assert.doesNotMatch(dockerfile, /python:3\.12-alpine|apk add/);
  assert.match(ocrRequirements, /^rapidocr==3\.9\.2$/m);
  assert.match(ocrRequirements, /^onnxruntime==1\.28\.0$/m);
  assert.doesNotMatch(ocrRequirements, /imageio-ffmpeg/);
  assert.doesNotMatch(dockerIgnore, /^engine\/requirements-ocr\.txt$/m);
  assert.doesNotMatch(dockerfile, /SCRAPEFLOW_RELEASE|release-input-manifest|test-evidence|source\.sha256/);
  assert.match(webDockerfile, /^FROM node:22-alpine AS build/);
  assert.match(webDockerfile, /RUN npm ci/);
  assert.match(webDockerfile, /RUN npm run build/);
  assert.match(webDockerfile, /COPY --from=build \/app\/out \/usr\/share\/nginx\/html/);
  assert.match(compose, /api:[\s\S]*dockerfile: Dockerfile\.api/);
  assert.match(compose, /gateway:[\s\S]*dockerfile: Dockerfile\.web/);
  assert.doesNotMatch(compose, /SCRAPEFLOW_API_IMAGE|SCRAPEFLOW_WEB_ROOT/);
  assert.match(compose, /SCRAPEFLOW_HOST_STATE_ROOT:\?set SCRAPEFLOW_HOST_STATE_ROOT/);
  assert.match(compose, /xhofe\/alist:v3\.62\.0@sha256:/);
  assert.match(compose, /api:[\s\S]*read_only: true[\s\S]*\/tmp:size=64m,mode=1777/);
  assert.match(compose, /SCRAPEFLOW_AUTO_REPLENISH_MISSING/);
  assert.doesNotMatch(compose, /SCRAPEFLOW_CONVERGENCE_(?:ENABLED|INTERVAL)/);
  assert.match(compose, /127\.0\.0\.1:\$\{SCRAPEFLOW_PORT:-3010\}:80/);
  assert.doesNotMatch(compose, /host\.docker\.internal:host-gateway/);
  assert.equal([...nginx.matchAll(/add_header X-Content-Type-Options/g)].length, 2);
  assert.equal([...nginx.matchAll(/add_header Content-Security-Policy/g)].length, 2);
  assert.match(webBuilder, /unsafe Web staging root outside \.runtime/);
  assert.match(webBuilder, /isSymbolicLink\(\)/);
  assert.match(webBuilder, /node_modules", "\.bin", "next"/);
  assert.doesNotMatch(webBuilder, /vinext|wrangler|cloudflare|\.openai|worker/);
  assert.doesNotMatch(webBuilder, /source-manifest|web-bundle-manifest|sha256/);
  assert.match(config, /SCRAPEFLOW_IGNORE_LOCAL_ENV/);
});

test("source exhaustion remains attached to the current title", async () => {
  const [app, contracts, css] = await Promise.all([
    readFile(new URL("../app/scrapeflow-app.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/core/contracts.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);
  assert.match(app, /replenishment\?\.status === "awaiting_sources"/);
  assert.match(app, /等待外部来源/);
  assert.match(app, /当前作品仍有/);
  assert.match(app, /补齐状态[\s\S]*等待合格来源/);
  assert.doesNotMatch(app, /下一收敛周期|不阻塞其他项目|自动复核/);
  assert.doesNotMatch(contracts, /business_state|next_review_at|review_interval_seconds|blocks_global_convergence/);
  assert.match(css, /taskdesk-awaiting-sources-panel/);
});
