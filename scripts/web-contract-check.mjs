#!/usr/bin/env node
/** Focused Web contract checks for the target-shelf start gate. */

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);

for (const extension of [".ts", ".tsx"]) {
  require.extensions[extension] = (module, filename) => {
    const source = readFileSync(filename, "utf8");
    const result = ts.transpileModule(source, {
      fileName: filename,
      compilerOptions: {
        esModuleInterop: true,
        jsx: ts.JsxEmit.ReactJSX,
        module: ts.ModuleKind.CommonJS,
        moduleResolution: ts.ModuleResolutionKind.NodeJs,
        target: ts.ScriptTarget.ES2022,
      },
    });
    module._compile(result.outputText, filename);
  };
}

function appFiles(directory = join(repository, "app")) {
  const rows = [];
  for (const name of readdirSync(directory)) {
    const path = join(directory, name);
    const stat = statSync(path);
    if (stat.isDirectory()) rows.push(...appFiles(path));
    else if (/\.(ts|tsx)$/.test(name)) rows.push(path);
  }
  return rows;
}

const React = require("react");
const { renderToStaticMarkup } = require("react-dom/server");
const { OperationsOverview } = require("../app/components/operations-overview.tsx");
const { TaskExpansion } = require("../app/components/task-expansion.tsx");
const { TaskRow } = require("../app/components/task-row.tsx");
const { scrapeFlowApi } = require("../app/core/api-client.ts");

function job(overrides = {}) {
  return {
    id: "job-1",
    source: "/library/waiting/Example",
    parent: "/library/waiting",
    updated_at: "2026-08-09T00:00:00Z",
    phase: "awaiting_target_shelf",
    error: null,
    plan: null,
    target_shelf: null,
    target_root: null,
    target_work_path: null,
    allowed_target_shelves: ["movie", "anime", "us_tv"],
    selected_at: null,
    progress: null,
    ...overrides,
  };
}

const handlers = {
  onClose: () => {},
  onStart: async () => true,
  onRetry: async () => true,
  onCancel: async () => true,
  onCleanup: async () => true,
};

const waitingMarkup = renderToStaticMarkup(
  React.createElement(TaskExpansion, { job: job(), pending: false, ...handlers }),
);
assert.match(waitingMarkup, /START GATE/);
assert.match(waitingMarkup, /Movie/);
assert.match(waitingMarkup, /Anime \/ TV/);
assert.match(waitingMarkup, /US TV/);
assert.doesNotMatch(waitingMarkup, /target_parent/);

const conflictMarkup = renderToStaticMarkup(
  React.createElement(TaskExpansion, {
    job: job({
      phase: "target_policy_conflict",
      error: "selected shelf does not match TMDB media type",
      target_shelf: "anime",
      target_root: "/library/anime",
    }),
    pending: false,
    ...handlers,
  }),
);
assert.match(conflictMarkup, /TARGET CONFLICT/);
assert.match(conflictMarkup, /selected shelf does not match TMDB media type/);

const failedIdentityMarkup = renderToStaticMarkup(
  React.createElement(TaskExpansion, {
    job: job({ phase: "failed_identity", error: "identity failed", target_shelf: "movie" }),
    pending: false,
    ...handlers,
  }),
);
assert.match(failedIdentityMarkup, /<option value=""(?: selected="")?>/);
assert.doesNotMatch(failedIdentityMarkup, /collection/i);
assert.doesNotMatch(failedIdentityMarkup, /归档密码/);

const failedArchiveMarkup = renderToStaticMarkup(
  React.createElement(TaskExpansion, {
    job: job({ phase: "failed_archive", error: "archive failed", target_shelf: "anime" }),
    pending: false,
    ...handlers,
  }),
);
assert.match(failedArchiveMarkup, /归档密码/);

const legacyFailedMarkup = renderToStaticMarkup(
  React.createElement(TaskExpansion, {
    job: job({ phase: "failed_planning", error: "legacy failure", target_shelf: null }),
    pending: false,
    ...handlers,
  }),
);
assert.match(legacyFailedMarkup, /旧任务未确认目标货架，不能自动重试/);
assert.doesNotMatch(legacyFailedMarkup, /立即重试/);

const waitingRow = renderToStaticMarkup(
  React.createElement(TaskRow, {
    job: job(),
    selected: false,
    expanded: false,
    pending: false,
    onToggle: () => {},
    onRetry: () => {},
  }),
);
assert.match(waitingRow, /<button class="taskdesk-task"/);
assert.match(waitingRow, /class="row-primary"/);

const legacyFailedRow = renderToStaticMarkup(
  React.createElement(TaskRow, {
    job: job({ phase: "failed_planning", error: "legacy failure", target_shelf: null }),
    selected: false,
    expanded: false,
    pending: false,
    onToggle: () => {},
    onRetry: () => {},
  }),
);
assert.match(legacyFailedRow, /查看异常/);
assert.doesNotMatch(legacyFailedRow, /立即重试/);

const overviewMarkup = renderToStaticMarkup(
  React.createElement(OperationsOverview, {
    health: {
      ok: true,
      mode: "automatic",
      connected: true,
      tmdb_configured: true,
      engine_configured: true,
      intake_monitoring: false,
      operations: {
        jobs_total: 2,
        jobs_awaiting_target_shelf: 7,
        jobs_active: 0,
        jobs_failed: 0,
        jobs_completed: 0,
        provider_active: 0,
        formal_write_workers: 0,
        provider_workers: 0,
        audit_running: false,
      },
    },
    control: { paused: true, updated_at: null, reason: null, persistent: true },
    jobs: [job(), job({ id: "job-2", phase: "queued", target_shelf: "movie" })],
    audit: null,
  }),
);
assert.match(overviewMarkup, /等待选择货架：7/);

const calls = [];
globalThis.fetch = async (url, init = {}) => {
  calls.push({ url, init });
  return {
    ok: true,
    json: async () => ({ job: job({ phase: "queued", target_shelf: "anime" }) }),
  };
};

await scrapeFlowApi.start("job-1", "anime");
assert.equal(calls.at(-1).url, "/api/jobs/job-1/start");
assert.deepEqual(JSON.parse(calls.at(-1).init.body), { target_shelf: "anime" });

await scrapeFlowApi.retry("job-1");
assert.equal(calls.at(-1).url, "/api/jobs/job-1/retry");
assert.deepEqual(JSON.parse(calls.at(-1).init.body), {});

for (const file of appFiles()) {
  const source = readFileSync(file, "utf8");
  assert.equal(
    source.includes("target_parent"),
    false,
    `Web must not accept or send target_parent: ${file}`,
  );
}

const dashboardSource = readFileSync(join(repository, "app/hooks/use-dashboard-controller.ts"), "utf8");
assert.match(dashboardSource, /START_GATE_PHASES\.has\(job\.phase\)/);
assert.match(dashboardSource, /refreshJobs/);

const apiSource = readFileSync(join(repository, "app/core/api-client.ts"), "utf8");
assert.doesNotMatch(apiSource, /collection/);

console.log("Web target-shelf contract check passed");
