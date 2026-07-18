import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

const root = new URL("../", import.meta.url);

async function render(pathname = "/") {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request(`http://localhost${pathname}`, { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("server-renders a selector for all five ScrapeFlow directions", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  const html = await response.text();
  assert.match(html, /<title>ScrapeFlow · AList 媒体自动整理<\/title>/);
  for (const title of ["深海控制台", "纸张编辑部", "极光向导", "终端机", "媒体便当"]) {
    assert.match(html, new RegExp(title));
  }
  for (const route of ["01", "02", "03", "04", "05"]) {
    assert.match(html, new RegExp(`href="/ui/${route}"`));
  }
  assert.doesNotMatch(html, /codex-preview|Your site is taking shape/);
});

test("each direction has its own independently rendered interface", async () => {
  const directions = [
    ["01", "01-command"],
    ["02", "02-paper"],
    ["03", "03-aurora"],
    ["04", "04-terminal"],
    ["05", "05-bento"],
  ];

  for (const [route, marker] of directions) {
    const response = await render(`/ui/${route}`);
    assert.equal(response.status, 200);
    assert.match(await response.text(), new RegExp(`data-ui="${marker}"`));
  }
});

test("ships the engine and a project-bound social card", async () => {
  const [demo, layout, engine] = await Promise.all([
    readFile(new URL("../app/ui-demo.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../engine/scraper.py", import.meta.url), "utf8"),
  ]);
  for (const component of ["CommandUI", "PaperUI", "AuroraUI", "TerminalUI", "BentoUI"]) {
    assert.match(demo, new RegExp(`export function ${component}`));
  }
  assert.match(layout, /\/og\.png/);
  assert.match(engine, /__version__ = "3\.3\.2"/);
  await access(new URL("../public/og.png", import.meta.url));
  await assert.rejects(access(new URL("../app/_sites-preview", root)));
});
