import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

const root = new URL("../", import.meta.url);

async function render() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request("http://localhost/", { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("server-renders all five ScrapeFlow directions", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  const html = await response.text();
  assert.match(html, /<title>ScrapeFlow · AList 媒体自动整理<\/title>/);
  for (const title of ["深海控制台", "纸张编辑部", "极光玻璃", "终端机", "媒体便当"]) {
    assert.match(html, new RegExp(title));
  }
  assert.match(html, /AList 媒体路径/);
  assert.match(html, /安全审核模式/);
  assert.doesNotMatch(html, /codex-preview|Your site is taking shape/);
});

test("ships the engine and a project-bound social card", async () => {
  const [page, layout, engine] = await Promise.all([
    readFile(new URL("../app/page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../engine/scraper.py", import.meta.url), "utf8"),
  ]);
  assert.match(page, /type ThemeId = "midnight" \| "paper" \| "aurora" \| "terminal" \| "bento"/);
  assert.match(layout, /\/og\.png/);
  assert.match(engine, /__version__ = "3\.3\.2"/);
  await access(new URL("../public/og.png", import.meta.url));
  await assert.rejects(access(new URL("../app/_sites-preview", root)));
});
