import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

async function render(pathname = "/") {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}-${pathname}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request(`http://localhost${pathname}`, { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("server-renders the real local ScrapeFlow application", async () => {
  for (const pathname of ["/", "/ui/04"]) {
    const response = await render(pathname);
    assert.equal(response.status, 200);
    const html = await response.text();
    assert.match(html, /<title>ScrapeFlow · AList 媒体自动整理<\/title>/);
    assert.match(html, /整理媒体目录/);
    assert.match(html, /分析并生成计划/);
    assert.match(html, /仅在本机 127\.0\.0\.1 运行/);
    assert.match(html, /任务记录/);
    assert.match(html, /连接状态/);
    assert.doesNotMatch(html, /86-不存在|EIGHTY SIX|真实海报/);
  }
});

test("the UI is wired to local job, approval, cancellation, and history APIs", async () => {
  const source = await readFile(new URL("../app/local-app.tsx", import.meta.url), "utf8");
  assert.match(source, /http:\/\/127\.0\.0\.1:8765\/api/);
  assert.match(source, /api<\{ job: Job \}>\("\/jobs"/);
  assert.match(source, /`\/jobs\/\$\{job\.id\}\/approve`/);
  assert.match(source, /`\/jobs\/\$\{job\.id\}\/cancel`/);
  assert.match(source, /api<\{ jobs: Job\[\] \}>\("\/jobs"\)/);
  assert.doesNotMatch(source, />浏览<|媒体服务器时间|等待识别媒体/);
});

test("ships the local bridge, engine, and launch script", async () => {
  const [server, engine, pkg] = await Promise.all([
    readFile(new URL("../local/server.py", import.meta.url), "utf8"),
    readFile(new URL("../engine/scraper.py", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);
  assert.match(server, /ThreadingHTTPServer/);
  assert.match(server, /127\.0\.0\.1/);
  assert.match(server, /awaiting_archive_approval/);
  assert.match(server, /awaiting_media_approval/);
  assert.match(engine, /__version__ = "3\.3\.2"/);
  assert.match(pkg, /"local": "node scripts\/local\.mjs"/);
  await access(new URL("../scripts/local.mjs", import.meta.url));
});
