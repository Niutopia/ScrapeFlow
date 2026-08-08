import { request, createServer } from "node:http";
import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";

async function loadEnvironment() {
  const merged = { ...process.env };
  if (merged.SCRAPEFLOW_IGNORE_LOCAL_ENV === "1") return merged;
  try {
    const contents = await readFile(".env.local", "utf8");
    for (const line of contents.split(/\r?\n/)) {
      const match = line.match(/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
      if (!match || merged[match[1]] !== undefined) continue;
      let value = match[2].trim();
      if (value.startsWith("\"") && value.endsWith("\"")) {
        try {
          value = JSON.parse(value);
        } catch {
          value = value.slice(1, -1);
        }
      } else if (value.startsWith("'") && value.endsWith("'")) {
        value = value.slice(1, -1).replaceAll("\\'", "'");
      } else {
        value = value.replace(/\s+#.*$/, "").trim();
      }
      merged[match[1]] = value;
    }
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  return merged;
}

function portFromEnv(source, name, fallback) {
  const raw = source[name]?.trim();
  if (!raw) return fallback;
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`${name} must be a TCP port between 1 and 65535`);
  }
  return port;
}

const env = await loadEnvironment();
const apiPort = portFromEnv(env, "SCRAPEFLOW_API_PORT", 8765);
const webPort = portFromEnv(
  env,
  "SCRAPEFLOW_LOCAL_PORT",
  portFromEnv(env, "SCRAPEFLOW_PORT", 3000),
);
const nextPort = portFromEnv(env, "SCRAPEFLOW_NEXT_DEV_PORT", webPort + 1);
if (nextPort > 65535) throw new Error("SCRAPEFLOW_NEXT_DEV_PORT must be a TCP port between 1 and 65535");
const apiEnv = {
  ...env,
  SCRAPEFLOW_API_HOST: env.SCRAPEFLOW_API_HOST || "127.0.0.1",
  SCRAPEFLOW_API_PORT: String(apiPort),
  // Docker explicitly mounts /data. Native `npm run local` instead keeps
  // durable state inside the ignored project-local runtime directory.
  SCRAPEFLOW_STATE_DIR: env.SCRAPEFLOW_STATE_DIR || ".scrapeflow",
};
const nextEnv = { ...env, PORT: String(nextPort) };

const children = [
  spawn("python3", ["-m", "local.simple_server"], { env: apiEnv, stdio: "inherit" }),
  spawn("npm", ["run", "dev", "--", "--port", String(nextPort)], {
    env: nextEnv,
    stdio: "inherit",
  }),
];

function isApiRequest(url) {
  return url === "/api" || url.startsWith("/api/");
}

function forwardRequest(req, res, targetPort, { preserveHost = false } = {}) {
  const headers = { ...req.headers };
  // The browser's Host must reach the API unchanged so its strict Origin/Host
  // same-origin check still works through this loopback development proxy.
  if (!preserveHost || !headers.host) headers.host = `127.0.0.1:${targetPort}`;
  const target = request({
    hostname: "127.0.0.1",
    port: targetPort,
    path: req.url || "/",
    method: req.method,
    headers,
  }, targetResponse => {
    res.writeHead(targetResponse.statusCode ?? 502, targetResponse.headers);
    targetResponse.pipe(res);
  });
  target.on("error", error => {
    if (!res.headersSent) res.writeHead(502, { "content-type": "text/plain; charset=utf-8" });
    res.end(`local proxy error: ${error.message}`);
  });
  req.pipe(target);
}

const proxy = createServer((req, res) => {
  const apiRequest = isApiRequest(req.url || "");
  forwardRequest(req, res, apiRequest ? apiPort : nextPort, { preserveHost: apiRequest });
});

// Next's development HMR uses a WebSocket upgrade.  Forward upgrades to the
// Next process while ordinary `/api` traffic goes to the Python service.
proxy.on("upgrade", (req, clientSocket, head) => {
  const target = request({
    hostname: "127.0.0.1",
    port: nextPort,
    path: req.url || "/",
    method: req.method,
    headers: { ...req.headers, host: `127.0.0.1:${nextPort}` },
  });
  target.on("upgrade", (targetResponse, targetSocket, targetHead) => {
    clientSocket.write(`HTTP/1.1 ${targetResponse.statusCode} ${targetResponse.statusMessage}\r\n`);
    for (const [name, value] of Object.entries(targetResponse.headers)) {
      if (Array.isArray(value)) {
        for (const item of value) clientSocket.write(`${name}: ${item}\r\n`);
      } else if (value !== undefined) {
        clientSocket.write(`${name}: ${value}\r\n`);
      }
    }
    clientSocket.write("\r\n");
    if (targetHead.length) clientSocket.write(targetHead);
    if (head.length) targetSocket.write(head);
    targetSocket.pipe(clientSocket).pipe(targetSocket);
  });
  target.on("error", () => clientSocket.destroy());
  target.end();
});

let closing = false;
function close(code = 0) {
  if (closing) return;
  closing = true;
  proxy.close();
  for (const child of children) {
    if (!child.killed) child.kill("SIGTERM");
  }
  const forceTimer = setTimeout(() => {
    for (const child of children) {
      if (!child.killed) child.kill("SIGKILL");
    }
    process.exit(code);
  }, 5000);
  forceTimer.unref();
  Promise.all(children.map(child => new Promise(resolve => {
    if (child.exitCode !== null || child.signalCode !== null) resolve();
    else child.once("exit", resolve);
  }))).then(() => process.exit(code));
}

proxy.on("error", error => {
  console.error(`本地 Web 代理启动失败: ${error.message}`);
  close(1);
});
proxy.listen(webPort, "127.0.0.1", () => {
  console.log(`ScrapeFlow 本地页面: http://127.0.0.1:${webPort}`);
});

for (const child of children) {
  child.on("exit", (code, signal) => {
    if (!closing) {
      console.error(`本地服务意外退出: ${signal ?? code ?? "unknown"}`);
      close(code === 0 ? 1 : (code ?? 1));
    }
  });
}

process.on("SIGINT", () => close(0));
process.on("SIGTERM", () => close(0));

console.log("ScrapeFlow 正在本地启动；API 与页面将通过同一地址提供。");
