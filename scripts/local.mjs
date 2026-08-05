import { spawn } from "node:child_process";

const env = { ...process.env };
const children = [
  spawn("python3", ["local/server.py"], { env, stdio: "inherit" }),
  spawn("npm", ["run", "dev"], { env, stdio: "inherit" }),
];

let closing = false;
function close(code = 0) {
  if (closing) return;
  closing = true;
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

console.log("ScrapeFlow 正在本地启动；页面地址将在下方显示。");
