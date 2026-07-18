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
  setTimeout(() => process.exit(code), 300);
}

for (const child of children) {
  child.on("exit", (code, signal) => {
    if (!closing && code !== 0) {
      console.error(`本地服务异常退出: ${signal ?? code}`);
      close(code ?? 1);
    }
  });
}

process.on("SIGINT", () => close(0));
process.on("SIGTERM", () => close(0));

console.log("ScrapeFlow 正在本地启动；页面地址将在下方显示。");
