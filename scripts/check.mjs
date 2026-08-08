#!/usr/bin/env node
/** Run the checks that exercise the current application. */

import { spawn } from "node:child_process";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const runtimeRoot = join(repository, ".runtime");
await mkdir(runtimeRoot, { recursive: true, mode: 0o700 });
const temporaryRoot = await mkdtemp(join(runtimeRoot, "check-"));
const stateRoot = join(temporaryRoot, "state");
const stagingRoot = join(temporaryRoot, "web");
const pythonCache = join(temporaryRoot, "pycache");

const environment = {
  ...process.env,
  PYTHONDONTWRITEBYTECODE: "1",
  PYTHONPYCACHEPREFIX: pythonCache,
  SCRAPEFLOW_STATE_DIR: stateRoot,
  SCRAPEFLOW_IGNORE_LOCAL_ENV: "1",
  SCRAPEFLOW_WEB_STAGING_ROOT: stagingRoot,
  SCRAPEFLOW_BUILD_ROOT: stagingRoot,
  ALIST_URL: "http://127.0.0.1:9",
  ALIST_USERNAME: "isolated-test",
  ALIST_PASSWORD: "isolated-test-not-a-secret",
  TMDB_API_KEY: "isolated-test-not-a-secret",
  SCRAPEFLOW_REPLENISHMENT_ADAPTER: "",
  SCRAPEFLOW_REPLENISHMENT_CATALOG: "",
  SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH: "0",
  SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH: "0",
  SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH: "0",
  SCRAPEFLOW_REPLENISHMENT_SUBSPLEASE_SEARCH: "0",
  SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH: "0",
  SCRAPEFLOW_REPLENISHMENT_DMHY_SEARCH: "0",
  SCRAPEFLOW_REPLENISHMENT_NYAA_SEARCH: "0",
};

const commands = [
  ["npm", "run", "lint"],
  ["npm", "run", "typecheck"],
  ["python3", "-c", "import local.simple_server"],
  ["python3", "-m", "unittest", "discover", "-s", "local/tests", "-p", "test_*.py"],
  ["npm", "run", "build:check"],
];

function run(argv) {
  return new Promise((accept, reject) => {
    const child = spawn(argv[0], argv.slice(1), {
      cwd: repository,
      env: environment,
      stdio: "inherit",
    });
    child.on("error", reject);
    child.on("close", code => code === 0
      ? accept()
      : reject(new Error(`${argv.join(" ")} exited with ${code ?? 1}`)));
  });
}

try {
  for (const command of commands) await run(command);
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
