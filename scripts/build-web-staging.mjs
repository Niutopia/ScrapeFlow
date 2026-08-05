#!/usr/bin/env node
/** Build Web in a disposable project-local directory for repository checks. */

import { cp, lstat, mkdir, rm, symlink } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const runtimeRoot = join(repository, ".runtime");
const staging = resolve(
  process.env.SCRAPEFLOW_WEB_STAGING_ROOT || join(runtimeRoot, "web-check"),
);
const relativeStaging = relative(runtimeRoot, staging);

if (
  !relativeStaging || relativeStaging === "." || relativeStaging === ".."
  || relativeStaging.startsWith(`..${sep}`)
) {
  throw new Error(`unsafe Web staging root outside .runtime: ${staging}`);
}
try {
  if ((await lstat(staging)).isSymbolicLink()) {
    throw new Error(`unsafe symlink Web staging root: ${staging}`);
  }
} catch (error) {
  if (error?.code !== "ENOENT") throw error;
}

await rm(staging, { recursive: true, force: true });
await mkdir(staging, { recursive: true, mode: 0o700 });

for (const source of ["app", "public"]) {
  await cp(join(repository, source), join(staging, source), { recursive: true });
}
for (const source of [
  "next.config.ts", "tsconfig.json", "postcss.config.mjs",
  "package.json", "package-lock.json",
]) {
  await cp(join(repository, source), join(staging, source));
}
await symlink(join(repository, "node_modules"), join(staging, "node_modules"), "dir");

const result = spawnSync(join(repository, "node_modules", ".bin", "next"), ["build"], {
  cwd: staging,
  env: {
    ...process.env,
    NEXT_TELEMETRY_DISABLED: "1",
    SCRAPEFLOW_TURBOPACK_ROOT: repository,
  },
  stdio: "inherit",
});
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);

console.log(`Web check build: ${join(staging, "out")}`);
