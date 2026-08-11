# ScrapeFlow 部署与开启顺序

本文件用于阶段 11。它不是生产授权书；自动审计和自动补源是否开启，必须由用户在真实验收后单独决定。2026-08-11 用户明确批准将 typed Quark Helper 从宿主后台改为 Compose sidecar；这只修订物理部署位置，不改变四动作、staging 边界或故障不降阶合同。

## 构建

1. 确认工作树干净。
2. 运行：

```sh
python3 scripts/scrapeflow_release_evidence.py --output-dir artifacts/release
```

3. 保存 `artifacts/release/scrapeflow-release-evidence.json` 与同目录原始日志；验收包必须通过 `--release-evidence` 引用该 JSON。
4. 记录 Git commit 和构建时间。
5. 不把 `.env.local`、状态目录、备份目录或媒体文件加入提交。

## 启动

1. 使用干净 commit 构建镜像。
2. 使用隔离或生产指定状态目录启动。
3. 保持 `SCRAPEFLOW_START_PAUSED=1`。
4. 保持 `SCRAPEFLOW_INTAKE_MONITOR=0`。
5. 保持 `SCRAPEFLOW_AUTOMATIC_AUDIT=0`。
6. 保持 `SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED=0`。
7. 保持 `SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED=0`。
8. 保持 `SCRAPEFLOW_PROVIDER_WORKERS=1`。
9. 若本机夸克尚未纳入固定 CDP 生命周期，先确认已有 API 为 paused 且活动操作归零，再由操作者执行 `python3 scripts/scrapeflow_quark_lifecycle.py --install-launch-agent --replace-running`。它通过 AppKit 正常退出当前的单一夸克主进程，然后由 Aqua LaunchAgent 直接以固定 `127.0.0.1:19222` 参数启动夸克。
10. 用 `python3 scripts/scrapeflow_quark_lifecycle.py --status` 和只读 CDP 核验确认只有一个夸克主进程，且 `127.0.0.1:19222` 的 listener 归属该 PID。
11. 在本机 `.env.local` 中为 API 和 `quark-helper` sidecar 配置同一个至少 24 个字符的 Bearer token；不记录或提交真实值。
12. 使用 `docker compose --env-file .env.local up -d alist api quark-helper` 启动。Helper 与 API 共享网络命名空间，只监听共享 `127.0.0.1:18765`，不发布宿主端口；API 不等待 Helper ready 才启动。Compose 显式重建 API 时必须同步重建 sidecar，不允许它留在旧 network namespace。
13. Helper 只被动连接宿主已存在的 `host.docker.internal:19222/json/list` CDP，不启动、重启、激活或点击夸克；CDP/WSG 不可用时必须 fail-closed。
14. 不恢复旧 backlog。
15. 不批量 retry。
16. 不批量 cleanup。

## 启动后核对

```sh
python3 scripts/scrapeflow_runtime_readiness.py \
  --api-url http://127.0.0.1:8765 \
  --expected-commit <git-commit>
curl -fsS http://127.0.0.1:8765/api/health
curl -fsS http://127.0.0.1:8765/api/control
```

必须确认：

- API 只暴露在本机入口。
- Compose 服务列表包含 `alist`、`api` 和无发布端口的 `quark-helper`。
- health 中的 build 信息符合本次 commit。
- AList 可用。
- TMDB 可用。
- Quark Helper readiness 符合本次验收目标。
- 夸克主进程 argv 只包含固定的两个 CDP 参数，CDP page target 属于 Quark renderer。
- control 为 paused。
- intake、audit、provider 自动 gate 均关闭。
- provider worker 为 1。

## 开启顺序

1. 用户验收普通入库：电影、番剧归档、美剧季度目录、错误密码、冲突、cancel、重启。
2. 用户验收手工 report-only 审计。
3. 只开启自动审计，Provider 仍关闭。
4. 使用精确 TMDB/gap pilot 开启一个补源任务。
5. 验证三阶补源样本和负例。
6. 用户确认后，才允许移除 pilot 限制。
7. 用户再次确认后，才允许全局 Provider。

## 禁止事项

- 不在 paused 状态下启动自动 backlog。
- 不把旧 gap/staging 批量删除成“干净状态”。
- 不在 Helper 故障时降到本地 Torrent。
- 不在 in-doubt 状态重复提交。
- 不让 Provider、Helper 或 aria2 决定正式库位置。
- 不让审计扫描修复 NFO 或海报。
- 不在活动操作尚未归零时重启夸克；正常 `--restart` 失败时也不自动升级为 `--force-restart`。

## 回退

1. pause。
2. 等待活动操作归零。
3. 停 `quark-helper`。
4. 停 API。
5. 停 AList。
6. 如需停止宿主夸克生命周期，执行 `python3 scripts/scrapeflow_quark_lifecycle.py --uninstall-launch-agent`；该操作只卸载精确 label 和 plist。
7. 使用阶段 8 的离线备份和正式媒体库外部恢复点恢复。
8. 恢复实例必须先以 paused 启动。
9. 复核 `/api/health` 和 `/api/control` 后再决定是否重试单个任务。
