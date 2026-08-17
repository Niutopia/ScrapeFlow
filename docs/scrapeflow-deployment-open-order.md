# ScrapeFlow 受控部署与开启顺序（P15）

本文件是当前部署 runbook，不是生产授权书；自动审计和自动补源是否开启，必须由用户在真实验收后单独决定。长期行为以 [AGENTS.md](../AGENTS.md) 为准。P15 视频补源固定为 `quark_share → alist_offline → magnet`：Helper 只有 `health` 与 `share-save` 两个动作，第二阶由 AList 的 `aria2` offline-download tool 和专用 `offline-aria2` 服务执行。

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
2. 使用隔离或生产指定状态目录启动。隔离实例必须在每个 Compose 命令显式使用独立 project（例如 `-p scrapeflow-acceptance-<run-id>`），并另用 `SCRAPEFLOW_HOST_STATE_ROOT`、`SCRAPEFLOW_ALIST_PORT`、`SCRAPEFLOW_API_PORT` 和 `SCRAPEFLOW_API_IMAGE`，不能共享主实例的 state、临时目录、端口或镜像标签。
3. 每个 API 进程都会以 paused 状态启动；保持模板的 `SCRAPEFLOW_START_PAUSED=1` 作为保守部署意图，但不要把它当作恢复执行的开关。
4. 保持 `SCRAPEFLOW_INTAKE_MONITOR=0`。
5. 保持 `SCRAPEFLOW_AUTOMATIC_AUDIT=0`。
6. 保持 `SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED=0`。
7. 保持 `SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED=0`。
8. 保持 `SCRAPEFLOW_PROVIDER_WORKERS=1`。
9. 若本机夸克尚未纳入固定 CDP 生命周期，先确认已有 API 为 paused 且活动操作归零，再由操作者执行 `python3 scripts/scrapeflow_quark_lifecycle.py --install-launch-agent --replace-running`。它通过 AppKit 正常退出当前的单一夸克主进程，然后由 Aqua LaunchAgent 直接以固定 `127.0.0.1:19222` 参数启动夸克。
10. 用 `python3 scripts/scrapeflow_quark_lifecycle.py --status` 和只读 CDP 核验确认只有一个夸克主进程，且 `127.0.0.1:19222` 的 listener 归属该 PID。
11. 在本机 `.env.local` 中配置 AList 管理员账号，并为 API 和 `quark-helper` sidecar 配置同一个至少 24 个字符的 Bearer token；不记录或提交真实值。Sidecar 只通过 Compose 内部 `http://alist:5244` 读取匹配 `/quark` 的 storage，`addition.cookie` 与 AList v3.62 的 `root_folder_id`（兼容旧 `root_id`）只在每次 `share-save` 期间保存在内存，不进入 env、health 响应、日志或验收证据。
12. 生产实例使用 `docker compose --env-file .env.local up -d alist api pansou offline-aria2 quark-helper` 启动。隔离验收使用 `docker compose -p scrapeflow-acceptance-<run-id> --env-file .env.local up -d alist api pansou offline-aria2 quark-helper`；后续 `ps`、日志、停止和清理命令必须复用同一个 `-p`。Helper 与 API 共享网络命名空间，只监听共享 `127.0.0.1:18765`，不发布宿主端口；API 不等待 Helper ready 才启动。Compose 显式重建 API 时必须同步重建 sidecar，不允许它留在旧 network namespace。验收 AList 必须全新初始化，只含一个挂载 `/quark` 的专用 Quark storage；其 `root_folder_id` 必须指向验收专用的物理目录，绝不克隆或启动生产 AList 数据。
13. 在 AList 中确认已实际配置可用的离线下载工具 `aria2`；Compose 只启动 aria2 daemon，不会替 AList 写入 tool 配置。确认 `offline-aria2` 直连运行并带 `--file-allocation=none`。这个设置只影响之后创建的 aria2 任务，不能追溯改变已有 GID 的预分配方式。真实 AList 交付必须先落在 attempt 专属 offline sibling，materializer 只将已验证的预期文件收进 staging。提交前容量门禁按完整 Torrent（含本次不收拢的 extras）计算：默认每任务至多 32 GiB，并要求共享 `SCRAPEFLOW_HOST_STATE_ROOT` 文件系统保留 `ceil(完整种子 × 1.15) + 20 GiB`。仅在已核验空间后才调整 `SCRAPEFLOW_ALIST_OFFLINE_MAX_DOWNLOAD_BYTES` 或 `SCRAPEFLOW_ALIST_OFFLINE_MIN_FREE_BYTES`；缺大小、超上限或空间不足必须同阶 retry，不得提交后清盘。转存 total deadline 默认 3600 秒（`SCRAPEFLOW_ALIST_OFFLINE_TRANSFER_TIMEOUT`）；取消未被 AList 确认时保持 in-doubt，对账而不重提。
14. Helper 只被动连接宿主已存在的 `host.docker.internal:19222/json/list` CDP；renderer 仅提供 WSG 能力，sidecar 只发出 `share-save`，不启动、重启、激活或点击夸克，也不提交或查询夸克磁力离线任务。CDP/WSG 不可用时，`quark_share` 必须 fail-closed 并停在原阶。
15. 对隔离验收，将 `SCRAPEFLOW_MEDIA_ROOT` 设为精确的 `/quark/影视/ScrapeFlow/验收/<run-id>`；API 与 Provider 会共同派生 `<media-root>/ScrapeFlow/补源`，不能用任意路径替代。该逻辑根必须经上述专用 AList storage 映射到独立物理媒体根。
16. 为本次启动注入完整 `SCRAPEFLOW_BUILD_COMMIT` 和 UTC `SCRAPEFLOW_BUILD_TIME`，启动后使用 `--expected-commit` 核验 health。
17. 不恢复旧 backlog。
18. 不批量 retry。
19. 不批量 cleanup。

## 启动后核对

```sh
python3 scripts/scrapeflow_runtime_readiness.py \
  --api-url http://127.0.0.1:3010 \
  --expected-commit <git-commit>
curl -fsS http://127.0.0.1:3010/api/health
curl -fsS http://127.0.0.1:3010/api/control

# 隔离实例：把三个 3010 都替换为该实例的 SCRAPEFLOW_API_PORT，
# 且只对与 -p scrapeflow-acceptance-<run-id> 对应的 API 执行检查。
```

必须确认：

- API 只暴露在本机入口。
- Compose 服务列表包含 `alist`、`api`、`pansou`、`offline-aria2` 和无发布端口的 `quark-helper`。
- health 中的 build 信息符合本次 commit。
- AList 可用。
- TMDB 可用。
- Quark Helper readiness 符合本次验收目标。
- 夸克主进程 argv 只包含固定的两个 CDP 参数，CDP page target 属于 Quark renderer。
- control 为 paused。
- intake、audit、provider 自动 gate 均关闭。
- provider worker 为 1。
- AList 离线容量参数与本机可用空间符合本次隔离样本；不得仅按被选中的缺口文件估算。

## 开启顺序

1. 用户验收普通入库：电影、番剧归档、美剧季度目录、错误密码、冲突、cancel、重启。
2. 用户验收手工 report-only 审计。
3. 只开启自动审计，Provider 仍关闭。
4. 使用精确 TMDB/gap pilot 开启一个补源任务。
5. 只有得到单独授权后，才向**隔离实例**的 `POST /api/control/resume` 放行实际样本；这不是环境变量开关。每个样本结束、修改配置或进入回退前都重新 `POST /api/control/pause`。
6. 验证 `quark_share → alist_offline → magnet` 的三阶补源样本和负例。
7. 用户确认后，才允许移除 pilot 限制。
8. 用户再次确认后，才允许全局 Provider。

## 禁止事项

- 不在 paused 状态下启动自动 backlog。
- 不把旧 gap/staging 批量删除成“干净状态”。
- 不在 quark_share/Helper 故障时推进到 alist_offline 或本地 Torrent；AList 或 Torrent 的基础设施故障同样保持原阶。
- 不在 in-doubt 状态重复提交。
- 不让 Provider、Helper 或 aria2 决定正式库位置。
- 不让审计扫描修复 NFO 或海报。
- 不在活动操作尚未归零时重启夸克；正常 `--restart` 失败时也不自动升级为 `--force-restart`。

## 回退

1. 对正确的生产或隔离 API endpoint pause；隔离实例的 Compose 操作继续使用它启动时的同一个 `-p`。
2. 等待活动操作归零。
3. 停 `quark-helper`、`offline-aria2` 和 `pansou`。
4. 停 API。
5. 停 AList。
6. 如需停止宿主夸克生命周期，执行 `python3 scripts/scrapeflow_quark_lifecycle.py --uninstall-launch-agent`；该操作只卸载精确 label 和 plist。
7. 使用离线备份和正式媒体库外部恢复点恢复。
8. 恢复实例必须先以 paused 启动。
9. 复核 `/api/health` 和 `/api/control` 后再决定是否重试单个任务。
