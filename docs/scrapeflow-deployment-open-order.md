# ScrapeFlow 受控部署与开启顺序

本文件是当前部署 runbook，不是生产授权书；自动审计和自动补源是否开启，必须由用户在真实验收后单独决定。长期行为以 [AGENTS.md](../AGENTS.md) 为准。视频补源固定为 `quark_share → magnet`：Helper 只有 `health` 与 `share-save` 两个动作。本项目已撤除 AList 离线下载，因为 AList v3 不暴露 `select-file` 且提交时会重新拉取 URL，无法证明“只下载缺口成员”。

## 构建

1. 确认工作树干净。
2. 运行：

```sh
python3 scripts/scrapeflow_release_evidence.py --output-dir artifacts/release
```

3. 保存 `artifacts/release/scrapeflow-release-evidence.json` 与同目录原始日志；验收包必须通过 `--release-evidence` 引用该 JSON。
4. 使用以下方式从干净候选构建，使 Git commit 和 UTC 构建时间同时写入镜像 OCI labels
   与 API 运行时（没有这些值的镜像会显示 `unrecorded`，不能作为验收版本）。若本次
   构建确实含未提交变更，记录实际的短 SHA 加 `-dirty`，而不是把它伪装成 clean SHA：

```sh
SCRAPEFLOW_BUILD_COMMIT="$(git rev-parse HEAD)" \
SCRAPEFLOW_BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
docker compose --env-file .env.local build api
```
5. 不把 `.env.local`、状态目录、备份目录或媒体文件加入提交。

## 启动

1. 使用干净 commit 构建镜像。
2. 使用隔离或生产指定状态目录启动。隔离实例必须在每个 Compose 命令显式使用独立 project（例如 `-p scrapeflow-acceptance-<run-id>`），并另用 `SCRAPEFLOW_HOST_STATE_ROOT`、`SCRAPEFLOW_ALIST_PORT`、`SCRAPEFLOW_API_PORT` 和 `SCRAPEFLOW_API_IMAGE`，不能共享主实例的 state、临时目录、端口或镜像标签。
3. 每个 API 进程都会以 paused 状态启动；保持模板的 `SCRAPEFLOW_START_PAUSED=1` 作为保守部署意图，但不要把它当作恢复执行的开关。若重建前共享 `global-control.json` 是 unpaused，新进程的 startup fence 不会自动改写它：在 arm/resume 任何 RootJob 前，先 `POST /api/control/pause` 使持久记录也成为 paused。单 RootJob 试运行时，在启动/重建 API 前把 `SCRAPEFLOW_ROOT_JOB_PILOT` 设为那个已知 RootJob id；空值不授权任何自动任务，非空值只会与 API 持久 scope 取交集，不能扩大范围。
4. 保持 `SCRAPEFLOW_INTAKE_MONITOR=0`。
5. 保持 `SCRAPEFLOW_AUTOMATIC_AUDIT=0`。
6. 保持 `SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED=0`。
7. 保持 `SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED=0`。
8. 保持 `SCRAPEFLOW_PROVIDER_WORKERS=1`。
9. 若本机夸克尚未纳入固定 CDP 生命周期，先确认已有 API 为 paused 且活动操作归零，再由操作者执行 `python3 scripts/scrapeflow_quark_lifecycle.py --install-launch-agent --replace-running`。它通过 AppKit 正常退出当前的单一夸克主进程，然后由 Aqua LaunchAgent 直接以固定 `127.0.0.1:19222` 参数启动夸克。
10. 用 `python3 scripts/scrapeflow_quark_lifecycle.py --status` 和只读 CDP 核验确认只有一个夸克主进程，且 `127.0.0.1:19222` 的 listener 归属该 PID。
11. 在本机 `.env.local` 中配置 AList 管理员账号，并为 API 和 `quark-helper` sidecar 配置同一个至少 24 个字符的 Bearer token；不记录或提交真实值。Sidecar 只通过 Compose 内部 `http://alist:5244` 读取匹配 `/quark` 的 storage，`addition.cookie` 与 AList v3.62 的 `root_folder_id`（兼容旧 `root_id`）只在每次 `share-save` 期间保存在内存，不进入 env、health 响应、日志或验收证据。
12. 生产实例使用 `docker compose --env-file .env.local up -d alist api pansou quark-helper` 启动。隔离验收使用 `docker compose -p scrapeflow-acceptance-<run-id> --env-file .env.local up -d alist api pansou quark-helper`；后续 `ps`、日志、停止和清理命令必须复用同一个 `-p`。Helper 与 API 共享网络命名空间，只监听共享 `127.0.0.1:18765`，不发布宿主端口；API 不等待 Helper ready 才启动。Compose 显式重建 API 时必须同步重建 sidecar，不允许它留在旧 network namespace。验收 AList 必须全新初始化，只含一个挂载 `/quark` 的专用 Quark storage；其 `root_folder_id` 必须指向验收专用的物理目录，绝不克隆或启动生产 AList 数据。
13. Helper 只被动连接宿主已存在的 `host.docker.internal:19222/json/list` CDP；renderer 仅提供 WSG 能力，sidecar 只发出 `share-save`，不启动、重启、激活或点击夸克，也不提交或查询夸克磁力离线任务。CDP/WSG 不可用时，`quark_share` 必须 fail-closed 并停在原阶。
14. 对隔离验收，将 `SCRAPEFLOW_MEDIA_ROOT` 设为精确的 `/quark/影视/ScrapeFlow/验收/<run-id>`；API 与 Provider 会共同派生 `<media-root>/ScrapeFlow/补源`，不能用任意路径替代。该逻辑根必须经上述专用 AList storage 映射到独立物理媒体根。
15. 确认本次**构建**已写入完整 `SCRAPEFLOW_BUILD_COMMIT` 和 UTC `SCRAPEFLOW_BUILD_TIME`；启动不以空 runtime env 覆盖镜像标识。启动后必须使用 `--expected-commit <本次 build id>` 核验 health，并核对 build_time。build id 必须是至少 7 位小写 SHA（可带 `-dirty`）；检查只允许 health 的实际 SHA 以该 expected SHA 为前缀，绝不反向模糊匹配；空值、`unrecorded` 或无效 UTC 时间都拒绝验收。
16. 不恢复旧 backlog。
17. 不批量 retry。
18. 不批量 cleanup。

## 启动后核对

```sh
python3 scripts/scrapeflow_runtime_readiness.py \
  --api-url http://127.0.0.1:3010 \
  --expected-commit <this-build-id>
curl -fsS http://127.0.0.1:3010/api/health
curl -fsS http://127.0.0.1:3010/api/control

# 隔离实例：把三个 3010 都替换为该实例的 SCRAPEFLOW_API_PORT，
# 且只对与 -p scrapeflow-acceptance-<run-id> 对应的 API 执行检查。
```

必须确认：

- API 只暴露在本机入口。
- Compose 服务列表包含 `alist`、`api`、`pansou` 和无发布端口的 `quark-helper`。
- health 中的 build 信息符合本次 commit。
- AList 可用。
- TMDB 可用。
- Quark Helper readiness 符合本次验收目标。
- 夸克主进程 argv 只包含固定的两个 CDP 参数，CDP page target 属于 Quark renderer。
- control 为 paused。
- health 的 `automatic_scope` 为 `none`，或为本次唯一 RootJob 的 `single_root`；`SCRAPEFLOW_ROOT_JOB_PILOT` 非空时必须与后者相同。
- intake、audit、provider 自动 gate 均关闭。
- provider worker 为 1。
- 本地 Magnet 候选已证明每个缺口只映射到所选 torrent 成员；不得因整季包或花絮扩大下载范围。

## 开启顺序

1. 用户验收普通入库：电影、番剧归档、美剧季度目录、错误密码、冲突、cancel、重启。
2. 用户验收手工 report-only 审计。
3. 只开启自动审计，Provider 仍关闭。
4. 在仍然 paused 时创建/确认唯一 RootJob，记录它的 id；如使用 Compose ceiling，确认 `.env.local` 的 `SCRAPEFLOW_ROOT_JOB_PILOT` 与该 id 完全相同后重建仍 paused 的 API。
5. 预设而不恢复：`POST /api/control/pilot {"root_job_id":"<root-job-id>"}`。随后核对 `/api/control` 与 `/api/health` 都显示这个 exact scope；其他 RootJob 不得 queue、retry 或进入 Provider。
6. 只有得到单独授权后，才向**隔离实例**的 `POST /api/control/resume` 放行实际样本；这不是环境变量开关。每个样本结束、修改配置或进入回退前都重新 `POST /api/control/pause`。
7. 验证 `quark_share → magnet` 的补源样本和负例，确认 Magnet 只选择当前缺口对应的 torrent 成员。
8. 用户确认后，才允许移除 pilot 限制。
9. 用户再次确认后，才允许全局 Provider。

## 禁止事项

- 不在 paused 状态下启动自动 backlog。
- 不把旧 gap/staging 批量删除成“干净状态”。
- 不在 quark_share/Helper 故障时推进到本地 Torrent；Torrent 基础设施故障同样保持原阶。
- 不在 in-doubt 状态重复提交。
- 不让 Provider、Helper 或下载器决定正式库位置。
- 不让审计扫描修复 NFO 或海报。
- 不在活动操作尚未归零时重启夸克；正常 `--restart` 失败时也不自动升级为 `--force-restart`。

## 回退

1. 对正确的生产或隔离 API endpoint pause；隔离实例的 Compose 操作继续使用它启动时的同一个 `-p`。
2. 等待活动操作归零。
3. 停 `quark-helper` 和 `pansou`。
4. 停 API。
5. 停 AList。
6. 如需停止宿主夸克生命周期，执行 `python3 scripts/scrapeflow_quark_lifecycle.py --uninstall-launch-agent`；该操作只卸载精确 label 和 plist。
7. 使用离线备份和正式媒体库外部恢复点恢复。
8. 恢复实例必须先以 paused 启动。
9. 复核 `/api/health` 和 `/api/control` 后再决定是否重试单个任务。
