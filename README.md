# ScrapeFlow

ScrapeFlow 是单用户、本机运行的 AList 影视库管理服务。长期产品与工程合同（A→P 主流程、门禁和补源纪律）以 [`AGENTS.md`](AGENTS.md) 为准；本 README 只说明当前操作入口。

## 当前运行模型（P15）

目标领域模型已经接入运行时：

```text
IntakeSource（只读发现）
  → RootJob（用户以来源 + 一级货架授权）
    → WorkUnit（独立作品单元）
      → 身份识别 → 三库五分类对账 → Planner / 单写器 / 精确回读
      → Gap 账本 → 严格三阶补源 → 同一 writer 闭环
```

标准入口分两步：

1. 入站监控在启用时只登记 `/quark/影视/待刮削` 的直接子目录为 `IntakeSource`；`GET /api/intake` 只读取已持久化的目录清单，不触发扫描。发现阶段不创建任务、不请求 TMDB、不做对账或写入。
2. 用户以 `POST /api/root-jobs` 同时提交来源路径和 `movie`、`anime` 或 `us_tv`，创建或恢复唯一 `RootJob`。随后才执行 B/W 边界分析、逐单元身份识别、三库五分类对账和写入。

对账结果是 `duplicate_complete`、`existing_gap`、`merge_existing`、`new_work` 或 `uncertain`。已有正式作品始终沿用已经验证的货架与作品根；RootJob 的货架只为新作品规划提供受限目标。身份或对账不确定时，只挂起相应 WorkUnit，不猜测身份、目标路径或删除动作。

P0–P14 的单元管线和根任务聚合均已接入。P15 的视频补源顺序固定为：

```text
quark_share → alist_offline → magnet
```

`quark_share` 只使用夸克分享快转；`alist_offline` 通过 AList 的 `aria2` offline-download tool 和专用 `offline-aria2` sidecar 直连下载，先转存到该 attempt 专属的 offline sibling，再由 materializer 只收拢已验证的预期文件到任务 staging；`magnet` 是本地 Torrent 兜底。Helper 不提供磁力提交或状态接口。所有补源产物都必须先进入任务专属 staging，再经过同一 Planner、单写器和回读验证；字幕缺口不占视频三阶。

`EngineJob` 仍是内部兼容执行载体，不再是顶层业务模型。`POST /api/jobs` 与 `/api/jobs/:id/start` 仅为 legacy 兼容入口；新功能应使用 RootJob/WorkUnit 路径。

每个 API 进程都会以 paused 状态启动。恢复自动执行前必须持久化一个精确 RootJob：先在 paused 时 `POST /api/control/pilot`，再由 `POST /api/control/resume` 执行只读 AList/aria2 preflight；空 selector、预检失败或预检期间控制状态变更都保持暂停。若进程重启前的持久记录仍是 unpaused，新进程的内存 startup fence 虽会显示 paused，仍须先 `POST /api/control/pause` 把**共享持久记录**写回 paused，才可 arm 或 resume；这避免第二个 API 进程改变正在运行的 pilot。`SCRAPEFLOW_ROOT_JOB_PILOT` 可作为 Compose 级的第二道精确 RootJob ceiling，与持久 scope 取交集，不能扩大范围，并会关闭全库自动审计。预检核对实际 AddURL offline sibling（`<staging>__offline__`）的最长 AList storage 挂载、启用/work 状态和已审阅的 Quark 上传能力；不会提交样本任务，所以 ready 仍不是传输成功证据。模板中的 intake、自动审计和自动补源 gate 均默认关闭；手动补源仍受 pause、scope 和 worker 门禁约束。

## 最少配置

复制环境文件并填写凭据：

```sh
cp .env.local.example .env.local
```

至少需要：

- `ALIST_PASSWORD`
- `TMDB_API_KEY`
- `SCRAPEFLOW_HOST_STATE_ROOT`（主机上的持久化状态目录）
- `SCRAPEFLOW_QUARK_HELPER_TOKEN`（随机生成、至少 24 个字符；不要保留模板占位值）

启动服务：

```sh
SCRAPEFLOW_BUILD_COMMIT="$(git rev-parse HEAD)" \
SCRAPEFLOW_BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
docker compose --env-file .env.local build api
docker compose --env-file .env.local up -d alist api pansou offline-aria2 quark-helper
python3 scripts/scrapeflow_runtime_readiness.py \
  --expected-commit <the-build-id-used-above>
```

构建命令会把提交号和 UTC 构建时间写入镜像 OCI labels 与 API 运行时；
`/api/health` 中的 build 字段与 `docker image inspect` 均可复核。未按上述方式
构建的镜像会明确显示 `unrecorded`，不能作为已验证版本使用。readiness 命令必须
显式给出 build id：使用至少 7 位小写 Git SHA（允许明确的 `-dirty` 后缀），且只接受
health 中以该 SHA 前缀开始、dirty 标记一致的实际 build id；空值、`unrecorded` 和无效
UTC build_time 都会失败。

在第一次启动 `offline-aria2` 前，操作者还必须在本机 `.env.local` 设置一个随机、非空的
`SCRAPEFLOW_ALIST_OFFLINE_ARIA2_RPC_SECRET`，并在 AList 的 offline-download `aria2`
工具配置中填入**完全相同**的 `aria2_secret`（地址为
`http://offline-aria2:6800/jsonrpc`）。该值仅传给 aria2 sidecar；不会进入 API 环境、
health 或 acceptance 证据。该 sidecar 位于专用 Compose bridge，只有 AList/API（及共享
API network namespace 的 Helper）能连接 RPC；bridge 不设 `internal`，以保留 aria2 的直连
下载出网能力。

### 代理与直连（Clash TUN 全局模式）

搜索层（TMDB、字幕站点、被墙索引）允许走代理；**BT 下载一律直连**（下载子进程会清空代理环境变量并开启 DHT/PEX/LPD）。夸克助手的夸克 API 调用跟随 `HTTP_PROXY`（代理路径对 drive.quark.cn 更稳定），CDP 渲染器连接与下载保持直连。在 TUN 全局代理下，需要给 Docker 网段加 DIRECT 规则，否则容器出站下载也会被代理截走：

```yaml
rules:
  - IP-CIDR,<actual-compose-subnet>,DIRECT,no-resolve
  - IP-CIDR,192.168.65.0/24,DIRECT,no-resolve # Docker Desktop VM 网段
```

Compose 没有固定 Docker 子网；先用 `docker network inspect <compose-project>_default` 取得当前 `IPAM.Config` 的 subnet，再替换上面的占位值。生产默认 project 是 `scrapeflow`；隔离验收使用其自己的 project 名。

Compose 的 API 进程入口是 `python3 -m local.simple_server`。容器内固定监听 `8765`，Compose 默认仅把它映射到宿主 <http://127.0.0.1:3010>；可用 `SCRAPEFLOW_API_PORT` 改宿主端口。API 容器连接 Compose 内部的 AList；媒体库根目录由 `SCRAPEFLOW_MEDIA_ROOT` 指定，默认是 `/quark/影视`。

同一镜像还会启动两动作 `quark-helper` sidecar：`health` 与 `share-save`。它只服务 `quark_share` 阶，与 API 共享网络命名空间，只监听共享的 `127.0.0.1:18765`，不发布第二个宿主端口。每次 `share-save` 才从匹配 `/quark` 的启用状态 Quark storage 临时解析 `addition.cookie` 和当前 AList 的 `root_folder_id`（兼容旧 `root_id`）；两者只保留在内存，不作为 Compose 环境变量、不落盘，也不出现在 health 响应、日志或验收证据中。Compose 会在 sidecar 异常退出时重拉，也会在显式重建 API 容器时同步重建 sidecar，避免它留在旧网络命名空间。第二阶由 AList 的 `aria2` offline-download tool 与专用 `offline-aria2` 服务完成：该服务直连下载，AList 先转存到 attempt 专属 offline sibling，materializer 再精确验证并把预期文件收进 staging。

提交第二阶前，API 会按**整个已核验 Torrent**的 `download_bytes`（不是本次缺口所需的文件和）检查与 `SCRAPEFLOW_HOST_STATE_ROOT` 共用的文件系统：默认单任务最多 32 GiB，且必须保留 `ceil(完整种子 × 1.15) + 20 GiB` 的可用空间。缺少完整大小、超上限或空间不足都会以同阶 infrastructure 停下，绝不先提交再赌磁盘；可通过 `SCRAPEFLOW_ALIST_OFFLINE_MAX_DOWNLOAD_BYTES` 与 `SCRAPEFLOW_ALIST_OFFLINE_MIN_FREE_BYTES` 在确认磁盘余量后调整。转存进入 transfer 状态后，默认 3600 秒总时限会持久化到 attempt；到期只有确认 AList 已取消后才删除任务并允许同阶重试，无法确认时保持 `waiting_reconcile`，不会重复提交。可用 `SCRAPEFLOW_ALIST_OFFLINE_TRANSFER_TIMEOUT` 调整该时限。

显式设置 `SCRAPEFLOW_INTAKE_MONITOR=1` 后，服务会按 `SCRAPEFLOW_INTAKE_SCAN_SECONDS` 轮询 `/quark/影视/待刮削/`，但只维护被动的 `IntakeSource` 目录清单：不创建 Job、不请求 TMDB、不做对账或写入。默认模板保持关闭；当前标准入口是用户通过 `POST /api/root-jobs` 为来源选择货架并授权 RootJob。

## 使用方式

1. 创建作品目录，例如 `/quark/影视/待刮削/作品名.年份/`，并放入视频、字幕或已有元数据。
2. 若已启用入站监控，等待它发现目录后用 `GET /api/intake` 查看结果；该 GET 不会主动扫描。默认监控关闭时可直接在下一步创建 RootJob，服务会登记该来源绑定。
3. 用 `POST /api/root-jobs` 一次提交来源路径与 `movie`、`anime` 或 `us_tv`，创建或恢复唯一 RootJob。
4. 在未明确授权执行前保持 API paused；授权后，RootJob 才会依次进行边界分析、逐单元身份识别、三库对账、规划、单写器执行与回读。身份或对账不确定时，只挂起对应 WorkUnit。
5. 用 `GET /api/jobs/:id/work-units`、`GET /api/jobs/:id` 和 `GET /api/jobs/:id/replenishment` 查看根任务、单元和缺口状态。

系统只清理本任务从入站目录移动的内容、任务创建的 staging 和明确归类的临时残留；已有正式媒体不会因名称推测而被删除。

## 对外 API

API 只暴露当前自动服务所需的操作：

```text
GET  /api/health
GET  /api/readiness/alist-offline
GET  /api/control
POST /api/control/pause
POST /api/control/pilot            {"root_job_id":"<root-job-id>"}
POST /api/control/resume           {"root_job_id":"<root-job-id>"}  # 或已 arm 后 {}
GET  /api/intake
POST /api/root-jobs               {"path":"/quark/影视/待刮削/作品目录","target_shelf":"movie|anime|us_tv"}
GET  /api/jobs
GET  /api/jobs/:id
GET  /api/jobs/:id/work-units
POST /api/jobs/:id/work-units/:unit/confirm
GET  /api/jobs/:id/replenishment
POST /api/jobs/:id/replenish      {}
POST /api/jobs/:id/retry
POST /api/jobs/:id/cancel
POST /api/jobs/:id/cleanup
POST /api/jobs/:id/repair-artifacts {}
GET  /api/library-audit/latest
POST /api/library-audit/run
GET  /api/browse?path=...&refresh=1

# legacy compatibility only
POST /api/jobs                    {"path":"/quark/影视/待刮削/作品目录"}
POST /api/jobs/:id/start          {"target_shelf":"movie|anime|us_tv"}
```

`GET /api/readiness/alist-offline` 只读取 AList 离线工具、管理员认证/task-manager、aria2 RPC/临时目录和目标 storage 路由；它不会创建目录、提交任务、下载、转存或删除任务。因此 `ready` 证明配置与可达性，**不**证明已完成真实文件传输。该诊断端点会以 HTTP 200 返回 `not_ready`/`unverified` 报告，故绝不能把 `curl -f` 的退出码当作就绪证明；必须使用上面的 runtime-readiness 命令，它要求 `status=ready` 和 `verified=true`。单任务试运行应在保持 paused 时先调用 `POST /api/control/pilot {"root_job_id":"…"}`；随后检查该 GET 和 `/api/health` 的 `automatic_scope`，只有 preflight 为 verified 才能 resume。自动全库审计投影在 single_root pilot 中保持关闭，避免它为非白名单根创建 Provider/retry 工作。

POST /api/root-jobs 是 A→P 合同的 S 步入口：一次提交来源与货架，创建或激活唯一 RootJob（同一来源幂等返回同一任务，已选货架不可更改），也是 Web 控制台“创建任务”面板调用的接口。已匹配的正式作品始终沿用既有货架/作品根；target_shelf 只为新作品规划提供受限目标。POST /api/jobs 与 POST /api/jobs/:id/start 仅保留为 legacy 兼容入口。

身份或对账结果为 uncertain 的 WorkUnit 会在其 identity_status 或 reconciliation_outcome 中保留 uncertain；根任务对外 phase 投影为 needs_attention。查看 GET /api/jobs/:id/work-units 后，只能通过 POST /api/jobs/:id/work-units/:unit/confirm 确认 tmdb_id、media_type（movie 或 tv）和可选 season；该入口不接受货架、路径、链接或 Provider 指令。GET /api/jobs/:id/work-units 返回单元和根任务聚合状态。

GET /api/jobs/:id/replenishment 是只读预览。POST /api/jobs/:id/replenish 可手动请求补源，但仍受 paused 状态和 worker 有效性保护；自动 provider/audit gate 默认关闭。生产补源只使用任务专属 staging /quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>。隔离验收只能把 SCRAPEFLOW_MEDIA_ROOT 设为精确的 /quark/影视/ScrapeFlow/验收/<run-id>，其 staging 只能派生为 <media-root>/ScrapeFlow/补源/<root-job-id>/<attempt-id>；内部 child 只投影到所属根任务。

POST /api/jobs/:id/repair-artifacts 只接受空 JSON 对象，并只重放已完成任务的确定性 NFO/海报计划；全库审计不会触发该写操作。

严格补源的第一阶只读取 PanSou 的 POST /api/search，随后用当前 AList 的夸克会话做只读递归清单核验；搜索结果本身不会直接成为可写候选。PanSou 是 Compose 内部服务，不发布宿主端口，只在内部网络以 http://pansou:8888 可达；模板默认仍是 SCRAPEFLOW_PANSOU_ENABLED=0，本机启用时在 .env.local 设 SCRAPEFLOW_PANSOU_ENABLED=1 与 SCRAPEFLOW_PANSOU_URL=http://pansou:8888。配置缺失、接口/会话失败、查询或链接被上限截断都会让任务停在 quark_share，不会伪造“没有候选”或推进到 alist_offline 或 magnet。

容器出口代理用 SCRAPEFLOW_HTTP_PROXY / SCRAPEFLOW_HTTPS_PROXY 配置，不继承宿主的 HTTP_PROXY：宿主值通常是 http://127.0.0.1:<port>，在容器内指向容器自身，会静默切断 TMDB 与 Torrent 搜索索引的全部出口，而 /api/health 仍报告 tmdb_configured: true。需要代理时填 http://host.docker.internal:<port>。

quark-helper 的固定 HTTP 合同只有两动作：health 与 share-save；它只支持 quark_share，绝不提交或查询夸克磁力离线任务。它只接受 Bearer 认证及与 API SCRAPEFLOW_MEDIA_ROOT 精确对应的任务 staging：生产为固定 /quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>，隔离验收为受限 run-id 根派生路径。Helper 不再作为 macOS 登录项或宿主 Python 后台进程运行；Compose 直接使用 API 同一镜像启动 typed sidecar。API 默认通过共享 loopback http://127.0.0.1:18765 访问它，Bearer token 必须在本机 .env.local 中显式配置且至少 24 个字符。

AList Quark storage 中的 cookie 由 sidecar 在每个 share-save 开始时临时解析，不复制到 Compose 模板、API 请求或持久状态；它不会出现在 health 响应、日志或验收证据中。sidecar 只通过固定 HTTPS API 发出 share-save；桌面夸克 renderer 只提供被动的 CDP/WSG 能力（加解密与能力探针），不接收 cookie，也不执行这些网络请求。sidecar 仍只被动附着固定 http://host.docker.internal:19222/json/list；它绝不启动、重启、激活或点击夸克。CDP/WSG 通道不可用时，quark_share 以 infrastructure 证据停在原阶，不会扫描其他端口或降级。

这一 Compose sidecar 拓扑取代了 2026-08-09 历史收敛计划中“宿主 Helper”的物理放置；当前合同是两动作 Helper、任务 staging、故障不降阶和 Helper 禁止控制 Quark。

桌面夸克本身的启动与重启是另一个宿主生命周期边界，不属于 Helper 两动作。为了使 CDP 参数每次一致，操作者可在 API paused 且活动操作归零后，安装一个直接运行 `QuarkCloudDrive` 的 macOS LaunchAgent：

```sh
python3 scripts/scrapeflow_quark_lifecycle.py --install-launch-agent --replace-running
python3 scripts/scrapeflow_quark_lifecycle.py --status
```

该 job 的参数只有夸克可执行文件、`--remote-debugging-address=127.0.0.1` 和 `--remote-debugging-port=19222`；后台进程是夸克本身，不是 Python Helper。安装替换已运行实例时，脚本通过 AppKit 发出正常退出请求，超时则停止并拒绝强杀。LaunchAgent 会在登录时启动、异常退出后重拉；用户正常退出夸克后会保持停止。可按需执行：

```sh
python3 scripts/scrapeflow_quark_lifecycle.py --start
python3 scripts/scrapeflow_quark_lifecycle.py --restart
# 只在 paused、操作归零且正常退出无法完成时：
python3 scripts/scrapeflow_quark_lifecycle.py --force-restart
```

`--restart` 仍是 AppKit 正常退出后重启；只有明确的 `--force-restart` 会让 launchd 强制替换它自己跟踪的那一个 job。sidecar 不会调用这些命令，CDP health 变为 503 也不会自动重启夸克。

## 可靠性边界

- 正式媒体库的 move、rename、upload 和 delete 共用一把本机写锁。
- 每次远端写入后都刷新相关父目录，并以精确路径、对象类型和字节数回读结果。
- 正式库视频在计划、写入与恢复回读三处均须通过最小字节准入（默认 1 MiB，硬下限 64 KiB）；补源本地 payload 还须通过 `ffprobe` 视频流核验后才能进入 staging。
- 补源 child 是 media-only：只回投视频，不新建或覆盖每集 NFO、海报；正式库已有的作品/季度元数据保持不动。
- 当显式开启 Provider/audit lane 时，`SCRAPEFLOW_REQUIRED_SUBTITLE_LANGUAGE`（默认示例为 `zh`）才会把侧车和内嵌字幕都缺少目标语言的情况交给补源流程；已有目标语言字幕不会重复写入。
- 内嵌字幕探针按次审计使用有界单调时间预算（默认 120 秒、最多 4 个并发）；预算内未完成的证据标记为 `unknown_subtitle_evidence`，不会误判为缺字幕。相同的字幕未知不会每 30 秒重复全库扫描，而是在新媒体提交或手动审计时再次核对。可用 `SCRAPEFLOW_SUBTITLE_PROBE_BUDGET_SECONDS`、`SCRAPEFLOW_SUBTITLE_PROBE_WORKERS` 和 `SCRAPEFLOW_SUBTITLE_PROBE_MAX_FILES` 调整。
- AList 离线任务按完整 Torrent 容量预检，默认最多 32 GiB，并在 15% 传输余量之外保留 20 GiB 空间；未通过容量预检不创建 AList 离线任务。转存 deadline 跨 API 重启保持有效，取消未获确认的任务保持 in-doubt，绝不当作可以重提。
- 新对象短暂不可见时，只对相关读取执行有界重试。
- 任务重启时会先根据 AList 当前状态重新核对，再继续或进入可重试的失败状态。
- 正式媒体库、AList 数据库和其他任务不属于当前任务的清理范围。
- 停服后的本机状态备份和隔离恢复演练见
  [离线备份与恢复演练](docs/scrapeflow-offline-backup.md)；正式媒体库恢复点必须由存储侧单独准备。
- 隔离真实验收按
  [验收记录模板](docs/scrapeflow-isolated-acceptance-record.md) 填写；部署与开启顺序见
  [部署与开启顺序](docs/scrapeflow-deployment-open-order.md)。
- 从干净 commit 生成完整发布原始证据：
  `python3 scripts/scrapeflow_release_evidence.py --output-dir artifacts/release`。
  本机验收包草稿可用 `python3 scripts/scrapeflow_acceptance_package.py --output /tmp/scrapeflow-acceptance-package.md --release-evidence artifacts/release/scrapeflow-release-evidence.json`
  生成；启动后应追加 `--preflight-report preflight-report.json`
  与 `--runtime-readiness-report readiness.json`。如同时提供
  `--preflight-declaration declaration.json`，验收包会要求它与报告内固化的声明完全一致。
  只提供 `--preflight-declaration` 是兼容模式，会对当前目录执行 live recheck。
  真实样本仍必须在独立环境中手工补证。
- 隔离环境声明可用 `python3 scripts/scrapeflow_isolated_preflight.py --template` 生成模板，
  并在独立 runtime 首次启动前执行
  `python3 scripts/scrapeflow_isolated_preflight.py declaration.json --report preflight-report.json`。
  该报告原子固化当时的空目录检查；启动后目录非空不会把已固化的通过误判为失败。
  报告属于本机 self-attested evidence：验收包会校验目录身份、备份 manifest
  完整性并记录报告 SHA-512，但它不是外部签名或密码学不可伪造证明。
- 隔离 API 启动后的只读核对必须显式给出已构建的 build id：
  `python3 scripts/scrapeflow_runtime_readiness.py --api-url http://127.0.0.1:<isolated-api-port> --expected-commit <build-id>`；
  `<isolated-api-port>` 必须是该隔离 Compose project 的 `SCRAPEFLOW_API_PORT`，不能沿用生产默认 `3010`。

文档权威层级如下：[`AGENTS.md`](AGENTS.md) 是唯一的长期产品与工程合同（含目标架构与 A→P 主流程）；当前实现事实与已知差距以源码、测试和 Git 工作树证据核对。本 README、[部署与开启顺序](docs/scrapeflow-deployment-open-order.md) 和[验收记录模板](docs/scrapeflow-isolated-acceptance-record.md)是当前操作入口，但不覆盖 AGENTS。明确标为“历史参考”的收敛计划、旧目标货架计划、RC 与阶段快照只保留史料，不授权解除全局暂停或开放自动执行。

## 检查

```sh
python3 scripts/scrapeflow_release_check.py
```

日常跑测试请用带 pytest 的解释器；通过数不得低于 AGENTS.md 记录的冻结基线，以本次命令输出作为实际计数：

```sh
/opt/homebrew/bin/python3.12 -m pytest local/tests/ -q
# 注意：不要用系统默认 python3（本机已升到 3.14，未装 pytest），
# 也不要先 source .env.local 再跑测试（代理/搜索开关会污染环境变量，导致 lane-gate 测试失败）
```

发布前补充按顺序运行：

```sh
SCRAPEFLOW_IGNORE_LOCAL_ENV=1 PYTHONDONTWRITEBYTECODE=1 \
  /opt/homebrew/bin/python3.12 -m unittest discover -s local/tests -p 'test_*.py'
git status --porcelain  # 必须为空
git diff --check
env -i PATH="$PATH" HOME="$HOME" SCRAPEFLOW_HOST_STATE_ROOT=/tmp/scrapeflow-state \
  docker compose config
docker build -f Dockerfile.api .
```

端到端测试不得加载生产 `.env.local`、生产 AList URL 或真实凭据，也不得把
`/quark/影视/待刮削`、`/quark/影视/ScrapeFlow/补源` 当作测试夹具。请使用
`SCRAPEFLOW_IGNORE_LOCAL_ENV=1`、临时状态目录和注入的 fake/in-memory AList；
若必须测试 HTTP，应为测试进程单独启动隔离的 AList 命名空间，并在结束时清理。
