# ScrapeFlow

ScrapeFlow 是一个单用户、本机运行的 AList 影视库管理服务。长期产品与工程合同(含
A→P 唯一主流程、门禁与补源纪律)见 [`AGENTS.md`](AGENTS.md)。当前 HEAD
的实现事实与已知差距以源码、测试和 Git 工作树证据核对。

目标普通入站流程先对来源做只读身份识别，并与电影、番剧、美剧正式库对账；结果只能是
`duplicate_complete`、`existing_gap`、`merge_existing`、`new_work` 或 `uncertain`。已有
正式作品继承其货架和作品根；只有确认为 `new_work` 且需要首次一级货架确认时，用户才选择
`movie`、`anime` 或 `us_tv`。身份不确定时安全停止，不猜测身份、货架、目标路径、删除或
Provider 降级。Provider 自动补源和自动审计保持默认关闭。

```text
待刮削作品目录
  → 只读身份识别 + 对电影 / 番剧 / 美剧正式库对账
  → duplicate_complete | existing_gap | merge_existing | new_work | uncertain
  → duplicate_complete：在身份与完整性充分确定后，仅清理任务拥有的输入
  → existing_gap：沿用匹配正式作品的货架/作品根，只补已确认缺口
  → merge_existing：经既有 Engine 和同一 writer 合并，不创建重复作品
  → new_work：必要时确认电影 / 番剧 / 美剧，再由既有 Engine 规划并写入
  → uncertain：停在 blocked / needs_attention
  → 正式写入后 AList 精确回读、NFO/海报/字幕处理与当前作品检查
```

当前 HEAD 已接入对账优先的入站入口；只有 reconciliation 判定为 `new_work` 的任务才进入
`awaiting_target_shelf` 货架确认阶段，旧记录仍按兼容规则处理。系统会在短暂的网络、TMDB 或 AList
延迟后按规则重试；最终失败的任务可重新尝试或取消。

## 当前实现状态与合同差距（2026-08-16 P0 冻结基线）

当前运行实现是 **legacy execution core**：入口仍是"对账优先、仅 `new_work` 事后选货架"的旧流程，
与 [`AGENTS.md`](AGENTS.md) 的 A→P 合同（创建任务时选择来源+货架 → 先边界分析 → 逐作品身份 →
三库五分类对账 → 统一写入）不一致。目标领域模型 `IntakeSource → RootJob → WorkUnit` 的四个纯函数模块
（`intake_source / source_inventory / boundary_analysis / work_units`）已就位并随 P0 提交，但尚未接入运行时。
迁移按已批准方案分 P0–P10 推进，**已全部完成**（2026-08-16）：

- **P0** 冻结 checkpoint 与基线校准 → **P1** 只读发现（零 job 创建）→ **P2** Web 创建任务（来源+货架，`POST /api/root-jobs`）→ **P3** 边界分析与 WorkUnit 拆分（`root_boundaries`）→ **P4** 逐单元身份与 durable override（`unit_identity`）→ **P5** 三库 `LibraryIndex` 五分类对账（`library_index`，跨货架查重）→ **P6** 单元驱动规划 + 唯一写器 + `WorkAcceptanceResult`（`unit_execution`）→ **P7** Gap 账本（`gap_ledger`，绑 `work_unit_id`）+ 字幕独立渠道缺陷修复 → **P8** 根任务聚合与最小确认回流（`root_aggregation`、`GET /api/jobs/:id/work-units`、`POST …/confirm`）→ **P9** 合规清理（硬编码数据化、删除货架矩阵、统一匹配器）→ **P10** `tests/corpus/` 10 场景 + 真实 A→B→W→C→D 链路回归。

**P11–P13（2026-08-16，已完成并实机验收）**：P11 实机运行回路切换（intake 绑定 RootJob 走权威单元管线
`root_pipeline`，legacy 链仅存记录读取）；P12 单元级 E1/E2/E3 通道（`unit_e_lanes`：重复消费归档 / 既有缺口登记与空目录 hold /
归并写入既有作品根）+ 多季绝对集数显式映射桥（`episode_map_<unit>.json`）；P13 Fate 式系列容器落盘规则（`unit_execution._container_plan`：
主系列拥有容器根、电影与外传嵌套其下，容器命名三级兜底，主导系列判定与失败重试退役陈旧载体）。
实机验收：真实《刀剑神域》合集 7/7 单元经 A→R 全链入库（主系列 S1–S4 合一 + 剧场版/外传嵌套），4 个 `.5` 特别篇按证据门禁留档。

期间任何代码变更不得使测试通过数低于 873（310 subtests）。存量 `EngineJob.summary` 业务字段与
provider gate 机制冻结不新增，随后续运行节奏逐步清退。三项用户裁决（2026-08-16）：创建任务时选货架；
存量字段渐进清退；字幕保留独立渠道并修复缺陷。Web 控制台视觉语言已按 Media Engine v4 设计重绘。

## 最少配置

复制环境文件并填写凭据：

```sh
cp .env.local.example .env.local
```

至少需要：

- `ALIST_PASSWORD`
- `TMDB_API_KEY`
- `SCRAPEFLOW_HOST_STATE_ROOT`（主机上的持久化状态目录）

启动服务：

```sh
docker compose --env-file .env.local build api
docker compose --env-file .env.local up -d alist api quark-helper pansou
curl -fsS http://127.0.0.1:3010/api/health
```

Compose 的 API 进程入口是 `python3 -m local.simple_server`，默认只在宿主机 <http://127.0.0.1:8765> 暴露。API 容器连接 Compose 内部的 AList；媒体库根目录由 `SCRAPEFLOW_MEDIA_ROOT` 指定，默认是 `/quark/影视`。
同一镜像还会启动四动作 `quark-helper` sidecar。它与 API 共享网络命名空间，只监听共享的 `127.0.0.1:18765`，不发布第二个宿主端口。Sidecar 使用同一组 `ALIST_USERNAME`/`ALIST_PASSWORD` 访问 Compose 内部 AList，每次从匹配 `/quark` 的启用状态 Quark storage 临时解析 `addition.cookie` 和当前 AList 的 `root_folder_id`（兼容旧 `root_id`），并只在该次固定 Quark HTTPS/WSG 操作期间保存在内存；两者不作为 Compose 环境变量、不落盘，也不出现在 health 响应、日志或验收证据中。Compose 会在 sidecar 异常退出时重拉，也会在显式重建 API 容器时同步重建 sidecar，避免它留在旧网络命名空间。

显式设置 `SCRAPEFLOW_INTAKE_MONITOR=1` 后，服务会按 `SCRAPEFLOW_INTAKE_SCAN_SECONDS` 轮询 `/quark/影视/待刮削/`。当前 HEAD 发现来源时创建 `reconciling` 记录并先执行只读身份识别/正式库对账；只有判定为 `new_work` 时才进入 `awaiting_target_shelf`。默认模板保持关闭，仍可通过 `POST /api/jobs` 提交来源路径。

## 使用方式

1. 创建作品目录，例如 `/quark/影视/待刮削/作品名.年份/`。
2. 放入视频、字幕或已有元数据。
3. 等待入站监控发现目录，或通过 `POST /api/jobs` 提交路径。
4. 目标流程会先只读识别并对账；`merge_existing` 沿用已匹配作品根，`existing_gap` 进入限定审计/补源链路，只有 `new_work` 才通过 `POST /api/jobs/:id/start` 传入 `movie`、`anime` 或 `us_tv` 确认一级货架。
5. 通过 `GET /api/jobs` 查看身份、阶段、重试次数、AList 回读和补源结果。

系统只清理本任务从入站目录移动的内容、任务创建的 staging 和明确归类的临时残留；已有正式媒体不会因名称推测而被删除。

## 对外 API

API 只暴露当前自动服务所需的操作：

```text
GET  /api/health
GET  /api/control
POST /api/control/pause
POST /api/control/resume
GET  /api/jobs
POST /api/jobs                    {"path":"/quark/影视/待刮削/作品目录"}
POST /api/root-jobs               {"path":"/quark/影视/待刮削/作品目录","target_shelf":"movie|anime|us_tv"}
GET  /api/jobs/:id
POST /api/jobs/:id/start          {"target_shelf":"movie|anime|us_tv"}
POST /api/jobs/:id/retry
POST /api/jobs/:id/cancel
POST /api/jobs/:id/repair-artifacts {}
GET  /api/library-audit/latest
POST /api/library-audit/run
GET  /api/browse?path=...
GET  /api/intake
```

`POST /api/jobs` 只接收来源目录路径，`POST /api/jobs/:id/start` 只接收三个固定 `target_shelf` 枚举之一；后端据此映射一级目标根，拒绝任意目标路径。`POST /api/root-jobs` 是 A→P 合同的 S 步入口：一次提交来源与货架，创建或激活唯一 RootJob（同一来源幂等返回同一任务，已选货架不可更改），也是 Web 控制台"创建任务"面板调用的接口；`POST /api/jobs` 与 `/start` 保留为 legacy 兼容入口。长期合同中，`target_shelf` 是新作品首次正式入库时的受限确认枚举，不是普通输入进行只读 Engine 身份识别或正式库对账的前置门；已匹配的正式作品沿用其既有货架/作品根，身份不确定时安全停止。`repair-artifacts` 只接受空 JSON 对象，并只重放该已完成任务的确定性 NFO/海报计划；全库审计不会触发该写操作。Provider/audit lane 默认关闭；生产补源使用任务专属 staging `/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>`。隔离验收只能把 `SCRAPEFLOW_MEDIA_ROOT` 设为精确的 `/quark/影视/ScrapeFlow/验收/<run-id>`，其 staging 只能派生为 `<media-root>/ScrapeFlow/补源/<root-job-id>/<attempt-id>`；内部 child 只投影到所属根任务。

对账结果为 `uncertain` 的任务会停在 `needs_attention`，同时 `GET /api/jobs/:id` 的 `reconciliation.identity_candidates` 列出系统计算的候选身份（类型 / TMDB id / 片名 / 年份 / 置信度，最多 5 条）。用户唯一的回流动作是从候选中确认身份后，向 `POST /api/jobs/:id/retry` 提交 `{"tmdb_id": 123, "media_type": "movie|tv", "season": 1}`（电影不带 `season`），系统随即重新执行同一只读对账；该入口不接受货架、路径、链接或 Provider 指令。

严格补源的第一阶只读取 PanSou 的 `POST /api/search`，随后用当前 AList 的夸克会话做只读递归清单核验；搜索结果本身不会直接成为可写候选。PanSou 现在是 Compose 的第四个服务，不发布宿主端口，只在内部网络以 `http://pansou:8888` 可达；模板默认仍是 `SCRAPEFLOW_PANSOU_ENABLED=0`，本机启用时在 `.env.local` 设 `SCRAPEFLOW_PANSOU_ENABLED=1` 与 `SCRAPEFLOW_PANSOU_URL=http://pansou:8888`。配置缺失、接口/会话失败、查询或链接被上限截断都会让任务停在 `quark_share`，不会伪造“没有候选”或跳到后续磁力层——因此第一阶缺席时整条补源链不会推进。

容器出口代理用 `SCRAPEFLOW_HTTP_PROXY` / `SCRAPEFLOW_HTTPS_PROXY` 配置，不继承宿主的 `HTTP_PROXY`：宿主值通常是 `http://127.0.0.1:<port>`，在容器内指向容器自身，会静默切断 TMDB 与磁力索引的全部出口，而 `/api/health` 仍报告 `tmdb_configured: true`。需要代理时填 `http://host.docker.internal:<port>`。

分享快转和夸克磁力离线统一依赖四动作 Helper：`health`、`share-save`、`magnet-submit`、`magnet-status`。它只接受 Bearer 认证及与 API `SCRAPEFLOW_MEDIA_ROOT` 精确对应的任务 staging：生产为固定 `/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>`，隔离验收为受限 run-id 根派生路径。Helper 不再作为 macOS 登录项或宿主 Python 后台进程运行；Compose 直接使用 API 同一镜像启动 typed sidecar。API 默认通过共享 loopback `http://127.0.0.1:18765` 访问它，Bearer token 必须在本机 `.env.local` 中显式配置且至少 24 个字符。

AList Quark storage 中的 cookie 由 sidecar 在每个 typed action 开始时临时解析，不复制到 Compose 模板、API 请求或持久状态；它不会出现在 health 响应、日志或验收证据中。sidecar 通过固定 HTTPS API 发出 `share-save` 和磁力离线请求，桌面夸克 renderer 只提供被动的 CDP/WSG 能力（加解密与能力探针），不接收 cookie，也不执行这些网络请求。sidecar 仍只被动附着固定 `http://host.docker.internal:19222/json/list`；它绝不启动、重启、激活或点击夸克。CDP/WSG 通道不可用时 Helper 会 fail-closed，不会扫描其他端口或降级到其他写入通道。

这一 Compose sidecar 拓扑是用户在 2026-08-11 明确批准的部署修订：它取代 2026-08-09 历史收敛计划中“宿主 Helper”的物理放置，但不改变四动作、任务 staging、故障不降阶和 Helper 禁止控制 Quark 的长期合同。

桌面夸克本身的启动与重启是另一个宿主生命周期边界，不属于 Helper 四动作。为了使 CDP 参数每次一致，操作者可在 API paused 且活动操作归零后，安装一个直接运行 `QuarkCloudDrive` 的 macOS LaunchAgent：

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
- 隔离 API 启动后的只读核对可用
  `python3 scripts/scrapeflow_runtime_readiness.py --api-url http://127.0.0.1:8765 --expected-commit <git-commit>`。

文档权威层级如下：[`AGENTS.md`](AGENTS.md) 是唯一的长期产品与工程合同(含目标架构与 A→O 主流程)；当前实现事实与已知差距以源码、测试和 Git 工作树证据核对。`docs/scrapeflow-final-convergence-plan-v1.md`、旧目标货架计划、RC 和验收文档均仅作历史参考，不覆盖前述权威。它们不授权解除全局暂停或开放自动执行。

## 检查

```sh
python3 scripts/scrapeflow_release_check.py
```

日常跑测试用带 pytest 的解释器（当前基线 883 tests + 310 subtests）：

```sh
/opt/homebrew/bin/python3.12 -m pytest local/tests/ -q
# 注意：不要用系统默认 python3（本机已升到 3.14，未装 pytest），
# 也不要先 source .env.local 再跑测试（代理/搜索开关会污染环境变量，导致 lane-gate 测试失败）
```

该命令会按顺序运行：

```sh
SCRAPEFLOW_IGNORE_LOCAL_ENV=1 PYTHONDONTWRITEBYTECODE=1 \
  python3 -m unittest discover -s local/tests -p 'test_*.py'
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
