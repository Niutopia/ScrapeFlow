# ScrapeFlow

ScrapeFlow 是一个单用户、本机运行的 AList 影视库管理服务。普通入站来源会先登记为待处理项；只有用户选择固定的一级目标货架并确认启动后，任务才进入自动整理主链。

把作品目录放入 `/quark/影视/待刮削/`，或通过 API 提交目录路径后，服务只记录来源和等待状态。用户随后选择 `/quark/影视/电影`、`/quark/影视/番剧` 或 `/quark/影视/美剧`（分别对应 `movie`、`anime`、`us_tv`）并调用启动接口；此后系统才会识别作品、匹配 TMDB、判断媒体类型、安排季集和具体作品目录，并完成文件整理、元数据写入以及最终 source/staging 处理。当前 WIP 中 Provider 自动补源和自动审计保持默认关闭。

```text
待刮削作品目录
  → awaiting_target_shelf（只登记）
  → 用户选择电影 / 番剧 / 美剧并确认启动
  → queued
  → 归档/媒体预处理、自动身份匹配与计划
  → 写入正式媒体库
  → AList 刷新与精确回读
  → NFO、海报、字幕处理
  → 显式开启时运行正式库审计
  → 显式开启时自动搜索并获取可判定缺口
  → 显式开启时由内部补源阶段写入正式库
  → 清理任务拥有的来源与 staging
```

日常使用需要配置服务、放入来源、通过 API 选择目标货架并启动任务，再查询结果。系统会在短暂的网络、TMDB 或 AList 延迟后按规则重试；最终失败的任务可重新尝试或取消。

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
docker compose --env-file .env.local up -d alist api
curl -fsS http://127.0.0.1:8765/api/health
```

Compose 的 API 进程入口是 `python3 -m local.simple_server`，默认只在宿主机 <http://127.0.0.1:8765> 暴露。API 容器连接 Compose 内部的 AList；媒体库根目录由 `SCRAPEFLOW_MEDIA_ROOT` 指定，默认是 `/quark/影视`。

显式设置 `SCRAPEFLOW_INTAKE_MONITOR=1` 后，服务会按 `SCRAPEFLOW_INTAKE_SCAN_SECONDS` 轮询 `/quark/影视/待刮削/`。发现来源时只创建 `awaiting_target_shelf` 记录，不会自动执行归档预处理、TMDB、规划或写入。默认模板保持关闭，仍可通过 `POST /api/jobs` 提交来源路径；任务同样必须由用户选择货架并调用 `/start` 后才会进入队列。

## 使用方式

1. 创建作品目录，例如 `/quark/影视/待刮削/作品名.年份/`。
2. 放入视频、字幕或已有元数据。
3. 等待入站监控发现目录，或通过 `POST /api/jobs` 提交路径。
4. 通过 `POST /api/jobs/:id/start` 传入 `movie`、`anime` 或 `us_tv` 启动。
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
GET  /api/jobs/:id
POST /api/jobs/:id/start          {"target_shelf":"movie|anime|us_tv"}
POST /api/jobs/:id/retry
POST /api/jobs/:id/cancel
GET  /api/library-audit/latest
POST /api/library-audit/run
GET  /api/browse?path=...
```

`POST /api/jobs` 只接收来源目录路径，只会登记待选择任务。`POST /api/jobs/:id/start` 只接收三个固定 `target_shelf` 枚举之一；后端据此映射一级目标根，拒绝任意目标路径。选择前不会调用 Engine；选择后，Engine 才根据远端事实决定作品身份、类型、季集、具体作品路径、候选和清理动作。若识别出的媒体类型与用户选择的货架不兼容，任务进入冲突状态而不会静默改选货架。Provider/audit lane 默认关闭；显式开启时，补源使用任务专属 staging `/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>`，内部 child 只投影到所属根任务。

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
- 本机验收包草稿可用 `python3 scripts/scrapeflow_acceptance_package.py --output /tmp/scrapeflow-acceptance-package.md`
  生成；已有隔离声明时可追加 `--preflight-declaration declaration.json`。真实样本仍必须在独立环境中手工补证。
- 隔离环境声明可用 `python3 scripts/scrapeflow_isolated_preflight.py --template` 生成模板，
  再用 `python3 scripts/scrapeflow_isolated_preflight.py declaration.json` 做阶段 10 前置检查。

当前的产品规则、启动门状态机、实施阶段和发布检查见[用户选择目标货架后启动实施计划](docs/scrapeflow-target-shelf-start-gate-plan.md)与[架构说明](ARCHITECTURE.md)。在该计划的完成门通过前，不应因本文档解除全局暂停或开放自动执行。

## 检查

```sh
python3 scripts/scrapeflow_release_check.py
```

该命令会按顺序运行：

```sh
SCRAPEFLOW_IGNORE_LOCAL_ENV=1 PYTHONDONTWRITEBYTECODE=1 \
  python3 -m unittest discover -s local/tests -p 'test_*.py'
git diff --check
env -i PATH="$PATH" HOME="$HOME" SCRAPEFLOW_HOST_STATE_ROOT=/tmp/scrapeflow-state \
  docker compose config
docker build -f Dockerfile.api .
```

端到端测试不得加载生产 `.env.local`、生产 AList URL 或真实凭据，也不得把
`/quark/影视/待刮削`、`/quark/影视/ScrapeFlow/补源` 当作测试夹具。请使用
`SCRAPEFLOW_IGNORE_LOCAL_ENV=1`、临时状态目录和注入的 fake/in-memory AList；
若必须测试 HTTP，应为测试进程单独启动隔离的 AList 命名空间，并在结束时清理。
