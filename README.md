# ScrapeFlow

ScrapeFlow 是一个单用户、本机运行的全自动 AList 影视库管理服务。

把作品目录放入 `/quark/影视/待刮削/`，或通过 Web 提交目录路径，系统会自动识别作品、匹配 TMDB、判断媒体类型、安排季集和正式目录，然后完成文件整理、元数据写入、媒体库审计、缺口补源和临时内容清理。

```text
待刮削作品目录
  → 自动身份匹配与计划
  → 写入正式媒体库
  → AList 刷新与精确回读
  → NFO、海报、字幕处理
  → 正式库审计
  → 自动搜索并获取可判定缺口
  → 内部补源阶段写入正式库
  → 清理任务拥有的来源与 staging
```

日常使用只需要配置服务、放入来源并查看结果。系统会在短暂的网络、TMDB 或 AList 延迟后自动重试；最终失败的任务可从 Web 重新尝试或取消。

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
docker compose --env-file .env.local build api gateway
docker compose --env-file .env.local up -d
curl -fsS http://127.0.0.1:3010/api/health
```

Compose 的 API 进程入口是 `python3 -m local.simple_server`，Web 默认地址为 <http://127.0.0.1:3010>。API 容器连接 Compose 内部的 AList；媒体库根目录由 `SCRAPEFLOW_MEDIA_ROOT` 指定，默认是 `/quark/影视`。

服务启动后会按 `SCRAPEFLOW_INTAKE_SCAN_SECONDS` 轮询 `/quark/影视/待刮削/`。将 `SCRAPEFLOW_INTAKE_MONITOR=0` 时，仍可通过 `POST /api/jobs` 提交来源路径；已经入队的任务继续按自动流程执行。

## 使用方式

1. 创建作品目录，例如 `/quark/影视/待刮削/作品名.年份/`。
2. 放入视频、字幕或已有元数据。
3. 等待入站监控发现目录，或在 Web 的“新建任务”中提交路径。
4. 在任务列表查看身份、阶段、重试次数、AList 回读和补源结果。

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
POST /api/jobs/:id/retry
POST /api/jobs/:id/cancel
GET  /api/library-audit/latest
POST /api/library-audit/run
GET  /api/browse?path=...
```

`POST /api/jobs` 只接收来源目录路径。作品身份、类型、季集、目标路径、候选和清理动作都由 Engine 与本地调度器根据远端事实决定。补源使用任务专属 staging `/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>`，内部阶段不会单独出现在 Web 任务列表中。

## 可靠性边界

- 正式媒体库的 move、rename、upload 和 delete 共用一把本机写锁。
- 每次远端写入后都刷新相关父目录，并以精确路径、对象类型和字节数回读结果。
- 正式库视频在计划、写入与恢复回读三处均须通过最小字节准入（默认 1 MiB，硬下限 64 KiB）；补源本地 payload 还须通过 `ffprobe` 视频流核验后才能进入 staging。
- 补源 child 是 media-only：只回投视频，不新建或覆盖每集 NFO、海报；正式库已有的作品/季度元数据保持不动。
- `SCRAPEFLOW_REQUIRED_SUBTITLE_LANGUAGE`（默认示例为 `zh`）会在侧车和内嵌字幕都缺少目标语言时生成字幕缺口，并自动获取对应字幕；已有目标语言字幕不会重复写入。
- 内嵌字幕探针按次审计使用有界单调时间预算（默认 120 秒、最多 4 个并发）；预算内未完成的证据标记为 `unknown_subtitle_evidence`，不会误判为缺字幕。相同的字幕未知不会每 30 秒重复全库扫描，而是在新媒体提交或手动审计时再次核对。可用 `SCRAPEFLOW_SUBTITLE_PROBE_BUDGET_SECONDS`、`SCRAPEFLOW_SUBTITLE_PROBE_WORKERS` 和 `SCRAPEFLOW_SUBTITLE_PROBE_MAX_FILES` 调整。
- 新对象短暂不可见时，只对相关读取执行有界重试。
- 任务重启时会先根据 AList 当前状态重新核对，再继续或进入可重试的失败状态。
- 正式媒体库、AList 数据库和其他任务不属于当前任务的清理范围。

完整的产品规则、状态机和发布检查见[全自动工作流规格](docs/scrapeflow-unified-workflow-remediation-plan.md)与[架构说明](ARCHITECTURE.md)。

## 检查

```sh
npm run check
```

该命令运行静态检查、活动 API 导入、自动化测试和生产 Web 构建。

端到端测试不得加载生产 `.env.local`、生产 AList URL 或真实凭据，也不得把
`/quark/影视/待刮削`、`/quark/影视/ScrapeFlow/补源` 当作测试夹具。请使用
`SCRAPEFLOW_IGNORE_LOCAL_ENV=1`、临时状态目录和注入的 fake/in-memory AList；
若必须测试 HTTP，应为测试进程单独启动隔离的 AList 命名空间，并在结束时清理。
