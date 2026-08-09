# ScrapeFlow 架构：用户选择目标货架后启动的单用户版

## 运行总览

```text
Web 与本地 API 客户端（目标货架选择与状态控制）
                    │
                    ▼
local.simple_server
  ├─ 待刮削目录发现、待选择登记与已确认任务调度
  ├─ 正式库审计与缺口分派
  ├─ provider 搜索、获取与 staging 校验
  └─ 一个正式媒体库写入 worker
                    │
                    ▼
Engine（启动后：TMDB、身份、媒体树、命名、NFO/海报、执行计划）
                    │
                    ▼
AList（待刮削、任务专属 staging、正式媒体库）
```

Web 与本地 API 客户端是状态与控制界面。普通来源提交后，服务仅登记 `awaiting_target_shelf`；用户必须通过 Dashboard 或 `POST /api/jobs/:id/start` 选择 `movie`、`anime` 或 `us_tv`，后端才将其映射为固定一级目标根并允许调度。用户不提交任意正式库路径；Engine 负责启动后的作品身份、媒体类型、季集、候选、具体作品目录和清理范围。若类型与用户货架冲突，服务 fail-closed，不自动改选货架。Web 大改版不属于当前最小启动门闭环。

## 主流程

```text
/quark/影视/待刮削/<作品目录>
  → awaiting_target_shelf（只登记，不调用 Engine）
  → 用户选择 电影 / 番剧 / 美剧 并调用 /start
  → queued
  → archive_preprocessing
  → identity_matching
  → planning
  → executing
  → verifying
  → cleaning
  → completed
```

普通任务只有在用户选择一级货架后才交给 Engine。Local 将固定目标根传入 Engine；Engine 在该根内生成具体的作品工作目录，并读取来源和 AList 事实生成包含身份、目标目录、文件名、NFO、海报、字幕以及清理动作的计划。Local 持久化任务、取得写锁、执行计划并在每次远端操作后重新读取结果。

一级货架固定映射为：`movie → /quark/影视/电影`、`anime → /quark/影视/番剧`、`us_tv → /quark/影视/美剧`。这项用户选择不是通用审批流程；它是普通入站任务唯一的正式启动门。完整状态机以[用户选择目标货架后启动实施计划](docs/scrapeflow-target-shelf-start-gate-plan.md)为准。

## 自动补源

```text
正式库审计
  → machine-readable gap
  → provider 搜索与候选排序
  → /quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>
  → staging 内容、路径、大小和类型校验
  → 内部补源阶段重新规划并执行
  → 正式库回读
  → 缺口重审与 staging 清理
```

Provider 只能写入当前任务专属 staging，不能决定正式库位置或作品身份。视频、集数和季缺口使用内部补源阶段；该 child 只回投媒体，不生成每集 NFO/海报。字幕缺口使用绑定到已审计视频的侧挂流程：先检查同目录侧车和内嵌目标语言，确认缺失后才获取。Provider/audit 自动 lane 当前默认关闭；只有显式开启并通过当前计划验收后，补源才会按配置有界重试。

## 状态与目录

API 容器中的 `/data` 由 `SCRAPEFLOW_HOST_STATE_ROOT/scrapeflow-data` 持久化。活动状态布局为：

```text
/data/
├── global-control.json       # 暂停/恢复与调度状态
├── jobs/<job-id>.json        # 根任务和内部阶段的可恢复记录
├── gaps/<job-id>/            # 最新审计缺口
├── staging/                  # provider 的本地下载工作区
├── library-audit/latest.json # 最近一次正式库审计
└── locks/                    # 正式库写锁
```

远端目录边界为：

- `/quark/影视/待刮削`：入站发现与待选择登记目录；只接受直接子目录，登记本身不启动正式任务。
- `/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>`：任务专属补源 staging。
- `/quark/影视/电影`、`/quark/影视/番剧`、`/quark/影视/美剧`：正式媒体库。

任务状态和临时工作区可以自动清理；正式媒体库与 AList 数据库始终是受保护的外部事实。

## 模块职责

- `local/simple_server.py`：HTTP 入口、入站监控、待选择登记、`/start` 调度、控制状态、单写 worker 和公共任务视图。
- `local/scrapeflow_api/simple_engine_runner.py`：Engine 任务持久化、目标货架选择、身份匹配、规划、执行、回读和重启协调。
- `local/scrapeflow_api/automatic_replenishment.py`：审计缺口到 provider、staging、内部补源阶段和清理的自动编排。
- `local/scrapeflow_api/simple_library_audit.py`：正式媒体库结构与语义审计，输出机器可处理的缺口。
- `engine/scraper.py` 与 `engine/scrapeflow/`：TMDB 访问、作品树、文件命名、元数据和 AList 计划。
- `app/`：展示根任务、审计结果、阶段进度和最终错误；不承担业务决策。

## 可靠性原则

1. 正式媒体库写入必须持有 `locks/worker.lock`，同一时刻只有一个 writer。
2. move、rename、upload、delete 后都执行 AList fresh listing 与 exact readback。
3. 路径、对象类型、字节数、来源归属和 staging 归属必须满足计划，才会推进任务。
4. 不确定的已有正式媒体不自动删除；无法稳定识别的来源进入可重试或最终失败状态。
5. 服务重启会保留 `awaiting_target_shelf` 状态而不把它排队；只恢复已经满足启动条件的可继续任务，并在恢复前核对远端现状，避免重复写入。

## 对外操作

公共 API 见 [README.md](README.md#对外-api)。统一检查命令为：

```sh
npm run check
```

当前工作树尚未达到发布门。阶段 0–6 全部通过且用户再次明确授权后，才可在隔离目录运行一个受控样本；不得用本文件授权真实来源整理或缺口补齐。
