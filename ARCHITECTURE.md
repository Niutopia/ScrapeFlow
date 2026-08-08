# ScrapeFlow 架构：全自动单用户版

## 运行总览

```text
Web（提交路径、查看状态、暂停、重试、取消）
                    │
                    ▼
local.simple_server
  ├─ 待刮削目录监控与根任务调度
  ├─ 正式库审计与缺口分派
  ├─ provider 搜索、获取与 staging 校验
  └─ 一个正式媒体库写入 worker
                    │
                    ▼
Engine（TMDB、身份、媒体树、命名、NFO/海报、执行计划）
                    │
                    ▼
AList（待刮削、任务专属 staging、正式媒体库）
```

Web 是状态与控制界面。它提交来源路径、显示自动阶段，并提供暂停、重试和取消；作品身份、媒体类型、季集、候选、目标目录和清理范围均由服务自动决定。

## 主流程

```text
/quark/影视/待刮削/<作品目录>
  → queued
  → analyzing / identity_matching
  → planning
  → executing
  → verifying
  → cleaning
  → completed
```

Engine 读取来源和 AList 事实，生成包含身份、目标目录、文件名、NFO、海报、字幕以及清理动作的计划。Local 持久化任务、取得写锁、执行计划并在每次远端操作后重新读取结果。

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

Provider 只能写入当前任务专属 staging，不能决定正式库位置或作品身份。视频、集数和季缺口使用内部补源阶段；该 child 只回投媒体，不生成每集 NFO/海报。字幕缺口使用绑定到已审计视频的侧挂流程：先检查同目录侧车和内嵌目标语言，确认缺失后才获取。补源失败会按配置退避重试，耗尽后记录可读的最终错误。

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

- `/quark/影视/待刮削`：自动入站目录。
- `/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>`：任务专属补源 staging。
- `/quark/影视/电影`、`/quark/影视/番剧`、`/quark/影视/美剧`：正式媒体库。

任务状态和临时工作区可以自动清理；正式媒体库与 AList 数据库始终是受保护的外部事实。

## 模块职责

- `local/simple_server.py`：HTTP 入口、入站监控、调度、控制状态、单写 worker 和公共任务视图。
- `local/scrapeflow_api/simple_engine_runner.py`：Engine 任务持久化、身份匹配、规划、执行、回读和重启协调。
- `local/scrapeflow_api/automatic_replenishment.py`：审计缺口到 provider、staging、内部补源阶段和清理的自动编排。
- `local/scrapeflow_api/simple_library_audit.py`：正式媒体库结构与语义审计，输出机器可处理的缺口。
- `engine/scraper.py` 与 `engine/scrapeflow/`：TMDB 访问、作品树、文件命名、元数据和 AList 计划。
- `app/`：展示根任务、审计结果、阶段进度和最终错误；不承担业务决策。

## 可靠性原则

1. 正式媒体库写入必须持有 `locks/worker.lock`，同一时刻只有一个 writer。
2. move、rename、upload、delete 后都执行 AList fresh listing 与 exact readback。
3. 路径、对象类型、字节数、来源归属和 staging 归属必须满足计划，才会推进任务。
4. 不确定的已有正式媒体不自动删除；无法稳定识别的来源进入可重试或最终失败状态。
5. 服务重启会读取 `/data/jobs/`，核对远端现状后恢复可继续任务，避免重复写入。

## 对外操作

公共 API 见 [README.md](README.md#对外-api)。统一检查命令为：

```sh
npm run check
```

发布前还应在目标环境运行一次健康检查、一次新来源整理和一次已知缺口补齐，观察最终路径、回读结果与任务专属 staging 清理。
