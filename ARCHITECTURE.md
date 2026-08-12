# ScrapeFlow 架构：先对账、仅新作品确认目标货架的单用户版

## 运行总览

```text
本地 API 客户端（任务状态与控制）
                    │
                    ▼
local.simple_server
  ├─ 待刮削目录发现、只读身份识别与三库对账、已确认任务调度
  ├─ 正式库审计与缺口分派
  ├─ provider 搜索、获取与 staging 校验
  └─ 一个正式媒体库写入 worker
                    │
                    ▼
Engine（身份、媒体树、命名、NFO/海报、执行计划；仅真正新作品需要一级货架确认）
                    │
                    ▼
AList（待刮削、任务专属 staging、正式媒体库）
```

本地 API 客户端是状态与控制入口。普通来源提交后，服务先进行只读身份识别，并对电影、番剧和美剧正式库做对账，得到 `duplicate_complete`、`existing_gap`、`merge_existing`、`new_work` 或 `uncertain`。已有作品从匹配的正式作品继承货架/工作根；仅当 `new_work` 确实需要一级货架确认时，用户才通过 `POST /api/jobs/:id/start` 选择 `movie`、`anime` 或 `us_tv`。用户不提交任意正式库路径；Engine 负责身份、媒体类型、季集、候选、具体作品目录和清理范围。类型与已确认货架冲突时，服务 fail-closed，不自动改选货架。

全库审计入口严格 report-only。确定性 NFO/海报补写只能由 `POST /api/jobs/:id/repair-artifacts` 显式触发，且只使用该任务已持久化的计划，不启动 Provider、重新规划或清理。

## 主流程

```text
/quark/影视/待刮削/<作品目录>
  → 只读身份识别 + 对电影 / 番剧 / 美剧正式库的对账
  → duplicate_complete | existing_gap | merge_existing | new_work | uncertain
  → duplicate_complete：身份与完整性充分确定时，仅清理任务所属输入
  → existing_gap：匹配正式作品确定货架/工作根，只补已确认缺口
  → merge_existing：经既有 Engine 和同一 writer 合并，不创建重复作品
  → new_work：必要时由用户确认 movie / anime / us_tv，随后由既有 Engine 规划并由同一 writer 写入
  → uncertain：停在 blocked / needs_attention，不猜测身份、货架、路径、删除或 Provider fallback
```

只读识别/对账不产生正式库写入，也不提交 Provider。正式写入由现有 Engine 规划并经同一 writer 执行；Local 持久化任务、取得写锁、执行计划并在每次远端操作后重新读取结果。

一级货架固定映射为：`movie → /quark/影视/电影`、`anime → /quark/影视/番剧`、`us_tv → /quark/影视/美剧`。这项用户选择只用于真正新作品的一级货架确认，不是普通入站任务的通用启动门。长期产品与工程合同以 [`AGENTS.md`](AGENTS.md) 为准，当前实现事实以 [`docs/CURRENT-STATE.md`](docs/CURRENT-STATE.md) 为准；历史收敛计划仅作参考。

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

生产根的 Provider staging 固定为 `/quark/影视/ScrapeFlow/补源`。隔离验收不得借由任意环境路径扩大该写入面：仅精确的 `/quark/影视/ScrapeFlow/验收/<run-id>` 可以作为验收媒体根，且其 staging 必须由同一根派生为 `<media-root>/ScrapeFlow/补源`。API、delivery 验证、Helper 与 preflight 共同执行该映射。

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

- `/quark/影视/待刮削`：入站发现与只读身份识别/对账目录；只接受直接子目录，登记本身不启动正式写入或 Provider 提交。
- `/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>`：任务专属补源 staging。
- `/quark/影视/电影`、`/quark/影视/番剧`、`/quark/影视/美剧`：正式媒体库。

任务状态和临时工作区可以自动清理；正式媒体库与 AList 数据库始终是受保护的外部事实。

## 模块职责

- `local/simple_server.py`：HTTP 入口、入站监控、只读对账、仅新作品的 `/start` 确认、控制状态、单写 worker 和公共任务视图。
- `local/scrapeflow_api/simple_engine_runner.py`：Engine 任务持久化、身份匹配、规划、执行、回读和重启协调；仅真正新作品接受目标货架确认。
- `local/scrapeflow_api/automatic_replenishment.py`：审计缺口到 provider、staging、内部补源阶段和清理的自动编排。
- `local/scrapeflow_api/simple_library_audit.py`：正式媒体库结构与语义审计，输出机器可处理的缺口。
- `engine/scraper.py` 与 `engine/scrapeflow/`：TMDB 访问、作品树、文件命名、元数据和 AList 计划。

## 可靠性原则

1. 正式媒体库写入必须持有 `locks/worker.lock`，同一时刻只有一个 writer。
2. move、rename、upload、delete 后都执行 AList fresh listing 与 exact readback。
3. 路径、对象类型、字节数、来源归属和 staging 归属必须满足计划，才会推进任务。
4. 不确定的已有正式媒体不自动删除；无法稳定识别的来源进入可重试或最终失败状态。
5. 服务或 API 进程重启后保持有效暂停；任务记录可以恢复，但此前持久化的 resumed 状态不授权新的正式工作。用户显式恢复后，才在下一外部副作用边界前重新核对远端现状。

## 对外操作

公共 API 见 [README.md](README.md#对外-api)。统一检查命令为：

```sh
python3 scripts/scrapeflow_release_check.py
```

它展开为：

```sh
SCRAPEFLOW_IGNORE_LOCAL_ENV=1 PYTHONDONTWRITEBYTECODE=1 \
  python3 -m unittest discover -s local/tests -p 'test_*.py'
git status --porcelain  # 必须为空
git diff --check
env -i PATH="$PATH" HOME="$HOME" SCRAPEFLOW_HOST_STATE_ROOT=/tmp/scrapeflow-state \
  docker compose config
docker build -f Dockerfile.api .
```

代码与本地发布门是受控 pilot 的必要条件，不是生产自动化授权。当前真实隔离验收、备份恢复演练和外部媒体恢复点仍必须逐项留证；在这些证据完成且用户单独授权前，必须保持全局 pause、所有自动 gate 关闭，且不得恢复旧 backlog、批量 retry 或 cleanup。
