# ScrapeFlow Engine

Engine 是 ScrapeFlow 的业务规划层，也是作品身份、媒体树和具体作品目录规则的权威。它由本地服务在任务正式启动后调用，不依赖任何客户端状态，也不要求用户提供作品类型、TMDB 或季集。普通入站的一级目标货架不由 Engine 自动决定：用户先选择固定货架，Local 将其映射为允许的目标根后才调用 Engine。

## 输入与输出

```text
已启动的来源目录 + 用户选择并由 Local 映射的一级目标根 + AList 事实
  → 身份与 TMDB 匹配
  → 电影 / 剧集 / 季集作品树
  → 文件名、NFO、海报和字幕计划
  → Local 单写 worker 执行
  → AList 刷新与精确回读
```

在用户选择 `movie`、`anime` 或 `us_tv` 并经 `/start` 确认之前，Engine 不应接收普通入站任务，也不得调用 TMDB、解包归档、生成正式计划或触发写入。选择后，Engine 在对应的 `/电影`、`/番剧` 或 `/美剧` 一级根内规划具体作品路径；身份结果与该货架不兼容时必须停止，而不是用语言、国家或路径 marker 覆盖用户选择。

Engine 生成普通 JSON 计划。计划包含来源与目标路径、文件动作、元数据、资源缺口和任务拥有的清理项；凭据只从环境变量读取，不进入计划、持久化状态、API 响应或日志。

## 自动补源边界

当 Provider/audit lane 被显式开启时，正式媒体库缺口才由审计器交给自动调度器。Provider 的结果先写入任务专属 staging：

```text
/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>
```

Engine 随后重新检查 staging 的路径、文件类型、大小和媒体身份，生成内部补源阶段的计划，再由同一个正式库 writer 执行。Provider 不决定正式库位置、TMDB 身份、季集或最终命名；其内部 child 继承根任务已确认的 `target_shelf`，不重新自动路由。

内部补源 child 是 media-only：只整理视频，保留正式库已有的作品/季度 NFO 与海报，不为每个补入集数新建 episode NFO；字幕缺口走独立的目标语言侧车流程。

## 可靠性边界

- 所有正式库文件动作都在本机单写锁内执行。
- AList 操作完成与否以 fresh listing 和 exact readback 判定。
- 路径越界、符号链接、对象类型或大小不符合计划时，计划失败并保存原因。
- 身份证据不足时继续使用可用上下文扩展匹配；策略耗尽后返回明确的自动识别失败。身份结果与已确认目标货架不兼容时返回明确的策略冲突。
- 计划执行和重启恢复都保持任务归属，清理只作用于本任务创建或移动的内容。

## 模块边界

- `engine/scraper.py`：对 Local 提供 Engine 调用入口。
- `engine/scrapeflow/core.py`：计划协调、AList 事实读取和执行基础设施。
- `engine/scrapeflow/planning/`：电影与剧集的身份、季集和文件规划。
- `engine/scrapeflow/identity_matching.py`：TMDB 候选评分与证据轨迹。
- `engine/scrapeflow/media_naming.py`：正式目录和文件名规则。
- `engine/scrapeflow/plan_artifacts.py`：NFO、海报及其他计划产物。

Local 服务负责入站登记、目标货架选择、调度、持久化和 API；Engine 负责启动后的业务判断。两者通过结构化请求和计划交接，任何远端写入都回到 Local 的单写执行路径。完整的全项目合同见[ScrapeFlow 最终收敛计划 v1](../docs/scrapeflow-final-convergence-plan-v1.md)。

## 仓库检查

在仓库根目录运行统一检查：

```sh
SCRAPEFLOW_IGNORE_LOCAL_ENV=1 PYTHONDONTWRITEBYTECODE=1 \
  python3 -m unittest discover -s local/tests -p 'test_*.py'
git diff --check
env -i PATH="$PATH" HOME="$HOME" SCRAPEFLOW_HOST_STATE_ROOT=/tmp/scrapeflow-state \
  docker compose config
docker build -f Dockerfile.api .
```
