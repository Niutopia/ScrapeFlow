# ScrapeFlow Engine

Engine 是 ScrapeFlow 的内部业务规划层，负责作品身份、媒体树和具体作品目录规则；长期产品合同以仓库根目录的 AGENTS.md 为准。它由 Local 在 RootJob 已获用户授权后调用，不依赖客户端状态，也不把作品类型、TMDB 或季集当作任意用户输入。用户在创建 RootJob 时为来源选择 movie、anime 或 us_tv；Engine 不自行选择货架。已匹配的正式作品始终沿用既有货架/作品根，只有真正新作品才使用 RootJob 已授权的固定货架映射允许的目标根。

## 输入与输出

```text
已授权 RootJob 的来源快照 + 固定一级货架 + AList 事实
  → 身份与 TMDB 匹配
  → 电影 / 剧集 / 季集作品树
  → 文件名、NFO、海报和字幕计划
  → Local 单写 worker 执行
  → AList 刷新与精确回读
```

在 RootJob 创建前，入站发现只登记 IntakeSource，不调用 Engine、TMDB 或正式库对账。RootJob 获授权后，Engine 才参与边界分析后的逐单元只读身份识别和三库对账；这些操作不创建正式作品、不解包归档、不触发写入。new_work 使用创建 RootJob 时选定的 movie、anime 或 us_tv 货架，随后 Engine 在对应的 /电影、/番剧 或 /欧美剧 一级根内规划具体作品路径。已有匹配锁定已验证的 shelf/work root；身份或对账证据不足时保持 fail-closed，而不是猜测目标路径或虚构额外的货架规则。当前实现事实以仓库源码、测试和 Git 工作树证据核对。

Engine 生成普通 JSON 计划。计划包含来源与目标路径、文件动作、元数据、资源缺口和任务拥有的清理项；凭据只从环境变量读取，不进入计划、持久化状态、API 响应或日志。

## 自动补源边界

已选中且未暂停的 RootJob 会直接处理自己的精确缺口，不需要全库审计、provider gate 或 pilot。视频补源固定按 `quark_share → magnet` 进行，产物先写入任务专属 staging：

```text
/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>
```

Engine 随后重新检查 staging 的路径、文件类型、大小和媒体身份，生成内部补源阶段的计划，再由同一个正式库 writer 执行。Provider 不决定正式库位置、TMDB 身份、季集或最终命名；其内部 child 沿用已验证的 identity、具体作品根和同一 writer，不重新自动路由。

内部补源 child 是 media-only：只整理视频，保留正式库已有的作品/季度 NFO 与海报，不为每个补入集数新建 episode NFO；字幕缺口走独立的目标语言侧车流程。

## 可靠性边界

- 所有正式库文件动作都在本机单写锁内执行。
- AList 操作完成与否以 fresh listing 和 exact readback 判定。
- 路径越界、符号链接、对象类型或大小不符合计划时，计划失败并保存原因。
- 身份证据不足时继续使用可用上下文扩展匹配；策略耗尽后返回明确的自动识别失败并保持 fail-closed。新作品只在 RootJob 已授权货架内规划，已有匹配则锁定既有 shelf/work root。
- 计划执行和重启恢复都保持任务归属，清理只作用于本任务创建或移动的内容。

## 模块边界

- `engine/scraper.py`：对 Local 提供 Engine 调用入口。
- `engine/scrapeflow/core.py`：计划协调、AList 事实读取和执行基础设施。
- `engine/scrapeflow/planning/`：电影与剧集的身份、季集和文件规划。
- `engine/scrapeflow/identity_matching.py`：TMDB 候选评分与证据轨迹。
- `engine/scrapeflow/media_naming.py`：正式目录和文件名规则。
- `engine/scrapeflow/plan_artifacts.py`：NFO、海报及其他计划产物。

Local 服务负责被动入站登记、用户以来源加货架创建 RootJob、调度、持久化和 API；Engine 负责该 RootJob 的身份和规划判断。两者通过结构化请求和计划交接，任何远端写入都回到 Local 的单写执行路径。完整的长期产品与工程合同见 [`../AGENTS.md`](../AGENTS.md)；当前实现事实以仓库源码、测试和 Git 工作树证据核对，明确标注的历史收敛计划仅作参考。

## 开发回归

测试仅供开发回归，不是本机部署或运行的前提。在仓库根目录可运行：

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m pytest local/tests -q
git diff --check
```
