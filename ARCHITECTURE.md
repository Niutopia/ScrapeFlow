# ScrapeFlow 架构

ScrapeFlow 把操作者界面、任务协调、媒体领域逻辑和外部存储分开，但它们只服务一个任务驱动的作品级闭环：Web 提交、识别刮削、到盘复核、当前作品补源、字幕/OCR 内部核验和当前作品再复核。

仓库文档不保存运行进度。任务、作品级闭环证据和暂停的实时权威来源是 Web 界面及 API，而不是 Markdown、离线导出或测试夹具。

## 组件

### Web

- `app/scrapeflow-app.tsx` 组合目录选择、任务列表、审核和结果界面。
- `app/hooks/use-scrapeflow.ts` 管理轮询和操作者动作。
- `app/core/api-client.ts` 是浏览器访问本地 API 的传输边界。
- `app/core/contracts.ts` 与 `job-state.ts` 定义类型和生命周期展示。
- `app/components/review-dialog.tsx` 与 `issue-workbench.tsx` 展示审核计划和审计问题。

Web 不持有 AList、TMDB 或 Quark 凭据。Web 镜像由 Compose 从当前源码构建，静态文件由只读 Nginx Gateway 提供。

### Local API

`local/server.py` 是本地 API 启动入口和总协调器，负责把路由、调度与各领域流程串联起来。

`local/scrapeflow_api/` 中的模块负责：

- `config.py`：环境、状态目录和 Engine 子进程配置；
- `contracts.py`、`models.py`：任务相位、持久化模型和公开模型；
- `scheduler.py`：只读分析池、变更池、FIFO 和路径预留；
- `lifecycle.py`：持久全局暂停；
- `validation.py`：严格 JSON、路径、摘要和脱敏；
- `replenishment.py`：缺口、候选、线路和失败域；
- `title_closure.py`、`title_closure_runtime.py`：当前任务作品的缺集、字幕和到盘闭环证据；
- `subtitle_*`：当前作品需要时使用的字幕精炼、来源发现和内部 OCR。

API 状态在容器内以 `/data` 为根；任务目录为 `/data/jobs/<job-id>`。Docker 通过宿主机 bind mount 持久化该目录。

### Engine

`engine/scraper.py` 是媒体计划、执行和恢复的 CLI 入口。`engine/scrapeflow/` 提供模型、错误、HTTP、序列化、放置、查补、Quark 和字幕领域逻辑。其中 `canonical_work_tree.py` 是跨作品目录树的唯一规划权威；`scraper.py` 只能通过统一 adapter 将已确认的作品身份交给它。`residual_policy.py` 是非正片残留分类的唯一决策入口。所有会替换、移动或删除远端正文的动作都必须先用本机单文件缓冲，在 `/quark/影视/ScrapeFlow/事务回滚` 建立并全量回读校验恢复副本；整批 manifest 封存后才允许首次目标写入，严格作品验收后才 commit 清理。全库审计只作为显式手工运维工具存在，不由 API 常驻调度。

`engine/tools/` 包含受 API 调用的运行工具和独立运维工具。工具是否允许写入，必须以各自 CLI 的显式门禁为准，不能因为位于 `tools/` 就假定只读或可直接执行。

### 基础设施与外部边界

- Nginx Gateway：提供静态 Web，并同源代理 `/api`。
- AList：目录读取、移动、创建、删除、上传和原生解压入口。
- TMDB：作品、季度、集和图稿元数据。
- Quark Helper：宿主机 loopback 服务，封装分享快转和磁力云离线所需的原生调用。
- Provider adapter：搜索和获取候选；内置本地适配器与 HTTP 适配器共享核心候选契约。

Compose 直接从仓库中的 `Dockerfile.api` 和 `Dockerfile.web` 构建两个本地镜像。`state/` 和 `backups/` 位于项目根内，但被 Git 与 Docker 构建上下文排除；重建镜像不会初始化或删除它们。

## 宿主机数据边界

项目只使用一个根目录，源码、状态和备份按顶层目录分隔：

```text
/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/          # 源码与配置
/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/state/    # 实时状态
/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/backups/  # 可验证快照
```

`state/` 是 Docker bind mount 的权威运行数据；`backups/` 是独立恢复副本，不得被任务运行时直接修改。

## 权威状态与工件

| 内容 | 权威位置 |
| --- | --- |
| 任务 phase、摘要、日志 | `/data/jobs/<job-id>/job.json` 与 `job.log` |
| 媒体/归档计划 | 对应任务目录中的计划 JSON 与完整 SHA-256 |
| 执行与恢复 | journal 及其摘要 |
| 全局暂停 | `/data/global-control.json`；API 内存闸门只是该持久值的派生执行视图 |
| 当前作品复核 | `/data/jobs/<job-id>/title-closure.json` 及该任务绑定的字幕证据 |
| 查补 | request、candidate、selection、attempt、acquisition 和 failure 工件 |
| 字幕 | refined audit、probe cache、discovery manifest、content witness 和执行 journal |

仓库中的 fixture 或离线导出不能覆盖这些运行时来源。

## 主要作品级闭环

1. **Web 提交**：用户选择一个待刮削目录，系统为该任务保存唯一身份和生命周期。
2. **安全刮削**：只读扫描生成规范计划与 SHA-256；执行前重读 AList 并校验源、目标、摘要和锁，提交过程写入 journal。
3. **当前作品复核**：根据任务已签名的作品根检查缺集、Season 00 和简体中文字幕，不扫描无关作品。
4. **作品级补源**：确定缺口进入 provider-neutral 候选选择，通过已配置的夸克分享、夸克磁力或本地 Torrent 线路获取。
5. **到盘回刮**：候选落到 `/quark/影视/ScrapeFlow/补源`，核对路径、名称、大小和内容身份后回到同一安全刮削流程。
6. **字幕/OCR 内部核验**：优先核对外挂字幕和内嵌轨；证据仍不足时才运行烧录字幕 OCR，确认缺简中后进入当前作品的字幕补源。

媒体身份不决定目标分类。TMDB 可以把番剧目录中的剧场版解析为 `movie`，但计划与后续补源仍绑定用户选定的目标分类/父边界；该边界之下的作品叶子和关系容器由 canonical 作品树决定。只有用户重新明确选择其他目标分类/父文件夹，路由才允许越界改变。

Canonical 作品树使用 `(metadata namespace, metadata id)` 作为唯一作品身份。同一身份的清晰度/发行版共用一个作品叶子；不同 ID 永远不能合并成同一 `Season` 树。已证明的主作品可占用容器根，独立续作/衍生作为它下方的独立身份叶子；没有主作品证据的系列目录（如 `Fate`）仅是容器。标题边界只能在同一已验证批次中提议目录容器，不能生成、合并或改写作品身份；任何目标冲突都失败关闭。
7. **当前作品再复核**：每次整理后只重读该作品；若主作品根下有其他已确认 ID 的独立作品，签名 target 同时封存精确 `excluded_roots`，主作品的缺集与字幕扫描必须排除这些子叶，每个子叶仍按自己的 TMDB ID 独立复核。缺口真实消失才标记收口，否则保留幂等重试或该作品的延后来源复查。

一次性全库补齐不属于上述常驻闭环。它只能通过显式手工、默认只读的工具生成工作清单，不提供定时器、常驻协调器、Web 状态或自动全库清理。

正式库是 `/quark/影视/{电影,番剧,美剧}`；用户入站是 `/quark/影视/待刮削`；`/quark/影视/ScrapeFlow` 是系统工件区，不计入正式库完整度。候选资源失效只隔离当前候选；网络、认证、配额或编排故障不能被伪装成“没有资源”。

自动执行不等于绕过保护。TMDB 身份不唯一、目标已存在、源快照变化或状态无法唯一证明时，闭环会停在当前现场而不是猜测。

## 跨层契约

- `contracts/job-phases.json` 是任务 phase 的共享契约；TypeScript 与 Python 测试必须与其一致。
- `contracts/quark-share-candidate.schema.json` 和 `quark-magnet-candidate.schema.json` 约束云候选身份。
- 浏览器只消费公开任务模型；内部 locator、凭据和敏感日志不进入响应。
- 计划评估同时驱动 API 门禁与 Web 解释，避免界面和执行器对风险作出不同判断。

## 数据安全不变量

1. 源目录、目标目录必须在允许边界内，且不得相互包含或重叠。
2. TMDB 作品身份或季集映射不唯一时停止自动执行。
3. 每次写入前重读 AList，源文件发生变化就拒绝旧计划。
4. 目标同名对象存在时默认不覆盖、不替换、不删除。
5. 执行必须绑定已保存计划的完整 digest；摘要变化表示计划已过期。
6. 媒体正文禁用远端 MOVE/rename；重叠路径使用锁和 create-only 串行写入。
7. 整批源文件先逐项写入夸克事务回滚区、全量回读 SHA-256 并封存 manifest；中断后按 journal 精确续执行或恢复，不能唯一判定时停止。全局 scrape-first 使用允许真实缺口存在的“刮削验收”，任务终态和事务 commit 使用要求所有缺口归零的“最终完成”，两者不得混用。
8. 残留必须经 `residual_policy.py` 分类；字幕留给精确视频字幕闭包，未知文件保留。已证明的非正片也必须先建立夸克恢复副本，失败/取消时 restore，严格作品验收后 commit。
9. 幂等键、来源身份、journal 和现状复核必须防止重试再次移动已成功文件。

事务回滚根是系统独占命名空间，普通 source/target 进入该根必须在计划期拒绝。AList remove 不提供 compare-and-delete，因此清理阶段使用 create-only 批次租约阻止 ScrapeFlow 内部并发，并在每次 remove 前重新校验当前终态、恢复正文和 manifest；外部写入者不得并发修改该根，检测到任何变化即失败关闭。

凭据不得进入浏览器、计划、journal 或普通日志。持久全局暂停是唯一暂停权威，它阻止新任务分析、作品复核、补源派发和写入；需要一致快照时还必须等待已开始的远程动作静默。运行进度只通过实时 API/UI 展示，不写入仓库文档。
