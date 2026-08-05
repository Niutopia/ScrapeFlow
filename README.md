# ScrapeFlow

ScrapeFlow 是个人本机使用的 AList + TMDB 媒体自动化系统。它只围绕用户当前提交的作品运行：

`Web 提交目录 → 自动识别与安全刮削 → 到盘复核 → 检查当前作品缺集/缺字幕 → 自动补源 → 新资源回到刮削 → 当前作品再复核，直到补齐`

补源是当前作品未补齐时的下一步，不是独立任务中心。OCR 不是独立产品功能，只在外挂字幕、内嵌字幕轨和内容证据仍不足以判断中文字幕时内部调用。全库扫描仅作为显式启动的一次性专项或手工只读排查工具，不在正式服务中周期运行。

目标文件夹与媒体身份彼此独立：用户选定的目标分类/父边界在整条流水线及补源回刮中保持不变，边界下的作品叶子与系列容器由唯一 canonical 作品树规划；其中的剧场版仍可按 TMDB `movie` 身份识别和命名，不会因为身份是电影就自动改投“电影”分类。

> 仓库文档只说明代码和配置，不记录媒体库或任务的实时进度。当前任务、作品级闭环证据和暂停状态只以 Web 界面以及 `/api/jobs`、`/api/control` 的实时结果为准。

## 运行组成

- Web：本地操作者界面，静态文件由 Nginx 只读提供。
- API：Python 任务协调器，管理计划、调度、作品级复核、journal 和恢复。
- Engine：AList/TMDB 识别、规划、执行与校验逻辑。
- AList：远程媒体文件与目录操作入口。
- Quark Helper：查补流程的宿主机执行桥，用于夸克分享快转和磁力云离线；其他补源线路共用同一候选和到盘校验契约。

详细边界见 [ARCHITECTURE.md](./ARCHITECTURE.md)。

## 配置

先创建本地配置：

```sh
cp .env.local.example .env.local
```

本地开发至少填写 `ALIST_PASSWORD` 和 `TMDB_API_KEY`。不要把凭据写入命令行、计划文件、日志或版本库。

## 数据布局

源码、实时状态和备份统一放在一个项目根中；`state/` 与 `backups/` 仍保持彼此独立，并同时排除在 Git 和 Docker 构建上下文之外：

```text
/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/
├── app/、engine/、local/、scripts/                # 源码
├── state/                                        # AList 与 ScrapeFlow 实时状态
└── backups/                                      # 状态快照
```

不要手工清理 `state/` 或 `backups/`。Docker Compose 在 `.env.local` 中使用：

```dotenv
SCRAPEFLOW_HOST_STATE_ROOT=/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/state
```

`SCRAPEFLOW_HOST_STATE_ROOT` 保存 AList 数据库、任务、journal、作品级闭环证据和持久暂停，应始终指向上面的 `state/`。改成其他目录会加载一套不同的状态。

主要配置分为：

- 连接：`ALIST_URL`、`ALIST_USERNAME`、`TMDB_API_KEY`；
- 调度：`SCRAPEFLOW_ANALYSIS_WORKERS`、`SCRAPEFLOW_EXECUTION_WORKERS`；
- 自动流程：`SCRAPEFLOW_AUTO_EXECUTE_MEDIA`、`SCRAPEFLOW_AUTO_REPLENISH_MISSING`；
- 作品级查补：`SCRAPEFLOW_REPLENISHMENT_*`、`SCRAPEFLOW_SOURCE_REVIEW_INTERVAL` 与 Quark Helper 来源配置。

具体取值和说明以 [.env.local.example](./.env.local.example) 为准。

## 本地开发

要求 Node.js、Python 3.10+，以及一个可访问的 AList 实例。

```sh
npm install
npm run local
```

开发服务器把 `/api` 同源代理到 `127.0.0.1:8765`。未显式设置 `SCRAPEFLOW_STATE_DIR` 时，本地 API 使用可丢弃的 `.scrapeflow/` 开发状态，不会触碰 Docker 的 `state/scrapeflow-data/`。

## Docker 运行

Compose 直接从当前源码构建 API 和 Web。填好 `.env.local` 后执行：

```sh
docker compose --env-file .env.local up --build -d
```

`npm run docker:up` 是同一命令的简写。构建只更新应用镜像，不会删除或初始化 `SCRAPEFLOW_HOST_STATE_ROOT` 中的数据。

默认入口：

- Web/API：<http://127.0.0.1:3010>
- AList 管理页：<http://127.0.0.1:5244>

可在 `.env.local` 中设置 `SCRAPEFLOW_PORT` 更换 Web 端口。

`global-control.json` 中的持久暂停值是唯一暂停权威。该文件不存在时，API 以暂停状态初始化；核对连接配置和挂载后，再从界面显式恢复调度。

查看和停止：

```sh
docker compose --env-file .env.local ps
docker compose --env-file .env.local logs -f
docker compose --env-file .env.local down
```

Compose teardown 不会删除宿主机 bind 目录。AList 状态位于状态根的 `alist-data/`，ScrapeFlow 任务与作品级证据位于 `scrapeflow-data/`；删除这些宿主机目录是独立的破坏性操作。

## 当前作品的无人值守闭环

1. 用户在 Web 提交一个待刮削目录。
2. 系统处理归档、TMDB 身份、季集映射和命名；只有唯一且无冲突的计划，且整批夸克恢复副本已经封存，才通过 create-only 事务执行。
3. 到盘后先做“刮削验收”：复核目录树、已有正片、NFO、海报、残留并生成逐视频缺口证据；此层允许确实存在缺集或缺字幕，但不允许结构和元数据未闭合。
4. 只有 `/待刮削` 已清空且所有普通作品都通过刮削验收，才允许任何作品进入补源；门禁未闭合时只等待和复核，不先移动字幕或残留。
5. 当前作品存在确定缺口时，先通过 PanSou、AnimeTosho、TokyoTosho、Mikan、SubsPlease 等已配置来源获得可审计候选，再按“夸克分享快转 → 夸克磁力云离线 → 本地 Torrent”线路补源，并区分资源不匹配与基础设施故障。
6. 新资源先落入 `/quark/影视/ScrapeFlow/补源`，通过路径、名称、大小和内容身份核验，再回到同一条刮削流程。
7. 字幕状态无法由外挂字幕、内嵌轨和内容证据确定时，内部按需运行烧录字幕 OCR。
8. 每次到盘后只重审当前作品；已收口缺口不重复派发，暂时无来源的缺口按该作品的 `next_review_at` 延后复查。只有结构验收持续有效且缺集、缺字幕和待核验状态全部归零，才通过“最终完成”并清理夸克回滚副本。

本次全库补齐使用独立、显式启动的只读审计/一次性规划工具。它不会由 API 定时器自动启动，也不拥有常驻状态或 Web 面板。

## 不可精简的数据安全底线

1. **路径边界**：源和目标必须位于允许的媒体树内，源/目标包含或重叠时拒绝执行。
2. **作品身份**：TMDB 候选不唯一，或年份、类型、季集映射冲突时停止自动执行。
3. **执行前刷新**：在真正写入前重新读取 AList，计划后发生的新增、删除、改名或身份变化使旧计划失效。
4. **永不默认覆盖**：目标同名对象存在时停止，不用文件名猜测哪一份可以删除。
5. **计划摘要**：执行的规范计划必须与保存的完整 SHA-256 一致，防止过期、被改动或串任务的计划落地。
6. **受控写入**：媒体写入不使用 AList/WebDAV MOVE 或 rename；重叠路径持有同一把锁，create-only 上传逐项提交。
7. **夸克事务回滚**：整批文件在首次目标写入前，逐项通过本机单文件缓冲写入 `/quark/影视/ScrapeFlow/事务回滚`，全量回读 SHA-256 并封存不可变 manifest；严格作品验收后才清理远端恢复副本。
8. **残留分类与可恢复删除**：所有非正片残留都经 `residual_policy.py` 同一规则分类；字幕延后到精确视频级闭包决定，未知文件或证据不足一律保留。可删项也必须先建立同一夸克事务恢复副本，再删除精确远程路径；失败、取消或重启必须可恢复。
9. **幂等重试**：任务键、来源身份、journal 和远程现状共同防止重复点击、重启或重试再次移动已完成文件。

`/quark/影视/ScrapeFlow/事务回滚` 是 ScrapeFlow 独占的保留命名空间，不能放入用户媒体，也不能作为普通任务的源或目标。AList 删除接口没有对象版本条件，因此 commit/abort 清理会持有 create-only 批次租约，并在每次删除前重新读取和核对正文、manifest 与租约；这保证 ScrapeFlow 内部不会并发替换。外部程序不得同时改写该目录，发现任何内容或租约变化时必须失败关闭并保留副本。

个人本机的 HTTP 边界保持简单：Gateway 与 AList 只绑定 loopback，API 校验 Host、Origin 和严格 JSON，破坏性操作需要明确确认。浏览器不持有 AList、TMDB 或 Quark 凭据。

## 验证与恢复

完整代码验证：

```sh
npm run check
```

该命令执行 lint、类型检查、Web 构建、Web 测试、本地 API 测试、Engine 测试和备份测试。

任务工件位于 API 状态根的 `jobs/<job-id>/`。发生中断时，从 Web 发起“检查并恢复”，先生成只读恢复摘要，再审核恢复 digest。不要手工删除 `.scraper-lock-*`、`.scraper-tmp-*` 或 journal。

运维说明：

- [当前作品闭环](./docs/current-title-closure.md)
- [宿主机状态备份](./docs/host-state-backup.md)
- [Engine CLI](./engine/README.md)
