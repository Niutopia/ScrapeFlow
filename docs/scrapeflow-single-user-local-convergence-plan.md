# ScrapeFlow 单用户本地轻量收敛实施计划（历史参考）

> 状态：历史收敛边界参考；其中 target-shelf-first 仅为当时方案，不是当前普通入站主线
>
> 制定日期：2026-08-08
>
> 适用范围：单用户、单机、单实例、单 AList 媒体库
>
> 长期产品与工程合同见 [`../AGENTS.md`](../AGENTS.md)；当前事实以根 AGENTS.md、源码、测试和 Git 工作树证据核对。本文件是历史单用户收敛快照。

## 1. 计划目的

本文是 2026-08-08 的轻量收敛边界参考，不声明当前新主线已经实施完成。它保留单用户、单 writer、路径安全、归档安全和轻量恢复等约束；普通入站的当前入口顺序、一级货架权威和启动状态机以根 `AGENTS.md` 为准。

当时的产品方向是：用户提交或入站监控发现一个来源目录时，系统先登记待处理项；用户选择电影、番剧或美剧的固定一级货架并调用 `/start` 后，系统才自动完成身份识别、规划、写入、审计、补源和任务自有临时内容清理。该 target-shelf-first 入口现已被 reconciliation-first 长期合同取代。

本计划重新约束实现方式：

- 只服务一个本地用户，不建设多租户、分布式或企业审批体系。
- 正式媒体库始终只有一个 writer；搜索和下载可以有少量有界并发。
- 网络或外部服务失败可以明确停止并由用户重试，不追求 exactly-once 证明。
- 正式写入只使用路径、对象类型、文件大小和媒体可读性验证，不为媒体文件计算 SHA-256。
- 不恢复 plan digest、receipt、nonce、epoch、远端回滚副本和复杂事务 journal。
- 不恢复覆盖所有 Provider、容器和真实凭据的 E2E 或持续冒烟平台。
- 只迁移旧模块中仍然需要的业务规则和安全内核，不整文件恢复旧运行时。

在本历史计划中，用户选择固定目标货架是普通入站的正式启动门，不是本计划明确排除的通用审批、review 或人工确认系统。

本计划不是一次“把旧项目还原回来”的计划。它要完成的是：

```text
旧模块能力盘点
  → 保留仍需要的业务/安全内核
  → 接入当前 simple_server 自动主链
  → 删除旧事务、审批和证明外壳
  → 用少量高价值回归测试固定行为
```

## 2. 产品边界与明确决策

### 2.1 支持的运行模型

- 一个用户。
- 一台本机。
- 一个 API 实例。
- 一个 AList 实例和一套媒体库根。
- 一个正式库 writer。
- 这是历史收敛时的 worker 容量记录；Provider/audit 自动 lane 的长期要求以根 `AGENTS.md` 为准，当前实现事实以源码、测试和 Git 工作树证据核对。
- 用户可以暂停、恢复、重试、取消和处理最终失败。

### 2.2 不再建设的能力

以下内容永久退出主路线：

- 全文件 SHA-256、计划 SHA、内容 receipt 和 digest 审批。
- 两阶段提交、exactly-once upload、远端事务回滚区和逐操作恢复证明。
- 多实例调度、分布式锁、租约、资源冲突图和多 writer。
- 通用人工批准计划后才能执行的 review/gate 流程；当时方案保留固定目标货架选择与 `/start` 启动门。
- 旧 job schema 的长期兼容和通用迁移框架。
- 通用 Provider 插件平台。
- 生产凭据自动 E2E、浏览器 E2E、持续冒烟平台、覆盖率 KPI。
- 自动删除正式库中的疑似重复项或未知附件。
- 把媒体库所有缺集、缺字幕清零作为软件版本发布条件。

### 2.3 仍然必须保留的本地安全底线

这些不是企业级过度设计，而是本地自动整理也必须具备的边界：

1. 来源、staging 和正式库路径必须位于允许的媒体树内。
2. 来源与目标不得出现危险的父子包含或越界关系。
3. 正式目标已存在时默认停止，绝不自动覆盖。
4. `.exe` 只能按字节魔数识别，绝不执行。
5. 归档成员必须拒绝绝对路径、路径穿越、链接和等价名称冲突。
6. 解压必须限制成员数、目录深度、展开大小、展开比例和磁盘余量。
7. 视频进入正式库前必须满足最小大小并通过 `ffprobe` 视频流检查。
8. 字幕进入正式库前必须绑定确切视频，并通过基本文本/语言检查。
9. 自动清理只允许任务自建 staging、明确临时文件和极小的无人值守白名单。
10. 存在 `problem_files` 或无法唯一判断时，整项任务停止并保留来源。
11. 控制状态损坏或无法读取时必须保持暂停，而不是继续写库。
12. 本地写接口仍需最小 same-origin/JSON 保护，避免网页跨站请求操作 localhost。

### 2.4 不使用 SHA 后的验证标准

| 操作 | 执行前 | 执行后 | 不一致时 |
| --- | --- | --- | --- |
| AList move/rename | 来源精确存在，目标不存在，大小合理 | 目标路径和大小正确，来源消失 | 停止，不覆盖 |
| 本地上传 | 目标不存在，使用 create-only | 精确路径和大小正确 | 停止并保留本地 payload |
| 普通视频 | 扩展名、最小大小、来源归属 | `ffprobe` 存在视频流 | 拒绝进入正式库 |
| 归档 | 7-Zip listing、成员预算和路径安全通过 | 解压退出成功，输出大小符合 listing，视频通过 `ffprobe` | 保留原归档和任务 staging |
| 字幕 | 精确视频/集/语言绑定，目标不存在 | 文本可解码、格式可解析、语言符合，目标大小正确 | 不安装、不覆盖 |
| 自动清理 | 路径属于当前任务且命中白名单 | 精确对象消失 | 标记清理失败，不扩大删除范围 |

BitTorrent infohash、7-Zip 自带 CRC 或外部协议已有的标识可以继续使用；禁止的是为了应用事务重新读取整份媒体计算 SHA-256。Git 自身的对象哈希也不属于应用级媒体事务。

## 3. 当前基线快照

以下数字只是 2026-08-08 的历史实施起点，不是当前运行时或当前主线实现状态的权威；实时状态仍以 Web 和 API 为准。

- AList、API 和 Web 容器健康。
- 系统因“伪装压缩包的安全识别与 staging 解包”处于持久暂停。
- 公共任务约 85 个，其中 2 个完成、1 个取消、34 个仍在缺口发现、48 个等待重试。
- 最近审计扫描 7,752 个文件，报告 385 个缺集、722 个缺字幕、723 个字幕证据未知、39 组疑似重复和 6 个空目录。
- 本地状态和 staging 约 3.5GB，包含未完成下载和旧 attempt。
- 当前分支有 137 个 tracked 文件变化、约 11.1 万行删除以及数十个 untracked 文件，尚未形成可回退的迁移提交基线。
- 当前 `npm run check` 通过，但只覆盖当前窄路径；Engine 专项测试已全部移除。

因此当时项目可定义为：

> 自动主链已经成形并有真实写入，但仍处于大规模未提交迁移期；软件闭环、状态一致性和归档入口尚未完成。

## 4. 当时的目标主流程

> 以下 target-shelf-first 顺序只记录当时方案，已被根 `AGENTS.md` 的 reconciliation-first 流程取代。

```text
用户提交 / 入站监控发现来源
  → 路径与任务归属检查及 awaiting_target_shelf 登记
  → 用户选择电影 / 番剧 / 美剧并调用 /start
  → 后端映射固定一级目标根后正式启动
  → 普通媒体 / 归档 / 保留项分类
  → 归档安全检查与任务 staging 解包（如需要）
  → TMDB 身份、电影/剧集、季集和命名规划
  → problem-file 与无人值守清理门禁
  → 一个正式库 writer 执行
  → 路径 + 类型 + 大小回读
  → 视频 ffprobe / 字幕文本验证
  → 当前作品定向审计
  → 可判定缺口进入 Provider
  → Provider payload 回到同一个 staging / 规划 / writer 流程
  → 清理任务自有 staging 和明确临时项
```

归档模块、Provider 和字幕模块都不能决定最终 TMDB 身份、正式目录或文件名；这些仍由现有 Engine 规划负责。

## 5. 被删除旧模块的能力处置

### 5.1 必须轻量迁移或立即修复

| 能力 | 旧来源 | 当前缺口 | 轻量处置 |
| --- | --- | --- | --- |
| 归档安全内核 | `engine/tools/extract_archives.py`、旧 `replenishment_local_adapter.py` 的 SFX 部分 | 当前没有 ZIP/7z/RAR/伪装 EXE 执行入口 | 迁移魔数、分卷、密码、安全 listing、预算、选择性解压；删除 SHA/receipt/journal |
| 持久暂停语义 | `local/scrapeflow_api/lifecycle.py` | 正常 shutdown 会写成永久暂停；损坏/缺失控制文件可能错误放行 | 提取约一个小型 control-state 模块；损坏 fail-closed；shutdown 不改用户持久状态 |
| 路径、归属与脱敏 | `local/scrapeflow_api/validation.py`、旧 server | 当前多个模块重复路径判断，异常文本可能原样返回 | 收敛媒体根、入站、staging、重叠检查和 token/password 脱敏；不恢复 digest |
| 本地控制面同源保护 | 旧 server Handler | 当前 POST 不检查 `Content-Type`、Origin 或 Sec-Fetch-Site | 只接受 `application/json`；Host/Origin 必须为同源或 loopback；加少量 Handler 测试 |
| 计划 finalizer | 旧 `finalize_plan_evidence` | runner 查找旧函数名，当前 `finalize_plan` 实际未调用 | 修正调用；先与当前 planner 已有首选字幕逻辑对比，只迁移尚未覆盖的“同视频只保留一条”场景；不迁移证据 digest |
| problem-file 执行门禁 | 旧 `_require_problem_free_media_plan` | 当前计划含 `problem_files` 时仍可能移动文件 | 任一未闭合问题默认阻断整次正式写入 |
| 无人值守删除白名单 | 旧 cleanup gate | 当前 detached audio、PDF、漫画、字体、manifest 等可能进入自动 cleanup | 仅允许任务 staging、引擎自建临时文件、`.DS_Store`、AppleDouble 等极小白名单；其他全部保留 |
| 公共 phase/provider 契约 | `contracts/job-phases.json` | Python/TS 已漂移，`failed_cleanup` 可能无法重启加载，Provider 类型不一致 | 恢复一个很小的当前版运行状态数据源和集合一致性测试，不恢复完整 transition/schema 系统 |
| 唯一季集解析 | 旧/当前 Engine 与 Local 多份实现 | 同一名称已能解析成不同集数 | `replenishment_matching.py` 成为唯一权威，其他模块只调用 |
| 唯一媒体类型策略 | 旧/当前多个常量集合 | `.iso/.mts/.strm/.flv` 等各阶段判断不一致 | 由一个 Engine 模块统一视频、字幕、归档、临时扩展名 |
| 字幕真实内容分类 | `refine_subtitle_audit.py` | 当前大多相信扩展名、文件名和大小 | 读取有限前缀，解码并解析 ASS/SRT/VTT，区分简中、繁中、日文、未知；不做内容 hash |
| 审计残留可见性 | `audit_live_library.py`、`build_library_audit_report.py` | 当前未知附件、归档、孤立字幕可能不影响 `clean` | 让当前审计复用 `residual_policy`，只报告，不自动删除 |
| 作品级定向审计 | `ordinary_completion.py`、`title_closure*.py` | 当前每次写入可能触发全库扫描 | 迁移“只重扫当前 target_root、排除嵌套独立作品”的思想；不恢复签名 closure 工件 |
| 本地启动配置 | `local/scrapeflow_api/config.py` | `npm run local` 未指定状态目录时默认 `/data`，原生启动可能不可用 | 保留少量 typed env；Compose 显式 `/data`，原生运行默认项目本地状态目录 |
| 终态任务与 staging 清理 | 旧 server 的终态维护入口 | 当前无安全公开清理入口，staging 和 gap 状态持续增长 | 只清 terminal root 及其拥有的 gaps/staging；active/retry/child owner 一律拒绝 |
| Web 失败修正入口 | 旧 review/failure UI | 失败身份只能原参数重试，归档密码无兜底入口 | 只在失败详情提供 TMDB/type/season 修正和临时 archive password；不恢复审批 UI |

### 5.2 当前已有替代，不恢复旧模块

| 被删除模块或能力 | 当前替代 | 后续动作 |
| --- | --- | --- |
| `local/server.py` | `local/simple_server.py` + runner/audit/replenishment | 保留新主链，只修已确认退化，不回滚旧单体 |
| `local/scrapeflow_api/models.py` | 当前 `EngineRequest`、`EngineJob` 和原子 JSON | 保留当前轻量模型 |
| `local/scrapeflow_api/scheduler.py` | 当前 executor、timer 和单 writer lock | 暂不为架构美观重写；只修关机、重试和统计问题 |
| `alist_exact_file_adapter.py` | 当前 AList client 和 `SimplePlanExecutor` 精确路径/大小回读 | 不恢复协议 adapter；共享少量 exact-read helper 即可 |
| `local_upload_transaction.py` | create-only upload + 路径/大小回读 | 永久删除旧事务实现 |
| `remote_file_transaction.py`、`hybrid_remote_transaction.py` | AList native move + 单 writer + 冲突拒绝 | 永久删除旧事务实现 |
| `remote_delete_transaction.py` | 任务归属检查 + 极小删除白名单 + 删除后回读 | 不恢复远端备份事务 |
| `simple_library_audit.py` | 替代旧 audit/report/title-closure 主体 | 直接补残留可见性和 scoped audit，不恢复旧报告器 |
| `automatic_replenishment.py`、当前 torrent adapter | 替代旧字幕/补源大部分编排 | 直接修当前路径，不恢复独立 runtime |
| 当前字幕 ledger 和 sidecar writer | 替代旧 subtitle executor 主体 | 只补文本语言校验和一致性 |
| `canonical_work_tree.py` | 文件仍在并由当前 Engine 使用 | 恢复少量核心行为测试，不恢复 one-time 外壳 |
| `residual_policy.py` | 文件仍在并由当前 Engine 使用 | 审计与执行共同使用；修正自动清理范围 |
| `serialization.py` | 当前仍提供原子 JSON | 保留 temp + replace；不扩展为事务日志体系 |

### 5.3 永久删除的旧能力

以下模块可以保留为 Git 历史参考，但不应回到活动源码：

- `engine/scrapeflow/formal_library_remediation.py`。
- `engine/scrapeflow/hybrid_remote_transaction.py`。
- `engine/scrapeflow/local_upload_transaction.py`。
- `engine/scrapeflow/remote_file_transaction.py`。
- `engine/scrapeflow/remote_delete_transaction.py`。
- `engine/scrapeflow/one_time_library_completion.py`。
- `engine/scrapeflow/one_time_movie_member_scope.py`。
- `engine/scrapeflow/one_time_tmdb_snapshot.py`。
- `engine/scrapeflow/one_time_tv_exclusion_scope.py`。
- 与上述一次性流程对应的 plan/audit/prepare/run 工具。
- `engine/scrapeflow/cli_args.py` 和旧 Engine CLI 密钥参数体系。
- `engine/scrapeflow/subtitle_content_witness.py` 的对白 SHA、时间线和多窗口证明。
- `engine/tools/replenishment_http_adapter.py`，直到确实出现第二种可执行远端 Provider。
- `local/scrapeflow_api/formal_library_maintenance.py` 及其 CLI。
- `local/scrapeflow_api/legacy_one_time_migration.py`。
- `local/tools/retire_legacy_one_time_owners.py`。
- `scripts/migrate_job_state_v2.py`。
- `scripts/run-unittest-json.py`。
- 旧 LaunchAgent 自动备份模板。
- `app/components/review-dialog.tsx` 的批准、摘要和 digest UI。
- `tests/rendered-html.test.mjs` 的旧界面正则测试。
- 旧 closure、migration、transaction、receipt 和 approval 专项测试。

### 5.4 默认关闭、按真实需求再决定的能力

| 可选能力 | 旧来源 | 默认决定 | 重新启用条件 |
| --- | --- | --- | --- |
| 夸克分享直存 | `quark_fast_save_bridge.py` | 不进入当前发布主线 | 用户确认分享链接是常用来源，并提供可稳定验证的真实样本 |
| 夸克磁力离线/CDP/WSG | `quark_native_helper.py` | 默认永久关闭 | 只有本地 Torrent 明确无法满足且用户接受宿主 helper 维护成本时单独立项 |
| 烧录字幕 OCR | `burned_in_subtitle_ocr.py`、`probe_burned_in_subtitles.py` | 默认关闭 | 实际硬字幕误报频繁，人工“已含硬字幕” override 不够用时再启用 |
| 对白级字幕 witness | `subtitle_content_witness.py` | 不恢复 | 当前产品没有需要该证明级别的场景 |
| 存量正式库自动整改 | `formal_library_remediation.py` | 只报告，不自动执行 | 用户明确要求自动移动存量文件时，另做轻量 action list + 隔离目录 |
| 自动正式库去重 | 旧 maintenance | 只报告 | 默认不启用；疑似重复仍由用户在 AList 手工处理 |
| 自动定时备份 | `backup_host_state.py` | 不恢复旧 989 行脚本 | 若需要，仅提供停机后复制 AList 数据和必要状态的简短手工方案 |
| 多层嵌套归档 | 旧 archive runtime | 第一版只支持一层 | 真实样本证明常见后，再增加固定最大深度 |

## 6. 分阶段实施计划

### 阶段 0：冻结范围并建立可回退基线

#### 目标

停止继续在 11 万行未提交变更上漂移，建立后续每个阶段都能比较和回退的源码基线。

#### 工作项

1. 保持当前全局暂停。
2. 不清理当前 staging、gap、旧任务或正式库。
3. 核对 tracked、untracked 和删除文件，确认没有用户文件被误纳入源码变更。
4. 将本计划中的“迁移、已有替代、永久删除、可选”矩阵作为当时的能力判断参考。
5. 形成当前迁移的 Git checkpoint；`state/`、`backups/`、凭据和媒体文件不得进入提交。
6. 记录服务健康、任务数量、审计数量和 staging 大小，不制作文件 SHA 清单。
7. 明确旧 API、旧 job schema、旧 approval UI 不再兼容。

#### 交付物

- 一个可识别、可回退的迁移 checkpoint。
- 本计划文档。
- 当前运行态的简短只读记录。

#### 验收条件

- 当前轻量检查通过。
- Git 变更边界可解释。
- 没有恢复队列、清理状态或修改正式库。

#### 明确禁止

- 不执行 `reset --hard` 或批量 checkout 旧目录。
- 不整批恢复被删模块。
- 不在本阶段修改真实媒体。

### 阶段 1：修复当前主链的轻量 P0 回归

#### 目标

在接入归档前，先保证当前普通媒体路径不会因控制、计划或清理退化造成错误写入和删除。

#### 工作项

1. 修正 runner 对 `finalize_plan_evidence` 的旧函数名调用，改为当前 `finalize_plan`。
2. 对比旧 `_retain_one_subtitle_track_per_video` 与当前 planner 已有的 `preferred_excluded_subtitles` 逻辑；只补当前未覆盖的场景，避免再维护第二套字幕选择器。
3. 在任何正式 move/upload 前增加 `problem_files` 阻断。
4. 将自动 cleanup 收窄为：
   - 当前任务明确创建的 staging/temp；
   - `.DS_Store`；
   - AppleDouble `._*`；
   - 当前任务明确创建且可重建的下载临时文件。
5. PDF、漫画、字体、独立音轨、manifest、未知图片、主题视频和未知可执行文件默认保留并报告。
6. 修复持久暂停：
   - 控制文件缺失、损坏、缺字段或类型错误时返回 paused；
   - 正常 shutdown 只关闭内存调度，不改写用户持久暂停；
   - 显式 pause/resume 才修改控制文件。
7. 修复 `failed_cleanup` 等 phase 在持久化、重启加载和前端展示中的集合漂移。
8. 为写请求增加最小控制面保护：JSON Content-Type、Host/Origin loopback/same-origin 检查。
9. 统一异常脱敏，归档密码、AList 密码、token 和 API key 不进入 job log、API 错误或 UI。
10. 修复原生 `npm run local` 的默认状态目录；Docker 仍显式使用 `/data`。

#### 交付物

- 小型 control-state 模块或等价收敛实现。
- 计划 finalizer 和 problem-file gate。
- 极小无人值守删除白名单。
- 本地 API 同源写保护。
- 当前版 phase 契约。

#### 验收条件

- 控制文件损坏或缺失时系统保持暂停。
- 正常重启不会把未暂停系统永久改成 `shutdown` 暂停。
- 含任意 `problem_files` 的计划不会执行 move、upload 或 cleanup。
- 字体、文档、音轨和未知附件不会被自动删除。
- `failed_cleanup` 任务可以重启加载并在 UI 显示。
- 跨站 `text/plain` POST 和非同源写请求被拒绝。
- 原生本地启动不会默认写入不存在或无权限的 `/data`。

#### 明确禁止

- 不恢复审批对话框。
- 不恢复 cleanup rollback transaction。
- 不为控制文件增加 SHA、receipt 或版本迁移框架。

### 阶段 2：统一最小领域定义和真实能力声明

#### 目标

先消除会让规划、审计、补源和归档继续产生不同结论的重复定义。

#### 工作项

1. 让 `engine/scrapeflow/replenishment_matching.py` 成为季集、范围、三位集数、分数集和候选覆盖的唯一权威。
2. 删除 Local 中“Keep this duplicated matcher aligned”对应的复制实现，改为调用共享函数。
3. 由一个 Engine 模块统一：
   - 视频扩展名；
   - 字幕扩展名；
   - 归档/分卷扩展名；
   - 下载临时扩展名；
   - 图片、文档、字体和音轨辅助分类。
4. planning、audit、replenishment、materializer 和 residual policy 全部引用该策略。
5. Provider 能力只由当前可执行 materializer 决定。
6. 当前若只能执行 Torrent，就只报告 `torrent/magnet` 可用；删除 `cloud_share ready` 和优先排序。
7. 搜索层不得接受执行层必然拒绝的 candidate kind。
8. 审计开始复用 residual policy，将未知残留、归档、孤立字幕和附件显示出来，但不自动删除。

#### 交付物

- 一个季集解析权威。
- 一个媒体/字幕/归档类型策略权威。
- 一个真实 Provider capability 响应。
- 少量参数化一致性测试。

#### 验收条件

- `S04-89 [S4][17_89]` 等已知分歧输入在所有阶段得到同一结果。
- `.iso/.mts/.strm/.flv/.rmvb` 等扩展名在所有阶段结论一致。
- API 不再显示无法执行的 Provider 为 ready。
- 未知残留和孤立字幕能在审计中看到，但不会被自动删除。

#### 明确禁止

- 不全面重写 Engine。
- 不为一个 Provider 设计插件框架。
- 不为了模块漂亮立即拆完 10k 行 `core.py`。

### 阶段 3：提取轻量归档安全内核

#### 目标

以 Git HEAD 中旧归档实现和测试为行为参考，建立一个不含事务证明的当前 archive domain。

#### 建议边界

```text
ArchiveInspector
  ├─ 扩展名、魔数和分卷识别
  ├─ 有界密码候选
  ├─ 7-Zip 安全成员列表
  ├─ 路径/链接/冲突检查
  └─ 展开预算和磁盘预算

ArchiveExtractor
  ├─ 只写任务 staging
  ├─ 只解选中的视频和字幕
  ├─ 输出路径与大小核对
  └─ 视频 ffprobe / 字幕文本验证
```

当前 AList client 已保留 `archive_meta`、`archive_member_bytes`、`archive_decompress`、文件前缀读取和下载接口，应优先复用，不再创建第二套 AList archive client。

#### 工作项

1. 从旧 `extract_archives.py` 提取普通 ZIP、7z、RAR 和连续分卷识别。
2. 通过文件前缀魔数识别伪装 `.exe/.bin/.dat`：
   - ZIP/7z/RAR 按归档处理；
   - 明确 MKV/MP4 可转入媒体识别；
   - 未知格式保留并失败；
   - 任何路径都不得执行 EXE。
3. 密码候选固定有界，建议顺序：
   1. 当前 retry 请求中的临时人工密码；
   2. 路径或同目录小文本中的明确“密码”标记；
   3. 来源树内唯一的密码提示；
   4. 最近父目录 basename；
   5. 最多再向上一层 basename。
4. 多个互相冲突的密码提示不盲猜；自动候选耗尽后进入可读失败。
5. job JSON 只记录 `password_source` 和尝试结果，不记录密码值。
6. 拒绝：
   - 绝对成员路径；
   - `..` 路径穿越；
   - 符号链接和硬链接；
   - Unicode/NFC/大小写等价冲突；
   - 缺失分卷；
   - 超成员数、超目录深度、超展开总量、超展开比例或磁盘不足。
7. 第一版只支持一层归档；成员仍是大归档时明确返回 `nested_archive_unsupported`。
8. 只解视频、字幕和规划所需的目录结构；字体、说明文件和其他附件留在原归档中。
9. 加入 API 容器所需 7-Zip 依赖。
10. 归档输出只能写入 `<task-staging>/archive/`，不能原地解到入站或正式库。

#### 交付物

- 一个共享 archive domain 模块。
- 一个接入当前 AList client 的 extractor。
- 精选归档安全回归测试。
- Docker 7-Zip 依赖。

#### 验收条件

- 普通 ZIP/7z/RAR 可以识别和列出成员。
- 连续分卷可识别，缺卷会停止。
- 有密码归档、明确密码标记和父目录名密码可以处理。
- 伪装 EXE 被识别且代码不存在执行 EXE 的调用。
- 路径穿越、绝对路径、链接、等价名称冲突和压缩炸弹被拒绝。
- 密码不出现在 job JSON、日志、异常或 Web 响应中。
- 解出视频通过大小和 `ffprobe`；字幕通过基本内容分类。
- 冲突或失败时原归档仍存在。

#### 明确禁止

- 不迁移 plan SHA、成员 SHA、receipt、nonce、lease 或复杂 journal。
- 不直接解压进正式库。
- 不自动递归无限嵌套归档。
- 不因失败删除未知 EXE。

### 阶段 4：把归档接入当前两条入口

#### 目标

在当时方案中，已由用户选择货架并通过 `/start` 启动的普通入站归档，与 Provider SFX 共用同一归档模块，解出后回到同一 Engine 规划和单 writer 路径。Provider child 继承根任务已确认的货架，不自行选择一级根。

#### 统一流程

```text
已选择并启动的入站来源 / 继承根任务货架的 Provider payload
  → 普通媒体或 ArchiveInspector
  → 任务专属 staging
  → ArchiveExtractor（如需要）
  → 当前 Engine 身份与季集规划
  → 当前 problem-file gate
  → 当前单 writer
  → 路径 + 大小 + ffprobe 回读
  → 当前作品审计
  → 任务 staging 清理
```

#### 工作项

1. 已启动的普通来源在 TMDB/媒体规划前增加 archive preprocessing；等待选择的来源不得进入该阶段。
2. Provider materializer 在 staging 准入前增加相同 preprocessing。
3. 解压输出必须重新经过当前身份、季集、命名和冲突检查，不能信任压缩包名称。
4. 补源 child 继续保持 media-only，不创建每集 NFO 或海报。
5. 字幕压缩包只能产出绑定到确切视频、确切集和确切语言的 sidecar。
6. 多作品混合归档无法唯一判断时整项失败，不擅自拆分多个作品。
7. 任务拥有的原归档和 staging 只能在正式写入、回读、元数据、定向审计和 Provider 达到终态后的最终 cleanup 中处理，不能在 writer 成功后立即清理。
8. 失败、取消和密码错误时保留原归档；任务自有解压临时目录可以安全重建。
9. 重启恢复只按阶段、路径和大小对账：
   - staging 输出完整则复用；
   - 输出不完整则清理任务自有解压目录后整次重解；
   - 正式目标存在且大小正确则继续完成；
   - 事实冲突则停止。
10. Web 增加归档阶段和失败原因；人工密码只在 retry 请求内存中使用。

#### 交付物

- 普通入站 archive adapter。
- Provider archive/SFX adapter。
- 统一归档状态和错误展示。
- 失败重试的临时密码入口。

#### 验收条件

- 普通视频、普通归档和伪装 SFX 最终都进入相同 planning/writer 路径。
- 代码中不存在普通入站与 Provider 各自一套解压器。
- 正式写入仍只有一个 writer。
- 成功任务不会因原归档留在入站而重复创建任务。
- 失败任务不会删除原输入。
- 重试不会创建第二份正式媒体。

#### 恢复队列的最低条件

只有阶段 0 至阶段 4 完成，并用一个受控普通归档和一个伪装归档样本确认结果后，才允许用户手工解除当前全局暂停。该样本确认是一次变更验收，不建设长期自动冒烟平台。

### 阶段 5：收敛 Provider、重试和下载残留

#### 目标

让本地补源队列有限、真实、可理解，不再无限堆积 retry timer、root job 和 `.aria2`。

#### 工作项

1. 主线只保留当前真实可执行的 Magnet/Torrent lane。
2. 删除 `cloud_share ready`、不可执行候选优先级和无调用者的通用 acquire 外壳。
3. 将错误分为少量用户可理解状态：
   - 没有候选；
   - 候选与缺口不匹配；
   - 没有可用 peer/下载停滞；
   - Provider/网络不可用；
   - payload 校验失败；
   - 归档检查或解压失败。
4. 自动重试达到上限后停止；只有用户手工 retry 才创建新 attempt。
5. 同一 gap 同时只能有一个活动 owner。
6. 已完成、取消或过期 attempt 的任务自有 `.aria2` 和 staging 按保留规则清理。
7. 下载完成 payload 只有通过扩展名、大小、归档检查和 `ffprobe` 才能进入远端 staging。
8. Provider worker 保持有界，不增加正式库 writer。
9. 若未来确实恢复夸克分享，作为独立可选阶段接入同一 staging/归档/规划路径。

#### 交付物

- 真实 Provider capability。
- 有上限的重试策略。
- attempt/staging 所有权和清理规则。
- 可理解的失败分类。

#### 验收条件

- 不可执行 Provider 永远不会被选择。
- 同一缺口只有一个活动 owner。
- 达到重试上限后不再继续创建 timer 或 attempt。
- 完成、取消和过期任务不留下无主 `.aria2`。
- Provider payload 不能绕过归档、大小、媒体和正式写入检查。

#### 明确禁止

- 不恢复通用 Provider 插件框架。
- 不恢复 Quark native helper 作为主线依赖。
- 不为了吞吐增加多个正式库 writer。

### 阶段 6：统一审计、字幕和作品级复核

#### 目标

解决当前 audit、ledger 和 sidecar writer 对同一文件得出不同结论的问题，并避免每次写入触发全库重扫。

#### 工作项

1. 审计和 ledger 使用同一个字幕判定函数和同一组媒体扩展名。
2. ledger 只作为缓存；键绑定路径、大小和可用版本信息，变化即失效。
3. 外挂字幕读取有限前缀，尝试 UTF-8、UTF-16、GB18030、Big5 等常见编码。
4. 只解析 ASS Dialogue、SRT/VTT 时间轴和正文：
   - 简中：可作为目标语言证据；
   - 繁中/日文：不能冒充简中；
   - 无法判断：保持 unknown。
5. `ffprobe` 超时、读取错误和信息不足只能生成 unknown，不能生成 `missing_subtitle`。
6. 成功、缺失、未知三种状态必须互斥。
7. 正式 sidecar 写入必须绑定 exact video、exact episode 和 exact language，目标已存在时停止。
8. 普通任务或补源完成后只重扫受影响作品根；全库审计只在启动低频或用户手工触发。
9. 嵌套独立电影/剧集根必须从父作品 scoped audit 排除。
10. 审计显式报告归档、孤立字幕、未知文件、临时下载和非媒体附件，但只观察不自动删除。
11. 硬字幕默认使用用户可设置的“已含硬字幕/无需补字幕”作品级 override；OCR 不进入主线。
12. 39 组疑似重复继续只报告，不自动处理。

#### 交付物

- 一个字幕内容轻量分类器。
- 一个 audit/ledger 统一判定入口。
- 一个 scoped audit 入口。
- 作品级字幕策略 override。

#### 验收条件

- 相同文件不会同时在 ledger 中为 satisfied、在 audit 中为 unknown。
- 探针失败不会创建字幕补源任务。
- 日文、繁中和无效文本不会作为简中 sidecar 安装。
- 已有目标语言字幕不会重复写入或覆盖。
- 成功侧挂后下一次作品级审计关闭对应缺口。
- 每次单任务写入不再强制触发完整媒体库扫描。

#### 明确禁止

- 不恢复 OCR 依赖和抽帧任务。
- 不恢复对白 SHA、时间线 witness 或不可变证据 cache。
- 不把缺字幕总数归零作为代码验收。

### 阶段 7：简化状态、清理、Web 和本地运维

#### 目标

保留本地使用真正需要的恢复和可观察性，同时清理迁移留下的状态堆积。

#### 工作项

1. 任务状态只保存：
   - 当前阶段；
   - 来源路径；
   - staging/输出路径；
   - 预期大小；
   - 最终路径；
   - attempt；
   - 可读错误；
   - 根任务与内部 child 归属。
2. 重启对账规则：
   - 目标存在且大小正确：继续完成；
   - 来源仍在、目标不存在：可以重试；
   - 两边都在：目标冲突，停止；
   - 两边都不在：来源丢失，停止；
   - 目标大小不符：停止，不覆盖。
3. 根任务汇总内部 child 的真实成功、失败和当前阶段；health 与 Web 只统计公共根任务。
4. 增加安全清理入口：
   - 只允许 terminal root；
   - 拒绝 active、retry_wait 和仍有活动 child 的任务；
   - 只清它拥有的 gap、local staging 和任务 JSON；
   - 正式媒体库永远不在该入口范围内。
5. 定义简单保留周期，例如：成功任务短期保留、失败任务更久保留；具体天数作为普通配置，不建设策略引擎。
6. `engine-jobs`、`replenishment-batches`、旧 `journals` 等残留在暂停状态下人工核对后移动到一次性 `legacy-archive/<date>`；一个版本后再决定删除。
7. Web 只增加失败修正入口：
   - `failed_identity` 可修正 TMDB ID、类型或季；
   - archive failure 可输入一次性密码；
   - terminal task 可清理记录；
   - 不恢复计划批准和 digest 展示。
8. 本地备份只写简短操作说明：停止 API/AList，复制 `alist-data` 和必要状态，排除可重下 staging，再启动。
9. 不恢复旧 989 行 backup daemon、LaunchAgent、pause journal 或 SHA manifest。

#### 交付物

- 简单重启对账矩阵。
- 准确的 root/child 汇总。
- terminal-only 清理 API 和 Web 入口。
- 遗留状态处理记录。
- 简短本地备份说明。

#### 验收条件

- 显式暂停跨重启保持，未暂停状态正常重启后仍可继续调度。
- 重启不会重复创建正式文件。
- health、Web 和磁盘公共任务统计一致。
- active/retry/child-owned 状态无法被清理。
- 清理只命中任务状态和 task-owned staging。
- 旧状态目录不再被当前代码引用。

#### 明确禁止

- 不恢复逐操作 journal。
- 不恢复 rollback payload。
- 不建立通用状态 schema 迁移框架。
- 不自动删除疑似重复或未知正式媒体。

### 阶段 8：代码收敛、轻量验证、文档和发布

#### 目标

去掉迁移外壳和虚假能力，用少量高价值测试防止再次整块丢失业务能力。

#### 工作项

1. `_replenishment_local_adapter_impl.py` 只保留活动 Torrent/归档调用；逐步让 `search.py`、`materialize.py` 成为真实所有者，最后删除无调用者兼容函数。
2. `core.py` 只在有清晰业务所有者时继续拆分；完整拆完不作为发布条件。
3. 删除重复 `_compat_dispatch`、运行时 binder 和无调用者 facade，但每次只改一个行为边界。
4. 恢复一个很小的 `engine/tests`，只测试危险纯逻辑和核心业务规则。
5. 调整轻量检查：
   - lint；
   - TypeScript 类型检查；
   - Python 活动模块导入；
   - 精选 Python 单元测试；
   - Web production build。
6. 不接入真实 AList、TMDB 或 Provider 凭据到自动检查。
7. 更新 README、ARCHITECTURE、环境示例和当前自动工作流规格，删除假 Provider、旧状态布局和过期可靠性承诺。
8. 每个阶段形成独立 Git checkpoint。
9. 对涉及真实写库的阶段，只做一次用户可观察的受控样本确认；不建设持续冒烟平台。

#### 精选回归行为

- 伪装 EXE 识别且绝不执行。
- 分卷、密码、路径穿越、链接、压缩炸弹和冲突拒绝。
- 密码不泄漏。
- 唯一季集解析器和唯一扩展名策略。
- 不同 TMDB ID 不合并，同身份多版本不误删。
- plan finalizer 实际执行，同视频只选择一条首选字幕。
- `problem_files` 阻断和无人值守删除白名单。
- create-only、目标冲突、路径越界、大小回读和源消失。
- 字幕可解码、格式有效、语言正确且 exact-video 绑定。
- 审计能看到未知残留、孤立字幕和归档。
- 控制文件损坏 fail-closed，shutdown 不改持久暂停。
- Provider 声明与可执行 materializer 一致。
- audit 与 ledger 对同一文件结论一致。
- active 任务不可被状态清理。

测试数量不是目标。优先使用纯函数、临时目录、Fake AList 和模拟 7-Zip listing；不恢复旧 1,427 个测试，不建立真实网络 E2E。

#### 发布验收条件

- 轻量检查通过。
- 文档列出的 Provider、阶段、目录和清理边界都能从代码核对。
- 没有被删除旧模块的活动 import。
- 没有 `cloud_share ready` 等虚假能力。
- 普通媒体、普通归档和伪装归档共用同一正式写入路径。
- 全局暂停是否解除由用户明确决定。

#### 不作为发布阻塞条件

- `core.py` 是否已经完全拆小。
- 媒体库 385 个缺集是否全部补齐。
- 722 个缺字幕是否全部清零。
- 39 组疑似重复是否已人工处理。
- 所有 Provider 是否都能找到资源。
- 是否拥有自动 OCR、Quark helper 或浏览器 E2E。

### 阶段 9：可选能力独立立项

这些能力不能夹带在主线修复中。只有用户明确确认需求后才建立独立阶段。

#### 9A：夸克分享直存

只迁移：分享链接发现、精确文件选择、直存到任务 staging、路径/大小回读、SFX 进入统一归档模块。不得恢复通用 Quark 事务、SHA receipt 或独立正式库写入。

#### 9B：夸克磁力离线

先验证是否确实需要 CDP/WSG。若需要，helper 只能绑定 loopback、凭据只驻留内存、输出只能进入任务 staging。该能力不能成为普通 Torrent 主链的依赖。

#### 9C：烧录字幕 OCR

只有硬字幕误报成为真实高频问题时启用。默认 off，限当前作品、有限抽帧、失败返回 unknown；不生成不可变证据链。

#### 9D：存量正式库自动整改

默认保持 audit report-only。若未来需要自动移动，使用明确 action list、目标冲突停止和轻量隔离目录；不恢复 hybrid transaction。

#### 9E：自动备份

先使用停机复制方案。只有手工备份确实无法满足时，再做一个短小脚本处理 SQLite 一致复制和保留数量；不恢复 SHA manifest、LaunchAgent 和暂停 journal。

## 7. 阶段依赖与优先级

| 优先级 | 阶段 | 依赖 | 是否阻止恢复队列 |
| --- | --- | --- | --- |
| P0 | 阶段 0：源码基线 | 无 | 是 |
| P0 | 阶段 1：当前安全回归 | 阶段 0 | 是 |
| P0 | 阶段 2：共享领域定义 | 阶段 1 | 是 |
| P0 | 阶段 3：归档内核 | 阶段 2 | 是 |
| P0 | 阶段 4：归档接入 | 阶段 3 | 是 |
| P1 | 阶段 5：Provider/重试 | 阶段 2、4 | 不阻止受控运行，但阻止宣称补源稳定 |
| P1 | 阶段 6：审计/字幕 | 阶段 2 | 不阻止普通媒体整理，但阻止宣称字幕闭环稳定 |
| P1 | 阶段 7：状态/Web/清理 | 阶段 1、5 | 不阻止受控运行，但阻止发布稳定版 |
| P1 | 阶段 8：收敛/发布 | 阶段 4 至 7 | 阻止发布稳定版 |
| P2 | 阶段 9：可选能力 | 主线稳定后 | 否 |

## 8. Git 与数据操作策略

### 8.1 Git checkpoint

- 阶段 0 先保存当前迁移基线。
- 后续每个阶段独立提交，不把归档、字幕、Provider、状态清理混成一个提交。
- Git checkpoint 是本计划的主要回退手段，不建设应用级 rollback transaction。
- 旧实现始终可以通过 `git show HEAD:<path>` 作为行为参考，不需要把整文件重新加入工作树。

### 8.2 真实数据边界

- 实施阶段默认保持调度暂停。
- 不自动清理现有 3.5GB staging，先建立所有权和状态映射。
- 不自动删除旧 `engine-jobs`、`replenishment-batches` 或备份目录。
- 正式媒体库只允许当前单 writer 按新门禁修改。
- 不确定时保留数据并产生可读错误。

### 8.3 失败与回退

如果某阶段代码不稳定：

1. 保持或恢复全局暂停。
2. 回退到上一 Git checkpoint。
3. 保留来源、归档和失败 staging。
4. 不通过批量删除“恢复干净状态”。
5. 修正后对一个受控样本重新确认。

## 9. 完成定义

### 9.1 可以恢复日常普通任务

- 阶段 0 至阶段 4 完成。
- problem-file 和 cleanup 门禁生效。
- 控制状态、同源写保护和原生启动修复完成。
- 普通媒体、普通归档、伪装归档共用同一 writer。
- 用户明确解除全局暂停。

### 9.2 可以称为“本地稳定版”

- 阶段 5 至阶段 8 完成。
- Provider 声明真实，重试有上限。
- audit/ledger 对同一文件结论一致。
- 状态和 staging 有安全清理规则。
- Web 能处理最终身份/归档失败。
- 轻量检查和 production build 通过。
- 文档与实际代码一致。

### 9.3 不要求完成

- 不要求媒体库所有资源缺口清零。
- 不要求恢复所有旧 Provider。
- 不要求恢复 OCR、自动备份或正式库自动整改。
- 不要求恢复旧测试数量。
- 不要求提供事务证明或 SHA receipt。
- 不要求把所有大文件一次性拆小。

## 10. 删除文件逐组处置索引

本节用于确保旧项目删除的模块都已进入计划，不因文件数量大而再次漏掉能力。

### 10.1 Engine domain

| 删除文件 | 处置 |
| --- | --- |
| `alist_exact_file_adapter.py` | 不恢复；当前 exact path/size helper 替代 |
| `burned_in_subtitle_ocr.py` | 可选阶段 9C，默认关闭 |
| `cli_args.py` | 永久删除 |
| `formal_library_remediation.py` | 主线永久删除；可选阶段 9D 只做轻量 action list |
| `hybrid_remote_transaction.py` | 永久删除 |
| `local_upload_transaction.py` | 永久删除 |
| `remote_delete_transaction.py` | 永久删除，改用删除白名单和归属检查 |
| `remote_file_transaction.py` | 永久删除 |
| `one_time_library_completion.py` | 永久删除，当前 audit/root task 替代 |
| `one_time_movie_member_scope.py` | 不恢复模块；必要的 exact-scope 行为迁入当前 planner/audit |
| `one_time_tmdb_snapshot.py` | 永久删除 |
| `one_time_tv_exclusion_scope.py` | 不恢复模块；嵌套作品排除规则迁入 scoped audit |
| `quark_fast_save_bridge.py` | 可选阶段 9A |
| `quark_native_helper.py` | 可选阶段 9B，默认关闭 |
| `subtitle_content_witness.py` | 永久删除 |
| `subtitle_member_acquisition.py` | 不恢复；当前 replenishment 替代，只迁移必要内容校验 |
| `subtitle_source_discovery.py` | 不恢复独立 runtime；Provider 搜索留在当前主链 |

### 10.2 Engine tools

| 删除文件组 | 处置 |
| --- | --- |
| `extract_archives.py` | 阶段 3 提取安全内核，旧工具本体不恢复 |
| `replenishment_local_adapter.py` | Torrent 已有替代；迁移 SFX/归档小内核，旧单体不恢复 |
| `replenishment_http_adapter.py` | 永久删除，直到真实第二 Provider 出现 |
| `subtitle_executor.py` | 当前 sidecar writer 替代；迁移内容校验和 no-overwrite |
| `audit_live_library.py`、`build_library_audit_report.py` | 当前 simple audit 替代；迁移残留可见性 |
| `refine_subtitle_audit.py` | 只迁移有限文本语言分类 |
| `probe_burned_in_subtitles.py` | 可选阶段 9C |
| `quark_native_helper.py` | 可选阶段 9B |
| `audit_one_time_title_batch.py` | 永久删除 |
| `plan_one_time_library_completion.py` | 永久删除 |
| `plan_one_time_movie_scope_worklist.py` | 永久删除 |
| `plan_one_time_tv_exclusion_worklist.py` | 永久删除 |
| `prepare_one_time_title_import.py` | 永久删除 |
| `prepare_one_time_title_imports_batch.py` | 永久删除 |
| `prepare_one_time_tmdb_movie_snapshots.py` | 永久删除 |
| `run_one_time_title_audits.py` | 永久删除 |

### 10.3 Local API 与工具

| 删除文件 | 处置 |
| --- | --- |
| `local/server.py` | 永久由 `local.simple_server` 替代 |
| `config.py` | 不恢复旧模块；迁移少量 essential env/default 逻辑 |
| `contracts.py` | 不恢复旧 transition graph；使用当前轻量 phase 契约 |
| `formal_library_maintenance.py` | 永久删除 |
| `legacy_one_time_migration.py` | 永久删除 |
| `lifecycle.py` | 迁移小型 fail-closed control-state 内核 |
| `models.py` | 当前轻量模型替代 |
| `ordinary_completion.py` | 不恢复 evidence 工件；迁移 scoped audit 思想 |
| `scheduler.py` | 当前 executor/单 writer 替代，不恢复资源图 |
| `subtitle_source_discovery.py` | 当前 replenishment 替代 |
| `title_closure.py`、`title_closure_runtime.py` | 不恢复 digest/closure；迁移作品范围和嵌套排除 |
| `validation.py` | 迁移路径、overlap、脱敏；删除 digest 部分 |
| `local/tools/formal_library_maintenance.py` | 永久删除 |
| `local/tools/retire_legacy_one_time_owners.py` | 永久删除 |
| `local/tools/__init__.py` | 空包标记，无能力需要迁移；随旧 tools 包删除 |

### 10.4 Contracts、Web、scripts 与 docs

| 删除文件 | 处置 |
| --- | --- |
| `contracts/job-phases.json` | 以当前 phase 集合恢复一个小型契约或等价单一数据源 |
| `contracts/quark-magnet-candidate.schema.json` | 主线永久删除；Quark 恢复时使用小型 runtime validator |
| `contracts/quark-share-candidate.schema.json` | 主线永久删除；Quark 恢复时使用小型 runtime validator |
| `app/components/review-dialog.tsx` | 永久删除 approval UI；失败修正入口另行实现 |
| `scripts/backup_host_state.py` | 旧脚本永久删除；默认手工停机复制，可选阶段 9E |
| `scripts/com.scrapeflow.host-state-backup.plist.example` | 永久删除 |
| `scripts/migrate_job_state_v2.py` | 永久删除 |
| `scripts/run-unittest-json.py` | 永久删除 |
| `tests/rendered-html.test.mjs` | 永久删除 |
| `docs/current-title-closure.md` | 不恢复旧文；scoped audit 规则写入当前架构文档 |
| `docs/host-state-backup.md` | 不恢复旧企业式文档；换成简短本地备份说明 |
| `engine/requirements-ocr.txt` | 默认删除；只有阶段 9C 启用时恢复独立可选依赖 |

### 10.5 被删除测试的处理原则

- 不按文件恢复旧测试目录。
- 从旧 `test_scraper.py`、archive、subtitle、server 和 adapter 测试中挑选能固定当前业务边界的案例。
- transaction、receipt、approval、one-time migration、backup daemon 和旧 UI 测试永久退出。
- `canonical_work_tree`、`residual_policy`、`replenishment_acquisition` 等仍在活动代码中的核心行为，应恢复少量直接测试，不能因为模块文件没删就连测试一起失去。
- Engine 测试可以少，但不能继续为零。

| 旧测试组 | 处理方式 |
| --- | --- |
| `test_extract_archives_transactions.py` 和 `test_scraper.py` 中的 archive cases | 只迁移魔数、密码、分卷、路径/链接、预算、冲突和输出验证案例；删除 transaction receipt 断言 |
| `test_replenishment_local_adapter.py` | 保留当前 Torrent 候选、精确集数映射、SFX 进入共享归档模块和 staging 归属案例 |
| `test_refine_subtitle_audit.py`、`test_subtitle_*` | 只迁移文本解码、语言分类、exact-video 绑定和 no-overwrite 案例 |
| `test_server.py` | 只迁移 pause、shutdown、same-origin、problem-file、cleanup allowlist 和 terminal cleanup 案例 |
| `test_canonical_work_tree.py`、`test_residual_policy.py`、`test_replenishment_acquisition.py` | 为仍在活动的业务规则恢复少量直接测试 |
| `test_*transaction.py` | 永久删除，不迁移 SHA、receipt、rollback 和 exactly-once 断言 |
| `test_one_time_*`、migration、formal maintenance | 永久删除 |
| `test_backup_host_state.py`、`test_migrate_job_state_v2.py` | 永久删除；若以后建立短小备份脚本，再为新脚本单独写最小测试 |
| `rendered-html.test.mjs` | 永久删除；TypeScript 与 production build 足够 |

---

本计划的核心判断是：

> 单用户本地项目不需要企业级事务证明，但仍需要保守的路径、归档、删除、字幕和控制边界。旧代码应该按能力裁剪复用，而不是整批恢复，也不能因为删除了旧入口就把仍需要的能力一起丢掉。
