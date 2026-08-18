# ScrapeFlow 产品与工程合同

## 0. 权威声明

本文件是 ScrapeFlow 唯一的长期产品与工程合同。层级如下:

- 本文件 = 目标行为 + 工程边界 + 领域模型 + Agent 纪律,冲突时的最高权威。
- `README.md` = 操作入口(部署、配置、API、检查命令)。
- `docs/` 下全部文档为历史参考,不覆盖本文件,不授权解除全局暂停或开放自动执行。
- 代码与测试是实现证据;与本文件冲突时以本文件为准,并明确报告冲突。
- 用户最新明确指令与本文件冲突时,报告冲突,不得擅自发明新架构。

### 实现状态说明 (Phase 0 冻结基线，2026-08-16)

目标领域模型（`IntakeSource → RootJob → WorkUnit`）已实现并接入入站链路（见下方迁移状态）。
既有 legacy execution core 保留为**内部单作品执行与回读载体**（`EngineJob`），继续冻结。禁止在过渡期内执行以下操作：

- **禁止**继续往 `EngineJob.summary` 新增业务语义字段；
- **禁止**新增 release evidence 包装、provider gate 层级、admission barrier 规则；
- **禁止**创建第二套 writer 或第二套 TMDB 匹配器；
- **必须**通过 `IntakeSource → RootJob → WorkUnit` 路径新增功能，而不是扩展 `EngineJob` phase 字段。

测试基线（2026-08-17 实测，三阶重构后）：**909 tests passed, 296 subtests passed, 0 failed**（`python3 -m pytest local/tests/ -q`，约 68 秒）。
任何代码变更不得使通过数减少；2026-08-17 用户指令删除 quark_magnet 阶。其后曾短暂采用 AList 离线下载作为中间阶；2026-08-18 用户再次裁决撤除该阶，相关运行配置、预检与测试必须同步清退。

迁移状态：目标架构迁移 **P0–P10 已完成**（2026-08-16）：

- A/S：待刮削只读发现（零 job 创建）、Web 创建任务（来源+货架，`POST /api/root-jobs`）；
- B/W/C/U：快照与边界分析（`root_boundaries`）、逐单元身份与 durable override（`unit_identity`，确认端点 `/api/jobs/:id/work-units/:unit/confirm`）；
- D：三库 `LibraryIndex` 五分类对账（跨货架查重，`library_index`）；
- F/G/H：单元驱动规划 + 唯一写器 + `WorkAcceptanceResult`（`unit_execution`，EngineJob 为内部载体）；
- J/N：Gap 账本绑 `work_unit_id`（`gap_ledger`）；字幕独立渠道默认关闭且缺陷已修；
- R：根任务聚合（`root_aggregation`，`GET /api/jobs/:id/work-units`）；
- P11：实机运行回路已切换（2026-08-16，`root_pipeline`）：intake 目录绑定的 RootJob 由调度器分派到权威单元管线（B/W 快照边界 → C/U 逐单元身份 → D 三库对账 → `execute_new_work_units` 驱动 F/G/H/J → R 聚合落 durable phase）；legacy 自动链保留为存量记录读取，不再接管新路径任务；身份/对账不确定单元保持 fail-closed 挂起，人工确认后重新分派（`/confirm` 触发）；
- P12：单元级 E1/E2/E3 通道已接入（2026-08-16，`unit_e_lanes`）：duplicate_complete 单元整边界移动至任务专属 `ScrapeFlow/归档/<root>/processed`（不写正式库，写后精确回读）；existing_gap 单元把 D 判定的未覆盖缺口坐标登记进 Gap 账本（绑 work_unit_id），空边界目录 hold 至 `existing-gap-hold`，非空来源保留并置 attention；merge_existing 单元以既有作品根为锁定目标走唯一 Planner/写器（目标根与身份双重校验、不覆盖）并记 `WorkAcceptanceResult` 语义；全部 lane 状态持久化在 WorkUnit 账本（`lane_status`），不动 `EngineJob.summary`；管线已知缺口坐标来自全部 Gap 账本聚合（`compute_known_gap_tokens`），单元载体标 `internal_child` 不出现为第二公开任务；多季绝对集数块（如 `[01]..[47]` 对 S3+S4）由管线从 B 快照+TMDB 官方季结构推导显式集号映射文件（`episode_map_<unit>.json`），仅内部请求携带路径，HTTP 载荷不接受该字段；根任务 completed 后清理本任务 intake 源树中经 fresh listing 验证为空的目录壳（`_cleanup_empty_source_shells`：目录绑定门禁、只删真空目录、pause 感知、失败不回滚完成态）；入站监视器在每次成功 fresh listing 后把已消失的 intake 目录条目置 `present=False`（不删记录、保留历史与 root_task_id 绑定，同路径重建自动复活）；
- P14：新路径补源接线（2026-08-16，`root_replenishment` + `replenishment_bridge`）：Gap 账本 → 运行时 request 形状（桥接，每 `(media_type,tmdb_id)` 一个 request）→ 严格两阶 lane（durable tier 状态文件 `replenishment_<root>.json`，进阶由 `replenishment_tiers.apply_tier_outcome` 驱动，candidate/infrastructure/in_doubt 三分类：infrastructure 原阶 retry_wait、in_doubt waiting_reconcile 且绝不重复提交同一坐标）→ 任务专属 staging → 同一 Planner/写器（internal child）→ 按执行计划文件证明缺口坐标已覆盖后才 `close_gap`，每次提交前先 `record_attempt`；`missing_subtitle` 缺口留在独立字幕渠道、不进视频补源 lane；L 步门禁复用 admission token（用户裁决 A）；服务端自动派发（管线完成后、门禁重开 sweep 覆盖 waiting 与未排 root、两阶耗尽后仅人工触发）+ `GET /api/jobs/:id/replenishment` 只读预览 + `POST /api/jobs/:id/replenish` 手动触发；
- P15（历史迁移与现行裁决）：`quark_magnet` 阶被夸克账号级突发限流卡死，已删除。2026-08-18 用户又要求删除曾作为中间阶的 `alist_offline`：AList v3 不暴露 aria2 `select-file`，且提交时会重新获取 URL，无法证明它仍是已核验的精确成员清单。当前视频补源固定为 **`quark_share → magnet`**。本地 Torrent 只允许已证明逐缺口映射的 `--select-file` 成员；整季包、花絮、压缩包或映射不完整的候选在下载前拒绝。AList 保留为媒体库与夸克存储访问，不再提交、预检或声明离线下载。此前部署已核验为空，旧本地任务记录已移除；ScrapeFlow 不保留 AList 离线任务 API、恢复或清理路径。未来人工审计若发现外部遗留离线任务，必须先在升级前人工取消并移除。下载仍走直连纪律（本地 Torrent 使用 aria2，搜索层才允许代理）；
- J 步正确性修正（2026-08-16，实机核验发现）：`_register_unit_episode_gaps` 只登记本单元自有季（执行计划视频 token + 裸季目录行 + durable identity 季），整目录移动计划回退 B 快照取实际覆盖，无法证明覆盖的季 fail-closed 不登记；`discover_episode_gaps` 按 (media_type,tmdb_id,season,episode) 跨单元去重；`gap_reaudit.reaudit_open_gaps` 对库内 fresh listing 证明已存在的缺口坐标做定向核销（`run_root_replenishment` 每次触发前先跑，幻影缺口绝不进入获取 lane）；
- 合规：作品名硬编码全部数据化（`engine/scrapeflow/data/release_lexicon.py`）、货架-媒体类型矩阵已删除、TMDB 匹配器统一为单一评分核心；
- 测试：`tests/corpus/` 10 场景 + 真实 A→B→W→C→D 链路回归。
- 存量清退（裁决②，渐进）：`target_work_path` 的新流程写入已移除（公开投影改由 plan/summary `target_root` 派生，legacy 合并交接读取保留）；创建期的 `reconciliation{status:blocked_by_target_shelf}` 标记已移除（等待任务即刻展示三货架枚举，符合 S 步）；`TargetShelfPolicyConflictError` 与其处理器已删除（矩阵删除后无生产 raise 位点，phase 值保留供 legacy 记录读取，身份媒体类型直接驱动规划）；死代码 `pre_reconciliation`、`_automatic_job_needs_dispatch` 已删；`automatic_stage` 的 engine-runner 侧状态镜像写入已全部移除（`phase`/`replenishment.status` 为唯一权威，执行链路接管记录时丢弃旧记录残留镜像值，测试夹具保留该键以模拟存量旧记录）；server 侧补源/重试 lane 的清退进展（2026-08-16 批次 5+6）：replenishment 防重写镜像对已清退（`next_core == current_core` 即 durable 等价）；五处写-only `automatic_stage` 镜像（`_defer_provider_dispatch`/E2 hold 失败/`_reconcile_paused_provider_root`/`_queue_provider_job`/`_record_replenishment_progress`）已清退（各自与同块 `replenishment.status` 或 E2 durable 标记同值）；身份修正门（`retry_public_job` 读 + `_record_automatic_retry` 写）经评估保留——现有 stock 字段无干净等价（`identity_attempts` 只增不清，是"历史曾有 identity 失败"的严格超集，会错误放行已越过 identity 的 write/planning 失败任务）；其余存量字段与 provider gate 机制保持冻结，继续按运行节奏清退。**provider gate 清退裁决（2026-08-16，用户选 A）**：L 步门禁语义本身保留（新路径继续复用 admission token：入站清空+全库只读审计完成才放行，新入站即撤销），只等 legacy lane 存量耗尽后渐进清退其派发机器（`_provider_futures`/`_provider_submission_grants`/`_provider_admission_epoch`/pilot 选择器）；不得以清退名义改变门禁行为。

## 1. 产品边界与核心领域模型

ScrapeFlow 是单用户、单机、Docker Compose 部署的 AList 影视库管理服务:一个 AList、一个 API 应用、一个正式库 writer、轻量本地 JSON 状态。它不是多用户、分布式、企业级或公有云平台。

### 核心领域主语层次

系统必须遵循以下第一等领域主语,禁止将所有职责压缩入单一扁平对象:

```text
IntakeSource (只读发现)
  └── RootJob (用户授权的唯一根任务)
        └── WorkUnit (独立作品单元)
              ├── ConfirmedIdentity (TMDB 身份)
              ├── ReconciliationDecision (三库五分类对账结果)
              ├── SourceObjectRef[] (来源文件所有权清单)
              ├── WorkPlan / WriterRun (作品级执行计划)
              └── Gap[] (缺口账本)
                    └── AcquisitionAttempt[] (补源尝试)
```

- **`IntakeSource`**: `/待刮削` 下直接子目录的只读发现记录。发现本身**不是执行任务**，不拥有执行 phase。
- **`RootJob`**: 用户授权选择一级货架后创建/激活的唯一根任务。同一来源路径永远映射到唯一 `RootJob`。
- **`WorkUnit`**: 根任务通过目录结构分析拆分出的独立作品。一个 Fate 容器根任务可包含多个 `WorkUnit`。
- **`MediaItem` / `SourceObjectRef`**: 具体媒体文件及其归属关系，禁止跨任务/跨单元重叠所有权。
- **`Gap`**: 属于特定作品和具体季集坐标的缺口。
- **`AcquisitionAttempt`**: 针对缺口的单次补源尝试。
- **既有 `EngineJob`**: 属于 legacy execution core，退化为内部单作品执行与回读载体，不再充当顶层业务实体。

优先级顺序:
1. 正确的媒体行为;
2. 绝不误写/误删正式库;
3. 目录结构理解与作品边界发现发生在身份对账之前;
4. 每个作品独立识别与对账，单个子项 uncertain 不阻塞兄弟作品;
5. 复用既有成熟单写器、回读、审计与补源 staging 安全代码;
6. 简单控制流 + 人工可恢复;
7. 最后才是抽象与扩展性。

## 2. 唯一主流程 (A→P 产品合同)

下图是 ScrapeFlow 的主流程架构。所有实现、测试和验收均需对齐此链路:

```mermaid
flowchart TD
    A["A 待刮削目录只读发现<br/>只登记直接子目录 IntakeSource"] -->
    S["S 用户选择一级货架并授权<br/>电影 / 番剧 / 美剧<br/>创建或激活唯一 RootJob"] -->
    B["B 来源快照与目录结构分析<br/>SourceSnapshot + BoundaryAnalysis<br/>识别单作品 / 系列容器 / 电影合集 / 季度"] -->
    W["W 拆分独立作品单元<br/>生成 WorkUnit 集合"]

    W --> C["C 逐作品 TMDB 身份识别<br/>IdentityResolver + IdentityEvidence<br/>综合目录/文件/父容器/季集/年份评分"]

    C -->|置信度不足/歧义| U["U uncertain → 单单元挂起<br/>仅阻塞当前 WorkUnit<br/>其余已确认 WorkUnit 继续推进"]
    U -->|用户确认 tmdb_id+type| C

    C -->|已确认身份| D["D 逐作品三库独立对账<br/>同时查询电影/番剧/美剧 LibraryIndex<br/>输出五分类判定"]

    D --> D1{"D1 五分类判定"}
    D1 -->|duplicate_complete| E1["E1 重复消费<br/>move 至任务专属归档，不写正式库"]
    D1 -->|existing_gap| E2["E2 登记既有作品缺口<br/>继承既有 work root，等待补源"]
    D1 -->|merge_existing| E3["E3 归并入库<br/>锁定既有 shelf 与 work root"]
    D1 -->|new_work| E4["E4 新作品入库<br/>使用 RootJob 指定货架与规范目录"]
    D1 -->|uncertain| U2["U2 对账冲突挂起<br/>如跨库重名或证据断裂"]

    E3 --> F["F 逐作品调用现有 Planner<br/>生成独立 WorkPlan"]
    E4 --> F

    F --> G["G 单写器串行执行<br/>worker lock + 目标不覆盖 + 路径校验"]
    G --> H["H AList 精确回读与验收<br/>校验路径/字节/NFO/海报/字幕/残留<br/>生成 WorkAcceptanceResult"]

    H --> I{"I 验收存在缺口？"}
    I -->|是| J["J 登记精确 Gap<br/>绑定 work_unit_id 与季集坐标"]
    I -->|否| K["K 作品入库完成"]
    E1 --> K
    E2 --> J

    K --> R["R 根任务状态聚合<br/>completed / attention / failed / gaps 统计"]
    J --> R

    R --> L{"存在待闭环缺口<br/>且全局门禁开放？"}
    L -->|否| M["M 入库完成阶段结束 (ingest_completed)"]
    L -->|是| N["N 严格两阶补源子系统<br/>Quark 分享 → 本地 Torrent（精确 select-file）<br/>专属 staging → 同一 Engine/Writer → scoped audit 闭环"]
    N --> M
```

### 各环节核心规则

1. **A/S 只读发现与根任务授权**:
   - 扫描 `/quark/影视/待刮削` 的直接子目录，更新 `IntakeSource`；
   - 未点击创建任务前，**零整理、零对账、零 TMDB 请求、零状态机 job**；
   - 用户选择来源并指定一级货架（`movie / anime / us_tv`）后创建或恢复唯一 `RootJob`。
2. **B/W 目录结构与作品边界分析 (BoundaryAnalysis)**:
   - 必须在 TMDB 匹配之前执行；
   - 提取目录层级、直接文件与子目录比例、视频/字幕分布、季度标记、集数序列、年份、剧场版/OVA/SP、版本压制组特征；
   - 目录角色枚举包括：`single_work`, `series_container`, `season`, `movie_collection`, `special_group`, `version_group`, `release_group`, `subtitle_group`, `extras_group`, `resource_group`, `uncertain`；
   - 严禁将 Fate、高达等系列容器顶层直接认作单部 TV。
   - **系列容器落盘规则（Fate 式，用户裁决 2026-08-16）**：容器 = 待刮削里用户放入的那一层文件夹，库中只出现一个容器条目，子作品全部嵌套其下，绝不散开。子作品归属只看证据不看名字：同一 TMDB 剧的若干季合并为一个子条目；不同 TMDB 剧各成一个子条目；电影成为容器内的电影子条目。容器命名三级兜底：库里已有同身份容器 → 直接并入；否则用清洗后的用户目录名（剥压制组/分辨率/编号前缀/乱码符号，含连续数字堆或清洗后过短视为不可用）；仍不可用 → 用单元 TMDB 标题。主 TV 单元（容器内唯一 TV 身份，或某 TV 身份占多个季单元而其余 TV 身份各只有一个单元时）拥有容器根，其余单元一律以其真实落盘根为父目录（主系列在根、电影与外传嵌套其下；多 TV 且无主导/纯电影容器：每个子作品嵌套在清洗名容器下）。失败单元重试会退役陈旧终态载体并从当前来源状态重新规划（不再钉死旧计划）。身份或写目标真分不清时维持 fail-closed 挂起。小数集特别篇（`[N.5]`）匹配顺序：官方标题字面含相同小数集号 → 直接认定（跳过季时间线否决）；否则跨正季查找 N/N+1 播出日区间（含季末 N 以下一季首集为上界的跨季边界），命中且节目时长≥18 分钟即认定——这是无数据行的通用机制，覆盖 Vivy 13.5、无头骑士 13.5（含季文件夹内）等；区间证据也失败时，`release_lexicon.FRACTIONAL_SPECIAL_ALIASES` 数据行作为最后兜底（如刀剑神域 24.5/36.5）。补源下载走直连（清空下载子进程的代理环境并开 DHT/PEX/LPD），搜索层才允许使用代理。
3. **C/U 作品级身份识别与证据评分**:
   - 匹配器接收结构化 `IdentityEvidence`（边界名、代表文件名、父容器弱证据、年份、媒体形态）；
   - 电影与剧集候选并行评分，记录打分明细与 margin；
   - 歧义项置为 `uncertain`，仅挂起当前 `WorkUnit`，**不阻塞兄弟作品单元推进**；
   - 人工确认持久化为 durable override，仅确认 `media_type + tmdb_id`（及必要季号），不暴露内部 JSON 或任意目标路径。
4. **D 逐作品三库独立对账 (LibraryIndex)**:
   - 同时在电影、番剧、美剧三个正式库中建立的 `LibraryIndex` 中查重；
   - 判定优先级：`uncertain`（冲突/断裂） $\rightarrow$ `merge_existing`（有可归并新媒体） $\rightarrow$ `existing_gap`（既有作品缺口） $\rightarrow$ `duplicate_complete`（完全重复） $\rightarrow$ `new_work`（全新作品）；
   - `merge_existing` 必须严格锁定既有作品的 shelf 与 `work_root`，禁止覆盖。
5. **G/H 统一单写器与精确回读验收**:
   - 所有正式库写入必须经过唯一写器（单写锁、不覆盖、路径防穿越）；
   - 写入后执行 fresh listing + 精确回读（路径、类型、字节数、NFO、海报、字幕证据）；
   - 生成结构化 `WorkAcceptanceResult`。
6. **J/N 补源作为缺口闭环子系统**:
   - Gap 精确绑定到 `work_unit_id` 与坐标；
   - 补源获取的媒体进入任务专属 staging，生成临时 inventory 重新经过统一 Engine 与 writer；
   - 经定向审计证明缺口真实消失后才核销对应 Gap 并清理 staging。

## 3. 必须保留的本地安全底线

- **源/staging/正式库路径绝对隔离与不重叠检查**；
- **目标已存在即停，默认绝不覆盖**；
- **归档路径防穿越与软硬链接拒绝**；
- **最小视频字节准入 (默认 1 MiB, 硬下限 64 KiB) + ffprobe 媒体流有效性检查**；
- **写后精确回读 (fresh listing + 字节/类型严格核对)**；
- **只清理当前任务所属范围，不触碰公共容器或正式库**；
- **任务所有权隔离 (两个任务绝不重叠 claim 同一来源对象)**；
- **敏感信息脱敏 (密码、token、cookie 绝不落盘与入日志)**。

## 4. 补源两阶纪律与 Staging 隔离

- 层级顺序固定：`quark_share → magnet(本地 Torrent)`，不得跳级；`magnet` 只下载已证明映射到当前 Gap 的 `--select-file` 成员；
- 失败三分类：`candidate`（资源问题，排除）、`infrastructure`（网络/认证/本地下载器故障，原阶 `retry_wait`，绝不降阶）、`in_doubt`（可能已提交，`waiting_reconcile`，绝不重复提交）；
- Provider 永不直接写正式库，永不决定正式库路径；
- 补源产物落入任务专属 staging，通过同一 Engine 和单写器正式入库。
- 字幕缺口走独立字幕渠道（字幕站点直搜；混合候选中只提取字幕成员，不下载整集视频），
  不占用视频补源带宽；该渠道同样遵守 staging 隔离、失败三分类（candidate/infrastructure/in_doubt）
  与同一 writer 闭环（2026-08-16 用户裁决）。

## 5. 暂停与可恢复性

- API 进程启动默认 paused，需用户显式 resume；
- 暂停边界为下一个外部副作用（move/upload/delete/submit/write）之前；
- 任务重启后凭持久化意图记录与远端 fresh 状态安全恢复，不重复执行已完成写入。

## 6. 不得引入的过度复杂度

- 不引入多服务/微服务架构；
- 不引入消息队列（RabbitMQ / Kafka / Redis）；
- 不引入外部数据库平台（MySQL / PostgreSQL / MongoDB），保持轻量原子本地 JSON；
- 不做应用级全量大文件 SHA-256 哈希（使用路径、类型、大小、版本四元组）；
- 不做第二套 writer 或第二套 TMDB 匹配器。

## 7. 测试与回归纪律

- 单元测试与真实案例回归是系统安全护栏；
- 建立 `tests/corpus/` 真实用例库，覆盖普通电影、多季美剧、绝对集数动画、OVA、电影合集、系列容器（Fate、高达、物语）等场景；
- 真实案例测试必须走完整真实链路，严禁在 fixture 中硬编码绕过 Engine；
- 严禁在业务代码中写入特定作品名称硬编码（如 `if "Fate" in name`）。

## 8. 端口与部署配置

- **容器内部端口**：API 服务监听 `8765`（固定，不可更改）；
- **宿主暴露端口**：默认 `3010`，由环境变量 `SCRAPEFLOW_API_PORT` 控制；
- **Compose 映射**：`127.0.0.1:${SCRAPEFLOW_API_PORT:-3010}:8765`；
- **测试 fake payload**：compose_evidence 测试中的 fake runner 须反映真实映射
  （`published: "3010"`, `target: 8765`），而非旧的 `published: "8765"` 形式；
- **Quark Helper**：宿主侧 `18765`（仅内部通信，不对外暴露）。
