# ScrapeFlow 目标货架启动门收敛计划

> 状态：当前唯一产品合同；工作树仍是 WIP，尚未发布。
>
> 日期：2026-08-09
>
> 适用范围：单用户、单机、单实例、单 AList 媒体库。
>
> 运行约束：保持全局暂停；Provider 自动补源和自动审计默认关闭；本计划不授权操作真实媒体、AList、旧任务或 staging。

本文取代此前普通入站的自动启动描述。历史收敛计划、发布审计、运行态审计和旧 WIP 分区只保留为证据或参考，不能声明当前 WIP 已完成，也不能改变本文件规定的流程。

## 1. 冻结产品路线

以下规则是实施合同，不得自行改变。

~~~text
/待刮削发现来源
  → awaiting_target_shelf
  → 用户选择 movie / anime / us_tv
  → queued
  → 归档或媒体预处理
  → TMDB 身份识别
  → 类型与货架兼容检查
  → 季集和作品目录规划
  → problem-file gate
  → 唯一正式库 writer
  → AList 精确路径、类型、大小回读
  → NFO、海报、字幕
  → 当前作品定向审计
  → Provider 明确完成、跳过或失败
  → source 与 staging 最终处理
  → completed / completed_with_gaps / failed
~~~

- /待刮削只负责发现并登记直接子目录。登记新任务时不得调用归档检查或解压、TMDB、planner、writer、audit 或 Provider。
- 新任务最小持久状态为 phase=awaiting_target_shelf、ingress_source_path、target_shelf=null、target_root=null、selected_at=null。
- 用户只能选择三个稳定枚举；唯一后端映射为 movie → /电影、anime → /番剧、us_tv → /美剧。Web、Server 和 Provider 不得自行维护另一张映射表，也不接受任意 target_parent。
- 用户调用 POST /api/jobs/:id/start 后，任务才可以排队并进入正式链路。Engine 可以决定作品和季目录，但不得覆盖用户已选的一级货架。
- 类型兼容矩阵是保守的：movie 仅允许 movie；anime 和 us_tv 仅允许 tv；unknown 一律停止。冲突进入 target_policy_conflict，保留来源，等待用户通过 /start 显式重选。
- source 与 staging 只能在正式写入、回读、元数据、定向审计及 Provider 终态之后处理。失败和取消保留 source。

## 2. 固定简化边界

必须保留：

- 基本路径规范化、越界检查、目标已存在即停止。
- 归档路径穿越、符号链接、成员数量和展开大小限制。
- 一个正式媒体库 writer，及 AList 精确路径、对象类型、文件大小回读。
- 视频最小大小与 ffprobe 检查。
- 失败与取消时的来源保留；Provider/audit 默认关闭且有界。

明确不做：

- 媒体 SHA-256、文件哈希清单、plan digest、approval receipt、nonce、epoch 或事务日志。
- 多实例、分布式锁、lease、exactly-once 证明、跨进程 ownership registry。
- 第二套 AList 客户端、归档检查器、解压器或通用 Provider 插件框架。
- Cloud share、Provider SFX、OCR、自动整改旧媒体库、自动迁移旧任务。
- 为 hidden test 建通用反射兼容层；Git 是普通回退点，不是内容哈希验收系统。

## 3. 当前 WIP 清单与阶段边界

所有现有改动保留，不 reset、不还原。下表是本轮唯一的阶段归属；带星号的热点只可在所属阶段以最小变更推进。

| 阶段 | 当前文件 | 责任边界 |
| --- | --- | --- |
| 0 合同与文档 | ARCHITECTURE.md、README.md、engine/README.md、docs/scrapeflow-convergence-decisions.md、docs/scrapeflow-convergence-release-audit.md、docs/scrapeflow-convergence-wip-partition.md、docs/scrapeflow-phase-checkpoints.md、docs/scrapeflow-runtime-audit-2026-08-09.md、docs/scrapeflow-phase-0-baseline.md、docs/scrapeflow-phase-1-safety.md、docs/scrapeflow-phase-2-domain.md、docs/scrapeflow-phase-3-archive.md、docs/scrapeflow-single-user-local-convergence-plan.md、docs/scrapeflow-target-shelf-start-gate-plan.md、已删除的 docs/scrapeflow-unified-workflow-remediation-plan.md | 产品说明、历史标识与唯一合同 |
| 1 启动门 | engine/scrapeflow/target_shelf.py、local/tests/test_target_shelf_start_gate.py、local/simple_server.py*、local/scrapeflow_api/simple_engine_runner.py* | 枚举、登记、选择和 /start |
| 2 已选主链 | local/simple_server.py*、local/scrapeflow_api/simple_engine_runner.py*、local/tests/test_simple_engine_runner.py* | 预处理→身份→规划及货架约束 |
| 3 恢复与取消 | local/simple_server.py*、local/scrapeflow_api/simple_engine_runner.py*、local/tests/test_simple_server.py*、local/tests/test_simple_engine_runner.py* | restart、retry、cancel、legacy 边界 |
| 4 最终生命周期 | local/scrapeflow_api/automatic_replenishment.py、local/simple_server.py*、local/scrapeflow_api/simple_engine_runner.py*、local/tests/test_phase4_golden_path.py、local/tests/test_simple_engine_runner.py* | source、staging、audit、Provider 收尾 |
| 5 Web | app/core/api-client.ts、app/core/contracts.ts、app/core/job-state.ts、app/globals.css，以及现有 TaskRow、TaskExpansion、hooks、ScrapeFlowApp | 最小选择、重选、取消和轮询闭环 |
| 6 RC 验证 | docs/scrapeflow-target-shelf-rc-audit-2026-08-09.md、各阶段最小回归测试、scripts/check.mjs、scripts/web-contract-check.mjs 及全量检查 | 不额外引入框架 |
| 旧状态治理 | docs/scrapeflow-state-ownership-audit-2026-08-09.md | 只读报告；不迁移、不清理、不混入功能提交 |

已删除的 docs/scrapeflow-unified-workflow-remediation-plan.md 是旧自动入口规格；不得恢复为当前产品来源。

## 4. 分阶段实施与验收

### 阶段 0：保护 WIP、统一合同

允许改动文档，不改业务逻辑、测试、Docker、运行态或真实媒体。

- 把本文作为唯一当前流程；README、架构和 Engine README 仅保留产品说明和链接。
- 把旧 release audit、runtime audit、checkpoint 与旧 WIP 分区明确标记为历史快照。
- 旧状态报告不混入功能提交；旁路 AList 认证和归档名称实验只作参考。

验收：所有 dirty/untracked 文件均能按上表解释；没有第二个当前流程；不把 WIP 表述为已发布版本。

### 阶段 1：发现、登记、选择、启动

只补齐启动门，不改归档主链、Provider、cleanup 或 Web 大改。

- TargetShelf 是 movie、anime、us_tv 的明确枚举；TypeScript 使用同样的联合类型。
- POST /jobs 与入站扫描只验证 /待刮削直接子目录、存在性和规范化路径去重，并持久化 awaiting_target_shelf。
- POST /api/jobs/:id/start 只接受 target_shelf；后端映射根，原子保存选择并转 queued。
- paused 时选择必须持久化但不提交 worker；相同 /start 幂等，不同货架在已运行任务上拒绝；target_policy_conflict 允许显式重选。

验收：选择前 archive/TMDB/planner/writer 调用均为零；重复发现不建第二任务；非法显示名、路径、collection 和 unknown 均拒绝；waiting restart 后仍 waiting。

### 阶段 2：选择后接回既有主链

只复用现有 ArchivePreprocessingAdapter、identity matcher、planner、plan validator、single writer 与 readback。

- worker 开始前必须验证 target_shelf、target_root、selected_at 和可调度 phase；缺一即 fail-closed。
- 顺序固定为 archive preprocessing → identity matching → planning。预处理失败不得调用 TMDB、planner 或 writer。
- 普通视频、ZIP、7z、RAR 和伪装归档共用同一预处理入口与 staging 规则。
- identity 的 media_type 必须通过兼容矩阵；规划的 target_work_path 必须包含在已确认 target_root 内。
- 只补现有能力的运行缺口，例如首次归档远端操作前的既有 AList 认证，以及归档后缀去除；不新增客户端或标题解析框架。

验收：movie→movie 成功；movie→tv 冲突且不写入；anime/us_tv→tv 成功；重选后成功；target 已存在和 problem files 阻断正式写入。

### 阶段 3：恢复、重试、取消与 legacy

- waiting 重启后仍等待；cancel 立即 cancelled 且不移动 source；retry 明确提示使用 /start。
- queued-but-paused 保留选择，重启后仍受 pause 阻挡，可取消。
- processing 复用现有安全边界取消，不建设抢占系统；后续清理不得在取消后运行。
- failed_identity 与 failed_planning 的 retry 继承原 shelf；身份修正不得提供 target_parent，结果仍须经过兼容检查。
- target_policy_conflict 只能 /start 重选，failed_cleanup 的 retry 只执行 cleanup。
- 不完整 target_shelf 的 legacy job 不自动排队、不自动推导回写、不允许 retry 伪装为 queued。

验收：waiting/paused restart、queued cancel、processing cancel、冲突重选、legacy retry 拒绝、cleanup-only retry 不调用 writer，以及 cancelled child 不计 active。

### 阶段 4：source、staging、audit 与 Provider 收尾

- /ScrapeFlow/归档/<job>/archive 是可删除的临时 archive staging。
- /ScrapeFlow/归档/<job>/processed 是成功后移动的原始 source，不属于 staging，绝不因清理 archive 而删除。
- finalizer 只负责读取最终结果、决定是否消费 source、删除 archive staging、保存 source_fate/staging_fate、更新最终 phase；不扩展事务框架。
- audit 只扫描当前作品；unknown 不生成 Provider gap，也不触发全库整改。
- Provider 默认关闭；关闭时记录 skipped/deferred 后允许最终 cleanup。开启时 child 继承 root 的 target_shelf 和 target_root，仍经同一 planner/writer，达到重试上限即停止。

验收：普通视频、ZIP、7z、RAR、伪装归档与密码错误均符合 source fate；成功后只删 archive、保留 processed；cleanup 不删父目录；cleanup retry 不重跑 writer；重扫不重建；Provider disabled 仍可收口。

### 阶段 5：最小 Web 日常闭环

复用现有 API client、contracts、job-state、TaskRow、TaskExpansion、use-scrapeflow、use-dashboard-controller 和 ScrapeFlowApp。

- waiting 任务必须在列表可见、可展开，并提供电影、番剧、美剧三个固定按钮。
- 点击调用 scrapeFlowApi.start；请求中禁用按钮。任务创建后保持可见，不能切到隐藏 waiting 的 running filter。
- target_policy_conflict 展示原选择、冲突原因和重选按钮；waiting 与 queued 均提供取消。
- 有 waiting 时保留低频任务轮询；没有 active job 时也不得完全停止刷新。
- 删除 retry UI 的 collection；TMDB ID 空时不自动传 movie；Web 不接收、拼接或展示可编辑 target_root。

验收：新登记任务无需刷新即可可见；三个枚举正确；重复点击不会二次启动；paused 下显示“已选择、等待恢复”；TypeScript、ESLint 与 production build 通过。

### 阶段 6：本地 RC

按领域、Server/API、Runner、生命周期和 Web 分层复核。完整门禁必须同时通过：

- 所有 targeted tests 与 Python 全量测试零错误。
- npm run check、ESLint、TypeScript、Web contract check、production Web build、git diff --check 通过。
- 工作树阶段边界清楚；不做真实 AList 数据测试、媒体哈希、源码哈希或应用内 rollback。
- 发布标识只使用普通 build_version；当前本地 RC 默认值为 target-shelf-rc1。

### 阶段 7：经明确授权的隔离真实样本

只有阶段 0–6 全部通过、工作树 clean、镜像仍全局暂停后，才可开始；必须由用户再次明确授权。

使用独立状态目录和明确隔离的 AList 测试目录，不复用旧 94 个 jobs、1121 个 gaps 或旧 staging。每次只跑一个任务，顺序为普通电影、番剧归档、美剧季度目录、密码或取消负例、restart、cleanup 后重扫。Provider/audit 自动 lane 保持关闭。三个正例通过后，由用户决定是否恢复日常 intake。

## 5. 执行纪律

每阶段遵循：确认目标 → 列复用点与文件白名单 → 最小改动 → targeted tests → diff 审查 → 验收 → 独立 checkpoint。

主进程负责修改；子进程只做只读审计、测试分析或代码复核。发现需要白名单外文件时，只有当前验收必需才扩展范围；顺手优化进入 parked。

新增代码前必须能回答：它对应哪条验收、能否复用已有实现、单机是否真的需要、如何用现有测试验证、以及不做是否阻碍当前阶段。

## 6. 完成定义与 parked

“目标货架日常版本完成”意味着：入站只登记；选择前零正式副作用；三个货架可选；Web 可选择与重选；/start 幂等；pause/restart/cancel/retry 正确；用户货架不被 Engine 覆盖；普通视频和归档复用同一链；source/staging 不冲突；legacy 不形成假 queued；全量检查通过；隔离样本通过；旧 backlog 未自动恢复；Provider/audit 默认仍关闭。

以下需求固定 parked：Provider SFX、Cloud share、OCR、自动去重或整改旧媒体库、多实例、通用插件、分布式锁、媒体 SHA、全量旧状态迁移和 Web 大改版（不含阶段 5 的最小选择/重选闭环）。
