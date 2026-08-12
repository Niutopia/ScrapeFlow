# ScrapeFlow 最终收敛计划 v1（历史参考）

> 文档性质：这是 2026-08-09 的历史收敛计划。它保留当时的方案、约束和验收建议，
> 不是当前产品、工程或实现合同；它不授权后续实施或验收。
>
> 长期产品与工程合同见 [`../AGENTS.md`](../AGENTS.md)；当前 HEAD 的实现事实和已知差距见
> [`CURRENT-STATE.md`](CURRENT-STATE.md)。其中 target-shelf-first 入口仅是本历史计划的当时方案，
> 已被长期合同中的 reconciliation-first 流程取代。

项目“完成”的定义是：普通入库、归档、审计、严格三阶补源、恢复、备份和本机部署全部闭环。新的 Web 不在本轮完成门内，未来只消费稳定 API。

## 一、历史方案中的核心约束

```text
夸克分享快转 ─┐
夸克磁力离线 ─┼→ 任务专属 staging
本地 Torrent ─┘
                    ↓
              统一准入校验
                    ↓
          受限 media-only Engine child
                    ↓
               同一个单 writer
                    ↓
                 正式媒体库
                    ↓
            精确回读 → 定向重审
                    ↓
              任务 staging 清理
```

三条线路的差异只能存在于“如何把资源送进 staging”。

固定 staging：

```text
/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>
```

硬性禁止：

- 三条线路直接写 `/电影`、`/番剧`、`/美剧`。
- Helper、aria2 或 Provider 接收、计算正式库路径。
- 三条线路分别实现 TMDB、季集、命名或刮削。
- 因为资源已经在夸克云盘，就绕过 staging 和 Engine。
- 基础设施故障被解释成“资源不存在”并降到下一阶。
- 恢复通用 Provider 插件平台、HTTP Provider 平台或第四条 `cloud_share` 线路。

## 二、简单化边界

固定运行模型：

- 单用户、本机、单 API、单 AList。
- API 只绑定 `127.0.0.1`。
- Provider worker 默认一个。
- 正式媒体库始终一个 writer。
- 状态继续使用原子 JSON，不引入数据库、消息队列或状态平台。
- 不做多租户、权限角色、分布式锁、租约和多副本调度。
- 不做两阶段提交、事务回滚库、receipt、digest、nonce、epoch。
- 不做无限自动重试，环境修好后允许用户显式 retry。

复用现有工具：

- AList：远端列表、移动、上传、删除和回读。
- Engine：TMDB、季集、命名、NFO、海报和正式计划。
- 7-Zip：归档识别、列表和解压。
- aria2：Torrent 下载。
- ffprobe：视频准入。
- Python `unittest`：测试，不再引入第二套测试框架。
- Quark Helper 如需连接 CDP，使用成熟 WebSocket 库，不再手写 RFC6455 客户端。

## 三、哈希规则

最终活动代码不再进行应用级 SHA-256 校验。

当前需要删除的两处是：

1. gap JSON 的 SHA-256 fingerprint：改为规范字段排序后直接比较。
2. 针对某部 Fate 作品硬编码的 8MiB 前缀 SHA-256 witness：整个单作品特例删除，无法确定的媒体回到 `unknown`。

允许保留：

- BitTorrent 协议原生 infohash。
- Torrent metainfo。
- 7-Zip 自带 CRC。
- Git 自身对象哈希。

媒体校验统一使用：

```text
路径 + 对象类型 + 文件大小 + create-only + AList 回读 + 一次 ffprobe
```

不周期性读取整部影片，不生成媒体 SHA 清单，不用另一种摘要替代 SHA-256。

## 四、当前基线

> 历史快照说明：以下“当前基线”保留 2026-08-09 的收敛起点，不能用来描述当前代码或验收状态。当前实现已具备分享快转、Quark 磁力、严格 report-only 审计、无应用级 SHA-256 的代码路径；本地测试/发布门状态应以当前 commit 的 release evidence 为准。尚未完成的仍是独立 AList/状态/媒体根、备份恢复演练和真实隔离样本，且自动 gate 必须保持关闭。

历史基线中曾被视为可复用：

- `awaiting_target_shelf → /start` 启动门（仅为当时的 target-shelf-first 方案，非现行合同）。
- 电影、番剧、美剧固定货架。
- TMDB、季集、命名和 Engine 计划。
- ZIP、7z、RAR、伪装归档安全预处理。
- 单 writer、路径边界、目标冲突停止和 AList 回读。
- 审计、字幕判断、gap 基础结构。
- 本地 Torrent 搜索、aria2、ffprobe 和上传。
- 346 项现有后端测试。

历史基线中的明确缺口（其中已实现项见上方快照说明）：

- 补源目前只有第三阶真正可执行。
- 第一阶夸克分享快转已被裁掉。
- 第二阶夸克磁力云离线已被裁掉。
- 当前手工审计在部分情况下可能修复 NFO/海报，不是严格只读。
- 当前仍有应用 SHA-256 和单作品硬编码特例。
- 三阶失败分类、降级证据及 in-doubt 恢复需要重新固定。
- 尚未完成正式的备份恢复演练和三阶真实隔离验收。

## 五、当时的 Agent 执行纪律

当时后续 agent 使用以下约束；它们不取代当前根 `AGENTS.md`：

1. 先只读检查，列出准备修改的文件和测试。
2. 不得修改 `.env.local`、真实 AList、`/data`、正式库或旧 staging。
3. 不得恢复历史整文件或整提交，只迁移必要的小段业务规则。
4. 不得增加 SHA、事务框架、Provider 注册表或分布式设计。
5. 不得扩大当前阶段范围。
6. 不得在同一阶段并行编辑相同文件。
7. 每个 agent 必须交回：
   - 修改文件列表；
   - 行为变化；
   - 删除的复杂度；
   - 执行的测试；
   - 未解决问题。
8. 主 agent 逐项审查 diff；违反当时约束的修改直接丢弃。
9. 每个阶段单独提交，失败可以独立回退。
10. 只有主 agent 可以决定阶段完成；最终完成只由你验收。

已经明确拒绝的错误建议包括：基础设施故障后继续降阶。正确规则是基础设施故障停留原阶。

## 六、分阶段执行方案

### 阶段 0：建立干净基线

执行：

- 将当前授权范围的清理与原有归档 staging 修复分成独立提交。
- 保存当前测试结果和工作树清单。
- 记录一份当时的最终收敛计划。
- 旧 WIP 文档退出当时的活动计划，只保留在 Git 历史。
- 保持全局暂停，所有自动 gate 关闭。

退出条件：

- 工作树干净。
- 每个改动来源清楚。
- 后端全量测试通过。
- 没有触碰现有运行实例或持久数据。

### 阶段 1：删除不符合边界的复杂度

执行：

- 删除 Fate 单作品 `content_identity_overrides` 模块、审计接线和专项测试。
- 删除应用级 gap SHA-256。
- gap 改为直接比较以下规范字段：

```text
kind
tmdb_id
media_type
target_root
path
season
episode 或 episode range
subtitle_language
```

- 删除活动 `cloud_share` 占位语义。
- Provider 默认 worker 改为 `1`。
- intake、自动审计、元数据修复、自动补源在示例配置中全部明确为 `0`。
- 不因为文件过长重写 `simple_server`、runner 或 audit，只做必要修改。

退出条件：

- 活动代码不再调用 `hashlib.sha256`。
- 没有硬编码作品路径、TMDB ID 或媒体内容 witness。
- gap 顺序变化不会制造新任务，语义字段变化会生成新 gap。
- 所有测试通过。

### 阶段 2：当时锁定的普通入库主链

> 以下是当时的 target-shelf-first 设计，已被当前长期合同的 reconciliation-first 流程取代。

执行：

```text
待刮削直接子目录
  → awaiting_target_shelf
  → 用户选择 movie/anime/us_tv
  → /start
  → archive preprocessing
  → TMDB/身份
  → 规划
  → problem_files gate
  → 单 writer
  → AList 回读
  → 元数据
  → 定向审计
  → finalizer
```

本阶段主要验证现有实现，不重写 Engine。

退出条件：

- 选择货架前不调用 TMDB、解压、planner 或 writer。
- 电影、番剧、美剧各有正例。
- ZIP、7z、RAR、伪装归档和密码归档通过。
- 路径穿越、链接、损坏归档、错误密码正确失败。
- 目标存在时不覆盖。
- 重启不重复写入。
- 失败、取消保留 source。
- cleanup 只处理本任务拥有的内容。

### 阶段 3：审计严格只读化

执行：

- `POST /api/library-audit/run` 对远端正式库严格只读。
- 它只允许写本机的审计报告和 gap 状态。
- NFO、海报修复从审计扫描中拆出，受独立 repair gate 控制。
- 普通任务结束后只审计当前作品，不做全库扫描。
- `missing`、`satisfied`、`unknown` 三态互斥。
- unknown 不进入补源。
- 重复文件、未知附件和残留只报告，不自动删除。
- 字幕必须精确绑定视频、集数和目标语言。

退出条件：

- 手工审计前后，正式库路径、类型、数量和字节数一致。
- ffprobe 超时产生 unknown，而不是缺字幕。
- 已有目标语言字幕不重复补。
- 相同事实重复审计不创建第二个 Provider 任务。

### 阶段 4：恢复严格三阶策略

只实现三个固定 Provider：

```text
quark_share  → quark_fast_save
quark_magnet → quark_magnet_offline
magnet       → torrent
```

发现源与交付阶分开：

- PanSou：第一阶分享候选。
- AnimeTosho、TokyoTosho、Mikan、SubsPlease、DMHY、Nyaa、ACG：磁力/Torrent 候选。
- 同一个 Torrent 候选可分别产生第二阶和第三阶变体。
- 第二阶失败不能把相同 infohash 的第三阶候选一起永久排除。

降级规则：

- 标题命中不算有效候选，必须有精确文件清单和 gap 覆盖。
- 分享失效、文件不存在、清单不符、资源身份不符属于候选失败。
- 网络、认证、限流、磁盘不足、Helper 不可达、AList 不可达属于基础设施失败。
- 基础设施失败不计入 30 次门槛、不排除资源、不降阶。
- 已经提交但结果不确定属于 `in_doubt`，只能对账，不能重提或降阶。
- 第一阶不得直接跳第三阶。
- 云阶原规则继续保留最少 30 个不同 locator 的有效资源失败。
- 真正完整搜索后为零候选，可以用持久化的 `search_complete_no_candidates` 证明推进，不需要伪造 30 个失败。
- 第二阶进入第三阶必须证明所有必需搜索源已完成，当前没有未检查的二级候选；只有“次数达到 30”仍不够。

退出条件：

- exact 三阶顺序测试通过。
- required source 超时不能生成 exhaustion proof。
- 第一阶成功时第二、三阶调用为零。
- Helper 故障时 aria2 调用为零。
- 重启后仍停留在原阶和原 attempt。

### 阶段 5：建立三阶共用的 Delivery 合同

三个获取器必须只返回：

```json
{
  "lane": "quark_share|quark_magnet|magnet",
  "attempt_id": "...",
  "staging_root": "...",
  "files": [
    {
      "path": "...",
      "size": 123,
      "kind": "video|subtitle",
      "gap_ids": ["..."]
    }
  ],
  "external_task_id": "可选"
}
```

禁止结果中出现：

```text
formal_path
target_root
destination_parent
movie_root
tv_root
```

统一准入：

- `staging_root` 必须属于当前 root job 和 attempt。
- AList 逐文件核对路径、类型和大小。
- 视频只做一次有界 ffprobe。
- 小文件、无视频流、越界路径、未知对象直接拒绝。
- 视频通过后创建受限 Engine child。
- 字幕走 exact-video/exact-language 侧挂流程。
- Provider v1 只声明直接媒体和字幕支持；补源归档暂不虚报。以后如需要，只能复用现有 archive preprocessing。

退出条件：

- 三个 fake materializer 使用同一种结果结构。
- 任一 materializer 返回正式库字段时立即拒绝。
- 三种 delivery 最终调用同一个 Engine child 和 writer。

### 阶段 6：实现三个获取器

#### 6A：夸克分享快转

只迁移历史实现中的必要部分：

- PanSou/受信任分享发现。
- 分享实时有效性检查。
- 只读递归文件清单。
- 精确 `file_id/path/size → gap_ids`。
- 从匹配的 AList Quark storage 临时取得会话，只用于分享发现和只读清单核验。
- 将已核验的 file ID 交给 typed `share-save`；当前 Compose sidecar 按 action 从匹配 AList storage 临时取得 Cookie 与 AList v3.62 的 `root_folder_id`（兼容旧 `root_id`），桌面夸克已登录 renderer 只提供被动 WSG 能力并不接收 Cookie。该条取代本历史计划中“由 renderer 直接快转、AList cookie 不进入 Helper”的旧部署假设。
- AList 到达回读。

不恢复 SHA request ID、receipt、回滚副本或通用分享平台。

真实退出条件：

- 一个有效分享完整走过：

```text
分享快转 → staging → 受限 Engine → 隔离正式根
```

- 二、三阶未调用。

#### 6B：夸克磁力云离线

执行：

- 复用当前 Torrent 搜索和 manifest 解析。
- 精确选择文件 index。
- 通过与 API 共享 loopback 的 Compose Quark Helper sidecar 提交离线任务。
- 保存 `attempt_id → Quark task_id`。
- 重启后继续查询相同 task ID。
- 离线目标只能是当前 attempt staging。

Helper 只保留固定动作：

```text
health
share-save
magnet-submit
magnet-status
```

不恢复：

- 通用 Quark endpoint 转发。
- 手写 WebSocket。
- SHA 请求 ID/build ID。
- SubmitJournal 平台。
- Python Helper LaunchAgent 或 Helper 自动安装。
- Helper 主动启动、重启、激活或点击 Quark。

只用一个小型 attempt JSON 保存 task ID。提交超时且不知道是否成功时进入 `waiting_reconcile`。

真实退出条件：

- 一个 magnet 完成：

```text
Quark 离线 → staging → 受限 Engine → 隔离正式根
```

- 本地 aria2 未调用。
- API/Helper 重启不产生第二个离线任务。

2026-08-11 部署修订（用户明确批准）：

- typed 四动作 Helper 改为 Compose sidecar，与 API 共享网络命名空间和 `127.0.0.1:18765`，不再安装 Python LaunchAgent。
- sidecar 只连接固定 `host.docker.internal:19222/json/list`，不扫描端口，不具备宿主进程或 UI 控制能力。
- 桌面夸克生命周期是与 Helper 分离的操作者边界：独立 Aqua LaunchAgent 直接运行 QuarkCloudDrive，argv 仅为可执行文件加固定 `127.0.0.1:19222` CDP 参数。
- 安装和普通重启只通过 AppKit 请求单一、精确 PID 正常退出；超时不自动强杀。只有操作者显式选择的紧急 force-restart 可让 launchd 替换它自己跟踪的 job。
- 该修订不扩展 Helper HTTP 面，不恢复通用 Quark proxy、Cookie 转发、手写 WebSocket 或 SubmitJournal。

#### 6C：本地 Torrent

复用当前：

- 搜索器。
- Torrent manifest。
- aria2 selected-file。
- 磁盘预算。
- ffprobe。
- AList 上传和回读。

执行：

- 只下载 gap 对应的 file index。
- 本地路径固定为 `/data/staging/<root>/<attempt>`。
- `.aria2` 未完成文件不得上传。
- 视频验证后上传同一个远端 attempt staging。
- 再进入完全相同的受限 Engine。

真实退出条件：

```text
本地 Torrent → 远端 staging → 受限 Engine → 隔离正式根
```

### 阶段 7：协调器、恢复和清理收口

最小持久字段：

```text
tier
active_attempt
candidate_failures_by_provider
exhaustion_proof_by_provider
external_task_id
next_retry_at
last_error_scope
```

继续使用现有 `/data/jobs`、`/data/gaps`、`/data/staging`，不重建状态系统。明确：

- job JSON 是 Engine 执行事实。
- gap JSON 是审计与补源事实。
- audit latest 是报告投影。
- staging 是任务拥有的临时内容。

规则：

- 同一 root 同时只有一个 active acquisition attempt。
- Provider worker 为一个。
- 基础设施失败最多自动重试 5 次，然后停止等待用户。
- 候选失败只排除精确 `(provider, locator/infohash)`。
- pause、cancel、in-doubt 都不降阶。
- 已有 external task ID 时只查询，不重复提交。
- 正式库目标冲突直接停下。
- 恢复依据路径、类型、大小和阶段，不用哈希。

清理：

- 只有正式库回读成功且定向重审确认 gap 消失，才删除本地和远端 staging。
- 部分成功只关闭真正消失的 gap。
- infrastructure、pause、cancel、in-doubt 保留 staging。
- 候选明确无效时只删当前 attempt 自有 payload。
- 正式媒体库永不属于补源 cleanup 范围。
- 旧 gap/staging 不批量删除。

### 阶段 8：持久化备份与恢复

代码开发使用 fake 数据，不触碰生产。第一次部署、状态切换或真实三阶开启前，必须建立同一时间点的三层恢复点：

1. 完整 AList `alist-data`。
2. 完整 ScrapeFlow `/data`，第一次包括旧 staging。
3. 正式媒体库的存储侧快照或可恢复副本。

正式库不需要在每次代码修改时全量下载，但首次真实启用自动写入前必须有存储侧恢复办法。没有正式库恢复手段，就只能继续在隔离根验收，不能全局开启。

备份顺序：

1. 全局 pause。
2. 等待 writer、Provider、audit 全部空闲。
3. 停 API。
4. 停 AList。
5. 复制 AList 数据和 `/data`。
6. 建立正式媒体库快照/副本。
7. 记录版本、时间、文件数和总字节数。
8. 在新目录恢复并启动隔离实例。
9. 恢复实例必须仍为 paused，且不自动派发旧任务。

验证只使用：

- SQLite `PRAGMA quick_check`。
- JSON 全量解析。
- 文件数和总大小。
- 隔离恢复启动。
- 代表性视频、NFO、海报可读。

不生成 SHA 清单，不建设备份 daemon。旧状态根保留为只读 legacy snapshot，不建设通用迁移框架。

### 阶段 9：自动测试和构建门

测试禁止加载生产 `.env.local` 和真实媒体根。

统一检查：

```sh
SCRAPEFLOW_IGNORE_LOCAL_ENV=1 PYTHONDONTWRITEBYTECODE=1 \
  python3 -m unittest discover -s local/tests -p 'test_*.py'

git diff --check
docker compose config
docker build -f Dockerfile.api .
```

必须覆盖：

- 普通三货架。
- 所有归档正负例。
- pause/resume/retry/cancel/restart。
- AList 回读和冲突。
- report-only 审计。
- gap 直接字段比较。
- exact 三阶顺序。
- 30 次门槛和 required-source proof。
- candidate/infrastructure/delivery-in-doubt 分类。
- 三个 materializer。
- staging 到受限 Engine。
- 部分成功、重启和清理。
- Helper down 时绝不调用 aria2。
- 活动代码无应用级 SHA-256。

30 次降级门由 fake 候选完成测试，不浪费 30 份真实资源。

### 阶段 10：隔离真实验收

使用：

- 独立 AList。
- 独立存储。
- 独立状态目录。
- 独立媒体根。
- Provider worker 1。
- 每次只运行一个任务。

普通入库样本：

- 电影。
- 番剧归档。
- 美剧季度目录。
- 错误密码。
- target 冲突。
- cancel。
- 执行中重启。

补源样本：

1. 有效夸克分享：只调用第一阶。
2. 无分享、有效 magnet：由夸克离线完成，aria2 不调用。
3. 前两阶已完整排除的 Torrent：本地 aria2 完成。
4. Helper 断线：停在当前云阶。
5. Quark submit 后 API 重启：继续同一 task ID。
6. 错误候选：不得创建 Engine child。
7. staging 内容不符：不得进入正式库。
8. 缺字幕：只安装正确目标语言侧车。

真实样本证明 materializer 能工作；30 次和复杂故障矩阵由自动测试证明，不通过真实网络重复浪费。

### 阶段 11：构建、部署和开启顺序

部署时：

1. 从干净 commit 构建镜像。
2. paused 启动。
3. 核对 build commit、AList、TMDB、Helper readiness。
4. 所有自动 gate 保持关闭。
5. 不恢复旧 backlog，不批量 retry，不批量 cleanup。
6. 先由你验收普通入库。
7. 再验收手工 report-only 审计。
8. 然后只开启自动审计，Provider 仍关闭。
9. 使用精确 TMDB/gap pilot 开启一个三阶补源任务。
10. 你再次确认后，才允许移除 pilot 限制并开启全局 Provider。

软件可以在自动 gate 关闭时判定“实现完成”；生产自动化是否开启由你单独授权。

## 七、最终交付给你的验收包

我会提交一份简洁的验收记录，不建设报告平台，内容包括：

- 最终 commit 和干净工作树。
- 实际 Compose 服务列表。
- 最终配置模板。
- 全量自动测试原始结果。
- Docker 构建结果。
- 三个普通入库样本结果。
- 三条补源线路各自的结果。
- pause/restart/in-doubt 负例。
- 每个样本的 AList 前后路径、对象类型和字节数。
- 三条线路进入同一 Engine child/writer 的证据。
- staging 成功清理、失败保留的证据。
- 备份位置、文件数、总字节数和恢复演练结果。
- 所有 gate 的最终状态。

你的最终验收可以按以下清单判定：

- [ ] 普通电影、番剧、美剧均正确。
- [ ] 归档和错误密码行为正确。
- [ ] 手工审计不修改正式库。
- [ ] 三条获取线路全部真实可执行。
- [ ] 三条线路全部先到任务 staging。
- [ ] 三条线路全部使用同一个受限 Engine 和 writer。
- [ ] 第一阶成功时后二阶不调用。
- [ ] 第二阶成功时本地 Torrent 不调用。
- [ ] 基础设施故障绝不降阶。
- [ ] in-doubt 不重复提交。
- [ ] 不进行媒体 SHA-256。
- [ ] 正式目标不覆盖。
- [ ] restart 不重复写入。
- [ ] cleanup 不触碰正式库或其他任务。
- [ ] AList、`/data`、正式媒体库恢复点真实可用。
- [ ] 旧 backlog、gap、staging 未被批量恢复或删除。
- [ ] 所有自动 gate 初始关闭。
- [ ] 最终是否开启自动审计和自动补源由你决定。

这就是当时建议冻结的完整项目收敛计划，现仅供历史参考。
