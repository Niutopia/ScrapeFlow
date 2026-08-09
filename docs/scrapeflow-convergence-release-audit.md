# ScrapeFlow 收敛发布审计

日期：2026-08-09

> 文档性质：这是 2026-08-09 的历史 release-candidate 审计快照，记录当时 fake 环境、分支与阶段检查的证据。它不证明当前工作树、当前运行时或“用户选择目标货架后启动”主线已经实施完成，也不授权解除暂停。
>
> 当前普通入站合同以[用户选择目标货架后启动实施计划](./scrapeflow-target-shelf-start-gate-plan.md)为准：发现来源只登记 `awaiting_target_shelf`，用户选择固定一级货架并通过 `/start` 后才正式执行。

本审计所记录的 release candidate 当时位于 `codex/scrapeflow-transactional-convergence`；阶段 checkpoint 已按阶段提交，最后由 convergence commit 收束跨阶段 runtime wiring。精确提交号应由当时部署的 `/api/health.build_commit` 和 Git 共同记录，本文不硬编码。

## T0–T1：基线、WIP 与语义

- 阶段 3 基线当时固定为 `8f54568`；保护分支为 `codex/wip-convergence-20260809`。
- 当时的 diff 分区及“WIP 不代表验收”声明见 `scrapeflow-convergence-wip-partition.md`。
- 当时的六项收敛语义决定见 `scrapeflow-convergence-decisions.md`；其中旧的“automatic”入站描述已被当前启动门合同取代。
- 未执行 destructive reset，未触碰真实媒体库或真实 AList 入站数据。

## T2–T5：历史阶段 4 主链证据

- 当时 automatic 顺序为 `archive_preprocess → identity → planning → writer`；预处理失败不调用 identity/writer。该顺序现仅适用于用户选择货架并经 `/start` 启动后的任务。
- 普通视频、ZIP、7z、RAR、伪装 EXE/BIN/DAT 与字幕 sidecar 进入同一 planner/problem gate/`SimplePlanExecutor`。
- 成功 automatic source 移到 task-owned processed；失败/密码错误保留 source。
- golden fixture 在 terminal cleanup 后执行新 intake scan，断言不会重建 job；重启也不会第二次调用 writer。
- 路径穿越、链接、NFC/大小写冲突、多作品、problem_files、密码及三类预算失败均保持正式库为空。

## T6：lane gate

- 生产 root 默认：`provider_auto_repair_enabled=false`、`audit_auto_repair_enabled=false`。
- 显式环境变量 `SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED=1` 和 `SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED=1` 才能分别开放。
- 当时普通入站仍受全局 pause 控制；缺少或损坏 `global-control.json` 时 fail-closed 为 paused。当前合同还要求暂停或恢复均不得绕过等待选择状态。
- disabled lane 不创建 provider/audit timer；Web health 返回当前门禁状态。

## T7–T8：retry 与 Provider

- gap fingerprint 稳定且顺序无关；相同 terminal fingerprint 不被 fresh audit 重开。
- terminal root 在 timer 创建、timer callback 和 stale future 启动三个边界均被拒绝。
- 用户显式 retry 才清 terminal/attempt 并重新排队。
- 仅 Magnet/Torrent capability 为 ready；Cloud share unavailable。
- 普通入站伪装归档属于阶段 4A；真实 Provider `archive_source` SFX 属于阶段 4B，当前明确 deferred，不计入已完成能力。
- 主路径不存在 `provider-gap-claims.json`、`.lock`、lease 或跨进程 ownership registry。
- Provider payload 仍通过现有 staging、媒体/字幕验证、problem gate 与 single writer。

## T9–T10：root/child 与 Web

- cancelled child 为 terminal、非 active，root projection 不再变成 retry_wait，也不阻塞 terminal cleanup。
- active/retry child 仍拒绝 cleanup；failed/successful child 使用同一状态矩阵。
- retry contract 只允许 `tmdb_id`、`media_type`、`season`、`archive_password`。
- title/year/target_parent 由后端 policy 推导或拒绝，Web 无法指定正式库父目录。
- 密码只存在下一次 retry 的内存槽，提交后前端清空，不进入 job JSON/日志/响应。

## T11：audit/subtitle

- 自动 audit 必须有可信作品 target_root；缺失或非法 scope 时 fail-closed，不退化到全库扫描。
- probe 失败产生 unknown，不生成 Provider gap。
- 语言 lane 仅为 `zh-Hans`、`non-zh-Hans`、`unknown`；显式 zh-Hans 不接受 zh-Hant/zh-TW，裸 zh 保持 unknown。
- 没有 OCR；unknown 不覆盖已有字幕；subtitle writer 只接受 task-owned/staged 内容。

## T12：本地发布门禁证据

- `npm run check`：2026-08-09 当时共 312 个 Python 测试通过；ESLint、TypeScript、Next.js production build 通过。
- `git diff --check`：当时通过。
- Git 工作树当时 clean；可回退到 `8f54568` 或任一阶段 checkpoint。
- `local.tests.test_phase4_golden_path`：7 个正例与 9 个负例通过。
- 全仓搜索无 `provider-gap-claims` 写入路径。
- 运行时保持 fail-closed pause，并有两条显式 production lane gate。

本历史快照记录了当时本地 fake 环境主链满足当时计划门禁的证据；它不证明当前本地 fake 环境、当前混合 WIP 或新启动门计划已完成，也不授权解除真实运行时暂停。
