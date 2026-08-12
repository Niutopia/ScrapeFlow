# 目标货架启动门本地 RC 审计

日期：2026-08-09

> 文档性质：这是 2026-08-09 的历史 RC 审计快照，只记录当时 target-shelf-first 实现的证据；不构成当前产品或工程合同。
> 长期合同见 [`../AGENTS.md`](../AGENTS.md)，当前 HEAD 实现事实见 [`CURRENT-STATE.md`](CURRENT-STATE.md)。

分支：`codex/scrapeflow-transactional-convergence`

发布标识：`target-shelf-rc2`

## 审计结论

2026-08-09 审计时，源码已形成“目标货架启动门”本地 `target-shelf-rc2`：普通来源先登记为 `awaiting_target_shelf`，用户通过 Dashboard 或 `/start` 选择 `movie`、`anime`、`us_tv` 后才允许归档、TMDB、规划和写入。Provider/audit 自动 lane 默认仍关闭。

本轮 rc2 只收掉 Web/文档尾项：waiting/conflict 展开详情随低频 jobs 刷新同步，Operations Overview 单独显示等待选择货架数量，缺少 `target_shelf` 的 legacy 失败任务不在任务行/详情中显示无效重试，归档密码输入只在 `failed_archive` 失败详情中显示。

本审计只证明阶段 0–6 的本地源码、fake AList、单机测试和 Web 构建门禁；它不证明真实媒体库样本已通过，也不授权解除全局暂停或恢复日常 intake。

## 阶段证据

| 阶段 | 结论 | 证据 |
| --- | --- | --- |
| 0 合同与边界 | 历史通过 | 这是目标货架 RC 当时的结论；当前长期合同是 `AGENTS.md`，当前实现事实见 `docs/CURRENT-STATE.md`，本表不得作为全项目完成声明。 |
| 1 登记与 `/start` | 通过 | `engine/scrapeflow/target_shelf.py` 提供唯一映射；`local/tests/test_target_shelf_start_gate.py` 覆盖 waiting 零正式副作用、非法枚举、重复 `/start`、paused start 和 waiting restart；`local/tests/test_simple_server.py` 覆盖 HTTP `/start`。 |
| 2 已选主链 | 通过 | `local/scrapeflow_api/simple_engine_runner.py` 在 worker/plan/execute/recovery 前校验 shelf；归档后缀身份查询、类型兼容矩阵、target root containment 和 problem gate 由 `test_target_shelf_start_gate.py` 与 `test_simple_engine_runner.py` 覆盖。 |
| 3 恢复、重试与取消 | 通过 | `test_simple_server.py` 覆盖 queued cancel、legacy retry 拒绝、target conflict 重选、cleanup-only retry 不落回 writer；`test_recovery_matrix.py` 与 runner 测试覆盖重启边界。 |
| 4 source/staging/audit/provider | 通过 | `test_phase4_golden_path.py` 覆盖普通视频、ZIP/7z/RAR、伪装归档、密码错误、路径穿越、processed 保留和 archive staging 清理；`test_simple_server.py` 覆盖 disabled lanes 收口与 `completed_with_gaps` 投影。 |
| 5 Web 闭环 | 通过 | `app` 只通过 `target_shelf` 调 `/start`，不发送 `target_parent`；waiting/conflict 可见、可展开、可重选；Web contract check 覆盖 `/start` payload、legacy retry 隐藏、归档密码输入范围、waiting 数量和无 `target_parent`。 |
| 6 本地 RC | 通过 | `target-shelf-rc2` 作为本地源码冻结标识；后续隔离或部署运行时通过 `SCRAPEFLOW_BUILD_VERSION` 暴露该普通 build_version；`npm run check`、`git diff --check` 通过；没有真实 AList 数据测试、媒体 SHA、源码 SHA manifest 或应用内 rollback。 |

## 已执行门禁

```text
npm run check
```

结果：

```text
Web target-shelf contract check passed
Ran 345 tests in 19.907s
OK
production Web build compiled successfully
```

```text
git diff --check
```

结果：通过，无输出。

## 明确未执行

- 未运行阶段 7 的真实隔离样本。
- 未解除全局暂停。
- 未迁移、重试或清理旧 94 个 jobs、1121 个 gaps 或旧 staging。
- 未构建媒体哈希、plan digest、approval receipt、nonce/epoch、分布式 lease 或 exactly-once 证明。
- 未新增第二套 AList client、归档检查器、Web 状态库或 Provider 平台。

## 阶段 7 前置条件

阶段 7 只有在以下条件同时满足后才可开始：

- 阶段 0–6 保持通过。
- 工作树 clean，并保留分阶段 checkpoint。
- 新镜像仍全局暂停。
- 用户再次明确授权使用独立状态目录和隔离 AList 测试目录。
- audit/provider auto repair 继续关闭。

样本顺序仍按计划执行：普通电影、番剧归档、美剧季度目录、密码错误或取消负例、restart 后不重复 writer、cleanup 后重扫不重建。
