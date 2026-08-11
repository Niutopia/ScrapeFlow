# ScrapeFlow 阶段 2 领域定义收敛

> 文档性质：2026-08-08 的历史领域收敛快照，不是当前目标货架启动门的阶段 2，也不证明当前 WIP 已完成。
>
> 当前阶段定义见[ScrapeFlow 最终收敛计划 v1](./scrapeflow-final-convergence-plan-v1.md)。本文件是历史阶段快照。

> 记录时间：2026-08-08
>
> 前置 checkpoint：`43a3d25 fix: close local safety and control boundaries`

阶段 2 在全局持久暂停状态下完成。未启动或重启服务，未调度任务，未清理 staging、gap 或正式媒体库。

## 已收敛边界

- `engine/scrapeflow/replenishment_matching.py` 是季集和覆盖证据的唯一实现。Local 补源和媒体库审计只保留兼容别名；`S04-89 [S4][17_89]` 使用季内 `S04E17`，`4x017-018` 展开为两个集，中文季集沿用同一坐标。
- 分数集（例如 `S01E01.5`）是独立证据，不会满足整数集缺口；分辨率后缀（例如 `.1080p`）不会被误判为分数集。
- `engine/scrapeflow/media_policy.py` 统一视频、字幕、归档/分卷、下载临时文件，以及音频、文档、字体、图片、manifest 和可执行文件分类。规划、媒体质量、补源、Local materializer、审计和 residual policy 均引用同一集合；`.iso`、`.mts`、`.strm`、`.flv`、`.rmvb` 等边界扩展名结论一致。
- Provider 能力从当前可执行 materializer 推导：可执行线路只有 `provider=magnet` + `acquisition.kind=torrent`；`cloud_share` 只保留 `unavailable` 投影。搜索、selector、preflight、materializer 和自动补源委托在持久化或写入前 fail-closed。
- 媒体库审计复用 `residual_policy`，将归档、附件、未知文件和孤立字幕写入顶层及 `observations`。这些条目不进入 `automatic_tasks`，不会获得自动删除或移动权限；含这些证据的快照不标记为 `clean`。
- Web/API capability、Provider 标签和审计残留摘要与上述当前契约同步。

## 验证

- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s local/tests -p 'test_*.py'`：248/248 通过。
- `npm run check`（lint、typecheck、Python 回归、production build）：通过。
- `git diff --check`：通过。

阶段 3 的归档安全内核和阶段 4 的普通入站/Provider 接入完成并通过受控样本验收前，全局暂停保持不变。
