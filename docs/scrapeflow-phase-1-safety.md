# ScrapeFlow 阶段 1 主链安全收敛

> 文档性质：2026-08-08 的历史安全收敛快照，不是当前目标货架启动门的阶段 1，也不证明当前 WIP 已完成。
>
> 当前阶段定义见[ScrapeFlow 最终收敛计划 v1](./scrapeflow-final-convergence-plan-v1.md)。本文件是历史阶段快照。

> 记录时间：2026-08-08
>
> 前置 checkpoint：`143506b chore: checkpoint single-user migration baseline`

阶段 1 在全局持久暂停状态下完成，未启动服务、未调度任务、未清理 staging，也未修改正式媒体库。

## 已收敛边界

- Runner 统一调用当前 `finalize_plan`；每个确切视频最多保留一条逻辑字幕轨，备选字幕写入 `scan_report.deferred_subtitles`，不再伪装成阻断性 `problem_files`。
- `problem_files` 在规划后、Runner、具体 executor、恢复检查和 cleanup 前重复阻断；正式 writer 不会绕过该门禁。
- 自动清理仅接受任务自有 staging 中可重建临时文件、`.DS_Store` 和 AppleDouble；PDF、漫画、字体、音轨、manifest、图片、主题视频及未知可执行文件保留并报告。
- 控制文件缺失、损坏、字段或类型错误时 fail-closed 为 paused；只有显式 pause/resume 写控制文件，正常 shutdown 不改写用户状态。
- POST 写接口要求 JSON，并校验 loopback Host、同源 Origin 和跨站 Fetch 元数据；原生启动默认使用项目内被忽略的 `.scrapeflow` 状态目录，Compose 继续使用 `/data`。
- `failed_cleanup`、`subtitle_installing` 已加入 Python、HTTP 和 Web 当前 phase 契约。
- API、公共任务、gap 状态和自动重试持久化统一经过共享凭据脱敏；密码、token 和 API key 不进入 job/gap JSON、错误响应或 UI 投影。

## 验证

- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s local/tests -p 'test_*.py'`：230/230 通过。
- `npm run lint`、`npm run typecheck`、`npm run build:check`：通过。
- `git diff --check`：通过。

全局暂停仍由运行时控制文件决定；阶段 2–4（领域定义和归档安全内核/接入）完成并通过受控样本验收前，不解除暂停。
