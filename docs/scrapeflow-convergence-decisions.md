# 收敛语义决定（历史阶段 4–7 边界）

本文保留 2026-08-09 之前收敛工作的安全与恢复决定。下列规则只描述当时的 target-shelf-first 行为，不声明这些规则已经在当前工作树完成实现，也不构成当前入口规则。长期产品与工程合同见 [`../AGENTS.md`](../AGENTS.md)；当前事实以根 AGENTS.md、源码、测试和 Git 工作树证据核对。

1. **普通任务启动与顺序**：`source intake registration → awaiting_target_shelf → 用户选择 target_shelf 并调用 /start → archive preprocessing → staging → identity → planning → problem-file gate → single writer`。选择前不调用 archive、TMDB、planner、writer 或 Provider；启动后的归档失败停在 archive 阶段，不调用身份解析或 writer。
2. **原始归档消费**：source 在正式写入、回读、元数据、定向审计和 Provider 终态之前保持原位；只在最后的任务自有 cleanup 中处理已确认可清理的 source 与 staging。失败、密码错误和取消保留 source；terminal cleanup 只删除任务记录及任务自有 staging。source 仍在入站时，扫描必须通过 source/任务指纹去重，不能重建同一任务。
3. **恢复与暂停**：本历史阶段的最小门禁不授权解除当前全局暂停。`awaiting_target_shelf` 在重启后继续等待，不能被恢复逻辑自动排队；不建设 scheduler、lease 或复杂状态机。
4. **retry 上限**：自动重试达到上限后进入 terminal `failed`，不再创建 timer/attempt；只有用户显式 retry 才清理该 attempt 状态并重新开始。未选择目标货架的等待任务不得用 retry 绕过 `/start` 启动门。
5. **target 已存在**：新执行中目标存在视为冲突；已进入写阶段且 source 消失时，可按路径、大小和阶段恢复；source 与 target 同时存在时停止，不能自动认领。媒体类型与用户选择的一级货架冲突时不自动改选，等待用户通过 `/start` 显式重选。
6. **Provider SFX**：只有真实 materializer 能产出 `archive_source` 才计入支持并接入样本；否则能力明确标记为 deferred，不把未来 adapter 当作阶段 4 完成。Provider child 继承根任务已确认的目标货架。
