# 收敛语义决定（阶段 4–7）

这是当前实现与测试引用的唯一语义来源。

1. **自动任务顺序**：`source intake → archive preprocessing → staging → identity → planning → problem-file gate → single writer`。自动归档失败停在 archive 阶段，不调用身份解析或 writer。
2. **原始归档消费**：成功正式写入后将 source 移到任务明确拥有的 `processed/quarantine` 隔离区；失败、密码错误和取消保留 source；terminal cleanup 只删除任务记录及任务自有 staging。source 仍在入站时，扫描必须通过 source/任务指纹去重，不能重建同一任务。
3. **阶段 4 后恢复**：阶段 5/6 最小门禁完成前保持全局暂停；不建设 scheduler、lease 或复杂状态机。
4. **retry 上限**：自动重试达到上限后进入 terminal `failed`，不再创建 timer/attempt；只有用户显式 retry 才清理该 attempt 状态并重新开始。
5. **target 已存在**：新执行中目标存在视为冲突；已进入写阶段且 source 消失时，可按路径、大小和阶段恢复；source 与 target 同时存在时停止，不能自动认领。
6. **Provider SFX**：只有真实 materializer 能产出 `archive_source` 才计入支持并接入样本；否则能力明确标记为 deferred，不把未来 adapter 当作阶段 4 完成。
