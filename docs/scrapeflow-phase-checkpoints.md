# 历史收敛 checkpoint 与发布审计

> 文档性质：2026-08-09 之前自动入口收敛的历史快照。它不代表当前工作树、当前运行时或目标货架启动门已经完成，也不授权解除暂停。

当前阶段门、测试要求和提交边界以[目标货架启动门收敛计划](./scrapeflow-target-shelf-start-gate-plan.md)为准。

历史记录中出现的阶段 3–7、来源已移动到 processed、当前 RC、源码 hash 或容器 hash 都只能解释当时的实验环境，不能作为本轮验收或内容哈希系统的要求。当前产品明确不建设媒体 SHA、源码 SHA 对比、manifest 或 receipt。

保留的历史结论仅限于：

- 单 writer、路径安全、归档安全和有界 Provider/audit 是可复用的安全边界。
- release audit 与 targeted fixture 需要在新的启动门、最终生命周期和 Web 合同下重新验证。
- 真实运行态仍应保持 fail-closed 暂停，直到当前计划阶段 0–6 通过并由用户明确授权隔离样本。
