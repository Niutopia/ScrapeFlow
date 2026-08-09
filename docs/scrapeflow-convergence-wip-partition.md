# 当前收敛 WIP 分区记录

日期：2026-08-09

## 基线与保护

- 最后一个清晰的阶段 3 基线：`8f54568`（`feat: add bounded archive safety kernel`）。
- 当前工作树未做 reset 或删除；已建立本地保护分支 `codex/wip-convergence-20260809` 指向该基线。
- 当前未提交差异另存为 `/tmp/scrapeflow-convergence-wip.patch`。该补丁只是恢复用备份，不代表验收或发布。
- 运行时继续保持暂停；本记录不触碰真实媒体库或真实 AList 入站数据。

## 当前文件分区

### 阶段 4：归档接入

- `engine/scrapeflow/archive_preprocessing.py`
- `local/tests/test_archive_preprocessing.py`
- `local/scrapeflow_api/simple_engine_runner.py`（普通入站预处理接线及相关测试）
- `local/tests/test_simple_engine_runner.py`（归档入口测试）

### 阶段 5：Provider/retry

- `local/scrapeflow_api/automatic_replenishment.py`
- `engine/scrapeflow/provider_capabilities.py`
- `engine/scrapeflow/replenishment_acquisition.py`
- `local/tests/test_audit_owned_root.py`（same-gap terminal/fingerprint）
- `local/tests/test_provider_capabilities.py`

### 阶段 6：audit/subtitle

- `local/scrapeflow_api/simple_library_audit.py`
- `local/tests/test_automatic_library_gaps.py`
- `engine/scrapeflow/residual_policy.py`
- `engine/scrapeflow/subtitle_content.py`
- `local/scrapeflow_api/subtitle_policy.py`
- `local/tests/test_subtitle_content.py`
- `local/tests/test_subtitle_policy_override.py`
- `local/tests/test_subtitle_sidecar_evidence.py`
- `local/tests/test_subtitle_writer_validation.py`

### 阶段 7：root/child/cleanup/Web

- `local/simple_server.py`
- `local/tests/test_recovery_matrix.py`
- `local/tests/test_root_child_projection.py`
- `local/tests/test_terminal_cleanup.py`
- `app/core/api-client.ts`
- `app/components/task-expansion.tsx`
- `app/components/task-row.tsx`
- `app/hooks/use-scrapeflow.ts`
- `app/hooks/use-dashboard-controller.ts`
- `app/scrapeflow-app.tsx`

后续提交必须只覆盖一个阶段；当前混合 WIP 未通过完整阶段验收。

阶段 checkpoint 与发布审计见 [scrapeflow-phase-checkpoints.md](scrapeflow-phase-checkpoints.md)。
