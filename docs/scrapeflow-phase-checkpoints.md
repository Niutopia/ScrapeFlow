# 收敛阶段 checkpoint 与发布审计

日期：2026-08-09

当前 release candidate HEAD：`69f6de3`。

这份表只记录当前工作树中有测试或运行时证据支持的状态；“通过”不等于已提交，也不等于真实媒体库验收。

| 阶段 | 当前状态 | 证据 | 备注 |
|---|---|---|---|
| 3 | frozen | 基线 `8f54568`、保护分支 `codex/wip-convergence-20260809` | 不修改基线，不删除混合 WIP |
| 4A | golden path passed | `local.tests.test_phase4_golden_path`；普通视频、ZIP/7z/RAR、EXE/BIN/DAT 伪装、字幕 sidecar 及负例 fixture | source 成功移入 task-owned processed；失败保留 source；重启不重复 writer |
| 4B | deferred | 没有真实 Provider materializer `archive_source` 输入 | 不计入已完成能力，保持 Provider SFX 关闭 |
| 5 | bounded / capability explicit | Provider capability、same-gap fingerprint、terminal stale timer 测试 | Magnet/Torrent ready；SFX deferred；无 durable claim registry |
| 6 | scoped / fail-closed | audit/subtitle 定向测试、strict `zh-Hans` lane | unknown 合法；probe 失败不建 Provider；不做 OCR |
| 7 | contract tested | root/child、cancelled child、terminal cleanup、Web correction 测试 | cancelled child 不进入 active children；retry contract 仅允许四个字段 |

## 当前发布门禁

- `npm run check` 必须通过；最近一次结果：311 个 Python 测试通过、lint/typecheck/build check 通过。
- `git diff --check` 必须通过；最近一次结果：通过。
- 真实运行时保持 fail-closed 暂停：缺少 `global-control.json` 时 `PersistentControlState` 返回 `paused=true`。
- 生产 root 的 `provider_auto_repair_enabled` 和 `audit_auto_repair_enabled` 默认关闭，只有显式环境门禁才开放。
- 阶段代码已提交；真实 Docker 镜像仍需在 pause 状态下重建并进行 hash/health 对齐。

## 明确 parked

- `provider-gap-claims.json`、锁文件、多实例 lease/ownership。
- 没有真实 materializer 输入支撑的 Provider SFX。
- 无限嵌套归档、OCR、完美字幕 witness、未来远端 adapter 插件框架。
