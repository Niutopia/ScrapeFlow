# ScrapeFlow 隔离真实验收记录（P15）

本记录用于当前 P15 的真实隔离验收。不要在没有独立 AList、独立存储、独立状态目录、独立媒体根和真实恢复点时填写通过结论。补源顺序固定为 `quark_share → alist_offline → magnet`；它不是旧 Quark 磁力离线流程的验收表。

## 环境

- Git commit:
- 构建时间:
- Compose project:
- API URL:
- AList URL:
- 隔离状态目录:
- 隔离媒体根:
- 离线备份 manifest:
- 正式媒体库外部恢复点:
- Quark Helper readiness:
- Provider worker 数量: 1
- 初始 pause: 是 / 否
- 自动审计 gate: 关闭 / 开启
- 自动补源 gate: 关闭 / pilot / 全局

## 预检查

- [ ] 从干净 commit 运行 `python3 scripts/scrapeflow_release_evidence.py --output-dir artifacts/release` 通过，并归档 JSON 与原始日志。
- [ ] 在 runtime 首次启动前执行
  `python3 scripts/scrapeflow_isolated_preflight.py declaration.json --report preflight-report.json`
  通过，并归档报告。验收包使用 `--preflight-report preflight-report.json`
  读取该固化结果，不在启动后重跑“目录必须为空”的瞬时检查。
  记录报告路径与 SHA-512；该报告是本机 self-attested evidence，不宣称具备外部签名。
- [ ] 离线备份 `verify` 通过。
- [ ] 隔离恢复 `restore` 通过，恢复状态仍为 paused。
- [ ] `/api/health` 显示预期 commit 或 build version。
- [ ] `/api/control` 显示 paused。
- [ ] 自动 gate 初始关闭。
- [ ] 没有恢复旧 backlog。
- [ ] 没有批量 retry。
- [ ] 没有批量 cleanup。

## 普通入库样本

| 样本 | 输入 | 预期 | 结果 | 证据 |
| --- | --- | --- | --- | --- |
| 电影 |  | 创建 RootJob 时选择 movie 后入库，回读正确 | 未执行 |  |
| 番剧归档 |  | 归档预处理后入库，回读正确 | 未执行 |  |
| 美剧季度目录 |  | 创建 RootJob 时选择 us_tv 后入库，回读正确 | 未执行 |  |
| 错误密码 |  | 停在归档错误，source 保留 | 未执行 |  |
| 正式目标冲突 |  | 停止，不覆盖 | 未执行 |  |
| cancel |  | 停止，source/staging 保留在本任务范围 | 未执行 |  |
| 执行中重启 |  | 重启后不重复写入 | 未执行 |  |

## 补源样本

| 样本 | 输入 gap | 预期 | 结果 | 证据 |
| --- | --- | --- | --- | --- |
| 有效 quark_share |  | 第一阶完成，后二阶未调用 | 未执行 |  |
| quark_share 完整排除、有效 alist_offline |  | AList/aria2 离线完成，本地 Torrent 未调用 | 未执行 |  |
| quark_share 与 alist_offline 完整排除后 magnet |  | 本地 Torrent 完成 | 未执行 |  |
| 有效 quark_share 候选时 Helper 不可用 |  | 停在 quark_share，不降阶 | 未执行 |  |
| AList 提交响应丢失后 API 重启 |  | 对账同一 AList task，不重复提交 | 未执行 |  |
| AList 完整种子超 32 GiB 或剩余空间不足 |  | 提交前同阶 infrastructure；没有 AList/aria2 新任务 | 未执行 |  |
| AList transfer 超过总时限 |  | 已确认取消才同阶 retry；未确认则 in-doubt 对账、不重复提交 | 未执行 |  |
| 错误候选 |  | 不创建 Engine child | 未执行 |  |
| staging 内容不符 |  | 不进入正式库 | 未执行 |  |
| 缺字幕 |  | 只安装正确目标语言侧车 | 未执行 |  |

## 每个样本必须记录

- AList 前后路径。
- 对象类型。
- 字节数。
- 任务 ID。
- provider lane。
- attempt staging。
- 内部 child ID。
- 正式库回读结果。
- 定向审计结果。
- staging 成功清理或失败保留证据。

## 最终判定

- [ ] 普通电影、番剧、美剧均正确。
- [ ] 归档和错误密码行为正确。
- [ ] 手工审计不修改正式库。
- [ ] quark_share、alist_offline、magnet 三条获取线路全部真实可执行。
- [ ] 三条线路全部先到任务 staging。
- [ ] 三条线路全部使用同一个受限 Engine 和 writer。
- [ ] 第一阶成功时后二阶不调用。
- [ ] 第二阶成功时本地 Torrent 不调用。
- [ ] 基础设施故障绝不降阶。
- [ ] in-doubt 不重复提交。
- [ ] AList 完整 Torrent 容量门禁在提交前生效，未按 selected files 低估。
- [ ] AList transfer deadline 跨重启不重置；取消未确认时保持 in-doubt。
- [ ] 不进行媒体内容指纹校验。
- [ ] 正式目标不覆盖。
- [ ] restart 不重复写入。
- [ ] cleanup 不触碰正式库或其他任务。
- [ ] AList、ScrapeFlow 状态、正式媒体库恢复点真实可用。
- [ ] 旧 backlog、gap、staging 未被批量恢复或删除。
- [ ] 所有自动 gate 初始关闭。
- [ ] 最终是否开启自动审计和自动补源已由用户单独授权。
