# ScrapeFlow 隔离真实验收记录

本记录用于阶段 10 的真实隔离验收。不要在没有独立 AList、独立存储、独立状态目录、独立媒体根和真实恢复点时填写通过结论。

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

- [ ] `python3 scripts/scrapeflow_release_check.py` 通过。
- [ ] `python3 scripts/scrapeflow_isolated_preflight.py declaration.json` 通过。
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
| 电影 |  | 选择 movie 后入库，回读正确 | 未执行 |  |
| 番剧归档 |  | 归档预处理后入库，回读正确 | 未执行 |  |
| 美剧季度目录 |  | 选择 us_tv 后入库，回读正确 | 未执行 |  |
| 错误密码 |  | 停在归档错误，source 保留 | 未执行 |  |
| 正式目标冲突 |  | 停止，不覆盖 | 未执行 |  |
| cancel |  | 停止，source/staging 保留在本任务范围 | 未执行 |  |
| 执行中重启 |  | 重启后不重复写入 | 未执行 |  |

## 补源样本

| 样本 | 输入 gap | 预期 | 结果 | 证据 |
| --- | --- | --- | --- | --- |
| 有效夸克分享 |  | 第一阶完成，后二阶未调用 | 未执行 |  |
| 无分享、有效 magnet |  | 夸克离线完成，本地 Torrent 未调用 | 未执行 |  |
| 前两阶完整排除后 Torrent |  | 本地 Torrent 完成 | 未执行 |  |
| Helper 断线 |  | 停在当前云阶，不降阶 | 未执行 |  |
| Quark submit 后 API 重启 |  | 继续同一外部 task | 未执行 |  |
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
- [ ] 三条获取线路全部真实可执行。
- [ ] 三条线路全部先到任务 staging。
- [ ] 三条线路全部使用同一个受限 Engine 和 writer。
- [ ] 第一阶成功时后二阶不调用。
- [ ] 第二阶成功时本地 Torrent 不调用。
- [ ] 基础设施故障绝不降阶。
- [ ] in-doubt 不重复提交。
- [ ] 不进行媒体内容指纹校验。
- [ ] 正式目标不覆盖。
- [ ] restart 不重复写入。
- [ ] cleanup 不触碰正式库或其他任务。
- [ ] AList、ScrapeFlow 状态、正式媒体库恢复点真实可用。
- [ ] 旧 backlog、gap、staging 未被批量恢复或删除。
- [ ] 所有自动 gate 初始关闭。
- [ ] 最终是否开启自动审计和自动补源已由用户单独授权。
