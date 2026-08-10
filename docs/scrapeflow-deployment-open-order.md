# ScrapeFlow 部署与开启顺序

本文件用于阶段 11。它不是生产授权书；自动审计和自动补源是否开启，必须由用户在真实验收后单独决定。

## 构建

1. 确认工作树干净。
2. 运行：

```sh
python3 scripts/scrapeflow_release_check.py
```

3. 记录 Git commit 和构建时间。
4. 不把 `.env.local`、状态目录、备份目录或媒体文件加入提交。

## 启动

1. 使用干净 commit 构建镜像。
2. 使用隔离或生产指定状态目录启动。
3. 保持 `SCRAPEFLOW_START_PAUSED=1`。
4. 保持 `SCRAPEFLOW_INTAKE_MONITOR=0`。
5. 保持 `SCRAPEFLOW_AUTOMATIC_AUDIT=0`。
6. 保持 `SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED=0`。
7. 保持 `SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED=0`。
8. 保持 `SCRAPEFLOW_PROVIDER_WORKERS=1`。
9. 不恢复旧 backlog。
10. 不批量 retry。
11. 不批量 cleanup。

## 启动后核对

```sh
curl -fsS http://127.0.0.1:8765/api/health
curl -fsS http://127.0.0.1:8765/api/control
```

必须确认：

- API 只暴露在本机入口。
- health 中的 build 信息符合本次 commit。
- AList 可用。
- TMDB 可用。
- Quark Helper readiness 符合本次验收目标。
- control 为 paused。
- intake、audit、provider 自动 gate 均关闭。
- provider worker 为 1。

## 开启顺序

1. 用户验收普通入库：电影、番剧归档、美剧季度目录、错误密码、冲突、cancel、重启。
2. 用户验收手工 report-only 审计。
3. 只开启自动审计，Provider 仍关闭。
4. 使用精确 TMDB/gap pilot 开启一个补源任务。
5. 验证三阶补源样本和负例。
6. 用户确认后，才允许移除 pilot 限制。
7. 用户再次确认后，才允许全局 Provider。

## 禁止事项

- 不在 paused 状态下启动自动 backlog。
- 不把旧 gap/staging 批量删除成“干净状态”。
- 不在 Helper 故障时降到本地 Torrent。
- 不在 in-doubt 状态重复提交。
- 不让 Provider、Helper 或 aria2 决定正式库位置。
- 不让审计扫描修复 NFO 或海报。

## 回退

1. pause。
2. 等待活动操作归零。
3. 停 API。
4. 停 AList。
5. 使用阶段 8 的离线备份和正式媒体库外部恢复点恢复。
6. 恢复实例必须先以 paused 启动。
7. 复核 `/api/health` 和 `/api/control` 后再决定是否重试单个任务。
