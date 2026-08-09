# 运行态只读审计（2026-08-09）

本报告只读采集，未重启、重建、迁移、删除状态，也未解除 pause。

## 源码与容器不一致

- 工作树 HEAD：`8f54568`。
- 工作树 `simple_server.py` 与 API 容器内版本 hash 不一致。
- 工作树 `simple_engine_runner.py` 与 API 容器内版本 hash 不一致。
- 当前 API 容器内没有本次新增的 `lane_gates` 投影代码。
- 容器没有源码 bind mount；Dockerfile 通过 `COPY` 将源码打进镜像。
- 当前 API/gateway 运行的是旧镜像，不能作为本次源码发布验收证据。

## 当前旧状态规模

- jobs：94 个 JSON；其中 executed 92、cancelled 1、failed 1。
- gaps：1121 个文件；其中 retry_wait 1111、resolved 10。
- staging：37 个文件/目录，约 3.5GB，含 MKV、`.aria2`、`.torrent` 和多个 attempt。
- library-audit：3 个文件，约 8MB。
- journals：0。
- locks：1。
- 整个 scrapeflow state 约 3.5GB。

## 运行策略

新镜像部署前不得直接恢复旧 backlog。正确顺序是：

1. 保持全局 pause。
2. 用新镜像只读分类旧 root/gap/staging 的所有权和阶段。
3. 保存只读分类报告及备份映射。
4. 新镜像启动后核对容器源码 hash、`build_commit`/health、lane gates 和 `/api/control`。
5. 只用隔离样本验证普通 lane；Provider/audit 自动 lane 继续关闭。
6. 不批量删除旧 JSON/gap/staging，不以旧 backlog 变绿作为验收标准。
