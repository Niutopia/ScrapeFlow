# 运行态只读审计（2026-08-09）

> 文档性质：这是目标货架启动门之前的历史运行态快照。它不证明当前 WIP、当前镜像或当前产品合同已完成，也不授权解除 pause、核验源码 hash、迁移旧状态或执行真实样本。
>
> 当前阶段门以[ScrapeFlow 最终收敛计划 v1](./scrapeflow-final-convergence-plan-v1.md)为准；本文件只是历史审计快照，不构成完成声明。

本报告先记录部署前的只读采集，再记录同日受控部署结果。全程未迁移、删除或恢复旧任务，也未解除 pause。

## 部署前：源码与容器不一致

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

## 受控部署结果

- release candidate：`1b7ed3edcf8735edeac25e2ca2dac6e4e9eb2d9c`。
- API image：`sha256:60a1641cd789857121c56c15919bd4064dbbed8ca3b729a912c830859c068d19`。
- Web image：`sha256:bedca9ac3d60bf01d20b15728dd40163d0a425a6662a02603f8a9d4b35ad7af6`。
- API、gateway 与 AList 均为 healthy。Compose 因依赖关系一并重建了 AList 容器，但沿用原持久卷，未修改或清理数据。
- `/api/health.build_commit` 与 release candidate 一致；宿主和 API 容器内 `simple_server.py`、`simple_engine_runner.py` 的 SHA-256 分别完全一致。
- `/api/control` 仍为 `paused=true`、`scheduler_paused=true`；旧容器退出时将暂停原因更新为 `shutdown`，暂停状态没有解除。
- `provider_auto_repair_enabled=false`、`audit_auto_repair_enabled=false`、`intake.enabled=false`、`formal_write_workers=0`、`provider_workers=0`。
- 部署后的只读复核仍为 jobs 94、engine-jobs 2、gaps 1121、staging 37 个文件（61 个目录，约 3.5GB）、journals 0、locks 1；未对这些对象执行清理或续跑。

阶段 4B 的真实 Provider `archive_source` / SFX 输入仍为 deferred；本次部署不把它表述为已通过，也没有运行真实媒体样本。
