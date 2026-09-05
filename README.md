# ScrapeFlow

ScrapeFlow 是一个只在本机运行的单用户 AList 影视库整理工具：从 `/quark/影视/待刮削` 发现来源，用户选定一个根任务和货架后，按作品拆分、识别、入库，并对该任务的缺集补源。

它只有一个 API、一个 AList 和一个本地 JSON 控制记录；不需要隔离验收环境、发布证明、离线备份演练或额外控制服务。

## 启动

```sh
cp .env.local.example .env.local
# 编辑 .env.local：至少填写 ALIST_*、TMDB_API_KEY、SCRAPEFLOW_HOST_STATE_ROOT
docker compose --env-file .env.local up -d --build
```

打开 `http://127.0.0.1:${SCRAPEFLOW_API_PORT:-3010}`。

API 仅绑定 `127.0.0.1`。容器内 API 端口固定为 `8765`，宿主默认端口为 `3010`。状态保存在 `SCRAPEFLOW_HOST_STATE_ROOT/scrapeflow-data`，AList 数据保存在同一目录的 `alist-data`。

## 日常使用

1. 在页面刷新待刮削目录；这一步只读取 `/quark/影视/待刮削` 的直接子目录，不创建任务也不请求 TMDB。
2. 可以逐项创建 RootJob，也可以先授权一个有序批次；批次授权只记录来源身份、可选货架和顺序，不会立即创建任务。
3. 点击“恢复”运行已选 RootJob，或显式运行批次。每次只有一个 RootJob/写入者活动；收口后会暂停并 fresh 检查下一项。
4. 身份或对账不确定时，在任务的作品单元中确认；真实失败直接显示在任务中。

控制状态只有本地 JSON 中的两个字段：`paused` 和 `root_job_id`。每次 API 启动都保持暂停，直到用户恢复已选任务。

## 本地 API

常用只读接口：

```text
GET  /api/health
GET  /api/control
GET  /api/intake
GET  /api/jobs
GET  /api/jobs/:id
GET  /api/jobs/:id/work-units
GET  /api/jobs/:id/replenishment
GET  /api/batch                    # 来源清单记录（只读，不驱动执行）
GET  /api/replacements/:id         # 服务端生成的 replacement manifest（只读）
```

常用操作：

```text
POST /api/intake/refresh
POST /api/root-jobs                 {"path":"/quark/影视/待刮削/作品","target_shelf":"anime"}
POST /api/control/select            {"root_job_id":"..."}
POST /api/control/resume            {"root_job_id":"..."}   # 可省略，恢复当前选择
POST /api/control/pause             {}
POST /api/jobs/:id/work-units/:unitId/confirm   {"media_type":"tv","tmdb_id":123,"season":1}
POST /api/jobs/:id/replenish        {}
POST /api/jobs/:id/retry            {}
POST /api/jobs/:id/cancel           {}
POST /api/jobs/:id/cleanup          {}
POST /api/jobs/:id/consume-source   {}   # 终态根消费源树与展开 staging，幂等可重跑
POST /api/jobs/:id/file-disc-ruling {}   # 光盘 scope 的播放列表→集数人工裁决
POST /api/batch                     {"items":[{"source_id":"...","shelf":"anime"}]}
POST /api/batch/retry               {"source_id":"...","shelf":"anime"}
```

批次清单只是来源顺序与货架的记录，本身不执行任何东西：每个来源都要用 `/api/control/select` 单独授权，一个来源收口后不会自动开始下一个。光盘镜像（ISO/UDF）由 X 相位只读解析并在任务 staging 展开后随普通链路入库；无法唯一证明映射的盘通过数据级人工裁决（`disc-ruling`）处理，未决的 scope 保持 attention 且不阻塞兄弟。归档/EXE 等其它容器在有界检查完成前停留在 attention。明确授权的 replacement 使用服务端生成的精确 manifest，仍复用普通 Planner、单 writer 和 fresh 回读。

终态根（completed / gaps_pending）默认全量消费来源树与展开 staging（`/ScrapeFlow/展开/<root>/`）：`/待刮削` 是 staging 不是存储，Gap 账本是缺口的唯一持久记录；删除以 fresh 回读证明，无法证明时记为残余并可重跑 `consume-source`。

所有变更接口只接受同源本机页面请求。

## 补源与安全边界

视频补源固定按 `quark_share → magnet` 顺序进行。Torrent 仅下载已经逐缺口证明映射的成员，并使用 `--select-file`；Torrent 的 SHA-1 infohash 是协议字段，仍会保留。补源暂存目录唯一为：

```text
/quark/影视/ScrapeFlow/补源/<root-job-id>/<attempt-id>
```

夸克 Helper 只在 Compose 内部使用，按夸克接口协议完成必要的加解密；不会新增 ScrapeFlow 自定义加密。Helper token、cookie 和其他凭据不会写入状态、API 响应或日志。

系统保留的本地安全底线是：已有目标不覆盖、任务专属 staging、容量限制、崩溃后不重复提交、单 RootJob 暂停控制、一把本地写锁，以及写后路径和大小的 fresh listing 回读。

如果你想留备份，可在服务暂停时手工复制 `SCRAPEFLOW_HOST_STATE_ROOT`；这完全是可选操作，不是启动、运行或恢复任务的前提。

## 配置

`.env.local.example` 只列出本机连接凭据、状态目录、可选搜索来源和必要的媒体限制。无需配置 pilot、审计门禁、worker 数、发布版本、镜像 digest 或验收路径。

## 开发检查

```sh
python3 -m pytest local/tests -q
```

测试用于开发回归，不是用户部署前的操作门槛。
