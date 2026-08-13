# ScrapeFlow 旧状态所有权只读审计（2026-08-09）

> 历史只读状态快照，仅供参考；不构成当前产品或工程合同。当前长期规则见根 `AGENTS.md`，当前实现事实以源码、测试和 Git 工作树核对。

采集时间：2026-08-09 15:03（Asia/Shanghai）。本报告只读取 compose/env、容器挂载、HTTP GET 投影和持久状态文件；未修改、删除、迁移或重放任何 job、gap、staging、AList 或正式媒体库对象，也未调用 pause/resume、retry、cleanup 或 audit run。唯一新增对象是本报告。

## 结论

旧状态不应直接恢复，也没有任何对象在本次审计中被认证为“可安全删除”。当前所有权关系可以静态追到 root，但 schema 明显早于 target-shelf start gate 和 gap fingerprint：

- 活动 `jobs/` 有 94 份有效 JSON：84 个 audit-owned public root、9 个 internal child、1 个普通/遗留 public root。
- 84 个 audit root 的 Engine 原始阶段都是 `executed`，但公共投影只有 2 个 `completed`；其余为 34 个 `gap_discovering` 和 48 个 `retry_wait`。`executed` 只是 Engine 写入事实，不是 root 工作流已收口的证据。
- 1121 份 gap 全部有现存 audit root owner，且目录 owner、JSON `job_id` 一致；但 399 份旧 `missing_episode` 状态使用会跨 root 重复的 `SxxEyy` 标识，和当前 job plan 的 385 个 episode gap ID 零交集。
- 82 个带 provider gap 的 root 均没有持久化 `provider_gap_fingerprint`，其 `replenishment` 也均没有 `gap_fingerprint`。不能声称同指纹终态保护已经覆盖这批旧 backlog。
- 本地 `staging/` 的 13 个一级目录都有现存 audit root owner，没有 job-level orphan；其中 8 个非空 attempt 均未被当前 gap 的 remote `staging_root` 精确引用，只能分类为“owner 存在、未引用的旧 attempt”，不能据此删除。
- AList 远端补源根只读可见 1 个 owner/attempt，owner 存在，attempt 与一个 `retry_wait` gap 精确匹配且当前为空；它不是 orphan。
- `engine-jobs/` 2 份和 `replenishment-batches/` 1 份是活动 `jobs/` 之外的 legacy projection。它们没有和活动 job ID 重叠，仍未迁移或归档。

## 持久状态根证据

| 层 | 值 | 证据 |
|---|---|---|
| compose 插值来源 | `SCRAPEFLOW_HOST_STATE_ROOT=/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/state` | `.env.local:1` |
| API 容器状态目录 | `/data` | `docker-compose.yml` 的 `SCRAPEFLOW_STATE_DIR=/data` |
| API host bind | `/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/state/scrapeflow-data -> /data` | `docker inspect scrapeflow-api-1` |
| AList host bind | `/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/state/alist-data -> /opt/alist/data` | `docker inspect scrapeflow-alist-1` |
| 本报告审计的活动状态根 | `/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/state/scrapeflow-data` | env、compose config 和实际容器 mount 三方一致 |

`docker-compose.yml` 中 `${SCRAPEFLOW_HOST_STATE_ROOT}` 是 compose-time 插值，service 的 `env_file: .env.local` 本身不会为 compose volume 插值赋值。因此复现 compose 配置必须显式使用 `docker compose --env-file .env.local ...`；不带 `--env-file` 的当前 shell 会报该变量缺失。

只读读取 `global-control.json` 和 `GET /api/control` 的结果一致：`paused=true`、`scheduler_paused=true`、`persistent=true`、`reason=shutdown`，更新时间为 `2026-08-09T02:44:00.625803Z`。本次审计没有解除或重写该栅栏。

## Jobs：root、child 与公共阶段

### 原始记录

| 类型 | 数量 | 原始 phase | 所有权/兼容性 |
|---|---:|---|---|
| audit-owned public root | 84 | `executed` 84 | 80 个可由现有正式 work path 推断为 `anime`，4 个推断为 `us_tv`；84 个均无显式 `target_shelf` |
| internal child | 9 | `executed` 8、`failed` 1 | 9 个均有现存 root、均出现在 root 的 `replenishment.child_jobs`，均无显式 `target_shelf` |
| ordinary/legacy public root | 1 | `cancelled` 1 | 无显式 `target_shelf`，也不能由持久 target root 唯一推断 shelf |
| 合计 | 94 | `executed` 92、`failed` 1、`cancelled` 1 | 94/94 JSON 可解析，文件 stem 与 JSON `id` 一致 |

路径推断仅用于旧状态分类，不等价于用户选择证据。94 个活动记录全部缺少新版持久 `target_shelf`；因此即使 93 个能从旧 work path 推断货架，也不应原地补写或自动迁移。

### 公共 root 投影

`GET /api/jobs` 隐藏 internal child，返回 85 个 public root：

| public phase | 数量 |
|---|---:|
| `completed` | 2 |
| `gap_discovering` | 34 |
| `retry_wait` | 48 |
| `cancelled` | 1 |

其中 84 个 audit root 的 `engine_phase` 仍全部为 `executed`。这解释了磁盘原始 phase 与 Web/API 阶段的差异：82 个 root 仍有 provider gap 投影，不能按 raw `executed` 当作 terminal cleanup 对象。

### Child 对账

9 个 child projection 和 9 份 child job JSON 一一对应，无重复、无 dangling root、无 projection-only child，且 projection phase 与 child JSON phase 全部一致：

| root | child 数 | child phase |
|---|---:|---|
| `audit-2d2b76ea5b954a39baa9710565dbf279` | 1 | `executed` |
| `audit-3003c8869615430281e15b5b78aa746b` | 1 | `executed` |
| `audit-544a088255134df3bf735eb70ccd5948` | 1 | `executed` |
| `audit-85cb1cf8596a4702b1b8eeaddd99e1f5` | 1 | `executed` |
| `audit-96ddf89e23d04e288cad94b5914b53dd` | 1 | `failed` |
| `audit-a3050d2421224efca239ad9767d8deea` | 1 | `executed` |
| `audit-d348123c992b492a88a72760b61d29e6` | 2 | `executed` 2 |
| `audit-f40da4cfae0a4586a1c81a64d681d1cd` | 1 | `executed` |

10 个 gap state 引用了 6 个 child ID，全部能解析到上述 child；另外 3 个 child 没有被当前 gap state 直接引用，但仍有正确 root projection，不能称为 orphan。9 个旧 projection 都有 `phase`，但 0 个带新版 `success` 字段，属于兼容性旧投影。

## Gaps：owner、status 与 fingerprint

### 数量与阶段

| kind | `retry_wait` | `resolved` | 合计 | owner |
|---|---:|---:|---:|---|
| `missing_episode` | 389 | 10 | 399 | 全部为现存 audit root |
| `missing_subtitle` | 722 | 0 | 722 | 全部为现存 audit root |
| 合计 | 1111 | 10 | 1121 | 82 个 gap owner 目录；0 个 child-owned、0 个 missing-job orphan |

其他结构校验：1121/1121 可解析，1121/1121 的父目录名等于 JSON `job_id`，同一 `(job_id, gap id)` 下无重复文件。

### Fingerprint 覆盖

| 层 | 有 fingerprint | 无 fingerprint | 说明 |
|---|---:|---:|---|
| root `summary.provider_gap_fingerprint` | 0 | 82 | 仅统计当前有 provider gap 的 root |
| root `summary.replenishment.gap_fingerprint` | 0 | 82 | 82 个 replenishment 均为 `terminal=false` |
| gap state JSON | 0 | 1121 | 旧 schema 没有单 gap/root-set fingerprint 字段 |

82 个 replenishment 的 status 为 `gap_discovering` 34、`retry_wait` 48。按当前源码的 canonical tuple 和 SHA-256 算法，可以从 82 个 current job plan 只读派生 82 个互不相同的 fingerprint；这些值没有持久化，且不是旧 attempt 的原始证据，不能回填后据此续跑。

### Job plan 与 gap-state 旧投影漂移

当前 85 个 public root 中，50 个 root 的 current plan gap ID 集合与 gap-state 集合一致（其中 3 个两边都为空），35 个存在漂移。聚合结果为：

| 集合 | 行数 |
|---|---:|
| current job plan provider gaps | 1107 |
| persisted gap states | 1121 |
| `(root, gap id)` 交集 | 722 |
| plan-only | 385 |
| state-only | 399（`retry_wait` 389、`resolved` 10） |

漂移完全落在 `missing_episode`：

- current plan 有 385 个 episode gap，带 `media`、`season`、`episode`，ID 为带作品身份的新版形式；
- persisted state 有 399 个 episode gap，ID 全是旧 `SxxEyy` token，带 `season` 但没有独立 `episode` 或 `media`；
- 两者 ID 交集为 0；旧 token 只有 84 个不同值，56 个 token 被多个 root 重用，同一 token 最多出现在 17 个 root；
- owner 目录仍能限定任务边界，但脱离 root 后的 `SxxEyy` 不是全局 fingerprint，不能按 ID 单独迁移、合并或删除；
- `missing_subtitle` 的 722 个 `(root, id)` 则与 current plan 722/722 精确一致，但它们同样没有持久 fingerprint 和显式 target shelf，仍不自动迁移。

因此这批 gap 的正确分类是“root-owned legacy projection”。其中 10 个 `resolved` 是历史终态记录；另有 389 个旧 episode `retry_wait` ID 与 current plan ID 不同。任何迁移都应先输出逐 root 的旧 token → current semantic tuple 映射并人工核对，而不是启动 worker 让运行时隐式重建。

## Staging：本地 workspace 与 AList 远端补源区

### 本地 `/data/staging`

| 分类 | 数量/大小 | 结论 |
|---|---|---|
| 一级 owner 目录 | 13 | 13/13 都对应现存 audit root；missing-job orphan 为 0 |
| 非空 owner | 5 | 共 8 个 `attempt-*` |
| 空 owner shell | 8 | owner 存在；只是当前没有本地 attempt 内容 |
| 当前 gap remote `staging_root` 精确引用的本地 attempt | 0 | 本地 workspace 与 remote staging 是不同位置；不能只凭“未引用”删除 |
| owner 存在但未引用的旧本地 attempt | 8 | 全部保留，列为 archive-review candidate，不是 safe-delete |
| 文件/目录 | 37 个文件、61 个目录 | 目录数不含 `staging/` 自身；无 symlink |
| 文件类型 | MKV 20、`.aria2` 8、`.torrent` 8、EXE 1 | 含未完成下载和未知可执行 payload，不能按普通临时文件清理 |
| apparent size | 6,299,853,119 bytes（约 5.87 GiB） | `.aria2`/稀疏预分配使 apparent size 大于实际占用 |
| allocated size | 3,763,970,048 bytes（约 3.51 GiB） | 与此前 `du` 口径的“约 3.5GB”一致 |

15 个 gap state 保存了 10 个不同 remote `staging_root`：10 个 `resolved` state 对应 6 个路径，5 个 `retry_wait` state 对应 4 个路径。这 10 个 remote attempt 均没有同名本地 workspace attempt；这不表示远端 orphan，也不授权删除本地旧 attempt。

### AList `/quark/影视/ScrapeFlow/补源`

通过 `GET /api/browse` 且未设置 `refresh=1` 做了限定在补源根内的只读列表：

- 远端补源根只有 1 个 owner：`audit-96ddf89e23d04e288cad94b5914b53dd`；该 job/root 存在。
- owner 下只有 `attempt-9becc5b21e2a4dc685c943ede4153160`，当前列表为空。
- 该 exact path 被 1 个 `retry_wait` gap state 的 `staging_root` 引用，并且也是上述 failed child 的 source root。
- 因而远端分类为“owned + referenced + empty”，不是 orphan；本次未清理。
- 其余 9 个持久 remote staging 引用在当前补源根列表中不可见。它们是历史引用缺失，不等于对应 gap 可删除或可自动重开。

本报告没有浏览正式媒体库内容，也没有读取或改写 AList 数据库文件。

## Legacy projection

| 路径 | 数量 | 与活动 `jobs/` 的关系 | 分类 |
|---|---:|---|---|
| `engine-jobs/` | 2 JSON | 两个 ID 均不在活动 `jobs/` | legacy-only projection |
| `replenishment-batches/` | 1 JSON | `phase=cleaned`；其 `child_job_id` 指向上述一个 legacy engine job | legacy batch projection |
| `journals/` | 0 | 空目录 | 空 legacy shell |

这些记录彼此可对账，但不是活动 root/child 图的一部分。它们只能在停服备份、生成 checksum/映射、明确获得归档许可后移动到一次性 legacy archive；本次没有移动，也没有给出物理删除清单。

## 治理分类

| 分类 | 当前对象 | 本次结论 |
|---|---|---|
| 保留 | 94 个 active job、1121 个 gap、远端 referenced attempt | owner 存在但旧 schema/非终态投影未收口，保持 pause 原样 |
| 人工映射后再决定 | 399 个旧 episode gap state、9 个 child projection | episode ID/fingerprint 与新版不兼容；child projection 缺 `success` |
| archive-review candidate | `engine-jobs/` 2、`replenishment-batches/` 1、本地未引用 attempt 8 | 仅候选；先停服备份、hash、逐 owner 清单和明确批准 |
| safe-delete | 无 | 本次没有对象达到可安全删除证据门槛 |

## 可复现的只读命令

以下命令只读；不要把其中任何 `GET` 改成 `POST`，也不要在审计期间运行 retry/cleanup/audit-run：

```bash
SCRAPEFLOW_AUDIT_HOST_ROOT="$(sed -n 's/^SCRAPEFLOW_HOST_STATE_ROOT=//p' .env.local)"
SCRAPEFLOW_AUDIT_DATA_ROOT="$SCRAPEFLOW_AUDIT_HOST_ROOT/scrapeflow-data"

docker compose --env-file .env.local config --format json |
  jq '{api:(.services.api.volumes[]|select(.target=="/data")),alist:(.services.alist.volumes[]|select(.target=="/opt/alist/data"))}'

docker inspect --format '{{.Name}}{{range .Mounts}}{{println}}{{.Destination}} <- {{.Source}}{{end}}' \
  scrapeflow-api-1 scrapeflow-alist-1

jq '.' "$SCRAPEFLOW_AUDIT_DATA_ROOT/global-control.json"
curl -fsS http://127.0.0.1:3010/api/control | jq '.'

jq -s 'group_by(.phase) | map({phase:.[0].phase,count:length})' \
  "$SCRAPEFLOW_AUDIT_DATA_ROOT"/jobs/*.json

find "$SCRAPEFLOW_AUDIT_DATA_ROOT/gaps" -mindepth 2 -maxdepth 2 -type f -name '*.json' -print0 |
  xargs -0 jq -r '[.gap.kind,.phase] | @tsv' | sort | uniq -c

find "$SCRAPEFLOW_AUDIT_DATA_ROOT/staging" -mindepth 1 -type f -print | wc -l
find "$SCRAPEFLOW_AUDIT_DATA_ROOT/staging" -mindepth 1 -type d -print | wc -l
du -sk "$SCRAPEFLOW_AUDIT_DATA_ROOT/staging"

curl -fsS --get --data-urlencode 'path=/quark/影视/ScrapeFlow/补源' \
  http://127.0.0.1:3010/api/browse | jq '{path,directories,files}'
```

要复现所有权矩阵，建议使用一个只向 stdout 输出 JSON 的 stdlib-only Python 脚本，保持以下结构，不导入或实例化 ScrapeFlow application/runtime：

1. 用 `Path.glob` + `json.loads` 建立 `jobs_by_id`；以 `summary.internal_child`/`summary.root_job_id` 分类 child，其余按 `summary.audit_owned` 分类 root。
2. 遍历 `gaps/*/*.json`，验证 `path.parent.name == row["job_id"]`，再按 `(owner, id)`、`gap.kind`、`phase` 聚合。
3. 分别取 job `plan.scan_report.resource_gaps` 和 gap-state `gap`，输出 per-root ID set 的 intersection/plan-only/state-only；fingerprint 只标记为 `persisted` 或 `derived`，不要把派生值回写。
4. 遍历 `staging/<owner>/<attempt>`，用 owner 是否在 `jobs_by_id` 和 gap `staging_root` 是否以 `/<owner>/<attempt>` 结尾分类；只读取 `lstat`，不删除、不 touch、不跟随 symlink。
5. 对 `engine-jobs/`、`replenishment-batches/`、`journals/` 单独统计，不能并入活动 `jobs/`。
6. 在起止各做一次 JSON 内容 digest 和 staging metadata digest；任何变化都使报告标记为 concurrent/unstable，而不是继续迁移判断。

## 快照与局限

采集起点的只读快照：

- 1219 个控制/job/gap/legacy JSON，共 4,469,299 bytes，按相对路径和内容串联后的 SHA-256 为 `359be471eb10a62665b4d0ef43b2823d5efd680c73d09a1ba5c9fe1cd7dc5312`。
- `staging/` 下 98 个条目（文件和目录）的 path/mode/size/mtime metadata SHA-256 为 `b66ad451180e42efd4165d7b5343b6bc5750dbffb90be9c0525f420dfd7729ad`。

报告写入后再次计算，两项 digest、文件数、字节数和条目数均与采集起点完全一致；`GET /api/control` 也仍返回同一 paused 状态和更新时间。因此本次采集窗口内没有观察到被审计状态的内容或 staging metadata 变化。

局限：

- JSON digest 不包含 `library-audit/`、AList DB、媒体文件内容或本地 staging 大文件内容；staging digest 只覆盖 metadata，不证明 payload 内容 hash。
- AList 证据是一次当前目录列表，不是历史存在性证明；只检查补源根，没有遍历正式媒体库。
- work path → shelf 只是静态推断，不能替代缺失的用户 `target_shelf` 选择记录。
- 派生 gap fingerprint 只描述当前 job plan，不证明旧 gap state/attempt 曾使用同一算法。
- 本报告是保留与后续人工映射的依据，不是 resume、migration、archive 或 delete 授权。
