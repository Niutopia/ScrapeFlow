# ScrapeFlow Engine

Engine 是 ScrapeFlow 的媒体领域实现。`engine/scraper.py` 提供媒体计划、执行和恢复 CLI；`engine/tools/extract_archives.py` 提供归档计划与执行。Local API 使用其中的缺口补源、字幕获取和 OCR 原语组成“当前作品复核 → 补源 → 到盘回刮 → 当前作品再复核”闭环。全库审计与规划只用于显式启动的一次性专项或手工只读排查，不是常驻服务。

本文只描述 Engine 能力，不代表媒体库、任务或部署的实时状态。实时状态请查看 ScrapeFlow Web/API。

## 依赖

- Python 3.10+；媒体规划核心仅使用标准库。
- AList 原生安全解压至少需要 v3.57.0。
- 本地归档后备可能需要 `7z` 或 `7zz`。
- 补源和字幕核验按当前线路使用 `aria2c`、`ffmpeg` 和 `ffprobe`。
- 烧录字幕 OCR 的宿主机依赖见 `engine/requirements-ocr.txt`。OCR 能力属于闭环，但只在常规字幕证据不足时按需启动。

查看 Engine CLI 版本与参数：

```sh
python3 engine/scraper.py --version
python3 engine/scraper.py --help
python3 engine/tools/extract_archives.py --help
```

以下命令均从项目根目录执行。

## 凭据

通过环境变量或权限受限的密码文件提供凭据：

```sh
export ALIST_PASSWORD='...'
export TMDB_API_KEY='...'
```

支持 `--password-file`、`--tmdb-key-file` 和归档工具的 `--archive-password-file`。不要使用进程参数传递明文密码，也不要把凭据写入计划或 journal。

## 媒体计划

跨作品目录层级只由 `engine/scrapeflow/canonical_work_tree.py` 规划。身份仅使用 TMDB namespace + ID：同 ID 的多清晰度/发行版必须共用作品叶子，不同 ID 必须保持独立叶子且不得伪装成同一剧集的季。`engine/scraper.py` 中的批次、混合 TV/电影、TMDB 合集和同身份去重只能经 `_plan_canonical_batch_tree` 这一 adapter 改写作品根。一次性工具只消费已封存 target，不得根据标题重新规划作品树。

生成并保存计划：

```sh
python3 engine/scraper.py \
  --alist-url http://127.0.0.1:5244 \
  --parent '/quark/影视/番剧' \
  --type tv --id 100565 --season 1 \
  --plan-json plans/show.json \
  '/quark/影视/待刮削/来源目录'
```

也可使用 `--type auto --auto-match`，或先用 `--search` 查询。绝对集数和特殊发行应优先使用 `--episode-group` 或显式 `--episode-map`，而不是依赖文件排序猜测。

执行前核对计划内容和命令输出的完整 SHA-256：

```sh
python3 engine/scraper.py \
  --alist-url http://127.0.0.1:5244 \
  --execute-plan plans/show.json \
  --approve-plan-sha256 '<64-hex-sha256>' \
  --journal journals/show.json \
  --execute
```

执行器会重新扫描来源并验证计划摘要、源身份、目标冲突、远端锁和临时对象。实时扫描产生的临时计划不能直接执行。Local API 可以自动执行唯一、无冲突的计划，但仍在内部绑定完整摘要；CLI 的显式摘要参数用于手工诊断和恢复。

## 归档计划

生成只读计划：

```sh
python3 engine/tools/extract_archives.py \
  --alist-url http://127.0.0.1:5244 \
  --plan-json plans/archive.json \
  '/quark/影视/待刮削/来源目录'
```

审核分卷连续性、成员、路径和摘要后执行：

```sh
python3 engine/tools/extract_archives.py \
  --alist-url http://127.0.0.1:5244 \
  --execute-plan plans/archive.json \
  --approve-plan-sha256 '<64-hex-sha256>' \
  --journal journals/archive.json \
  --execute
```

归档密码只保留在当前进程内存。原归档默认保留；AList 任务丢失、成员身份变化或目标冲突会失败关闭。

## 恢复

先只读检查失败 journal：

```sh
python3 engine/scraper.py --inspect-journal journals/show.json
```

确认恢复摘要后，再使用 `--recover-journal`、`--approve-recovery-sha256` 和 `--execute`。不要手工删除远端锁、临时对象或 journal 来跳过恢复检查。

## 执行底线

Engine 与调用它的 Local API 必须同时保证：

1. 源/目标路径在允许边界内且不重叠。
2. TMDB 身份或季集映射不唯一时不自动执行。
3. 每次写入前重读 AList 并重验来源身份。
4. 目标同名对象存在时不覆盖。
5. 执行计划与完整 digest 严格绑定。
6. 重命名使用唯一临时名，重叠路径使用锁和串行写入。
7. 写操作逐步记录 journal，中断后只在远端现状可证明时续执行或回滚。
8. 非正片残留只经 `residual_policy.py` 分类；字幕延后到精确视频级闭包，未知文件保留。可删项与正片移动一同纳入 `hybrid_remote_transaction.py` 封存批次：先在夸克事务回滚区建立并全量 SHA-256 回读验证恢复副本，再精确删除源文件；只有 Local 普通作品验收才能提交并释放远程回滚副本。
9. 重试必须幂等，不得再次移动已成功文件。

## 代码边界

- `engine/scraper.py`：媒体计划、执行和恢复 CLI 入口；
- `engine/scrapeflow/canonical_work_tree.py`：跨作品身份叶子、主作品根和系列容器的唯一规划权威；
- `engine/scrapeflow/residual_policy.py`：非正片残留的唯一纯分类入口；
- `engine/scrapeflow/remote_delete_transaction.py`：仅保留用于历史本机隔离事务恢复兼容；生产主链的删除统一使用 `hybrid_remote_transaction.py`；
- `engine/scrapeflow/models.py`、`errors.py`、`serialization.py`：领域模型、稳定错误和规范序列化；
- `engine/scrapeflow/clients/`：有界、重试和脱敏的 HTTP 传输；
- `engine/scrapeflow/one_time_library_completion.py`：一次性全库专项使用的纯规划原语，不由正式 API 调度；
- `engine/scrapeflow/replenishment_*`、`quark_*`：候选获取与云端桥；
- `engine/scrapeflow/subtitle_*`、`burned_in_subtitle_ocr.py`：字幕证据、发现、获取和 OCR 策略；
- `engine/tools/`：API 运行工具与显式运维入口。

不要把任意 `engine/tools/*.py` 视为稳定公共 API。自动化应优先调用主 CLI 或 Local API 已使用的契约入口。

## 兼容原则

- Engine CLI 默认只生成计划；执行必须加载已保存计划并提交完整 SHA-256。
- 明文 `--password` 和 `--tmdb-key` 参数不受支持。
- 无法可靠映射的绝对集数、小数集数或特别篇不会按顺序猜测。
- 原压缩包和非空来源目录默认保留。
- 计划 schema 的兼容范围以 `engine/scraper.py` 中的常量和加载校验为准。

## 测试

只运行 Engine 测试：

```sh
PYTHONDONTWRITEBYTECODE=1 \
PYTHONWARNINGS='error::ResourceWarning' \
python3 -m unittest discover -s engine/tests -v
```

项目完整验证使用：

```sh
npm run check
```
