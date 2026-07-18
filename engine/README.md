# AList + TMDB 媒体整理器 v3.3.2

面向 AList 网盘媒体库的安全整理与 TMDB 刮削工具。视频和字幕的重命名、移动、建目录均通过 AList API 在服务端完成；只有海报等小型元数据需要上传，不会把视频下载到本机再传回。

要求 Python 3.10 或更高版本，仅使用标准库。压缩包自动解压要求 AList v3.57.0 或更高版本；`.7z.001` 多分卷建议使用 v3.62.0 或更高版本。

## 已实现能力

- 电视剧、电影、电影合集与自动 TMDB 匹配。
- 标准目录和文件名、绝对集数、TMDB episode group、显式集数映射。
- `SxxExx`、多集文件、特别篇、字幕语言、版本和电影 edition 识别。
- `11.5`、`18.5` 等小数回顾篇必须显式映射，避免误并入正片。
- Infuse 图稿：`folder.jpg`、`poster.jpg`、`fanart.jpg`、季度海报。
- `.7z.001`、`.zip.001` 和 `part01.rar` 自动解压；校验分卷连续性、归档成员、目标冲突和解压结果。
- 自动读取归档同目录的 `密码：实际密码`、`解压密码：实际密码` 标记；标记可以是文件或文件夹。
- 密码只保存在当前进程内存，不进入计划、journal 或终端输出。
- 所有写操作采用“生成计划 → 审核 SHA-256 → 执行”的两阶段流程。
- 源快照、目标碰撞检查、远端任务锁、执行 journal、失败恢复和最终复核。

## 凭据

不要把密码或 TMDB Key 写在命令行中，也不要提交到仓库。推荐临时环境变量或权限为 `0600` 的密码文件：

```sh
export ALIST_PASSWORD='...'
export TMDB_API_KEY='...'
```

AList 默认用户名为 `admin`。也可使用 `--password-file` 和 `--tmdb-key-file`。归档密码可使用 `--archive-password-file`；若未提供，工具会先查找同目录密码标记，再安全地交互询问。

## 1. 自动解压有密码分卷

先生成只读计划：

```sh
python3 tools/extract_archives.py \
  --alist-url http://127.0.0.1:5244 \
  --plan-json plans/archive-v1.json \
  '/quark/影视/番剧/待整理目录'
```

核对归档、成员、目标路径与输出的 SHA-256 后执行：

```sh
python3 tools/extract_archives.py \
  --alist-url http://127.0.0.1:5244 \
  --execute-plan plans/archive-v1.json \
  --approve-plan-sha256 '<完整 SHA-256>' \
  --journal journals/archive-v1.json \
  --execute
```

解压成功后默认保留原压缩包，便于回退和补种。脚本不会自动删除分卷或密码标记。

## 2. 生成 TMDB 整理计划

指定条目：

```sh
python3 scraper.py \
  --alist-url http://127.0.0.1:5244 \
  --parent '/quark/影视/番剧' \
  --type tv --id 100565 --season 1 \
  --episode-map episode-map.json \
  --plan-json plans/show-v1.json \
  '/quark/影视/番剧/待整理目录'
```

也可使用 `--type auto --auto-match` 自动匹配，或先用 `--search` 查询。长篇番剧可使用 `--absolute`，优先配合 `--episode-group` 或显式 `--episode-map`。

集数覆盖文件示例：

```json
{
  "11.5": "S00E02",
  "18.5": "S00E04",
  "24": "S02E01"
}
```

## 3. 审核并执行整理

```sh
python3 scraper.py \
  --alist-url http://127.0.0.1:5244 \
  --execute-plan plans/show-v1.json \
  --approve-plan-sha256 '<完整 SHA-256>' \
  --journal journals/show-v1.json \
  --execute
```

执行前会重新扫描源文件并验证大小、修改时间和可用哈希。目标已存在、源发生变化、计划被改动、存在遗留锁或临时文件时都会停止。

失败后先只读检查 journal：

```sh
python3 scraper.py --inspect-journal journals/show-v1.json
```

按输出的恢复摘要审核后，再使用 `--recover-journal` 与 `--approve-recovery-sha256`。不要在不清楚远端现状时手工删除 `.scraper-lock-*` 或 `.scraper-tmp-*`。

## 测试

```sh
PYTHONDONTWRITEBYTECODE=1 \
PYTHONWARNINGS='error::ResourceWarning' \
python3 -m unittest discover -s tests -v
```

v3.3.2 当前包含 99 项单元与回归测试。
