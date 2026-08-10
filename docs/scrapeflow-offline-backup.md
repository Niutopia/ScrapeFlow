# ScrapeFlow 离线备份与恢复演练

本工具只用于停服后的手工备份演练。它复制本机两层活动状态：

- AList `alist-data`
- ScrapeFlow `/data`

正式媒体库不由脚本复制。启用真实自动写入前，必须另行准备存储侧快照或可恢复副本，并在命令中用 `--media-snapshot-note` 记录位置、时间或编号。

## 前置条件

1. 全局 pause。
2. 等待 writer、Provider、audit 空闲。
3. 停 API。
4. 停 AList。
5. 准备正式媒体库的外部恢复点。

## 创建备份

```sh
python3 scripts/scrapeflow_offline_backup.py create \
  --alist-data "$SCRAPEFLOW_HOST_STATE_ROOT/alist-data" \
  --scrapeflow-data "$SCRAPEFLOW_HOST_STATE_ROOT/scrapeflow-data" \
  --output-dir /path/to/offline-backups \
  --media-snapshot-note "media snapshot: provider-console-id-or-path" \
  --label "2026-08-10-before-provider"
```

命令会拒绝未 pause 的 ScrapeFlow 状态，并写出
`scrapeflow-offline-backup.json`。该 manifest 只记录路径、文件数、目录数、总字节数、JSON 解析结果和 SQLite `PRAGMA quick_check` 结果。

## 验证备份

```sh
python3 scripts/scrapeflow_offline_backup.py verify \
  /path/to/offline-backups/2026-08-10-before-provider
```

验证只读取备份目录，不访问生产 AList、`/data` 或正式媒体库。

## 隔离恢复

```sh
python3 scripts/scrapeflow_offline_backup.py restore \
  /path/to/offline-backups/2026-08-10-before-provider \
  --restore-dir /path/to/isolated-restore
```

恢复目录必须为空。恢复后的 `scrapeflow-data` 仍必须读取为 paused；隔离实例启动前也必须保持所有自动 gate 关闭，不恢复旧 backlog，不批量 retry，不批量 cleanup。

## 不做的事

- 不访问或修改正式媒体库。
- 不建设后台任务、保留策略或定时器。
- 不生成媒体内容指纹清单。
- 不迁移旧状态 schema。
- 不自动启动隔离实例。
