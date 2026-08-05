# 宿主机状态备份

ScrapeFlow 只使用一个项目根 `/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/`。Docker 状态由其中的 `state/` bind mount 持有：AList 使用 `state/alist-data/`，API 使用 `state/scrapeflow-data/`；快照写入独立的 `backups/`。这两个目录均排除在 Git 和 Docker 构建上下文之外。`scripts/backup_host_state.py` 为该状态根生成失败关闭的一致快照。

该快照只保护数据库、任务、journal 和 manifest，不复制媒体正文。尚未通过严格作品验收的媒体恢复正文位于 AList/夸克 `/quark/影视/ScrapeFlow/事务回滚`；本机只保留正在处理的单文件缓冲。状态快照中的事务 manifest 与远端恢复副本必须同时可用，才能执行媒体 restore。两者不能互相冒充。

本文说明备份契约，不声明计划任务是否已经安装或最近一次备份是否成功。运行状态应通过脚本输出、API 和备份目录中的 manifest 核验。这个 manifest 是备份完整性契约，用于证明快照内容与恢复结果可复验，必须保留。

## 保证

备份只在以下条件同时成立时被接受：

- `global-control.json` 中唯一权威的持久暂停已生效，由它派生的工作器状态也已停止新派发；
- 媒体变更、恢复执行和补源搜索均已静默；
- AList SQLite 可通过 backup API 复制并通过完整性检查；
- 状态文件复制前后稳定，整树指纹没有变化；
- 备份结束后成功恢复调用前的暂停状态。

任一条件不成立时，命令退出失败且不发布快照。脚本不会通过直接改写控制文件来模拟 API 已暂停。

## 协调流程

1. 获取备份根下的进程锁，拒绝并发备份。
2. 处理上次异常退出留下的 `.pause-journal.json`，且不覆盖其他操作者的新暂停原因。
3. 记录原始 `global-control.json` 字节和 API 状态。
4. 若系统原本未暂停，通过 `POST /api/control/pause` 建立持久暂停；若原本已暂停，保持原状态。
5. 等待媒体/恢复执行相和补源搜索 slot 静默，并持续复核 API 与持久文件中的暂停没有被解除。
6. 使用 SQLite backup API 复制 AList 数据库，稳定复制 API 状态树，并比较复制前后指纹。
7. 将带 `manifest.json` 的时间戳快照原子落盘。
8. 仅当本次主动建立暂停时，通过 API 恢复；异常路径也执行同一恢复逻辑。

暂停阻止新派发，但不会强行中断已经开始的远程动作，因此静默等待不可省略。

## 只读检查

先确认脚本看到的状态根、API、暂停一致性和活动工作：

```sh
python3 scripts/backup_host_state.py \
  --state-root '/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/state' \
  --backup-root '/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/backups' \
  --api-url http://127.0.0.1:3010 \
  --check
```

`--check` 不暂停、不复制、不清理快照。

## 创建快照

```sh
python3 scripts/backup_host_state.py \
  --state-root '/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/state' \
  --backup-root '/Users/niutopia/文件/Codex WorkSpace/ScrapeFlow/backups' \
  --api-url http://127.0.0.1:3010 \
  --quiesce-timeout 600 \
  --retention 14
```

`--state-root` 必须与 Docker Compose 的 `SCRAPEFLOW_HOST_STATE_ROOT` 指向同一目录。脚本直接访问绑定在 loopback 的 API；`--api-url` 应保持为 `127.0.0.1` 或等价的本机回环地址。

快照 manifest 记录来源、归档摘要、协调暂停、静默证据、恢复方式和恢复结果。只有 manifest 表明恢复成功且归档摘要可复验时，快照才可用于恢复演练。

## LaunchAgent 模板

仓库提供 `scripts/com.scrapeflow.host-state-backup.plist.example`。它只是模板；安装前必须检查其中的 Python 路径、项目路径、状态根、备份根、API 端口和触发时间。

安装示例：

```sh
cp scripts/com.scrapeflow.host-state-backup.plist.example \
  ~/Library/LaunchAgents/com.scrapeflow.host-state-backup.plist
launchctl unload ~/Library/LaunchAgents/com.scrapeflow.host-state-backup.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.scrapeflow.host-state-backup.plist
launchctl list | grep scrapeflow.host-state-backup
```

模板把标准输出和错误写入配置的日志路径。日志出现并不等于快照有效；仍需检查退出结果、快照 manifest 和暂停恢复状态。

## 验证与故障处理

- API 不可达、控制状态不一致、静默超时或复制期间状态变化都会拒绝快照。
- 系统原本已暂停时，备份不得发出 resume，也不得改变控制文件字节。
- 系统原本运行时，成功或失败后都应恢复原状态；硬杀或断电由下一次运行使用 pause journal 恢复。
- 保留策略只处理符合自动快照命名和 manifest 约束的目录，其他手工创建的目录不参与自动轮转。
- 定期在隔离位置执行恢复演练；不要用 live 状态根验证恢复。
