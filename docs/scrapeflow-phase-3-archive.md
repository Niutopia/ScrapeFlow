# ScrapeFlow 阶段 3：轻量归档安全内核

本阶段把归档处理收敛为 `engine/scrapeflow/archive.py` 的两个边界：
`ArchiveInspector` 负责有界魔数、分卷、密码候选、7-Zip `-slt` listing、成员路径和展开预算；
`ArchiveExtractor` 只把已选择的视频/字幕写入任务自有 staging，并回读路径、大小、链接状态，最后做视频
`ffprobe` 或字幕内容校验。

当前实现支持普通 ZIP、7z、RAR 及 `.7z.001`、`.zip.001`、`.partNN.rar` 连续分卷。旧式 `.r00` 仅在
`media_policy` 中作为审计残留分类，活动归档 inspector 会显式返回不支持；不会默默把不完整的旧式分卷交给
7-Zip。

伪装的 `.exe`、`.bin`、`.dat` 只读取有限前缀：ZIP/7z/RAR 魔数优先，明确 MKV/MP4 才作为媒体，未知或普通
MZ 失败。代码没有执行输入文件的路径。成员拒绝绝对路径、点段、链接、等价名称冲突、7-Zip 通配符/列表文件
标记及超出成员、深度、展开量、比例、归档大小或磁盘余量的输入；嵌套归档返回
`nested_archive_unsupported`。

密码只在当前调用栈内存中使用，候选有界且显式提示冲突即停止；7-Zip 通过 `-p*` 从 stdin 读取密码。listing
和 extraction projection 只保留 `password_source`，不会持久化密码或命令输出。远端入口复用现有 AList
`list`、`read_file_prefix` 和 `download_file_to_path`，在下载前完成魔数、总大小和 staging 磁盘预算检查。

API 镜像增加 Debian `7zip` 运行时依赖。归档失败时源文件和任务 staging 保留；本阶段不包含正式库写入、远端
事务、receipt、digest、nonce 或 journal。

验证：

```text
python3 -m unittest local.tests.test_archive_domain -v
```

归档普通入站与 Provider 的组合接入由阶段 4 文档和共享 preprocessing adapter 继续负责；全局暂停保持不变。
