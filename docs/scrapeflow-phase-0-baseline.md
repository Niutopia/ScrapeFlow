# ScrapeFlow 阶段 0 源码迁移基线

> 记录时间：2026-08-08
>
> 范围：源码迁移 checkpoint 前的只读快照；不代表正式媒体库已完成整理。

## 运行态快照

- AList、API、Gateway 三个 Compose 服务均为 healthy。
- 全局控制状态为持久 `paused: true`，原因是接入伪装压缩包安全识别与 staging 解包；本阶段未解除暂停，也未调度、清理或修改媒体库。
- 公共根任务：85 个；其中 82 个仍为活动/补源投影、2 个已完成、0 个失败。正式库 writer、Provider worker 与 audit worker 均为 0。
- 最近一次全库审计已完成：7,752 个文件、4,175 个视频、2,704 个字幕；1,107 个缺口、723 个未知项、39 组疑似重复、6 个空目录。它仍是报告态，`clean` 和 `library_complete` 均为 false。
- 当前应用状态目录约 3.5 GiB；运行期临时目录约 72 MiB。它们均保持在 Git 忽略范围内。

## 源码边界与验证

- 此 checkpoint 仅包含当前迁移的源码、测试和文档；不包含 `state/`、`backups/`、`.runtime/`、`.scrapeflow/`、真实环境文件、媒体文件或构建产物。
- `git diff --check` 已通过；未跟踪项均为源码、测试或文档。
- `npm run check` 已通过：ESLint、TypeScript 类型检查、210 个 Python 回归测试和 Web production build 均成功。

## 后续约束

- 阶段 1 开始前后都保持全局暂停，直到计划规定的受控样本验收完成且用户明确恢复。
- `SCRAPEFLOW_START_PAUSED` 的默认值不能替代现有持久控制状态；阶段 1 会把控制文件缺失、损坏和正常 shutdown 的语义收敛为 fail-closed。
- 本提交是可回退的迁移基线，不是“本地稳定版”或可恢复队列的声明。
