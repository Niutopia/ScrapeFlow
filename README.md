# ScrapeFlow

ScrapeFlow 是 AList + TMDB 媒体整理器的 Web 控制台。当前版本从 Infuse、Apple TV、群晖与 Emby 的产品语言出发，提供五套结构、导航和操作方式都不同的独立界面；所有方案共享“输入 AList 路径 → 识别媒体 → 生成安全计划 → 审核后执行”的工作流。

## 目录

- `app/`：Web 端界面与交互。
- `engine/`：经过 99 项测试的 Python 刮削、解压与整理核心。
- `public/`：站点品牌资源。
- `.openai/hosting.json`：Sites 托管配置。

## 五套方案

1. `/ui/01` 沉浸式任务卡：借鉴 Infuse 的全屏媒体艺术与极简控制。
2. `/ui/02` 影院式工作台：借鉴 Apple TV 的内容层次与横向浏览体验。
3. `/ui/03` 媒体文件管家：借鉴群晖的文件管理效率与可靠状态反馈。
4. `/ui/04` 媒体服务器中心：借鉴 Emby 的侧栏结构与服务端控制效率。
5. `/ui/05` ScrapeFlow Cinema：融合影音沉浸和专业刮削控制的原创方向。

## 当前交互边界

设计选择阶段的“开始刮削”使用安全演示数据，不会修改 AList。选定风格后，将增加只运行在本机的桥接服务，由它调用 `engine/scraper.py`；托管页面本身不能直接启动用户电脑上的 Python 进程。

## 本地运行

```sh
npm install
npm run dev
```

## 验证

```sh
npm test
PYTHONDONTWRITEBYTECODE=1 PYTHONWARNINGS='error::ResourceWarning' \
  python3 -m unittest discover -s engine/tests
```
