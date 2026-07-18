# ScrapeFlow

ScrapeFlow 是 AList + TMDB 媒体整理器的 Web 控制台。当前版本提供五套结构、导航和操作方式都不同的独立界面，用于确定最终产品方向；所有方案共享“输入 AList 路径 → 识别媒体 → 生成安全计划 → 审核后执行”的工作流。

## 目录

- `app/`：Web 端界面与交互。
- `engine/`：经过 99 项测试的 Python 刮削、解压与整理核心。
- `public/`：站点品牌资源。
- `.openai/hosting.json`：Sites 托管配置。

## 五套方案

1. `/ui/01` 深海控制台：三栏任务中心，适合批量管理与长期运行。
2. `/ui/02` 纸张编辑部：单页线性表单，适合清晰、低学习成本的操作。
3. `/ui/03` 极光向导：沉浸式分步流程，适合一次只聚焦一个决定。
4. `/ui/04` 终端机：命令行工作台，适合高密度信息与高级用户。
5. `/ui/05` 媒体便当：模块化彩色卡片，适合触控和家庭媒体中心。

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
