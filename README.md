# ScrapeFlow

ScrapeFlow 是 AList + TMDB 媒体整理器的 Web 控制台。当前版本提供五套可切换的完整界面方案，用于确定最终视觉方向；所有方案共享“输入 AList 路径 → 识别媒体 → 生成安全计划 → 审核后执行”的工作流。

## 目录

- `app/`：Web 端界面与交互。
- `engine/`：经过 99 项测试的 Python 刮削、解压与整理核心。
- `public/`：站点品牌资源。
- `.openai/hosting.json`：Sites 托管配置。

## 五套方案

1. 深海控制台：深色专业运维风。
2. 纸张编辑部：暖色编辑表单风。
3. 极光玻璃：影音应用式玻璃拟态。
4. 终端机：高密度等宽字符界面。
5. 媒体便当：明亮 Bento 卡片布局。

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
