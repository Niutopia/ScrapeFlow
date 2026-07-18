# ScrapeFlow

ScrapeFlow 是只在本机运行的 AList + TMDB 媒体整理应用。Web 页面负责输入路径、查看实时日志、审核计划和批准执行；本地 Python 服务负责调用 `engine/`，不会由托管网页远程启动电脑上的脚本。

## 功能流程

1. 输入单个媒体目录或完整 AList 链接。
2. 检查 `.7z.001`、`.zip.001`、`part01.rar` 等分卷和密码标记。
3. 如有压缩包，显示解压计划；批准后通过 AList 在服务器端解压，默认保留原压缩包。
4. 自动匹配 TMDB，生成季度、集数、字幕、移动和重命名计划。
5. 显示计划 SHA-256 和逐文件路径；再次批准后才执行。
6. 返回实时日志、journal 和最终校验结果。

## 本地配置

复制 `.env.local.example` 为 `.env.local`，填写本机凭据：

```sh
cp .env.local.example .env.local
```

`.env.local` 已被 Git 忽略。AList 密码、TMDB Key 和归档密码不会进入网页、任务计划、日志或版本库。

## 启动

```sh
npm install
npm run local
```

然后打开终端显示的本地地址，默认是 `http://127.0.0.1:3000`。本地 API 只监听 `127.0.0.1:8765`，且只接受 localhost 页面访问。

## 验证

```sh
npm test
PYTHONDONTWRITEBYTECODE=1 PYTHONWARNINGS='error::ResourceWarning' \
  python3 -m unittest discover -s engine/tests
```
