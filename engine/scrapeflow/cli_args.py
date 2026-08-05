"""CLI parser and secret resolution, separate from engine orchestration."""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from pathlib import Path

from .errors import ScraperError


def read_secret_file(path: Path, label: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ScraperError(f"无法读取 {label} 文件: {path}; {exc}") from exc
    if not value:
        raise ScraperError(f"{label} 文件为空: {path}")
    return value


def resolve_password(args: argparse.Namespace) -> str:
    if args.password_file:
        return read_secret_file(args.password_file, "AList 密码")
    password = os.getenv("ALIST_PASSWORD")
    if password:
        return password
    if not sys.stdin.isatty():
        raise ScraperError("缺少 AList 密码，请设置 ALIST_PASSWORD 或使用 --password-file")
    return getpass.getpass("AList 密码: ")


def resolve_tmdb_key(args: argparse.Namespace) -> str:
    if args.tmdb_key_file:
        return read_secret_file(args.tmdb_key_file, "TMDB API Key")
    key = os.getenv("TMDB_API_KEY")
    if not key:
        raise ScraperError("缺少 TMDB API Key，请设置 TMDB_API_KEY 或使用 --tmdb-key-file")
    return key


def build_parser(*, version: str, default_alist_url: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TMDB 元数据刮削 + AList 安全重命名/移动工具",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("src", nargs="?", help="源目录（仅用于生成新计划）")
    parser.add_argument("--version", action="version", version=f"%(prog)s {version}")
    parser.add_argument("--parent", help="目标父目录；生成新计划时必须提供")
    parser.add_argument("--id", type=int, dest="tmdb_id", help="TMDB ID")
    parser.add_argument("--type", choices=["tv", "movie", "collection", "auto"], help="媒体类型")
    parser.add_argument("--auto-match", action="store_true", help="根据源目录名自动搜索并选择 TMDB 条目")
    parser.add_argument("--query", help="自动 TMDB 匹配使用的标题；默认取源目录名")
    parser.add_argument("--min-confidence", type=float, default=0.88, help="自动匹配最低置信度")
    parser.add_argument(
        "--wizard", action="store_true",
        help="生成并保存计划后，在同一进程中等待 SHA-256 确认并执行",
    )
    parser.add_argument("--season", type=int, help="电视剧季度；生成计划时默认为 1")
    parser.add_argument("--absolute", action="store_true", help="按绝对集数映射")
    parser.add_argument(
        "--auto-episode-mode", action="store_true",
        help="普通季度映射不成立时，仅在绝对集映射完整有效时自动切换",
    )
    parser.add_argument("--allow-unmapped", action="store_true")
    parser.add_argument("--prefer-simplified", action="store_true")
    parser.add_argument("--collection-map", type=Path)
    parser.add_argument("--episode-map", type=Path, help="源集数到 SxxExx 的显式 JSON 覆盖映射")
    parser.add_argument("--episode-group", help="绝对集数使用的 TMDB episode group ID")
    parser.add_argument("--allow-index-mapping", action="store_true")
    parser.add_argument(
        "--ignore-orphan-temp", action="store_true",
        help="忽略 .scraper-tmp-* 遗留条目；默认遇到即停止",
    )
    parser.add_argument("--search", help="搜索 TMDB；不连接 AList")
    parser.add_argument("--plan-json", type=Path, help="将新生成的计划保存为可校验 JSON；拒绝覆盖已有文件")
    parser.add_argument(
        "--execute-plan", type=Path,
        help="加载并执行已保存计划；必须同时提供 --execute 与计划 SHA-256",
    )
    parser.add_argument("--approve-plan-sha256", help="人工核对后批准的完整 64 位计划 SHA-256")
    parser.add_argument("--execute", action="store_true", help="仅配合 --execute-plan 使用")
    parser.add_argument("--no-dry-run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--skip-poster", action="store_true")
    parser.add_argument("--overwrite-poster", action="store_true")
    parser.add_argument(
        "--cleanup-empty-source", action="store_true",
        help="成功后通过 AList 空目录接口清理确认仍为空的源目录",
    )
    parser.add_argument("--journal", type=Path, help="执行日志路径；拒绝覆盖已有文件")
    parser.add_argument("--inspect-journal", type=Path, help="只读检查执行 journal 及其恢复摘要")
    parser.add_argument("--recover-journal", type=Path, help="根据失败的执行 journal 生成或执行恢复")
    parser.add_argument(
        "--approve-recovery-sha256",
        help="批准 --recover-journal 显示的完整 64 位 journal SHA-256",
    )
    parser.add_argument("--alist-url", default=os.getenv("ALIST_URL", default_alist_url))
    parser.add_argument("--username", default=os.getenv("ALIST_USERNAME", "admin"))
    parser.add_argument("--password-file", type=Path, help="仅包含 AList 密码的本地文件")
    parser.add_argument("--tmdb-key-file", type=Path, help="仅包含 TMDB API Key 的本地文件")
    parser.add_argument(
        "--allow-insecure-http", action="store_true",
        help="允许非环回地址使用明文 HTTP 连接 AList",
    )
    parser.add_argument("--language", default=os.getenv("TMDB_LANGUAGE", "zh-CN"))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--retries", type=int, default=3)
    return parser
