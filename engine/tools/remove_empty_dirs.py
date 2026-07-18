#!/usr/bin/env python3
"""仅删除经 AList 刷新检查后确认为空的目录。"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import AListClient, DEFAULT_ALIST_URL, ScraperError, normalize_remote_path  # noqa: E402


def resolve_password(path: Path | None) -> str:
    if path:
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ScraperError(f"无法读取密码文件 {path}: {exc}") from exc
        if not value:
            raise ScraperError(f"密码文件为空: {path}")
        return value
    value = os.getenv("ALIST_PASSWORD")
    if value:
        return value
    if not sys.stdin.isatty():
        raise ScraperError("缺少 ALIST_PASSWORD")
    return getpass.getpass("AList 密码: ")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--alist-url", default=os.getenv("ALIST_URL", DEFAULT_ALIST_URL))
    parser.add_argument("--username", default=os.getenv("ALIST_USERNAME", "admin"))
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--allow-insecure-http", action="store_true")
    args = parser.parse_args()

    try:
        alist = AListClient(
            args.alist_url,
            args.username,
            resolve_password(args.password_file),
            allow_insecure_http=args.allow_insecure_http,
        )
        alist.login()
        empty: list[str] = []
        nonempty: list[str] = []
        for path in args.paths:
            normalized = normalize_remote_path(path)
            if normalized == "/":
                raise ScraperError("拒绝检查或删除 AList 根目录")
            content = alist.list(normalized, refresh=True)
            (empty if not content else nonempty).append(normalized)

        for path in empty:
            print(f"空目录: {path}")
        for path in nonempty:
            print(f"跳过非空目录: {path}")
        if not args.execute:
            print("DRY RUN：未删除。确认后添加 --execute。")
            return 0
        for path in empty:
            if alist.remove_empty_dir(path):
                print(f"已删除: {path}")
        return 0
    except (ScraperError, OSError, ValueError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
