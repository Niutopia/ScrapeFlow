#!/usr/bin/env python3
"""只读扫描 AList 目录并列出媒体文件。"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import AListClient, DEFAULT_ALIST_URL, MEDIA_EXTS, ScraperError  # noqa: E402


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
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--ignore-orphan-temp", action="store_true")
    parser.add_argument("--alist-url", default=os.getenv("ALIST_URL", DEFAULT_ALIST_URL))
    parser.add_argument("--username", default=os.getenv("ALIST_USERNAME", "admin"))
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--allow-insecure-http", action="store_true")
    args = parser.parse_args()

    try:
        if args.limit < 0:
            raise ScraperError("--limit 不能小于 0")
        alist = AListClient(
            args.alist_url,
            args.username,
            resolve_password(args.password_file),
            allow_insecure_http=args.allow_insecure_http,
        )
        alist.login()
        for path in args.paths:
            media = [
                item
                for item in alist.walk(
                    path, ignore_orphan_temp=args.ignore_orphan_temp
                )
                if Path(str(item.get("name", ""))).suffix.lower() in MEDIA_EXTS
            ]
            print(f"\n=== {path} ===")
            print(f"媒体文件: {len(media)}")
            for item in media[: max(args.limit, 0)]:
                print(f"  {item['full_path']}")
            if len(media) > args.limit >= 0:
                print(f"  ... 其余 {len(media) - args.limit} 个")
        return 0
    except (ScraperError, OSError, ValueError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
