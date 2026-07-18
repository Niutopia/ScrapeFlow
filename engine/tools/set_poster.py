#!/usr/bin/env python3
"""将指定 TMDB 条目的海报安全上传为 AList 目录中的 folder.jpg。"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import re
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import (  # noqa: E402
    AListClient,
    DEFAULT_ALIST_URL,
    ScraperError,
    TMDBClient,
    resolve_poster_target,
)


def read_secret(path: Path | None, env_name: str, prompt: str) -> str:
    if path:
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ScraperError(f"无法读取凭据文件 {path}: {exc}") from exc
        if not value:
            raise ScraperError(f"凭据文件为空: {path}")
        return value
    value = os.getenv(env_name)
    if value:
        return value
    if not sys.stdin.isatty():
        raise ScraperError(f"缺少 {env_name}")
    return getpass.getpass(prompt)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("target_dir")
    parser.add_argument("--type", choices=["tv", "movie", "collection"], required=True)
    parser.add_argument("--id", type=int, required=True, dest="tmdb_id")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--approve-sha256", help="批准 dry-run 输出的完整 64 位 SHA-256")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--alist-url", default=os.getenv("ALIST_URL", DEFAULT_ALIST_URL))
    parser.add_argument("--username", default=os.getenv("ALIST_USERNAME", "admin"))
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--tmdb-key-file", type=Path)
    parser.add_argument("--allow-insecure-http", action="store_true")
    args = parser.parse_args()

    try:
        if args.tmdb_id <= 0:
            raise ScraperError("--id 必须是正整数")
        if args.execute and not re.fullmatch(r"[0-9a-fA-F]{64}", args.approve_sha256 or ""):
            raise ScraperError("--execute 必须同时提供完整的 --approve-sha256")
        if not args.execute and args.approve_sha256:
            raise ScraperError("--approve-sha256 只能与 --execute 同时使用")
        tmdb_key = read_secret(args.tmdb_key_file, "TMDB_API_KEY", "TMDB API Key: ")
        password = read_secret(args.password_file, "ALIST_PASSWORD", "AList 密码: ")
        tmdb = TMDBClient(tmdb_key)
        data = tmdb.get(f"/{args.type}/{args.tmdb_id}")
        poster_path = data.get("poster_path")
        if not poster_path:
            raise ScraperError("TMDB 条目没有 poster_path")

        alist = AListClient(
            args.alist_url,
            args.username,
            password,
            allow_insecure_http=args.allow_insecure_http,
        )
        alist.login()
        target, preexisting = resolve_poster_target(
            alist, args.target_dir, overwrite=args.overwrite
        )
        if preexisting and not args.overwrite:
            raise ScraperError("目标目录已有 folder.jpg；确认替换后添加 --overwrite")

        approval_payload = {
            "tmdb_type": args.type,
            "tmdb_id": args.tmdb_id,
            "poster_path": str(poster_path),
            "target": target,
            "preexisting": preexisting,
            "overwrite": bool(args.overwrite),
        }
        approval_digest = hashlib.sha256(
            json.dumps(
                approval_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        print(f"计划: TMDB {args.type}/{args.tmdb_id} → {target}")
        print(f"计划 SHA-256: {approval_digest}")
        if not args.execute:
            print(
                "DRY RUN：未上传。确认后重新运行并添加 "
                f"--approve-sha256 {approval_digest} --execute。"
            )
            return 0
        approved = (args.approve_sha256 or "").lower()
        if approved != approval_digest:
            raise ScraperError(
                f"海报计划 SHA-256 不匹配: approved={approved}, actual={approval_digest}"
            )
        alist.upload_bytes(target, tmdb.download_poster(str(poster_path)), "image/jpeg")
        print("海报上传完成。")
        return 0
    except (ScraperError, OSError, ValueError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
