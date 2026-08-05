#!/usr/bin/env python3
"""安全的 TMDB 元数据刮削与 AList 媒体文件整理工具。

默认只生成并保存计划（dry-run）。实际修改必须加载已保存计划，提交其
SHA-256，并显式传入 ``--execute``；实时重新扫描的计划禁止直接执行。
"""

from __future__ import annotations

import difflib
import copy
import contextlib
import hashlib
import html
import http.client
import ipaddress
import json
import os
import posixpath
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import unicodedata
import time
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Collection, Iterable, Mapping, Sequence

try:  # Package import in tests; local import when invoked as ``python engine/scraper.py``.
    from engine.scrapeflow.clients.http import (
        JsonHttpClient,
        ValidatingRedirectHandler,
        redact_sensitive_text as _redact_sensitive_text,
        redact_url as _redact_url,
    )
    from engine.scrapeflow.cli_args import (
        build_parser as _package_build_parser,
        read_secret_file as _read_secret_file,
        resolve_password as _resolve_password,
        resolve_tmdb_key as _resolve_tmdb_key,
    )
    from engine.scrapeflow.canonical_work_tree import (
        CanonicalTreeError,
        CanonicalWork,
        WorkIdentity,
        plan_canonical_work_tree,
    )
    from engine.scrapeflow.errors import ApiError, PartialMoveError, PlanError, ScraperError
    from engine.scrapeflow.models import (
        AutoMatch, EpisodeKey, ExecutionJournal, ExecutionRecord, Plan, PlanNotice,
        PlannedCleanup, PlannedFile, PlannedProblem, RecoveryState,
    )
    from engine.scrapeflow.placement import placement_for
    from engine.scrapeflow.alist_exact_file_adapter import AListExactFileAdapter
    from engine.scrapeflow.remote_file_transaction import (
        RemoteFileTransferSpec,
        TransactionUncertain,
        discard_completed_remote_file_transaction,
        prepare_remote_file_transaction,
        run_remote_file_transaction,
    )
    from engine.scrapeflow.local_upload_transaction import (
        LocalUploadConflict,
        LocalUploadSpec,
        deterministic_local_upload_id,
        run_local_upload_transaction,
    )
    from engine.scrapeflow.hybrid_remote_transaction import (
        DEFAULT_ROLLBACK_ROOT,
        REMOTE_ROLLBACK_ROOT_ENV,
        HybridTransferSpec,
        prepare_hybrid_batch,
        restore_hybrid_transfer,
        run_hybrid_transfer,
    )
    from engine.scrapeflow.residual_policy import (
        classify_residual,
        recoverable_delete_reason,
    )
    from engine.scrapeflow.serialization import (
        canonical_json_bytes as _canonical_json_bytes,
        reserve_output_path as _reserve_output_path,
        write_json_reserved as _write_json_reserved,
    )
except ModuleNotFoundError:
    from scrapeflow.clients.http import (
        JsonHttpClient,
        ValidatingRedirectHandler,
        redact_sensitive_text as _redact_sensitive_text,
        redact_url as _redact_url,
    )
    from scrapeflow.cli_args import (
        build_parser as _package_build_parser,
        read_secret_file as _read_secret_file,
        resolve_password as _resolve_password,
        resolve_tmdb_key as _resolve_tmdb_key,
    )
    from scrapeflow.canonical_work_tree import (
        CanonicalTreeError,
        CanonicalWork,
        WorkIdentity,
        plan_canonical_work_tree,
    )
    from scrapeflow.errors import ApiError, PartialMoveError, PlanError, ScraperError
    from scrapeflow.models import (
        AutoMatch, EpisodeKey, ExecutionJournal, ExecutionRecord, Plan, PlanNotice,
        PlannedCleanup, PlannedFile, PlannedProblem, RecoveryState,
    )
    from scrapeflow.placement import placement_for
    from scrapeflow.alist_exact_file_adapter import AListExactFileAdapter
    from scrapeflow.remote_file_transaction import (
        RemoteFileTransferSpec,
        TransactionUncertain,
        discard_completed_remote_file_transaction,
        prepare_remote_file_transaction,
        run_remote_file_transaction,
    )
    from scrapeflow.local_upload_transaction import (
        LocalUploadConflict,
        LocalUploadSpec,
        deterministic_local_upload_id,
        run_local_upload_transaction,
    )
    from scrapeflow.hybrid_remote_transaction import (
        DEFAULT_ROLLBACK_ROOT,
        REMOTE_ROLLBACK_ROOT_ENV,
        HybridTransferSpec,
        prepare_hybrid_batch,
        restore_hybrid_transfer,
        run_hybrid_transfer,
    )
    from scrapeflow.residual_policy import (
        classify_residual,
        recoverable_delete_reason,
    )
    from scrapeflow.serialization import (
        canonical_json_bytes as _canonical_json_bytes,
        reserve_output_path as _reserve_output_path,
        write_json_reserved as _write_json_reserved,
    )

__version__ = "3.3.2"
PLAN_SCHEMA_VERSION = 4
SUPPORTED_PLAN_SCHEMA_VERSIONS = {2, 3, 4}

DEFAULT_ALIST_URL = "http://127.0.0.1:5244"
DEFAULT_TMDB_BASE = "https://api.themoviedb.org/3"
DEFAULT_IMAGE_BASE = "https://image.tmdb.org/t/p/original"
AUTO_MATCH_MIN_MARGIN = 0.08
LOCK_PREFIX = ".scraper-lock-"
PROGRESS_PREFIX = "SCRAPEFLOW_PROGRESS "
RECOVERY_DIGEST_PREFIX = "SCRAPEFLOW_RECOVERY_DIGEST "
RECOVERY_ITEM_PREFIX = "SCRAPEFLOW_RECOVERY_ITEM "


def _trace_io(message: str) -> None:
    """Emit opt-in, credential-free I/O diagnostics for stuck live plans."""
    if os.getenv("SCRAPEFLOW_TRACE_IO") == "1":
        print(f"SCRAPEFLOW_TRACE_IO {message}", file=sys.stderr, flush=True)

PROVEN_SAFE_WARNING_PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"^确认执行后将删除明确无用的",
        r"^TMDB 标题与现有作品目录仅大小写或 Unicode 拼写不同",
        r"^现有 (?:tvshow\.nfo|电影 NFO) 已确认相同 TMDB ID \d+；",
        r"^\d+ 个文件只有单一弱快照字段",
        r"^根据 TMDB 官方特别篇标题将 .+ 自动映射为 SP\d+$",
        r"^根据文件中的官方特别篇标签将 SP\d+ 映射为 SP\d+$",
        r"^根据文件中的子系列标题将 SP\d+ 映射为 SP\d+$",
        r"^根据第 \d+ 季官方时间线将 (?:OVBSP|OVA|OAV|OAD) 自动映射为 SP\d+$",
        r"^E\d+ 超出第 \d+ 季官方正片集数；根据 TMDB 标题映射为 SP\d+$",
        r"^源文件的 E00 已对应 TMDB 第 0 季第 1 集",
        r"^已检索TMDB 官方开播时间与完整时长并唯一确认源文件 E00 对应 "
        r"(?:E|SP)\d+(?:-(?:E|SP)\d+)?（序章/第 0 话）$",
        r"^(?:E|SP)\d+(?:-(?:E|SP)\d+)? 使用显式覆盖映射$",
        r"^已从目录结构识别并合并 \d+ 个季度$",
        r"^源根目录中的裸集号完整覆盖 TMDB 唯一官方季度；已按完整边界归入 Season 01$",
        r"^源根目录中的裸集号完整覆盖已确认的 TMDB Season \d{2,3} 边界；已按完整边界归入$",
        r"^源发行将长篇剧集分为重置编号的跨季 absolute 块；"
        r"已仅在所有视频块完整覆盖 01–N，且与 TMDB 全部季集数"
        r"边界唯一分割时自动映射：.+$",
        r"^已识别主系列及 \d+ 个独立衍生剧集",
        r"^子目录《.+》已通过 TMDB 标题/别名和完整 \d+ 集边界"
        r"唯一确认是独立剧集《.+》（(?:19|20)\d{2}），未归入母作品 Season 00$",
        r"^已按 Infuse 规则整理 \d+ 个预告/花絮文件$",
        r"^\d+ 个明确位于特典目录的幕后/访谈/花絮视频"
        r"已按 Infuse Extras 命名保留，不作为正片集号$",
        r"^已按文件名中的 TMDB 编号识别 \d+ 部合集电影$",
        r"^E\d+\.5 经 TMDB 官方 Season 00/季度多证据评分.*自动映射为 S\d{2}E\d{2}",
        r"^确认执行后将删除 \d+ 个明确无用的",
        r"^(?:E|SP)\d+(?:-(?:E|SP)\d+)? 存在同一 TMDB 集号的多个清晰度版本；已优先保留最高可确认清晰度，并计划清理 \d+ 个重复版本$",
        r"^已识别 \d+ 个独立作品目录",
        r"^已识别 \d+ 个独立电影文件；每个文件均已独立确认不同 TMDB 电影身份",
        r"^编号 01–\d{2,4} 的完整视频序列与 TMDB \d+ 部电影的"
        r"官方标题/别名逐一一致；已仅按官方上映日期顺序建立电影归属$",
        r"^其中 \d+ 个未标注 TMDB 编号的(?:子作品|电影文件)已独立检索确认$",
        r"^\d+ 部系列电影已保留独立 TMDB 身份和各自独立电影目录$",
        r"^\d+ 部系列电影已保留独立 TMDB 身份，并以影片、同名 NFO/海报扁平归入各自系列根目录$",
        r"^已跳过无视频的空占位目录:",
        r"^(?:E|SP)\d+(?:-(?:E|SP)\d+)? 存在同清晰度的内封/软字幕与内嵌/硬字幕版本；已保留可切换字幕版本，并计划清理 \d+ 个硬字幕重复视频$",
        r"^识别到 \d+ 部独立电影；已保留独立 TMDB 身份并放入与电视剧作品目录并列的独立电影目录$",
        r"^识别到 \d+ 部剧场版，已保留独立 TMDB 电影身份并以文件、同名 NFO/海报扁平归入本系列根目录$",
        r"^\d+ 个特典小动画/OVA 已依官方短片时长、发行断档和源季序映射到全局 Season 00 编号$",
        r"^已根据子目录名称与多语言官方特别篇标题的唯一连续匹配、"
        r"(?:多语言官方标题和|(?:19|20)\d{2} 年官方发行时间线和)?"
        r"完整连续源编号，"
        r"将 \d+ 集短篇映射为 SP\d+–SP\d+$",
        r"^\d+ 个与已确认特别篇视频同名的外挂字幕已跟随视频的官方季集映射$",
        r"^电影原目标目录 .+ 已扁平化；影片、同名 NFO 与海报将直接旁挂在系列根目录$",
        r"^同一视频的多份外挂字幕仅保留 1 条首选轨道；\d+ 个备选字幕已保留在源目录$",
        r"^\d+ 个独立字幕目录文件已通过唯一同发行 basename "
        r"跟随已确认视频$",
        r"^(?:E\d+(?:\.\d+)?|SP\d+)(?:-(?:E|SP)\d+)? 的 ASS 文本伴侣 title 样式"
        r"唯一标记为已确认的 TMDB movie/\d+；已按同一电影版本参与清晰度去重$",
        r"^同一视频的多份外挂字幕仅保留 1 条首选轨道；\d+ 个备选字幕已保留在源目录$",
        r"^\d+ 个繁体字幕已有同发行简体对应；备选字幕已保留在源目录$",
    )
)

COMPLETE_OFFICIAL_SEASON_BOUNDARY_RE = re.compile(
    r"^源根目录中的裸集号完整覆盖已确认的 TMDB "
    r"Season (\d{2,3}) 边界；已按完整边界归入$"
)

VIDEO_EXTS = {
    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".wmv", ".mov",
    ".webm", ".flv", ".mpeg", ".mpg", ".rmvb", ".strm",
}


def emit_progress(
    stage: str,
    *,
    completed: int,
    total: int,
    percent: float,
    message: str,
) -> None:
    payload = {
        "stage": stage,
        "completed": max(0, int(completed)),
        "total": max(0, int(total)),
        "percent": max(0.0, min(100.0, round(float(percent), 1))),
        "message": message,
    }
    print(PROGRESS_PREFIX + json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)
SUBTITLE_EXTS = {".ass", ".srt", ".ssa", ".sub", ".idx", ".vtt", ".sup", ".mks"}
MEDIA_EXTS = VIDEO_EXTS | SUBTITLE_EXTS

# 仅匹配独立标签，不会把 Whisper、Display 等普通单词误判为 SP。
IGNORED_EXTRA_TAG_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:NCOP|NCED|PV|MENU|FONTS?|EXTRAS?)(?:\d+)?(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
TITLE_EXTRA_TAG_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])EXTRAS?(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
DISPOSABLE_VIDEO_TAG_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:NCOP|NCED)(?:\d+(?:v\d+)?)?(?:$|[\s._\-\[\]()])|"
    r"(?:^|[\s._\-\[\]()])(?:MENU(?:OVA|MAIN)?|GAME[ ._-]*(?:OP|ED))(?:\d+)?(?:$|[\s._\-\[\]()])|"
    r"(?:メニュー映像|メニュー画面|菜单视频|菜單影片)|"
    r"(?:WEB予告|本予告|特報|CM集|ノンクレジット(?:OP|ED))|"
    r"\[\s*CM\s*\]|"
    r"(?:^|[\s._\-\[\]()])CM(?:\d{1,3}|[ ._-]*COLLECTION)(?:$|[\s._\-\[\]()])|"
    r"(?:SPONSOR|SPONSER)[ ._-]*EYECATCH[ ._-]*COLLECTION|"
    r"(?:^|[\s._\-\[()])EYE[ ._-]*CATCH(?:$|[\s._\-\])()])|"
    r"(?:THEME[ ._-]*SONG[ ._-]*MV|主题曲[ ._-]*MV)",
    re.IGNORECASE,
)
EPISODE_THEME_VARIANT_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:OP|ED)[ ._-]*EP[ ._-]*0*(\d{1,4})(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
BONUS_DIRECTORY_RE = re.compile(
    r"(?:^|/)(?:SPs?|Extras?|Bonus|Tokuten|特典|映像特典)(?:/|$)",
    re.IGNORECASE,
)
ADVERTISEMENT_IMAGE_RE = re.compile(
    r"(?:防失联.*(?:链接|联系)|海量.*(?:资源|合集)|资源文档|"
    r"(?:番剧|动漫|動漫|影视|影視)?合集文档|扫码|二维码|加群|公众号)",
    re.IGNORECASE,
)
NON_MEDIA_LIBRARY_CONTEXT_RE = re.compile(
    r"(?:^|/)(?:小说|同人|漫画|书籍|电子书|文库(?:版)?|短篇集|"
    r"txt|texts?|novels?|ebooks?|comics?)(?:/|$)",
    re.IGNORECASE,
)
ADVERTISEMENT_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
FONT_RESOURCE_RE = re.compile(
    r"(?:字体(?:包|库|文件)?|字库|(?:^|[\s._\-\[\]()])fonts?(?:$|[\s._\-\[\]()]))",
    re.IGNORECASE,
)
FONT_RESOURCE_EXTS = {
    ".exe", ".bin", ".dat", ".zip", ".7z", ".rar",
    ".ttf", ".otf", ".ttc", ".woff", ".woff2",
}
SAMPLE_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:sample|样片|试看)(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
BONUS_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("behindthescenes", re.compile(r"behind[ ._-]*the[ ._-]*scenes?|幕后", re.IGNORECASE)),
    ("deleted", re.compile(r"(?:^|[ ._\-])deleted(?:$|[ ._\-])|删减片段", re.IGNORECASE)),
    ("featurette", re.compile(r"featurette|制作特辑", re.IGNORECASE)),
    ("interview", re.compile(r"interview|访谈", re.IGNORECASE)),
    ("trailer", re.compile(r"trailer|预告", re.IGNORECASE)),
    ("scene", re.compile(r"(?:^|[ ._\-])scene(?:$|[ ._\-])|片段", re.IGNORECASE)),
    ("short", re.compile(r"(?:^|[ ._\-])short(?:$|[ ._\-])|短片", re.IGNORECASE)),
)
PLANNED_BONUS_SUFFIX_RE = re.compile(
    rf"-(?:{'|'.join(re.escape(label) for label, _ in BONUS_PATTERNS)})(?:\d+)?$",
    re.IGNORECASE,
)
BONUS_CONTAINER_RE = re.compile(
    r"^(?:extras?|trailers?|behind[ ._-]*the[ ._-]*scenes|deleted[ ._-]*scenes|"
    r"featurettes?|interviews?|scenes?|shorts?|花絮|预告|幕后|访谈)$",
    re.IGNORECASE,
)
EDITION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (label, re.compile(pattern, re.IGNORECASE))
    for label, pattern in (
        (
            "Director's Cut",
            r"director(?:'?s|')?[ ._-]*cut|导演剪辑|"
            r"\[\s*\d{1,4}\s+cut\s*\]",
        ),
        ("New Edit", r"新编集版|新編集版|shin[ ._-]*hensh(?:u|uu)[ ._-]*ban"),
        ("Extended Cut", r"extended[ ._-]*cut|加长版"),
        ("Theatrical Cut", r"theatrical[ ._-]*cut|院线版"),
        ("Final Cut", r"final[ ._-]*cut"),
        ("Unrated Cut", r"unrated[ ._-]*cut|未分级"),
        ("3D", r"(?:^|[ ._\-\[\]()])(?:SBS[ ._-]*)?3D(?:$|[ ._\-\[\]()])"),
        ("IMAX", r"(?:^|[ ._\-\[\]()])imax(?:$|[ ._\-\[\]()])"),
        (
            "Musani Staff Credit",
            r"musani[ ._-]*staff[ ._-]*credit(?:[ ._-]*ver(?:sion)?)?",
        ),
        (
            "Original Staff Credit",
            r"original[ ._-]*staff[ ._-]*credit(?:[ ._-]*ver(?:sion)?)?",
        ),
        ("Special Edition", r"special[ ._-]*edition|特别版"),
    )
)
MULTI_EPISODE_RE = re.compile(
    r"(?:^|[^0-9])(?:E|EP)?\s*0*(\d{1,3})\s*[-~+&]\s*(?:E|EP)?\s*0*(\d{1,3})(?:$|[^0-9])",
    re.IGNORECASE,
)
MULTI_EPISODE_CONCAT_RE = re.compile(
    r"(?:^|[^A-Za-z0-9])(?:S\d{1,2})?E\s*0*(\d{1,3})E\s*0*(\d{1,3})(?:$|[^0-9])",
    re.IGNORECASE,
)
SEASON_DASH_EPISODE_RE = re.compile(
    r"(?:^|[^A-Za-z0-9])S\d{1,2}\s*[-._ ]+\s*0*(\d{1,4})(?:$|[^0-9])",
    re.IGNORECASE,
)
DUAL_BRACKET_EPISODE_RE = re.compile(
    r"\[\s*0*(\d{1,3})\s*[_/]\s*0*(\d{1,3})\s*\]",
    re.IGNORECASE,
)
SEASON_LOCAL_ABSOLUTE_RE = re.compile(
    r"(?:S(?:eason)?\s*\d{1,3}|\d{1,3}(?:st|nd|rd|th)\s+Season)"
    r"\s*-\s*0*(\d{1,3})\s*\(\s*0*(\d{1,3})\s*\)",
    re.IGNORECASE,
)
EPISODE_NOISE_RE = re.compile(
    r"(?:4320|2160|1440|1080|720|576|480)[pi]|(?:4|8)k|x[._ ]?26[45]|h\.?26[45]|"
    r"10bit|8bit|av1|(?<!\d)(?:1|2|5|7)\.1(?!\d)|(?<!\d)(?:1|2)\.0(?!\d)",
    re.IGNORECASE,
)
DATE_NOISE_RE = re.compile(r"\b(?:19|20)\d{2}[-._]\d{1,2}[-._]\d{1,2}\b")

SIMPLIFIED_MARKERS = (
    "简体",
    "简中",
    "简日",
    "简英",
    "chs",
    "zh-cn",
    "zh_hans",
    "zh-hans",
    "gb2312",
    "gbk",
)
TRADITIONAL_MARKERS = (
    "繁体",
    "繁中",
    "繁日",
    "繁英",
    "cht",
    "zh-tw",
    "zh-hk",
    "zh_hant",
    "zh-hant",
    "big5",
)
ENGLISH_MARKERS = ("english", "eng", "en")
JAPANESE_MARKERS = ("japanese", "jpn", "jp", "ja")


def _collision_key(value: str) -> str:
    """保守的跨平台冲突键：NFC、casefold，并忽略段尾空格和句点。"""
    normalized = unicodedata.normalize("NFC", value).casefold()
    if "/" in normalized:
        return "/".join(part.rstrip(" .") for part in normalized.split("/"))
    return normalized.rstrip(" .")


def _path_is_within(path: str, root: str) -> bool:
    path_key = _collision_key(normalize_remote_path(path).rstrip("/") or "/")
    root_key = _collision_key(normalize_remote_path(root).rstrip("/") or "/")
    return path_key == root_key or path_key.startswith(root_key.rstrip("/") + "/")


def _validate_alist_transport(base_url: str, *, allow_insecure_http: bool) -> str:
    try:
        parsed = urllib.parse.urlsplit(base_url)
    except ValueError as exc:
        raise ValueError(f"无效 AList URL: {base_url!r}") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("AList URL 必须是完整的 http:// 或 https:// 地址")
    try:
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("AList URL 包含无效端口") from exc
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("AList URL 不得包含用户名或密码")
    if parsed.query or parsed.fragment:
        raise ValueError("AList URL 不得包含查询参数或片段")
    if parsed.scheme == "http" and not allow_insecure_http:
        host = parsed.hostname
        is_loopback = host.lower() == "localhost"
        if not is_loopback:
            try:
                is_loopback = ipaddress.ip_address(host).is_loopback
            except ValueError:
                is_loopback = False
        if not is_loopback:
            raise ScraperError(
                "远程 AList 使用 HTTP 会明文传输密码和 token；请改用 HTTPS，"
                "或明确传入 --allow-insecure-http 接受风险"
            )
    return base_url.rstrip("/")


def _extract_year(value: Any) -> str:
    text = str(value or "")
    match = re.match(r"^(19|20)\d{2}(?:$|[-/])", text)
    return match.group(0)[:4] if match else "未知年份"


def _entry_hash_value(entry: Mapping[str, Any]) -> str | None:
    for key in ("hash_info", "hash", "etag", "sha1", "sha256", "md5"):
        value = entry.get(key)
        if value not in (None, "", {}, []):
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            return str(value)
    return None


def _entry_modified_value(entry: Mapping[str, Any]) -> str | None:
    for key in ("modified", "updated_at", "mtime", "last_modified"):
        value = entry.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _entry_size_value(entry: Mapping[str, Any]) -> int | None:
    value = entry.get("size")
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _planned_file_from_entry(
    item: Mapping[str, Any],
    *,
    final_name: str,
    target_dir: str,
    episode_key: str | None = None,
) -> "PlannedFile":
    source_path = normalize_remote_path(str(item["full_path"]))
    source_dir, original_name = split_remote(source_path)
    return PlannedFile(
        source_path=source_path,
        source_dir=source_dir,
        original_name=original_name,
        final_name=final_name,
        target_dir=target_dir,
        media_kind=media_kind(original_name),
        episode_key=episode_key,
        source_size=_entry_size_value(item),
        source_modified=_entry_modified_value(item),
        source_hash=_entry_hash_value(item),
    )


def _has_unsafe_unicode(text: str) -> bool:
    return any(unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in text)


def _terminal_text(value: Any) -> str:
    return "".join(
        "?" if unicodedata.category(char) in {"Cc", "Cf", "Cs"} else char
        for char in str(value)
    )


def _validate_remote_basename(name: str) -> str:
    if not isinstance(name, str) or not name or name in {".", ".."}:
        raise ValueError(f"无效远端文件名: {name!r}")
    if "/" in name or "\\" in name or _has_unsafe_unicode(name):
        raise ValueError(f"远端文件名包含非法或不可见控制字符: {name!r}")
    return name


class AListClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        timeout: float = 20.0,
        retries: int = 3,
        allow_insecure_http: bool = False,
    ) -> None:
        self.base_url = _validate_alist_transport(
            base_url, allow_insecure_http=allow_insecure_http
        )
        self.username = username
        self.password = password
        # Keep the credential only for the lifetime of this local worker so an
        # AList restart can invalidate its token without permanently killing a
        # long-running job.  ``password`` remains cleared after the first login
        # to preserve the existing public/debug surface.
        self._auth_password = password
        self.http = JsonHttpClient(timeout=timeout, retries=retries)
        self.token: str | None = None

    def _validate_api_url(self, url: str) -> None:
        """Keep authenticated AList traffic on the configured origin."""
        try:
            candidate = urllib.parse.urlsplit(url)
            base = urllib.parse.urlsplit(self.base_url)
            candidate_port = candidate.port or (443 if candidate.scheme == "https" else 80)
            base_port = base.port or (443 if base.scheme == "https" else 80)
        except ValueError as exc:
            raise ApiError("AList API 返回了无效的重定向地址") from exc
        if (
            candidate.scheme != base.scheme
            or not candidate.hostname
            or not base.hostname
            or candidate.hostname.casefold() != base.hostname.casefold()
            or candidate_port != base_port
            or candidate.username is not None
            or candidate.password is not None
        ):
            raise ApiError("拒绝把 AList 凭据发送到不同来源")

    def login(self) -> str:
        response = self.http.request_json(
            f"{self.base_url}/api/auth/login",
            method="POST",
            json_body={"username": self.username, "password": self._auth_password},
            url_validator=self._validate_api_url,
        )
        self._require_success(response, "AList 登录")
        token = (response.get("data") or {}).get("token")
        if not isinstance(token, str) or not token:
            raise ApiError("AList 登录成功但未返回 token")
        self.token = token
        self.password = ""
        return token

    @staticmethod
    def _requires_reauthentication(response: Mapping[str, Any]) -> bool:
        code = response.get("code")
        message = str(response.get("message") or "").casefold()
        return code in {401, 403} or any(marker in message for marker in (
            "token is invalidated",
            "token invalidated",
            "invalid token",
            "token expired",
            "unauthorized",
            "未登录",
            "登录失效",
            "令牌失效",
        ))

    def _request_json_authenticated(self, url: str, **kwargs: Any) -> dict[str, Any]:
        """Retry exactly once after an authentication-only failure.

        AList returns authentication failures either as HTTP 401/403 or as a
        successful HTTP response containing ``code`` 401/403.  Replaying is
        safe here because the rejected request was not authorized and therefore
        could not have performed the requested file operation.
        """
        extra_headers = dict(kwargs.pop("headers", {}) or {})
        for attempt in range(2):
            try:
                response = self.http.request_json(
                    url,
                    headers={**self._headers(), **extra_headers},
                    url_validator=self._validate_api_url,
                    **kwargs,
                )
            except ApiError as exc:
                if attempt or exc.status_code not in {401, 403}:
                    raise
                self.login()
                continue
            if not self._requires_reauthentication(response) or attempt:
                return response
            self.login()
        raise ApiError("AList 重新登录后仍未通过认证")

    def _request_bytes_authenticated(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        max_bytes: int,
    ) -> bytes:
        for attempt in range(2):
            try:
                return self.http.request_bytes(
                    url,
                    headers={**self._headers(), **dict(headers or {})},
                    max_bytes=max_bytes,
                    url_validator=self._validate_api_url,
                )
            except ApiError as exc:
                if attempt or exc.status_code not in {401, 403}:
                    raise
                self.login()
        raise ApiError("AList 重新登录后仍未通过认证")

    def _headers(self) -> dict[str, str]:
        if not self.token:
            raise ApiError("尚未登录 AList")
        return {"Authorization": self.token}

    def _require_success(self, response: Mapping[str, Any], action: str) -> dict[str, Any]:
        code = response.get("code")
        if code != 200:
            detail = response.get("message") or response
            raise ApiError(
                f"{action}失败: "
                f"{_redact_sensitive_text(str(detail), (self.password, self._auth_password, self.token or ''))}"
            )
        return dict(response)

    def call(
        self, endpoint: str, body: Mapping[str, Any], *, retryable: bool = False
    ) -> dict[str, Any]:
        response = self._request_json_authenticated(
            f"{self.base_url}/api/fs/{endpoint}",
            method="POST",
            json_body=body,
            retryable=retryable,
        )
        return self._require_success(response, f"AList {endpoint}")

    def admin_storages(self) -> list[dict[str, Any]]:
        """Return admin storage rows for in-process credential delegation.

        Callers must treat ``addition`` as secret material and keep it out of
        logs, artifacts, command arguments, and environment variables.
        """
        response = self._request_json_authenticated(
            f"{self.base_url}/api/admin/storage/list?page=1&per_page=1000",
            method="GET",
        )
        data = self._require_success(response, "AList storage list").get("data")
        content = data.get("content") if isinstance(data, Mapping) else None
        if not isinstance(content, list) or not all(isinstance(row, dict) for row in content):
            raise ApiError("AList storage list returned an invalid payload")
        return [dict(row) for row in content]

    def list(self, path: str, refresh: bool = False) -> list[dict[str, Any]]:
        """完整列出目录内容，显式处理 AList 的分页响应。"""
        normalized = normalize_remote_path(path)
        page = 1
        per_page = 500
        output: list[dict[str, Any]] = []
        seen_pages: set[int] = set()
        seen_signatures: set[tuple[str, ...]] = set()
        seen_entries: set[tuple[str, bool]] = set()

        while True:
            _trace_io(
                f"alist.list start path={normalized!r} page={page} "
                f"refresh={bool(refresh if page == 1 else False)}"
            )
            response = self.call(
                "list",
                {
                    "path": normalized,
                    "page": page,
                    "per_page": per_page,
                    # 只在第一页强制刷新，避免每翻一页都重新排列目录内容。
                    "refresh": refresh if page == 1 else False,
                },
                retryable=True,
            )
            _trace_io(f"alist.list done path={normalized!r} page={page}")
            data = response.get("data") or {}
            if not isinstance(data, Mapping):
                raise ApiError(f"AList list 返回格式异常: {normalized}")
            content = data.get("content")
            if content is None:
                content = []
            if not isinstance(content, list):
                raise ApiError(f"AList list 返回格式异常: {normalized}")
            for item in content:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                if isinstance(name, str):
                    entry_key = (name, bool(item.get("is_dir")))
                    if entry_key in seen_entries:
                        raise ApiError(
                            f"AList list 分页包含重复条目，目录可能在扫描中发生变化: "
                            f"{normalized}/{name}"
                        )
                    seen_entries.add(entry_key)
                output.append(dict(item))

            signature = tuple(
                str(item.get("name", "")) for item in content if isinstance(item, dict)
            )
            if signature and signature in seen_signatures:
                raise ApiError(f"AList list 返回了重复分页内容，已停止: {normalized}")
            seen_signatures.add(signature)

            raw_page = data.get("page")
            if isinstance(raw_page, bool):
                raise ApiError(
                    f"AList list 返回了无效页码: {normalized}; {raw_page!r}"
                )
            try:
                returned_page = int(raw_page or page)
            except (TypeError, ValueError) as exc:
                raise ApiError(
                    f"AList list 返回了无效页码: {normalized}; {data.get('page')!r}"
                ) from exc
            if returned_page < 1:
                raise ApiError(f"AList list 返回了无效页码: {normalized}; page={returned_page}")
            if returned_page in seen_pages:
                raise ApiError(f"AList list 分页重复，已停止: {normalized}; page={returned_page}")
            seen_pages.add(returned_page)

            has_more = data.get("has_more")
            pages_total = data.get("pages_total")
            total = data.get("filtered_total")
            if isinstance(total, bool) or not isinstance(total, int):
                total = data.get("total")
            if isinstance(total, bool):
                total = None
            metadata_claims_more = False
            if isinstance(has_more, bool):
                more = has_more
                metadata_claims_more = has_more
            elif isinstance(pages_total, int) and not isinstance(pages_total, bool):
                more = returned_page < pages_total
                metadata_claims_more = more
            elif isinstance(total, int):
                more = len(output) < total
                metadata_claims_more = more
            else:
                # 旧版 AList 没有分页元数据时，满页后再请求一页确认。
                more = len(content) >= per_page

            if not more:
                return output
            if not content:
                if metadata_claims_more:
                    raise ApiError(
                        f"AList list 声称仍有下一页，但当前页为空，已停止: {normalized}; "
                        f"page={returned_page}"
                    )
                return output
            page = returned_page + 1
            if page > 100_000:
                raise ApiError(f"AList list 分页数量异常: {normalized}")

    def try_list(self, path: str, refresh: bool = False) -> list[dict[str, Any]] | None:
        try:
            return self.list(path, refresh=refresh)
        except ApiError as exc:
            message = str(exc).lower()
            missing_markers = (
                "not found",
                "no such file",
                "not exist",
                "object does not exist",
                "不存在",
                "未找到",
            )
            if any(marker in message for marker in missing_markers):
                return None
            raise

    def walk(
        self,
        path: str,
        *,
        refresh: bool = True,
        max_directories: int = 10_000,
        max_files: int = 200_000,
        ignore_orphan_temp: bool = False,
        include_bonus: bool = False,
        include_title_extras: bool = False,
        excluded_roots: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if max_directories <= 0 or max_files <= 0:
            raise ValueError("max_directories 与 max_files 必须大于 0")
        root = normalize_remote_path(path)
        normalized_excludes = tuple(
            normalize_remote_path(value).rstrip("/") or "/"
            for value in (excluded_roots or [])
        )

        def excluded(candidate: str) -> bool:
            return any(
                excluded_root == "/"
                or candidate == excluded_root
                or candidate.startswith(excluded_root + "/")
                for excluded_root in normalized_excludes
            )

        if excluded(root):
            return []
        output: list[dict[str, Any]] = []
        stack = [root]
        visited: set[str] = set()

        while stack:
            current = stack.pop()
            if current in visited:
                continue
            if len(visited) >= max_directories:
                raise PlanError(
                    f"递归目录数超过安全上限 {max_directories}，已停止扫描: {root}"
                )
            visited.add(current)
            for item in self.list(current, refresh=refresh):
                name = item.get("name")
                if not isinstance(name, str) or not name:
                    continue
                try:
                    _validate_remote_basename(name)
                except ValueError as exc:
                    raise PlanError(
                        f"AList 返回了无法安全表示的条目名称: {current}/{name!r}"
                    ) from exc
                full_path = join_remote(current, name)
                item["full_path"] = full_path
                if excluded(full_path):
                    continue
                if is_scraper_lock(name):
                    raise PlanError(
                        f"发现仍在执行或上次异常退出遗留的整理锁，已停止: {full_path}。"
                        "请确认没有其他整理任务运行，再使用 --inspect-journal 或 "
                        "--recover-journal 处理对应任务。"
                    )
                if is_scraper_temp(name):
                    if not ignore_orphan_temp:
                        raise PlanError(
                            f"发现上次中断遗留的临时条目，已停止: {full_path}。"
                            "请依据对应 journal 恢复或人工核对；只有明确接受遗漏风险时才使用 "
                            "--ignore-orphan-temp。"
                        )
                    continue
                if item.get("is_dir"):
                    if should_ignore_extra(name) and not (
                        include_bonus and BONUS_CONTAINER_RE.fullmatch(name.strip(" []()"))
                    ) and not (
                        include_title_extras and TITLE_EXTRA_TAG_RE.search(name)
                    ):
                        continue
                    stack.append(full_path)
                else:
                    if should_ignore_extra(name) and cleanup_reason(name) is None and not (
                        include_bonus and bonus_type(name) is not None
                    ) and not (
                        include_title_extras and TITLE_EXTRA_TAG_RE.search(name)
                    ):
                        continue
                    output.append(item)
                    if len(output) > max_files:
                        raise PlanError(
                            f"递归文件数超过安全上限 {max_files}，已停止扫描: {root}"
                        )
        return output

    def rename(self, full_path: str, new_name: str) -> None:
        self.call(
            "rename",
            {"path": normalize_remote_path(full_path), "name": _validate_remote_basename(new_name)},
        )

    def move(self, src_dir: str, dst_dir: str, names: Sequence[str]) -> None:
        if not names:
            return
        clean_names = [_validate_remote_basename(name) for name in names]
        self.call(
            "move",
            {
                "src_dir": normalize_remote_path(src_dir),
                "dst_dir": normalize_remote_path(dst_dir),
                "names": clean_names,
            },
        )

    def mkdir(self, path: str) -> None:
        normalized = normalize_remote_path(path)
        try:
            self.call("mkdir", {"path": normalized})
        except ApiError as original_exc:
            # 部分 AList 存储对已存在目录返回错误。只有目录确实可列出时才视为成功。
            if self.try_list(normalized, refresh=True) is not None:
                return
            # Quark may return the misleading logical error ``illegal text``
            # after a burst of otherwise valid directory creations.  Retry
            # only that proven transient response, reconciling visibility on
            # every attempt; genuinely invalid names still fail closed after
            # the bounded cooldown.
            if "illegal text" not in str(original_exc).casefold():
                raise
            for attempt in range(6):
                time.sleep(min(1.5 * (2**attempt), 12.0))
                try:
                    self.call("mkdir", {"path": normalized})
                    return
                except ApiError as retry_exc:
                    if self.try_list(normalized, refresh=True) is not None:
                        return
                    original_exc = retry_exc
                    if "illegal text" not in str(retry_exc).casefold():
                        raise
            raise original_exc

    def remove(self, parent: str, names: Sequence[str]) -> None:
        if not names:
            return
        clean_names = [_validate_remote_basename(name) for name in names]
        self.call("remove", {"dir": normalize_remote_path(parent), "names": clean_names})

    def remove_empty_dir(self, path: str) -> bool:
        normalized = normalize_remote_path(path)
        if normalized == "/":
            raise PlanError("拒绝删除 AList 根目录")
        try:
            self.call("remove_empty_directory", {"src_dir": normalized})
            return True
        except ApiError as exc:
            message = str(exc).lower()
            nonempty_markers = (
                "not empty",
                "directory is not empty",
                "目录非空",
                "文件夹非空",
                "不是空目录",
            )
            if any(marker in message for marker in nonempty_markers):
                return False
            raise

    def server_version(self) -> str:
        """返回 AList 公开版本号，不需要管理员 API。"""
        response = self.http.request_json(
            f"{self.base_url}/api/public/settings",
            url_validator=self._validate_api_url,
        )
        self._require_success(response, "AList 版本查询")
        data = response.get("data") or {}
        version = data.get("version") if isinstance(data, Mapping) else None
        if not isinstance(version, str) or not version.strip():
            raise ApiError("AList 未返回可识别版本号")
        return version.strip()

    def archive_meta(
        self, path: str, *, archive_password: str = "", refresh: bool = True
    ) -> dict[str, Any]:
        """只读解析归档目录；密码只发送给 AList，不写入计划。"""
        response: dict[str, Any] | None = None
        for attempt in range(4):
            try:
                response = self.call(
                    "archive/meta",
                    {
                        "path": normalize_remote_path(path),
                        "refresh": refresh,
                        "archive_pass": archive_password,
                    },
                    retryable=True,
                )
                break
            except ApiError as exc:
                if "ExceedMaxConcurrency" not in str(exc) or attempt == 3:
                    raise
                time.sleep(0.5 * (attempt + 1))
        if response is None:
            raise ApiError("AList archive/meta 未返回结果")
        data = response.get("data") or {}
        if not isinstance(data, Mapping):
            raise ApiError("AList archive/meta 返回格式异常")
        return dict(data)

    def archive_member_bytes(
        self,
        archive_path: str,
        inner_path: str,
        *,
        archive_password: str = "",
        archive_metadata: Mapping[str, Any] | None = None,
        max_bytes: int = 64 * 1024 * 1024,
    ) -> bytes:
        """Read one archive member without asking AList to upload it back.

        AList's decompression uploader derives the provider ``format_type`` from
        Go's extension MIME table. Subtitle extensions such as ``.ass`` may
        therefore reach Quark with an empty type and be rejected. Reading the
        member and uploading it through ``fs/put`` lets ScrapeFlow provide an
        explicit, correct content type. The member is data only and is never
        executed.
        """
        clean_inner = "/".join(
            _validate_remote_basename(part)
            for part in inner_path.replace("\\", "/").split("/")
            if part
        )
        if not clean_inner:
            raise ValueError("归档成员路径不能为空")
        meta = (
            dict(archive_metadata)
            if archive_metadata is not None
            else self.archive_meta(
                archive_path,
                archive_password=archive_password,
                refresh=True,
            )
        )
        raw_url = meta.get("raw_url")
        if not isinstance(raw_url, str) or not raw_url:
            raise ApiError("AList archive/meta 未返回成员读取地址")
        parsed = urllib.parse.urlsplit(raw_url)
        if parsed.path != "/ae" and not parsed.path.startswith(("/ae/", "/ad/", "/ap/")):
            raise ApiError("AList archive/meta 返回了无效的成员读取地址")
        base = urllib.parse.urlsplit(self.base_url)
        query = {
            "inner": clean_inner,
            "pass": archive_password,
        }
        sign = meta.get("sign")
        if isinstance(sign, str) and sign:
            query["sign"] = sign
        url = urllib.parse.urlunsplit(
            (
                base.scheme,
                base.netloc,
                parsed.path,
                urllib.parse.urlencode(query),
                "",
            )
        )
        return self._request_bytes_authenticated(
            url,
            max_bytes=max_bytes,
        )

    def file_link(self, path: str, *, refresh: bool = True) -> tuple[str, dict[str, str]]:
        """Return a fresh read-only download link and provider-required headers."""
        response = self.call(
            "get",
            {
                "path": normalize_remote_path(path),
                "password": "",
                "page": 1,
                "per_page": 0,
                "refresh": refresh,
            },
            retryable=False,
        )
        data = response.get("data") or {}
        if not isinstance(data, Mapping):
            raise ApiError("AList fs/get 返回格式异常")
        raw_url = data.get("raw_url")
        if not isinstance(raw_url, str) or not raw_url:
            raise ApiError("AList fs/get 未返回文件下载地址")
        self._validate_download_url(raw_url)
        raw_headers = data.get("header") or {}
        headers = (
            {str(key): str(value) for key, value in raw_headers.items()}
            if isinstance(raw_headers, Mapping)
            else {}
        )
        return raw_url, headers

    def exact_file_info(self, path: str) -> dict[str, object] | None:
        """Return exact-path file identity without trusting a parent listing."""
        normalized = normalize_remote_path(path)
        try:
            response = self.call(
                "get",
                {
                    "path": normalized,
                    "password": "",
                    "page": 1,
                    "per_page": 0,
                    "refresh": True,
                },
                retryable=False,
            )
        except ApiError as exc:
            missing_markers = (
                "not found", "no such file", "not exist",
                "object does not exist", "不存在", "未找到",
            )
            if any(marker in str(exc).casefold() for marker in missing_markers):
                return None
            raise
        data = response.get("data") or {}
        if not isinstance(data, Mapping):
            raise ApiError(f"AList fs/get 返回格式异常: {normalized}")
        if data.get("is_dir"):
            raise ApiError(f"AList exact stat 期望文件但得到目录: {normalized}")
        size = _entry_size_value(data)
        if size is None:
            raise ApiError(f"AList fs/get 未返回有效文件大小: {normalized}")
        expected_name = split_remote(normalized)[1]
        returned_name = data.get("name")
        if isinstance(returned_name, str) and returned_name != expected_name:
            raise ApiError(
                f"AList fs/get 返回了非精确路径文件: "
                f"expected={expected_name!r}, actual={returned_name!r}"
            )
        sha256: str | None = None
        hash_info = data.get("hash_info")
        if isinstance(hash_info, Mapping):
            for key, value in hash_info.items():
                if str(key).casefold().replace("-", "") == "sha256":
                    candidate = str(value).strip().casefold()
                    if re.fullmatch(r"[0-9a-f]{64}", candidate):
                        sha256 = candidate
                    break
        if sha256 is None:
            candidate = str(data.get("sha256") or "").strip().casefold()
            if re.fullmatch(r"[0-9a-f]{64}", candidate):
                sha256 = candidate
        version = _entry_modified_value(data)
        return {"size": size, "sha256": sha256, "version": version}

    @contextlib.contextmanager
    def open_file_reader(self, path: str):
        """Open one non-retrying exact remote read for staging or verification."""
        raw_url, headers = self.file_link(path, refresh=True)
        request = urllib.request.Request(raw_url, headers=headers)
        opener = urllib.request.build_opener(
            ValidatingRedirectHandler(self._validate_download_url)
        )
        response = opener.open(request, timeout=60)
        try:
            yield response
        finally:
            response.close()

    def _validate_download_url(self, url: str) -> None:
        """Allow public provider URLs and the configured AList origin only."""
        try:
            parsed = urllib.parse.urlsplit(url)
            port = parsed.port
        except ValueError as exc:
            raise ApiError("AList 返回了无效的文件下载地址") from exc
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ApiError("AList 返回了无效的文件下载地址")
        if parsed.username is not None or parsed.password is not None:
            raise ApiError("AList 返回的文件地址不得包含账号信息")
        base = urllib.parse.urlsplit(self.base_url)
        effective_port = port or (443 if parsed.scheme == "https" else 80)
        base_port = base.port or (443 if base.scheme == "https" else 80)
        if (
            parsed.scheme == base.scheme
            and parsed.hostname.casefold() == (base.hostname or "").casefold()
            and effective_port == base_port
        ):
            return
        try:
            addresses = {
                row[4][0]
                for row in socket.getaddrinfo(
                    parsed.hostname,
                    effective_port,
                    type=socket.SOCK_STREAM,
                )
            }
        except socket.gaierror as exc:
            raise ApiError("AList 文件下载域名无法解析") from exc
        if not addresses:
            raise ApiError("AList 文件下载域名未返回可用地址")
        for address in addresses:
            ip = ipaddress.ip_address(address)
            if (
                not ip.is_global
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_multicast
                or ip.is_unspecified
            ):
                raise ApiError("AList 文件下载地址指向了不允许的内网或保留网段")
        if effective_port not in {80, 443}:
            raise ApiError("AList 外部文件下载地址使用了不允许的端口")

    def read_file_prefix(self, path: str, *, max_bytes: int = 1024 * 1024) -> bytes:
        """Read only the beginning of a remote file; used for signature checks."""
        raw_url, headers = self.file_link(path, refresh=True)
        headers["Range"] = f"bytes=0-{max_bytes - 1}"
        return self.http.request_bytes(
            raw_url,
            headers=headers,
            max_bytes=max_bytes,
            url_validator=self._validate_download_url,
        )

    def read_file_bytes(self, path: str, *, max_bytes: int) -> bytes:
        """Download one bounded remote file without relying on its MIME type."""
        if max_bytes <= 0:
            raise ValueError("max_bytes 必须大于 0")
        raw_url, headers = self.file_link(path, refresh=True)
        return self.http.request_bytes(
            raw_url,
            headers=headers,
            max_bytes=max_bytes,
            url_validator=self._validate_download_url,
        )

    def download_file_to_path(
        self,
        path: str,
        destination: Path,
        *,
        expected_size: int,
    ) -> None:
        """Stream one remote file to disk with an exact snapshot-size bound."""
        if expected_size <= 0:
            raise ValueError("expected_size 必须大于 0")
        written = destination.stat().st_size if destination.exists() else 0
        if written > expected_size:
            destination.unlink()
            written = 0
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                raw_url, headers = self.file_link(path, refresh=True)
                if written:
                    headers["Range"] = f"bytes={written}-"
                request = urllib.request.Request(raw_url, headers=headers)
                opener = urllib.request.build_opener(
                    ValidatingRedirectHandler(self._validate_download_url)
                )
                with opener.open(request, timeout=60) as response:
                    append = written > 0 and getattr(response, "status", 200) == 206
                    if written and not append:
                        written = 0
                    with destination.open("ab" if append else "wb") as output:
                        while True:
                            chunk = response.read(1024 * 1024)
                            if not chunk:
                                break
                            written += len(chunk)
                            if written > expected_size:
                                raise ApiError(f"远端文件超过计划大小: {path}")
                            output.write(chunk)
                if written == expected_size:
                    return
                last_error = ApiError(
                    f"流式下载大小不匹配: {path}; "
                    f"expected={expected_size}, actual={written}"
                )
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code != 429 and not 500 <= exc.code < 600:
                    raise ApiError(f"流式下载失败，HTTP {exc.code}: {path}") from exc
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                last_error = exc
            if attempt < 5:
                time.sleep(min(2 ** attempt, 16))
        raise ApiError(f"流式下载失败: {path}; {last_error}") from last_error

    def upload_file(
        self,
        target_path: str,
        source: Path,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Stream a local file into AList without loading media into memory."""
        size = source.stat().st_size
        parsed = urllib.parse.urlsplit(self.base_url)
        connection_type = (
            http.client.HTTPSConnection
            if parsed.scheme == "https"
            else http.client.HTTPConnection
        )
        # Cloud providers can take several minutes to commit a multi-gigabyte
        # upload after the request body has been sent.  A fixed 60-second
        # response timeout makes a successful transfer look failed and tempts
        # callers to upload the same media twice.
        response_timeout = max(
            120,
            # AList's Quark driver first caches the complete request and then
            # performs the provider upload before returning.  A 4–5 GiB file
            # can therefore legitimately need well over ten minutes even on a
            # fast local connection to AList.
            min(3600, 300 + int(size / (4 * 1024 * 1024))),
        )
        connection = connection_type(
            parsed.hostname,
            parsed.port,
            timeout=response_timeout,
        )
        endpoint = (parsed.path.rstrip("/") if parsed.path else "") + "/api/fs/put"
        try:
            connection.putrequest("PUT", endpoint)
            for key, value in {
                **self._headers(),
                "File-Path": urllib.parse.quote(normalize_remote_path(target_path)),
                # AList v3.62.0 supports an explicit create-only upload.  The
                # default is overwrite=true, which can overwrite a concurrent
                # object or let a backing provider manufacture ``(1)`` names.
                # Safe file transactions always require the exact requested
                # path to remain unoccupied until this single PUT commits.
                "Overwrite": "false",
                "Content-Type": content_type,
                "Content-Length": str(size),
            }.items():
                connection.putheader(key, value)
            connection.endheaders()
            with source.open("rb") as stream:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    connection.send(chunk)
            response = connection.getresponse()
            body = response.read(1024 * 1024 + 1)
            if len(body) > 1024 * 1024:
                raise ApiError("AList 上传响应过大")
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ApiError(f"AList 上传响应无效: HTTP {response.status}") from exc
            if not isinstance(payload, Mapping):
                raise ApiError("AList 上传响应格式异常")
            self._require_success(payload, "AList 流式上传")
        except (OSError, http.client.HTTPException) as exc:
            target_dir, target_name = split_remote(target_path)
            for delay in (0, 1, 2, 4, 8):
                if delay:
                    time.sleep(delay)
                try:
                    matches = [
                        item
                        for item in (self.try_list(target_dir, refresh=True) or [])
                        if not item.get("is_dir")
                        and _collision_key(str(item.get("name") or ""))
                        == _collision_key(target_name)
                    ]
                except ApiError:
                    continue
                if len(matches) == 1 and _entry_size_value(matches[0]) == size:
                    return
                if matches:
                    break
            raise ApiError(f"AList 流式上传失败: {target_path}; {exc}") from exc
        finally:
            connection.close()

    def archive_decompress(
        self,
        *,
        src_dir: str,
        dst_dir: str,
        name: str,
        archive_password: str = "",
        cache_full: bool = True,
        put_into_new_dir: bool = False,
    ) -> list[dict[str, Any]]:
        response = self.call(
            "archive/decompress",
            {
                "src_dir": normalize_remote_path(src_dir),
                "dst_dir": normalize_remote_path(dst_dir),
                "name": [_validate_remote_basename(name)],
                "archive_pass": archive_password,
                "inner_path": "",
                "cache_full": bool(cache_full),
                "put_into_new_dir": bool(put_into_new_dir),
            },
            retryable=False,
        )
        data = response.get("data") or {}
        tasks = data.get("task") if isinstance(data, Mapping) else None
        if tasks is None:
            return []
        if not isinstance(tasks, list) or not all(isinstance(item, dict) for item in tasks):
            raise ApiError("AList archive/decompress 返回任务格式异常")
        return [dict(item) for item in tasks]

    def archive_tasks(self, kind: str, *, done: bool) -> list[dict[str, Any]]:
        if kind not in {"decompress", "decompress_upload"}:
            raise ValueError(f"无效归档任务类型: {kind}")
        state = "done" if done else "undone"
        response = self._request_json_authenticated(
            f"{self.base_url}/api/task/{kind}/{state}",
        )
        self._require_success(response, f"AList {kind} 任务查询")
        data = response.get("data")
        if data is None:
            return []
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise ApiError(f"AList {kind} 任务返回格式异常")
        return [dict(item) for item in data]

    def upload_bytes(
        self,
        target_path: str,
        data: bytes,
        content_type: str,
        *,
        overwrite: bool = False,
    ) -> None:
        response = self._request_json_authenticated(
            f"{self.base_url}/api/fs/put",
            method="PUT",
            headers={
                "File-Path": urllib.parse.quote(normalize_remote_path(target_path)),
                "Content-Type": content_type,
                "Overwrite": "true" if overwrite else "false",
            },
            raw_body=data,
            retryable=False,
        )
        self._require_success(response, "AList 上传")


class TMDBClient:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str | None = None,
        image_base_url: str | None = None,
        language: str = "zh-CN",
        timeout: float = 20.0,
        retries: int = 5,
        cache_ttl: float = 600.0,
        cache_max_entries: int = 2048,
        proxy_url: str | None = None,
    ) -> None:
        if not api_key:
            raise ScraperError("缺少 TMDB API Key，请设置 TMDB_API_KEY 或使用 --tmdb-key-file")
        self.api_key = api_key
        self.base_url = self._validated_base_url(
            base_url or os.getenv("TMDB_BASE_URL") or DEFAULT_TMDB_BASE,
            "TMDB_BASE_URL",
        )
        self.image_base_url = self._validated_base_url(
            image_base_url or os.getenv("TMDB_IMAGE_BASE_URL") or DEFAULT_IMAGE_BASE,
            "TMDB_IMAGE_BASE_URL",
        )
        self.language = language
        configured_proxy = proxy_url or os.getenv("TMDB_PROXY_URL")
        self.proxy_url = (
            self._validated_proxy_url(configured_proxy)
            if configured_proxy
            else None
        )
        self.http = JsonHttpClient(
            timeout=timeout, retries=retries, proxy_url=self.proxy_url,
        )
        self.cache_ttl = max(0.0, float(cache_ttl))
        self.cache_max_entries = max(0, int(cache_max_entries))
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.request_count = 0
        self.cache_hits = 0

    @staticmethod
    def _validated_base_url(value: str, label: str) -> str:
        try:
            parsed = urllib.parse.urlsplit(value)
        except ValueError as exc:
            raise ScraperError(f"{label} 不是有效 URL") from exc
        if parsed.scheme != "https" or not parsed.hostname:
            raise ScraperError(f"{label} 必须使用可信的 HTTPS 地址")
        if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
            raise ScraperError(f"{label} 不能包含凭据、查询参数或片段")
        return value.rstrip("/")

    @staticmethod
    def _validated_proxy_url(value: str) -> str:
        try:
            parsed = urllib.parse.urlsplit(value)
            _ = parsed.port
        except ValueError as exc:
            raise ScraperError("TMDB_PROXY_URL 不是有效代理 URL") from exc
        if parsed.scheme != "http" or not parsed.hostname:
            raise ScraperError("TMDB_PROXY_URL 必须是可信网络上的 HTTP 代理地址")
        if (
            parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ScraperError("TMDB_PROXY_URL 不得包含凭据、路径、查询参数或片段")
        return value.rstrip("/")

    @staticmethod
    def _actionable_error(exc: ApiError) -> ApiError:
        message = str(exc)
        lowered = message.casefold()
        if exc.status_code in {401, 403}:
            detail = "TMDB API Key 无效或没有访问权限，请检查 TMDB_API_KEY"
        elif exc.status_code == 429:
            detail = "TMDB 请求过于频繁，请稍后重试"
        elif exc.status_code is not None and exc.status_code >= 500:
            detail = "TMDB 服务暂时不可用，请稍后重试"
        elif any(marker in lowered for marker in (
            "name or service not known", "nodename nor servname",
            "temporary failure in name resolution", "getaddrinfo failed",
        )):
            detail = (
                "TMDB 域名解析失败，请检查本机或容器 DNS；"
                "如使用可信的 TMDB 反向代理，可配置 TMDB_BASE_URL"
            )
        elif "timed out" in lowered or "timeout" in lowered:
            detail = "连接 TMDB 超时，请检查网络、代理或 DNS 后重试"
        elif any(marker in lowered for marker in (
            "certificate verify", "cert_verify", "certificate validation",
            "self signed certificate", "hostname mismatch",
        )):
            detail = "TMDB HTTPS 证书校验失败，请检查代理或系统时间"
        elif any(marker in lowered for marker in (
            "unexpected_eof", "unexpected eof", "eof occurred in violation",
            "tls connection", "ssl connection",
        )):
            detail = "TMDB TLS 连接被中途断开，请检查代理、DNS 或网络链路"
        else:
            return exc
        return ApiError(detail, status_code=exc.status_code)

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        if not path.startswith("/"):
            path = "/" + path
        query = {"api_key": self.api_key, "language": self.language}
        query.update({key: value for key, value in params.items() if value is not None})
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(sorted(query.items()), doseq=True)}"
        cache_key = url
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None:
            expires_at, value = cached
            if now < expires_at:
                self.cache_hits += 1
                _trace_io(f"tmdb.get cache path={path!r} language={query.get('language')!r}")
                return copy.deepcopy(value)
            self._cache.pop(cache_key, None)
        try:
            self.request_count += 1
            _trace_io(f"tmdb.get start path={path!r} language={query.get('language')!r}")
            response = self.http.request_json(
                url, headers={"User-Agent": f"alist-tmdb-scraper/{__version__}"}
            )
            _trace_io(f"tmdb.get done path={path!r} language={query.get('language')!r}")
        except ApiError as exc:
            raise self._actionable_error(exc) from exc
        if response.get("success") is False:
            status_code = response.get("status_code")
            raise ApiError(
                "TMDB 请求失败: "
                + _redact_sensitive_text(
                    str(response.get("status_message") or response), (self.api_key,)
                ),
                status_code=int(status_code) if isinstance(status_code, int) else None,
            )
        if self.cache_ttl > 0 and self.cache_max_entries > 0:
            if len(self._cache) >= self.cache_max_entries:
                oldest_key = min(self._cache, key=lambda key: self._cache[key][0])
                self._cache.pop(oldest_key, None)
            self._cache[cache_key] = (now + self.cache_ttl, copy.deepcopy(response))
        return copy.deepcopy(response)

    def cache_report(self) -> dict[str, int | float]:
        return {
            "request_count": self.request_count,
            "cache_hits": self.cache_hits,
            "cache_entries": len(self._cache),
            "ttl_seconds": self.cache_ttl,
        }

    def download_poster(self, poster_path: str) -> bytes:
        try:
            return self.http.request_bytes(f"{self.image_base_url}{poster_path}")
        except ApiError as exc:
            raise self._actionable_error(exc) from exc


# ---------------------------------------------------------------------------
# 兼容旧辅助脚本的函数式接口。凭据与 URL 必须来自环境变量或参数。
# ---------------------------------------------------------------------------

ALIST = os.getenv("ALIST_URL", DEFAULT_ALIST_URL)
TMDB_BASE = os.getenv("TMDB_BASE_URL", DEFAULT_TMDB_BASE)
TMDB_KEY = os.getenv("TMDB_API_KEY", "")
_COMPAT_CLIENTS: dict[str, AListClient] = {}


def alist_login(
    password: str,
    username: str = "admin",
    alist_url: str | None = None,
    *,
    allow_insecure_http: bool = False,
) -> str:
    client = AListClient(
        alist_url or ALIST,
        username,
        password,
        allow_insecure_http=allow_insecure_http,
    )
    token = client.login()
    _COMPAT_CLIENTS[token] = client
    return token


def _compat_client(token: str) -> AListClient:
    client = _COMPAT_CLIENTS.get(token)
    if client is None:
        client = AListClient(ALIST, "admin", "")
        client.token = token
        _COMPAT_CLIENTS[token] = client
    return client


def alist_list(token: str, path: str, refresh: bool = False) -> list[dict[str, Any]]:
    return _compat_client(token).list(path, refresh=refresh)


def alist_walk(token: str, path: str) -> list[dict[str, Any]]:
    return _compat_client(token).walk(path)


def alist_rename(token: str, full_path: str, new_name: str) -> dict[str, Any]:
    del token, full_path, new_name
    raise ScraperError(
        "旧 alist_rename 裸写接口已永久移除；请使用带本机 payload、完整 SHA-256 "
        "回读和 journal 的文件事务"
    )


def alist_move(token: str, src_dir: str, dst_dir: str, names: Sequence[str]) -> dict[str, Any]:
    del token, src_dir, dst_dir, names
    raise ScraperError(
        "旧 alist_move 裸写接口已永久移除；请使用带本机 payload、完整 SHA-256 "
        "回读和 journal 的文件事务"
    )


def alist_mkdir(token: str, path: str) -> dict[str, Any]:
    del token, path
    raise ScraperError("旧 alist_mkdir 裸写接口已永久移除")


def alist_remove(token: str, parent: str, names: Sequence[str]) -> dict[str, Any]:
    del token, parent, names
    raise ScraperError(
        "旧 alist_remove 裸删除接口已永久移除；媒体删除必须使用 "
        "hybrid delete 封存批次"
    )


def tmdb(path: str, lang: str = "zh-CN") -> dict[str, Any]:
    api_key = os.getenv("TMDB_API_KEY", TMDB_KEY)
    if not api_key:
        raise ScraperError("缺少 TMDB_API_KEY")
    if "?" in path:
        base_path, query_text = path.split("?", 1)
        params = dict(urllib.parse.parse_qsl(query_text))
    else:
        base_path, params = path, {}
    return TMDBClient(api_key, language=lang).get(base_path, **params)


def resolve_poster_target(
    alist: AListClient,
    target_dir: str,
    *,
    overwrite: bool,
) -> tuple[str, bool]:
    """返回实际海报路径及是否覆盖已有文件，并阻止大小写/Unicode 歧义。"""
    normalized_dir = normalize_remote_path(target_dir)
    content = alist.try_list(normalized_dir, refresh=True)
    if content is None:
        raise PlanError(f"目标目录不存在或不可读: {normalized_dir}")
    matches = [
        entry
        for entry in content
        if isinstance(entry.get("name"), str)
        and _collision_key(str(entry["name"])) == _collision_key("folder.jpg")
    ]
    if any(entry.get("is_dir") for entry in matches):
        raise PlanError(f"目标目录存在名为 folder.jpg 的目录: {normalized_dir}")
    file_matches = [entry for entry in matches if not entry.get("is_dir")]
    if len(file_matches) > 1:
        raise PlanError(
            f"目标目录存在多个大小写或 Unicode 等价的 folder.jpg，拒绝选择: {normalized_dir}"
        )
    actual_name = str(file_matches[0]["name"]) if file_matches else "folder.jpg"
    return join_remote(normalized_dir, actual_name), bool(file_matches)


def resolve_artwork_target(
    alist: AListClient, target_path: str
) -> tuple[str, bool]:
    normalized = normalize_remote_path(target_path)
    target_dir, requested_name = split_remote(normalized)
    content = alist.try_list(target_dir, refresh=True)
    if content is None:
        raise PlanError(f"图稿目标目录不存在或不可读: {target_dir}")
    matches = [
        entry
        for entry in content
        if isinstance(entry.get("name"), str)
        and _collision_key(str(entry["name"])) == _collision_key(requested_name)
    ]
    if any(entry.get("is_dir") for entry in matches):
        raise PlanError(f"图稿目标被同名目录占用: {normalized}")
    file_matches = [entry for entry in matches if not entry.get("is_dir")]
    if len(file_matches) > 1:
        raise PlanError(f"存在多个大小写或 Unicode 等价图稿，拒绝选择: {normalized}")
    actual_name = str(file_matches[0]["name"]) if file_matches else requested_name
    return join_remote(target_dir, actual_name), bool(file_matches)


def planned_artwork(plan: Plan) -> list[tuple[str, str, str]]:
    """返回 (目标路径, TMDB 图片路径, 用途)，并按冲突键去重。"""
    requests: list[tuple[str, str, str]] = []
    primary_root = (
        str(plan.metadata.get("series_root"))
        if plan.mode == "mixed" and plan.metadata.get("series_root")
        else plan.target_root
    )
    poster_path = plan.metadata.get("poster_path")
    backdrop_path = plan.metadata.get("backdrop_path")
    if isinstance(poster_path, str) and poster_path:
        requests.append((join_remote(primary_root, "folder.jpg"), poster_path, "folder"))
        if plan.mode in {"tv", "mixed"}:
            requests.append((join_remote(primary_root, "poster.jpg"), poster_path, "series-poster"))
        elif plan.mode == "movie":
            for item in plan.files:
                if item.media_kind != "video" or is_planned_bonus(item.final_name):
                    continue
                requests.append(
                    (
                        join_remote(item.target_dir, f"{Path(item.final_name).stem}.jpg"),
                        poster_path,
                        "movie-poster",
                    )
                )
    if isinstance(backdrop_path, str) and backdrop_path:
        requests.append((join_remote(primary_root, "fanart.jpg"), backdrop_path, "fanart"))
    season_posters = plan.metadata.get("season_posters")
    if plan.mode in {"tv", "mixed"} and isinstance(season_posters, Mapping):
        for season_number, image_path in season_posters.items():
            if isinstance(image_path, str) and image_path:
                requests.append(
                    (
                        join_remote(primary_root, f"season {int(season_number)}-poster.jpg"),
                        image_path,
                        "season-poster",
                    )
                )
    member_posters = plan.metadata.get("member_posters")
    if plan.mode in {"collection", "mixed", "batch"} and isinstance(member_posters, Mapping):
        planned_videos = {
            _collision_key(join_remote(item.target_dir, item.final_name)): item
            for item in plan.files
            if item.media_kind == "video" and not is_planned_bonus(item.final_name)
        }
        for target, image_path in member_posters.items():
            if not isinstance(target, str) or not isinstance(image_path, str) or not image_path:
                continue
            normalized_target = normalize_remote_path(target)
            flat_video = planned_videos.get(_collision_key(normalized_target))
            if flat_video is not None:
                requests.append(
                    (
                        join_remote(
                            flat_video.target_dir,
                            f"{Path(flat_video.final_name).stem}.jpg",
                        ),
                        image_path,
                        "member-movie-poster",
                    )
                )
                continue
            requests.append((join_remote(normalized_target, "folder.jpg"), image_path, "member-folder"))
            for item in plan.files:
                if item.target_dir == normalized_target and item.media_kind == "video":
                    requests.append(
                        (
                            join_remote(normalized_target, f"{Path(item.final_name).stem}.jpg"),
                            image_path,
                            "member-movie-poster",
                        )
                    )
    member_tv = plan.metadata.get("member_tv")
    if plan.mode == "batch" and isinstance(member_tv, Mapping):
        # A franchise root is itself rendered as a shelf item by Infuse.  Batch
        # plans normally have no single TMDB identity, so use the shallowest
        # deterministic TV member as representative artwork instead of leaving
        # the parent as a blank folder.  Member-specific artwork below remains
        # authoritative for every actual show/movie.
        representative_tv: Mapping[str, Any] | None = None
        for series_root, identity in sorted(
            member_tv.items(),
            key=lambda pair: (
                normalize_remote_path(str(pair[0])).count("/"),
                _collision_key(str(pair[0])),
            ),
        ):
            if not isinstance(series_root, str) or not isinstance(identity, Mapping):
                continue
            if isinstance(identity.get("poster_path"), str) and identity.get("poster_path"):
                representative_tv = identity
                break
        if representative_tv is not None:
            representative_poster = str(representative_tv["poster_path"])
            requests.extend(
                [
                    (join_remote(plan.target_root, "folder.jpg"), representative_poster, "batch-folder"),
                    (join_remote(plan.target_root, "poster.jpg"), representative_poster, "batch-poster"),
                ]
            )
            representative_backdrop = representative_tv.get("backdrop_path")
            if isinstance(representative_backdrop, str) and representative_backdrop:
                requests.append(
                    (join_remote(plan.target_root, "fanart.jpg"), representative_backdrop, "batch-fanart")
                )
        for series_root, identity in member_tv.items():
            if not isinstance(series_root, str) or not isinstance(identity, Mapping):
                continue
            member_poster = identity.get("poster_path")
            member_backdrop = identity.get("backdrop_path")
            if isinstance(member_poster, str) and member_poster:
                requests.extend(
                    [
                        (join_remote(series_root, "folder.jpg"), member_poster, "folder"),
                        (join_remote(series_root, "poster.jpg"), member_poster, "series-poster"),
                    ]
                )
            if isinstance(member_backdrop, str) and member_backdrop:
                requests.append(
                    (join_remote(series_root, "fanart.jpg"), member_backdrop, "fanart")
                )
            raw_seasons = identity.get("season_posters")
            if isinstance(raw_seasons, Mapping):
                for season_number, image_path in raw_seasons.items():
                    if isinstance(image_path, str) and image_path:
                        requests.append(
                            (
                                join_remote(series_root, f"season {int(season_number)}-poster.jpg"),
                                image_path,
                                "season-poster",
                            )
                        )
    deduplicated: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for request in requests:
        key = _collision_key(request[0])
        if key not in seen:
            seen.add(key)
            deduplicated.append(request)
    return deduplicated


def planned_movie_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    if plan.mode not in {"movie", "collection", "mixed", "batch"}:
        return []
    member_movies = plan.metadata.get("member_movies")
    member_movies = member_movies if isinstance(member_movies, Mapping) else {}
    output: list[tuple[str, bytes]] = []
    for item in plan.files:
        if item.media_kind != "video" or is_planned_bonus(item.final_name):
            continue
        stem = Path(item.final_name).stem
        target_path = join_remote(item.target_dir, item.final_name)
        identity = (
            plan.metadata
            if plan.mode == "movie"
            else (
                member_movies.get(normalize_remote_path(target_path))
                or member_movies.get(normalize_remote_path(item.target_dir))
            )
        )
        if isinstance(identity, Mapping):
            raw_tmdb_id = identity.get("tmdb_id")
            raw_title = identity.get("title")
            raw_year = identity.get("year")
            if (
                isinstance(raw_tmdb_id, int)
                and not isinstance(raw_tmdb_id, bool)
                and raw_tmdb_id > 0
                and isinstance(raw_title, str)
                and raw_title
                and isinstance(raw_year, str)
            ):
                tmdb_id = str(raw_tmdb_id)
                title = raw_title
                year = raw_year
            else:
                identity = None
        if not isinstance(identity, Mapping):
            # Backward compatibility for plans created before clean folder names.
            id_match = re.search(r"\{tmdb-(\d+)\}", stem, re.IGNORECASE)
            year_match = re.search(r"\(((?:19|20)\d{2})\)", stem)
            if not id_match:
                continue
            tmdb_id = id_match.group(1)
            title = re.sub(r"\s*\((?:19|20)\d{2}\).*", "", stem).strip()
            year = year_match.group(1) if year_match else ""
        payload = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            "<movie>\n"
            f"  <title>{html.escape(title)}</title>\n"
            f"  <year>{html.escape(year)}</year>\n"
            f"  <tmdbid>{tmdb_id}</tmdbid>\n"
            f"  <uniqueid type=\"tmdb\" default=\"true\">{tmdb_id}</uniqueid>\n"
            "</movie>\n"
        ).encode("utf-8")
        output.append((join_remote(item.target_dir, f"{stem}.nfo"), payload))
    return output


def planned_tv_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    if plan.mode == "batch":
        output: list[tuple[str, bytes]] = []
        members = plan.metadata.get("member_tv")
        if not isinstance(members, Mapping):
            return output
        for series_root, identity in members.items():
            if not isinstance(series_root, str) or not isinstance(identity, Mapping):
                continue
            tmdb_id = identity.get("tmdb_id")
            title = identity.get("title")
            year = identity.get("year")
            if (
                isinstance(tmdb_id, bool)
                or not isinstance(tmdb_id, int)
                or tmdb_id <= 0
                or not isinstance(title, str)
                or not title
                or not isinstance(year, str)
            ):
                continue
            payload = (
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                "<tvshow>\n"
                f"  <title>{html.escape(title)}</title>\n"
                f"  <year>{html.escape(year)}</year>\n"
                f"  <tmdbid>{tmdb_id}</tmdbid>\n"
                f"  <uniqueid type=\"tmdb\" default=\"true\">{tmdb_id}</uniqueid>\n"
                "</tvshow>\n"
            ).encode("utf-8")
            output.append((join_remote(series_root, "tvshow.nfo"), payload))
        return output
    if plan.mode not in {"tv", "mixed"}:
        return []
    tmdb_id = plan.metadata.get("tmdb_id")
    title = plan.metadata.get("title")
    year = plan.metadata.get("year")
    if (
        isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
        or not isinstance(title, str)
        or not title
        or not isinstance(year, str)
    ):
        return []
    series_root = (
        str(plan.metadata.get("series_root"))
        if plan.mode == "mixed" and plan.metadata.get("series_root")
        else plan.target_root
    )
    payload = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<tvshow>\n"
        f"  <title>{html.escape(title)}</title>\n"
        f"  <year>{html.escape(year)}</year>\n"
        f"  <tmdbid>{tmdb_id}</tmdbid>\n"
        f"  <uniqueid type=\"tmdb\" default=\"true\">{tmdb_id}</uniqueid>\n"
        "</tvshow>\n"
    ).encode("utf-8")
    return [(join_remote(series_root, "tvshow.nfo"), payload)]


def planned_tv_episode_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    """Generate one deterministic episode sidecar for every planned TV video.

    The plan currently carries no authoritative TMDB episode object ID, so the
    sidecar deliberately does not invent one.  Season/episode/range, canonical
    show identity, title and year are still sufficient for a correct local NFO
    and keep the producer aligned with the completion contract.
    """
    identities: list[tuple[str, Mapping[str, Any]]] = []
    if plan.mode == "batch":
        members = plan.metadata.get("member_tv")
        if isinstance(members, Mapping):
            identities.extend(
                (normalize_remote_path(root), identity)
                for root, identity in members.items()
                if isinstance(root, str) and isinstance(identity, Mapping)
            )
    elif plan.mode in {"tv", "mixed"}:
        root = (
            str(plan.metadata.get("series_root"))
            if plan.mode == "mixed" and plan.metadata.get("series_root")
            else plan.target_root
        )
        identities.append((normalize_remote_path(root), plan.metadata))
    if not identities:
        return []

    output: list[tuple[str, bytes]] = []
    seen: set[str] = set()
    for item in plan.files:
        if item.media_kind != "video" or is_planned_bonus(item.final_name):
            continue
        target_dir = normalize_remote_path(item.target_dir)
        matches = [
            pair for pair in identities
            if _path_is_within(target_dir, pair[0])
        ]
        if not matches:
            continue
        series_root, identity = max(matches, key=lambda pair: len(pair[0]))
        token = re.search(
            r"(?:^|[ ._-])S0*(\d{1,3})E0*(\d{1,4})(?:-E0*(\d{1,4}))?(?:$|[ ._-])",
            Path(item.final_name).stem,
            re.IGNORECASE,
        )
        if token is None:
            continue
        season = int(token.group(1))
        episode = int(token.group(2))
        end_episode = int(token.group(3)) if token.group(3) else episode
        show_title = str(identity.get("title") or split_remote(series_root)[1]).strip()
        year = str(identity.get("year") or "").strip()
        stem = Path(item.final_name).stem
        title_tail = re.split(
            r"\s+-\s+S\d{2,3}E\d{2,4}(?:-E\d{2,4})?\s+-\s+",
            stem,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        episode_title = title_tail[1].strip() if len(title_tail) == 2 else stem
        target = join_remote(item.target_dir, f"{stem}.nfo")
        key = _collision_key(target)
        if key in seen:
            raise PlanError(f"多个视频生成同一集 NFO 目标: {target}")
        seen.add(key)
        range_fields = (
            f"  <displayepisode>{episode}-{end_episode}</displayepisode>\n"
            if end_episode != episode else ""
        )
        payload = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            "<episodedetails>\n"
            f"  <title>{html.escape(episode_title)}</title>\n"
            f"  <showtitle>{html.escape(show_title)}</showtitle>\n"
            f"  <year>{html.escape(year)}</year>\n"
            f"  <season>{season}</season>\n"
            f"  <episode>{episode}</episode>\n"
            f"{range_fields}"
            "</episodedetails>\n"
        ).encode("utf-8")
        output.append((target, payload))
    return output


def planned_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    return [
        *planned_tv_nfos(plan),
        *planned_tv_episode_nfos(plan),
        *planned_movie_nfos(plan),
    ]


def download_poster(
    token: str,
    poster_path: str | None,
    target_dir: str,
    *,
    overwrite: bool = False,
) -> bool:
    """已永久移除的裸海报写入兼容接口。"""
    del token, poster_path, target_dir, overwrite
    raise ScraperError(
        "旧 download_poster 裸写接口已永久移除；图稿必须经签名计划和 "
        "exact local upload transaction"
    )


# ---------------------------------------------------------------------------
# 路径、命名与集数识别
# ---------------------------------------------------------------------------


def normalize_remote_path(path: str) -> str:
    if not isinstance(path, str):
        raise ValueError("远端路径必须是字符串")
    parts: list[str] = []
    for part in path.replace("\\", "/").split("/"):
        if not part:
            continue
        if part in {".", ".."}:
            raise ValueError(f"远端路径不能包含 {part!r} 段: {path}")
        if _has_unsafe_unicode(part):
            raise ValueError("远端路径不能包含控制或不可见格式字符")
        parts.append(part)
    return "/" + "/".join(parts) if parts else "/"


def join_remote(parent: str, name: str) -> str:
    return normalize_remote_path(f"{normalize_remote_path(parent).rstrip('/')}/{name}")


def _preserve_equivalent_source_root(src_path: str, desired_root: str) -> str:
    """Keep the existing directory spelling when only case/Unicode form differs."""
    source_root = normalize_remote_path(src_path).rstrip("/") or "/"
    target_root = normalize_remote_path(desired_root).rstrip("/") or "/"
    if source_root != target_root and _collision_key(source_root) == _collision_key(target_root):
        return source_root
    return target_root


def _nfo_tmdb_ids(
    alist: AListClient,
    directory: str,
    *,
    tv: bool,
    refresh: bool = True,
) -> set[int]:
    entries = alist.try_list(directory, refresh=refresh)
    if entries is None:
        return set()
    candidates = [
        str(item["name"])
        for item in entries
        if not item.get("is_dir")
        and isinstance(item.get("name"), str)
        and str(item["name"]).casefold().endswith(".nfo")
        and (not tv or str(item["name"]).casefold() == "tvshow.nfo")
    ][:20]
    identities: set[int] = set()
    for name in candidates:
        try:
            payload = alist.read_file_bytes(join_remote(directory, name), max_bytes=2 * 1024 * 1024)
            lowered = payload.lower()
            if b"<!doctype" in lowered or b"<!entity" in lowered:
                continue
            root = ET.fromstring(payload)
        except (ApiError, ET.ParseError, ValueError):
            continue
        for element in root.iter():
            tag = str(element.tag).rsplit("}", 1)[-1].casefold()
            tmdb_unique = tag == "uniqueid" and str(element.attrib.get("type", "")).casefold() == "tmdb"
            if tag != "tmdbid" and not tmdb_unique:
                continue
            value = (element.text or "").strip()
            if value.isdigit() and int(value) > 0:
                identities.add(int(value))
    return identities


def _library_root_contains_any_file(
    alist: AListClient,
    root: str,
    *,
    max_directories: int = 128,
) -> bool:
    """Conservatively distinguish an empty preflight tree from real media.

    A failed transaction can leave only the directories it created before the
    first file move.  Such an empty same-name tree has no competing identity
    and is safe to reuse on the automatic retry.  Unreadable or unexpectedly
    large trees return ``True`` so real library content still requires NFO
    evidence.
    """
    stack = [normalize_remote_path(root)]
    visited: set[str] = set()
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        if len(visited) >= max_directories:
            return True
        visited.add(current)
        rows = alist.try_list(current, refresh=True)
        if rows is None:
            return True
        for row in rows:
            name = row.get("name")
            if not isinstance(name, str) or not name:
                return True
            if row.get("is_dir"):
                stack.append(join_remote(current, name))
            else:
                return True
    return False


def resolve_existing_library_root(
    alist: AListClient,
    *,
    parent_path: str,
    desired_root: str,
    tmdb_id: int,
    tv: bool,
) -> tuple[str, str]:
    """Reuse a same-id library folder and block identity-unsafe merges."""
    parent = normalize_remote_path(parent_path)
    desired = normalize_remote_path(desired_root)
    # Refreshing a large provider root and then force-refreshing every child
    # can block a small follow-up job for many minutes.  A cached parent
    # listing is enough to discover names; the matching destination itself is
    # still refreshed below before its NFO identity is trusted.
    entries = alist.try_list(parent, refresh=False)
    if entries is None:
        return desired, "new"
    directory_roots = [
        join_remote(parent, str(entry["name"]))
        for entry in entries
        if entry.get("is_dir") and isinstance(entry.get("name"), str)
    ]
    equivalent_roots = [
        candidate
        for candidate in directory_roots
        if _collision_key(candidate) == _collision_key(desired)
    ]
    if len(equivalent_roots) > 1:
        raise PlanError(
            "媒体库中有多个名称等价的目标目录，拒绝自动选择: "
            + ", ".join(equivalent_roots)
        )
    if equivalent_roots:
        equivalent_root = equivalent_roots[0]
        identities = _nfo_tmdb_ids(alist, equivalent_root, tv=tv, refresh=True)
        if identities and identities != {tmdb_id}:
            raise PlanError(
                f"目标目录 NFO 的 TMDB 身份与本次计划不同，拒绝合并: "
                f"{equivalent_root}; existing={sorted(identities)}, planned={tmdb_id}"
            )
        if identities == {tmdb_id}:
            return equivalent_root, "same_tmdb_id"
        if not _library_root_contains_any_file(alist, equivalent_root):
            return equivalent_root, "empty_without_nfo"
        return equivalent_root, "matching_name_without_nfo"

    # No same-name destination exists.  Retain the legacy renamed-library
    # discovery, but only use non-refreshing child listings.  A positive NFO
    # identity is still cryptographic content evidence; a stale miss merely
    # creates a new desired directory and cannot overwrite an unrelated one.
    same_id_roots: list[str] = []
    for candidate in directory_roots:
        identities = _nfo_tmdb_ids(alist, candidate, tv=tv, refresh=False)
        if tmdb_id in identities and identities != {tmdb_id}:
            raise PlanError(
                f"NFO 同时声明多个 TMDB 身份，拒绝自动合并: "
                f"{candidate}; identities={sorted(identities)}"
            )
        if identities == {tmdb_id}:
            same_id_roots.append(candidate)
    unique_same_id = list(dict.fromkeys(same_id_roots))
    if len(unique_same_id) > 1:
        raise PlanError(
            f"媒体库中有多个目录声明同一 TMDB ID {tmdb_id}，拒绝自动选择: "
            + ", ".join(unique_same_id)
        )
    if unique_same_id:
        return unique_same_id[0], "same_tmdb_id"
    return desired, "new"


def split_remote(path: str) -> tuple[str, str]:
    normalized = normalize_remote_path(path).rstrip("/")
    parent, _, name = normalized.rpartition("/")
    return parent or "/", name


def _truncate_utf8(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    output: list[str] = []
    used = 0
    for char in text:
        size = len(char.encode("utf-8"))
        if used + size > max_bytes:
            break
        output.append(char)
        used += size
    return "".join(output).rstrip(" .")


def safe_name(name: str, max_bytes: int = 160) -> str:
    normalized = unicodedata.normalize("NFC", str(name))
    cleaned = "".join(
        "-" if unicodedata.category(char) in {"Cc", "Cf", "Cs"} else char
        for char in normalized
    )
    cleaned = re.sub(r"[/\\:*?\"<>|]", "-", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    cleaned = _truncate_utf8(cleaned, max_bytes)
    return cleaned or "未命名"


def provider_safe_episode_title(name: str) -> str:
    """Normalize episode-title tokens rejected by Quark rename APIs.

    Quark accepts existing uploaded names containing ``OVA`` but rejects a
    new rename containing that token with ``illegal text``.  Keep the Infuse
    season/episode identity in ``S00E##`` and translate only the descriptive
    title, preserving the official suffix such as JACK/PINTO.
    """
    normalized = str(name)
    normalized = re.sub(
        r"(?<![A-Za-z0-9])OVA\s*0*(\d+)(?![A-Za-z0-9])",
        lambda match: f"特别篇 {int(match.group(1))}",
        normalized,
        flags=re.I,
    )
    normalized = re.sub(
        r"(?<![A-Za-z0-9])OVA(?![A-Za-z0-9])",
        "特别篇",
        normalized,
        flags=re.I,
    )
    return safe_name(normalized)


def _limit_filename(name: str, max_bytes: int = 240) -> str:
    suffix = Path(name).suffix
    stem = name[: -len(suffix)] if suffix else name
    budget = max_bytes - len(suffix.encode("utf-8"))
    if budget <= 0:
        raise PlanError(f"文件扩展名过长: {name}")
    return _truncate_utf8(stem, budget) + suffix


def _compose_filename(base: str, semantic_suffix: str, extension: str, max_bytes: int = 240) -> str:
    tail = f"{semantic_suffix}{extension}"
    budget = max_bytes - len(tail.encode("utf-8"))
    if budget <= 0:
        raise PlanError(f"文件名后缀过长: {tail}")
    limited_base = _truncate_utf8(base, budget)
    return f"{limited_base}{tail}"


def is_scraper_temp(name: str) -> bool:
    return _collision_key(name).startswith(".scraper-tmp-")


def is_scraper_lock(name: str) -> bool:
    return _collision_key(name).startswith(LOCK_PREFIX)


def should_ignore_extra(name: str) -> bool:
    return bool(IGNORED_EXTRA_TAG_RE.search(name))


def is_sample(name: str) -> bool:
    return bool(SAMPLE_RE.search(name))


def bonus_type(name: str) -> str | None:
    for label, pattern in BONUS_PATTERNS:
        if pattern.search(name):
            return label
    return None


def is_planned_bonus(name: str) -> bool:
    """仅识别本工具生成的 Infuse 花絮后缀。

    不能直接对标准化后的片名再调用 bonus_type，否则《The Trailer》
    之类本身含花絮关键词的电影会被错当成预告。
    """
    return bool(PLANNED_BONUS_SUFFIX_RE.search(Path(name).stem))


def edition_tag(name: str) -> str | None:
    explicit = re.search(r"\{edition-([^{}]+)\}", name, re.IGNORECASE)
    if explicit:
        return safe_name(explicit.group(1), max_bytes=60)
    for label, pattern in EDITION_PATTERNS:
        if pattern.search(name):
            return label
    return None


def entry_edition_tag(item: Mapping[str, Any]) -> str | None:
    """Detect a named edition from either the file or its enclosing folder."""
    override = item.get("_edition_override")
    if isinstance(override, str) and override.strip():
        return safe_name(override.strip(), max_bytes=60)
    return edition_tag(
        f"{item.get('name', '')} {item.get('full_path', '')}"
    )


def _token_present(text: str, marker: str) -> bool:
    if marker.isascii():
        return bool(
            re.search(
                rf"(?:^|[.\-_\[\]()\s]){re.escape(marker)}(?:$|[.\-_\[\]()\s])",
                text,
            )
        )
    return marker in text


def subtitle_language(name: str) -> str | None:
    lower = name.lower()
    if any(_token_present(lower, marker) for marker in SIMPLIFIED_MARKERS):
        return "zh-CN"
    if re.search(r"(?:^|[.\-_\[\]()\s])(?:sc|简)(?:$|[.\-_\[\]()\s])", lower):
        return "zh-CN"
    if any(_token_present(lower, marker) for marker in TRADITIONAL_MARKERS):
        return "zh-TW"
    if re.search(r"(?:^|[.\-_\[\]()\s])(?:tc|繁)(?:$|[.\-_\[\]()\s])", lower):
        return "zh-TW"
    if any(_token_present(lower, marker) for marker in ENGLISH_MARKERS):
        return "en"
    if any(_token_present(lower, marker) for marker in JAPANESE_MARKERS):
        return "ja"
    return None


def is_traditional_sub(name: str) -> bool:
    return subtitle_language(name) == "zh-TW"


def is_simplified_sub(name: str) -> bool:
    return subtitle_language(name) == "zh-CN"


ROMAN_MAP = {
    "Ⅰ": "1",
    "Ⅱ": "2",
    "Ⅲ": "3",
    "Ⅳ": "4",
    "Ⅴ": "5",
    "Ⅵ": "6",
    "Ⅶ": "7",
    "Ⅷ": "8",
    "Ⅸ": "9",
    "Ⅹ": "10",
}


def extract_episode_key(text: str) -> EpisodeKey | None:
    clean = DATE_NOISE_RE.sub(" ", text)
    clean = EPISODE_NOISE_RE.sub(" ", clean)
    for roman, arabic in ROMAN_MAP.items():
        clean = clean.replace(roman, f" {arabic} ")

    # A canonical leading/embedded SxxExx token is already an exact routing
    # identity.  Do not let a later descriptive release token such as
    # ``OVA 01`` replace ``S00E07`` and collide with an unrelated special.
    explicit_season_episode = re.search(
        r"(?:^|[^A-Za-z0-9])S\s*0*(\d{1,3})\s*E\s*0*(\d{1,4})"
        r"(?:$|[^0-9])",
        clean,
        re.IGNORECASE,
    )
    if explicit_season_episode:
        season = int(explicit_season_episode.group(1))
        episode = int(explicit_season_episode.group(2))
        return EpisodeKey("special" if season == 0 else "regular", episode)

    for pattern in (SEASON_LOCAL_ABSOLUTE_RE, DUAL_BRACKET_EPISODE_RE):
        dual_match = pattern.search(clean)
        if dual_match and int(dual_match.group(2)) > int(dual_match.group(1)):
            return EpisodeKey("regular", int(dual_match.group(1)))

    # Release groups use decimal episode labels for interludes, recaps and
    # sometimes ordinary web extras. Preserve the complete decimal token.
    # Whether it is a TMDB special is decided later from official metadata;
    # the parser must not turn every decimal into Season 00.
    fractional_match = re.search(
        r"(?:^|[\[\s_\-(])0*(\d{1,3})\.(\d{1,3})(?:v\d+)?"
        r"(?:[\]\s_\-.)]|$)",
        clean,
        re.IGNORECASE,
    )
    if fractional_match:
        digits = fractional_match.group(2).rstrip("0") or "0"
        return EpisodeKey(
            "fractional",
            int(fractional_match.group(1)),
            fractional_digits=digits,
        )

    special_patterns = [
        r"\[\s*(?:SP|SPECIAL|OVA|OAV|OAD)\s*\]\s*\[\s*0*(\d{1,3})\s*\]",
        r"(?:^|[\s._\-\[\]()])(?:OVA|OAV|OAD)[\s._-]*(?:SERIES|系列)[\s._-]*\[?\s*0*(\d{1,3})\s*\]?(?:$|[\s._\-\[\]()])",
        r"(?:^|[\s._\-\[\]()])(?:SP|SPECIAL|OVA|OAV|OAD)[\s._-]*[\[(]\s*0*(\d{1,3})\s*[\])](?:$|[\s._\-\[\]()])",
        r"(?:^|[\s._\-\[\]()])(?:SP|SPECIAL|OVA|OAV|OAD)[\s._-]*0*(\d{1,3})(?:$|[\s._\-\[\]()])",
        r"(?:^|[\s._\-\[\]()])0*(\d{1,3})[\s._-]*(?:OVA|OAV|OAD)(?:$|[\s._\-\[\]()])",
        r"(?:^|[\s._\-\[\]()])TOKUTEN[ ._-]*ANIME[ ._-]*0*(\d{1,3})(?:$|[\s._\-\[\]()])",
        r"第\s*0*(\d{1,3})\s*(?:话|集)?\s*(?:特别篇|特典)",
    ]
    for pattern in special_patterns:
        match = re.search(pattern, clean, re.IGNORECASE)
        if match:
            return EpisodeKey("special", int(match.group(1)))

    if re.search(
        r"(?:^|[\s._\-\[\]()])(?:SP|SPECIAL|OVA|OAV|OAD)(?:$|[\s._\-\[\]()])",
        clean,
        re.IGNORECASE,
    ):
        # 0 表示标签明确但编号缺失；计划阶段必须拒绝猜测。
        return EpisodeKey("special", 0)

    # 字幕组常把修正版写成 [02v2]。v2 是文件修订号，02 才是集号。
    versioned_episode = re.search(
        r"(?:^|[\[\s_\-(])0*(\d{1,3})v\d+(?=[\]\s_\-.()]|$)",
        clean,
        re.IGNORECASE,
    )
    if versioned_episode:
        return EpisodeKey("regular", int(versioned_episode.group(1)))

    hash_episode = re.search(
        r"(?<![A-Za-z0-9])#\s*0*(\d{1,3})(?:v\d+)?(?=[^0-9]|$)",
        clean,
        re.IGNORECASE,
    )
    if hash_episode:
        return EpisodeKey("regular", int(hash_episode.group(1)))

    finale_bracket = re.search(
        r"\[\s*0*(\d{1,3})\s*(?:END|FIN(?:AL)?)\s*\]",
        clean,
        re.IGNORECASE,
    )
    if finale_bracket:
        return EpisodeKey("regular", int(finale_bracket.group(1)))

    # A bare number in the title can be a sequel marker rather than an
    # episode number (``White Album 2 [01]``).  A release's final pure-numeric
    # bracket is the stronger episode token and must win over that title digit.
    bracketed_episodes = re.findall(r"\[\s*0*(\d{1,3})\s*\]", clean)
    if bracketed_episodes:
        return EpisodeKey("regular", int(bracketed_episodes[-1]))
    titled_bracket_episodes = re.findall(
        r"\[\s*0*(\d{1,3})\s*\([^]\r\n]+\)\s*\]",
        clean,
    )
    if titled_bracket_episodes:
        return EpisodeKey("regular", int(titled_bracket_episodes[-1]))
    cut_bracket = re.search(
        r"\[\s*0*(\d{1,3})\s+(?:director(?:'?s)?[ ._-]*)?cut\s*\]",
        clean,
        re.IGNORECASE,
    )
    if cut_bracket:
        return EpisodeKey("regular", int(cut_bracket.group(1)))

    regular_patterns = [
        r"(?:^|[^A-Za-z0-9])S\d{1,2}\s*E\s*0*(\d{1,4})(?:$|[^0-9])",
        SEASON_DASH_EPISODE_RE.pattern,
        r"(?:^|[^A-Za-z0-9])(?:EP?|E)\s*0*(\d{1,4})(?:$|[\s._\-\[\]()])",
        r"第\s*0*(\d{1,4})\s*(?:话|話|集)",
        r"(?:^|[\s_\-.(])0*(\d{1,3})(?:[\s_\-.()]|$)",
    ]
    for pattern in regular_patterns:
        match = re.search(pattern, clean, re.IGNORECASE)
        if match:
            return EpisodeKey("regular", int(match.group(1)))
    return None


def parse_ep_files(
    files: Sequence[Mapping[str, Any]],
    *,
    prefer_simplified: bool = False,
    defer_unnumbered_specials: bool = False,
) -> dict[EpisodeKey, list[dict[str, Any]]]:
    groups: dict[EpisodeKey, list[dict[str, Any]]] = defaultdict(list)
    for raw_item in files:
        item = dict(raw_item)
        if item.get("is_dir"):
            continue
        name = item.get("name")
        full_path = item.get("full_path")
        if not isinstance(name, str) or not isinstance(full_path, str):
            continue
        if cleanup_reason(name) is not None:
            continue
        ext = Path(name).suffix.lower()
        if ext not in MEDIA_EXTS:
            continue
        multi_text = DATE_NOISE_RE.sub(" ", name)
        multi_clean = EPISODE_NOISE_RE.sub(" ", multi_text)
        # Release groups also write a single episode as ``S4 - 01``.  The
        # generic range matcher used to read that as episodes 4 through 1.
        season_dash_episode = SEASON_DASH_EPISODE_RE.search(multi_clean)
        dual_number_episode = (
            SEASON_LOCAL_ABSOLUTE_RE.search(multi_clean)
            or DUAL_BRACKET_EPISODE_RE.search(multi_clean)
        )
        parent_path, _ = split_remote(full_path)
        parent_season = next(
            (
                season_number
                for segment in reversed(parent_path.strip("/").split("/"))
                if (season_number := _season_from_source("/" + segment)) is not None
            ),
            None,
        )
        titled_season_dash_episode = (
            re.search(
                rf"\b0*{parent_season}\s*-\s*0*(\d{{1,3}})(?=[^0-9]|$)",
                multi_clean,
                re.IGNORECASE,
            )
            if parent_season is not None
            else None
        )
        multi_match = None if season_dash_episode or dual_number_episode else (
            None
            if titled_season_dash_episode
            else (
                MULTI_EPISODE_RE.search(multi_clean)
                or MULTI_EPISODE_CONCAT_RE.search(multi_clean)
            )
        )
        override_key = item.get("_episode_key_override")
        override_end = item.get("_episode_end_override")
        override_kind = item.get("_episode_kind_override", "regular")
        key: EpisodeKey | None = (
            EpisodeKey(
                str(override_kind),
                int(override_key),
                int(override_end)
                if isinstance(override_end, int)
                and not isinstance(override_end, bool)
                and override_end >= int(override_key)
                else 0,
            )
            if isinstance(override_key, int)
            and not isinstance(override_key, bool)
            and override_key >= 0
            and override_kind in {"regular", "special"}
            else None
        )
        if key is None and titled_season_dash_episode is not None:
            key = EpisodeKey("regular", int(titled_season_dash_episode.group(1)))
        if key is None and dual_number_episode is not None:
            key = EpisodeKey("regular", int(dual_number_episode.group(1)))
        if key is None and multi_match and multi_match.group(1) != multi_match.group(2):
            start = int(multi_match.group(1))
            end = int(multi_match.group(2))
            if end < start:
                # Release names such as ``Youjitsu 3 - 01`` use the first
                # number as the season and the second as the episode. A true
                # multi-episode range is ascending, so keep the episode here.
                key = EpisodeKey("regular", end)
            else:
                key = EpisodeKey("regular", start, end)
        if key is None:
            key = extract_episode_key(name)
        if key is None:
            parent_name = split_remote(full_path)[0].rsplit("/", 1)[-1]
            parent_clean = EPISODE_NOISE_RE.sub(" ", DATE_NOISE_RE.sub(" ", parent_name))
            parent_is_release_range = bool(
                MULTI_EPISODE_RE.search(parent_clean)
                or MULTI_EPISODE_CONCAT_RE.search(parent_clean)
            )
            if (
                not parent_is_release_range
                and not re.fullmatch(
                    r"(?:season|s)\s*0*\d{1,3}", parent_name, re.IGNORECASE
                )
            ):
                key = extract_episode_key(parent_name)
        if key is not None:
            if key.kind == "special" and key.number == 0:
                if defer_unnumbered_specials:
                    # Smart TV planning gets a chance to match the title
                    # against TMDB; an unmatched subtitle can safely stay put.
                    continue
                raise PlanError(f"发现未编号特别篇，不能安全猜测集号: {full_path}")
            groups[key].append(item)

    if prefer_simplified:
        for key, items in list(groups.items()):
            explicit_simplified = any(
                Path(item["name"]).suffix.lower() in SUBTITLE_EXTS and is_simplified_sub(item["name"])
                for item in items
            )
            if explicit_simplified:
                groups[key] = [
                    item
                    for item in items
                    if not (
                        Path(item["name"]).suffix.lower() in SUBTITLE_EXTS
                        and is_traditional_sub(item["name"])
                    )
                ]
    return dict(groups)


def media_kind(name: str) -> str:
    ext = Path(name).suffix.lower()
    return "video" if ext in VIDEO_EXTS else "subtitle"


def video_resolution_rank(item: Mapping[str, Any]) -> int:
    """Return the best-effort vertical resolution advertised by a video entry."""
    name = unicodedata.normalize("NFKC", str(item.get("name", ""))).lower()
    full_path = unicodedata.normalize(
        "NFKC", str(item.get("full_path", name))
    ).lower()

    def parse(value: str) -> int:
        if re.search(r"(?:^|[^0-9a-z])8k(?:$|[^0-9a-z])|7680\s*[x×]\s*4320", value):
            return 4320
        if re.search(
            r"(?:^|[^0-9a-z])(?:4k|2160[pi])(?:$|[^0-9a-z])|"
            r"3840\s*[x×]\s*2160",
            value,
        ):
            return 2160
        for resolution in (1440, 1080, 720, 576, 480):
            if re.search(
                rf"(?:^|[^0-9]){resolution}[pi]?(?:$|[^0-9])",
                value,
            ):
                return resolution
        return 0

    # A file-level tag is more reliable than a release directory.  When the
    # filename is silent, inspect only the three nearest parent segments. A
    # remote collection root such as ``R 4K ...`` can contain mixed 4K/1080p
    # releases and must not label every descendant as 4K.
    file_rank = parse(name)
    if file_rank:
        return file_rank
    parent_segments = full_path.replace("\\", "/").rsplit("/", 1)[0].split("/")
    for segment in reversed(parent_segments[-3:]):
        if rank := parse(segment):
            return rank
    return 0


def subtitle_presentation_rank(item: Mapping[str, Any]) -> int:
    """Prefer switchable subtitle tracks over permanently burned-in subtitles.

    The ranking is used only when both source paths explicitly advertise their
    subtitle presentation.  An unlabelled release is never deleted based on
    this heuristic.
    """
    full_path = unicodedata.normalize(
        "NFKC", str(item.get("full_path", item.get("name", "")))
    ).casefold()
    # Inspect the nearest labelled directory first.  A collection root may say
    # ``内封+内嵌`` because it contains both releases; that combined parent must
    # not override the concrete child release folder.
    for segment in reversed(full_path.replace("\\", "/").split("/")):
        soft = bool(re.search(r"内封|外挂|软字幕|soft[ ._-]*sub|softsub", segment))
        hard = bool(re.search(r"内嵌|硬字幕|hard[ ._-]*sub|hardsub", segment))
        if soft and not hard:
            return 2
        if hard and not soft:
            return 1
    return 0


def _prefer_highest_resolution_videos(
    items: Sequence[Mapping[str, Any]],
    *,
    prefer_simplified: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Drop lower-resolution duplicates when a higher-resolution copy exists.

    Named editions are compared independently so a director's cut, theatrical
    cut, OVA or other meaningful edition is never discarded merely because its
    resolution is lower.
    """
    kept = [dict(item) for item in items]
    removed: list[dict[str, Any]] = []
    video_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in kept:
        name = str(item.get("name", ""))
        if Path(name).suffix.lower() not in VIDEO_EXTS:
            continue
        edition = entry_edition_tag(item)
        part = item.get("_episode_part_override")
        part_bucket = (
            f"part:{int(part)}"
            if isinstance(part, int) and not isinstance(part, bool) and part > 0
            else ""
        )
        bucket = "|".join(
            value
            for value in (
                _collision_key(edition) if edition else "",
                part_bucket,
            )
            if value
        )
        video_buckets[bucket].append(item)

    removed_paths: set[str] = set()
    removed_video_companions: list[tuple[str, str, str, str]] = []
    for bucket_items in video_buckets.values():
        ranks = {id(item): video_resolution_rank(item) for item in bucket_items}
        best_rank = max(ranks.values(), default=0)
        best_resolution_items = [
            item for item in bucket_items if ranks[id(item)] == best_rank
        ]
        presentation_ranks = {
            id(item): subtitle_presentation_rank(item)
            for item in best_resolution_items
        }
        best_presentation = max(presentation_ranks.values(), default=0)
        presentation_winners = [
            item
            for item in best_resolution_items
            if presentation_ranks[id(item)] == best_presentation
        ]
        language_preferred = False
        if prefer_simplified:
            simplified_winners = [
                item
                for item in presentation_winners
                if is_simplified_sub(
                    str(item.get("full_path", item.get("name", "")))
                )
            ]
            traditional_winners = [
                item
                for item in presentation_winners
                if is_traditional_sub(
                    str(item.get("full_path", item.get("name", "")))
                )
            ]
            # Language is a safe tie-breaker only when both alternatives say
            # what they contain.  An unlabelled or bilingual release is not
            # discarded merely because another path advertises 简中.
            if simplified_winners and traditional_winners:
                presentation_winners = simplified_winners
                language_preferred = True
        # Once content identity, edition and resolution are equal, prefer the
        # most complete release.  A larger positive source size is useful
        # evidence here (higher bitrate and/or additional audio/subtitle
        # tracks); the path is only a deterministic tie-breaker and never by
        # itself justifies deleting an equal-size/unknown-size copy.
        preferred = min(
            presentation_winners,
            key=lambda item: (
                -(_entry_size_value(item) or 0),
                _collision_key(
                    str(item.get("full_path", item.get("name", "")))
                ),
            ),
        )
        preferred_source = normalize_remote_path(str(preferred["full_path"]))
        preferred_size = _entry_size_value(preferred) or 0
        for item in bucket_items:
            cleanup_kind: str | None = None
            if ranks[id(item)] < best_rank and (
                ranks[id(item)] > 0 or best_rank >= 2160
            ):
                cleanup_kind = "lower_resolution"
            elif (
                item is not preferred
                and ranks[id(item)] == best_rank
                and best_presentation > 0
                and presentation_ranks[id(item)] < best_presentation
            ):
                cleanup_kind = "burned_subtitle_duplicate"
            elif (
                item is not preferred
                and language_preferred
                and ranks[id(item)] == best_rank
                and presentation_ranks.get(id(item), 0) == best_presentation
                and is_traditional_sub(
                    str(item.get("full_path", item.get("name", "")))
                )
            ):
                cleanup_kind = "traditional_language_duplicate"
            elif (
                item is not preferred
                and ranks[id(item)] == best_rank
                and presentation_ranks.get(id(item), 0) == best_presentation
                and preferred_size > 0
                and (_entry_size_value(item) or 0) > 0
                and preferred_size > (_entry_size_value(item) or 0)
            ):
                cleanup_kind = "same_resolution_duplicate"
            if cleanup_kind is None:
                continue
            removed_item = dict(item)
            removed_item["_preferred_resolution_source"] = preferred_source
            removed_item["_duplicate_cleanup_kind"] = cleanup_kind
            removed.append(removed_item)
            removed_paths.add(
                _collision_key(str(item.get("full_path", item.get("name", ""))))
            )
            if cleanup_kind == "lower_resolution":
                removed_video_companions.append((
                    normalize_remote_path(str(item["full_path"])),
                    _collision_key(Path(str(item.get("name", ""))).stem),
                    preferred_source,
                    _collision_key(Path(str(preferred.get("name", ""))).stem),
                ))

    # Exact-basename subtitle pairs are release-local companions.  If both the
    # discarded and retained video have such a subtitle, remove only the one
    # proven to belong to the low-resolution release.  Language-suffixed or
    # otherwise distinct subtitles do not share the exact video stem and are
    # deliberately preserved.
    if removed_video_companions:
        subtitles_by_stem: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in kept:
            if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS:
                subtitles_by_stem[
                    _collision_key(Path(str(item.get("name", ""))).stem)
                ].append(item)
        for (
            removed_video,
            removed_stem,
            preferred_video,
            preferred_stem,
        ) in removed_video_companions:
            removed_release_dir, _ = split_remote(removed_video)
            preferred_release_dir, _ = split_remote(preferred_video)
            # A basename such as ``01`` is shared by many release folders.
            # Pair a subtitle only with the concrete release directory that
            # contains its video (or a nested ``字幕备份`` directory).
            # When both videos live in the same directory, release ownership
            # is ambiguous and no subtitle is deleted.
            if _collision_key(removed_release_dir) == _collision_key(
                preferred_release_dir
            ):
                continue
            lower_subtitles = [
                candidate
                for candidate in subtitles_by_stem.get(removed_stem, [])
                if _path_is_within(
                    normalize_remote_path(str(candidate["full_path"])),
                    removed_release_dir,
                )
            ]
            preferred_subtitles = [
                candidate
                for candidate in subtitles_by_stem.get(preferred_stem, [])
                if _path_is_within(
                    normalize_remote_path(str(candidate["full_path"])),
                    preferred_release_dir,
                )
            ]
            for subtitle in lower_subtitles:
                same_format = [
                    candidate
                    for candidate in preferred_subtitles
                    if Path(str(candidate.get("name", ""))).suffix.lower()
                    == Path(str(subtitle.get("name", ""))).suffix.lower()
                ]
                if len(same_format) != 1:
                    continue
                preferred_subtitle = same_format[0]
                if _collision_key(str(preferred_subtitle["full_path"])) == _collision_key(
                    str(subtitle["full_path"])
                ):
                    continue
                removed_item = dict(subtitle)
                removed_item["_preferred_resolution_source"] = normalize_remote_path(
                    str(preferred_subtitle["full_path"])
                )
                removed_item["_duplicate_cleanup_kind"] = "lower_resolution_subtitle"
                removed.append(removed_item)
                removed_paths.add(
                    _collision_key(str(subtitle.get("full_path", subtitle.get("name", ""))))
                )

    if removed_paths:
        kept = [
            item
            for item in kept
            if _collision_key(str(item.get("full_path", item.get("name", ""))))
            not in removed_paths
        ]
    return kept, removed


def _lower_resolution_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号已有更高清晰度版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除较低清晰度重复视频"
    )


def _lower_resolution_movie_cleanup_reason(
    preferred_source_path: str,
    tmdb_id: int,
) -> str:
    return (
        f"同一 TMDB 电影 movie/{tmdb_id} 已有更高清晰度版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除较低清晰度重复视频"
    )


def _burned_subtitle_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号已有同清晰度的内封/软字幕版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除同内容重复视频"
    )


def _same_resolution_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号已有同清晰度但文件更完整的版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除较小的重复视频"
    )


def _traditional_language_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号已有同清晰度同字幕形态的简体中文字幕版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除繁体中文字幕重复视频"
    )


def _lower_resolution_subtitle_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号的更高清晰度版本已有对应字幕 "
        f"{normalize_remote_path(preferred_source_path)}，删除低清发布版附带的重复字幕"
    )


def _dedupe_merged_tv_target_variants(plan: Plan) -> None:
    """Resolve cross-subplan quality duplicates for one confirmed TV target.

    Smart planning can place the same official episode in separate seasonal or
    special subplans before the source labels are fully normalized.  Per-plan
    quality selection cannot see the other copy.  At merge time the exact
    target path is the strongest available identity: it already includes the
    confirmed series, season, episode, multipart range and edition suffix.
    Therefore only exact target collisions with one uniquely highest
    resolution are eligible.  Theme videos, named editions and parts retain
    distinct final names and never enter the same bucket.
    """
    buckets: dict[tuple[str, str], list[PlannedFile]] = defaultdict(list)
    for item in plan.files:
        if item.media_kind == "video":
            buckets[_planned_companion_key(item.target_dir, item.final_name)].append(item)

    removed_ids: set[int] = set()
    cleanup: list[PlannedCleanup] = []
    removed_video_count = 0
    removed_subtitle_count = 0
    preferred_by_key: dict[tuple[str, str], tuple[int, PlannedFile]] = {}
    for companion_key, members in buckets.items():
        if len(members) < 2:
            continue
        ranks = {
            id(item): video_resolution_rank(
                {"name": item.original_name, "full_path": item.source_path}
            )
            for item in members
        }
        best_rank = max(ranks.values(), default=0)
        winners = [item for item in members if ranks[id(item)] == best_rank]
        if best_rank <= 0 or len(winners) != 1:
            continue
        winner = winners[0]
        losers = [
            item
            for item in members
            if ranks[id(item)] < best_rank
            and (ranks[id(item)] > 0 or best_rank >= 2160)
        ]
        # Do not partially rewrite a collision.  Every non-winner must have
        # sufficient resolution evidence before the unique high-quality copy
        # is allowed to win.
        if len(losers) != len(members) - 1:
            continue
        preferred_by_key[companion_key] = (best_rank, winner)
        for item in losers:
            # A lower-quality copy may already have won inside its own
            # subplan and therefore be referenced by an even lower cleanup
            # item.  Once that intermediate winner loses at merge time,
            # redirect the exact generated reason to the surviving winner so
            # execution validation retains an unbroken evidence chain.
            intermediate_reason = _lower_resolution_cleanup_reason(
                item.source_path
            )
            final_reason = _lower_resolution_cleanup_reason(winner.source_path)
            for existing_cleanup in plan.cleanup_files:
                if existing_cleanup.reason == intermediate_reason:
                    existing_cleanup.reason = final_reason
            removed_ids.add(id(item))
            reason = final_reason
            removed_video_count += 1
            cleanup.append(
                PlannedCleanup(
                    source_path=item.source_path,
                    source_dir=item.source_dir,
                    original_name=item.original_name,
                    reason=reason,
                    source_size=item.source_size,
                    source_modified=item.source_modified,
                    source_hash=item.source_hash,
                )
            )

    def subtitle_slot(final_name: str) -> str:
        stem = Path(final_name).stem
        language = re.search(
            r"\.(zh-CN|zh-TW|en|ja)(?:\.\d+)?$", stem, re.IGNORECASE
        )
        if language:
            return _collision_key(language.group(1))
        return "subtitle" if re.search(r"\.subtitle\d*$", stem, re.I) else stem

    subtitle_buckets: dict[tuple[tuple[str, str], str], list[PlannedFile]] = defaultdict(list)
    for item in plan.files:
        if item.media_kind != "subtitle":
            continue
        companion_key = _planned_companion_key(item.target_dir, item.final_name)
        if companion_key in preferred_by_key:
            subtitle_buckets[(companion_key, subtitle_slot(item.final_name))].append(item)
    for (companion_key, _slot), members in subtitle_buckets.items():
        best_video_rank, winner_video = preferred_by_key[companion_key]
        ranks = {
            id(item): video_resolution_rank(
                {"name": item.original_name, "full_path": item.source_path}
            )
            for item in members
        }
        # A lower-release subtitle is deleted only when the retained release
        # supplies the same language/presentation slot.  Otherwise it remains
        # useful and is preserved.
        winners = [item for item in members if ranks[id(item)] == best_video_rank]
        if not winners:
            continue
        preferred_subtitle = min(
            winners, key=lambda item: _collision_key(item.source_path)
        )
        for item in members:
            rank = ranks[id(item)]
            if item is preferred_subtitle or rank >= best_video_rank:
                continue
            if not (rank > 0 or best_video_rank >= 2160):
                continue
            removed_ids.add(id(item))
            removed_subtitle_count += 1
            intermediate_reason = _lower_resolution_subtitle_cleanup_reason(
                item.source_path
            )
            final_reason = _lower_resolution_subtitle_cleanup_reason(
                preferred_subtitle.source_path
            )
            for existing_cleanup in plan.cleanup_files:
                if existing_cleanup.reason == intermediate_reason:
                    existing_cleanup.reason = final_reason
            cleanup.append(
                PlannedCleanup(
                    source_path=item.source_path,
                    source_dir=item.source_dir,
                    original_name=item.original_name,
                    reason=final_reason,
                    source_size=item.source_size,
                    source_modified=item.source_modified,
                    source_hash=item.source_hash,
                )
            )

    if not removed_ids:
        return
    plan.files = [item for item in plan.files if id(item) not in removed_ids]
    plan.cleanup_files = _dedupe_cleanup_files([*plan.cleanup_files, *cleanup])
    if removed_video_count:
        plan.warnings.append(
            "合并子计划后发现同一 TMDB 集号的跨目录清晰度副本；"
            f"已保留最高清晰度并计划清理 {removed_video_count} 个低清视频"
        )
    if removed_subtitle_count:
        plan.warnings.append(
            f"已计划清理 {removed_subtitle_count} 个仅属于低清发布版的重复字幕"
        )


def make_unique_media_names(
    base_name: str,
    files: Sequence[Mapping[str, Any]],
    *,
    preserve_editions: bool = False,
) -> list[str]:
    """为同一集或同一电影的多个文件生成稳定且不会冲突的名称。

    返回值顺序与 ``files`` 输入顺序一致。已符合规范的主视频、v2 视频和字幕
    会优先保留其序号，避免重复运行时交换不同文件的名称。
    """
    output_by_index: dict[int, str] = {}
    used: set[str] = set()
    video_index = 0
    unknown_sub_counts: dict[str, int] = defaultdict(int)
    language_counts: dict[tuple[str, str], int] = defaultdict(int)
    pair_suffixes: dict[str, str] = {}
    named_video_editions = {
        edition
        for item in files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and (edition := entry_edition_tag(item)) is not None
    }

    def canonical_rank(index_and_item: tuple[int, Mapping[str, Any]]) -> tuple[Any, ...]:
        index, item = index_and_item
        original = str(item["name"])
        stem = Path(original).stem
        ext = Path(original).suffix.lower()
        path_key = _collision_key(str(item.get("full_path", original)))
        if ext in VIDEO_EXTS:
            if stem == base_name:
                return (0, 0, path_key, index)
            match = re.fullmatch(re.escape(base_name) + r" - v(\d+)", stem, re.IGNORECASE)
            if match:
                return (0, int(match.group(1)), path_key, index)
            return (0, 10_000, path_key, index)

        # 已规范化字幕按已有语言/序号排序，保证再次运行时稳定。
        if stem.startswith(base_name + "."):
            suffix = stem[len(base_name) + 1 :]
            language_match = re.fullmatch(r"(zh-CN|zh-TW|en|ja)(?:\.(\d+))?", suffix, re.IGNORECASE)
            if language_match:
                language_order = {"zh-cn": 0, "zh-tw": 1, "en": 2, "ja": 3}
                return (
                    1,
                    language_order[language_match.group(1).lower()],
                    int(language_match.group(2) or 1),
                    path_key,
                    index,
                )
            unknown_match = re.fullmatch(r"subtitle(\d+)?", suffix, re.IGNORECASE)
            if unknown_match:
                return (1, 10, int(unknown_match.group(1) or 1), path_key, index)
        return (1, 100, 10_000, path_key, index)

    ranked_files = sorted(enumerate(files), key=canonical_rank)
    for original_index, item in ranked_files:
        original = str(item["name"])
        stem = Path(original).stem.lower()
        ext = Path(original).suffix.lower()
        if ext in VIDEO_EXTS:
            edition = entry_edition_tag(item) if preserve_editions else None
            if edition:
                suffix = f" {{edition-{edition}}}"
            else:
                video_index += 1
                suffix = "" if video_index == 1 else f" - v{video_index}"
            candidate = _compose_filename(base_name, suffix, ext)
        else:
            subtitle_edition = entry_edition_tag(item) if preserve_editions else None
            if (
                subtitle_edition is None
                and preserve_editions
                and len(named_video_editions) == 1
                and re.search(
                    r"(?:^|[\s._\-\[\]()])cut(?:$|[\s._\-\[\]()])",
                    original,
                    re.IGNORECASE,
                )
            ):
                subtitle_edition = next(iter(named_video_editions))
            edition_suffix = (
                f" {{edition-{subtitle_edition}}}" if subtitle_edition else ""
            )
            lang = subtitle_language(original)
            pair_key = re.sub(r"(?:[.\-_\[\]()\s])(?:idx|sub)$", "", stem)
            if pair_key in pair_suffixes:
                suffix = pair_suffixes[pair_key]
            elif lang:
                language_key = (edition_suffix, lang)
                language_counts[language_key] += 1
                count = language_counts[language_key]
                suffix = (
                    f"{edition_suffix}.{lang}"
                    if count == 1
                    else f"{edition_suffix}.{lang}.{count}"
                )
                pair_suffixes[pair_key] = suffix
            else:
                unknown_sub_counts[edition_suffix] += 1
                unknown_sub_index = unknown_sub_counts[edition_suffix]
                suffix = (
                    f"{edition_suffix}.subtitle"
                    if unknown_sub_index == 1
                    else f"{edition_suffix}.subtitle{unknown_sub_index}"
                )
                pair_suffixes[pair_key] = suffix
            candidate = _compose_filename(base_name, suffix, ext)

        serial = 2
        while _collision_key(candidate) in used:
            candidate = _compose_filename(base_name, f"{suffix}.{serial}", ext)
            serial += 1
        used.add(_collision_key(candidate))
        output_by_index[original_index] = candidate

    return [output_by_index[index] for index in range(len(files))]


# ---------------------------------------------------------------------------
# 计划生成
# ---------------------------------------------------------------------------


def _contextual_theme_cleanup_paths(
    files: Iterable[Mapping[str, Any]],
) -> set[str]:
    """Identify OP/ED episode variants only with directory and peer evidence."""
    entries = [dict(item) for item in files]
    primary_episode_numbers: set[int] = set()
    for item in entries:
        name = str(item.get("name") or "")
        if (
            item.get("is_dir")
            or Path(name).suffix.lower() not in VIDEO_EXTS
            or EPISODE_THEME_VARIANT_RE.search(name)
        ):
            continue
        key = extract_episode_key(name)
        if key is not None and key.kind == "regular" and not key.end_number:
            primary_episode_numbers.add(key.number)

    matched: set[str] = set()
    for item in entries:
        name = str(item.get("name") or "")
        full_path = str(item.get("full_path") or "")
        match = EPISODE_THEME_VARIANT_RE.search(name)
        if (
            item.get("is_dir")
            or not match
            or Path(name).suffix.lower() not in VIDEO_EXTS
            or not BONUS_DIRECTORY_RE.search(full_path)
            or int(match.group(1)) not in primary_episode_numbers
        ):
            continue
        matched.add(_collision_key(normalize_remote_path(full_path)))
    return matched


def _filter_media(files: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    entries = [dict(item) for item in files]
    contextual_cleanup = _contextual_theme_cleanup_paths(entries)
    result = []
    for item in entries:
        name = item.get("name")
        if item.get("is_dir") or not isinstance(name, str):
            continue
        if cleanup_reason(name) is not None or _contextual_cleanup_reason(item) is not None:
            continue
        full_path = str(item.get("full_path") or "")
        if full_path and _collision_key(normalize_remote_path(full_path)) in contextual_cleanup:
            continue
        if NON_MEDIA_LIBRARY_CONTEXT_RE.search(full_path):
            continue
        if Path(name).suffix.lower() in MEDIA_EXTS:
            result.append(dict(item))
    return result


def cleanup_reason(name: str) -> str | None:
    if name.startswith("._"):
        return "macOS AppleDouble 隐藏文件"
    suffix = Path(name).suffix.lower()
    if suffix in VIDEO_EXTS and DISPOSABLE_VIDEO_TAG_RE.search(name):
        return "无字幕片头/片尾/光盘菜单视频"
    if suffix in ADVERTISEMENT_IMAGE_EXTS and ADVERTISEMENT_IMAGE_RE.search(name):
        return "发布组广告图片"
    if suffix in FONT_RESOURCE_EXTS and FONT_RESOURCE_RE.search(name):
        return "字体资源包"
    return None


def _contextual_cleanup_reason(item: Mapping[str, Any]) -> str | None:
    """Classify a release extra only when filename and parent context agree."""
    name = unicodedata.normalize("NFKC", str(item.get("name") or ""))
    full_path = normalize_remote_path(str(item.get("full_path") or "/"))
    if (
        Path(name).suffix.lower() in VIDEO_EXTS
        and re.search(
            r"(?:^|/)(?:NCOP(?:\s*[&+／/]\s*(?:NC)?ED)?|NCED|OP\s*[&+／/]\s*ED)(?:/|$)",
            full_path,
            re.I,
        )
        and re.search(
            r"(?:^|[\s._\-\[\]()])(?:NC)?(?:OP|ED)(?:\d+(?:v\d+)?)?"
            r"(?:$|[\s._\-\[\]()])",
            name,
            re.I,
        )
    ):
        return "无字幕片头/片尾/光盘菜单视频"
    if (
        Path(name).suffix.lower() in VIDEO_EXTS
        and BONUS_DIRECTORY_RE.search(full_path)
        and re.search(r"(?:^|[\s._\-\[\]()])MagiRepo(?:$|[\s._\-\[\]()])", name, re.I)
        and extract_episode_key(name) is not None
    ):
        return "特典动画广告/Animated Magia Report Commercial"
    return None


def _planned_cleanup_files(files: Iterable[Mapping[str, Any]]) -> list[PlannedCleanup]:
    entries = [dict(item) for item in files]
    contextual_cleanup = _contextual_theme_cleanup_paths(entries)
    planned: list[PlannedCleanup] = []
    seen: set[str] = set()
    for item in entries:
        name = item.get("name")
        full_path = item.get("full_path")
        if item.get("is_dir") or not isinstance(name, str) or not isinstance(full_path, str):
            continue
        reason = cleanup_reason(name) or _contextual_cleanup_reason(item)
        if (
            reason is None
            and _collision_key(normalize_remote_path(full_path)) in contextual_cleanup
        ):
            reason = "经特典目录与同集正片交叉确认的片头/片尾视频"
        if reason is None:
            decision = classify_residual(full_path)
            reason = recoverable_delete_reason(decision)
        if reason is None:
            continue
        source_path = normalize_remote_path(full_path)
        source_key = _collision_key(source_path)
        if source_key in seen:
            continue
        seen.add(source_key)
        source_dir, original_name = split_remote(source_path)
        planned.append(
            PlannedCleanup(
                source_path=source_path,
                source_dir=source_dir,
                original_name=original_name,
                reason=reason,
                source_size=_entry_size_value(item),
                source_modified=_entry_modified_value(item),
                source_hash=_entry_hash_value(item),
            )
        )
    return sorted(planned, key=lambda item: _collision_key(item.source_path))


def _dedupe_cleanup_files(
    items: Iterable[PlannedCleanup],
) -> list[PlannedCleanup]:
    unique: dict[str, PlannedCleanup] = {}
    for item in items:
        key = _collision_key(item.source_path)
        previous = unique.get(key)
        if previous is not None and previous != item:
            raise PlanError(f"同一清理文件出现不一致计划: {item.source_path}")
        unique[key] = item
    return sorted(unique.values(), key=lambda item: _collision_key(item.source_path))


def _append_cleanup_warning(warnings: list[str], cleanup_files: Sequence[PlannedCleanup]) -> None:
    if not cleanup_files:
        return
    preview = "、".join(item.original_name for item in cleanup_files[:4])
    suffix = f" 等 {len(cleanup_files)} 个文件" if len(cleanup_files) > 4 else ""
    warnings.append(f"确认执行后将删除明确无用的系统隐藏/片头片尾/广告文件：{preview}{suffix}")


def _cleanup_is_generated_housekeeping(item: PlannedCleanup) -> bool:
    """Return true only for operating-system litter, never for media assets."""
    name = item.original_name.casefold()
    return name.startswith("._") or name in {".ds_store", "thumbs.db", "desktop.ini"}


def _unparsed_media_paths(
    files: Sequence[Mapping[str, Any]],
    groups: Mapping[EpisodeKey, Sequence[Mapping[str, Any]]],
) -> list[str]:
    recognized = {
        _collision_key(str(item.get("full_path", "")))
        for items in groups.values()
        for item in items
        if isinstance(item.get("full_path"), str)
    }
    return sorted(
        str(item["full_path"])
        for item in _filter_media(files)
        if isinstance(item.get("full_path"), str)
        and cleanup_reason(str(item.get("name", ""))) is None
        and _collision_key(str(item["full_path"])) not in recognized
    )


def _raise_unparsed_media(paths: Sequence[str], label: str) -> None:
    if not paths:
        return
    preview = ", ".join(paths[:5])
    suffix = f"，另有 {len(paths) - 5} 个" if len(paths) > 5 else ""
    raise PlanError(
        f"发现无法识别{label}编号的媒体文件，拒绝静默遗漏: {preview}{suffix}"
    )


def _add_snapshot_warnings(plan: Plan) -> None:
    weak = []
    for item in plan.files:
        available = sum(
            value is not None
            for value in (item.source_size, item.source_modified, item.source_hash)
        )
        if item.source_hash is None and available < 2:
            weak.append(item.source_path)
    if weak:
        plan.warnings.append(
            f"{len(weak)} 个文件只有单一弱快照字段；同大小或同时间戳替换可能无法识别"
        )


def _resource_gap(
    kind: str,
    label: str,
    reason: str,
    *,
    files: Sequence[str] = (),
) -> dict[str, Any]:
    return {
        "kind": kind,
        "label": label,
        "reason": reason,
        "files": [normalize_remote_path(path) for path in files],
    }


def _planned_tv_episode_numbers(
    plan: Plan,
    season: int,
    *,
    series_dir: str | None = None,
) -> set[int]:
    pattern = re.compile(
        rf"(?:^|[ ._-])S0*{season}E0*(\d{{1,4}})(?:-E0*(\d{{1,4}}))?(?:$|[ ._-])",
        re.I,
    )
    numbers: set[int] = set()
    expected_target = (
        _collision_key(join_remote(series_dir, f"Season {season:02d}"))
        if series_dir is not None
        else None
    )
    for item in plan.files:
        if item.media_kind != "video":
            continue
        if expected_target is not None and _collision_key(item.target_dir) != expected_target:
            continue
        match = pattern.search(Path(item.final_name).stem)
        if match:
            start = int(match.group(1))
            end = int(match.group(2) or start)
            numbers.update(range(min(start, end), max(start, end) + 1))
    return numbers


def _existing_tv_episode_numbers(
    alist: AListClient,
    series_dir: str,
    season: int,
) -> set[int]:
    season_dir = join_remote(series_dir, f"Season {season:02d}")
    pattern = re.compile(
        rf"(?:^|[ ._-])S0*{season}E0*(\d{{1,4}})(?:-E0*(\d{{1,4}}))?(?:$|[ ._-])",
        re.I,
    )
    numbers: set[int] = set()
    for item in alist.try_list(season_dir, refresh=True) or []:
        name = str(item.get("name", ""))
        if item.get("is_dir") or Path(name).suffix.lower() not in VIDEO_EXTS:
            continue
        match = pattern.search(Path(name).stem)
        if match:
            start = int(match.group(1))
            end = int(match.group(2) or start)
            numbers.update(range(min(start, end), max(start, end) + 1))
    return numbers


def _tv_episode_resource_gaps(
    alist: AListClient,
    plan: Plan,
    *,
    series_dir: str,
    season: int,
    official_episodes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    available = _planned_tv_episode_numbers(
        plan, season, series_dir=series_dir
    ) | _existing_tv_episode_numbers(
        alist, series_dir, season
    )
    today = date.today()
    gaps: list[dict[str, Any]] = []
    for episode in official_episodes:
        number = episode.get("episode_number")
        if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
            continue
        air_date = str(episode.get("air_date") or "")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", air_date):
            continue
        if datetime.fromisoformat(air_date).date() > today:
            continue
        if number in available:
            continue
        title = str(episode.get("name") or f"第 {number} 集").strip()
        gaps.append(_resource_gap(
            "missing_episode",
            f"S{season:02d}E{number:02d} {title}",
            "TMDB 已发布该集，但源目录和现有目标库均没有对应视频",
        ))
    return gaps


def _planned_member_movie_special_coverage(
    plan: Plan,
    official_episodes: Sequence[Mapping[str, Any]],
) -> dict[int, dict[str, Any]]:
    """Return TV specials already represented by a planned independent movie.

    TMDB occasionally exposes the same feature both as a movie and as a TV
    Season 00 row.  ScrapeFlow deliberately keeps the independent movie
    identity for Infuse, so the TV gap audit must not request a second copy of
    that exact content.  Exact normalized title plus release year is required;
    a merely similar title is never suppressed.
    """
    raw_member_movies = plan.metadata.get("member_movies")
    if not isinstance(raw_member_movies, Mapping):
        return {}
    planned_video_paths = {
        normalize_remote_path(join_remote(item.target_dir, item.final_name))
        for item in plan.files
        if item.media_kind == "video"
    }
    planned_video_dirs = {
        normalize_remote_path(item.target_dir)
        for item in plan.files
        if item.media_kind == "video"
    }
    represented: dict[tuple[str, str], dict[str, Any]] = {}
    for raw_target, identity in raw_member_movies.items():
        if not isinstance(raw_target, str) or not isinstance(identity, Mapping):
            continue
        target = normalize_remote_path(raw_target)
        if target not in planned_video_paths and target not in planned_video_dirs:
            continue
        title = str(identity.get("title") or "").strip()
        year = str(identity.get("year") or "").strip()
        title_key = _normalize_match_title(title)
        if not title_key or not re.fullmatch(r"(?:19|20)\d{2}", year):
            continue
        represented[(title_key, year)] = {
            "tmdb_id": identity.get("tmdb_id"),
            "title": title,
            "year": year,
            "target": target,
        }
    covered: dict[int, dict[str, Any]] = {}
    for episode in official_episodes:
        number = episode.get("episode_number")
        air_date = str(episode.get("air_date") or "")
        title = str(episode.get("name") or "").strip()
        if (
            not isinstance(number, int)
            or isinstance(number, bool)
            or number <= 0
            or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", air_date)
        ):
            continue
        match = represented.get((_normalize_match_title(title), air_date[:4]))
        if match is not None:
            covered[number] = match
    return covered


def _append_complete_tv_resource_gaps(
    alist: AListClient,
    tmdb_client: TMDBClient,
    plan: Plan,
) -> None:
    """Check every published TMDB episode, including Season 00.

    Per-season builders used to inspect only the season currently being
    planned.  A complete franchise plan could therefore finish while a
    published special was absent.  Run this once on the final combined plan so
    planned files and existing target files are considered together.
    """
    identities: list[tuple[str, int, Mapping[str, Any]]] = []
    raw_member_tv = plan.metadata.get("member_tv")
    if isinstance(raw_member_tv, Mapping):
        for series_root, identity in raw_member_tv.items():
            if not isinstance(series_root, str) or not isinstance(identity, Mapping):
                continue
            tmdb_id = identity.get("tmdb_id")
            if isinstance(tmdb_id, int) and not isinstance(tmdb_id, bool) and tmdb_id > 0:
                identities.append((normalize_remote_path(series_root), tmdb_id, identity))
    raw_tmdb_id = plan.metadata.get("tmdb_id")
    if (
        isinstance(raw_tmdb_id, int)
        and not isinstance(raw_tmdb_id, bool)
        and raw_tmdb_id > 0
        and plan.mode in {"tv", "mixed"}
    ):
        identities.append((
            normalize_remote_path(str(plan.metadata.get("series_root") or plan.target_root)),
            raw_tmdb_id,
            plan.metadata,
        ))

    # Subplans may have recorded a gap before all sibling seasons were merged
    # into the final plan.  Recompute episode/season absence from the complete
    # file set instead of retaining those stale intermediate observations.
    existing_gaps = [
        dict(gap)
        for gap in (plan.scan_report.get("resource_gaps") or [])
        if isinstance(gap, Mapping)
        and str(gap.get("kind") or "") not in {"missing_episode", "missing_season"}
    ]
    seen = {
        (
            str(gap.get("kind") or ""),
            str(gap.get("label") or ""),
        )
        for gap in existing_gaps
    }
    deduplicated_identities: dict[tuple[str, int], Mapping[str, Any]] = {}
    for series_root, tmdb_id, identity in identities:
        deduplicated_identities[(series_root, tmdb_id)] = identity
    for (series_root, tmdb_id), identity in deduplicated_identities.items():
        try:
            show = tmdb_client.get(f"/tv/{tmdb_id}")
        except ApiError:
            continue
        season_numbers = sorted({
            int(item["season_number"])
            for item in (show.get("seasons") or [])
            if isinstance(item, Mapping)
            and isinstance(item.get("season_number"), int)
            and not isinstance(item.get("season_number"), bool)
            and int(item["season_number"]) >= 0
            and isinstance(item.get("episode_count"), int)
            and not isinstance(item.get("episode_count"), bool)
            and int(item["episode_count"]) > 0
        })
        for season_number in season_numbers:
            try:
                season_payload = tmdb_client.get(
                    f"/tv/{tmdb_id}/season/{season_number}"
                )
            except ApiError:
                continue
            official_episode_rows = [
                item
                for item in (season_payload.get("episodes") or [])
                if isinstance(item, Mapping)
            ]
            covered_by_movie = (
                _planned_member_movie_special_coverage(plan, official_episode_rows)
                if season_number == 0 else {}
            )
            if covered_by_movie:
                audit_rows = plan.scan_report.setdefault(
                    "tv_specials_covered_by_member_movies", [],
                )
                if isinstance(audit_rows, list):
                    for episode_number, movie in sorted(covered_by_movie.items()):
                        row = {
                            "tv_tmdb_id": tmdb_id,
                            "episode": episode_number,
                            "movie_tmdb_id": movie.get("tmdb_id"),
                            "title": movie.get("title"),
                            "year": movie.get("year"),
                            "target": movie.get("target"),
                        }
                        if row not in audit_rows:
                            audit_rows.append(row)
            gaps = _tv_episode_resource_gaps(
                alist,
                plan,
                series_dir=series_root,
                season=season_number,
                official_episodes=[
                    item for item in official_episode_rows
                    if item.get("episode_number") not in covered_by_movie
                ],
            )
            season_name = str(season_payload.get("name") or "").strip()
            if season_name:
                for gap in gaps:
                    gap["season_name"] = season_name
            for gap in gaps:
                gap["media"] = {
                    "tmdb_id": tmdb_id,
                    "title": str(identity.get("title") or show.get("name") or "").strip(),
                    "original_title": str(
                        identity.get("original_title") or show.get("original_name") or ""
                    ).strip(),
                    "year": str(identity.get("year") or "").strip(),
                    "target_root": series_root,
                }
                key = (str(gap.get("kind") or ""), str(gap.get("label") or ""))
                if key in seen:
                    continue
                seen.add(key)
                existing_gaps.append(gap)
    if existing_gaps:
        plan.scan_report["resource_gaps"] = existing_gaps


def _tv_season_resource_gaps(
    alist: AListClient,
    plan: Plan,
    *,
    series_dir: str,
    official_seasons: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    today = date.today()
    gaps: list[dict[str, Any]] = []
    for season_meta in official_seasons:
        number = season_meta.get("season_number")
        if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
            continue
        air_date = str(season_meta.get("air_date") or "")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", air_date):
            continue
        if datetime.fromisoformat(air_date).date() > today:
            continue
        available = _planned_tv_episode_numbers(
            plan, number, series_dir=series_dir
        ) | _existing_tv_episode_numbers(alist, series_dir, number)
        if available:
            continue
        expected = season_meta.get("episode_count")
        expected_text = (
            f"，TMDB 记录 {expected} 集"
            if isinstance(expected, int) and not isinstance(expected, bool) and expected > 0
            else ""
        )
        title = str(season_meta.get("name") or f"Season {number}").strip()
        gap = _resource_gap(
            "missing_season",
            f"Season {number:02d} {title}",
            f"TMDB 已发布该季{expected_text}，但源目录和现有目标库均没有任何该季视频",
        )
        if title:
            gap["season_name"] = title
        if isinstance(expected, int) and not isinstance(expected, bool) and expected > 0:
            gap["expected_episode_count"] = expected
        gaps.append(gap)
    return gaps


def _build_tv_episode_map(
    tmdb_client: TMDBClient,
    show: Mapping[str, Any],
    tmdb_id: int,
    season: int,
    absolute: bool,
    episode_group_id: str | None = None,
    special_titles: dict[EpisodeKey, str] | None = None,
    special_season_candidates: dict[int, list[EpisodeKey]] | None = None,
) -> dict[EpisodeKey, tuple[int, int, str]]:
    mapping: dict[EpisodeKey, tuple[int, int, str]] = {}
    season_data: Mapping[str, Any] | None = None
    if absolute and episode_group_id:
        group_data = tmdb_client.get(f"/tv/episode_group/{episode_group_id}")
        absolute_number = 1
        groups = sorted(
            (item for item in (group_data.get("groups") or []) if isinstance(item, Mapping)),
            key=lambda item: int(item.get("order", 0)),
        )
        for group in groups:
            episodes = sorted(
                (item for item in (group.get("episodes") or []) if isinstance(item, Mapping)),
                key=lambda item: int(item.get("order", item.get("episode_number", 0))),
            )
            for episode in episodes:
                target_season = int(episode["season_number"])
                target_episode = int(episode["episode_number"])
                mapping[EpisodeKey("regular", absolute_number)] = (
                    target_season,
                    target_episode,
                    str(episode.get("name") or f"第{absolute_number}集"),
                )
                absolute_number += 1
    elif absolute:
        seasons = sorted(
            (
                int(item["season_number"])
                for item in (show.get("seasons") or [])
                if isinstance(item, dict) and int(item.get("season_number", 0)) > 0
            )
        )
        absolute_number = 1
        for season_number in seasons:
            season_data = tmdb_client.get(f"/tv/{tmdb_id}/season/{season_number}")
            for episode in (season_data.get("episodes") or []):
                mapping[EpisodeKey("regular", absolute_number)] = (
                    season_number,
                    int(episode["episode_number"]),
                    str(episode.get("name") or f"第{absolute_number}集"),
                )
                absolute_number += 1
    else:
        season_data = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
        for episode in (season_data.get("episodes") or []):
            number = int(episode["episode_number"])
            mapping[EpisodeKey("regular", number)] = (
                season,
                number,
                str(episode.get("name") or f"第{number}集"),
            )

    # 特别篇统一映射到 Season 00。即使 TMDB 没有条目，也只在号码明确时回退名称。
    try:
        specials_data = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
    except ApiError as exc:
        if exc.status_code == 404:
            specials_data = {"episodes": []}
        else:
            raise
    for episode in (specials_data.get("episodes") or []):
        number = int(episode["episode_number"])
        key = EpisodeKey("special", number)
        title = str(episode.get("name") or f"特别篇 {number}")
        mapping[key] = (
            0,
            number,
            title,
        )
        if special_titles is not None:
            special_titles[key] = title
    if special_season_candidates is not None and not absolute and season_data is not None:
        regular_dates = sorted(
            str(item.get("air_date"))
            for item in (season_data.get("episodes") or [])
            if isinstance(item, Mapping)
            and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
        )
        if regular_dates:
            season_start = datetime.fromisoformat(regular_dates[0]).date()
            season_end = datetime.fromisoformat(regular_dates[-1]).date()
            later_season_starts = sorted(
                datetime.fromisoformat(str(item.get("air_date"))).date()
                for item in (show.get("seasons") or [])
                if isinstance(item, Mapping)
                and isinstance(item.get("season_number"), int)
                and int(item["season_number"]) > season
                and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
            )
            window_end = (
                later_season_starts[0]
                if later_season_starts
                # The final season can receive an OVA years later through a
                # game or anniversary release. With no later season boundary,
                # keep the official post-season timeline open instead of
                # dropping a verified TMDB special after an arbitrary cutoff.
                else datetime.max.date()
            )
            candidates = sorted(
                (
                    datetime.fromisoformat(str(item.get("air_date"))).date(),
                    EpisodeKey("special", int(item["episode_number"])),
                )
                for item in (specials_data.get("episodes") or [])
                if isinstance(item, Mapping)
                and isinstance(item.get("episode_number"), int)
                and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
                and season_start
                <= datetime.fromisoformat(str(item.get("air_date"))).date()
                < window_end
            )
            if candidates:
                special_season_candidates[season] = [key for _, key in candidates]
    return mapping


def _special_title_key(value: str, series_title: str) -> str:
    value = re.sub(r"\{(?:tmdb|imdb)-[^{}]+\}", " ", value, flags=re.IGNORECASE)
    value = re.sub(
        r"(?:\.(?:mkv|mp4|m4v|avi|mov|wmv|flv|ts|m2ts|ass|ssa|srt|vtt|sub))+$",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    key = _normalize_match_title(value)
    series_key = _normalize_match_title(series_title)
    if series_key:
        key = key.replace(series_key, "")
    for noise in ("特别篇", "特典", "续章", "specials", "special", "movie", "ova", "oav", "oad"):
        key = key.replace(_normalize_match_title(noise), "")
    return key


def _special_label_tokens(value: str) -> set[str]:
    """Return release labels such as MMR03/OVA2 that also occur in TMDB titles."""
    normalized = unicodedata.normalize("NFKC", value).upper()
    tokens = {
        f"{label}{int(number)}"
        for label, number in re.findall(r"(?<![A-Z])([A-Z]{2,12})[\s._#-]*0*(\d{1,3})(?!\d)", normalized)
        if label not in {"EP", "HEVC", "AVC", "AAC", "FLAC", "WEB", "BD", "MA"}
    }
    tokens.update(
        f"{label}{int(number)}"
        for label, number in re.findall(
            r"(?<![A-Z])(OVA|OAV|OAD)[\s._-]*(?:SERIES|系列)[\s._-]*\[?\s*0*(\d{1,3})",
            normalized,
        )
    )
    roman_values = {"I": 1, "V": 5, "X": 10}
    for label, roman in re.findall(
        r"(?<![A-Z])([A-Z]{2,12})[\s._-]+([IVX]{1,5})(?![A-Z])",
        normalized,
    ):
        if label in {"EP", "HEVC", "AVC", "AAC", "FLAC", "WEB", "BD", "MA"}:
            continue
        total = 0
        previous = 0
        for char in reversed(roman):
            value = roman_values[char]
            total += -value if value < previous else value
            previous = max(previous, value)
        if 0 < total <= 100:
            tokens.add(f"{label}{total}")
    return tokens


def _special_context_tokens(value: str) -> set[str]:
    """Return title words that can distinguish two equal OVA/OAD labels.

    TMDB can contain more than one physical release called ``OVA #3`` inside
    one Season 00.  Release names usually retain the sub-series label (for
    example ``Darkness``), while the original OVA does not.  Only meaningful
    Latin title words participate here; codec, container and release-group
    noise must never become identity evidence.
    """
    ignored = {
        "subtitle", "group", "season", "series", "special", "episode",
        "ova", "oav", "oad", "bdrip", "bluray", "webrip", "webdl",
        "hevc", "x264", "x265", "flac", "truehd", "aac", "mkv", "mp4",
        "chs", "cht", "jpn", "eng", "bit", "backup",
    }
    return {
        token
        for token in re.findall(
            r"[a-z]{3,}", unicodedata.normalize("NFKC", value).casefold()
        )
        if token not in ignored
    }


def _fractional_signatures(value: str) -> set[tuple[int, str]]:
    """Extract normalized decimal episode labels from a metadata title."""
    normalized = unicodedata.normalize("NFKC", value)
    return {
        (int(whole), fractional.rstrip("0") or "0")
        for whole, fractional in re.findall(
            r"(?<!\d)0*(\d{1,3})\.(\d{1,3})(?!\d)",
            normalized,
        )
    }


def _multilingual_episode_titles(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    episode_map: Mapping[EpisodeKey, tuple[int, int, str]],
) -> dict[tuple[int, int], list[str]]:
    """Load online TMDB regular and special titles in useful languages.

    This is deliberately evidence gathering, not a fallback guess. A caller
    may auto-map only when the same explicit decimal label identifies one
    TMDB episode across the collected titles.
    """
    titles: dict[tuple[int, int], list[str]] = {}
    for target_season, target_episode, title in episode_map.values():
        target = (target_season, target_episode)
        if title and title not in titles.setdefault(target, []):
            titles[target].append(title)
    target_seasons = sorted({season for season, _ in titles})
    primary_language = str(getattr(tmdb_client, "language", "") or "")
    for language in ("zh-CN", "zh-TW", "ja-JP", "en-US"):
        if language == primary_language:
            continue
        for target_season in target_seasons:
            try:
                payload = tmdb_client.get(
                    f"/tv/{tmdb_id}/season/{target_season}",
                    language=language,
                )
            except ApiError:
                # The main metadata request already succeeded. An unavailable
                # translation is optional evidence and must not break planning.
                continue
            for episode in payload.get("episodes") or []:
                if (
                    not isinstance(episode, Mapping)
                    or isinstance(episode.get("episode_number"), bool)
                    or not isinstance(episode.get("episode_number"), int)
                ):
                    continue
                target = (target_season, int(episode["episode_number"]))
                title = str(episode.get("name") or "").strip()
                if title and title not in titles.setdefault(target, []):
                    titles[target].append(title)
    return titles


def _fractional_recap_evidence_candidates(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    season: int,
    source_key: EpisodeKey,
    titles: Mapping[tuple[int, int], Sequence[str]],
    source_items: Sequence[Mapping[str, Any]],
) -> tuple[
    list[tuple[int, int, str, tuple[str, ...], tuple[str, ...]]],
    str,
]:
    """Resolve N.5 using scored independent evidence and conflict vetoes."""
    if source_key.fractional_digits != "5":
        return [], "没有找到可用于非 N.5 小数集的高置信半集识别证据"

    recap_re = re.compile(
        r"(?:总集篇|總集篇|総集編|総集篇|recap|digest|compilation|summary|回顾|回顧)",
        re.IGNORECASE,
    )
    interlude_re = re.compile(
        r"(?:半集|半话|半話|闲话|閑話|幕间|幕間|interlude|extra|"
        r"side[ ._-]*story|番外|特别篇|special)",
        re.IGNORECASE,
    )
    ova_re = re.compile(
        r"(?:^|[^a-z0-9])(?:ova|oav|oad)(?:$|[^a-z0-9])",
        re.IGNORECASE,
    )
    promo_re = re.compile(
        r"(?:^|[^a-z0-9])(?:pv|cm|trailer|preview|menu)(?:$|[^a-z0-9])|"
        r"预告|預告|特报|特報|メニュー",
        re.IGNORECASE,
    )

    def parsed_date(value: Any) -> date | None:
        raw = str(value or "")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            return None
        return datetime.fromisoformat(raw).date()

    def referenced_seasons(values: Sequence[str]) -> set[int]:
        references: set[int] = set()
        for value in values:
            for match in re.finditer(
                r"(?:第\s*0*(\d{1,2})\s*季|\bseason\s*0*(\d{1,2})\b|"
                r"\b0*(\d{1,2})(?:st|nd|rd|th)\s+season\b)",
                unicodedata.normalize("NFKC", value),
                re.IGNORECASE,
            ):
                references.add(
                    int(next(group for group in match.groups() if group is not None))
                )
        return references

    source_values = tuple(dict.fromkeys(
        str(item.get(field) or "")
        for item in source_items
        for field in ("name", "full_path")
        if str(item.get(field) or "")
    ))
    source_text = " ".join(source_values)
    source_recap = bool(recap_re.search(source_text))
    source_interlude = bool(interlude_re.search(source_text))
    source_ova = bool(ova_re.search(source_text))
    source_promo = bool(promo_re.search(source_text))
    source_tokens = _special_context_tokens(source_text)
    source_durations: list[float] = []
    for item in source_items:
        for field, divisor in (
            ("duration_ms", 60_000.0),
            ("duration_seconds", 60.0),
            ("runtime_minutes", 1.0),
        ):
            value = item.get(field)
            if (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and float(value) > 0
            ):
                source_durations.append(float(value) / divisor)
                break
    source_duration = (
        sorted(source_durations)[len(source_durations) // 2]
        if source_durations
        else None
    )

    try:
        season_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
    except ApiError:
        season_payload = {}
    try:
        specials_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
    except ApiError:
        specials_payload = {}
    regular_episodes = [
        item
        for item in season_payload.get("episodes") or []
        if isinstance(item, Mapping)
        and isinstance(item.get("episode_number"), int)
        and not isinstance(item.get("episode_number"), bool)
    ]
    regular_by_number = {
        int(item["episode_number"]): item for item in regular_episodes
    }
    before = regular_by_number.get(source_key.number)
    after = regular_by_number.get(source_key.number + 1)
    before_date = parsed_date(before.get("air_date")) if before else None
    after_date = parsed_date(after.get("air_date")) if after else None
    regular_dates = sorted(
        item_date
        for item in regular_episodes
        if (item_date := parsed_date(item.get("air_date"))) is not None
    )
    regular_runtimes = sorted(
        int(item["runtime"])
        for item in regular_episodes
        if isinstance(item.get("runtime"), int)
        and not isinstance(item.get("runtime"), bool)
        and int(item["runtime"]) > 0
    )
    median_runtime = (
        regular_runtimes[len(regular_runtimes) // 2]
        if regular_runtimes
        else None
    )
    special_metadata: dict[tuple[int, int], Mapping[str, Any]] = {}
    for item in specials_payload.get("episodes") or []:
        if (
            isinstance(item, Mapping)
            and isinstance(item.get("episode_number"), int)
            and not isinstance(item.get("episode_number"), bool)
        ):
            special_metadata[(0, int(item["episode_number"]))] = item

    signature = (source_key.number, source_key.fractional_digits)
    candidate_targets = {
        target
        for target, localized_titles in titles.items()
        if target[0] == 0
        or any(signature in _fractional_signatures(title) for title in localized_titles)
    }
    candidate_targets.update(special_metadata)
    scored: list[dict[str, Any]] = []
    for target_season, target_episode in sorted(candidate_targets):
        metadata = special_metadata.get((target_season, target_episode), {})
        aliases = tuple(dict.fromkeys((
            *titles.get((target_season, target_episode), ()),
            str(metadata.get("name") or ""),
        )))
        aliases = tuple(alias for alias in aliases if alias)
        if not aliases:
            continue
        signatures: set[tuple[int, str]] = set()
        for alias in aliases:
            signatures.update(_fractional_signatures(alias))
        explicit_fractional = signature in signatures
        if target_season != 0 and not explicit_fractional:
            continue

        score = 1.0
        evidence = ["源文件明确使用 N.5/半集编号"]
        conflicts: list[str] = []
        if explicit_fractional:
            score += 6.0
            evidence.append("官方多语言标题含相同小数集号")
        elif signatures:
            labels = ", ".join(
                f"{whole}.{digits}" for whole, digits in sorted(signatures)
            )
            conflicts.append(f"官方标题明写其他小数集号 {labels}")

        official_text = " ".join(aliases)
        official_recap = bool(recap_re.search(official_text))
        official_interlude = bool(interlude_re.search(official_text))
        official_ova = bool(ova_re.search(official_text))
        official_promo = bool(promo_re.search(official_text))
        if official_recap:
            score += 2.0
            evidence.append("官方 Season 00 标题/别名明确为总集篇或回顾")
        if official_interlude:
            score += 1.5
            evidence.append("官方标题/别名含半集、幕间或番外语义")
        if source_recap and official_recap:
            score += 2.0
            evidence.append("源标题与官方总集篇语义一致")
        if source_interlude and official_interlude:
            score += 1.0
            evidence.append("源标题与官方半集/幕间语义一致")
        if source_recap and official_ova and not official_recap:
            conflicts.append("源标题是总集篇，官方标题却是 OVA/OAD")
        if source_ova and official_recap and not source_recap:
            conflicts.append("源标题是 OVA/OAD，官方标题却是总集篇")
        if source_promo or official_promo:
            conflicts.append("源或官方标题表明这是预告/PV/菜单而非半集正片")

        season_refs = referenced_seasons(aliases)
        if season_refs and season not in season_refs:
            conflicts.append(
                "官方标题归属于其他季度 "
                + "/".join(str(value) for value in sorted(season_refs))
            )
        elif season in season_refs:
            score += 1.5
            evidence.append(f"官方标题明确归属第 {season} 季")

        official_tokens = _special_context_tokens(official_text)
        common_tokens = {
            token for token in source_tokens & official_tokens if len(token) >= 4
        }
        normalized_source = _normalize_match_title(source_text)
        exact_title_overlap = any(
            len(normalized_alias := _normalize_match_title(alias)) >= 6
            and normalized_alias in normalized_source
            for alias in aliases
        )
        if exact_title_overlap:
            score += 2.0
            evidence.append("源标题含官方特别篇标题/别名")
        elif len(common_tokens) >= 2 or any(
            len(token) >= 7 for token in common_tokens
        ):
            score += 1.0
            evidence.append("源标题与官方别名有特异词重合")

        candidate_date = parsed_date(metadata.get("air_date"))
        exact_interval = bool(
            candidate_date
            and before_date
            and after_date
            and before_date < candidate_date < after_date
        )
        if exact_interval:
            score += 3.0
            evidence.append("官方播出日位于 N 与 N+1 之间")
        elif candidate_date and before_date and not after_date:
            if 0 < (candidate_date - before_date).days <= 35:
                score += 1.5
                evidence.append("缺少 N+1 日期时，播出日紧随 N 之后")
        elif candidate_date and after_date and not before_date:
            if 0 < (after_date - candidate_date).days <= 35:
                score += 1.5
                evidence.append("缺少 N 日期时，播出日紧邻 N+1 之前")
        if candidate_date and regular_dates:
            if regular_dates[0] <= candidate_date <= regular_dates[-1]:
                score += 1.0
                evidence.append(f"播出日落在第 {season} 季官方时间线内")
            elif min(
                abs((candidate_date - regular_dates[0]).days),
                abs((candidate_date - regular_dates[-1]).days),
            ) > 120:
                conflicts.append(f"播出日与第 {season} 季时间线相距超过 120 天")

        runtime_value = metadata.get("runtime")
        runtime = (
            int(runtime_value)
            if isinstance(runtime_value, int)
            and not isinstance(runtime_value, bool)
            and runtime_value > 0
            else None
        )
        if runtime is not None:
            if runtime < 8 and not explicit_fractional:
                conflicts.append(f"官方运行时长仅 {runtime} 分钟，更像短预告/特典")
            elif median_runtime and 0.65 * median_runtime <= runtime <= 2.25 * median_runtime:
                score += 1.0
                evidence.append("官方运行时长与本季正片/长篇范围一致")
            elif runtime >= 18:
                score += 0.5
                evidence.append("官方运行时长达到完整节目级别")
            if runtime >= 40:
                score += 0.5
                evidence.append("官方运行时长表明这是长篇特别篇")
            if source_duration is not None:
                if abs(source_duration - runtime) <= max(3.0, runtime * 0.2):
                    score += 1.5
                    evidence.append("源视频与官方运行时长相符")
                elif abs(source_duration - runtime) > max(6.0, runtime * 0.35):
                    conflicts.append(
                        f"源视频约 {source_duration:.1f} 分钟，与官方 {runtime} 分钟明显冲突"
                    )

        movie_release_inside = False
        if target_season == 0 and runtime is not None and runtime >= 40:
            release_dates: set[date] = set()
            for alias in aliases:
                query_key = _normalize_match_title(alias)
                if len(query_key) < 4:
                    continue
                try:
                    movie_results = tmdb_client.get(
                        "/search/movie", query=alias
                    ).get("results") or []
                except ApiError:
                    continue
                for movie in movie_results[:5]:
                    if not isinstance(movie, Mapping):
                        continue
                    result_keys = {
                        _normalize_match_title(str(movie.get(field) or ""))
                        for field in ("title", "original_title")
                    }
                    if not any(
                        result
                        and (query_key in result or result in query_key)
                        for result in result_keys
                    ):
                        continue
                    release_date = parsed_date(movie.get("release_date"))
                    if release_date is not None:
                        release_dates.add(release_date)
            if len(release_dates) == 1 and before_date and after_date:
                release_date = next(iter(release_dates))
                if before_date < release_date < after_date:
                    movie_release_inside = True
                    score += 1.0
                    evidence.append("同名官方电影发行日也落在 N/N+1 区间")
                elif exact_interval:
                    conflicts.append("同名官方电影发行日否定 TMDB TV 特别篇日期")

        identity_evidence = bool(
            explicit_fractional
            or official_recap
            or (official_interlude and (source_interlude or source_recap))
            or (
                source_recap
                and (exact_title_overlap or bool(common_tokens))
                and candidate_date
            )
            or (
                runtime is not None
                and runtime >= 40
                and exact_interval
                and (movie_release_inside or exact_title_overlap)
            )
        )
        scored.append({
            "target": (target_season, target_episode),
            "title": aliases[0],
            "aliases": aliases,
            "score": score,
            "evidence": evidence,
            "conflicts": conflicts,
            "identity": identity_evidence,
        })

    eligible = [
        item
        for item in scored
        if item["identity"] and not item["conflicts"] and item["score"] >= 6.0
    ]
    eligible.sort(key=lambda item: (-item["score"], item["target"]))
    if len(eligible) == 1:
        eligible[0]["score"] += 1.0
        eligible[0]["evidence"].append("高分候选唯一")
    if eligible and eligible[0]["score"] >= 7.0:
        top_score = float(eligible[0]["score"])
        contenders = [
            item for item in eligible if top_score - float(item["score"]) < 2.0
        ]
        if len(contenders) == 1:
            winner = contenders[0]
            if len(eligible) > 1:
                winner["evidence"].append("与次名得分差满足自动映射安全间隔")
            target_season, target_episode = winner["target"]
            return [(
                target_season,
                target_episode,
                winner["title"],
                winner["aliases"],
                tuple(winner["evidence"]),
            )], ""
        return [
            (
                item["target"][0],
                item["target"][1],
                item["title"],
                item["aliases"],
                tuple(item["evidence"]),
            )
            for item in contenders
        ], "多个候选的多证据得分差小于自动映射安全间隔"

    if scored:
        strongest = max(scored, key=lambda item: float(item["score"]))
        target_season, target_episode = strongest["target"]
        target_label = f"S{target_season:02d}E{target_episode:02d}"
        if strongest["conflicts"]:
            return [], (
                f"{target_label} 存在排他冲突："
                + "；".join(str(value) for value in strongest["conflicts"])
            )
        return [], (
            f"{target_label} 的标题、时间线、季度、语义、运行时长与唯一性"
            "证据未达到高置信门槛"
        )
    return [], "TMDB 官方 Season 00/季度中没有可参与半集多证据评分的候选"


def _e00_special_candidates(
    items: Sequence[Mapping[str, Any]],
    titles: Mapping[tuple[int, int], Sequence[str]],
    series_title: str,
) -> list[EpisodeKey]:
    """Resolve E00 only from a unique official title match.

    ``E00`` describes the source numbering, not a TMDB Season 00 index.  It
    therefore must not silently become S00E01.  Search the multilingual TMDB
    titles and accept only one candidate whose explicit prologue/episode-zero
    wording is also present in the release filename or path.
    """
    source_labels: list[str] = []
    for item in items:
        label = f"{item.get('name', '')} {item.get('full_path', '')}"
        label = re.sub(
            r"(?<![A-Za-z0-9])(?:S\d{1,3}\s*)?E\s*0+(?!\d)|"
            r"(?<![A-Za-z0-9])0{1,3}(?!\d)",
            " ",
            unicodedata.normalize("NFKC", label),
            flags=re.I,
        )
        key = _special_title_key(label, series_title)
        if key:
            source_labels.append(key)
    candidates: list[EpisodeKey] = []
    explicit_zero_title = re.compile(
        r"(?:prologue|episode\s*0|第\s*0\s*[话話集]|序章|プロローグ|前日[譚谭])",
        re.I,
    )
    for (target_season, target_episode), localized_titles in titles.items():
        if target_season != 0:
            continue
        matching_title_keys = [
            _special_title_key(title, series_title)
            for title in localized_titles
            if explicit_zero_title.search(unicodedata.normalize("NFKC", title))
        ]
        matching_title_keys = [key for key in matching_title_keys if len(key) >= 3]
        if not matching_title_keys:
            continue
        if any(
            title_key in source_key or source_key in title_key
            for source_key in source_labels
            for title_key in matching_title_keys
        ):
            candidates.append(EpisodeKey("special", target_episode))
    return sorted(set(candidates))


def _unique_official_episode_zero_candidate(
    titles: Mapping[tuple[int, int], Sequence[str]],
) -> list[EpisodeKey]:
    """Return one Season 00 entry explicitly titled as episode zero.

    This is stronger evidence than merely having one unused special.  It is
    used for bare release names such as ``Show [00].mkv`` only when TMDB's
    multilingual official titles themselves uniquely say Episode 0/第 0 话.
    Generic specials, prologues without a zero label, and ambiguous duplicate
    zero titles remain unresolved.
    """
    explicit_zero_title = re.compile(
        r"(?:episode\s*[:#-]?\s*0(?!\d)|第\s*0\s*[话話集](?!\d)|"
        r"第\s*0\s*话|第零[话話集])",
        re.I,
    )
    candidates = {
        EpisodeKey("special", target_episode)
        for (target_season, target_episode), localized_titles in titles.items()
        if target_season == 0
        and any(
            explicit_zero_title.search(unicodedata.normalize("NFKC", title))
            for title in localized_titles
        )
    }
    return sorted(candidates) if len(candidates) == 1 else []


def _unique_season_referenced_special_candidate(
    titles: Mapping[tuple[int, int], Sequence[str]],
    season: int,
) -> list[EpisodeKey]:
    """Return one special whose official title explicitly names the season."""
    season_ref = re.compile(
        rf"(?:第\s*0*{season}\s*季|\bseason\s*0*{season}\b|"
        rf"\b0*{season}(?:st|nd|rd|th)\s+season\b)",
        re.I,
    )
    candidates = {
        EpisodeKey("special", target_episode)
        for (target_season, target_episode), localized_titles in titles.items()
        if target_season == 0
        and any(
            season_ref.search(unicodedata.normalize("NFKC", title))
            for title in localized_titles
        )
    }
    return sorted(candidates) if len(candidates) == 1 else []


def _e00_timeline_candidates(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    season: int,
) -> list[EpisodeKey]:
    """Find a unique full-length Episode 0 immediately preceding a season.

    This is a second, independent online-evidence path for sources named only
    ``00.mkv``.  A candidate must be the sole full-length Season 00 episode in
    the year before the official season premiere.  The wider window covers
    officially released Episode 0 previews that precede the broadcast season
    by roughly half a year. Recaps released during the season, old unrelated
    specials and short promos are excluded.
    """
    try:
        season_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
        specials_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
    except ApiError:
        return []
    regular_episodes = [
        item
        for item in (season_payload.get("episodes") or [])
        if isinstance(item, Mapping)
    ]
    regular_dates = sorted(
        datetime.fromisoformat(str(item["air_date"])).date()
        for item in regular_episodes
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
    )
    regular_runtimes = sorted(
        int(item["runtime"])
        for item in regular_episodes
        if isinstance(item.get("runtime"), int)
        and not isinstance(item.get("runtime"), bool)
        and int(item["runtime"]) > 0
    )
    if not regular_dates or not regular_runtimes:
        return []
    premiere = regular_dates[0]
    median_runtime = regular_runtimes[len(regular_runtimes) // 2]
    candidates: list[EpisodeKey] = []
    recap_title = re.compile(
        r"(?:总集篇|總集篇|総集編|総集篇|recap|digest|compilation|summary)",
        re.IGNORECASE,
    )
    for item in specials_payload.get("episodes") or []:
        if (
            not isinstance(item, Mapping)
            or not isinstance(item.get("episode_number"), int)
            or isinstance(item.get("episode_number"), bool)
            or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}",
                str(item.get("air_date") or ""),
            )
            or not isinstance(item.get("runtime"), int)
            or isinstance(item.get("runtime"), bool)
        ):
            continue
        # TMDB occasionally carries recap broadcasts with placeholder or
        # incorrect early dates. An explicit recap title is negative evidence
        # and must never compete with an original Episode 0/prologue.
        if recap_title.search(str(item.get("name") or "")):
            continue
        air_date = datetime.fromisoformat(str(item["air_date"])).date()
        runtime = int(item["runtime"])
        days_before = (premiere - air_date).days
        if (
            0 <= days_before <= 365
            and 0.75 * median_runtime <= runtime <= 1.5 * median_runtime
        ):
            candidates.append(EpisodeKey("special", int(item["episode_number"])))
    return sorted(set(candidates))


def _map_unique_remaining_unnumbered_special_by_runtime(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    files: Sequence[Mapping[str, Any]],
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
) -> list[str]:
    """Map one unnumbered special only with independent runtime evidence.

    A sole unused TMDB Season 00 slot is never enough. The source must contain
    exactly one still-unmapped video explicitly labelled as a special, an
    already identified special from the same release must provide a size
    anchor, and the source-size ratio must agree with TMDB's official runtime
    ratio. This lets a generic ``Special.mkv`` be resolved after an evidenced
    ``E00`` while leaving multiple extras or weak metadata for review.
    """
    recognized = {
        _collision_key(str(item.get("full_path", "")))
        for items in groups.values()
        for item in items
    }
    pending_videos = [
        dict(item)
        for item in _filter_media(files)
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and _collision_key(str(item.get("full_path", ""))) not in recognized
        and _has_special_context(item)
    ]
    if len(pending_videos) != 1:
        return []

    used_specials = {key for key in groups if key.kind == "special"}
    remaining = sorted(set(special_titles) - used_specials)
    if len(remaining) != 1:
        return []
    target_key = remaining[0]

    try:
        payload = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
    except ApiError:
        return []
    runtimes = {
        EpisodeKey("special", int(item["episode_number"])): int(item["runtime"])
        for item in (payload.get("episodes") or [])
        if isinstance(item, Mapping)
        and isinstance(item.get("episode_number"), int)
        and not isinstance(item.get("episode_number"), bool)
        and isinstance(item.get("runtime"), int)
        and not isinstance(item.get("runtime"), bool)
        and int(item["runtime"]) > 0
    }
    target_runtime = runtimes.get(target_key, 0)
    target_size = int(pending_videos[0].get("size") or 0)
    if target_runtime <= 0 or target_size <= 0:
        return []

    matching_anchors: list[EpisodeKey] = []
    for anchor_key in sorted(used_specials):
        anchor_runtime = runtimes.get(anchor_key, 0)
        anchor_sizes = sorted(
            int(item.get("size") or 0)
            for item in groups.get(anchor_key, ())
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and int(item.get("size") or 0) > 0
        )
        if anchor_runtime <= 0 or not anchor_sizes:
            continue
        anchor_size = anchor_sizes[len(anchor_sizes) // 2]
        source_ratio = target_size / anchor_size
        official_ratio = target_runtime / anchor_runtime
        # Container/codec variation makes byte ratios approximate, but a
        # 33% margin still rejects ordinary same-length episodes, trailers and
        # unrelated extras when the official item is a double-length special.
        if 0.67 <= source_ratio / official_ratio <= 1.33:
            matching_anchors.append(anchor_key)
    if not matching_anchors:
        return []

    groups.setdefault(target_key, []).append(pending_videos[0])
    return [
        "已在线检索 TMDB Season 00：未编号 SP 只剩一个官方候选，且其"
        f"文件大小比例与已确认 {matching_anchors[0].display} 的官方时长比例一致；"
        f"唯一确认映射为 {target_key.display}"
    ]


def _map_unnumbered_special_from_subtitle_title(
    alist: AListClient,
    files: Sequence[Mapping[str, Any]],
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
    *,
    series_titles: Sequence[str],
    regular_episode_count: int,
    tmdb_client: TMDBClient | None = None,
    tmdb_id: int | None = None,
    season: int | None = None,
) -> list[str]:
    """Resolve one generic ``SP`` from bounded ASS/SSA script metadata.

    Some releases deliberately name the bonus video and its subtitle only
    ``[SP]`` while the subtitle's ``[Script Info]`` title retains the official
    release ordinal (for example ``Steins;Gate 25``).  A sole unused Season 00
    row is not enough evidence, so this path additionally requires:

    * exactly one unresolved SP-labelled video and subtitle;
    * a Script Info title naming the same work and the next episode ordinal;
    * exactly one still-unused official Season 00 episode.

    Only the small subtitle file is read.  Dialogue text is intentionally not
    used as identity evidence, and any ambiguity leaves both files untouched.
    """
    recognized = {
        _collision_key(str(item.get("full_path", "")))
        for items in groups.values()
        for item in items
    }
    pending = [
        dict(item)
        for item in _filter_media(files)
        if _collision_key(str(item.get("full_path", ""))) not in recognized
        and _has_special_context(item)
    ]
    videos = [
        item for item in pending
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    subtitles = [
        item for item in pending
        if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
    ]
    if len(videos) != 1 or len(subtitles) != 1 or regular_episode_count <= 0:
        return []
    remaining = sorted(set(special_titles) - {key for key in groups if key.kind == "special"})
    subtitle = subtitles[0]
    size = int(subtitle.get("size") or 0)
    if size <= 0 or size > 2 * 1024 * 1024:
        return []
    reader = getattr(alist, "read_file_bytes", None)
    if not callable(reader):
        return []
    try:
        payload = reader(
            str(subtitle.get("full_path", "")),
            max_bytes=2 * 1024 * 1024,
        )
    except (ApiError, OSError, ValueError):
        return []
    text = ""
    for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "gb18030"):
        try:
            text = payload.decode(encoding)
            break
        except UnicodeError:
            continue
    if not text:
        return []
    script_info = re.split(r"^\s*\[(?!Script Info\])", text, maxsplit=1, flags=re.MULTILINE)[0]
    title_match = re.search(r"^\s*Title\s*:\s*(.+?)\s*$", script_info, re.MULTILINE | re.IGNORECASE)
    if title_match is None:
        return []
    metadata_title = unicodedata.normalize("NFKC", title_match.group(1)).strip()
    ordinals = [int(value) for value in re.findall(r"(?<!\d)(\d{1,4})(?!\d)", metadata_title)]
    if ordinals != [regular_episode_count + 1]:
        return []
    metadata_key = _normalize_match_title(
        re.sub(r"(?<!\d)\d{1,4}(?!\d)|\b(?:gb|chs|cht|sc|tc)\b", " ", metadata_title, flags=re.IGNORECASE)
    )
    title_keys = {
        _normalize_match_title(value)
        for value in series_titles
        if isinstance(value, str) and value.strip()
    }
    title_keys.discard("")
    if not metadata_key or not any(
        metadata_key == title_key
        or (
            min(len(metadata_key), len(title_key)) >= 6
            and (metadata_key in title_key or title_key in metadata_key)
        )
        for title_key in title_keys
    ):
        return []
    evidence = "官方 Season 00 只剩"
    if len(remaining) != 1:
        if (
            tmdb_client is None
            or not isinstance(tmdb_id, int)
            or isinstance(tmdb_id, bool)
            or not isinstance(season, int)
            or isinstance(season, bool)
        ):
            return []
        try:
            regular_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
            special_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
        except ApiError:
            return []
        regular_rows = [
            item for item in (regular_payload.get("episodes") or [])
            if isinstance(item, Mapping)
        ]
        dates = [
            datetime.fromisoformat(str(item["air_date"])).date()
            for item in regular_rows
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
        ]
        runtimes = sorted(
            int(item["runtime"])
            for item in regular_rows
            if isinstance(item.get("runtime"), int)
            and not isinstance(item.get("runtime"), bool)
            and int(item["runtime"]) > 0
        )
        if not dates or not runtimes:
            return []
        finale = max(dates)
        median_runtime = runtimes[len(runtimes) // 2]
        timeline_candidates: list[EpisodeKey] = []
        for item in special_payload.get("episodes") or []:
            if (
                not isinstance(item, Mapping)
                or not isinstance(item.get("episode_number"), int)
                or isinstance(item.get("episode_number"), bool)
                or not isinstance(item.get("runtime"), int)
                or isinstance(item.get("runtime"), bool)
                or not re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or "")
                )
            ):
                continue
            key = EpisodeKey("special", int(item["episode_number"]))
            days_after = (
                datetime.fromisoformat(str(item["air_date"])).date() - finale
            ).days
            runtime = int(item["runtime"])
            if (
                key in special_titles
                and key not in groups
                and 0 <= days_after <= 365
                and 0.75 * median_runtime <= runtime <= 1.5 * median_runtime
            ):
                timeline_candidates.append(key)
        if len(set(timeline_candidates)) != 1:
            return []
        remaining = [timeline_candidates[0]]
        evidence = "官方季终后一年内且与正片等时长的特别篇只有"
    target = remaining[0]
    groups.setdefault(target, []).extend([videos[0], subtitle])
    return [
        "已有界读取未编号 SP 外挂字幕的 Script Info：内部标题"
        f"明确标记为正片 {regular_episode_count} 集后的第 "
        f"{regular_episode_count + 1} 话，且{evidence} "
        f"{target.display}；已唯一确认映射"
    ]


def _e00_movie_queries(item: Mapping[str, Any]) -> list[str]:
    """Build title queries for an E00 that may actually be a standalone work.

    Release groups use E00 for both genuine television prologues and separately
    catalogued pilot movies. The latter often carries a subtitle after the
    episode token (or in its own bracket), so preserve that subtitle while
    removing only numbering and encode noise.
    """
    raw_stem = unicodedata.normalize(
        "NFKC",
        Path(str(item.get("name", ""))).stem,
    )
    bracket_titles = [
        value.strip()
        for value in re.findall(r"\[([^\]]+)\]", raw_stem)
        if re.search(r"[A-Za-z\u3400-\u9fff\u3040-\u30ff]", value)
        and not re.search(
            r"(?:\b(?:AVC|HEVC|AAC|FLAC|WEB|BD|BluRay|CHS|CHT|JPN|"
            r"1080P|2160P|720P|4K|8BIT|10BIT)\b|字幕组|字幕社)",
            value,
            re.IGNORECASE,
        )
    ]
    queries = list(_movie_queries_from_item(item))
    # A release such as ``[Group] Series [00][Movie Subtitle]`` is best
    # searched by combining the clean series query with the specific subtitle.
    for left in list(queries):
        for right in bracket_titles:
            if left != right:
                queries.append(f"{left} {right}")
    cleaned = re.sub(
        r"(?<![A-Za-z0-9])S\s*0*\d+\s*E\s*0+(?!\d)|"
        r"(?<![A-Za-z0-9])(?:E|EP)\s*0+(?!\d)",
        " ",
        raw_stem,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"[\[\](){}]", " ", cleaned)
    cleaned = re.sub(
        r"\b(?:AVC|HEVC|AAC|FLAC|WEB(?:-?DL)?|BluRay|BD|CR|x26[45]|"
        r"1080P|2160P|720P|4K|8BIT|10BIT|CHS|CHT|JPN|MP4|MKV)\b.*$",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"[._]+|\s+", " ", cleaned).strip(" -")
    if cleaned:
        queries.append(cleaned)
    return list(dict.fromkeys(query for query in queries if query))


def _e00_independent_movie_match(
    tmdb_client: TMDBClient,
    items: Sequence[Mapping[str, Any]],
    show: Mapping[str, Any],
) -> AutoMatch | None:
    """Resolve E00 as a movie only when every release version proves one work.

    A sole unfilled special slot is deliberately ignored. Each E00 video must
    instead contain a title longer than the enclosing TV title and independently
    resolve, at high confidence, to the same TMDB movie. This is suitable for
    separately catalogued pilots while leaving generic ``00.mkv`` for review.
    """
    videos = [
        item
        for item in items
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    if not videos:
        return None
    show_keys = {
        _normalize_match_title(str(show.get(field) or ""))
        for field in ("name", "original_name")
    }
    show_keys.discard("")
    resolved: list[AutoMatch] = []
    for item in videos:
        item_matches: dict[int, AutoMatch] = {}
        for query in _e00_movie_queries(item):
            evidence_query = re.sub(
                r"(?<![A-Za-z0-9])S\s*0*\d+\s*E\s*0+(?!\d)|"
                r"(?<![A-Za-z0-9])(?:E|EP)\s*0+(?!\d)",
                " ",
                unicodedata.normalize("NFKC", query),
                flags=re.IGNORECASE,
            )
            evidence_query = re.sub(
                r"\b(?:19|20)\d{2}\b|"
                r"\b(?:AVC|HEVC|AAC|FLAC|WEB(?:-?DL)?|BluRay|BD|CR|x26[45]|"
                r"1080P|2160P|720P|4K|8BIT|10BIT|CHS|CHT|JPN|MP4|MKV)\b",
                " ",
                evidence_query,
                flags=re.IGNORECASE,
            )
            query_key = _normalize_match_title(evidence_query)
            if not query_key:
                continue
            # The query must add a real work subtitle, not merely repeat the
            # enclosing series name with E00/codec tokens.
            if show_keys and not any(
                show_key in query_key and len(query_key.replace(show_key, "")) >= 4
                for show_key in show_keys
            ):
                continue
            try:
                match, _ = auto_match_tmdb(
                    tmdb_client,
                    query,
                    media_type="movie",
                    min_confidence=0.96,
                    prefer_animation=True,
                )
                if match.status != "confirmed":
                    continue
            except ScraperError:
                continue
            if match.media_type == "movie":
                item_matches[match.tmdb_id] = match
        if len(item_matches) != 1:
            return None
        resolved.append(next(iter(item_matches.values())))
    tmdb_ids = {item.tmdb_id for item in resolved}
    return resolved[0] if len(tmdb_ids) == 1 else None


def _remap_numbered_specials_by_official_label(
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
) -> list[str]:
    """Prefer a distinctive official label over a release's reset SP number.

    Disc releases commonly restart at ``SP01`` for every season while also
    carrying the franchise-wide label (for example MMR03). TMDB numbers the
    latter globally in Season 00. Matching the shared label prevents SP01 from
    three seasons collapsing onto the same S00E01 target.
    """
    warnings: list[str] = []
    official_tokens = {
        key: _special_label_tokens(title)
        for key, title in special_titles.items()
    }
    official_context = {
        key: _special_context_tokens(title)
        for key, title in special_titles.items()
    }

    # ``parse_ep_files`` initially groups by the release-local number.  Two
    # different mini-series can therefore collapse into one SP03 bucket before
    # the official-label remapper sees them.  Split that bucket only when every
    # member independently resolves: a distinctive shared title word wins; an
    # otherwise ambiguous member may remain at its literal source number.  This
    # keeps an original ``OVA03`` separate from ``Darkness OVA03`` without
    # guessing when neither file carries sub-series evidence.
    for source_key in sorted(list(groups)):
        if source_key.kind != "special":
            continue
        source_items = list(groups[source_key])
        if sum(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in source_items
        ) < 2:
            continue
        source_contexts = [
            _special_context_tokens(
                f"{item.get('name', '')} {item.get('full_path', '')}"
            )
            for item in source_items
        ]
        common_source_context = (
            set.intersection(*(set(tokens) for tokens in source_contexts))
            if source_contexts
            else set()
        )
        resolved: list[tuple[dict[str, Any], EpisodeKey]] = []
        for item, raw_context in zip(source_items, source_contexts):
            text = f"{item.get('name', '')} {item.get('full_path', '')}"
            labels = _special_label_tokens(text)
            candidates = sorted(
                key for key, tokens in official_tokens.items() if labels & tokens
            )
            if not candidates:
                resolved = []
                break
            # Franchise/release-group words shared by every item in the
            # collapsed bucket are not sub-series evidence.  For example,
            # ``To Love-Ru Trouble`` appears on both releases while only the
            # later one adds ``Darkness``.
            context = raw_context - common_source_context
            scores = {
                key: len(context & official_context[key]) for key in candidates
            }
            best_score = max(scores.values(), default=0)
            best = [key for key, score in scores.items() if score == best_score]
            if best_score > 0 and len(best) == 1:
                target_key = best[0]
            elif source_key in candidates:
                target_key = source_key
            else:
                resolved = []
                break
            resolved.append((item, target_key))
        video_targets = {
            target_key
            for item, target_key in resolved
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        }
        if len(resolved) != len(source_items) or len(video_targets) < 2:
            continue
        groups.pop(source_key)
        for item, target_key in resolved:
            item["_episode_kind_override"] = target_key.kind
            item["_episode_key_override"] = target_key.number
            groups.setdefault(target_key, []).append(item)
            if target_key != source_key:
                warnings.append(
                    f"根据文件中的子系列标题将 {source_key.display} 映射为 {target_key.display}"
                )

    original_specials = [
        (key, list(groups[key]))
        for key in sorted(groups)
        if key.kind == "special"
    ]
    video_routes: dict[EpisodeKey, EpisodeKey] = {}
    subtitle_routes: dict[EpisodeKey, list[tuple[dict[str, Any], EpisodeKey]]] = {}
    used_targets: set[EpisodeKey] = set()
    for source_key, source_items in original_specials:
        video_items = [
            item for item in source_items
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        ]
        item_matches: list[tuple[dict[str, Any], list[EpisodeKey]]] = []
        for item in source_items:
            tokens = _special_label_tokens(
                f"{item.get('name', '')} {item.get('full_path', '')}"
            )
            item_matches.append(
                (
                    item,
                    [key for key, official in official_tokens.items() if tokens & official],
                )
            )

        # A video and its companion subtitles stay together.  Prefer the video
        # label, but if that label is a duplicate typo, use the only still-free
        # official label mentioned by another member of the same SP group.
        if video_items:
            video_candidates = {
                key
                for item, matches in item_matches
                if item in video_items
                for key in matches
            }
            all_candidates = {
                key for _item, matches in item_matches for key in matches
            }
            available_video = sorted(video_candidates - used_targets)
            available_all = sorted(all_candidates - used_targets)
            target_key = (
                available_video[0]
                if len(available_video) == 1
                else available_all[0]
                if len(available_all) == 1
                else None
            )
            if target_key is None:
                continue
            used_targets.add(target_key)
            video_routes[source_key] = target_key
            continue

        # Subtitle-only extras may share a reset SP number.  They can still be
        # separated safely when every file carries one unique official label.
        resolved = [
            (item, matches[0])
            for item, matches in item_matches
            if len(matches) == 1
        ]
        if resolved and len(resolved) == len(source_items):
            subtitle_routes[source_key] = resolved
            continue

    # Apply the complete route table atomically.  Sequentially deleting and
    # inserting dictionary keys corrupts shifted mappings such as
    # SP01→SP02, SP02→SP03, …: the second move consumes the first move's files.
    # Rebuilding from the immutable snapshot preserves every source group.
    occupied_sources = {key for key, _items in original_specials}
    accepted_video_routes = {
        source_key: target_key
        for source_key, target_key in video_routes.items()
        if (
            target_key == source_key
            or target_key not in occupied_sources
            or (
                target_key in video_routes
                and video_routes[target_key] != target_key
            )
        )
    }
    for source_key, _source_items in original_specials:
        groups.pop(source_key, None)
    for source_key, source_items in original_specials:
        if source_key in accepted_video_routes:
            target_key = accepted_video_routes[source_key]
            for item in source_items:
                item["_episode_kind_override"] = target_key.kind
                item["_episode_key_override"] = target_key.number
            groups.setdefault(target_key, []).extend(source_items)
            if target_key != source_key:
                warnings.append(
                    f"根据文件中的官方特别篇标签将 {source_key.display} 映射为 {target_key.display}"
                )
            continue
        if source_key in subtitle_routes:
            for item, target_key in subtitle_routes[source_key]:
                item["_episode_kind_override"] = target_key.kind
                item["_episode_key_override"] = target_key.number
                groups.setdefault(target_key, []).append(item)
                if target_key != source_key:
                    warnings.append(
                        f"根据文件中的官方特别篇标签将 {source_key.display} 映射为 {target_key.display}"
                    )
            continue
        groups.setdefault(source_key, []).extend(source_items)

    return list(dict.fromkeys(warnings))


def _remap_postseason_oav_suffix(
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
) -> list[str]:
    """Resolve ``[13 OAV]`` only from a complete season boundary.

    Some releases use ``N OAV`` to mean the OAV shipped after ordinary
    episode N, not OAV number N.  Accept that interpretation only when regular
    videos completely cover E01–EN and TMDB exposes exactly one official
    special.  Prefix forms such as ``OAV13`` and incomplete seasons remain
    unresolved.
    """
    regular_numbers = {
        key.number
        for key, items in groups.items()
        if key.kind == "regular"
        and not key.end_number
        and any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in items
        )
    }
    if not regular_numbers or regular_numbers != set(range(1, max(regular_numbers) + 1)):
        return []
    boundary = max(regular_numbers)
    source_key = EpisodeKey("special", boundary)
    source_items = groups.get(source_key)
    if not source_items or len(special_titles) != 1:
        return []
    suffix_marker = re.compile(
        rf"(?:^|[\s._\-\[()])0*{boundary}[ ._-]+OAV(?:$|[\s._\-\])()])",
        re.IGNORECASE,
    )
    if not all(
        suffix_marker.search(str(item.get("name", "")))
        for item in source_items
    ):
        return []
    target_key = next(iter(special_titles))
    if target_key in groups and target_key != source_key:
        return []
    for item in source_items:
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = target_key.number
    groups.pop(source_key)
    groups.setdefault(target_key, []).extend(source_items)
    return [
        f"根据完整 E01–E{boundary:02d} 正片边界、后置 OAV 标记和 TMDB 唯一官方"
        f"特别篇，将 {source_key.display} 映射为 {target_key.display}"
    ]


def _remap_suffix_oav_on_air_versions(
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    official_season_counts: Mapping[int, int],
) -> list[str]:
    """Disambiguate release shorthand ``[N OAV]`` as On-Air Version.

    ``OAV`` is overloaded in release names.  It can mean Original Animation
    Video, but some encodes use it for On-Air Version.  Treat it as the latter
    only when TMDB says an ordinary season ends at N, a plain N video exists,
    and the release also contains N+1 as the separately numbered extra.  The
    complete three-way boundary is required; a lone OAV marker is never enough.
    """
    warnings: list[str] = []
    official_boundaries = set(official_season_counts.values())
    for boundary in sorted(official_boundaries):
        source_key = EpisodeKey("special", boundary)
        target_key = EpisodeKey("regular", boundary)
        next_key = EpisodeKey("regular", boundary + 1)
        source_items = groups.get(source_key)
        if not source_items or target_key not in groups or next_key not in groups:
            continue
        suffix_marker = re.compile(
            rf"(?:^|[\s._\-\[()])0*{boundary}[ ._-]+OAV(?:$|[\s._\-\])()])",
            re.IGNORECASE,
        )
        if not all(suffix_marker.search(str(item.get("name", ""))) for item in source_items):
            continue
        if not any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in groups[target_key]
        ) or not any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in groups[next_key]
        ):
            continue
        groups.pop(source_key)
        for item in source_items:
            item["_episode_kind_override"] = "regular"
            item["_episode_key_override"] = boundary
            item["_edition_override"] = "On-Air Version"
        groups[target_key].extend(source_items)
        warnings.append(
            f"根据 TMDB 正片边界 E{boundary:02d}、同集普通版本及独立 E{boundary + 1:02d}，"
            f"将后置 OAV 解释为 On-Air Version，并归入 E{boundary:02d} 版本比较"
        )
    return warnings


def _has_special_context(item: Mapping[str, Any]) -> bool:
    text = unicodedata.normalize(
        "NFKC",
        f"{item.get('name', '')} {item.get('full_path', '')}",
    )
    key = extract_episode_key(str(item.get("name", "")))
    if key is not None and key.kind == "special":
        return True
    return bool(
        re.search(
            r"(?:特别篇|特典|番外|specials?|ovbsp|ova|oav|oad|通往大人的阶梯|最大的危机|"
            r"ex[ ._-]*season|fate[ ._/-]*prototype|special[ ._-]*season|"
            r"柯里乌斯之梦|coleus[ ._-]*no[ ._-]*yume|"
            r"break[ ._-]*time|休息时间|休憩時間|小剧场|小劇場|petit|ぷち|"
            r"课外授业篇|課外授業編|kagai[ ._-]*jugy[oō][ ._-]*hen|"
            r"(?:^|[/\\])SPs?(?:[/\\]|$))",
            text,
            re.IGNORECASE,
        )
    )


def _physical_special_markers(items: Sequence[Mapping[str, Any]]) -> set[str]:
    """Return explicit physical-release markers without inferring placement."""
    markers: set[str] = set()
    for item in items:
        text = unicodedata.normalize(
            "NFKC",
            f"{item.get('name', '')} {item.get('full_path', '')}",
        ).upper()
        markers.update(
            re.findall(r"(?<![A-Z])(?:OVA|OAV|OAD)(?![A-Z])", text)
        )
    return markers


def _numbered_physical_special_markers(
    items: Sequence[Mapping[str, Any]],
) -> set[str]:
    """Return OVA/OAV/OAD markers directly attached to a release ordinal.

    ``OAD 02``, ``[13 OAV]`` and ``OAD Series [01]`` number the
    physical-release order, which is release-local and never by itself equals
    a TMDB Season 00 index.  A marker without an attached number
    (``OVA.mkv``) is resolved by the title/runtime/timeline evidence paths
    instead, so it must not trigger the number-identity gate.  Years are
    excluded (``OAD 2016`` keeps its dated-run evidence path), and a marker
    followed by an ordinal word (``OVA 2nd Season``) is a season label, not a
    release ordinal.
    """
    markers: set[str] = set()
    for item in items:
        text = unicodedata.normalize(
            "NFKC",
            f"{item.get('name', '')} {item.get('full_path', '')}",
        ).upper()
        for marker, number in re.findall(
            r"(?<![A-Z])(OVA|OAV|OAD)[\s._-]*(?:SERIES|系列)?"
            r"[\s._-]*0*(\d{1,3})(?!(?:st|nd|rd|th)|\d)",
            text,
        ):
            if 0 < int(number) <= 999:
                markers.add(f"{marker}{int(number)}")
        for number, marker in re.findall(
            r"(?<![A-Za-z0-9])0*(\d{1,3})[\s._-]*(OVA|OAV|OAD)(?!\d)",
            text,
        ):
            if 0 < int(number) <= 999:
                markers.add(f"{marker}{int(number)}")
    return markers


def _oav_numbered_special_has_official_evidence(
    key: EpisodeKey,
    items: Sequence[Mapping[str, Any]],
    *,
    season: int,
    special_titles: Mapping[EpisodeKey, str],
    special_season_candidates: Mapping[int, Sequence[EpisodeKey]] | None,
) -> bool:
    """Whether an OVA/OAV/OAD-numbered special is backed by official evidence.

    OAD/OVA/OAV numbering is release-local, so the bare number match against
    a TMDB Season 00 index is never identity evidence.  Mapping is allowed
    only when one of these official Season 00 evidence classes holds:

    * an evidence helper already remapped the group by official label,
      complete season boundary or dated release run (every video item then
      carries the official target number override);
    * TMDB's official air-date timeline places this Season 00 episode inside
      the current season's window (work-relationship evidence);
    * TMDB's official Season 00 title itself names this physical-release
      ordinal (for example an official ``OVA 2``).
    """
    videos = [
        item
        for item in items
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    if videos and all(
        item.get("_episode_kind_override") == "special"
        and item.get("_episode_key_override") == key.number
        for item in videos
    ):
        return True
    if special_season_candidates is not None and key in special_season_candidates.get(
        season, ()
    ):
        return True
    official_title = special_titles.get(key)
    if not official_title:
        return False
    return bool(
        re.search(
            rf"(?<![A-Z])(?:OVA|OAV|OAD)\D*(?<!\d)0*{key.number}(?!\d)",
            unicodedata.normalize("NFKC", official_title).upper(),
        )
    )


def _special_context_overrides_parent_season(item: Mapping[str, Any]) -> bool:
    """Whether explicit inner context must override an outer TV-season folder."""
    text = f"{item.get('name', '')} {item.get('full_path', '')}"
    parent = str(item.get("full_path", "")).rsplit("/", 1)[0]
    return bool(
        re.search(
            r"(?:break[ ._-]*time|休息时间|休憩時間|小剧场|小劇場|petit|ぷち|"
            r"课外授业篇|課外授業編|kagai[ ._-]*jugy[oō][ ._-]*hen|"
            r"(?:^|[/\\])SPs?(?:[/\\]|$))",
            text,
            re.IGNORECASE,
        )
        or re.search(
            r"(?:^|[/\\])(?:OVA|OAV|OAD)(?:[ ._-]|[/\\]|$)",
            parent,
            re.IGNORECASE,
        )
    )


def _special_series_ordinal(value: str) -> int | None:
    """Return an explicit mini-series generation such as ``2nd season``.

    A bare number is intentionally ignored: it may be an episode, a story arc
    or part of the title.  Only an ordinal attached to ``season/series`` (or an
    equivalent CJK marker) is acceptable evidence.
    """
    normalized = unicodedata.normalize("NFKC", value)
    patterns = (
        r"(?<!\d)(\d{1,2})(?:st|nd|rd|th)\s*(?:season|series)\b",
        r"\b(?:season|series)\s*(\d{1,2})(?!\d)",
        r"第\s*(\d{1,2})\s*(?:季|期)",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            number = int(match.group(1))
            if 0 < number <= 99:
                return number
    return None


def _ova_volume_ordinal(value: str) -> int | None:
    """Return an explicit OVA/physical-volume ordinal from a directory label."""
    normalized = unicodedata.normalize("NFKC", value)
    patterns = (
        r"(?:^|[\s._\-\[(])(?:OVA|OAV)[\s._-]*0*(\d{1,3})(?=$|[\s._\-\])（(])",
        r"(?:^|[\s._\-\[(])VOL(?:UME)?[\s._-]*0*(\d{1,3})(?=$|[\s._\-\])（(])",
        r"(?:^|[\s._\-\[(])(\d{1,2})(?:ST|ND|RD|TH)[\s._-]*(?:SEASON|SERIES)(?=$|[\s._\-\])（(])",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match and 0 < int(match.group(1)) <= 999:
            return int(match.group(1))
    labels = (
        (1, r"(?:上卷|前篇|前编|前編)"),
        (2, r"(?:下卷|后篇|後篇|后编|後編)"),
    )
    matched = [number for number, pattern in labels if re.search(pattern, normalized)]
    return matched[0] if len(matched) == 1 else None


def _special_release_latin_tokens(value: str) -> set[str]:
    """Extract stable Latin title words while discarding release metadata."""
    normalized = unicodedata.normalize("NFKC", value).casefold()
    ignored = {
        "season", "series", "special", "specials", "episode", "episodes",
        "the", "from", "with", "and", "part", "version", "video", "subtitle",
        "truehd", "flac", "bluray", "webrip", "bdrip", "ma10p", "x264",
        "x265", "hevc", "av1", "ass", "mkv", "mp4", "backup",
    }
    return {
        token
        for token in re.findall(r"[a-z]{3,}", normalized)
        if token not in ignored
    }


def _named_special_context_matches(source_key: str, title: str) -> bool:
    title_key = _normalize_match_title(title)
    if len(source_key) < 6 or len(title_key) < 6:
        return False
    blocks = difflib.SequenceMatcher(None, source_key, title_key).get_matching_blocks()
    longest_block = max(blocks, key=lambda block: block.size, default=None)
    longest = longest_block.size if longest_block is not None else 0
    # Sharing only the franchise name is insufficient.  The named child label
    # must substantially overlap the official special title.
    if longest >= 6 and longest / min(len(source_key), len(title_key)) >= 0.65:
        return True
    # A five-character CJK arc label ending in an explicit narrative-unit
    # suffix is also distinctive when it occurs verbatim in both the source
    # directory and an official special title.  This covers labels such as
    # “课外授业篇” without treating a shared franchise name as evidence.
    shared = (
        source_key[longest_block.a : longest_block.a + longest]
        if longest_block is not None
        else ""
    )
    return longest >= 5 and bool(re.search(r"(?:篇|編|编|章|物语|物語)$", shared))


def _special_release_source_ordinal(item: Mapping[str, Any]) -> int | None:
    """Return a physical episode ordinal without mistaking its year for one.

    Releases such as ``OAD 2016 [01]`` deliberately place the release year
    between the OAD marker and the actual episode bracket.  The general
    episode parser must treat bare OAD as unnumbered, so this narrowly scoped
    parser accepts the final pure-numeric bracket only when the same item also
    carries an explicit OVA/OAV/OAD marker.
    """
    name = unicodedata.normalize("NFKC", str(item.get("name", "")))
    key = extract_episode_key(name)
    if key is not None and key.kind in {"regular", "special"} and key.number > 0:
        return key.number
    if not re.search(r"(?<![A-Z])(?:OVA|OAV|OAD)(?![A-Z])", name, re.IGNORECASE):
        return None
    bracket_numbers = {
        int(match.group(1))
        for match in re.finditer(r"\[\s*0*(\d{1,3})\s*\]", name)
        if 0 < int(match.group(1)) <= 999
    }
    return next(iter(bracket_numbers)) if len(bracket_numbers) == 1 else None


def _special_arc_title_key(value: str) -> str:
    """Normalize one official arc title while retaining its identity words."""
    normalized = unicodedata.normalize("NFKC", value).strip()
    part_suffix = re.compile(
        r"(?:\s*[-:,，：]?\s*(?:"
        r"前篇|後篇|后篇|上篇|下篇|前編|後編|后编|"
        r"第\s*[12一二]\s*(?:话|話|集)|"
        r"part\s*(?:1|2|one|two|i|ii)|episode\s*(?:1|2)"
        r"))\s*$",
        re.IGNORECASE,
    )
    previous = None
    while normalized and normalized != previous:
        previous = normalized
        normalized = part_suffix.sub("", normalized).strip(" -,:，：")
    return _normalize_match_title(normalized)


def _unique_named_special_run(
    parent_label: str,
    *,
    run_length: int,
    source_year: int,
    official_title_variants: Mapping[int, Sequence[str]],
    official_air_dates: Mapping[int, str],
) -> tuple[int, ...] | None:
    """Return one strongly named, dated consecutive Season 00 run.

    Every episode must have a multilingual official title whose arc identity
    matches the source directory, every official air date must agree with the
    explicit source release year, and the best run must beat the runner-up by
    the global eight-point ambiguity margin.
    """
    parent_keys = {
        _normalize_match_title(parent_label),
        *(
            _normalize_match_title(query)
            for query in _franchise_member_queries("/" + parent_label)
        ),
    }
    parent_keys = {key for key in parent_keys if len(key) >= 5}
    if not parent_keys or run_length < 2:
        return None

    official_numbers = sorted(
        number
        for number in official_title_variants
        if isinstance(number, int) and not isinstance(number, bool) and number > 0
    )
    ranked: list[tuple[float, tuple[int, ...]]] = []
    official_set = set(official_numbers)
    for start in official_numbers:
        candidate = tuple(range(start, start + run_length))
        if not set(candidate).issubset(official_set):
            continue
        dates = [str(official_air_dates.get(number) or "") for number in candidate]
        if any(not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) for value in dates):
            continue
        if any(int(value[:4]) != source_year for value in dates):
            continue
        episode_scores: list[float] = []
        for number in candidate:
            variant_scores = [
                _title_similarity(parent_key, title_key)
                for parent_key in parent_keys
                for variant in official_title_variants.get(number, ())
                if (title_key := _special_arc_title_key(str(variant)))
            ]
            best = max(variant_scores, default=0.0)
            if best < 0.90:
                break
            episode_scores.append(best)
        if len(episode_scores) != run_length:
            continue
        # The weakest episode is the safety boundary; the mean only breaks
        # ties between otherwise fully qualifying runs.
        score = min(episode_scores) * 0.8 + (sum(episode_scores) / run_length) * 0.2
        ranked.append((score, candidate))
    ranked.sort(key=lambda row: (-row[0], row[1]))
    if not ranked:
        return None
    if len(ranked) > 1 and ranked[0][0] - ranked[1][0] < AUTO_MATCH_MIN_MARGIN:
        return None
    return ranked[0][1]


def _map_explicit_special_release_runs(
    items: list[dict[str, Any]],
    official_title_variants: Mapping[int, Sequence[str]],
    official_air_dates: Mapping[int, str] | None = None,
) -> list[str]:
    """Map a reset-numbered special mini-series only with converging evidence.

    Disc folders commonly place a named mini-series under ``SPs`` and restart
    its files at ``01``. A dated OAD/OVA run requires all of:

    * a distinctive directory matching every multilingual official arc title;
    * one explicit release year matching every official episode air date;
    * a complete consecutive source run matching one consecutive official run;
    * no runner-up official run inside the global ambiguity margin.

    The older explicit generation/named-arc routes remain available for
    undated releases, but a dated release cannot fall back around these gates.
    """
    warnings: list[str] = []
    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        parent, _ = split_remote(normalize_remote_path(str(item.get("full_path", "/"))))
        by_parent[parent].append(item)

    for parent, members in sorted(by_parent.items(), key=lambda row: _collision_key(row[0])):
        videos_by_number: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in members:
            if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
                continue
            source_number = _special_release_source_ordinal(item)
            if source_number is not None:
                videos_by_number[source_number].append(item)
        video_source_numbers = sorted(videos_by_number)
        if (
            not video_source_numbers
            or video_source_numbers != list(range(1, len(video_source_numbers) + 1))
        ):
            continue
        candidate_run: tuple[int, ...] | None = None
        evidence: str | None = None
        text = " ".join(
            [split_remote(parent)[1], *(str(item.get("name", "")) for item in members)]
        )
        ordinal_values = {
            value
            for item in members
            if (value := _special_series_ordinal(
                f"{item.get('name', '')} {item.get('full_path', '')}"
            )) is not None
        }
        source_tokens = {
            token
            for item in members
            for token in _special_release_latin_tokens(str(item.get("name", "")))
        }
        if len(ordinal_values) == 1 and len(source_tokens) >= 2:
            ordinal = next(iter(ordinal_values))
            candidates = sorted({
                int(number)
                for number, variants in official_title_variants.items()
                if any(_special_series_ordinal(title) == ordinal for title in variants)
                and max(
                    (
                        len(source_tokens & _special_release_latin_tokens(title))
                        for title in variants
                    ),
                    default=0,
                ) >= 2
            })
            if (
                len(candidates) == len(video_source_numbers)
                and candidates == list(range(candidates[0], candidates[-1] + 1))
            ):
                candidate_run = tuple(candidates)
                evidence = f"特别篇系列标识（第 {ordinal} 代）、多语言官方标题"

        source_years = {
            int(value)
            for value in re.findall(r"(?<!\d)((?:19|20)\d{2})(?!\d)", text)
        }
        markers = _physical_special_markers(members)
        if (
            candidate_run is None
            and markers
            and len(source_years) == 1
            and official_air_dates is not None
        ):
            source_year = next(iter(source_years))
            parent_label = split_remote(parent)[1]
            candidate_run = _unique_named_special_run(
                parent_label,
                run_length=len(video_source_numbers),
                source_year=source_year,
                official_title_variants=official_title_variants,
                official_air_dates=official_air_dates,
            )
            if candidate_run is not None:
                evidence = (
                    "子目录名称与多语言官方特别篇标题的唯一连续匹配、"
                    f"{source_year} 年官方发行时间线"
                )
        # Preserve the older strongly named arc rule for undated releases.
        # Dated OAD/OVA runs deliberately cannot fall through here: they must
        # satisfy the stricter official-year/timeline and runner-up gates above.
        if candidate_run is None and not source_years:
            parent_key = _normalize_match_title(split_remote(parent)[1])
            parent_key = re.sub(r"^(?:剧中剧|劇中劇|作中作)", "", parent_key)
            named_aliases = {
                "daisanhikoushoujotai": "第三飞行少女队",
            }
            parent_key = named_aliases.get(parent_key, parent_key)
            candidates = sorted({
                int(number)
                for number, variants in official_title_variants.items()
                if len(parent_key) >= 6
                and any(
                    _named_special_context_matches(parent_key, title)
                    for title in variants
                )
            })
            if (
                len(candidates) == len(video_source_numbers)
                and candidates == list(range(candidates[0], candidates[-1] + 1))
            ):
                candidate_run = tuple(candidates)
                evidence = (
                    "子目录名称与多语言官方特别篇标题的唯一连续匹配、"
                    "多语言官方标题"
                )
        if candidate_run is None or evidence is None:
            continue
        mapped_count = 0
        for item in members:
            source_number = _special_release_source_ordinal(item)
            if (
                source_number is None
                or not 1 <= source_number <= len(candidate_run)
            ):
                continue
            item["_episode_kind_override"] = "special"
            item["_episode_key_override"] = candidate_run[source_number - 1]
            mapped_count += 1
        # Backup subtitle trees can repeat the named release directory under
        # a quality/group wrapper. Attach only subtitles whose own cleaned
        # parent independently resolves to this exact official run; another
        # video directory still needs its own complete-boundary proof.
        for item in items:
            if (
                item in members
                or Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS
                or "_episode_key_override" in item
            ):
                continue
            subtitle_parent, _ = split_remote(
                normalize_remote_path(str(item.get("full_path", "/")))
            )
            subtitle_parent_key = _normalize_match_title(split_remote(subtitle_parent)[1])
            subtitle_parent_key = re.sub(
                r"^(?:剧中剧|劇中劇|作中作)", "", subtitle_parent_key
            )
            subtitle_parent_key = {
                "daisanhikoushoujotai": "第三飞行少女队",
            }.get(subtitle_parent_key, subtitle_parent_key)
            subtitle_candidates = tuple(sorted({
                int(number)
                for number, variants in official_title_variants.items()
                if len(subtitle_parent_key) >= 6
                and any(
                    _named_special_context_matches(subtitle_parent_key, title)
                    for title in variants
                )
            }))
            source_number = _special_release_source_ordinal(item)
            if (
                subtitle_candidates != candidate_run
                or source_number is None
                or not 1 <= source_number <= len(candidate_run)
            ):
                continue
            item["_episode_kind_override"] = "special"
            item["_episode_key_override"] = candidate_run[source_number - 1]
            mapped_count += 1
        if mapped_count:
            warnings.append(
                f"已根据{evidence}和完整连续源编号，将 "
                f"{len(video_source_numbers)} 集短篇映射为 "
                f"SP{candidate_run[0]:02d}–SP{candidate_run[-1]:02d}"
            )
    return warnings


def _propagate_explicit_video_episode_overrides(
    items: Iterable[dict[str, Any]],
) -> int:
    """Copy a proven video override to exact-basename subtitle companions."""
    entries = list(items)
    routes: dict[str, set[tuple[str, int]]] = defaultdict(set)
    for item in entries:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        kind = item.get("_episode_kind_override")
        number = item.get("_episode_key_override")
        if kind not in {"regular", "special"} or not isinstance(number, int):
            continue
        stem = _batch_subtitle_release_stem(str(item.get("name", "")))
        routes[stem].add((str(kind), int(number)))
    changed = 0
    for item in entries:
        if Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS:
            continue
        stem = _batch_subtitle_release_stem(str(item.get("name", "")))
        matches = routes.get(stem, set())
        if len(matches) != 1:
            continue
        kind, number = next(iter(matches))
        item["_episode_kind_override"] = kind
        item["_episode_key_override"] = number
        changed += 1
    return changed


def _map_explicit_beta_alternate(
    items: Iterable[dict[str, Any]],
    official_title_variants: Mapping[int, Sequence[str]],
) -> int:
    """Map an explicit ``23B/23β`` release to one official beta special."""
    beta_title = re.compile(
        r"(?:β|BETA|MISSING[ ._-]*LINK|ミッシングリンク|缺失之环|缺失之環)",
        re.IGNORECASE,
    )
    candidates = {
        number
        for number, titles in official_title_variants.items()
        if any(beta_title.search(str(title)) for title in titles)
    }
    if len(candidates) != 1:
        return 0
    target = next(iter(candidates))
    marker = re.compile(r"\[\s*23\s*(?:B|β)\s*\]", re.IGNORECASE)
    changed = 0
    for item in items:
        if not marker.search(str(item.get("name", ""))):
            continue
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = target
        item["_edition_override"] = "23β"
        changed += 1
    return changed


def _map_minitodo_release_editions(
    items: Iterable[dict[str, Any]],
    official_title_variants: Mapping[int, Sequence[str]],
) -> int:
    """Map evidenced Mini Todoke 2D/3D and epilogue release labels.

    The 2D and stereoscopic 3D files are two presentations of the same
    official Romeo & Juliet mini-theatre episode; ``Epilogue``/``Sorekara`` is
    the following After Story.  The mapping is enabled only when multilingual
    TMDB titles expose one unique candidate for each identity, so these release
    labels cannot affect an unrelated show's generic ``2D``/``3D`` extras.
    """
    romeo_re = re.compile(
        r"romeo.*juliet|罗密欧.*朱丽叶|羅密歐.*朱麗葉|"
        r"ロミオ.*ジュリエット",
        re.IGNORECASE,
    )
    after_re = re.compile(
        r"after[ ._-]*story|epilogue|后日谈|後日談|后篇|後篇|それから",
        re.IGNORECASE,
    )
    romeo = {
        number for number, titles in official_title_variants.items()
        if any(
            romeo_re.search(str(title)) and not after_re.search(str(title))
            for title in titles
        )
    }
    after = {
        number for number, titles in official_title_variants.items()
        if any(after_re.search(str(title)) for title in titles)
    }
    if len(romeo) != 1 or len(after) != 1 or romeo == after:
        return 0
    changed = 0
    for item in items:
        name = unicodedata.normalize("NFKC", str(item.get("name", "")))
        if not re.search(r"mini[ ._-]*todo|minitodo|ミニ届", name, re.IGNORECASE):
            continue
        target: int | None = None
        if re.search(r"epilogue|sorekara|それから", name, re.IGNORECASE):
            target = next(iter(after))
        elif re.search(
            r"romeo.*juliet|\b(?:2D|3D)(?:[ ._-]*ver)?\b",
            name,
            re.IGNORECASE,
        ):
            target = next(iter(romeo))
        if target is None:
            continue
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = target
        changed += 1
    return changed


def _map_disc_extras_by_official_release_runs(
    items: Iterable[dict[str, Any]],
    *,
    show: Mapping[str, Any],
    positive_seasons: Sequence[Mapping[str, Any]],
    special_runtimes: Mapping[int, int],
    special_air_dates: Mapping[int, str],
    special_title_variants: Mapping[int, Sequence[str]] | None = None,
) -> int:
    """Map numbered disc mini-anime and adjacent full-length OVAs.

    Some multi-season releases restart ``Tokuten_Anime01`` for every TV
    season, while TMDB stores all disc extras in one Season 00 sequence.  We
    derive one short-extra run per official TV season from runtime and release
    gaps.  A full-length special immediately following a run is that release
    season's OVA.  The mapping is enabled only when the run count exactly
    matches the official positive-season count.
    """
    items = list(items)
    dated_short: list[tuple[int, date]] = []
    for number, runtime in special_runtimes.items():
        raw_date = special_air_dates.get(number, "")
        if runtime > 10 or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
            continue
        dated_short.append((number, datetime.fromisoformat(raw_date).date()))
    dated_short.sort()
    runs: list[list[int]] = []
    previous_number: int | None = None
    previous_date: date | None = None
    for number, air_date in dated_short:
        if (
            not runs
            or previous_number is None
            or number != previous_number + 1
            or previous_date is None
            or (air_date - previous_date).days >= 180
        ):
            runs.append([])
        runs[-1].append(number)
        previous_number, previous_date = number, air_date
    season_numbers = sorted(
        int(item["season_number"])
        for item in positive_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
    )
    season_episode_counts = {
        int(item["season_number"]): int(item["episode_count"])
        for item in positive_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and isinstance(item.get("episode_count"), int)
        and not isinstance(item.get("episode_count"), bool)
        and int(item["episode_count"]) > 0
    }
    if not season_numbers:
        return 0
    season_air_dates = {
        int(item["season_number"]): datetime.fromisoformat(str(item["air_date"])).date()
        for item in positive_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
    }
    titled_full_specials_by_season: dict[int, list[int]] = defaultdict(list)
    for number, titles in (special_title_variants or {}).items():
        if special_runtimes.get(number, 0) < 18:
            continue
        matched_seasons = {
            season_number
            for title in titles
            if (season_number := _season_from_source("/" + str(title))) is not None
            and season_number in season_episode_counts
        }
        if len(matched_seasons) == 1:
            titled_full_specials_by_season[next(iter(matched_seasons))].append(
                int(number)
            )
    for numbers in titled_full_specials_by_season.values():
        numbers.sort()
    run_by_season: dict[int, list[int]] = {}
    ambiguous_run_seasons: set[int] = set()
    for run in runs:
        first_date = datetime.fromisoformat(special_air_dates[run[0]]).date()
        eligible = [
            (air_date, season_number)
            for season_number, air_date in season_air_dates.items()
            if air_date <= first_date
        ]
        if not eligible:
            continue
        source_season = max(eligible)[1]
        # Multiple indistinguishable short runs for one release season are not
        # safe enough for local SP ordinal mapping.
        if source_season in run_by_season or source_season in ambiguous_run_seasons:
            run_by_season.pop(source_season, None)
            ambiguous_run_seasons.add(source_season)
            continue
        run_by_season[source_season] = run
    if not run_by_season and len(runs) == len(season_numbers):
        run_by_season = dict(zip(season_numbers, runs))

    def item_source_season(item: Mapping[str, Any]) -> int | None:
        name = unicodedata.normalize("NFKC", str(item.get("name", "")))
        source_season = _season_from_series_variant(name, show)
        if source_season is not None:
            return source_season
        for segment in reversed(
            normalize_remote_path(str(item.get("full_path", ""))).split("/")[:-1]
        ):
            source_season = _season_from_source("/" + segment)
            if source_season is None:
                source_season = _season_from_series_variant(segment, show)
            if source_season is not None:
                return source_season
        return None

    # Some series have no short disc extras at all: every Season 00 row is a
    # normal-length episode.  A local ``SP01`` still cannot be copied directly
    # to global S00E01, but a *complete* SP01..SPNN video run can be mapped to
    # the official full-length rows inside that source season's air-date
    # window.  This is the Clannad boundary: first-season overflow E23/E24 owns
    # S00E01/E02, while After Story's complete SP01..SP03 run owns S00E03..E05.
    full_specials_by_season: dict[int, list[int]] = {}
    for source_season, season_start in season_air_dates.items():
        later_dates = [
            air_date
            for number, air_date in season_air_dates.items()
            if number > source_season
        ]
        cutoff = min(later_dates) if later_dates else date.max
        candidates = sorted(
            number
            for number, runtime in special_runtimes.items()
            if runtime >= 18
            and re.fullmatch(
                r"\d{4}-\d{2}-\d{2}", special_air_dates.get(number, "")
            )
            and season_start
            <= datetime.fromisoformat(special_air_dates[number]).date()
            < cutoff
        )
        if (
            candidates
            and candidates == list(range(candidates[0], candidates[-1] + 1))
        ):
            full_specials_by_season[source_season] = candidates

    local_sp_video_numbers: dict[int, set[int]] = defaultdict(set)
    for item in items:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        source_season = item_source_season(item)
        if source_season is None:
            continue
        match = re.search(
            r"(?:^|[\[ _.-])SP\s*0*(\d{1,2})(?:[\] _.-]|$)",
            unicodedata.normalize("NFKC", str(item.get("name", ""))),
            re.I,
        )
        if match is not None:
            local_sp_video_numbers[source_season].add(int(match.group(1)))
    complete_full_sp_runs = {
        source_season: candidates
        for source_season, candidates in full_specials_by_season.items()
        if sorted(local_sp_video_numbers.get(source_season, set()))
        == list(range(1, len(candidates) + 1))
    }
    full_after_run: dict[int, int] = {}
    assigned_runs = sorted(
        (run[0], season_number, run)
        for season_number, run in run_by_season.items()
    )
    for index, (_start, season_number, run) in enumerate(assigned_runs):
        next_start = (
            assigned_runs[index + 1][0]
            if index + 1 < len(assigned_runs)
            else 10**9
        )
        candidates = [
            number
            for number, runtime in special_runtimes.items()
            if run[-1] < number < next_start and runtime >= 18
        ]
        if len(candidates) == 1:
            full_after_run[season_number] = candidates[0]

    changed = 0
    for item in items:
        name = unicodedata.normalize("NFKC", str(item.get("name", "")))
        full_path = str(item.get("full_path", ""))
        # The release filename is the most specific evidence for shared
        # ``字幕备份`` directories.  Looking at generic parents first can
        # incorrectly select the base season before seeing ``2wei/Herz/3rei``.
        source_season = item_source_season(item)
        run = run_by_season.get(source_season or -1)
        tokuten = re.search(r"tokuten[ ._-]*anime\s*0*(\d{1,2})", name, re.I)
        local_sp = re.search(
            r"(?:^|[\[ _.-])SP\s*0*(\d{1,2})(?:[\] _.-]|$)",
            name,
            re.I,
        )
        target: int | None = None
        if tokuten is not None and run is not None:
            ordinal = int(tokuten.group(1))
            if 1 <= ordinal <= len(run):
                target = run[ordinal - 1]
        elif local_sp is not None and run is not None:
            ordinal = int(local_sp.group(1))
            if 1 <= ordinal <= len(run):
                target = run[ordinal - 1]
        elif local_sp is not None and source_season in complete_full_sp_runs:
            ordinal = int(local_sp.group(1))
            full_run = complete_full_sp_runs[int(source_season)]
            if 1 <= ordinal <= len(full_run):
                target = full_run[ordinal - 1]
        elif re.search(r"(?:^|[\[ _-])OVA(?:[\] _.-]|$)", name, re.I):
            target = full_after_run.get(source_season or -1)
        if target is None and source_season in season_air_dates:
            key = extract_episode_key(name)
            season_meta = next(
                (
                    item
                    for item in positive_seasons
                    if item.get("season_number") == source_season
                ),
                None,
            )
            boundary = (
                int(season_meta["episode_count"])
                if isinstance(season_meta, Mapping)
                and isinstance(season_meta.get("episode_count"), int)
                and not isinstance(season_meta.get("episode_count"), bool)
                else 0
            )
            prior_seasons = range(1, int(source_season))
            prior_count = (
                sum(season_episode_counts[number] for number in prior_seasons)
                if all(number in season_episode_counts for number in prior_seasons)
                else 0
            )
            cumulative_regular = (
                prior_count > 0
                and prior_count < key.number <= prior_count + boundary
                if key is not None and key.kind == "regular"
                else False
            )
            if (
                key is not None
                and key.kind in {"regular", "special"}
                and key.number > boundary
                # A sequel may continue the whole-series counter.  Those
                # values are ordinary episodes, not local overflow ordinals
                # for disc OVAs.  Leave them for the strict cumulative-season
                # normalizer, which still requires the complete TMDB boundary.
                and not cumulative_regular
            ):
                next_season_dates = [
                    air_date
                    for number, air_date in season_air_dates.items()
                    if number > source_season
                ]
                cutoff = min(next_season_dates) if next_season_dates else date.max
                dated_full_candidates = sorted(
                    number
                    for number, runtime in special_runtimes.items()
                    if runtime >= 18
                    and re.fullmatch(
                        r"\d{4}-\d{2}-\d{2}", special_air_dates.get(number, "")
                    )
                    and season_air_dates[source_season]
                    <= datetime.fromisoformat(special_air_dates[number]).date()
                    < cutoff
                )
                full_candidates = (
                    titled_full_specials_by_season.get(int(source_season), [])
                    or dated_full_candidates
                )
                ordinal = key.number - boundary
                if 1 <= ordinal <= len(full_candidates):
                    target = full_candidates[ordinal - 1]
        if target is None:
            continue
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = target
        changed += 1

    # Some disc releases name the last short extra only ``OVA``/``OVBSP``
    # instead of continuing the local SP ordinal.  Do not guess from that
    # marker alone: map it only when the same release season has exactly one
    # still-unclaimed official short-extra slot and exactly one unmapped
    # basename carrying an actual video.  Exact-basename subtitle companions
    # follow that video.  This covers layouts such as SP01, SP02, OVBSP and
    # SP01, SP02, OVA without weakening orphan-subtitle handling.
    claimed_by_season: dict[int, set[int]] = defaultdict(set)
    unmapped_named_extras: dict[int, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for item in items:
        name = unicodedata.normalize("NFKC", str(item.get("name", "")))
        full_path = str(item.get("full_path", ""))
        source_season = item_source_season(item)
        run = run_by_season.get(source_season or -1)
        if run is None:
            continue
        override = item.get("_episode_key_override")
        if (
            item.get("_episode_kind_override") == "special"
            and isinstance(override, int)
            and not isinstance(override, bool)
        ):
            if override in run:
                claimed_by_season[int(source_season)].add(override)
            # A full-length OVA can sit outside the short-extra run.  It is
            # already proven and must not count as a second *unmapped* named
            # basename when resolving an adjacent OVBSP.
            continue
        if not re.search(r"(?:^|[\[ _-])(?:OVBSP|OVA|OAV|OAD)(?:[\] _.-]|$)", name, re.I):
            continue
        stem = _collision_key(Path(name).stem)
        unmapped_named_extras[int(source_season)][stem].append(item)

    for source_season, by_stem in unmapped_named_extras.items():
        remaining = [
            number
            for number in run_by_season.get(source_season, ())
            if number not in claimed_by_season[source_season]
        ]
        video_stems = [
            stem
            for stem, members in by_stem.items()
            if any(
                Path(str(member.get("name", ""))).suffix.lower() in VIDEO_EXTS
                for member in members
            )
        ]
        if len(remaining) != 1 or len(video_stems) != 1:
            continue
        target = remaining[0]
        for item in by_stem[video_stems[0]]:
            item["_episode_kind_override"] = "special"
            item["_episode_key_override"] = target
            changed += 1
    return changed


def _map_split_official_special_folder(
    items: Iterable[dict[str, Any]],
    official_title_variants: Mapping[int, Sequence[str]],
) -> int:
    """Split a proven multi-file special when TMDB collapsed it to one row."""
    items = list(items)
    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        full_path = normalize_remote_path(str(item.get("full_path", "/")))
        by_parent[split_remote(full_path)[0]].append(item)

    changed = 0
    for parent, videos in by_parent.items():
        numbered = [
            (key.number, item)
            for item in videos
            if (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        ]
        numbered.sort(key=lambda pair: pair[0])
        numbers = [number for number, _ in numbered]
        if len(numbered) < 2 or len(numbered) > 4 or numbers != list(range(1, len(numbered) + 1)):
            continue
        parent_label = split_remote(parent)[1]
        parent_queries = {
            _normalize_match_title(query)
            for query in _franchise_member_queries("/" + parent_label)
            if len(_normalize_match_title(query)) >= 6
        }
        matches = {
            number
            for number, variants in official_title_variants.items()
            if any(
                _named_special_context_matches(query, title)
                for query in parent_queries
                for title in variants
            )
        }
        if len(matches) != 1:
            continue
        first_target = next(iter(matches))
        if any(
            first_target + offset in official_title_variants
            for offset in range(1, len(numbered))
        ):
            continue
        base_title = next(
            (
                title
                for title in official_title_variants[first_target]
                if isinstance(title, str) and title.strip()
            ),
            parent_label,
        )
        # TMDB deliberately stores the broadcast as one Season 00 episode,
        # while some releases split that episode into two or more files. Keep
        # the official S00 episode identity and express the physical split as
        # part1/part2; inventing S00E07 would collide with future/real TMDB
        # numbering and can also send a bare ``02.mp4`` to an unrelated movie.
        parent_items = [
            item
            for item in items
            if split_remote(normalize_remote_path(str(item.get("full_path", "/"))))[0]
            == parent
        ]
        for index, (source_number, _video) in enumerate(numbered):
            matching_part_items = [
                item
                for item in parent_items
                if (
                    (key := extract_episode_key(str(item.get("name", ""))))
                    is not None
                    and key.kind == "regular"
                    and key.number == source_number
                    and not key.end_number
                )
            ]
            for item in matching_part_items:
                item["_episode_kind_override"] = "special"
                item["_episode_key_override"] = first_target
                item["_episode_part_override"] = index + 1
                item["_episode_title_override"] = base_title
                changed += 1
    return changed


def _matches_named_special_release_context(
    item: Mapping[str, Any],
    official_title_variants: Mapping[int, Sequence[str]],
    *,
    series_titles: Sequence[str] = (),
) -> bool:
    """Recognize a named official special run even without an SP/OVA token."""
    parent_label = normalize_remote_path(
        str(item.get("full_path", "/"))
    ).rsplit("/", 2)[-2]
    # An explicit ordinary season directory is stronger evidence than fuzzy
    # overlap with an official special title that repeats the franchise name
    # (for example ``魔法禁书目录 第三季`` versus a similarly named
    # disc extra).  Only let a season-looking parent enter the named-special
    # matcher when that parent itself also carries explicit special evidence.
    if (
        _season_from_source("/" + parent_label) is not None
        and not re.search(
            r"(?:特别篇|特典|番外|specials?|ovbsp|ova|oav|oad|"
            r"break[ ._-]*time|休息时间|休憩時間|小剧场|小劇場|petit|ぷち|"
            r"课外授业篇|課外授業編|kagai[ ._-]*jugy[oō][ ._-]*hen|"
            r"(?:^|[/\\])SPs?(?:[/\\]|$))",
            parent_label,
            re.IGNORECASE,
        )
    ):
        return False
    parent_keys = {
        _normalize_match_title(parent_label),
        *(
            _normalize_match_title(query)
            for query in _franchise_member_queries("/" + parent_label)
        ),
    }
    parent_keys = {
        re.sub(r"^(?:剧中剧|劇中劇|作中作)", "", key)
        for key in parent_keys
    }
    aliases = {"daisanhikoushoujotai": "第三飞行少女队"}
    parent_keys = {aliases.get(key, key) for key in parent_keys if len(key) >= 6}
    # Franchise query cleanup can reduce a noisy release folder to the exact
    # series title.  That shared title is not evidence for Season 00 when an
    # official special merely repeats the franchise name.
    series_keys = {
        _normalize_match_title(title)
        for title in series_titles
        if _normalize_match_title(title)
    }
    parent_keys.difference_update(series_keys)
    return any(
        _named_special_context_matches(parent_key, title)
        for parent_key in parent_keys
        for variants in official_title_variants.values()
        for title in variants
    )


def _embedded_movie_tmdb_id(item: Mapping[str, Any]) -> int | None:
    text = f"{item.get('name', '')} {item.get('full_path', '')}"
    if not re.search(r"(?:剧场版|电影|movie|film)", text, re.IGNORECASE):
        return None
    match = re.search(r"\{tmdb-(\d+)\}", text, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _has_movie_context(item: Mapping[str, Any]) -> bool:
    full_path = str(item.get("full_path", ""))
    # The release title itself can be the only explicit movie marker.  This is
    # common in otherwise ordinary series shelves such as
    # ``STEINS;GATE Movie``.  Looking only at the parent directory made that
    # file depend on the later fuzzy child-work pass and a transient metadata
    # lookup failure could silently leave it as an unidentified extra.
    for segment in normalize_remote_path(full_path).split("/"):
        if not re.search(
            r"(?:剧场版|劇場版|电影|電影|真人版|live[ ._-]*action|movies?|films?)",
            segment,
            re.IGNORECASE,
        ):
            continue
        # A release-container label such as “S01-S03 合集，附两部剧场版”
        # advertises nested bonus movies; it is not movie evidence for every
        # episode below that root. The actual movie subdirectory still carries
        # its own unqualified movie marker and remains detectable.
        if re.search(
            r"(?:(?:附|含|包含)|[+＋/&、和与及])\s*"
            r"(?:\d+|[一二两三四五六七八九十]+)\s*部?\s*"
            r"(?:剧场版|劇場版|电影|電影|movies?|films?)",
            segment,
            re.IGNORECASE,
        ):
            continue
        return True
    return False


def _movie_query_from_item(item: Mapping[str, Any]) -> str:
    name = Path(str(item.get("name", ""))).stem
    name = re.sub(r"\[[^\]]*\]|\{(?:tmdb|imdb)-[^{}]+\}", " ", name, flags=re.I)
    name = re.sub(
        r"[（(][^（）()]*?(?:Ma\d+p|x26[45]|HEVC|AVC|flac|AAC|ass)"
        r"[^（）()]*[）)]",
        " ",
        name,
        flags=re.I,
    )
    name = re.sub(r"\b(?:OVA|OAV|OAD)\s*0*\d+\b", " ", name, flags=re.I)
    name = re.sub(r"\(\s*[0-9A-F]{8}\s*\)\s*$", " ", name, flags=re.I)
    # Chinese release names often put the clean title first and append an
    # English title/codec payload after a dot.
    if re.search(r"[\u3400-\u9fff]", name):
        name = re.split(r"\.(?=[A-Za-z])", name, maxsplit=1)[0]
    name = re.sub(
        r"\b(?:BD|WEB|BluRay|1080P|2160P|720P|4K|x26[45]|HEVC|日语中字|中字).*$",
        " ",
        name,
        flags=re.I,
    )
    return re.sub(r"[._]+|\s+", " ", name).strip(" -")


def _movie_queries_from_item(item: Mapping[str, Any]) -> list[str]:
    """Return the localized title plus an embedded Latin release-title fallback."""
    primary = _movie_query_from_item(item)
    raw_stem = Path(str(item.get("name", ""))).stem
    bracket_latin = [
        match.strip()
        for match in re.findall(r"\[([^\]]*[A-Za-z][^\]]{5,})\]", raw_stem)
        if not re.fullmatch(
            r"(?:BD|WEB|BluRay|1080P|2160P|720P|4K|x26[45]|HEVC|AV1|GB|CHS|CHT|MP4)"
            r"(?:[_\s-].*)?",
            match.strip(),
            flags=re.I,
        )
    ]
    stem = raw_stem
    stem = re.sub(r"\[[^\]]*\]|\{(?:tmdb|imdb)-[^{}]+\}", " ", stem, flags=re.I)
    latin_match = re.search(r"[A-Za-z][A-Za-z0-9'’:&+._\- ]{5,}", stem)
    latin = latin_match.group(0) if latin_match else ""
    latin = re.sub(
        r"\b(?:BD(?:1080P|2160P|720P)?|WEB|BluRay|1080P|2160P|720P|4K|x26[45]|HEVC|AV1|日语中字|中字)\b.*$",
        " ",
        latin,
        flags=re.I,
    )
    latin = re.sub(r"[._]+|\s+", " ", latin).strip(" -")
    latin_without_year = re.sub(r"\s+(?:19|20)\d{2}$", "", latin).strip()
    parent_queries: list[str] = []
    full_path = normalize_remote_path(str(item.get("full_path", "")))
    for segment in reversed(full_path.rsplit("/", 1)[0].split("/")):
        if not re.search(
            r"(?:剧场版|劇場版|电影|電影|真人版|live[ ._-]*action|movie|film)",
            segment,
            re.I,
        ):
            continue
        cleaned = re.sub(r"^\s*(?:19|20)\d{2}(?:[.\-]\d{1,2})?\s*", "", segment)
        cleaned = re.sub(r"\[[^\]]*\]", " ", cleaned)
        cleaned = re.sub(
            r"\b(?:4k|8k|2160p|1080p|720p|bluray|blu-?ray|web-?dl|webrip)\b.*$",
            " ",
            cleaned,
            flags=re.I,
        )
        cleaned = re.sub(r"[._]+|\s+", " ", cleaned).strip(" -")
        if cleaned:
            parent_queries.extend(
                [
                    cleaned,
                    re.sub(r"^(?:剧场版|劇場版)[：:\s]*", "", cleaned).strip(),
                    re.split(
                        r"(?:剧场版|劇場版|电影|電影|movie|film)[：:\s]*",
                        cleaned,
                        maxsplit=1,
                        flags=re.I,
                    )[-1].strip(),
                ]
            )
            if parent_queries:
                break
    return list(
        dict.fromkeys(
            value
            for value in (
                primary,
                latin,
                latin_without_year,
                *bracket_latin,
                *parent_queries,
            )
            if value
        )
    )


def _specific_movie_query_agrees_with_match(query: str, match: AutoMatch) -> bool:
    """Reject a generic franchise alias when the source names another special."""
    query_key = _normalize_match_title(query)
    if len(query_key) < 6:
        return False
    titles = [
        match.title,
        *(match.decision_trace.get("official_titles") or []),
        *(match.decision_trace.get("aliases_checked") or []),
    ]
    for title in titles:
        title_key = _normalize_match_title(str(title))
        if not title_key:
            continue
        if query_key == title_key:
            return True
        length_ratio = min(len(query_key), len(title_key)) / max(
            len(query_key), len(title_key)
        )
        if (
            length_ratio >= 0.72
            and (query_key in title_key or title_key in query_key)
        ):
            return True
        if difflib.SequenceMatcher(None, query_key, title_key).ratio() >= 0.82:
            return True
    return False


def _numbered_movie_collection_ordinal(item: Mapping[str, Any]) -> int | None:
    """Return a release-order number from a clearly numbered movie filename.

    Movie packs often attach the number to the title (``死亡笔记1：前篇``),
    which is intentionally not parsed as a TV episode.  A second encode may
    use only a leading number (``1国粤日音轨``).  Keep this parser private to
    the collection-evidence pass so those forms cannot become TV episodes.
    """
    stem = unicodedata.normalize("NFKC", Path(str(item.get("name", ""))).stem)
    stem = re.sub(r"^\[[^\]]+\]\s*", "", stem)
    match = re.match(
        r"^\s*(?:(?P<title>.*?[A-Za-z\u3400-\u9fff\u3040-\u30ff])\s*)?"
        r"(?P<number>[1-9]\d{0,2})(?=\s*(?:[:：._\-]|[A-Za-z\u3400-\u9fff\u3040-\u30ff]))",
        stem,
    )
    if not match:
        return None
    title = (match.group("title") or "").strip()
    if title and len(_normalize_match_title(title)) < 3:
        return None
    return int(match.group("number"))


def _has_numbered_movie_collection_context(item: Mapping[str, Any]) -> bool:
    if _numbered_movie_collection_ordinal(item) is None:
        return False
    parent = str(item.get("full_path", "")).rsplit("/", 1)[0]
    parent_label = split_remote(parent)[1]
    return bool(
        re.search(
            r"(?:真人版|live[ ._-]*action|电影合集|電影合集|"
            r"电影系列|電影系列|movie[ ._-]*collection)",
            parent_label,
            re.I,
        )
    )


def _numbered_movie_collection_queries(parent: str, items: Sequence[Mapping[str, Any]]) -> list[str]:
    """Build title-only collection queries from a live-action movie folder."""
    labels = [split_remote(parent)[1]]
    grandparent = split_remote(parent)[0]
    if grandparent and grandparent != "/":
        labels.append(split_remote(grandparent)[1])
    prefixes: list[str] = []
    for item in items:
        stem = unicodedata.normalize("NFKC", Path(str(item.get("name", ""))).stem)
        ordinal = _numbered_movie_collection_ordinal(item)
        if ordinal is None:
            continue
        match = re.match(r"^\s*(.*?[A-Za-z\u3400-\u9fff\u3040-\u30ff])\s*" + str(ordinal), stem)
        if match and len(_normalize_match_title(match.group(1))) >= 3:
            prefixes.append(match.group(1).strip())

    queries: list[str] = []
    for value in [*prefixes, *labels]:
        cleaned = _query_from_source("/" + value)
        cleaned = re.sub(r"\d+\s*[-–—~～至到]\s*\d+", " ", cleaned)
        cleaned = re.sub(r"[（(][^）)]*(?:真人版|live[ ._-]*action)[^）)]*[）)]", " ", cleaned, flags=re.I)
        cleaned = re.sub(
            r"(?:真人版|live[ ._-]*action|中文字幕|字幕版|"
            r"国粤日音轨|國粵日音軌|电影合集|電影合集|"
            r"电影系列|電影系列|movie[ ._-]*collection)",
            " ",
            cleaned,
            flags=re.I,
        )
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_()（）")
        if len(_normalize_match_title(cleaned)) >= 3:
            queries.append(cleaned)
    return list(dict.fromkeys(queries))


def _resolve_numbered_movie_collection_groups(
    tmdb_client: TMDBClient,
    unresolved: Sequence[dict[str, Any]],
) -> tuple[dict[int, list[dict[str, Any]]], list[dict[str, Any]], list[str]]:
    """Resolve numbered live-action movie encodes from official TMDB collections.

    Evidence is deliberately conjunctive: an explicit live-action/movie-pack
    directory, a unique continuous source order, a matching TMDB collection,
    and either the complete official member count or an explicit source range.
    This also lets a ``1-3`` alternate encode join the same first three movies
    without treating its bare numbers as TV episodes.
    """
    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in unresolved:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        parent, _ = split_remote(normalize_remote_path(str(item.get("full_path", ""))))
        if _has_numbered_movie_collection_context(item):
            by_parent[parent].append(item)

    resolved: dict[int, list[dict[str, Any]]] = defaultdict(list)
    consumed: set[str] = set()
    warnings: list[str] = []
    for parent, videos in sorted(by_parent.items()):
        number_to_videos: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in videos:
            ordinal = _numbered_movie_collection_ordinal(item)
            if ordinal is not None:
                number_to_videos[ordinal].append(item)
        numbers = sorted(number_to_videos)
        if len(numbers) < 2 or numbers != list(range(1, numbers[-1] + 1)):
            continue
        range_match = re.search(r"(?<!\d)1\s*[-–—~～至到]\s*(\d{1,3})(?!\d)", split_remote(parent)[1])
        explicit_range = int(range_match.group(1)) if range_match else None
        if explicit_range is not None and explicit_range != numbers[-1]:
            continue

        candidates: dict[int, tuple[float, Mapping[str, Any]]] = {}
        for query in _numbered_movie_collection_queries(parent, videos):
            response = tmdb_client.get("/search/collection", query=query)
            query_key = _normalize_match_title(query)
            for item in response.get("results") or []:
                if not isinstance(item, Mapping) or isinstance(item.get("id"), bool):
                    continue
                title = str(item.get("name") or item.get("original_name") or "")
                score = _title_similarity(query_key, title)
                if score < 0.88:
                    continue
                collection_id = int(item["id"])
                previous = candidates.get(collection_id)
                if previous is None or score > previous[0]:
                    candidates[collection_id] = (score, item)

        eligible: list[tuple[float, int, list[Mapping[str, Any]]]] = []
        for collection_id, (score, _search_item) in candidates.items():
            detail = tmdb_client.get(f"/collection/{collection_id}")
            parts = [
                item for item in (detail.get("parts") or [])
                if isinstance(item, Mapping)
                and not isinstance(item.get("id"), bool)
                and str(item.get("release_date") or "")
            ]
            parts.sort(key=lambda item: (str(item.get("release_date")), int(item["id"])))
            if numbers[-1] > len(parts):
                continue
            if explicit_range is None and len(parts) != numbers[-1]:
                continue
            eligible.append((score, collection_id, parts))
        eligible.sort(reverse=True, key=lambda item: item[0])
        if not eligible or (
            len(eligible) > 1 and eligible[0][0] - eligible[1][0] < 0.08
        ):
            continue

        _score, collection_id, parts = eligible[0]
        for number, members in number_to_videos.items():
            movie_id = int(parts[number - 1]["id"])
            resolved[movie_id].extend(members)
            consumed.update(str(item.get("full_path", "")) for item in members)
        warnings.append(
            f"目录 {parent} 已根据明确真人版语义、连续编号与 "
            f"TMDB 合集 {collection_id} 官方成员上映顺序唯一映射"
        )

    remaining = [
        item for item in unresolved
        if str(item.get("full_path", "")) not in consumed
    ]
    return dict(resolved), remaining, warnings


def _season_overflow_special(
    special_titles: Mapping[EpisodeKey, str],
    *,
    season: int,
    ordinal: int,
    explicit_special_context: bool = False,
    special_season_candidates: Mapping[int, Sequence[EpisodeKey]] | None = None,
) -> tuple[EpisodeKey, str] | None:
    chinese = {
        1: "一", 2: "二", 3: "三", 4: "四", 5: "五",
        6: "六", 7: "七", 8: "八", 9: "九", 10: "十",
    }.get(season)
    season_tokens = [str(season), *( [chinese] if chinese else [])]
    for key, title in special_titles.items():
        normalized = unicodedata.normalize("NFKC", title)
        if not any(re.search(rf"第?\s*{re.escape(token)}\s*季", normalized) for token in season_tokens):
            continue
        ova = re.search(r"(?:OVA|OAV|OAD|特典)\s*(\d+)?", normalized, re.IGNORECASE)
        if not ova:
            continue
        title_ordinal = int(ova.group(1)) if ova.group(1) else 1
        if title_ordinal == ordinal:
            return key, title
    if special_season_candidates is not None:
        candidates = list(special_season_candidates.get(season) or [])
        if 0 < ordinal <= len(candidates):
            key = candidates[ordinal - 1]
            title = special_titles.get(key)
            if title:
                return key, title
    return None


def _map_unnumbered_specials(
    files: Sequence[Mapping[str, Any]],
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
    series_title: str,
    *,
    season: int | None = None,
    special_season_candidates: Mapping[int, Sequence[EpisodeKey]] | None = None,
) -> list[str]:
    """Map unnumbered, explicitly-labelled specials only when one TMDB title wins clearly."""
    recognized = {
        _collision_key(str(item.get("full_path", "")))
        for items in groups.values()
        for item in items
    }
    warnings: list[str] = []
    for raw_item in _filter_media(files):
        full_path = str(raw_item.get("full_path", ""))
        if _collision_key(full_path) in recognized or not _has_special_context(raw_item):
            continue
        source_dir, _ = split_remote(full_path)
        labels = [str(raw_item.get("name", "")), source_dir.rsplit("/", 1)[-1]]
        source_keys = [
            key for key in (_special_title_key(label, series_title) for label in labels)
            if len(key) >= 3
        ]
        if not source_keys:
            continue
        ranked: list[tuple[float, EpisodeKey]] = []
        for episode_key, title in special_titles.items():
            title_key = _special_title_key(title, series_title)
            if len(title_key) < 3:
                continue
            score = max(
                1.0
                if source_key in title_key or title_key in source_key
                else difflib.SequenceMatcher(None, source_key, title_key).ratio()
                for source_key in source_keys
            )
            ranked.append((score, episode_key))
        ranked.sort(key=lambda row: (-row[0], row[1]))
        if not ranked:
            continue
        best_score, best_key = ranked[0]
        second_score = ranked[1][0] if len(ranked) > 1 else 0.0
        if best_score < 0.65 or best_score - second_score < 0.15:
            continue
        item = dict(raw_item)
        groups.setdefault(best_key, []).append(item)
        recognized.add(_collision_key(full_path))
        warnings.append(
            f"根据 TMDB 官方特别篇标题将 {item['name']} 自动映射为 {best_key.display}"
        )

    if season is not None and special_season_candidates is not None:
        recognized = {
            _collision_key(str(item.get("full_path", "")))
            for items in groups.values()
            for item in items
        }
        pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
        priorities = {"OVBSP": 0, "OVA": 1, "OAV": 1, "OAD": 2}
        for raw_item in _filter_media(files):
            full_path = str(raw_item.get("full_path", ""))
            if _collision_key(full_path) in recognized or not _has_special_context(raw_item):
                continue
            normalized = unicodedata.normalize(
                "NFKC", str(raw_item.get("name", ""))
            ).upper()
            marker_match = re.search(r"OVBSP|OVA|OAV|OAD", normalized)
            if marker_match:
                pending[marker_match.group(0)].append(dict(raw_item))
        used = {key for key in groups if key.kind == "special"}
        candidates = [
            key
            for key in special_season_candidates.get(season, ())
            if key not in used
        ]
        release_groups = sorted(
            pending,
            key=lambda marker: (priorities[marker], marker),
        )
        if release_groups and len(release_groups) == len(candidates):
            for marker, target_key in zip(release_groups, candidates):
                groups.setdefault(target_key, []).extend(pending[marker])
                warnings.append(
                    f"根据第 {season} 季官方时间线将 {marker} "
                    f"自动映射为 {target_key.display}"
                )
    return warnings


def _align_subtitles_to_video_sequence(
    groups: dict[EpisodeKey, list[dict[str, Any]]],
) -> bool:
    """Align a complete subtitle track by order when release numbering has safe gaps."""
    video_keys = sorted(
        key
        for key, items in groups.items()
        if key.kind == "regular"
        and key.end_number == 0
        and any(Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS for item in items)
    )
    subtitle_keys = sorted(
        key
        for key, items in groups.items()
        if key.kind == "regular"
        and key.end_number == 0
        and any(Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS for item in items)
    )
    if (
        len(video_keys) < 2
        or len(video_keys) != len(subtitle_keys)
        or video_keys == subtitle_keys
        or [key.number for key in video_keys]
        != list(range(video_keys[0].number, video_keys[0].number + len(video_keys)))
    ):
        return False
    subtitle_items = {
        key: [
            item
            for item in groups[key]
            if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
        ]
        for key in subtitle_keys
    }
    if any(not items for items in subtitle_items.values()):
        return False
    for key in subtitle_keys:
        remaining = [
            item
            for item in groups[key]
            if Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS
        ]
        if remaining:
            groups[key] = remaining
        else:
            del groups[key]
    for source_key, target_key in zip(subtitle_keys, video_keys):
        groups.setdefault(target_key, []).extend(subtitle_items[source_key])
    return True


def build_tv_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    tmdb_id: int,
    season: int,
    absolute: bool,
    prefer_simplified: bool,
    allow_unmapped: bool,
    ignore_orphan_temp: bool = False,
    episode_map_path: Path | None = None,
    episode_group_id: str | None = None,
    auto_special_title_match: bool = False,
    auto_align_subtitles: bool = False,
    source_files: Sequence[Mapping[str, Any]] | None = None,
) -> Plan:
    show = tmdb_client.get(f"/tv/{tmdb_id}")
    title = safe_name(str(show.get("name") or show.get("original_name") or tmdb_id))
    original_title = safe_name(str(show.get("original_name") or title))
    year = _extract_year(show.get("first_air_date"))
    series_label = title
    desired_series_dir = join_remote(parent_path, series_label)
    series_dir, library_identity_state = resolve_existing_library_root(
        alist,
        parent_path=parent_path,
        desired_root=desired_series_dir,
        tmdb_id=tmdb_id,
        tv=True,
    )
    special_titles: dict[EpisodeKey, str] = {}
    special_season_candidates: dict[int, list[EpisodeKey]] = {}
    episode_map = _build_tv_episode_map(
        tmdb_client,
        show,
        tmdb_id,
        season,
        absolute,
        episode_group_id=episode_group_id,
        special_titles=special_titles,
        special_season_candidates=special_season_candidates,
    )
    episode_overrides = _load_episode_map(episode_map_path) if episode_map_path else {}
    override_official_titles: dict[tuple[int, int], str] = {}
    if episode_overrides:
        target_seasons = sorted({value[0] for value in episode_overrides.values()})
        for target_season in target_seasons:
            try:
                payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{target_season}")
            except ApiError as exc:
                raise PlanError(
                    f"无法核验显式集号映射的 TMDB Season {target_season:02d}"
                ) from exc
            episodes = payload.get("episodes") if isinstance(payload, Mapping) else None
            if not isinstance(episodes, list):
                raise PlanError(
                    f"显式集号映射目标 Season {target_season:02d} 缺少官方集数"
                )
            for item in episodes:
                if (
                    isinstance(item, Mapping)
                    and type(item.get("episode_number")) is int
                    and int(item["episode_number"]) > 0
                ):
                    override_official_titles[(target_season, int(item["episode_number"]))] = str(
                        item.get("name") or f"第{int(item['episode_number'])}集"
                    )
        for source_key, (target_season, target_episode, target_end) in episode_overrides.items():
            source_end = source_key.end_number or source_key.number
            resolved_target_end = target_end or target_episode
            if source_end - source_key.number != resolved_target_end - target_episode:
                raise PlanError(
                    f"显式集号映射范围长度不一致: {source_key.display}"
                )
            missing = [
                number for number in range(target_episode, resolved_target_end + 1)
                if (target_season, number) not in override_official_titles
            ]
            if missing:
                raise PlanError(
                    f"显式集号映射目标不存在: "
                    f"S{target_season:02d}E{missing[0]:02d}"
                )

    if source_files is not None:
        files = [dict(item) for item in source_files]
    else:
        files = alist.walk(src_path, ignore_orphan_temp=ignore_orphan_temp)
        # Some OVA-only works store every playable file below folders named
        # ``OVA 01``/``Special``. The normal walk intentionally skips bonus
        # containers for ordinary seasons, but when that produces no video at
        # all, retry with bonus containers included instead of declaring the
        # work empty.
        if not any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in _filter_media(files)
        ):
            files = alist.walk(
                src_path,
                ignore_orphan_temp=ignore_orphan_temp,
                include_bonus=True,
            )
    cleanup_files = _planned_cleanup_files(files)
    # Keep destructive cleanup candidates out of every downstream episode
    # parser.  Computing ``cleanup_files`` alone is not sufficient: parsing
    # the original scan again can otherwise put one source path into both the
    # media plan and the cleanup plan (for example an ED_EP45 variant whose
    # matching E45 primary video proves it is a bonus ending).
    media_files = _filter_media(files)
    tv_bonus_files = [
        item
        for item in media_files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and BONUS_DIRECTORY_RE.search(str(item.get("full_path", "")))
        and bonus_type(str(item.get("name", ""))) is not None
    ]
    if tv_bonus_files:
        bonus_paths = {
            _collision_key(str(item.get("full_path", "")))
            for item in tv_bonus_files
        }
        media_files = [
            item
            for item in media_files
            if _collision_key(str(item.get("full_path", ""))) not in bonus_paths
        ]
    if not any(
        Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        for item in media_files
    ):
        raise PlanError(
            "未找到剧集视频文件；目录可能只有字幕或未解压的分卷压缩包，"
            "已停止以避免仅移动字幕。"
        )
    warnings: list[str] = []
    _append_cleanup_warning(warnings, cleanup_files)
    if library_identity_state == "same_tmdb_id" and series_dir != desired_series_dir:
        warnings.append(
            f"现有 tvshow.nfo 已确认相同 TMDB ID {tmdb_id}；"
            f"已保留现有目录名 {split_remote(series_dir)[1]!r}"
        )
    elif library_identity_state == "matching_name_without_nfo":
        warnings.append(
            "目标存在同名目录但没有可验证的 tvshow.nfo；标题/年份相符，"
            "必须人工核对后才能合并"
        )
    if ignore_orphan_temp:
        warnings.append("已显式忽略 .scraper-tmp-* 遗留条目，可能存在未恢复文件")
    all_groups = parse_ep_files(
        media_files,
        prefer_simplified=False,
        defer_unnumbered_specials=auto_special_title_match,
    )
    subtitle_alignment_applied = bool(
        auto_align_subtitles and _align_subtitles_to_video_sequence(all_groups)
    )
    if subtitle_alignment_applied:
        warnings.append(
            "检测到字幕发布序号与连续视频集号错位；已在数量完全相等时按顺序对齐，请重点审核字幕映射"
        )
    if auto_special_title_match:
        warnings.extend(
            _remap_postseason_oav_suffix(all_groups, special_titles)
        )
        warnings.extend(
            _remap_numbered_specials_by_official_label(all_groups, special_titles)
        )
        warnings.extend(
            _map_unnumbered_specials(
                media_files,
                all_groups,
                special_titles,
                title,
                season=season,
                special_season_candidates=special_season_candidates,
            )
        )
        warnings.extend(
            _map_unnumbered_special_from_subtitle_title(
                alist,
                media_files,
                all_groups,
                special_titles,
                series_titles=[
                    str(show.get("name") or ""),
                    str(show.get("original_name") or ""),
                ],
                regular_episode_count=sum(
                    1
                    for key, (target_season, _target_episode, _title) in episode_map.items()
                    if key.kind == "regular" and target_season == season
                ),
                tmdb_client=tmdb_client,
                tmdb_id=tmdb_id,
                season=season,
            )
        )
    unparsed_paths = _unparsed_media_paths(media_files, all_groups)
    unparsed_videos = [
        path for path in unparsed_paths if Path(path).suffix.lower() in VIDEO_EXTS
    ]
    if unparsed_videos and auto_special_title_match:
        warnings.append(
            f"{len(unparsed_videos)} 个无法唯一识别的附加视频将保留于"
            "源目录待人工确认；"
            "其余可确认媒体仍会正常整理"
        )
    else:
        _raise_unparsed_media(unparsed_videos, "剧集")
    retained_subtitles = [
        path for path in unparsed_paths if Path(path).suffix.lower() in SUBTITLE_EXTS
    ]
    if retained_subtitles:
        warnings.append(
            f"{len(retained_subtitles)} 个无对应视频或无法唯一编号的字幕将保留于"
            "源目录待人工确认，不影响其余媒体整理"
        )
    problem_files = [
        *(
            PlannedProblem(
                source_path=path,
                reason="无法唯一识别的附加视频；保留原位待人工确认",
            )
            for path in unparsed_videos
        ),
        *(
            PlannedProblem(
                source_path=path,
                reason="无对应视频或无法唯一编号的字幕；保留原位待人工确认",
            )
            for path in retained_subtitles
        ),
    ]

    def record_problem(source_path: str, reason: str, target_path: str | None = None) -> None:
        existing = next(
            (item for item in problem_files if item.source_path == source_path), None
        )
        if existing is None:
            problem_files.append(
                PlannedProblem(
                    source_path=source_path,
                    reason=reason,
                    target_path=target_path,
                )
            )
            return
        if reason not in existing.reason:
            existing.reason = f"{existing.reason}；{reason}"
        if target_path:
            existing.target_path = target_path
    groups = (
        parse_ep_files(
            media_files,
            prefer_simplified=True,
            defer_unnumbered_specials=auto_special_title_match,
        )
        if prefer_simplified
        else all_groups
    )
    preferred_excluded_subtitles: set[str] = set()
    preferred_excluded_subtitle_paths: list[str] = []
    if prefer_simplified and groups is not all_groups:
        preferred_paths = {
            _collision_key(str(item.get("full_path", "")))
            for items in groups.values()
            for item in items
        }
        preferred_excluded_subtitles = {
            _collision_key(str(item.get("full_path", "")))
            for items in all_groups.values()
            for item in items
            if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
            and is_traditional_sub(str(item.get("name", "")))
            and _collision_key(str(item.get("full_path", ""))) not in preferred_paths
        }
        if preferred_excluded_subtitles:
            preferred_excluded_subtitle_paths = sorted({
                normalize_remote_path(str(item.get("full_path", "")))
                for items in all_groups.values()
                for item in items
                if _collision_key(str(item.get("full_path", "")))
                in preferred_excluded_subtitles
            }, key=_collision_key)
            warnings.append(
                f"{len(preferred_excluded_subtitles)} 个繁体字幕已有同发行简体对应；"
                "备选字幕已保留在源目录"
            )
            problem_files.extend(
                PlannedProblem(
                    source_path=path,
                    reason=(
                        "同发行简体字幕已被选用；该备选字幕将保留于"
                        "源目录"
                    ),
                )
                for path in preferred_excluded_subtitle_paths
            )
    if auto_special_title_match and groups is not all_groups:
        _remap_postseason_oav_suffix(groups, special_titles)
        _remap_numbered_specials_by_official_label(groups, special_titles)
        _map_unnumbered_specials(
            media_files,
            groups,
            special_titles,
            title,
            season=season,
            special_season_candidates=special_season_candidates,
        )
        _map_unnumbered_special_from_subtitle_title(
            alist,
            media_files,
            groups,
            special_titles,
            series_titles=[
                str(show.get("name") or ""),
                str(show.get("original_name") or ""),
            ],
            regular_episode_count=sum(
                1
                for key, (target_season, _target_episode, _title) in episode_map.items()
                if key.kind == "regular" and target_season == season
            ),
            tmdb_client=tmdb_client,
            tmdb_id=tmdb_id,
            season=season,
        )
    if auto_align_subtitles and groups is not all_groups:
        _align_subtitles_to_video_sequence(groups)
    if not groups:
        raise PlanError("未找到可识别的剧集媒体文件")

    fractional_titles = (
        _multilingual_episode_titles(tmdb_client, tmdb_id, episode_map)
        if any(key.kind == "fractional" for key in groups)
        else {}
    )

    zero_key = EpisodeKey("regular", 0)
    if not absolute and zero_key in groups:
        source_regular_numbers = sorted({
            key.number
            for key in groups
            if key.kind == "regular" and not key.end_number
        })
        official_regular_numbers = sorted({
            key.number
            for key, (target_season, _target_episode, _title) in episode_map.items()
            if key.kind == "regular" and target_season == season
        })
        # Some disc releases number a complete season as 00..N-1 even though
        # TMDB uses 01..N (High School DxD Hero is a real example).  The exact
        # complete boundary proves a zero-based sequence; an isolated E00 or a
        # release that already contains E01..EN still follows the conservative
        # special/prologue evidence path below.
        if (
            official_regular_numbers
            and len(official_regular_numbers) >= 2
            and official_regular_numbers
            == list(range(1, len(official_regular_numbers) + 1))
            and source_regular_numbers
            == list(range(0, len(official_regular_numbers)))
        ):
            shifted_groups: dict[EpisodeKey, list[dict[str, Any]]] = {}
            for key, items in groups.items():
                shifted_key = (
                    EpisodeKey("regular", key.number + 1)
                    if key.kind == "regular" and not key.end_number
                    else key
                )
                shifted_groups.setdefault(shifted_key, []).extend(items)
            groups = shifted_groups
            warnings.append(
                f"源季完整使用零基编号 00–"
                f"{len(official_regular_numbers) - 1:02d}；已按 TMDB 完整边界转换为 "
                f"S{season:02d}E01–E{len(official_regular_numbers):02d}"
            )
    if not absolute and zero_key in groups:
        multilingual_titles = _multilingual_episode_titles(
            tmdb_client,
            tmdb_id,
            episode_map,
        )
        e00_candidates = [
            key
            for key in _e00_special_candidates(
                groups[zero_key],
                multilingual_titles,
                title,
            )
            if key in episode_map and key not in groups
        ]
        evidence = "TMDB 多语言标题"
        if not e00_candidates:
            e00_candidates = [
                key
                for key in _unique_season_referenced_special_candidate(
                    multilingual_titles,
                    season,
                )
                if key in episode_map and key not in groups
            ]
            evidence = f"TMDB 多语言官方第 {season} 季标题"
        if not e00_candidates:
            e00_candidates = [
                key
                for key in _unique_official_episode_zero_candidate(
                    multilingual_titles
                )
                if key in episode_map and key not in groups
            ]
            evidence = "TMDB 多语言官方「Episode 0/第 0 话」标题"
        regular_source_numbers = {
            key.number
            for key in groups
            if key.kind == "regular" and key.number > 0 and not key.end_number
        }
        official_regular_numbers = {
            key.number
            for key, (target_season, _target_episode, _title) in episode_map.items()
            if key.kind == "regular" and target_season == season
        }
        if (
            not e00_candidates
            and regular_source_numbers
            and regular_source_numbers == official_regular_numbers
        ):
            e00_candidates = [
                key
                for key in _e00_timeline_candidates(
                    tmdb_client,
                    tmdb_id,
                    season,
                )
                if key in episode_map and key not in groups
            ]
            evidence = "TMDB 官方开播时间与完整时长"
        if len(e00_candidates) == 1:
            target_key = e00_candidates[0]
            groups[target_key] = groups.pop(zero_key)
            warnings.append(
                f"已检索{evidence}并唯一确认源文件 E00 对应 "
                f"{target_key.display}（序章/第 0 话）"
            )

    if auto_special_title_match:
        warnings.extend(
            _map_unique_remaining_unnumbered_special_by_runtime(
                tmdb_client,
                tmdb_id,
                media_files,
                groups,
                special_titles,
            )
        )
        # The first pass intentionally runs before E00 evidence gathering.
        # Rebuild the unresolved list now so a newly evidenced SP is not still
        # shown as a stale problem.
        warnings = [
            warning
            for warning in warnings
            if "个无法唯一识别的附加视频已保留原位" not in warning
            and "个无对应视频或无法唯一编号的字幕已保留原位" not in warning
            and "个无法唯一识别的附加视频将在执行后自动隔离到" not in warning
            and "个无对应视频或无法唯一编号的字幕将在执行后自动隔离到" not in warning
        ]
        unparsed_paths = [
            path
            for path in _unparsed_media_paths(media_files, groups)
            if _collision_key(path) not in preferred_excluded_subtitles
        ]
        unparsed_videos = [
            path for path in unparsed_paths if Path(path).suffix.lower() in VIDEO_EXTS
        ]
        retained_subtitles = [
            path
            for path in unparsed_paths
            if Path(path).suffix.lower() in SUBTITLE_EXTS
        ]
        problem_files = [
            *(
                PlannedProblem(
                    source_path=path,
                    reason="无法唯一识别的附加视频；保留原位待人工确认",
                )
                for path in unparsed_videos
            ),
            *(
                PlannedProblem(
                    source_path=path,
                    reason="无对应视频或无法唯一编号的字幕；保留原位待人工确认",
                )
                for path in retained_subtitles
            ),
            *(
                PlannedProblem(
                    source_path=path,
                    reason=(
                        "同发行简体字幕已被选用；该备选字幕将保留于"
                        "源目录"
                    ),
                )
                for path in preferred_excluded_subtitle_paths
            ),
        ]
        if unparsed_videos:
            warnings.append(
                f"{len(unparsed_videos)} 个无法唯一识别的附加视频将保留于"
                "源目录待人工确认；"
                "其余可确认媒体仍会正常整理"
            )
        if retained_subtitles:
            warnings.append(
                f"{len(retained_subtitles)} 个无对应视频或无法唯一编号的字幕"
                "将保留原位待人工确认，不影响其余媒体整理"
            )

    planned: list[PlannedFile] = []
    unresolved: list[str] = []
    regular_episode_max = max(
        (
            key.number
            for key, mapped_value in episode_map.items()
            if key.kind == "regular" and mapped_value[0] == season
        ),
        default=0,
    )

    for key in sorted(groups):
        review_reason: str | None = None
        fractional_match: tuple[
            int, int, str, tuple[str, ...], tuple[str, ...]
        ] | None = None
        if key.kind == "fractional":
            fractional_candidates, fractional_resolution_reason = (
                _fractional_recap_evidence_candidates(
                    tmdb_client,
                    tmdb_id,
                    season,
                    key,
                    fractional_titles,
                    groups[key],
                )
            )
            if len(fractional_candidates) == 1:
                fractional_match = fractional_candidates[0]
            else:
                if fractional_candidates:
                    candidate_text = "、".join(
                        f"S{target_season:02d}E{target_episode:02d}「{title}」"
                        for target_season, target_episode, title, _aliases, _evidence
                        in fractional_candidates
                    )
                    reason = (
                        f"{key.display} 的 TMDB 多证据评分出现多个近分候选："
                        f"{candidate_text}；{fractional_resolution_reason}；"
                        "未自动猜测；保留原位待人工确认"
                    )
                else:
                    reason = (
                        f"{key.display} 未通过 TMDB 官方标题/别名、时间线、季度归属、"
                        f"源标题语义、运行时长、唯一性和冲突证据门禁："
                        f"{fractional_resolution_reason}；保留于"
                        "源目录待人工确认"
                    )
                for item in groups[key]:
                    record_problem(normalize_remote_path(str(item["full_path"])), reason)
                warnings.append(reason)
                continue
        preferred_group, lower_resolution_videos = _prefer_highest_resolution_videos(
            groups[key], prefer_simplified=prefer_simplified
        )
        if lower_resolution_videos:
            groups[key] = preferred_group
            for item in lower_resolution_videos:
                source_path = normalize_remote_path(str(item["full_path"]))
                source_dir, original_name = split_remote(source_path)
                preferred_source = str(item["_preferred_resolution_source"])
                cleanup_kind = str(
                    item.get("_duplicate_cleanup_kind", "lower_resolution")
                )
                if cleanup_kind == "burned_subtitle_duplicate":
                    reason = _burned_subtitle_cleanup_reason(preferred_source)
                elif cleanup_kind == "traditional_language_duplicate":
                    reason = _traditional_language_cleanup_reason(preferred_source)
                elif cleanup_kind == "same_resolution_duplicate":
                    reason = _same_resolution_cleanup_reason(preferred_source)
                elif cleanup_kind == "lower_resolution_subtitle":
                    reason = _lower_resolution_subtitle_cleanup_reason(preferred_source)
                else:
                    reason = _lower_resolution_cleanup_reason(preferred_source)
                cleanup_files.append(
                    PlannedCleanup(
                        source_path=source_path,
                        source_dir=source_dir,
                        original_name=original_name,
                        reason=reason,
                        source_size=_entry_size_value(item),
                        source_modified=_entry_modified_value(item),
                        source_hash=_entry_hash_value(item),
                    )
                )
            lower_count = sum(
                item.get("_duplicate_cleanup_kind") == "lower_resolution"
                for item in lower_resolution_videos
            )
            burned_count = sum(
                item.get("_duplicate_cleanup_kind") == "burned_subtitle_duplicate"
                for item in lower_resolution_videos
            )
            same_resolution_count = sum(
                item.get("_duplicate_cleanup_kind") == "same_resolution_duplicate"
                for item in lower_resolution_videos
            )
            lower_subtitle_count = sum(
                item.get("_duplicate_cleanup_kind") == "lower_resolution_subtitle"
                for item in lower_resolution_videos
            )
            if lower_count:
                warnings.append(
                    f"{key.display} 存在同一 TMDB 集号的多个清晰度版本；"
                    f"已优先保留最高可确认清晰度，并计划清理 "
                    f"{lower_count} 个低清晰度重复版本"
                )
            if burned_count:
                warnings.append(
                    f"{key.display} 存在同清晰度的内封/软字幕与内嵌/硬字幕版本；"
                    f"已保留可切换字幕版本，并计划清理 {burned_count} 个硬字幕重复视频"
                )
            if same_resolution_count:
                warnings.append(
                    f"{key.display} 存在同清晰度的重复发布版；"
                    f"已保留文件更完整的版本，并计划清理 "
                    f"{same_resolution_count} 个较小重复视频"
                )
            if lower_subtitle_count:
                warnings.append(
                    f"{key.display} 已计划清理 {lower_subtitle_count} 个"
                    "仅属于低清发布版的重复字幕"
                )
        explicit_special_context = any(_has_special_context(item) for item in groups[key])
        override = episode_overrides.get(key)
        if override is not None:
            override_season, override_episode, override_end = override
            mapped = (
                override_season,
                override_episode,
                override_official_titles[(override_season, override_episode)],
            )
            mapped_end = None
            if override_end:
                mapped_end = (
                    override_season,
                    override_end,
                    override_official_titles[(override_season, override_end)],
                )
            warnings.append(f"{key.display} 使用显式覆盖映射")
        else:
            if fractional_match is not None:
                (
                    target_season,
                    target_episode,
                    target_title,
                    _evidence_titles,
                    mapping_evidence,
                ) = fractional_match
                mapped = (target_season, target_episode, target_title)
                warnings.append(
                    f"{key.display} 经 TMDB 官方 Season 00/季度多证据评分"
                    "（含多语言季度/特别篇标题）"
                    f"自动映射为 S{target_season:02d}E{target_episode:02d}"
                    f"「{target_title}」；证据：{'、'.join(mapping_evidence)}"
                )
            else:
                lookup_key = EpisodeKey(key.kind, key.number)
                mapped = episode_map.get(lookup_key)
                if (
                    mapped is not None
                    and key.kind == "special"
                    and _numbered_physical_special_markers(groups[key])
                    and not _oav_numbered_special_has_official_evidence(
                        key,
                        groups[key],
                        season=season,
                        special_titles=special_titles,
                        special_season_candidates=special_season_candidates,
                    )
                ):
                    # OAD/OVA/OAV numbering is release-local: a coincident
                    # TMDB Season 00 index proves nothing about identity.
                    # Only official Season 00 identity/title/work-relationship
                    # evidence may map it; otherwise fail closed below.
                    mapped = None
            mapped_end = (
                episode_map.get(EpisodeKey(key.kind, key.end_number))
                if key.end_number
                else None
            )
        title_overrides = {
            str(item.get("_episode_title_override"))
            for item in groups[key]
            if isinstance(item.get("_episode_title_override"), str)
            and str(item.get("_episode_title_override")).strip()
        }
        if mapped is not None and len(title_overrides) == 1:
            mapped = (mapped[0], mapped[1], next(iter(title_overrides)))
        if mapped is None:
            overflow_special = (
                _season_overflow_special(
                    special_titles,
                    season=season,
                    ordinal=key.number - regular_episode_max,
                    explicit_special_context=explicit_special_context,
                    special_season_candidates=special_season_candidates,
                )
                if key.kind == "regular"
                and not absolute
                and regular_episode_max > 0
                and key.number > regular_episode_max
                else None
            )
            if overflow_special is not None:
                special_key, special_title = overflow_special
                mapped = (0, special_key.number, special_title)
                warnings.append(
                    f"{key.display} 超出第 {season} 季正片集数；根据 TMDB 标题映射为 {special_key.display}"
                )
        if mapped is None:
            if key.kind == "special":
                physical_markers = _physical_special_markers(groups[key])
                if physical_markers:
                    marker_label = "/".join(sorted(physical_markers))
                    reason = (
                        f"{marker_label} {key.display} 未在 TMDB 官方特别篇中唯一确认身份；"
                        "发行形态不能单独证明 Season 00 归属，"
                        f"源编号也不能机械改写为 S00E{key.number:02d}；"
                        "保留原位待人工确认"
                    )
                else:
                    reason = (
                        f"{key.display} 未在 TMDB 官方特别篇中找到对应集；"
                        "源编号不能证明目标 Season 00 集号；保留于"
                        "源目录待人工确认"
                    )
                for item in groups[key]:
                    record_problem(
                        normalize_remote_path(str(item["full_path"])),
                        reason,
                    )
                warnings.append(reason)
                continue
            elif (
                key.kind == "regular"
                and not absolute
                and regular_episode_max > 0
                and regular_episode_max < key.number <= regular_episode_max + 3
            ):
                reason = (
                    f"{key.display} 超出第 {season} 季官方正片集数，且未检索到"
                    "唯一官方特别篇映射；不能把源编号当作 Season 00 集号；"
                    "保留原位待人工确认"
                )
                for item in groups[key]:
                    record_problem(
                        normalize_remote_path(str(item["full_path"])),
                        reason,
                    )
                warnings.append(reason)
                continue
            elif not allow_unmapped:
                unresolved.append(key.display)
                continue
            elif key.kind == "fractional":
                unresolved.append(key.display)
                continue
            elif absolute:
                unresolved.append(key.display)
                continue
            else:
                mapped = (season, key.number, f"第{key.number}集")
                review_reason = f"{key.display} 未在 TMDB 中找到，将使用回退名称"
                warnings.append(f"{key.display} 未在 TMDB 中找到，使用回退名称")

        if key.end_number:
            if mapped_end is None:
                if allow_unmapped and not absolute and key.kind == "regular":
                    mapped_end = (season, key.end_number, f"第{key.end_number}集")
                    warnings.append(
                        f"{key.display} 的结束集未在 TMDB 中找到，使用回退名称"
                    )
                else:
                    unresolved.append(key.display)
                    continue
            if mapped[0] != mapped_end[0]:
                raise PlanError(
                    f"多集文件 {key.display} 跨越 TMDB 季度 "
                    f"S{mapped[0]:02d} → S{mapped_end[0]:02d}，拒绝生成含糊名称；"
                    "请拆分文件或提供单独覆盖映射。"
                )

        target_season, target_episode, raw_episode_title = mapped
        episode_title = provider_safe_episode_title(raw_episode_title)
        episode_token = f"S{target_season:02d}E{target_episode:02d}"
        if mapped_end is not None:
            episode_token += f"-E{mapped_end[1]:02d}"
            episode_title = provider_safe_episode_title(
                f"{raw_episode_title} + {mapped_end[2]}"
            )
        base_name = f"{title} - {episode_token} - {episode_title}"
        group_files = sorted(groups[key], key=lambda x: _collision_key(str(x["full_path"])))
        part_numbers = sorted({
            int(item["_episode_part_override"])
            for item in group_files
            if isinstance(item.get("_episode_part_override"), int)
            and not isinstance(item.get("_episode_part_override"), bool)
            and int(item["_episode_part_override"]) > 0
        })
        group_videos = [
            item
            for item in group_files
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        ]
        split_episode = (
            len(part_numbers) >= 2
            and group_videos
            and all(
                isinstance(item.get("_episode_part_override"), int)
                and not isinstance(item.get("_episode_part_override"), bool)
                and int(item["_episode_part_override"]) in part_numbers
                for item in group_videos
            )
        )
        if split_episode:
            final_name_by_path: dict[str, str] = {}
            for part_number in part_numbers:
                part_files = [
                    item
                    for item in group_files
                    if item.get("_episode_part_override") == part_number
                ]
                part_names = make_unique_media_names(
                    f"{base_name} - part{part_number}",
                    part_files,
                    preserve_editions=True,
                )
                for item, part_name in zip(part_files, part_names):
                    final_name_by_path[str(item["full_path"])] = part_name
            final_names = [
                final_name_by_path.get(str(item["full_path"]))
                or make_unique_media_names(
                    base_name, [item], preserve_editions=True
                )[0]
                for item in group_files
            ]
        else:
            final_names = make_unique_media_names(
                base_name,
                group_files,
                preserve_editions=True,
            )
        season_dir = join_remote(series_dir, f"Season {target_season:02d}")
        planned_group: list[PlannedFile] = []
        for item, final_name in zip(group_files, final_names):
            planned_item = _planned_file_from_entry(
                item,
                final_name=final_name,
                target_dir=season_dir,
                episode_key=key.display,
            )
            planned.append(planned_item)
            planned_group.append(planned_item)
            if review_reason:
                record_problem(
                    planned_item.source_path,
                    review_reason,
                    join_remote(planned_item.target_dir, planned_item.final_name),
                )
        video_versions = [item for item in planned_group if item.media_kind == "video"]
        video_editions = [
            edition_tag(item.final_name) or edition_tag(item.source_path)
            for item in video_versions
        ]
        distinct_named_editions = (
            len(video_versions) > 1
            and len(set(video_editions)) == len(video_versions)
            and any(video_editions)
        )
        if len(video_versions) > 1 and not distinct_named_editions and not split_episode:
            duplicate_reason = (
                f"同一集检测到 {len(video_versions)} 个视频版本，"
                "已自动保留为主文件及 v2/v3 多发行版，不要求人工确认"
            )
            for item in video_versions:
                record_problem(
                    item.source_path,
                    duplicate_reason,
                    join_remote(item.target_dir, item.final_name),
                )

    bonus_counts: dict[str, int] = defaultdict(int)
    for item in sorted(
        tv_bonus_files,
        key=lambda value: _collision_key(str(value.get("full_path", ""))),
    ):
        kind = bonus_type(str(item.get("name", ""))) or "other"
        bonus_counts[kind] += 1
        serial = "" if bonus_counts[kind] == 1 else str(bonus_counts[kind])
        final_name = _compose_filename(
            series_label,
            f"-{kind}{serial}",
            Path(str(item.get("name", ""))).suffix.lower(),
        )
        planned.append(
            _planned_file_from_entry(
                item,
                final_name=final_name,
                target_dir=series_dir,
            )
        )
    if tv_bonus_files:
        warnings.append(
            f"{len(tv_bonus_files)} 个明确位于特典目录的幕后/访谈/花絮视频"
            "已按 Infuse Extras 命名保留，不作为正片集号"
        )

    if unresolved:
        unresolved_text = ", ".join(sorted(set(unresolved)))
        raise PlanError(
            f"以下集数未在 TMDB 映射中找到，已停止以避免错误归档: {unresolved_text}。"
            "可核对文件名或在非绝对集数模式下使用 --allow-unmapped。"
        )

    if subtitle_alignment_applied:
        existing_problem_paths = {item.source_path for item in problem_files}
        for item in planned:
            if item.media_kind != "subtitle" or item.source_path in existing_problem_paths:
                continue
            record_problem(
                item.source_path,
                "字幕发布序号已按连续视频集号自动对齐，请核对",
                join_remote(item.target_dir, item.final_name),
            )

    cleanup_files = sorted(
        {
            _collision_key(item.source_path): item
            for item in cleanup_files
        }.values(),
        key=lambda item: _collision_key(item.source_path),
    )

    plan = Plan(
        mode="tv",
        source_root=normalize_remote_path(src_path),
        target_root=series_dir,
        files=planned,
        warnings=warnings,
        metadata={
            "tmdb_id": tmdb_id,
            "title": title,
            "original_title": original_title,
            "year": year,
            "poster_path": show.get("poster_path"),
            "backdrop_path": show.get("backdrop_path"),
            "season_posters": {
                str(int(item["season_number"])): str(item["poster_path"])
                for item in (show.get("seasons") or [])
                if isinstance(item, Mapping)
                and not isinstance(item.get("season_number"), bool)
                and item.get("season_number") is not None
                and isinstance(item.get("poster_path"), str)
                and item.get("poster_path")
            },
            "season": season,
            "absolute": absolute,
            "episode_group": episode_group_id,
        },
        cleanup_files=cleanup_files,
        problem_files=problem_files,
    )
    if not absolute and season > 0:
        try:
            completeness_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
        except ApiError:
            completeness_payload = {}
        if isinstance(completeness_payload, Mapping):
            episode_gaps = _tv_episode_resource_gaps(
                alist,
                plan,
                series_dir=series_dir,
                season=season,
                official_episodes=[
                    item
                    for item in (completeness_payload.get("episodes") or [])
                    if isinstance(item, Mapping)
                ],
            )
            if episode_gaps:
                plan.scan_report["resource_gaps"] = episode_gaps
    _add_snapshot_warnings(plan)
    validate_plan(alist, plan)
    return plan


def _combine_plans_as_batch(source_root: str, plans: Sequence[Plan], warning: str) -> Plan:
    member_tv: dict[str, dict[str, Any]] = {}
    member_movies: dict[str, dict[str, Any]] = {}
    member_posters: dict[str, str] = {}
    for plan in plans:
        if plan.mode in {"tv", "mixed"}:
            series_root = str(plan.metadata.get("series_root") or plan.target_root)
            member_tv[series_root] = {
                key: plan.metadata.get(key)
                for key in (
                    "tmdb_id", "title", "original_title", "year", "poster_path", "backdrop_path",
                    "season_posters",
                )
            }
        raw_movies = plan.metadata.get("member_movies")
        raw_posters = plan.metadata.get("member_posters")
        if plan.mode == "movie":
            member_movies[plan.target_root] = {
                "tmdb_id": plan.metadata["tmdb_id"],
                "title": plan.metadata["title"],
                "year": plan.metadata["year"],
            }
        if isinstance(raw_movies, Mapping):
            member_movies.update({str(key): dict(value) for key, value in raw_movies.items()})
        if isinstance(raw_posters, Mapping):
            member_posters.update({str(key): str(value) for key, value in raw_posters.items()})
    target_root = normalize_remote_path(posixpath.commonpath([plan.target_root for plan in plans]))
    resource_gaps = [
        dict(gap)
        for plan in plans
        for gap in (plan.scan_report.get("resource_gaps") or [])
        if isinstance(gap, Mapping)
    ]
    return Plan(
        mode="batch",
        source_root=source_root,
        target_root=target_root,
        files=[item for plan in plans for item in plan.files],
        cleanup_files=_dedupe_cleanup_files(
            item for plan in plans for item in plan.cleanup_files
        ),
        problem_files=[item for plan in plans for item in plan.problem_files],
        warnings=list(dict.fromkeys([warning, *(item for plan in plans for item in plan.warnings)])),
        metadata={
            "title": split_remote(source_root)[1],
            "member_tv": member_tv,
            "member_movies": member_movies,
            "member_posters": member_posters,
        },
        scan_report={"resource_gaps": resource_gaps} if resource_gaps else {},
    )


def _flatten_movie_plan_into_series_root(
    plan: Plan,
    series_root: str,
    *,
    record_warning: bool = True,
) -> Plan:
    """Reject the retired layout that mixed movie and TV entities.

    Kept as a fail-closed compatibility shim so an old internal caller cannot
    silently reintroduce movies beside ``tvshow.nfo``.
    """
    del plan, series_root, record_warning
    raise PlanError("电影不得扁平旁挂到电视剧作品根目录")


def _flatten_series_owned_batch_movies(plans: Sequence[Plan]) -> int:
    """Compatibility shim: movie members now retain independent directories."""
    del plans
    return 0


def _canonical_plan_identity(plan: Plan) -> WorkIdentity | None:
    """Return the singular metadata identity carried by a leaf sub-plan."""
    tmdb_id = plan.metadata.get("tmdb_id")
    if not isinstance(tmdb_id, int) or isinstance(tmdb_id, bool) or tmdb_id <= 0:
        return None
    namespace = {
        "tv": "tmdb.tv",
        "movie": "tmdb.movie",
    }.get(plan.mode)
    return WorkIdentity(namespace, tmdb_id) if namespace is not None else None


def _canonical_leaf_name(
    namespace: str,
    title: str,
    identity: Mapping[str, Any],
) -> str:
    """Derive a work leaf only from confirmed canonical metadata."""
    if namespace == "tmdb.movie":
        year = identity.get("year")
        if isinstance(year, (str, int)) and str(year).strip():
            return safe_name(f"{title} ({str(year).strip()})")
    return safe_name(title)


def _canonical_plan_bindings(
    plan: Plan,
) -> list[tuple[WorkIdentity, str, str, str, str | None]]:
    """Return identity/title/canonical leaf/old root/poster per work."""
    singular = _canonical_plan_identity(plan)
    if singular is not None:
        title = plan.metadata.get("title")
        if isinstance(title, str) and title.strip():
            poster = plan.metadata.get("poster_path")
            return [(
                singular,
                title.strip(),
                _canonical_leaf_name(singular.namespace, title.strip(), plan.metadata),
                normalize_remote_path(plan.target_root),
                str(poster) if isinstance(poster, str) and poster else None,
            )]

    bindings: list[tuple[WorkIdentity, str, str, str, str | None]] = []
    if plan.mode == "mixed":
        tmdb_id = plan.metadata.get("tmdb_id")
        title = plan.metadata.get("title")
        series_root = plan.metadata.get("series_root")
        if (
            isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and tmdb_id > 0
            and isinstance(title, str)
            and title.strip()
            and isinstance(series_root, str)
        ):
            raw_poster = plan.metadata.get("poster_path")
            bindings.append((
                WorkIdentity("tmdb.tv", tmdb_id),
                title.strip(),
                _canonical_leaf_name("tmdb.tv", title.strip(), plan.metadata),
                normalize_remote_path(series_root),
                str(raw_poster)
                if isinstance(raw_poster, str) and raw_poster
                else None,
            ))
    member_posters = plan.metadata.get("member_posters")
    poster_by_root = (
        {normalize_remote_path(str(root)): str(path) for root, path in member_posters.items()}
        if isinstance(member_posters, Mapping)
        else {}
    )
    for field, namespace in (("member_tv", "tmdb.tv"), ("member_movies", "tmdb.movie")):
        rows = plan.metadata.get(field)
        if not isinstance(rows, Mapping):
            continue
        for raw_root, raw_identity in rows.items():
            if not isinstance(raw_identity, Mapping):
                continue
            tmdb_id = raw_identity.get("tmdb_id")
            title = raw_identity.get("title")
            if (
                not isinstance(tmdb_id, int)
                or isinstance(tmdb_id, bool)
                or tmdb_id <= 0
                or not isinstance(title, str)
                or not title.strip()
            ):
                continue
            root = normalize_remote_path(str(raw_root))
            raw_poster = raw_identity.get("poster_path")
            poster = (
                str(raw_poster)
                if isinstance(raw_poster, str) and raw_poster
                else poster_by_root.get(root)
            )
            bindings.append((
                WorkIdentity(namespace, tmdb_id),
                title.strip(),
                _canonical_leaf_name(namespace, title.strip(), raw_identity),
                root,
                poster,
            ))
    return bindings


def _rebase_plan_identity_roots(
    plan: Plan,
    root_map: Mapping[str, str],
) -> None:
    """Rebase several possibly nested identity leaves in one atomic mapping."""
    mappings = sorted(
        (
            (normalize_remote_path(old), normalize_remote_path(new))
            for old, new in root_map.items()
            if _collision_key(old) != _collision_key(new)
        ),
        key=lambda pair: (-len(pair[0]), _collision_key(pair[0])),
    )
    if not mappings:
        return

    def rebase(path: str) -> str:
        normalized = normalize_remote_path(path)
        for old, new in mappings:
            if _path_is_within(normalized, old):
                suffix = normalized[len(old):].lstrip("/")
                return join_remote(new, suffix) if suffix else new
        return normalized

    for item in plan.files:
        item.target_dir = rebase(item.target_dir)
    for item in plan.problem_files:
        if item.target_path:
            item.target_path = rebase(item.target_path)
    if any(_collision_key(plan.target_root) == _collision_key(old) for old, _new in mappings):
        plan.target_root = rebase(plan.target_root)
    series_root = plan.metadata.get("series_root")
    if isinstance(series_root, str):
        plan.metadata["series_root"] = rebase(series_root)
    for field in ("member_posters", "member_movies", "member_tv"):
        rows = plan.metadata.get(field)
        if isinstance(rows, Mapping):
            plan.metadata[field] = {rebase(str(path)): value for path, value in rows.items()}


def _plan_canonical_batch_tree(
    plans: Sequence[Plan],
    *,
    outer_root: str,
    root_identity: WorkIdentity | None = None,
    allow_family_boundaries: bool = True,
    identities: set[WorkIdentity] | None = None,
    preserve_container_root: bool = False,
) -> tuple[str, list[str], dict[str, str]]:
    """Route all identity-bearing sub-plans through the sole tree planner."""
    keyed_bindings: dict[str, tuple[Plan, str]] = {}
    works: list[CanonicalWork] = []
    for index, plan in enumerate(plans):
        for binding_index, (identity, title, leaf_name, old_root, poster) in enumerate(
            _canonical_plan_bindings(plan)
        ):
            if identities is not None and identity not in identities:
                continue
            member_key = (
                f"{index}:{binding_index}:{normalize_remote_path(plan.source_root)}:"
                f"{old_root}"
            )
            keyed_bindings[member_key] = (plan, old_root)
            works.append(CanonicalWork(
                member_key=member_key,
                identity=identity,
                title=safe_name(title),
                leaf_name=leaf_name,
                poster_path=poster,
            ))
    if not works:
        return normalize_remote_path(outer_root), [], {}
    try:
        tree = plan_canonical_work_tree(
            works,
            container_root=normalize_remote_path(outer_root),
            root_identity=root_identity,
            allow_family_boundaries=allow_family_boundaries,
        )
    except CanonicalTreeError as exc:
        raise PlanError(f"作品树无法唯一确定: {exc}") from exc

    per_plan_roots: dict[int, tuple[Plan, dict[str, str]]] = {}
    for placement in tree.placements:
        binding_plan, old_root = keyed_bindings[placement.member_key]
        _plan, roots = per_plan_roots.setdefault(
            id(binding_plan), (binding_plan, {})
        )
        roots[old_root] = placement.target_root
    for binding_plan, roots in per_plan_roots.values():
        _rebase_plan_identity_roots(binding_plan, roots)
        if identities is None:
            binding_plan.target_root = (
                tree.container_root
                if preserve_container_root
                else normalize_remote_path(posixpath.commonpath(list(roots.values())))
            )

    family_posters: dict[str, str] = {}
    for family_root in tree.family_roots:
        candidates = sorted(
            (
                (placement, keyed_bindings[placement.member_key][0])
                for placement in tree.placements
                if _path_is_within(placement.target_root, family_root)
            ),
            key=lambda pair: (
                normalize_remote_path(pair[0].target_root).count("/"),
                _collision_key(str(pair[1].metadata.get("title") or "")),
            ),
        )
        for _placement, candidate_plan in candidates:
            poster = candidate_plan.metadata.get("poster_path")
            if isinstance(poster, str) and poster:
                family_posters[family_root] = poster
                break
    warnings = [
        f"已识别子系列 {split_remote(path)[1]!r}；"
        "canonical 作品树已保持每个独立 TMDB 身份的叶子边界"
        for path in tree.family_roots
    ]
    return tree.container_root, warnings, family_posters


def _nest_batch_subseries(
    plans: Sequence[Plan],
    *,
    outer_root: str,
) -> tuple[list[str], dict[str, str]]:
    """Compatibility adapter; hierarchy decisions belong to the planner."""
    _root, warnings, posters = _plan_canonical_batch_tree(
        plans,
        outer_root=outer_root,
    )
    return warnings, posters


def _confirmed_movie_roots(plan: Plan) -> dict[str, int]:
    """Return target roots carrying an explicit TMDB movie identity."""
    roots: dict[str, int] = {}
    if plan.mode == "movie":
        tmdb_id = plan.metadata.get("tmdb_id")
        if isinstance(tmdb_id, int) and not isinstance(tmdb_id, bool) and tmdb_id > 0:
            roots[normalize_remote_path(plan.target_root)] = tmdb_id
    raw_movies = plan.metadata.get("member_movies")
    if isinstance(raw_movies, Mapping):
        for raw_root, identity in raw_movies.items():
            if not isinstance(identity, Mapping):
                continue
            tmdb_id = identity.get("tmdb_id")
            if isinstance(tmdb_id, int) and not isinstance(tmdb_id, bool) and tmdb_id > 0:
                roots[normalize_remote_path(str(raw_root))] = tmdb_id
    return roots


def _batch_movie_source_variant_key(
    original_name: str,
    source_path: str,
    final_name: str = "",
) -> str:
    """Keep movie parts, named editions and title extras in separate buckets."""
    final_stem = Path(final_name).stem
    if final_name and is_planned_bonus(final_name):
        return f"bonus:{_collision_key(source_path)}"
    release_text = f"{original_name} {source_path} {final_name}"
    if re.search(
        r"(?:^|[\s._\-\[\]()])(?:NC)?(?:OP|ED)(?:\d+)?(?:$|[\s._\-\[\]()])",
        release_text,
        re.IGNORECASE,
    ):
        return f"theme:{_collision_key(source_path)}"
    part_match = re.search(r"(?:^|[\s._\-])part\s*0*(\d+)(?:$|[\s._\-])", final_stem, re.I)
    part = f"part:{int(part_match.group(1))}" if part_match else "main"
    edition = entry_edition_tag({"name": original_name, "full_path": source_path})
    return f"{part}|edition:{_collision_key(edition) if edition else ''}"


def _batch_movie_variant_key(item: PlannedFile) -> str:
    return _batch_movie_source_variant_key(
        item.original_name,
        item.source_path,
        item.final_name,
    )


def _retarget_transitive_movie_cleanups(
    plans: Sequence[Plan],
    removed_winner: PlannedFile,
    final_winner: PlannedFile,
    tmdb_id: int,
) -> int:
    """Retarget proven duplicates when their intermediate winner is removed."""
    removed_entry = {
        "name": removed_winner.original_name,
        "full_path": removed_winner.source_path,
    }
    removed_rank = video_resolution_rank(removed_entry)
    removed_edition = edition_tag(removed_winner.original_name) or edition_tag(
        removed_winner.source_path
    )
    removed_presentation = subtitle_presentation_rank(removed_entry)
    final_rank = video_resolution_rank({
        "name": final_winner.original_name,
        "full_path": final_winner.source_path,
    })
    final_variant = _batch_movie_variant_key(final_winner)
    retargeted = 0
    for plan in plans:
        for cleanup in plan.cleanup_files:
            loser_entry = {
                "name": cleanup.original_name,
                "full_path": cleanup.source_path,
            }
            loser_rank = video_resolution_rank(loser_entry)
            loser_edition = edition_tag(cleanup.original_name) or edition_tag(
                cleanup.source_path
            )
            loser_presentation = subtitle_presentation_rank(loser_entry)
            original_link_proven = False
            if cleanup.reason == _burned_subtitle_cleanup_reason(
                removed_winner.source_path
            ):
                original_link_proven = (
                    loser_edition == removed_edition
                    and loser_rank == removed_rank > 0
                    and loser_presentation < removed_presentation
                )
            elif cleanup.reason == _same_resolution_cleanup_reason(
                removed_winner.source_path
            ):
                original_link_proven = (
                    loser_edition == removed_edition
                    and loser_rank == removed_rank > 0
                    and loser_presentation == removed_presentation
                    and cleanup.source_size is not None
                    and removed_winner.source_size is not None
                    and removed_winner.source_size > cleanup.source_size > 0
                )
            elif cleanup.reason == _lower_resolution_cleanup_reason(
                removed_winner.source_path
            ):
                original_link_proven = (
                    loser_edition == removed_edition
                    and removed_rank > loser_rank
                    and (loser_rank > 0 or removed_rank >= 2160)
                )
            if not original_link_proven:
                continue
            if (
                final_rank <= loser_rank
                or _batch_movie_source_variant_key(
                    cleanup.original_name, cleanup.source_path
                ) != final_variant
                or not _same_concrete_movie_release(
                    cleanup.original_name, cleanup.source_path, final_winner
                )
            ):
                continue
            cleanup.reason = _lower_resolution_movie_cleanup_reason(
                final_winner.source_path, tmdb_id
            )
            retargeted += 1
    return retargeted


def _dedupe_confirmed_batch_movies(
    plans: Sequence[Plan],
    *,
    outer_root: str,
) -> list[str]:
    """Prefer the highest resolution across paths for the same TMDB movie.

    This deliberately requires identical confirmed TMDB movie IDs.  Titles
    never participate in identity.  Equal-resolution files, multipart movies,
    named cuts and OP/ED/bonus videos remain independent.
    """
    outer_root = normalize_remote_path(outer_root)
    roots_by_id: dict[int, set[str]] = defaultdict(set)
    for plan in plans:
        for root, tmdb_id in _confirmed_movie_roots(plan).items():
            if not _path_is_within(root, outer_root):
                continue
            roots_by_id[tmdb_id].add(root)

    warnings: list[str] = []
    for tmdb_id, roots in sorted(roots_by_id.items()):
        if len(roots) < 2:
            continue
        candidates: dict[str, list[tuple[Plan, PlannedFile]]] = defaultdict(list)
        for plan in plans:
            for item in plan.files:
                if item.media_kind != "video":
                    continue
                matching_roots = [
                    root
                    for root in roots
                    if _path_is_within(item.target_dir, root)
                ]
                if not matching_roots:
                    continue
                candidates[_batch_movie_variant_key(item)].append((plan, item))

        removed = 0
        for members in candidates.values():
            if len(members) < 2:
                continue
            ranks = {
                id(item): video_resolution_rank({
                    "name": item.original_name,
                    "full_path": item.source_path,
                })
                for _plan, item in members
            }
            best_rank = max(ranks.values(), default=0)
            if best_rank <= 0:
                continue
            preferred = min(
                (item for _plan, item in members if ranks[id(item)] == best_rank),
                key=lambda item: _collision_key(item.source_path),
            )
            for plan, item in members:
                rank = ranks[id(item)]
                if rank >= best_rank or not (rank > 0 or best_rank >= 2160):
                    continue
                _retarget_transitive_movie_cleanups(
                    plans,
                    item,
                    preferred,
                    tmdb_id,
                )
                plan.files.remove(item)
                plan.cleanup_files.append(
                    PlannedCleanup(
                        source_path=item.source_path,
                        source_dir=item.source_dir,
                        original_name=item.original_name,
                        reason=_lower_resolution_movie_cleanup_reason(
                            preferred.source_path,
                            tmdb_id,
                        ),
                        source_size=item.source_size,
                        source_modified=item.source_modified,
                        source_hash=item.source_hash,
                    )
                )
                removed += 1

        _plan_canonical_batch_tree(
            plans,
            outer_root=outer_root,
            allow_family_boundaries=False,
            identities={WorkIdentity("tmdb.movie", tmdb_id)},
        )
        if removed:
            warnings.append(
                f"TMDB movie/{tmdb_id} 在多个源目录中存在同内容清晰度副本；"
                f"已统一到确认的电影目录并计划清理 {removed} 个低清视频"
            )
    return warnings


def _normalize_cumulative_season_episode_numbers(
    season_number: int,
    source_files: Sequence[Mapping[str, Any]],
    official_seasons: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], str | None]:
    """Convert a proven whole-series counter into season-relative numbers.

    Some releases keep counting regular episodes across seasons (25–48 for
    season 2, 49–72 for season 3).  This conversion is intentionally narrow:
    every earlier official season must have a known episode count and the
    source video sequence must start exactly at the resulting season boundary.
    Decimal labels are evidence-bearing source identifiers and are never
    changed here; they still go through the regular-season/S00 online search.
    """
    if season_number <= 1:
        return [dict(item) for item in source_files], None

    counts = {
        int(item["season_number"]): int(item["episode_count"])
        for item in official_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and isinstance(item.get("episode_count"), int)
        and not isinstance(item.get("episode_count"), bool)
        and int(item["season_number"]) > 0
        and int(item["episode_count"]) > 0
    }
    required_seasons = range(1, season_number + 1)
    if any(number not in counts for number in required_seasons):
        return [dict(item) for item in source_files], None

    prior_count = sum(counts[number] for number in range(1, season_number))
    current_count = counts[season_number]
    regular_video_keys: list[int] = []
    video_keys_by_resolution: dict[int, set[int]] = defaultdict(set)
    for item in source_files:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        key = extract_episode_key(str(item.get("name", "")))
        if key is None or key.kind != "regular":
            continue
        if key.end_number or item.get("_episode_key_override") is not None:
            return [dict(value) for value in source_files], None
        regular_video_keys.append(key.number)
        video_keys_by_resolution[video_resolution_rank(item)].add(key.number)

    unique_keys = sorted(set(regular_video_keys))
    simple_cumulative = (
        bool(unique_keys)
        and unique_keys[0] == prior_count + 1
        and unique_keys == list(range(unique_keys[0], unique_keys[-1] + 1))
        and unique_keys[-1] <= prior_count + current_count
    )
    # Some releases split a two-cour sequel into separate TMDB seasons while
    # keeping a sequel-local counter (S03=01–12, S04=13–24).  An explicit
    # season group containing exactly the official count as one consecutive
    # run proves the local offset without guessing from a partial batch.
    local_run_offset = (
        unique_keys[0] - 1
        if (
            not simple_cumulative
            and len(unique_keys) == current_count
            and unique_keys[0] > 1
            and unique_keys
            == list(range(unique_keys[0], unique_keys[0] + current_count))
        )
        else 0
    )

    # A library can contain both release-numbering conventions for the same
    # season: a preferred 2160p set numbered cumulatively across the whole
    # show (25–48) and one or more 1080p backups numbered either 25–48 or
    # 01–24.  Treating their union as E01–E48 makes the latter half look
    # unmapped.  Accept the mixed form only when the two legal ranges do not
    # overlap and the highest-resolution cumulative run starts exactly at the
    # official TMDB season boundary.  This keeps the inference mechanical and
    # lets the normal duplicate-quality pass remove every lower-resolution
    # counterpart after both conventions receive the same season-relative key.
    mixed_numbering = False
    if not simple_cumulative and prior_count >= current_count and video_keys_by_resolution:
        best_resolution = max(video_keys_by_resolution)
        best_keys = video_keys_by_resolution[best_resolution]
        best_cumulative = sorted(
            key
            for key in best_keys
            if prior_count < key <= prior_count + current_count
        )
        relative_keys = sorted(key for key in unique_keys if 1 <= key <= current_count)
        cumulative_keys = sorted(
            key
            for key in unique_keys
            if prior_count < key <= prior_count + current_count
        )
        legal_keys = set(relative_keys) | set(cumulative_keys)
        mixed_numbering = (
            bool(relative_keys)
            and bool(best_cumulative)
            and best_cumulative[0] == prior_count + 1
            and best_cumulative
            == list(range(best_cumulative[0], best_cumulative[-1] + 1))
            and relative_keys == list(range(1, relative_keys[-1] + 1))
            and cumulative_keys
            == list(range(prior_count + 1, cumulative_keys[-1] + 1))
            and set(unique_keys) == legal_keys
        )

    if not simple_cumulative and not mixed_numbering and not local_run_offset:
        return [dict(item) for item in source_files], None
    if not local_run_offset and (
        not unique_keys
        or max(key for key in unique_keys if key > prior_count)
        > prior_count + current_count
    ):
        return [dict(item) for item in source_files], None

    normalized: list[dict[str, Any]] = []
    for raw_item in source_files:
        item = dict(raw_item)
        key = extract_episode_key(str(item.get("name", "")))
        if (
            key is not None
            and key.kind == "regular"
            and not key.end_number
        ):
            if local_run_offset and key.number in unique_keys:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = key.number - local_run_offset
            elif prior_count < key.number <= prior_count + current_count:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = key.number - prior_count
            elif mixed_numbering and 1 <= key.number <= current_count:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = key.number
        normalized.append(item)

    cumulative_keys = (
        unique_keys
        if local_run_offset
        else [
            key for key in unique_keys
            if prior_count < key <= prior_count + current_count
        ]
    )
    mapped_end = max(cumulative_keys) - (
        local_run_offset if local_run_offset else prior_count
    )
    warning = (
        f"检测到第 {season_number} 季使用"
        + ("续作内累计编号 " if local_run_offset else "全剧累计编号 ")
        + f"{min(cumulative_keys)}–{max(cumulative_keys)}；已依据 "
        + (
            f"TMDB 第 {season_number} 季完整 {current_count} 集边界"
            if local_run_offset
            else f"TMDB 前序季度的 {prior_count} 集边界"
        )
        + "换算为 "
        f"S{season_number:02d}E01–S{season_number:02d}E{mapped_end:02d}。"
        + (
            "同时识别到季度内从 01 重新编号的低清晰度备份，"
            "已合并为同集版本并按清晰度去重。"
            if mixed_numbering
            else ""
        )
        +
        "小数集号未参与换算，仍需分别检索常规季与特别篇后确认"
    )
    return normalized, warning


def _tmdb_long_season_block_counts(
    episodes: Sequence[Mapping[str, Any]],
    *,
    minimum_gap_days: int = 90,
) -> list[int]:
    """Split one TMDB season into provable broadcast blocks.

    Some TMDB records keep every broadcast season in one continuously numbered
    season.  A quarterly-or-longer air-date gap is usable evidence for reset
    points used by release folders.  The caller still requires the source
    folders to cover those exact blocks, so a pause alone cannot force a split.
    Invalid, incomplete or non-contiguous metadata deliberately returns no
    blocks so the caller falls back to review instead of guessing.
    """
    rows: list[tuple[int, datetime]] = []
    for item in episodes:
        number = item.get("episode_number")
        air_date = item.get("air_date")
        if (
            not isinstance(number, int)
            or isinstance(number, bool)
            or number <= 0
            or not isinstance(air_date, str)
            or not air_date
        ):
            return []
        try:
            parsed_date = datetime.strptime(air_date, "%Y-%m-%d")
        except ValueError:
            return []
        rows.append((number, parsed_date))
    rows.sort(key=lambda row: row[0])
    if (
        len(rows) < 2
        or [number for number, _date in rows] != list(range(1, len(rows) + 1))
    ):
        return []
    block_counts: list[int] = []
    block_start = 0
    for index in range(1, len(rows)):
        if (rows[index][1] - rows[index - 1][1]).days >= minimum_gap_days:
            block_counts.append(index - block_start)
            block_start = index
    block_counts.append(len(rows) - block_start)
    return block_counts if len(block_counts) >= 2 else []


def _merge_broadcast_folders_into_long_tmdb_season(
    season_groups: dict[int, list[dict[str, Any]]],
    *,
    official_season: int,
    block_counts: Sequence[int],
) -> list[str]:
    """Merge reset/cumulative broadcast folders into one TMDB long season.

    The official-season group may already contain a whole-series absolute
    release (for example E01-E24).  Later source folders can then be either
    local E01-EN or cumulative E(prior+1)-E(prior+N).  Merge a folder only when
    it exactly covers one air-date block and the preceding absolute range is
    already complete.
    """
    base_items = season_groups.get(official_season)
    if not base_items or not block_counts:
        return []

    def video_numbers(items: Sequence[Mapping[str, Any]]) -> set[int]:
        return {
            int(item.get("_episode_key_override", key.number))
            for item in items
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        }

    absolute_numbers = video_numbers(base_items)
    # The ordinary packer below already handles one local folder per block,
    # including a partially aired latest block.  This helper is specifically
    # for the mixed layout where the official group contains an absolute
    # whole-series release extending beyond the first broadcast block.
    if not absolute_numbers or max(absolute_numbers) <= int(block_counts[0]):
        return []
    warnings: list[str] = []
    for source_season in sorted(set(season_groups) - {official_season}):
        block_index = source_season - 1
        if block_index <= 0 or block_index >= len(block_counts):
            continue
        block_count = int(block_counts[block_index])
        prior_count = sum(int(value) for value in block_counts[:block_index])
        if not set(range(1, prior_count + 1)).issubset(absolute_numbers):
            continue
        members = season_groups[source_season]
        raw_numbers = {
            int(item.get("_episode_key_override", key.number))
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        }
        local_range = set(range(1, block_count + 1))
        cumulative_range = set(range(prior_count + 1, prior_count + block_count + 1))
        if raw_numbers == cumulative_range:
            numbering = "累计"
            route = {number: number for number in cumulative_range}
        elif raw_numbers == local_range:
            numbering = "本季重置"
            route = {number: prior_count + number for number in local_range}
        else:
            continue
        for item in members:
            key = extract_episode_key(str(item.get("name", "")))
            if key is None or key.kind != "regular" or key.end_number:
                continue
            mapped = route.get(int(item.get("_episode_key_override", key.number)))
            if mapped is not None:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = mapped
        base_items.extend(members)
        season_groups.pop(source_season)
        absolute_numbers.update(cumulative_range)
        warnings.append(
            f"源第 {source_season} 季完整使用{numbering}编号；根据 TMDB 播出断档块 "
            f"{block_count} 集映射为长季 E{prior_count + 1:02d}–"
            f"E{prior_count + block_count:02d}"
        )
    return warnings


def _merge_release_seasons_into_long_tmdb_season_by_major_gaps(
    season_groups: dict[int, list[dict[str, Any]]],
    *,
    official_season: int,
    official_episodes: Sequence[Mapping[str, Any]],
    today: date | None = None,
    minimum_gap_days: int = 180,
) -> list[str]:
    """Map explicit release seasons into one TMDB long season safely.

    A long TMDB season can contain several real TV seasons and also split
    cours.  The ordinary 90-day block detector intentionally sees both.  This
    fallback uses only major gaps (six months), ignores unaired future rows,
    and requires each source season to cover the exact corresponding local
    range.  It therefore maps Re:Zero S3/S4 without treating a mid-season cour
    break as a new season or importing TMDB's future E78-E85 rows.
    """
    cutoff = today or datetime.now().date()
    rows: list[tuple[int, date]] = []
    for item in official_episodes:
        number = item.get("episode_number")
        raw_date = item.get("air_date")
        if (
            not isinstance(number, int)
            or isinstance(number, bool)
            or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(raw_date or ""))
        ):
            continue
        parsed = datetime.fromisoformat(str(raw_date)).date()
        if parsed <= cutoff:
            rows.append((number, parsed))
    rows.sort()
    if not rows or [number for number, _ in rows] != list(range(1, len(rows) + 1)):
        return []
    segments: list[list[int]] = [[]]
    for index, (number, air_date) in enumerate(rows):
        if index and (air_date - rows[index - 1][1]).days >= minimum_gap_days:
            segments.append([])
        segments[-1].append(number)
    if len(segments) < 2:
        return []
    base = season_groups.get(official_season)
    if base is None:
        return []
    source_seasons = sorted(set(season_groups) - {official_season})
    latest_source_season = max(season_groups)
    if source_seasons != list(range(official_season + 1, latest_source_season + 1)):
        return []
    # Validate the whole release layout before mutating any group.  This keeps
    # the fallback atomic and deliberately rejects mixed local+cumulative
    # backup numbering, which is handled by the ordinary edition packer.
    routes: dict[int, tuple[dict[int, int], list[int], bool]] = {}
    for source_season in source_seasons:
        if source_season > len(segments):
            return []
        segment = segments[source_season - 1]
        raw_numbers = {
            key.number
            for item in season_groups[source_season]
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        }
        local_range = set(range(1, len(segment) + 1))
        absolute_range = set(segment)
        has_complete_local = local_range.issubset(raw_numbers)
        local_with_cumulative_editions = (
            has_complete_local
            and raw_numbers.issubset(local_range | absolute_range)
        )
        absolute_complete = raw_numbers == absolute_range
        local_latest_prefix = (
            source_season == latest_source_season
            and raw_numbers
            and raw_numbers == set(range(1, max(raw_numbers) + 1))
            and max(raw_numbers) <= len(segment)
        )
        absolute_latest_prefix = (
            source_season == latest_source_season
            and raw_numbers
            and raw_numbers == set(segment[: len(raw_numbers)])
        )
        if not (
            local_with_cumulative_editions
            or absolute_complete
            or local_latest_prefix
            or absolute_latest_prefix
        ):
            return []
        if local_with_cumulative_editions:
            raw_to_local = {
                number: number for number in raw_numbers if number in local_range
            }
            raw_to_local.update({
                absolute: index
                for index, absolute in enumerate(segment, start=1)
                if absolute in raw_numbers and absolute not in local_range
            })
            routed_segment = segment
        elif absolute_complete or absolute_latest_prefix:
            raw_to_local = {
                absolute: index
                for index, absolute in enumerate(segment, start=1)
                if absolute in raw_numbers
            }
            routed_segment = segment[: len(raw_numbers)]
        else:
            raw_to_local = {number: number for number in raw_numbers}
            routed_segment = segment[: max(raw_numbers)]
        routes[source_season] = (
            {
                raw: segment[local - 1]
                for raw, local in raw_to_local.items()
            },
            routed_segment,
            len(routed_segment) == len(segment),
        )
    warnings: list[str] = []
    for source_season in source_seasons:
        segment = segments[source_season - 1]
        members = season_groups[source_season]
        route, routed_segment, is_complete = routes[source_season]
        for item in members:
            key = extract_episode_key(str(item.get("name", "")))
            if key is None or key.kind != "regular" or key.end_number:
                continue
            mapped = route.get(key.number)
            if mapped is not None:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = mapped
        base.extend(members)
        season_groups.pop(source_season)
        warnings.append(
            f"源第 {source_season} 季完整覆盖 TMDB 长季在官方播出日期半年级"
            f"播出断档后的 "
            + (
                f"完整 {len(segment)} 个已播集"
                if is_complete
                else f"当前连续 {len(routed_segment)}/{len(segment)} 集"
            )
            + f"；已映射为 E{routed_segment[0]:02d}–E{routed_segment[-1]:02d}，"
            "未包含未播集"
        )
    return warnings


def _usable_release_title_query(query: str) -> bool:
    """Reject bare episode ordinals before using a file query as work identity."""
    key = _normalize_match_title(query)
    if not key or key.isdigit():
        return False
    residual = re.sub(
        r"(?:^|[\s._+\-/\[\]()])(?:MAI|TUDO|YGM|VCB(?:-Studio)?|"
        r"(?:[A-Z0-9]{2,12}[-_.])?Raws?|"
        r"Ma10p|x26[45]|HEVC|AVC|AV1|FLAC|AAC|EAC3|AC3|"
        r"BDRip|WEBRip|WEB-?DL|Blu-?Ray|ASS|SSA|SRT|SUB|10bit|8bit|"
        r"2160p|1440p|1080p|720p)(?=$|[\s._+\-/\[\]()])",
        " ",
        unicodedata.normalize("NFKC", query),
        flags=re.I,
    )
    # A release directory may contain only language/subtitle advertising and
    # the release group (for example ``简日双语 喵萌奶茶屋``).  Those words are
    # useful for edition preference but are not work identity.  Strip only a
    # bounded metadata vocabulary here; a real title left beside it remains a
    # valid query and still has to pass normal TMDB confidence/ambiguity gates.
    residual = re.sub(
        r"(?:简日双语|简繁双语|繁简双语|简英双语|繁英双语|"
        r"简繁|繁简|简中|繁中|简体|繁体|中文|中字|日语|英语|双语|"
        r"内封|内嵌|外挂|硬字幕|软字幕|字幕|"
        r"[\u3400-\u9fff]{1,12}字幕组|喵萌奶茶屋|Nekomoe[ ._-]*kissaten)",
        " ",
        residual,
        flags=re.I,
    )
    residual = re.sub(r"[\W\d_]+", "", residual, flags=re.UNICODE)
    if not residual:
        return False
    return len(key) >= 4 or bool(re.search(r"[\u3400-\u9fff\u3040-\u30ff]", query))


def _child_work_query_variants(
    release_queries: Sequence[str],
    *,
    parent_titles: Sequence[str],
    parent_aliases: Sequence[str],
) -> list[str]:
    """Build specific child-work queries across the parent's title scripts.

    Release names may romanize only the franchise token while TMDB exposes a
    child under the native-script parent title (``Gintama The Semi-Final`` vs
    ``銀魂 THE SEMI-FINAL``).  A parent alias already proven by the selected
    TMDB record may be replaced with that record's canonical title, but the
    child-specific suffix must remain.  Bare parent aliases and codec payloads
    never become child identities.
    """
    output: list[str] = []
    aliases = sorted(
        {str(value).strip() for value in parent_aliases if str(value).strip()},
        key=len,
        reverse=True,
    )
    canonical = list(dict.fromkeys(
        str(value).strip() for value in parent_titles if str(value).strip()
    ))
    for raw_query in release_queries:
        query = str(raw_query).strip()
        if _usable_release_title_query(query):
            output.append(query)
        for alias in aliases:
            match = re.match(
                rf"^{re.escape(alias)}(?=$|[\W_])",
                query,
                flags=re.IGNORECASE,
            )
            if match is None:
                continue
            suffix = query[match.end():].strip(" \t~～:：_./-[]()（）")
            if len(_normalize_match_title(suffix)) < 4:
                continue
            for title in canonical:
                rewritten = f"{title} {suffix}".strip()
                if _usable_release_title_query(rewritten):
                    output.append(rewritten)
            break
    return list(dict.fromkeys(output))


def _partition_movie_groups_with_video(
    movie_groups: Mapping[int, Sequence[Mapping[str, Any]]],
) -> tuple[dict[int, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Keep subtitle-only movie matches visible without aborting a TV batch."""
    valid: dict[int, list[dict[str, Any]]] = {}
    orphan_files: list[dict[str, Any]] = []
    for tmdb_id, members in movie_groups.items():
        copied = [dict(item) for item in members]
        if any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in copied
        ):
            valid[int(tmdb_id)] = copied
        else:
            orphan_files.extend(copied)
    return valid, orphan_files


def _detach_numbered_subgroups_from_mixed_movie_groups(
    movie_groups: dict[int, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Return numbered sibling works that a movie folder must not absorb.

    A release folder named for one movie can also contain a separately
    catalogued two-part TV special (for example ``The Final`` beside
    ``The Semi-Final [01]/[02]``).  If the same provisional movie identity
    contains multiple distinct release-title groups, detach only a complete
    contiguous 01..N subgroup with at least two videos.  The normal
    independent special-work matcher then has to prove its own TMDB identity;
    if it cannot, the files remain in place.  This structural gate prevents
    the movie quality pass from deleting the smaller sibling work as a
    supposed duplicate without guessing where it belongs.
    """
    detached: list[dict[str, Any]] = []
    for tmdb_id, members in list(movie_groups.items()):
        video_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in members:
            if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
                continue
            queries = _movie_queries_from_item(item)
            if not queries:
                continue
            video_groups[_normalize_match_title(queries[0])].append(item)
        if len(video_groups) < 2:
            continue
        detach_paths: set[str] = set()
        detach_stems: set[str] = set()
        for videos in video_groups.values():
            numbers = sorted({
                key.number
                for item in videos
                if (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
            })
            if (
                len(videos) < 2
                or len(numbers) != len(videos)
                or numbers != list(range(1, len(numbers) + 1))
            ):
                continue
            for item in videos:
                detach_paths.add(str(item.get("full_path", "")))
                detach_stems.add(_batch_subtitle_release_stem(str(item.get("name", ""))))
        if not detach_paths:
            continue
        retained: list[dict[str, Any]] = []
        for item in members:
            path = str(item.get("full_path", ""))
            suffix = Path(str(item.get("name", ""))).suffix.lower()
            follows_detached_video = (
                suffix in SUBTITLE_EXTS
                and _batch_subtitle_release_stem(str(item.get("name", "")))
                in detach_stems
            )
            if path in detach_paths or follows_detached_video:
                detached.append(item)
            else:
                retained.append(item)
        movie_groups[tmdb_id] = retained
    return detached


def _proven_missing_root_season_files(
    source_root: str,
    unknown_media: Sequence[Mapping[str, Any]],
    season_groups: Mapping[int, Sequence[Mapping[str, Any]]],
    official_season_counts: Mapping[int, int],
) -> tuple[int | None, list[dict[str, Any]], list[dict[str, Any]]]:
    """Assign a complete root-level run only to the sole missing official season."""
    missing = sorted(set(official_season_counts) - set(season_groups))
    copied = [dict(item) for item in unknown_media]
    if len(missing) != 1:
        return None, [], copied
    season_number = missing[0]
    expected_count = int(official_season_counts[season_number])
    root_videos: list[dict[str, Any]] = []
    for item in copied:
        path = str(item.get("full_path", ""))
        parent, _ = split_remote(path)
        key = extract_episode_key(str(item.get("name", "")))
        if (
            parent == source_root.rstrip("/")
            and Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and not _has_special_context(item)
            and not _has_movie_context(item)
            and key is not None
            and key.kind == "regular"
            and not key.end_number
        ):
            root_videos.append(item)
    video_numbers = {
        extract_episode_key(str(item.get("name", ""))).number
        for item in root_videos
    }
    if video_numbers != set(range(1, expected_count + 1)):
        return None, [], copied
    attached: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    for item in copied:
        path = str(item.get("full_path", ""))
        relative = path[len(source_root.rstrip("/")) :].lstrip("/")
        parts = relative.split("/")
        key = extract_episode_key(str(item.get("name", "")))
        root_or_backup = len(parts) == 1 or parts[0] in {
            "备份字幕", "字幕", "Subtitles",
        }
        if (
            root_or_backup
            and key is not None
            and key.kind == "regular"
            and not key.end_number
            and 1 <= key.number <= expected_count
            and not _has_special_context(item)
            and not _has_movie_context(item)
        ):
            attached.append(item)
        else:
            remaining.append(item)
    return season_number, attached, remaining


def _season_parent_identity_queries(
    items: Sequence[Mapping[str, Any]],
    *,
    source_root: str,
    season_number: int,
    show: Mapping[str, Any],
) -> list[str]:
    """Use the season-bearing directory, never an individual episode, as work identity."""
    queries: list[str] = []
    for item in items[:3]:
        full_path = str(item.get("full_path", ""))
        relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
        for segment in reversed(relative.split("/")[:-1]):
            explicit = _season_from_source("/" + segment)
            variant = _season_from_series_variant(segment, show)
            if explicit == season_number or variant == season_number:
                query = _query_from_source("/" + segment)
                generic_season_label = bool(re.fullmatch(
                    r"(?:第\s*[一二三四五六七八九十\d]{1,3}\s*季|"
                    r"s(?:eason)?\s*0*\d{1,3})",
                    unicodedata.normalize("NFKC", query).strip(),
                    flags=re.I,
                ))
                if generic_season_label:
                    queries.extend(
                        candidate
                        for candidate in _movie_queries_from_item(item)
                        if _usable_release_title_query(candidate)
                    )
                elif _usable_release_title_query(query):
                    queries.append(query)
                break
    return list(dict.fromkeys(queries))


def _proven_root_first_broadcast_block_files(
    source_root: str,
    unknown_media: Sequence[Mapping[str, Any]],
    block_count: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return a complete root-level alternate of the first proven broadcast block."""
    copied = [dict(item) for item in unknown_media]
    root_videos = [
        item for item in copied
        if split_remote(str(item.get("full_path", "")))[0] == source_root.rstrip("/")
        and Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and not _has_special_context(item)
        and not _has_movie_context(item)
        and (key := extract_episode_key(str(item.get("name", "")))) is not None
        and key.kind == "regular"
        and not key.end_number
    ]
    numbers = {
        extract_episode_key(str(item.get("name", ""))).number
        for item in root_videos
    }
    if numbers != set(range(1, block_count + 1)):
        return [], copied
    attached: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    for item in copied:
        relative = str(item.get("full_path", ""))[
            len(source_root.rstrip("/")):
        ].lstrip("/")
        parts = relative.split("/")
        key = extract_episode_key(str(item.get("name", "")))
        if (
            (len(parts) == 1 or parts[0] in {"备份字幕", "字幕", "Subtitles"})
            and key is not None
            and key.kind == "regular"
            and not key.end_number
            and 1 <= key.number <= block_count
            and not _has_special_context(item)
            and not _has_movie_context(item)
        ):
            attached.append(item)
        else:
            remaining.append(item)
    return attached, remaining


def _probe_remote_duration_minutes(
    alist: AListClient,
    source_path: str,
) -> float | None:
    """Read only container metadata for an otherwise ambiguous remote video.

    The signed URL has already passed ``AListClient``'s SSRF validation.  The
    probe is deliberately optional: installations without ffprobe retain the
    existing review row instead of weakening identity checks.
    """
    ffprobe = shutil.which("ffprobe")
    file_link = getattr(alist, "file_link", None)
    if ffprobe is None or not callable(file_link):
        return None
    try:
        raw_url, headers = file_link(source_path, refresh=True)
    except (ApiError, OSError, ValueError):
        return None
    command = [ffprobe, "-v", "error"]
    safe_headers: list[str] = []
    for raw_name, raw_value in headers.items():
        name = str(raw_name).strip()
        value = str(raw_value).strip()
        if (
            not re.fullmatch(r"[A-Za-z0-9-]+", name)
            or "\r" in value
            or "\n" in value
        ):
            return None
        safe_headers.append(f"{name}: {value}\r\n")
    if safe_headers:
        command.extend(["-headers", "".join(safe_headers)])
    command.extend([
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        raw_url,
    ])
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=25,
        )
        if completed.returncode != 0:
            return None
        seconds = float(completed.stdout.strip().splitlines()[0])
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError):
        return None
    if seconds <= 0 or seconds > 8 * 60 * 60:
        return None
    return seconds / 60.0


def _runtime_matched_related_animation_movie(
    tmdb_client: TMDBClient,
    show: Mapping[str, Any],
    duration_minutes: float,
    official_special_runtimes: Mapping[int, int],
) -> int | None:
    """Return one related animated movie only from unique runtime evidence."""
    tolerance = max(1.5, min(4.0, duration_minutes * 0.08))
    if any(
        abs(float(runtime) - duration_minutes) <= tolerance
        for runtime in official_special_runtimes.values()
    ):
        return None
    show_titles = [
        str(show.get(field) or "").strip()
        for field in ("name", "original_name")
        if str(show.get(field) or "").strip()
    ]
    show_keys = {_normalize_match_title(title) for title in show_titles}
    show_keys.discard("")
    if not show_keys:
        return None
    matches: set[int] = set()
    seen_results: set[int] = set()
    for query in show_titles:
        try:
            payload = tmdb_client.get("/search/movie", query=query)
        except ApiError:
            continue
        for result in payload.get("results") or []:
            if (
                not isinstance(result, Mapping)
                or isinstance(result.get("id"), bool)
                or not isinstance(result.get("id"), int)
            ):
                continue
            movie_id = int(result["id"])
            if movie_id in seen_results:
                continue
            seen_results.add(movie_id)
            try:
                movie = tmdb_client.get(f"/movie/{movie_id}")
            except ApiError:
                continue
            runtime = movie.get("runtime")
            if (
                isinstance(runtime, bool)
                or not isinstance(runtime, int)
                or runtime <= 0
                or abs(float(runtime) - duration_minutes) > tolerance
            ):
                continue
            genre_ids = {
                int(genre["id"])
                for genre in (movie.get("genres") or [])
                if isinstance(genre, Mapping)
                and isinstance(genre.get("id"), int)
                and not isinstance(genre.get("id"), bool)
            }
            if 16 not in genre_ids:
                continue
            candidate_titles = [
                *_search_item_titles(movie, "movie"),
                *_alternative_tmdb_titles(tmdb_client, "movie", movie_id),
            ]
            candidate_keys = {
                _normalize_match_title(title) for title in candidate_titles
            }
            if not any(
                len(show_key) >= 4
                and (
                    show_key in candidate_key
                    or candidate_key in show_key
                )
                for show_key in show_keys
                for candidate_key in candidate_keys
                if candidate_key
            ):
                continue
            matches.add(movie_id)
    return next(iter(matches)) if len(matches) == 1 else None


def _extract_runtime_proven_overflow_movies(
    alist: AListClient,
    tmdb_client: TMDBClient,
    show: Mapping[str, Any],
    season_groups: Mapping[int, list[dict[str, Any]]],
    official_season_counts: Mapping[int, int],
    official_special_runtimes: Mapping[int, int],
    unknown_media: list[dict[str, Any]] | None = None,
) -> tuple[dict[int, list[dict[str, Any]]], list[str]]:
    """Remove uniquely identified standalone movies from TV season groups."""
    resolved: dict[int, list[dict[str, Any]]] = defaultdict(list)
    warnings: list[str] = []
    probe_candidates: list[
        tuple[int, list[dict[str, Any]], int, dict[int, list[dict[str, Any]]]]
    ] = []
    for season_number, members in season_groups.items():
        official_count = official_season_counts.get(season_number)
        if not official_count:
            continue
        overflow_videos: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in members:
            key = extract_episode_key(str(item.get("name", "")))
            if (
                Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and key is not None
                and key.kind == "regular"
                and not key.end_number
                and official_count < key.number <= official_count + 3
            ):
                overflow_videos[key.number].append(item)
        # Runtime probing is an expensive last-resort identity check.  Limit
        # it to the common disc layout where a complete season has exactly one
        # trailing file (E{N+1}).  Multiple overflow ordinals are a release
        # run/cumulative-numbering problem and must stay in the normal strict
        # mappers without opening several remote media streams.
        if sorted(overflow_videos) != [official_count + 1]:
            continue
        probe_candidates.append(
            (season_number, members, official_count, dict(overflow_videos))
        )
    # Large franchise roots may contain several seasons each followed by a
    # disc extra.  That is not the isolated one-file ambiguity this expensive
    # fallback is intended to solve, so do not probe any of them here.
    if len(probe_candidates) != 1:
        return {}, []
    for season_number, members, official_count, overflow_videos in probe_candidates:
        moved_paths: set[str] = set()
        for source_number, videos in sorted(overflow_videos.items()):
            durations = [
                duration
                for video in videos
                if (duration := _probe_remote_duration_minutes(
                    alist, str(video.get("full_path", ""))
                )) is not None
            ]
            if not durations:
                continue
            movie_ids = {
                movie_id
                for duration in durations
                if (movie_id := _runtime_matched_related_animation_movie(
                    tmdb_client,
                    show,
                    duration,
                    official_special_runtimes,
                )) is not None
            }
            if len(movie_ids) != 1:
                continue
            movie_id = next(iter(movie_ids))
            video_parents = {
                split_remote(str(video.get("full_path", "")))[0]
                for video in videos
            }
            video_release_keys = {
                _normalize_match_title(query)
                for video in videos
                for query in _movie_queries_from_item(video)
                if _usable_release_title_query(query)
            }
            companions = []
            for item in members:
                key = extract_episode_key(str(item.get("name", "")))
                item_release_keys = {
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(item)
                    if _usable_release_title_query(query)
                }
                if (
                    key is not None
                    and key.kind == "regular"
                    and key.number == source_number
                    and (
                        split_remote(str(item.get("full_path", "")))[0]
                        in video_parents
                        or bool(video_release_keys & item_release_keys)
                    )
                ):
                    companions.append(item)
            resolved[movie_id].extend(companions)
            moved_paths.update(str(item.get("full_path", "")) for item in companions)
            warnings.append(
                f"E{source_number:02d} 超出第 {season_number} 季官方边界；"
                f"远程媒体时长与唯一同名动画电影 TMDB/{movie_id} 一致，"
                "且不匹配任何官方 Season 00 时长，已作为独立电影"
            )
        if moved_paths:
            members[:] = [
                item for item in members
                if str(item.get("full_path", "")) not in moved_paths
            ]
    if unknown_media is not None and resolved:
        attached_unknown_paths: set[str] = set()
        for movie_id, members in resolved.items():
            source_numbers = {
                key.number
                for member in members
                if (key := extract_episode_key(str(member.get("name", "")))) is not None
                and key.kind == "regular"
            }
            release_keys = {
                _normalize_match_title(query)
                for member in members
                for query in _movie_queries_from_item(member)
                if _usable_release_title_query(query)
            }
            for item in unknown_media:
                key = extract_episode_key(str(item.get("name", "")))
                item_keys = {
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(item)
                    if _usable_release_title_query(query)
                }
                if (
                    Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
                    and key is not None
                    and key.kind == "regular"
                    and key.number in source_numbers
                    and bool(release_keys & item_keys)
                ):
                    resolved[movie_id].append(item)
                    attached_unknown_paths.add(str(item.get("full_path", "")))
        if attached_unknown_paths:
            unknown_media[:] = [
                item for item in unknown_media
                if str(item.get("full_path", "")) not in attached_unknown_paths
            ]
    return dict(resolved), warnings


def _attach_unique_numbered_backup_subtitles(
    unknown_media: list[dict[str, Any]],
    season_groups: Mapping[int, list[dict[str, Any]]],
    official_season_counts: Mapping[int, int],
) -> tuple[list[dict[str, Any]], int]:
    """Attach a partial backup subtitle only to one exact release/episode video."""
    attached = 0
    remaining: list[dict[str, Any]] = []
    for item in unknown_media:
        key = extract_episode_key(str(item.get("name", "")))
        if (
            Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS
            or key is None
            or key.kind != "regular"
            or key.end_number
        ):
            remaining.append(item)
            continue
        subtitle_keys = {
            _normalize_match_title(query)
            for query in _movie_queries_from_item(item)
            if _usable_release_title_query(query)
        }
        candidates: list[int] = []
        for season_number, members in season_groups.items():
            if not 1 <= key.number <= int(official_season_counts.get(season_number, 0)):
                continue
            matching_video = False
            for video in members:
                video_key = extract_episode_key(str(video.get("name", "")))
                if (
                    Path(str(video.get("name", ""))).suffix.lower() not in VIDEO_EXTS
                    or video_key is None
                    or video_key.kind != "regular"
                    or video_key.number != key.number
                ):
                    continue
                video_keys = {
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(video)
                    if _usable_release_title_query(query)
                }
                if subtitle_keys and subtitle_keys & video_keys:
                    matching_video = True
                    break
            if matching_video:
                candidates.append(season_number)
        if len(candidates) != 1:
            remaining.append(item)
            continue
        season_groups[candidates[0]].append(item)
        attached += 1
    return remaining, attached


def _unique_backup_subtitle_release_owners(
    top_level_groups: Mapping[str, Sequence[Mapping[str, Any]]],
    unknown_media: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    """Route a flattened backup subtitle to one top-level release identity.

    Franchise sequels often share a short alias (``Clannad``) while only the
    sequel carries the longer identity (``Clannad After Story``).  The former
    greedy per-directory pass attached a sequel subtitle to whichever folder
    sorted first.  Compare every sibling release before staging: the longest
    exact normalized title key wins, and equal best scores remain unresolved.
    """
    def identity_key(value: str) -> str:
        """Normalize a release title without erasing its disambiguating year.

        ``_normalize_match_title`` intentionally removes years for TMDB title
        matching.  That is unsafe for sibling release ownership: ``Clannad
        2007`` and ``Clannad After Story 2008`` both expose the short alias
        ``Clannad``.  Here an explicit release year is identity evidence, just
        like the sequel subtitle, so preserve it while still folding Unicode,
        punctuation and case.
        """
        normalized = unicodedata.normalize("NFKC", str(value)).casefold()
        return "".join(
            char for char in normalized
            if char.isalnum() or "\u3400" <= char <= "\u9fff"
        )

    release_keys: dict[str, set[str]] = {}
    for segment, members in top_level_groups.items():
        keys = {
            identity_key(query)
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for query in _movie_queries_from_item(item)
            if _usable_release_title_query(query)
        }
        # The top-level folder is itself release-local evidence and often
        # carries the year even when a release parser also emits a short alias.
        keys.add(identity_key(segment))
        keys.discard("")
        if keys:
            release_keys[segment] = keys

    owners: dict[str, str] = {}
    for item in unknown_media:
        if Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS:
            continue
        subtitle_keys = {
            identity_key(query)
            for query in _movie_queries_from_item(item)
            if _usable_release_title_query(query)
        }
        subtitle_keys.discard("")
        scores = {
            segment: max(
                (
                    min(len(subtitle_key), len(release_key))
                    for subtitle_key in subtitle_keys
                    for release_key in keys
                    if (
                        subtitle_key == release_key
                        or subtitle_key in release_key
                        or release_key in subtitle_key
                    )
                ),
                default=0,
            )
            for segment, keys in release_keys.items()
        }
        best_score = max(scores.values(), default=0)
        best = [segment for segment, score in scores.items() if score == best_score > 0]
        if len(best) == 1:
            owners[str(item.get("full_path", ""))] = best[0]
            continue
        # Two sibling folders can be resolution variants of the same child
        # work.  In that case the subtitle's full, specific release identity
        # is present verbatim in every tied group.  Attach it once to the
        # deterministic first group; the caller later merges both groups by
        # the same TMDB child id.  A short shared franchise alias does not pass
        # this gate (for example bare ``Clannad`` beside ``After Story``).
        shared_exact = {
            subtitle_key
            for subtitle_key in subtitle_keys
            if len(subtitle_key) >= 8
            and all(subtitle_key in release_keys[segment] for segment in best)
            and not any(
                subtitle_key != release_key and subtitle_key in release_key
                for segment in best
                for release_key in release_keys[segment]
            )
        }
        if best and shared_exact:
            owners[str(item.get("full_path", ""))] = sorted(best)[0]
    return owners


def _proven_absolute_season_group_endpoint(
    source_season: int,
    source_files: Sequence[Mapping[str, Any]],
    official_seasons: Sequence[Mapping[str, Any]],
) -> int | None:
    """Return an official cumulative endpoint proved by a source video run."""
    if source_season != 1:
        return None
    numbers: set[int] = set()
    for item in source_files:
        if Path(str(item.get("name") or "")).suffix.lower() not in VIDEO_EXTS:
            continue
        key = extract_episode_key(str(item.get("name") or ""))
        if key is None or key.kind != "regular":
            continue
        numbers.update(range(key.number, (key.end_number or key.number) + 1))
    if not numbers or numbers != set(range(1, max(numbers) + 1)):
        return None
    counts = [
        (int(item["season_number"]), int(item["episode_count"]))
        for item in official_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and int(item["season_number"]) > 0
        and isinstance(item.get("episode_count"), int)
        and not isinstance(item.get("episode_count"), bool)
        and int(item["episode_count"]) > 0
    ]
    counts.sort()
    cumulative = 0
    first_count = counts[0][1] if counts else 0
    for season_number, count in counts:
        cumulative += count
        if season_number > 1 and max(numbers) == cumulative and cumulative > first_count:
            return cumulative
    return None


def _remap_complete_reset_absolute_season_groups(
    season_groups: Mapping[int, list[dict[str, Any]]],
    official_seasons: Sequence[Mapping[str, Any]],
) -> tuple[dict[int, list[dict[str, Any]]], str | None]:
    """Split complete release-level absolute blocks on official boundaries.

    Long-running shows are sometimes packaged as a few release "seasons"
    whose counters each restart at 01, while TMDB has many broadcast seasons.
    Accept this only when every source group is a complete contiguous 01..N
    run and the ordered group endpoints partition the *entire* ordered TMDB
    season-count vector exactly.
    """
    if len(season_groups) < 2:
        return {number: list(items) for number, items in season_groups.items()}, None
    official = sorted(
        (
            int(item["season_number"]),
            int(item["episode_count"]),
        )
        for item in official_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and int(item["season_number"]) > 0
        and isinstance(item.get("episode_count"), int)
        and not isinstance(item.get("episode_count"), bool)
        and int(item["episode_count"]) > 0
    )
    if len(official) <= len(season_groups):
        return {number: list(items) for number, items in season_groups.items()}, None

    def source_key(item: Mapping[str, Any]) -> EpisodeKey | None:
        bracket_range = re.search(
            r"\[\s*0*(\d{1,4})\s*[-–—~～至到]\s*0*(\d{1,4})\s*\]",
            unicodedata.normalize("NFKC", str(item.get("name") or "")),
        )
        if bracket_range is not None:
            start, end = map(int, bracket_range.groups())
            if 0 < start <= end:
                return EpisodeKey("regular", start, end)
        key = extract_episode_key(str(item.get("name") or ""))
        if key is not None:
            return key
        try:
            parsed = parse_ep_files(
                [item],
                prefer_simplified=False,
                defer_unnumbered_specials=True,
            )
        except PlanError:
            return None
        return next(iter(parsed), None) if len(parsed) == 1 else None

    source_blocks: list[tuple[int, int, list[dict[str, Any]]]] = []
    for source_season, items in sorted(season_groups.items()):
        numbers: set[int] = set()
        for item in items:
            if Path(str(item.get("name") or "")).suffix.lower() not in VIDEO_EXTS:
                continue
            key = source_key(item)
            if key is None or key.kind != "regular":
                continue
            numbers.update(range(key.number, (key.end_number or key.number) + 1))
        if not numbers or numbers != set(range(1, max(numbers) + 1)):
            return {number: list(values) for number, values in season_groups.items()}, None
        source_blocks.append((source_season, max(numbers), items))

    partitions: list[list[tuple[int, int]]] = []
    official_index = 0
    for _source_season, endpoint, _items in source_blocks:
        block: list[tuple[int, int]] = []
        total = 0
        while official_index < len(official) and total < endpoint:
            season_number, count = official[official_index]
            block.append((season_number, count))
            total += count
            official_index += 1
        if total != endpoint:
            return {number: list(values) for number, values in season_groups.items()}, None
        partitions.append(block)
    if official_index != len(official) or not any(len(block) > 1 for block in partitions):
        return {number: list(values) for number, values in season_groups.items()}, None

    remapped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    summaries: list[str] = []
    for (source_season, endpoint, items), block in zip(source_blocks, partitions):
        route: dict[int, tuple[int, int]] = {}
        offset = 0
        for target_season, count in block:
            for local in range(1, count + 1):
                route[offset + local] = (target_season, local)
            offset += count
        staged: list[tuple[int, dict[str, Any]]] = []
        for original in items:
            item = dict(original)
            key = source_key(item)
            if key is None or key.kind != "regular":
                # Named/fractional extras inherit only the uniquely proven
                # official block container. Their own strict special mapper
                # still decides Season 00 identity later.
                logical_number = key.number if key is not None else 1
                container = route.get(logical_number, (block[0][0], 1))[0]
                staged.append((container, item))
                continue
            start = route.get(key.number)
            end = route.get(key.end_number or key.number)
            if start is None or end is None or start[0] != end[0]:
                return {number: list(values) for number, values in season_groups.items()}, None
            item["_episode_kind_override"] = "regular"
            item["_episode_key_override"] = start[1]
            if end[1] != start[1]:
                item["_episode_end_override"] = end[1]
            staged.append((start[0], item))
        for target_season, item in staged:
            remapped[target_season].append(item)
        summaries.append(
            f"源第 {source_season} 组 01–{endpoint} → "
            f"S{block[0][0]:02d}–S{block[-1][0]:02d}"
        )
    return dict(remapped), (
        "源发行将长篇剧集分为重置编号的跨季 absolute 块；"
        "已仅在所有视频块完整覆盖 01–N，且与 TMDB 全部季集数"
        "边界唯一分割时自动映射：" + "；".join(summaries)
    )


def build_tv_plan_smart(*, auto_episode_mode: bool, **kwargs: Any) -> Plan:
    """Split explicit multi-season roots and retry proven absolute-number releases."""
    proven_member_season = bool(kwargs.pop("_proven_member_season", False))
    smart_kwargs = dict(kwargs)
    # Explicit episode maps intentionally bypass smart season inference, but
    # the common post-plan resource-gap audit still consumes this collection.
    positive_seasons: list[Mapping[str, Any]] = []
    if auto_episode_mode and kwargs.get("episode_map_path") is None:
        smart_kwargs["auto_special_title_match"] = True
        smart_kwargs["auto_align_subtitles"] = True
        provided_files = kwargs.get("source_files")
        files = [dict(item) for item in provided_files] if provided_files is not None else [
            dict(item)
            for item in kwargs["alist"].walk(
                kwargs["src_path"],
                ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
            )
        ]
        # Keep the smart wrapper consistent with ``build_tv_plan``: a work
        # whose only playable media lives under an Extras/SP container must
        # get the evidence-gated bonus rescan before we freeze ``source_files``.
        # Otherwise the pre-scan masks the lower-level fallback and falsely
        # reports a subtitle-only directory.
        if not any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in _filter_media(files)
        ):
            files = [
                dict(item)
                for item in kwargs["alist"].walk(
                    kwargs["src_path"],
                    ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                    include_bonus=True,
                )
            ]
        smart_kwargs["source_files"] = files
        season_groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
        special_files: list[dict[str, Any]] = []
        movie_groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
        unknown_media: list[dict[str, Any]] = []
        child_tv_plans: list[Plan] = []
        retained_future_media: list[dict[str, Any]] = []
        retained_unpublished_season_media: list[tuple[dict[str, Any], int]] = []
        out_of_range_season_by_path: dict[str, int] = {}
        packed_single_season = False
        long_season_pack_diagnostic: str | None = None
        edition_group_warnings: dict[int, list[str]] = defaultdict(list)
        source_root = normalize_remote_path(str(kwargs["src_path"]))
        show_for_season_names: Mapping[str, Any] = {}
        official_special_titles: dict[int, str] = {}
        official_special_runtimes: dict[int, int] = {}
        official_special_air_dates: dict[int, str] = {}
        official_special_title_variants: dict[int, list[str]] = defaultdict(list)
        official_positive_season_numbers: set[int] = set()
        official_long_season_block_counts: list[int] = []
        official_long_season_episodes: list[Mapping[str, Any]] = []
        special_release_warnings: list[str] = []
        independent_e00_warning: str | None = None
        tmdb_id = kwargs.get("tmdb_id")
        tmdb_get = getattr(kwargs.get("tmdb_client"), "get", None)
        if (
            isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and callable(tmdb_get)
        ):
            show_payload = tmdb_get(f"/tv/{tmdb_id}")
            if isinstance(show_payload, Mapping):
                show_for_season_names = show_payload
                positive_seasons = [
                    item
                    for item in (show_payload.get("seasons") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("season_number"), int)
                    and not isinstance(item.get("season_number"), bool)
                    and int(item["season_number"]) > 0
                    and isinstance(item.get("episode_count"), int)
                    and not isinstance(item.get("episode_count"), bool)
                ]
                official_positive_season_numbers = {
                    int(item["season_number"]) for item in positive_seasons
                }
                if len(positive_seasons) == 1:
                    long_season_number = int(positive_seasons[0]["season_number"])
                    try:
                        long_season_payload = tmdb_get(
                            f"/tv/{tmdb_id}/season/{long_season_number}"
                        )
                    except ApiError:
                        long_season_payload = {}
                    if isinstance(long_season_payload, Mapping):
                        official_long_season_episodes = [
                            item
                            for item in (long_season_payload.get("episodes") or [])
                            if isinstance(item, Mapping)
                        ]
                        official_long_season_block_counts = (
                            _tmdb_long_season_block_counts(
                                official_long_season_episodes
                            )
                        )
            try:
                special_payload = tmdb_get(f"/tv/{tmdb_id}/season/0")
            except ApiError:
                special_payload = {}
            if isinstance(special_payload, Mapping):
                official_special_titles = {
                    int(item["episode_number"]): str(item["name"])
                    for item in (special_payload.get("episodes") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("episode_number"), int)
                    and not isinstance(item.get("episode_number"), bool)
                    and isinstance(item.get("name"), str)
                    and item.get("name")
                }
                official_special_runtimes = {
                    int(item["episode_number"]): int(item["runtime"])
                    for item in (special_payload.get("episodes") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("episode_number"), int)
                    and not isinstance(item.get("episode_number"), bool)
                    and isinstance(item.get("runtime"), int)
                    and not isinstance(item.get("runtime"), bool)
                    and int(item["runtime"]) > 0
                }
                official_special_air_dates = {
                    int(item["episode_number"]): str(item["air_date"])
                    for item in (special_payload.get("episodes") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("episode_number"), int)
                    and not isinstance(item.get("episode_number"), bool)
                    and re.fullmatch(
                        r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or "")
                    )
                }
                for number, title in official_special_titles.items():
                    official_special_title_variants[number].append(title)
                primary_language = str(
                    getattr(kwargs.get("tmdb_client"), "language", "") or ""
                )
                for language in ("zh-CN", "zh-TW", "ja-JP", "en-US"):
                    if language == primary_language:
                        continue
                    try:
                        translated_specials = tmdb_get(
                            f"/tv/{tmdb_id}/season/0",
                            language=language,
                        )
                    except ApiError:
                        continue
                    if not isinstance(translated_specials, Mapping):
                        continue
                    for translated in translated_specials.get("episodes") or []:
                        if (
                            not isinstance(translated, Mapping)
                            or isinstance(translated.get("episode_number"), bool)
                            or not isinstance(translated.get("episode_number"), int)
                        ):
                            continue
                        title = str(translated.get("name") or "").strip()
                        number = int(translated["episode_number"])
                        if (
                            title
                            and title
                            not in official_special_title_variants[number]
                        ):
                            official_special_title_variants[number].append(title)
        beta_alternate_count = _map_explicit_beta_alternate(
            files,
            official_special_title_variants,
        )
        if beta_alternate_count:
            special_release_warnings.append(
                f"{beta_alternate_count} 个明确 23B/23β 版本已根据多语言官方"
                "β/Missing Link 特别篇标题映射到 Season 00"
            )
        minitodo_count = _map_minitodo_release_editions(
            files,
            official_special_title_variants,
        )
        if minitodo_count:
            special_release_warnings.append(
                f"{minitodo_count} 个 Mini Todoke 2D/3D/后日谈视频或字幕"
                "已根据多语言官方「罗密欧与朱丽叶」/后日谈标题"
                "映射到 Season 00；3D 作为同集版本保留"
            )
        split_special_count = _map_split_official_special_folder(
            files,
            official_special_title_variants,
        )
        if split_special_count:
            special_release_warnings.append(
                f"{split_special_count} 个分篇文件所在目录与唯一官方特别篇标题一致；"
                "TMDB 仅建一条时已保留同一 S00 集号并按连续 part 命名"
            )
        disc_extra_count = _map_disc_extras_by_official_release_runs(
            files,
            show=show_for_season_names,
            positive_seasons=positive_seasons,
            special_runtimes=official_special_runtimes,
            special_air_dates=official_special_air_dates,
            special_title_variants=official_special_title_variants,
        )
        if disc_extra_count:
            special_release_warnings.append(
                f"{disc_extra_count} 个特典小动画/OVA 已依官方短片时长、"
                "发行断档和源季序映射到全局 Season 00 编号"
            )
        # A suffix such as ``[13 OAV]`` describes an extra released after
        # episode 13, not necessarily OAV number 13.  Resolve this before the
        # smart planner splits regular seasons and special folders; after that
        # split the complete E01-E13 boundary evidence would be unavailable.
        # The helper mutates only the uniquely proven OAV and its exact-name
        # subtitle companions with an explicit Season 00 override.
        pre_split_groups: dict[EpisodeKey, list[dict[str, Any]]] = defaultdict(list)
        for item in _filter_media(files):
            key = extract_episode_key(str(item.get("name", "")))
            if key is not None:
                pre_split_groups[key].append(item)
        special_release_warnings.extend(
            _remap_suffix_oav_on_air_versions(
                pre_split_groups,
                {
                    int(item["season_number"]): int(item["episode_count"])
                    for item in positive_seasons
                },
            )
        )
        special_release_warnings.extend(
            _remap_postseason_oav_suffix(
                pre_split_groups,
                {
                    EpisodeKey("special", number): title
                    for number, title in official_special_titles.items()
                },
            )
        )
        # ``_filter_media`` intentionally returns defensive copies.  Carry
        # only the proven pre-split overrides back to the planner's private
        # file list; otherwise the evidence pass would disappear when the
        # smart planner later rebuilds its season/special groups.
        pre_split_overrides = {
            str(item.get("full_path", "")): item
            for members in pre_split_groups.values()
            for item in members
            if isinstance(item.get("_episode_key_override"), int)
        }
        for item in files:
            proven = pre_split_overrides.get(str(item.get("full_path", "")))
            if proven is None:
                continue
            for field in (
                "_episode_kind_override",
                "_episode_key_override",
                "_episode_end_override",
                "_edition_override",
            ):
                if field in proven:
                    item[field] = proven[field]
        official_season_counts = {
            int(item["season_number"]): int(item["episode_count"])
            for item in positive_seasons
        }
        packed_ova_seasons: dict[int, int] = {}
        ova_volume_videos: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for raw_item in _filter_media(files):
            if Path(str(raw_item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
                continue
            full_path = str(raw_item.get("full_path", ""))
            relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
            parent_segments = relative.split("/")[:-1]
            volume = None
            for segment in reversed(parent_segments):
                volume = _ova_volume_ordinal(segment)
                if volume is not None:
                    break
            if volume is not None:
                ova_volume_videos[volume].append(dict(raw_item))
        # An OVA release may call each physical volume a “Season” and pack
        # several short official episodes into one video. Accept that layout
        # only when the parent volume numbers cover the complete TMDB season
        # set, every volume contains exactly one video, and every corresponding
        # official season contains multiple episodes.
        if (
            ova_volume_videos
            and set(ova_volume_videos) == set(official_season_counts)
            and all(len(items) == 1 for items in ova_volume_videos.values())
            and all(count > 1 for count in official_season_counts.values())
        ):
            packed_ova_seasons = dict(official_season_counts)
        e00_media = [
            item
            for item in _filter_media(files)
            if (
                (key := extract_episode_key(str(item.get("name", ""))))
                is not None
                and key.kind == "regular"
                and key.number == 0
                and not key.end_number
            )
        ]
        e00_movie_match = (
            _e00_independent_movie_match(
                kwargs["tmdb_client"],
                e00_media,
                show_for_season_names,
            )
            if e00_media and show_for_season_names
            else None
        )
        if e00_movie_match is not None:
            independent_e00_warning = (
                "源文件 E00 的明确副标题已通过 TMDB 搜索，并由所有发行版本唯一"
                f"确认对应独立电影《{e00_movie_match.title}》"
                f"（{e00_movie_match.year}）；未把它猜作 Season 00 特别篇"
            )
        for raw_item in _filter_media(files):
            item = dict(raw_item)
            full_path = str(item["full_path"])
            relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
            segments = relative.split("/")
            inferred_season = None
            inferred_season_from_parent = False
            for segment in reversed(segments[:-1]):
                inferred_season = _season_from_source("/" + segment)
                if inferred_season is None:
                    inferred_season = _season_from_series_variant(
                        segment, show_for_season_names
                    )
                if inferred_season is not None:
                    inferred_season_from_parent = True
                    break
            if inferred_season is None:
                inferred_season = _season_from_source("/" + segments[-1])
            if inferred_season is None:
                inferred_season = _season_from_series_variant(
                    segments[-1], show_for_season_names
                )
            if inferred_season is None:
                match = re.search(r"S(?:eason)?\s*0*(\d{1,3})\s*E\s*\d+", segments[-1], re.I)
                if match:
                    inferred_season = int(match.group(1))
            explicit_release_pair = _explicit_release_season_episode(
                segments[-1], official_season_counts,
            )
            if inferred_season is None and explicit_release_pair is not None:
                inferred_season, explicit_episode = explicit_release_pair
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = explicit_episode
            movie_tmdb_id = _embedded_movie_tmdb_id(item)
            source_episode_key = extract_episode_key(str(item.get("name", "")))
            ova_volume = None
            for segment in reversed(segments[:-1]):
                ova_volume = _ova_volume_ordinal(segment)
                if ova_volume is not None:
                    break
            if ova_volume in packed_ova_seasons:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = 1
                item["_episode_end_override"] = packed_ova_seasons[ova_volume]
                season_groups[ova_volume].append(item)
                if not edition_group_warnings[ova_volume]:
                    edition_group_warnings[ova_volume].append(
                        f"父目录 OVA {ova_volume:02d} 与 TMDB 第 {ova_volume} 季"
                        f"唯一对应，且该卷只有一个视频；已按官方 "
                        f"{packed_ova_seasons[ova_volume]} 集结构保留为合并集"
                    )
                continue
            if (
                e00_movie_match is not None
                and source_episode_key is not None
                and source_episode_key.kind == "regular"
                and source_episode_key.number == 0
                and not source_episode_key.end_number
            ):
                movie_groups[e00_movie_match.tmdb_id].append(item)
                continue
            # New Edit is an alternate cut of Season 01, not evidence that the
            # enclosing release folder itself is an ordinary season folder.
            # Keep it out of direct season inference so the proven 2N-1 range
            # mapper below can preserve the multi-episode edition correctly.
            if entry_edition_tag(item) == "New Edit":
                unknown_media.append(item)
                continue
            # A surrounding series label can resemble the parent TV title and
            # make ``_season_from_series_variant`` infer Season 01.  Explicit
            # numbered live-action/movie-collection entries must reach the
            # official collection evidence pass before that TV inference.
            if _has_numbered_movie_collection_context(item):
                unknown_media.append(item)
                continue
            # An inner ``SPs``/OVA/mini-anime directory is more specific than
            # an outer folder such as “第二季”.  Let the special mapper handle
            # it before inheriting the parent TV season.
            if (
                source_episode_key is not None
                and source_episode_key.kind == "fractional"
                and inferred_season in official_positive_season_numbers
            ):
                # A decimal release such as Zoku Shou 10.5 belongs to the
                # explicitly inferred source season even when an enclosing
                # pack name also advertises ``SP+Extras``.  Keeping it in a
                # global special bucket would later attach it to the first
                # season and make the correct broadcast interval impossible
                # to evaluate.
                season_groups[int(inferred_season)].append(item)
            elif item.get("_episode_kind_override") == "special":
                special_files.append(item)
            elif (
                inferred_season is not None
                # A numbered top-level release folder such as
                # ``05 剧场版：雪下的誓言`` is a movie identity, not
                # Season 05 or episode 05.  Let the independently evidenced
                # movie matcher below consume it before any season-number
                # inheritance can turn the folder ordinal into an episode.
                and not _has_movie_context(item)
                and not _special_context_overrides_parent_season(item)
                and (
                    inferred_season_from_parent
                    or not _has_special_context(item)
                )
                and not _matches_named_special_release_context(
                    item,
                    official_special_title_variants,
                    series_titles=(
                        str(show_for_season_names.get("name") or ""),
                        str(show_for_season_names.get("original_name") or ""),
                    ),
                )
            ):
                season_groups[inferred_season].append(item)
            elif movie_tmdb_id is not None:
                movie_groups[movie_tmdb_id].append(item)
            elif (
                _has_movie_context(item)
                and Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
            ):
                # A generic ``简中.ass`` cannot establish a movie identity by
                # itself. Stage it for the proven same-directory/video-title
                # companion pass below; otherwise a subtitle-only movie group
                # can be created beside the correct video group.
                unknown_media.append(item)
            elif _has_movie_context(item) and Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS:
                movie_match = None
                last_movie_error: PlanError | None = None
                for movie_query in _movie_queries_from_item(item):
                    try:
                        movie_match, _ = auto_match_tmdb(
                            kwargs["tmdb_client"],
                            movie_query,
                            media_type="movie",
                            min_confidence=0.88,
                            prefer_animation=_media_context_from_source_and_target(
                                source_root,
                                str(kwargs["parent_path"]),
                            )[1],
                        )
                        if movie_match.status != "confirmed":
                            continue
                        break
                    except PlanError as exc:
                        last_movie_error = exc
                if movie_match is None:
                    release_years = [
                        int(value)
                        for value in re.findall(r"(?:19|20)\d{2}", full_path)
                    ]
                    if release_years and max(release_years) >= datetime.now().year:
                        retained_future_media.append(item)
                        continue
                    raise PlanError(
                        f"剧场版无法自动识别: {full_path}；{last_movie_error or '没有可用标题'}"
                    ) from last_movie_error
                movie_groups[movie_match.tmdb_id].append(item)
            elif _has_special_context(item) or _matches_named_special_release_context(
                item,
                official_special_title_variants,
                series_titles=(
                    str(show_for_season_names.get("name") or ""),
                    str(show_for_season_names.get("original_name") or ""),
                ),
            ):
                # A named unnumbered OVA can be an independently catalogued
                # movie (for example Prisma Phantasm).  First preserve any
                # official Season 00 override proven by the release-run mapper
                # above; otherwise accept a movie only from a confirmed exact
                # TMDB title match. Generic ``OVA.mkv`` cannot pass this path.
                if (
                    item.get("_episode_kind_override") != "special"
                    and Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                    and source_episode_key is None
                ):
                    named_ova_movie: AutoMatch | None = None
                    for movie_query in _movie_queries_from_item(item):
                        if not _usable_release_title_query(movie_query):
                            continue
                        try:
                            candidate, _ = auto_match_tmdb(
                                kwargs["tmdb_client"],
                                movie_query,
                                media_type="movie",
                                min_confidence=0.88,
                                prefer_animation=_media_context_from_source_and_target(
                                    source_root,
                                    str(kwargs["parent_path"]),
                                )[1],
                            )
                        except ScraperError:
                            continue
                        if (
                            candidate.status == "confirmed"
                            and candidate.media_type == "movie"
                            and _specific_movie_query_agrees_with_match(
                                movie_query, candidate
                            )
                        ):
                            named_ova_movie = candidate
                            break
                    if named_ova_movie is not None:
                        movie_groups[named_ova_movie.tmdb_id].append(item)
                        continue
                source_title_key = _normalize_match_title(_query_from_source(source_root))
                series_title_keys = {
                    _normalize_match_title(str(show_for_season_names.get(field) or ""))
                    for field in ("name", "original_name")
                }
                series_title_keys.discard("")
                relative_parents = segments[:-1]
                label_keys: set[str] = set()
                for label in relative_parents:
                    label_key = _normalize_match_title(label)
                    for series_key in {source_title_key, *series_title_keys}:
                        if series_key:
                            label_key = label_key.replace(series_key, "")
                    if len(label_key) >= 4:
                        label_keys.add(label_key)
                file_label_key = _normalize_match_title(
                    Path(str(item.get("name", ""))).stem
                )
                # A numbered named-special run commonly uses filenames such
                # as ``EX Season 01``/``EX Season 02``.  Strip only the final
                # source ordinal so both files can match the shared official
                # TMDB title prefix and then be assigned in source order.
                file_label_key = re.sub(r"\d+$", "", file_label_key)
                if len(file_label_key) >= 4:
                    label_keys.add(file_label_key)
                matching_specials = {
                    number
                    for number, official_title in official_special_titles.items()
                    if any(
                        label_key in _normalize_match_title(official_title)
                        or _normalize_match_title(official_title) in label_key
                        for label_key in label_keys
                    )
                }
                if not matching_specials:
                    # Release directories often wrap a named special arc in
                    # dates, group names and codec tags, so the whole directory
                    # label is no longer a substring of any one episode title.
                    # Recover only a sufficiently long official prefix shared
                    # by a consecutive TMDB special run.  For example, all
                    # three official ``柯里乌斯之梦 …`` titles share the same
                    # arc prefix even inside a noisy ``2023.11 … [WebRip]``
                    # release path.  A lone or non-consecutive title is not
                    # enough evidence for this fallback.
                    official_title_keys = {
                        number: _normalize_match_title(title)
                        for number, title in official_special_titles.items()
                    }
                    prefix_matches: set[tuple[int, ...]] = set()
                    for title_key in official_title_keys.values():
                        for length in range(5, len(title_key) + 1):
                            prefix = title_key[:length]
                            numbers = tuple(sorted(
                                number
                                for number, candidate in official_title_keys.items()
                                if candidate.startswith(prefix)
                            ))
                            if (
                                len(numbers) >= 2
                                and list(numbers)
                                == list(range(numbers[0], numbers[-1] + 1))
                                and any(prefix in label_key for label_key in label_keys)
                            ):
                                prefix_matches.add(numbers)
                    if len(prefix_matches) == 1:
                        matching_specials = set(next(iter(prefix_matches)))
                if len(matching_specials) == 1:
                    item["_episode_kind_override"] = "special"
                    item["_episode_key_override"] = next(iter(matching_specials))
                elif matching_specials:
                    # Named special mini-series are commonly stored as a
                    # numbered three-part folder.  When that folder label
                    # matches an equally sized consecutive run of official
                    # TMDB special titles, preserve source order and map the
                    # numbered files to that official run.  This covers
                    # releases such as “柯里乌斯之梦” without treating them as
                    # Season 01 episodes.
                    source_key = extract_episode_key(str(item.get("name", "")))
                    ordered_specials = sorted(matching_specials)
                    if (
                        source_key is not None
                        and source_key.kind == "regular"
                        and not source_key.end_number
                        and 1 <= source_key.number <= len(ordered_specials)
                    ):
                        item["_episode_kind_override"] = "special"
                        item["_episode_key_override"] = ordered_specials[
                            source_key.number - 1
                        ]
                special_files.append(item)
            else:
                unknown_media.append(item)

        remapped_season_groups, reset_absolute_warning = (
            _remap_complete_reset_absolute_season_groups(
                season_groups,
                positive_seasons,
            )
        )
        if reset_absolute_warning:
            season_groups = defaultdict(list, remapped_season_groups)
            special_release_warnings.append(reset_absolute_warning)

        # A second encode of the same one-season show may use bare bracket
        # numbers (``[01]``) while another encode uses explicit ``S01E01``.
        runtime_movie_groups, runtime_movie_warnings = (
            _extract_runtime_proven_overflow_movies(
                kwargs["alist"],
                kwargs["tmdb_client"],
                show_for_season_names,
                season_groups,
                official_season_counts,
                official_special_runtimes,
                unknown_media,
            )
        )
        for runtime_movie_id, runtime_movie_files in runtime_movie_groups.items():
            movie_groups[runtime_movie_id].extend(runtime_movie_files)
        special_release_warnings.extend(runtime_movie_warnings)

        # Once the TV work has exactly one official positive season, accept the
        # bare form only when its cleaned release title exactly names that same
        # show and every number is within the official season boundary.
        if len(positive_seasons) == 1 and unknown_media:
            sole_season = int(positive_seasons[0]["season_number"])
            sole_count = int(positive_seasons[0]["episode_count"])
            show_title_keys = {
                _normalize_match_title(str(show_for_season_names.get(field) or ""))
                for field in ("name", "original_name")
            }
            show_title_keys.discard("")
            proven_bare_items: list[dict[str, Any]] = []
            remaining_unknown: list[dict[str, Any]] = []
            for item in unknown_media:
                key = extract_episode_key(str(item.get("name", "")))
                query_keys = {
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(item)
                }
                if (
                    key is not None
                    and key.kind == "regular"
                    and not key.end_number
                    and 1 <= key.number <= sole_count
                    and bool(query_keys & show_title_keys)
                    and entry_edition_tag(item) is None
                ):
                    proven_bare_items.append(item)
                else:
                    remaining_unknown.append(item)
            if proven_bare_items:
                season_groups[sole_season].extend(proven_bare_items)
                edition_group_warnings[sole_season].append(
                    "检测到同一作品的另一发行版本使用裸 [01] 集号；其清理后标题与"
                    "TMDB 正式剧名完全一致，且编号位于唯一官方季度范围内，已合并比较"
                )
                unknown_media = remaining_unknown

        special_release_warnings.extend(
            _map_explicit_special_release_runs(
                special_files,
                official_special_title_variants,
                official_special_air_dates,
            )
        )
        # Resolve metadata-backed generic SP files while the smart planner
        # still has the complete official/used-special context.  Waiting until
        # the split sub-plan loses that context (for example SP02-SP05 may be
        # in a named short-series child while 23β is an explicit override),
        # leaving a provable SP01 as a false orphan.
        if len(positive_seasons) == 1 and special_files:
            pre_split_special_groups = parse_ep_files(
                special_files,
                prefer_simplified=False,
                defer_unnumbered_specials=True,
            )
            metadata_special_warnings = _map_unnumbered_special_from_subtitle_title(
                kwargs["alist"],
                special_files,
                pre_split_special_groups,
                {
                    EpisodeKey("special", number): title
                    for number, title in official_special_titles.items()
                },
                series_titles=[
                    str(show_for_season_names.get("name") or ""),
                    str(show_for_season_names.get("original_name") or ""),
                ],
                regular_episode_count=int(positive_seasons[0]["episode_count"]),
                tmdb_client=kwargs["tmdb_client"],
                tmdb_id=int(tmdb_id),
                season=int(positive_seasons[0]["season_number"]),
            )
            if metadata_special_warnings:
                mapped_special_paths = {
                    str(item.get("full_path", "")): key.number
                    for key, members in pre_split_special_groups.items()
                    if key.kind == "special"
                    for item in members
                }
                for item in special_files:
                    mapped_number = mapped_special_paths.get(
                        str(item.get("full_path", ""))
                    )
                    if mapped_number is None:
                        continue
                    item["_episode_kind_override"] = "special"
                    item["_episode_key_override"] = mapped_number
                special_release_warnings.extend(metadata_special_warnings)
        propagated_special_subtitles = _propagate_explicit_video_episode_overrides(
            [
                *special_files,
                *unknown_media,
                *(item for group in season_groups.values() for item in group),
            ]
        )
        if propagated_special_subtitles:
            special_release_warnings.append(
                f"{propagated_special_subtitles} 个与已确认特别篇视频同名的外挂字幕"
                "已跟随视频的官方季集映射"
            )
            # A backup subtitle directory may have been classified under the
            # parent season before its exact-basename video proved a special
            # mapping.  Keep the proven companions in the same subplan as the
            # video; otherwise split planning would see an orphan subtitle.
            moved_paths: set[str] = set()
            for season_number in list(season_groups):
                retained: list[dict[str, Any]] = []
                for item in season_groups[season_number]:
                    if item.get("_episode_kind_override") == "special":
                        special_files.append(item)
                        moved_paths.add(str(item.get("full_path", "")))
                    else:
                        retained.append(item)
                season_groups[season_number] = retained
            remaining_unknown: list[dict[str, Any]] = []
            for item in unknown_media:
                if item.get("_episode_kind_override") == "special":
                    special_files.append(item)
                    moved_paths.add(str(item.get("full_path", "")))
                else:
                    remaining_unknown.append(item)
            unknown_media = remaining_unknown
            if moved_paths:
                special_files = list({
                    str(item.get("full_path", "")): item
                    for item in special_files
                }.values())

        special_files.extend(
            _detach_numbered_subgroups_from_mixed_movie_groups(movie_groups)
        )

        # A folder labelled OAD/OVA/“特别篇” may be a separately catalogued
        # child work rather than the parent's Season 00.  Resolve the complete
        # top-level folder as its own identity using the cleaned release title,
        # the parent show title plus folder label, and (for TV children) the
        # exact episode count.  Only one confirmed strongest TMDB identity is
        # accepted; otherwise the files remain in the ordinary special review
        # path below.
        special_top_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in special_files:
            full_path = str(item.get("full_path", ""))
            relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
            parts = relative.split("/")
            if len(parts) >= 2:
                special_top_groups[parts[0]].append(item)
        independently_routed_paths: set[str] = set()
        for segment, group_items in sorted(special_top_groups.items()):
            videos = [
                item for item in group_items
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            ]
            # A complete official Season 00 mapping is stronger than a fuzzy
            # independent-work search for the enclosing folder. Bare part
            # filenames can match an unrelated numeric movie, and a disc
            # extra run can share a title with a separately catalogued OVA.
            # Once every video has an explicit official-special override, do
            # not let this later child-work pass steal it.
            if videos and all(
                item.get("_episode_kind_override") == "special"
                and isinstance(item.get("_episode_key_override"), int)
                for item in videos
            ):
                continue
            source_numbers = sorted({
                key.number
                for item in videos
                if (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind in {"regular", "special"}
                and not key.end_number
            })
            if (
                not videos
                or source_numbers != list(range(1, len(source_numbers) + 1))
                or len(source_numbers) != len(videos)
            ):
                continue
            show_title = str(
                show_for_season_names.get("name")
                or show_for_season_names.get("original_name")
                or ""
            ).strip()
            parent_titles = [
                str(show_for_season_names.get(field) or "").strip()
                for field in ("name", "original_name")
                if str(show_for_season_names.get(field) or "").strip()
            ]
            try:
                parent_aliases = [
                    *parent_titles,
                    *_alternative_tmdb_titles(
                        kwargs["tmdb_client"], "tv", int(kwargs["tmdb_id"])
                    ),
                ]
            except (ApiError, KeyError, TypeError, ValueError):
                parent_aliases = parent_titles
            queries = _child_work_query_variants(
                [
                    query
                    for item in videos[:3]
                    # Only the concrete release title is child identity.
                    # Later fallbacks may be the bare franchise name, codec
                    # payload, or enclosing movie folder and can introduce an
                    # unrelated two-episode title into the ranking.
                    for query in _movie_queries_from_item(item)[:1]
                ],
                parent_titles=parent_titles,
                parent_aliases=parent_aliases,
            )
            if show_title and not queries:
                directory_query = f"{show_title} {segment}"
                if _usable_release_title_query(directory_query):
                    queries.append(directory_query)
            queries = list(dict.fromkeys(queries))
            candidates: dict[tuple[str, int], AutoMatch] = {}
            for query in queries:
                try:
                    candidate, _ = auto_match_tmdb(
                        kwargs["tmdb_client"],
                        query,
                        media_type="tv",
                        min_confidence=0.88,
                        prefer_animation=_media_context_from_source_and_target(
                            source_root,
                            str(kwargs["parent_path"]),
                        )[1],
                        expected_episode_count=len(source_numbers),
                    )
                except ScraperError:
                    continue
                if (
                    candidate.status != "confirmed"
                    or candidate.tmdb_id == kwargs.get("tmdb_id")
                    or candidate.media_type not in {"tv", "movie"}
                ):
                    continue
                identity = (candidate.media_type, candidate.tmdb_id)
                previous = candidates.get(identity)
                if previous is None or candidate.confidence > previous.confidence:
                    candidates[identity] = candidate
            if not candidates:
                continue
            ranked = sorted(
                candidates.values(),
                key=lambda item: item.confidence,
                reverse=True,
            )
            if len(ranked) > 1 and ranked[0].confidence - ranked[1].confidence < 0.08:
                continue
            child_match = ranked[0]
            if child_match.media_type == "movie":
                movie_groups[child_match.tmdb_id].extend(group_items)
                independently_routed_paths.update(
                    str(item["full_path"]) for item in group_items
                )
                special_release_warnings.append(
                    f"子目录《{segment}》已通过 TMDB 标题/别名唯一确认是独立电影"
                    f"《{child_match.title}》（{child_match.year}），未归入母作品 Season 00"
                )
                continue
            child_detail = kwargs["tmdb_client"].get(f"/tv/{child_match.tmdb_id}")
            positive_child_seasons = [
                item
                for item in (child_detail.get("seasons") or [])
                if isinstance(item, Mapping)
                and isinstance(item.get("season_number"), int)
                and int(item["season_number"]) > 0
                and isinstance(item.get("episode_count"), int)
            ]
            if (
                len(positive_child_seasons) != 1
                or int(positive_child_seasons[0]["episode_count"])
                != len(source_numbers)
            ):
                continue
            child_season = int(positive_child_seasons[0]["season_number"])
            child_files: list[dict[str, Any]] = []
            for original in group_items:
                item = dict(original)
                key = extract_episode_key(str(item.get("name", "")))
                if key is not None and key.kind in {"regular", "special"}:
                    item["_episode_kind_override"] = "regular"
                    item["_episode_key_override"] = key.number
                child_files.append(item)
            child_plan = build_tv_plan(
                alist=kwargs["alist"],
                tmdb_client=kwargs["tmdb_client"],
                src_path=source_root,
                parent_path=kwargs["parent_path"],
                tmdb_id=child_match.tmdb_id,
                season=child_season,
                absolute=False,
                prefer_simplified=bool(kwargs.get("prefer_simplified")),
                allow_unmapped=False,
                ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                episode_map_path=None,
                episode_group_id=None,
                auto_special_title_match=True,
                auto_align_subtitles=True,
                source_files=child_files,
            )
            child_tv_plans.append(child_plan)
            independently_routed_paths.update(
                str(item["full_path"]) for item in group_items
            )
            special_release_warnings.append(
                f"子目录《{segment}》已通过 TMDB 标题/别名和完整 {len(source_numbers)} 集"
                f"边界唯一确认是独立剧集《{child_match.title}》（{child_match.year}），"
                "未归入母作品 Season 00"
            )
        if independently_routed_paths:
            special_files = [
                item for item in special_files
                if str(item.get("full_path", "")) not in independently_routed_paths
            ]

        # A directly matched season may store ordinary episodes as bare
        # ``01.mkv`` files under quality/language wrappers.  Accept the run
        # either for a one-season work, or when the franchise member matcher
        # has already proven a non-default official season from the exact
        # member title and complete episode boundary.  The internal proof flag
        # is deliberately unavailable to the CLI's ordinary ``--season``
        # default, so a multi-season root cannot be guessed into Season 01.
        requested_meta = next(
            (
                item for item in positive_seasons
                if int(item["season_number"]) == int(kwargs.get("season") or 1)
            ),
            None,
        )
        boundary_meta = (
            requested_meta
            if proven_member_season and requested_meta is not None
            else positive_seasons[0]
            if len(positive_seasons) == 1
            else None
        )
        if boundary_meta is not None and not season_groups and unknown_media:
            sole_season = int(boundary_meta["season_number"])
            sole_count = int(boundary_meta["episode_count"])
            regular_video_rows = [
                key.number
                for item in unknown_media
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and not _has_special_context(item)
                and (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
            ]
            if (
                set(regular_video_rows) == set(range(1, sole_count + 1))
            ):
                retained_unknown: list[dict[str, Any]] = []
                for item in unknown_media:
                    key = extract_episode_key(str(item.get("name", "")))
                    if (
                        key is not None
                        and key.kind == "regular"
                        and not key.end_number
                        and 1 <= key.number <= sole_count
                        and not _has_special_context(item)
                    ):
                        season_groups[sole_season].append(item)
                    else:
                        retained_unknown.append(item)
                unknown_media = retained_unknown
                edition_group_warnings[sole_season].append(
                    "源根目录中的裸集号完整覆盖已确认的 TMDB "
                    f"Season {sole_season:02d} 边界；已按完整边界归入"
                )

        # Some release folders flatten several official TMDB seasons into one
        # cumulative E01..EN sequence (often with E00 as the prologue).  Split
        # that sequence only when it exactly covers the sum of the advertised
        # official seasons; this avoids guessing from a partial or irregular
        # release.  The mapping is generic and follows TMDB season counts.
        named_child_video_segments: set[str] = set()
        for item in unknown_media:
            if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
                continue
            full_path = str(item.get("full_path", ""))
            relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
            parts = relative.split("/")
            if len(parts) < 2:
                continue
            segment = parts[0]
            if (
                _season_from_source("/" + segment) is None
                and _season_from_series_variant(segment, show_for_season_names) is None
                and _usable_release_title_query(_query_from_source("/" + segment))
            ):
                named_child_video_segments.add(segment)
        if (
            not season_groups
            and len(positive_seasons) >= 2
            and unknown_media
            and not named_child_video_segments
        ):
            official_seasons = sorted(
                (
                    int(item["season_number"]),
                    int(item["episode_count"]),
                )
                for item in positive_seasons
            )
            official_total = sum(count for _, count in official_seasons)
            source_video_keys = sorted({
                key.number
                for item in unknown_media
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
                and key.number > 0
            })
            if source_video_keys == list(range(1, official_total + 1)):
                cumulative_map: dict[int, tuple[int, int]] = {}
                offset = 0
                for official_season, episode_count in official_seasons:
                    for local_episode in range(1, episode_count + 1):
                        cumulative_map[offset + local_episode] = (
                            official_season,
                            local_episode,
                        )
                    offset += episode_count
                remaining_unknown: list[dict[str, Any]] = []
                zero_items: list[dict[str, Any]] = []
                for original in unknown_media:
                    key = extract_episode_key(str(original.get("name", "")))
                    if key is None or key.kind != "regular" or key.end_number:
                        remaining_unknown.append(original)
                        continue
                    if key.number == 0:
                        zero_items.append(original)
                        continue
                    mapped = cumulative_map.get(key.number)
                    if mapped is None:
                        remaining_unknown.append(original)
                        continue
                    official_season, local_episode = mapped
                    item = dict(original)
                    item["_episode_kind_override"] = "regular"
                    item["_episode_key_override"] = local_episode
                    season_groups[official_season].append(item)
                unknown_media = remaining_unknown
                if zero_items and official_special_titles:
                    prologue_candidates = [
                        number
                        for number, titles in official_special_title_variants.items()
                        if any(
                            re.search(
                                r"(?:prologue|序章|プロローグ|前日[譚谭])",
                                title,
                                re.IGNORECASE,
                            )
                            for title in titles
                        )
                    ]
                    prologue_special = (
                        prologue_candidates[0]
                        if len(prologue_candidates) == 1
                        else None
                    )
                else:
                    prologue_special = None
                if zero_items and prologue_special is not None:
                    for original in zero_items:
                        item = dict(original)
                        item["_episode_kind_override"] = "special"
                        item["_episode_key_override"] = prologue_special
                        special_files.append(item)
                    remaining_specials = sorted(
                        set(official_special_titles) - {prologue_special}
                    )
                    unnumbered_specials = [
                        item
                        for item in special_files
                        if (
                            (key := extract_episode_key(str(item.get("name", ""))))
                            is not None
                            and key.kind == "special"
                            and key.number == 0
                            and item.get("_episode_key_override") is None
                        )
                    ]
                    # A sole remaining TMDB candidate is not evidence by
                    # itself.  Require the relative file sizes of E00 and SP
                    # to agree with the official TMDB runtimes; releases from
                    # the same encode family then provide an independent
                    # signal before we assign the unnumbered special.
                    zero_sizes = sorted(
                        int(item.get("size") or 0)
                        for item in zero_items
                        if Path(str(item.get("name", ""))).suffix.lower()
                        in VIDEO_EXTS
                        and int(item.get("size") or 0) > 0
                    )
                    special_sizes = sorted(
                        int(item.get("size") or 0)
                        for item in unnumbered_specials
                        if Path(str(item.get("name", ""))).suffix.lower()
                        in VIDEO_EXTS
                        and int(item.get("size") or 0) > 0
                    )
                    source_ratio = (
                        special_sizes[len(special_sizes) // 2]
                        / zero_sizes[len(zero_sizes) // 2]
                        if zero_sizes and special_sizes
                        else 0.0
                    )
                    runtime_ratio = (
                        official_special_runtimes.get(remaining_specials[0], 0)
                        / official_special_runtimes.get(prologue_special, 0)
                        if len(remaining_specials) == 1
                        and official_special_runtimes.get(prologue_special, 0)
                        else 0.0
                    )
                    ratio_agrees = (
                        source_ratio > 0
                        and runtime_ratio > 0
                        and 0.67 <= source_ratio / runtime_ratio <= 1.5
                    )
                    if (
                        len(remaining_specials) == 1
                        and unnumbered_specials
                        and ratio_agrees
                    ):
                        for item in unnumbered_specials:
                            item["_episode_kind_override"] = "special"
                            item["_episode_key_override"] = remaining_specials[0]
                        edition_group_warnings[official_seasons[0][0]].append(
                            "E00 已由 TMDB 多语言标题确认对应序章；未编号 SP 的"
                            "文件大小比例与 TMDB 官方特别篇时长比例一致，已据此确认映射"
                        )
                packed_single_season = True
                edition_group_warnings[official_seasons[0][0]].append(
                    "源目录使用跨季度连续集号；已按 TMDB 各季度官方集数边界"
                    f"拆分为 {len(official_seasons)} 个 Season"
                )

        # A named "New Edit" is an alternate cut, not a separate season or a
        # generic Director's Cut.  When the ordinary first-season source has
        # exactly 2N-1 episodes and the re-edit has N consecutively numbered
        # files, the one-hour re-edit structure is mechanically provable:
        # file 1 covers E01 and each later file covers the next two episodes.
        # Keep both versions by assigning explicit multi-episode ranges.
        new_edit_items = [
            item
            for item in unknown_media
            if entry_edition_tag(item) == "New Edit"
        ]
        if 1 in season_groups and new_edit_items:
            regular_first_season_keys = sorted({
                key.number
                for item in season_groups[1]
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and entry_edition_tag(item) is None
                and (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
            })
            new_edit_keys = sorted({
                key.number
                for item in new_edit_items
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
            })
            if (
                regular_first_season_keys
                and new_edit_keys
                and regular_first_season_keys
                == list(range(1, len(regular_first_season_keys) + 1))
                and new_edit_keys == list(range(1, len(new_edit_keys) + 1))
                and len(regular_first_season_keys) == 2 * len(new_edit_keys) - 1
            ):
                consumed_new_edit: set[str] = set()
                for original in new_edit_items:
                    item = dict(original)
                    source_key = extract_episode_key(str(item.get("name", "")))
                    if (
                        source_key is None
                        or source_key.kind != "regular"
                        or source_key.end_number
                    ):
                        continue
                    if source_key.number == 1:
                        start_episode = 1
                        end_episode = 0
                    else:
                        start_episode = source_key.number * 2 - 2
                        end_episode = start_episode + 1
                    item["_episode_kind_override"] = "regular"
                    item["_episode_key_override"] = start_episode
                    if end_episode:
                        item["_episode_end_override"] = end_episode
                    season_groups[1].append(item)
                    consumed_new_edit.add(str(item["full_path"]))
                unknown_media = [
                    item
                    for item in unknown_media
                    if str(item["full_path"]) not in consumed_new_edit
                ]
                edition_group_warnings[1].append(
                    f"已识别 {len(new_edit_keys)} 集 New Edit：第 1 集对应 S01E01，"
                    f"其后按双集重编范围对应至 S01E{len(regular_first_season_keys):02d}；"
                    "保留为 {edition-New Edit}，未改称 Director's Cut"
                )

        # Some TMDB records expose every broadcast cour under one long season,
        # while release folders reset numbering per cour (for example White
        # Album and Oshi no Ko).  When the source groups collectively cover the
        # exact official episode count, concatenate them in directory-season
        # order instead of requesting non-existent TMDB seasons.
        #
        # Before packing, verify that a folder labelled “第二季” is not actually
        # an independently catalogued sequel.  White Album 2 is the canonical
        # counterexample: the shelf calls it the second season, but TMDB stores
        # it as a different TV work.  A different id is accepted only when the
        # release title itself matches at high confidence and its exact episode
        # count agrees, so ordinary multi-season shows remain grouped.
        divergent_seasons: list[int] = []
        if len(season_groups) >= 2 and not packed_single_season:
            for source_season, group_items in sorted(season_groups.items()):
                if source_season <= min(season_groups):
                    continue
                videos = [
                    item
                    for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                ]
                episode_count = len({
                    key.number
                    for item in videos
                    if (key := extract_episode_key(str(item.get("name", "")))) is not None
                    and key.kind == "regular"
                }) or None
                if not videos or episode_count is None:
                    continue
                release_queries = _season_parent_identity_queries(
                    videos,
                    source_root=source_root,
                    season_number=source_season,
                    show=show_for_season_names,
                )
                for release_query in release_queries:
                    try:
                        candidate, _ = auto_match_tmdb(
                            kwargs["tmdb_client"],
                            release_query,
                            media_type="tv",
                            min_confidence=0.92,
                            prefer_animation=_media_context_from_source_and_target(
                                source_root,
                                str(kwargs["parent_path"]),
                            )[1],
                            expected_episode_count=episode_count,
                        )
                        if candidate.status != "confirmed":
                            continue
                    except ScraperError:
                        continue
                    if candidate.tmdb_id != kwargs.get("tmdb_id"):
                        divergent_seasons.append(source_season)
                        break
        for source_season in divergent_seasons:
            unknown_media.extend(season_groups.pop(source_season))

        if len(positive_seasons) == 1 and len(season_groups) >= 2:
            official_season = int(positive_seasons[0]["season_number"])
            official_count = int(positive_seasons[0]["episode_count"])
            direct_long_season_warnings = (
                _merge_broadcast_folders_into_long_tmdb_season(
                    season_groups,
                    official_season=official_season,
                    block_counts=official_long_season_block_counts,
                )
            )
            if direct_long_season_warnings:
                edition_group_warnings[official_season].extend(
                    direct_long_season_warnings
                )
                if len(season_groups) == 1:
                    packed_single_season = True
            # A release may place both locally numbered episodes (01..N) and
            # a lower-resolution whole-series numbering (prior+1..prior+N)
            # inside the same explicit broadcast-season folder. Fold only the
            # cumulative copy for which a complete local TMDB block already
            # proves the boundary; partial local runs remain untouched.
            for source_season, group_items in season_groups.items():
                if not (1 < source_season <= len(official_long_season_block_counts)):
                    continue
                block_count = official_long_season_block_counts[source_season - 1]
                prior_count = sum(official_long_season_block_counts[: source_season - 1])
                raw_video_numbers = {
                    key.number
                    for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                    and (key := extract_episode_key(str(item.get("name", "")))) is not None
                    and key.kind == "regular"
                    and not key.end_number
                }
                has_complete_local_run = set(
                    range(1, block_count + 1)
                ).issubset(raw_video_numbers)
                has_complete_cumulative_run = raw_video_numbers == set(
                    range(prior_count + 1, prior_count + block_count + 1)
                )
                if not (has_complete_local_run or has_complete_cumulative_run):
                    continue
                cumulative_numbers = {
                    number
                    for number in raw_video_numbers
                    if prior_count < number <= prior_count + block_count
                }
                if not cumulative_numbers:
                    continue
                for item in group_items:
                    key = extract_episode_key(str(item.get("name", "")))
                    if (
                        key is not None
                        and key.kind == "regular"
                        and not key.end_number
                        and key.number in cumulative_numbers
                    ):
                        item["_episode_key_override"] = key.number - prior_count
                edition_group_warnings[official_season].append(
                    (
                        f"同一播出季度同时包含完整本季编号 01–{block_count:02d} 与"
                        if has_complete_local_run else
                        "该播出季度完整使用"
                    )
                    + f"全剧累计编号 {prior_count + 1:02d}–"
                    f"{max(cumulative_numbers):02d}；已按 TMDB 长期断档边界"
                    + ("合并为同集版本" if has_complete_local_run else "换算为本季集号")
                )
            major_gap_warnings = (
                _merge_release_seasons_into_long_tmdb_season_by_major_gaps(
                    season_groups,
                    official_season=official_season,
                    official_episodes=official_long_season_episodes,
                )
            )
            if major_gap_warnings:
                edition_group_warnings[official_season].extend(major_gap_warnings)
                if len(season_groups) == 1:
                    packed_single_season = True
            group_keys: dict[int, list[int]] = {}
            for source_season, group_items in season_groups.items():
                group_keys[source_season] = sorted({
                    int(item.get("_episode_key_override", key.number))
                    for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                    and (key := extract_episode_key(str(item.get("name", "")))) is not None
                    and key.kind == "regular"
                    and not key.end_number
                })
            source_season_numbers = sorted(group_keys)
            exact_complete_match = (
                sum(len(keys) for keys in group_keys.values()) == official_count
            )
            broadcast_prefix_match = (
                source_season_numbers
                == list(range(1, len(source_season_numbers) + 1))
                and len(source_season_numbers)
                <= len(official_long_season_block_counts)
                and all(
                    len(group_keys[season_number])
                    == official_long_season_block_counts[season_number - 1]
                    for season_number in source_season_numbers[:-1]
                )
                and bool(source_season_numbers)
                and 0 < len(group_keys[source_season_numbers[-1]])
                <= official_long_season_block_counts[source_season_numbers[-1] - 1]
            )
            if (
                all(group_keys.values())
                and (exact_complete_match or broadcast_prefix_match)
                and all(
                    keys == list(range(1, len(keys) + 1))
                    for keys in group_keys.values()
                )
            ):
                packed: list[dict[str, Any]] = []
                next_episode = 1
                for source_season in sorted(season_groups):
                    key_map = {
                        source_key: next_episode + index
                        for index, source_key in enumerate(group_keys[source_season])
                    }
                    next_episode += len(key_map)
                    for original in season_groups[source_season]:
                        item = dict(original)
                        source_key = extract_episode_key(str(item.get("name", "")))
                        if (
                            source_key is not None
                            and source_key.kind == "regular"
                        ):
                            logical_key = int(
                                item.get("_episode_key_override", source_key.number)
                            )
                            mapped_key = key_map.get(logical_key)
                            if mapped_key is not None:
                                item["_episode_key_override"] = mapped_key
                        packed.append(item)
                season_groups = defaultdict(list, {official_season: packed})
                packed_single_season = True
                if broadcast_prefix_match and not exact_complete_match:
                    completed_blocks = ", ".join(
                        str(value)
                        for value in official_long_season_block_counts[
                            : len(source_season_numbers) - 1
                        ]
                    )
                    latest_count = len(group_keys[source_season_numbers[-1]])
                    latest_total = official_long_season_block_counts[
                        source_season_numbers[-1] - 1
                    ]
                    edition_group_warnings[official_season].append(
                        "TMDB 将多个播出季度连续编号在同一 Season；已依据官方播出日期的"
                        f"长期断档边界合并完整前序季度（{completed_blocks or '无'} 集）并"
                        f"保留最新季度当前 {latest_count}/{latest_total} 集，未要求未播内容"
                    )
            else:
                long_season_pack_diagnostic = (
                    "TMDB 长季播出块="
                    f"{official_long_season_block_counts or '无可验证断档'}；"
                    "源季度集号="
                    + ", ".join(
                        f"S{number:02d}:{keys}"
                        for number, keys in sorted(group_keys.items())
                    )
                    + f"；完整总数匹配={exact_complete_match}，"
                    f"播出前缀匹配={broadcast_prefix_match}"
                )
                edition_group_warnings[official_season].append(
                    "TMDB 长季自动合并未通过：" + long_season_pack_diagnostic
                )
        # If the source groups cannot be packed into TMDB's one official
        # season, an out-of-range “第二季” may actually be an independent sequel
        # work.  Move only those impossible season groups through the child
        # title matcher instead of calling a known-nonexistent season endpoint.
        if not packed_single_season and official_positive_season_numbers:
            impossible_seasons = [
                season_number
                for season_number in season_groups
                if season_number not in official_positive_season_numbers
            ]
            for season_number in impossible_seasons:
                impossible_items = season_groups.pop(season_number)
                if season_number > max(official_positive_season_numbers):
                    out_of_range_season_by_path.update({
                        str(item["full_path"]): season_number
                        for item in impossible_items
                    })
                unknown_media.extend(impossible_items)
        if unknown_media:
            numbered_collection_groups, unknown_media, collection_warnings = (
                _resolve_numbered_movie_collection_groups(
                    kwargs["tmdb_client"],
                    unknown_media,
                )
            )
            for movie_id, collection_files in numbered_collection_groups.items():
                movie_groups[movie_id].extend(collection_files)
            special_release_warnings.extend(collection_warnings)
        if unknown_media:
            top_level_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in unknown_media:
                full_path = str(item["full_path"])
                relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
                parts = relative.split("/")
                directory_parts = parts[:-1]
                while directory_parts and (
                    re.fullmatch(
                        r"(?:4k|8k|1080p|2160p)?\s*(?:动漫|动画|备份版|备份|资源)?",
                        directory_parts[0],
                        flags=re.IGNORECASE,
                    )
                    or (
                        re.search(
                            r"(?:4k|8k|2160p|1080p|720p)",
                            directory_parts[0],
                            flags=re.IGNORECASE,
                        )
                        and re.search(
                            r"(?:字幕|内封|内嵌|外挂|硬字|软字|版本|压制)",
                            directory_parts[0],
                            flags=re.IGNORECASE,
                        )
                    )
                ):
                    directory_parts = directory_parts[1:]
                if (
                    directory_parts
                    and directory_parts[0] not in {"备份字幕", "字幕", "Subtitles"}
                ):
                    top_level_groups[directory_parts[0]].append(item)
            consumed: set[str] = set()
            staged_child_paths: set[str] = set()
            child_tv_sources: dict[int, list[dict[str, Any]]] = defaultdict(list)
            backup_subtitle_owners = _unique_backup_subtitle_release_owners(
                top_level_groups,
                unknown_media,
            )

            def companion_subtitles(
                segment: str,
                videos: Sequence[Mapping[str, Any]],
            ) -> list[dict[str, Any]]:
                del videos
                return [
                    item for item in unknown_media
                    if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
                    and str(item.get("full_path", "")) not in consumed
                    and str(item.get("full_path", "")) not in staged_child_paths
                    and backup_subtitle_owners.get(str(item.get("full_path", "")))
                    == segment
                ]

            for segment, group_items in sorted(top_level_groups.items()):
                videos = [
                    item for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                ]
                if not videos:
                    continue
                try:
                    child_queries = list(dict.fromkeys([
                        _query_from_source(join_remote(source_root, segment)),
                        *(
                            query
                            for item in videos[:3]
                            for query in _movie_queries_from_item(item)
                        ),
                    ]))
                    child_episode_count = len({
                        key.number
                        for item in videos
                        if (key := extract_episode_key(str(item.get("name", "")))) is not None
                        and key.kind == "regular"
                    }) or None
                    child_match = None
                    matched_parent = False
                    last_child_error: ScraperError | None = None
                    for child_query in child_queries:
                        try:
                            candidate, _ = auto_match_tmdb(
                                kwargs["tmdb_client"], child_query,
                                media_type="tv", min_confidence=0.88,
                                prefer_animation=_media_context_from_source_and_target(
                                    source_root,
                                    str(kwargs["parent_path"]),
                                )[1],
                                expected_episode_count=child_episode_count,
                            )
                        except PlanError as exc:
                            last_child_error = exc
                            continue
                        if candidate.status != "confirmed":
                            continue
                        if candidate.tmdb_id == kwargs.get("tmdb_id"):
                            # Do not collapse an explicit future S04 folder
                            # into the parent's sole published Season 01 just
                            # because both happen to contain E01..EN.  The
                            # explicit out-of-range marker is stronger than an
                            # equal episode count and must remain reviewable.
                            if any(
                                str(video.get("full_path", ""))
                                in out_of_range_season_by_path
                                for video in videos
                            ):
                                continue
                            requested_parent_season = int(
                                kwargs.get("season") or 1
                            )
                            requested_meta = next(
                                (
                                    item for item in positive_seasons
                                    if int(item["season_number"])
                                    == requested_parent_season
                                ),
                                None,
                            )
                            if requested_meta is None:
                                if len(positive_seasons) != 1:
                                    continue
                                requested_meta = positive_seasons[0]
                            parent_season = int(requested_meta["season_number"])
                            parent_count = int(requested_meta["episode_count"])
                            video_numbers = [
                                key.number
                                for video in videos
                                if (key := extract_episode_key(str(video.get("name", "")))) is not None
                                and key.kind == "regular"
                                and not key.end_number
                            ]
                            if (
                                len(video_numbers) != parent_count
                                or sorted(video_numbers) != list(range(1, parent_count + 1))
                            ):
                                continue
                            parent_files = [
                                *group_items,
                                *companion_subtitles(segment, videos),
                            ]
                            parent_files = list({
                                str(item["full_path"]): item for item in parent_files
                            }.values())
                            season_groups[parent_season].extend(parent_files)
                            consumed.update(str(item["full_path"]) for item in parent_files)
                            staged_child_paths.update(str(item["full_path"]) for item in parent_files)
                            matched_parent = True
                            break
                        child_match = candidate
                        break
                    if matched_parent:
                        continue
                    if child_match is None:
                        for child_query in child_queries:
                            try:
                                child_match, _ = auto_match_tmdb(
                                    kwargs["tmdb_client"], child_query,
                                    media_type="movie", min_confidence=0.88,
                                    prefer_animation=_media_context_from_source_and_target(
                                        source_root,
                                        str(kwargs["parent_path"]),
                                    )[1],
                                )
                                if child_match.status != "confirmed":
                                    child_match = None
                                    continue
                                break
                            except PlanError as exc:
                                last_child_error = exc
                    if child_match is None:
                        raise last_child_error or PlanError(
                            f"无法识别子作品目录: {segment}"
                        )
                    if child_match.media_type == "movie":
                        movie_groups[child_match.tmdb_id].extend(group_items)
                        consumed.update(str(item["full_path"]) for item in group_items)
                        continue
                    companions = companion_subtitles(segment, videos)
                    child_files = list({str(item["full_path"]): item for item in [*group_items, *companions]}.values())
                    # Several release roots can represent different
                    # resolutions of the same independently identified
                    # spin-off. Stage them by TMDB id and plan them together so
                    # the normal quality pass can compare 2160p and 1080p
                    # counterparts before target-name validation.
                    child_tv_sources[child_match.tmdb_id].extend(child_files)
                    staged_child_paths.update(
                        str(item["full_path"]) for item in child_files
                    )
                except PlanError:
                    continue
            for child_tmdb_id, staged_files in sorted(child_tv_sources.items()):
                child_files = list({
                    str(item["full_path"]): item for item in staged_files
                }.values())
                try:
                    # The child has already been identified as an independent
                    # work.  Do not re-interpret its enclosing shelf label
                    # (for example “第二季”) as the child's own TMDB season;
                    # White Album 2 starts at Season 01 in its separate record.
                    child_args = dict(
                        alist=kwargs["alist"], tmdb_client=kwargs["tmdb_client"],
                        src_path=source_root, parent_path=kwargs["parent_path"],
                        tmdb_id=child_tmdb_id, season=1, absolute=False,
                        prefer_simplified=bool(kwargs.get("prefer_simplified")),
                        allow_unmapped=False,
                        ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                        episode_map_path=None, episode_group_id=None,
                        source_files=child_files,
                    )
                    try:
                        child_plan = build_tv_plan(
                            **child_args,
                            auto_special_title_match=True,
                            auto_align_subtitles=True,
                        )
                    except PlanError:
                        child_plan = build_tv_plan_smart(
                            auto_episode_mode=True,
                            **child_args,
                        )
                except PlanError:
                    continue
                child_tv_plans.append(child_plan)
                consumed.update(str(item["full_path"]) for item in child_files)
            unknown_media = [item for item in unknown_media if str(item["full_path"]) not in consumed]
        if unknown_media:
            numbered_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in unknown_media:
                key = extract_episode_key(str(item.get("name", "")))
                queries = _movie_queries_from_item(item)
                if key and key.kind == "regular" and queries:
                    numbered_groups[_normalize_match_title(queries[0])].append(item)
            consumed_numbered: set[str] = set()
            for group_items in numbered_groups.values():
                videos = [
                    item for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                ]
                video_keys = sorted(
                    {
                        key.number
                        for item in videos
                        if (key := extract_episode_key(str(item.get("name", "")))) is not None
                    }
                )
                if len(videos) < 2 or video_keys != list(range(1, len(video_keys) + 1)):
                    continue
                query = _movie_queries_from_item(videos[0])[0]
                results = [
                    item for item in (
                        kwargs["tmdb_client"].get("/search/movie", query=query).get("results") or []
                    )
                    if isinstance(item, Mapping)
                    and not isinstance(item.get("id"), bool)
                    and str(item.get("release_date") or "")
                ]
                if len(results) != len(video_keys):
                    continue
                query_key = _normalize_match_title(query)
                results_have_title_evidence = True
                for result in results:
                    result_id = int(result["id"])
                    official_titles = _search_item_titles(result, "movie")
                    aliases = _alternative_tmdb_titles(
                        kwargs["tmdb_client"], "movie", result_id
                    )
                    all_titles = [*official_titles, *aliases]
                    best_title_score = max(
                        (_title_similarity(query_key, title) for title in all_titles),
                        default=0.0,
                    )
                    if best_title_score < 0.88 or (
                        _cross_script_unique_match(query, all_titles)
                        and not any(
                            _title_similarity(query_key, alias) >= 0.88
                            for alias in aliases
                        )
                    ):
                        results_have_title_evidence = False
                        break
                if not results_have_title_evidence:
                    continue
                ordered_results = sorted(
                    results,
                    key=lambda item: (str(item.get("release_date") or ""), int(item["id"])),
                )
                for number, result in zip(video_keys, ordered_results):
                    members = [
                        item for item in group_items
                        if (key := extract_episode_key(str(item.get("name", "")))) is not None
                        and key.number == number
                    ]
                    movie_groups[int(result["id"])].extend(members)
                    consumed_numbered.update(str(item["full_path"]) for item in members)
                special_release_warnings.append(
                    f"编号 01–{len(video_keys):02d} 的完整视频序列与 TMDB "
                    f"{len(ordered_results)} 部电影的官方标题/别名逐一一致；"
                    "已仅按官方上映日期顺序建立电影归属"
                )
            unknown_media = [
                item for item in unknown_media
                if str(item["full_path"]) not in consumed_numbered
            ]
        if movie_groups and unknown_media:
            unknown_media = _attach_unique_movie_subtitles(
                movie_groups,
                unknown_media,
                tmdb_client=kwargs["tmdb_client"],
                prefer_animation=_media_context_from_source_and_target(
                    source_root,
                    str(kwargs["parent_path"]),
                )[1],
            )
        if movie_groups and season_groups:
            for season_number, season_items in list(season_groups.items()):
                fractional_items = [
                    item for item in season_items
                    if (
                        (key := extract_episode_key(str(item.get("name", ""))))
                        is not None and key.kind == "fractional"
                    )
                ]
                if not fractional_items:
                    continue
                retained_fractional, fractional_movie_warnings = (
                    _attach_fractional_feature_by_ass_title_to_movie_groups(
                        kwargs["alist"], kwargs["tmdb_client"], movie_groups,
                        fractional_items, files,
                    )
                )
                fractional_paths = {
                    _collision_key(str(item.get("full_path", "")))
                    for item in fractional_items
                }
                season_groups[season_number] = [
                    item for item in season_items
                    if _collision_key(str(item.get("full_path", "")))
                    not in fractional_paths
                ] + retained_fractional
                special_release_warnings.extend(fractional_movie_warnings)
        if movie_groups and special_files:
            special_files, fractional_movie_warnings = (
                _attach_fractional_feature_by_ass_title_to_movie_groups(
                    kwargs["alist"],
                    kwargs["tmdb_client"],
                    movie_groups,
                    special_files,
                    files,
                )
            )
            special_release_warnings.extend(fractional_movie_warnings)
            special_files = _attach_unique_movie_subtitles(
                movie_groups,
                special_files,
                tmdb_client=kwargs["tmdb_client"],
                prefer_animation=_media_context_from_source_and_target(
                    source_root,
                    str(kwargs["parent_path"]),
                )[1],
            )
        # A clearly labelled season beyond TMDB's currently published range is
        # not a parser failure.  First give it the normal child-work matcher
        # above (some shelves call an independently catalogued sequel “S2”).
        # If no independent work can be proven, keep the files in place as
        # review rows instead of aborting every already verified season.
        if out_of_range_season_by_path and unknown_media:
            still_unknown: list[dict[str, Any]] = []
            for item in unknown_media:
                path = str(item["full_path"])
                season_number = out_of_range_season_by_path.get(path)
                if season_number is None:
                    still_unknown.append(item)
                else:
                    retained_unpublished_season_media.append((item, season_number))
            unknown_media = still_unknown
        if (
            packed_single_season
            and official_long_season_block_counts
            and unknown_media
        ):
            attached_root_block, remaining_unknown = (
                _proven_root_first_broadcast_block_files(
                    source_root,
                    unknown_media,
                    official_long_season_block_counts[0],
                )
            )
            if attached_root_block:
                season_groups[int(positive_seasons[0]["season_number"])].extend(
                    attached_root_block
                )
                unknown_media = remaining_unknown
                edition_group_warnings[int(positive_seasons[0]["season_number"])].append(
                    "源根目录完整覆盖 TMDB 长季的第一播出块；已作为该块的同集发行版本"
                )
        if season_groups and unknown_media:
            missing_season, attached_root_files, remaining_unknown = (
                _proven_missing_root_season_files(
                    source_root,
                    unknown_media,
                    season_groups,
                    official_season_counts,
                )
            )
            if missing_season is not None:
                season_groups[missing_season].extend(attached_root_files)
                unknown_media = remaining_unknown
                edition_group_warnings[missing_season].append(
                    f"源根目录完整覆盖官方第 {missing_season} 季 E01–"
                    f"E{official_season_counts[missing_season]:02d}，且这是唯一尚未"
                    "由明确季度目录覆盖的官方季；已按缺失季度边界归入"
                )
        if season_groups and unknown_media:
            # A release may flatten a complete alternate subtitle track into
            # ``备份字幕`` without repeating the season marker in each
            # filename.  Assign such a track only from an exact, unique
            # episode boundary: the subtitle set and the already identified
            # videos must both cover E01..EN, and exactly one official season
            # may have that N.  Partial sets and equal-length seasons remain
            # unresolved instead of being guessed by title similarity.
            subtitle_directories: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in unknown_media:
                if Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS:
                    continue
                full_path = str(item.get("full_path", ""))
                parent_directory, _ = split_remote(full_path)
                subtitle_directories[parent_directory].append(item)
            attached_subtitles: set[str] = set()
            for directory, subtitle_items in subtitle_directories.items():
                subtitle_numbers = {
                    key.number
                    for item in subtitle_items
                    if (key := extract_episode_key(str(item.get("name", "")))) is not None
                    and key.kind == "regular"
                    and not key.end_number
                    and key.number > 0
                }
                if not subtitle_numbers or subtitle_numbers != set(
                    range(1, max(subtitle_numbers) + 1)
                ):
                    continue
                matching_seasons: list[int] = []
                for season_number, season_items in season_groups.items():
                    official_count = official_season_counts.get(season_number)
                    if official_count != max(subtitle_numbers):
                        continue
                    video_numbers = {
                        int(item.get("_episode_key_override") or key.number)
                        for item in season_items
                        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                        and (key := extract_episode_key(str(item.get("name", "")))) is not None
                        and key.kind == "regular"
                        and not key.end_number
                    }
                    if video_numbers == subtitle_numbers:
                        matching_seasons.append(season_number)
                if len(matching_seasons) != 1:
                    continue
                matched_season = matching_seasons[0]
                season_groups[matched_season].extend(subtitle_items)
                attached_subtitles.update(
                    str(item["full_path"]) for item in subtitle_items
                )
                edition_group_warnings[matched_season].append(
                    f"备份字幕目录 {directory} 完整覆盖 E01-E{max(subtitle_numbers):02d}，"
                    f"且仅与第 {matched_season} 季的官方边界及视频集合唯一一致；"
                    "已作为该季的完整备份字幕轨"
                )
            if attached_subtitles:
                unknown_media = [
                    item for item in unknown_media
                    if str(item["full_path"]) not in attached_subtitles
                ]
        if season_groups and unknown_media:
            unknown_media, attached_backup_subtitle_count = (
                _attach_unique_numbered_backup_subtitles(
                    unknown_media,
                    season_groups,
                    official_season_counts,
                )
            )
            if attached_backup_subtitle_count:
                special_release_warnings.append(
                    f"{attached_backup_subtitle_count} 个分散备份字幕已通过同集号、"
                    "同发行标题和唯一季度视频对应归属"
                )
        if len(season_groups) >= 2 or (season_groups and movie_groups):
            if unknown_media:
                if long_season_pack_diagnostic:
                    preview = ", ".join(
                        str(item["full_path"]) for item in unknown_media[:5]
                    )
                    suffix = (
                        f"，另有 {len(unknown_media) - 5} 个"
                        if len(unknown_media) > 5 else ""
                    )
                    raise PlanError(
                        "发现无法识别多季度目录中的季度编号的媒体文件；"
                        + long_season_pack_diagnostic
                        + f"；问题文件: {preview}{suffix}"
                    )
                _raise_unparsed_media(
                    [str(item["full_path"]) for item in unknown_media],
                    "多季度目录中的季度",
                )
        if (
            season_groups
            and special_files
            and not any(
                item.get("_episode_kind_override") == "special"
                for item in special_files
            )
        ):
            # Specials belong to the identified TV work, but their release
            # labels do not by themselves prove Season 00 placement.  Keep
            # them in the same evidence pass as the ordinary season.  Official
            # TMDB matches can still map them; unresolved SP/OVA/OAD/OAV files
            # become retained problem rows instead of an empty standalone
            # sub-plan that aborts the whole series batch.
            season_groups[min(season_groups)].extend(special_files)
            special_files = []
        should_split = bool(movie_groups or child_tv_plans) or (
            bool(season_groups)
            and (
                packed_single_season
                or
                len(season_groups) >= 2
                or bool(special_files)
                or bool(edition_group_warnings)
                or bool(retained_future_media)
                or bool(retained_unpublished_season_media)
                or next(iter(season_groups)) != int(kwargs.get("season") or 1)
            )
        )
        if should_split:
            subplans: list[Plan] = []
            if season_groups:
                for season_number, season_files in sorted(season_groups.items()):
                    sub_kwargs = dict(smart_kwargs)
                    sub_kwargs["season"] = season_number
                    normalized_files, normalization_warning = (
                        _normalize_cumulative_season_episode_numbers(
                            season_number,
                            season_files,
                            positive_seasons,
                        )
                    )
                    sub_kwargs["source_files"] = normalized_files
                    subplan = build_tv_plan(**sub_kwargs)
                    for edition_warning in reversed(
                        edition_group_warnings.get(season_number, [])
                    ):
                        subplan.warnings.insert(0, edition_warning)
                    if normalization_warning:
                        subplan.warnings.insert(0, normalization_warning)
                    subplans.append(subplan)
                if special_files:
                    special_kwargs = dict(smart_kwargs)
                    special_kwargs["season"] = min(season_groups)
                    special_kwargs["source_files"] = special_files
                    subplans.append(build_tv_plan(**special_kwargs))
            else:
                consumed_paths = {
                    str(item["full_path"])
                    for group in movie_groups.values()
                    for item in group
                }
                consumed_paths.update(
                    item.source_path for plan in child_tv_plans for item in plan.files
                )
                main_kwargs = dict(smart_kwargs)
                main_kwargs["source_files"] = [
                    item for item in files
                    if str(item.get("full_path", "")) not in consumed_paths
                ]
                subplans.append(build_tv_plan(**main_kwargs))
            first = subplans[0]
            executable_movie_groups, orphan_movie_files = (
                _partition_movie_groups_with_video(movie_groups)
            )
            movie_parent = split_remote(first.target_root)[0]
            try:
                # A nested franchise root (for example ``/Fate``) can safely
                # contain TV and movie siblings under one lock.  If the TV is
                # already directly below a production category root, that
                # category itself is intentionally not lockable; keep the
                # movie in its own child directory below the TV root instead.
                placement_for(source_root, movie_parent)
            except ValueError:
                movie_parent = first.target_root
            movie_plans = [
                build_movie_plan(
                    kwargs["alist"],
                    kwargs["tmdb_client"],
                    src_path=source_root,
                    # Independent movies are siblings of the TV work, never
                    # children or loose files beside its ``tvshow.nfo``.
                    parent_path=movie_parent,
                    tmdb_id=movie_tmdb_id,
                    ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                    source_files=movie_files,
                    defer_validation=True,
                )
                for movie_tmdb_id, movie_files in sorted(executable_movie_groups.items())
            ]
            metadata = dict(first.metadata)
            if movie_plans:
                metadata["series_root"] = first.target_root
                metadata["member_posters"] = {
                    movie_plan.target_root: movie_plan.metadata["poster_path"]
                    for movie_plan in movie_plans
                    if isinstance(movie_plan.metadata.get("poster_path"), str)
                    and movie_plan.metadata.get("poster_path")
                }
                metadata["member_movies"] = {
                    movie_plan.target_root: {
                        "tmdb_id": movie_plan.metadata["tmdb_id"],
                        "title": movie_plan.metadata["title"],
                        "year": movie_plan.metadata["year"],
                    }
                    for movie_plan in movie_plans
                }
            combined_warnings = [
                *(
                    [f"已从目录结构识别并合并 {sum(number > 0 for number in season_groups)} 个季度"]
                    if season_groups else []
                ),
                *(
                    [
                        f"识别到 {len(movie_plans)} 部独立电影；"
                        "已保留独立 TMDB 身份并放入与电视剧作品目录"
                        "并列的独立电影目录"
                    ]
                    if movie_plans else []
                ),
                *([independent_e00_warning] if independent_e00_warning else []),
                *special_release_warnings,
                *(warning for subplan in subplans for warning in subplan.warnings),
                *(warning for movie_plan in movie_plans for warning in movie_plan.warnings),
            ]
            plan = Plan(
                mode="mixed" if movie_plans else "tv",
                source_root=source_root,
                target_root=(
                    normalize_remote_path(posixpath.commonpath([
                        first.target_root,
                        *(movie_plan.target_root for movie_plan in movie_plans),
                    ]))
                    if movie_plans else first.target_root
                ),
                files=[
                    item
                    for subplan in [*subplans, *movie_plans]
                    for item in subplan.files
                ],
                cleanup_files=_dedupe_cleanup_files([
                    *_planned_cleanup_files(files),
                    *(
                        item
                        for subplan in [*subplans, *movie_plans]
                        for item in subplan.cleanup_files
                    ),
                ]),
                problem_files=[
                    item
                    for subplan in [*subplans, *movie_plans]
                    for item in subplan.problem_files
                ] + [
                    PlannedProblem(
                        source_path=str(item["full_path"]),
                        reason="尚未在 TMDB 发布的未来剧场版；保留原位待人工确认",
                    )
                    for item in retained_future_media
                ] + [
                    PlannedProblem(
                        source_path=str(item["full_path"]),
                        reason=(
                            f"明确标记为第 {season_number} 季，但 TMDB 当前尚未发布"
                            "该季；保留原位待人工确认"
                        ),
                    )
                    for item, season_number in retained_unpublished_season_media
                ] + [
                    PlannedProblem(
                        source_path=str(item["full_path"]),
                        reason="已匹配到独立电影，但没有对应视频；保留原位待人工确认",
                    )
                    for item in orphan_movie_files
                ],
                warnings=list(dict.fromkeys(combined_warnings)),
                metadata=metadata,
                scan_report={
                    "resource_gaps": [
                        dict(gap)
                        for subplan in [*subplans, *movie_plans]
                        for gap in (subplan.scan_report.get("resource_gaps") or [])
                        if isinstance(gap, Mapping)
                    ]
                },
            )
            missing_season_gaps = _tv_season_resource_gaps(
                kwargs["alist"],
                plan,
                series_dir=first.target_root,
                official_seasons=positive_seasons,
            )
            if missing_season_gaps:
                plan.scan_report.setdefault("resource_gaps", []).extend(
                    missing_season_gaps
                )
            if retained_future_media:
                plan.warnings.append(
                    f"{len(retained_future_media)} 个尚未在 TMDB 发布的未来剧场版文件"
                    "将保留原位待人工确认"
                )
            if orphan_movie_files:
                plan.warnings.append(
                    f"{len(orphan_movie_files)} 个独立电影字幕没有对应视频，"
                    "将保留原位待人工确认"
                )
            if retained_unpublished_season_media:
                retained_seasons = sorted({
                    season_number
                    for _, season_number in retained_unpublished_season_media
                })
                plan.warnings.append(
                    f"{len(retained_unpublished_season_media)} 个明确季度文件超出 "
                    f"TMDB 当前已发布范围（第 "
                    f"{', '.join(map(str, retained_seasons))} 季），"
                    "将保留原位待人工确认"
                )
            canonical_warnings: list[str] = []
            if movie_plans:
                # These movies were independently identified from tagged or
                # bounded source groups.  They share an execution plan with
                # the TV work, but no parent/child work relation was proven.
                # Keep all exact identities as siblings at the already-safe
                # movie parent and disable title-derived family containers.
                _movie_root, movie_tree_warnings, _movie_tree_posters = (
                    _plan_canonical_batch_tree(
                        [plan],
                        outer_root=movie_parent,
                        allow_family_boundaries=False,
                    )
                )
                canonical_warnings.extend(movie_tree_warnings)
            if child_tv_plans:
                root_identity = WorkIdentity(
                    "tmdb.tv", int(first.metadata["tmdb_id"])
                )
                _canonical_root, child_tree_warnings, _canonical_posters = (
                    _plan_canonical_batch_tree(
                        [first, *child_tv_plans],
                        outer_root=first.target_root,
                        root_identity=root_identity,
                    )
                )
                canonical_warnings.extend(child_tree_warnings)
                if plan.mode == "mixed":
                    plan.metadata["series_root"] = first.target_root
                plan = _combine_plans_as_batch(
                    source_root, [plan, *child_tv_plans],
                    f"已识别主系列及 {len(child_tv_plans)} 个独立衍生剧集，并保留在主系列目录内",
                )
            if canonical_warnings:
                plan.warnings.extend(
                    warning
                    for warning in canonical_warnings
                    if warning not in plan.warnings
                )
            _dedupe_merged_tv_target_variants(plan)
            validate_plan(kwargs["alist"], plan)
            return plan
    try:
        plan = build_tv_plan(**smart_kwargs)
        missing_season_gaps = _tv_season_resource_gaps(
            kwargs["alist"],
            plan,
            series_dir=plan.target_root,
            official_seasons=positive_seasons,
        )
        if missing_season_gaps:
            plan.scan_report.setdefault("resource_gaps", []).extend(
                missing_season_gaps
            )
        return plan
    except PlanError as seasonal_error:
        can_retry = (
            auto_episode_mode
            and not kwargs.get("absolute")
            and kwargs.get("episode_map_path") is None
            and kwargs.get("episode_group_id") is None
            and "以下集数未在 TMDB 映射中找到" in str(seasonal_error)
        )
        if not can_retry:
            raise
        absolute_kwargs = dict(smart_kwargs)
        absolute_kwargs["absolute"] = True
        try:
            plan = build_tv_plan(**absolute_kwargs)
        except PlanError:
            raise seasonal_error
        plan.warnings.insert(
            0,
            "普通季度映射无法覆盖源集数，系统已验证并自动改用 TMDB 绝对集数映射；请在执行前核对季集结果",
        )
        return plan


def build_movie_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    tmdb_id: int,
    ignore_orphan_temp: bool = False,
    source_files: Sequence[Mapping[str, Any]] | None = None,
    defer_validation: bool = False,
) -> Plan:
    movie = tmdb_client.get(f"/movie/{tmdb_id}")
    title = safe_name(str(movie.get("title") or movie.get("original_title") or tmdb_id))
    year = _extract_year(movie.get("release_date"))
    movie_label = safe_name(f"{title} ({year})")
    desired_movie_dir = join_remote(parent_path, movie_label)
    movie_dir, library_identity_state = resolve_existing_library_root(
        alist,
        parent_path=parent_path,
        desired_root=desired_movie_dir,
        tmdb_id=tmdb_id,
        tv=False,
    )
    scanned_entries = (
        [dict(item) for item in source_files]
        if source_files is not None
        else alist.walk(
            src_path,
            ignore_orphan_temp=ignore_orphan_temp,
            include_bonus=True,
            include_title_extras=True,
        )
    )
    scanned_files = [
        item
        for item in _filter_media(scanned_entries)
        if not should_ignore_extra(str(item.get("name", "")))
    ]
    if not any(
        Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        for item in scanned_files
    ):
        # ``extra`` is normally a release-extra marker, but it can also be an
        # official movie-title word (for example ``未来福音 extra chorus``).
        # In explicit movie mode, recover a filtered primary video only when
        # its cleaned release title agrees with the source directory title.
        # This does not turn a generic Extras folder into a movie.
        source_title_key = _normalize_match_title(_query_from_source(src_path))
        title_matched_videos = [
            item
            for item in scanned_entries
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and cleanup_reason(str(item.get("name", ""))) is None
            and not is_sample(str(item.get("name", "")))
            and any(
                len(query_key) >= 5
                and (
                    query_key in source_title_key
                    or source_title_key in query_key
                )
                for query_key in (
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(item)
                )
            )
        ]
        if title_matched_videos:
            scanned_files = [
                item
                for item in scanned_entries
                if item in title_matched_videos
                or Path(str(item.get("name", ""))).suffix.lower()
                in SUBTITLE_EXTS
            ]
    cleanup_files = _planned_cleanup_files(scanned_entries)
    samples = [item for item in scanned_files if is_sample(str(item.get("name", "")))]
    bonus_files = [
        item
        for item in scanned_files
        if item not in samples and bonus_type(str(item.get("name", ""))) is not None
    ]
    files = [
        item for item in scanned_files if item not in samples and item not in bonus_files
    ]
    if not any(Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS for item in files):
        raise PlanError("未找到电影媒体文件")

    # Detect numbered movie parts before quality de-duplication.  Otherwise
    # equally encoded part01/part02/... files land in one movie bucket and the
    # smaller parts can be mistaken for inferior copies of part01.
    candidate_video_items = [
        item
        for item in files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    candidate_video_keys = [
        extract_episode_key(str(item.get("name", "")))
        for item in candidate_video_items
    ]
    candidate_part_numbers = sorted(
        {key.number for key in candidate_video_keys if key is not None}
    )
    release_query_keys = {
        _normalize_match_title(query)
        for item in candidate_video_items
        for query in _movie_queries_from_item(item)[:1]
        if query
    }
    numbered_movie_parts = (
        len(candidate_part_numbers) >= 2
        and all(
            key is not None and key.kind in {"regular", "special"}
            for key in candidate_video_keys
        )
        and len({key.kind for key in candidate_video_keys if key is not None}) == 1
        and len(release_query_keys) == 1
    )
    expected_part_numbers = (
        list(range(1, candidate_part_numbers[-1] + 1))
        if numbered_movie_parts else []
    )
    missing_part_numbers = sorted(
        set(expected_part_numbers) - set(candidate_part_numbers)
    )
    multipart_movie = numbered_movie_parts and not missing_part_numbers
    split_movie_parts = numbered_movie_parts

    # Every entry in this plan has already been confirmed as the same TMDB
    # movie.  Apply the same conservative 4K preference as TV episodes while
    # keeping named cuts/editions in separate comparison buckets. Numbered
    # movie parts are independent content identities, so compare quality only
    # inside each part rather than across the whole movie.
    if split_movie_parts:
        files_by_part: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
        unkeyed_files: list[Mapping[str, Any]] = []
        for item in files:
            key = extract_episode_key(str(item.get("name", "")))
            if key is not None and key.kind in {"regular", "special"}:
                files_by_part[key.number].append(item)
            else:
                unkeyed_files.append(item)
        files = [dict(item) for item in unkeyed_files]
        lower_resolution_videos: list[dict[str, Any]] = []
        for number in candidate_part_numbers:
            kept_part, removed_part = _prefer_highest_resolution_videos(
                files_by_part[number]
            )
            files.extend(kept_part)
            lower_resolution_videos.extend(removed_part)
    else:
        files, lower_resolution_videos = _prefer_highest_resolution_videos(files)
    for item in lower_resolution_videos:
        source_path = normalize_remote_path(str(item["full_path"]))
        source_dir, original_name = split_remote(source_path)
        preferred_source = str(item["_preferred_resolution_source"])
        cleanup_kind = str(item.get("_duplicate_cleanup_kind", "lower_resolution"))
        if cleanup_kind == "burned_subtitle_duplicate":
            reason = _burned_subtitle_cleanup_reason(preferred_source)
        elif cleanup_kind == "same_resolution_duplicate":
            reason = _same_resolution_cleanup_reason(preferred_source)
        elif cleanup_kind == "lower_resolution_subtitle":
            reason = _lower_resolution_subtitle_cleanup_reason(preferred_source)
        else:
            reason = _lower_resolution_cleanup_reason(preferred_source)
        cleanup_files.append(
            PlannedCleanup(
                source_path=source_path,
                source_dir=source_dir,
                original_name=original_name,
                reason=reason,
                source_size=_entry_size_value(item),
                source_modified=_entry_modified_value(item),
                source_hash=_entry_hash_value(item),
            )
        )

    base_name = movie_label
    files = sorted(files, key=lambda x: _collision_key(str(x["full_path"])))
    video_items = [
        item
        for item in files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    if split_movie_parts:
        names_by_path: dict[str, str] = {}
        keyed_files: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in files:
            key = extract_episode_key(str(item.get("name", "")))
            if key is not None and key.kind in {"regular", "special"}:
                keyed_files[key.number].append(item)
        for number, part_files in keyed_files.items():
            part_names = make_unique_media_names(
                f"{base_name} - part{number}",
                part_files,
                preserve_editions=True,
            )
            for item, name in zip(part_files, part_names):
                names_by_path[str(item["full_path"])] = name
        names = [
            names_by_path.get(str(item["full_path"]))
            or make_unique_media_names(base_name, [item], preserve_editions=True)[0]
            for item in files
        ]
    else:
        names = make_unique_media_names(base_name, files, preserve_editions=True)
    planned = []
    for item, final_name in zip(files, names):
        planned.append(
            _planned_file_from_entry(
                item,
                final_name=final_name,
                target_dir=movie_dir,
            )
        )

    bonus_counts: dict[str, int] = defaultdict(int)
    for item in sorted(bonus_files, key=lambda value: _collision_key(str(value["full_path"]))):
        kind = bonus_type(str(item["name"])) or "other"
        bonus_counts[kind] += 1
        serial = "" if bonus_counts[kind] == 1 else str(bonus_counts[kind])
        final_name = _compose_filename(
            base_name,
            f"-{kind}{serial}",
            Path(str(item["name"])).suffix.lower(),
        )
        planned.append(
            _planned_file_from_entry(item, final_name=final_name, target_dir=movie_dir)
        )

    warnings: list[str] = []
    _append_cleanup_warning(warnings, cleanup_files)
    if library_identity_state == "same_tmdb_id" and movie_dir != desired_movie_dir:
        warnings.append(
            f"现有电影 NFO 已确认相同 TMDB ID {tmdb_id}；"
            f"已保留现有目录名 {split_remote(movie_dir)[1]!r}"
        )
    elif library_identity_state == "matching_name_without_nfo":
        warnings.append(
            "目标存在同名目录但没有可验证的电影 NFO；标题/年份相符，"
            "必须人工核对后才能合并"
        )
    if ignore_orphan_temp:
        warnings.append("已显式忽略 .scraper-tmp-* 遗留条目，可能存在未恢复文件")
    if samples:
        warnings.append(
            f"已排除 {len(samples)} 个 sample/样片文件；"
            "保留原位待人工确认"
        )
    if bonus_files:
        warnings.append(f"已按 Infuse 规则整理 {len(bonus_files)} 个预告/花絮文件")
    if multipart_movie:
        warnings.append(
            f"检测到电影被拆为 {len(candidate_part_numbers)} 个连续分段，"
            f"已按 part1-part{candidate_part_numbers[-1]} 命名"
        )

    plan = Plan(
        mode="movie",
        source_root=normalize_remote_path(src_path),
        target_root=movie_dir,
        files=planned,
        warnings=warnings,
        metadata={
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year,
            "poster_path": movie.get("poster_path"),
            "backdrop_path": movie.get("backdrop_path"),
        },
        cleanup_files=cleanup_files,
        problem_files=[
            PlannedProblem(
                source_path=str(item["full_path"]),
                reason="sample/样片文件不参与整理；保留原位待人工确认",
            )
            for item in samples
        ],
        scan_report={
            "resource_gaps": [
                _resource_gap(
                    "missing_multipart_segment",
                    f"{movie_label} - part{number}",
                    "同一电影的源分段编号不连续，该分段缺失",
                    files=[
                        str(item["full_path"])
                        for item in candidate_video_items
                    ],
                )
                for number in missing_part_numbers
            ]
        } if missing_part_numbers else {},
    )
    _add_snapshot_warnings(plan)
    if not defer_validation:
        validate_plan(alist, plan)
    return plan


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"JSON 不允许非有限数值: {value}")


def _reject_duplicate_object_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"JSON 对象包含重复字段: {key!r}")
        result[key] = value
    return result


def _load_json_text(text: str) -> Any:
    return json.loads(
        text,
        parse_constant=_reject_json_constant,
        object_pairs_hook=_reject_duplicate_object_pairs,
    )


def _load_collection_map(path: Path) -> dict[int, int]:
    try:
        raw = _load_json_text(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"无法读取合集映射文件: {path}; {exc}") from exc
    if not isinstance(raw, dict):
        raise PlanError("合集映射文件必须是 JSON 对象，例如 {\"1\": 12345}")
    mapping: dict[int, int] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not re.fullmatch(r"[1-9]\d*", key):
            raise PlanError(f"合集映射的源编号必须是十进制正整数字符串: {key!r}")
        if isinstance(value, bool) or not isinstance(value, int):
            raise PlanError(
                f"合集映射的 TMDB ID 必须是 JSON 正整数，不能使用布尔值、"
                f"小数或字符串: {key!r}: {value!r}"
            )
        source_number = int(key)
        movie_id = value
        if source_number <= 0 or movie_id <= 0:
            raise PlanError(f"合集映射的编号和 TMDB ID 必须为正整数: {key!r}: {value!r}")
        if source_number in mapping:
            raise PlanError(f"合集映射存在重复编号: {key!r}")
        mapping[source_number] = movie_id
    return mapping


def _parse_episode_map_key(value: str) -> EpisodeKey:
    fractional = re.fullmatch(
        r"(?:E)?0*(\d{1,3})\.(\d{1,3})",
        value.strip(),
        re.IGNORECASE,
    )
    if fractional:
        number = int(fractional.group(1))
        if number <= 0:
            raise PlanError(f"无效源集数覆盖键: {value!r}")
        digits = fractional.group(2).rstrip("0") or "0"
        return EpisodeKey("fractional", number, fractional_digits=digits)
    match = re.fullmatch(
        r"(?:(SP|E))?0*(\d{1,4})(?:-(?:(?:SP|E))?0*(\d{1,4}))?",
        value.strip(),
        re.IGNORECASE,
    )
    if not match:
        raise PlanError(f"无效源集数覆盖键: {value!r}")
    kind = "special" if (match.group(1) or "").upper() == "SP" else "regular"
    start = int(match.group(2))
    end = int(match.group(3) or 0)
    if start <= 0 or (end and end < start):
        raise PlanError(f"无效源集数覆盖范围: {value!r}")
    return EpisodeKey(kind, start, end)


def _load_episode_map(path: Path) -> dict[EpisodeKey, tuple[int, int, int]]:
    try:
        raw = _load_json_text(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"无法读取剧集覆盖映射: {path}; {exc}") from exc
    if not isinstance(raw, dict):
        raise PlanError('剧集覆盖映射必须是对象，例如 {"13":"S02E01"}')
    output: dict[EpisodeKey, tuple[int, int, int]] = {}
    for source, target in raw.items():
        if not isinstance(source, str) or not isinstance(target, str):
            raise PlanError("剧集覆盖映射的键和值都必须是字符串")
        key = _parse_episode_map_key(source)
        match = re.fullmatch(
            r"S0*(\d{1,3})E0*(\d{1,4})(?:-E?0*(\d{1,4}))?",
            target.strip(),
            re.IGNORECASE,
        )
        if not match:
            raise PlanError(f"无效目标剧集覆盖值: {target!r}")
        season = int(match.group(1))
        episode = int(match.group(2))
        end_episode = int(match.group(3) or 0)
        if episode <= 0 or (end_episode and end_episode < episode):
            raise PlanError(f"无效目标剧集覆盖范围: {target!r}")
        if bool(key.end_number) != bool(end_episode):
            raise PlanError(
                f"剧集覆盖的源和目标必须同时为单集或同时为范围: "
                f"{source!r}: {target!r}"
            )
        if key in output:
            raise PlanError(f"剧集覆盖映射包含等价重复键: {source!r}")
        output[key] = (season, episode, end_episode)
    return output


def build_collection_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    tmdb_id: int,
    mapping_path: Path | None,
    allow_index_mapping: bool,
    ignore_orphan_temp: bool = False,
) -> Plan:
    collection = tmdb_client.get(f"/collection/{tmdb_id}")
    collection_title = safe_name(str(collection.get("name") or tmdb_id))
    desired_collection_dir = join_remote(parent_path, collection_title)
    collection_dir = _preserve_equivalent_source_root(src_path, desired_collection_dir)
    parts = [
        dict(item) for item in (collection.get("parts") or []) if isinstance(item, dict)
    ]
    if not parts:
        raise PlanError("TMDB 合集中没有电影条目")
    parts.sort(key=lambda item: str(item.get("release_date") or "9999-99-99"))

    explicit_map = _load_collection_map(mapping_path) if mapping_path else None
    if explicit_map is None and not allow_index_mapping:
        raise PlanError(
            "合集按 01、02 顺序映射存在错配风险。请提供 --collection-map JSON 文件，"
            "或显式传入 --allow-index-mapping。"
        )

    source_files = alist.walk(src_path, ignore_orphan_temp=ignore_orphan_temp)
    cleanup_files = _planned_cleanup_files(source_files)
    groups = parse_ep_files(source_files, prefer_simplified=False)
    _raise_unparsed_media(_unparsed_media_paths(source_files, groups), "合集")
    special_keys = sorted(key.display for key in groups if key.kind == "special")
    if special_keys:
        raise PlanError(
            "电影合集不支持特别篇编号，拒绝静默遗漏: " + ", ".join(special_keys)
        )
    regular_groups = {key: value for key, value in groups.items() if key.kind == "regular"}
    if not regular_groups:
        raise PlanError("未找到带编号的合集媒体文件")

    parts_by_id = {int(item["id"]): item for item in parts if item.get("id") is not None}
    warnings: list[str] = []
    _append_cleanup_warning(warnings, cleanup_files)
    if collection_dir != desired_collection_dir:
        warnings.append(
            "TMDB 标题与现有作品目录仅大小写或 Unicode 拼写不同；"
            f"已保留现有目录名 {split_remote(collection_dir)[1]!r}"
        )
    if ignore_orphan_temp:
        warnings.append("已显式忽略 .scraper-tmp-* 遗留条目，可能存在未恢复文件")
    if explicit_map is None:
        warnings.append("使用按上映日期排序的序号映射；执行前必须人工核对 dry-run 输出")
    else:
        source_numbers = {key.number for key in regular_groups}
        extra_numbers = sorted(set(explicit_map) - source_numbers)
        if extra_numbers:
            raise PlanError(
                "合集映射包含源目录中不存在的编号: "
                + ", ".join(str(number) for number in extra_numbers)
            )

    planned: list[PlannedFile] = []
    member_posters: dict[str, str] = {}
    member_movies: dict[str, dict[str, Any]] = {}
    for key in sorted(regular_groups):
        if key.end_number:
            raise PlanError(
                f"电影合集不支持一个文件对应多个合集成员: {key.display}"
            )
        if explicit_map is not None:
            movie_id = explicit_map.get(key.number)
            if movie_id is None:
                raise PlanError(f"合集映射文件缺少编号 {key.number}")
            part = parts_by_id.get(movie_id)
            if part is None:
                raise PlanError(
                    f"合集映射中的 TMDB 电影 ID {movie_id} 不属于合集 {tmdb_id}"
                )
        else:
            index = key.number - 1
            if index < 0 or index >= len(parts):
                raise PlanError(f"编号 {key.number} 超出合集条目范围")
            part = parts[index]
            movie_id = int(part["id"])

        title = safe_name(str(part.get("title") or part.get("original_title") or movie_id))
        year = _extract_year(part.get("release_date"))
        base_name = safe_name(f"{title} ({year})")
        member_dir = join_remote(collection_dir, base_name)
        member_movies[member_dir] = {
            "tmdb_id": movie_id,
            "title": title,
            "year": year,
        }
        if isinstance(part.get("poster_path"), str) and part.get("poster_path"):
            member_posters[member_dir] = str(part["poster_path"])
        group_files = sorted(regular_groups[key], key=lambda x: _collision_key(str(x["full_path"])))
        names = make_unique_media_names(base_name, group_files, preserve_editions=True)
        for item, final_name in zip(group_files, names):
            planned.append(
                _planned_file_from_entry(
                    item,
                    final_name=final_name,
                    target_dir=member_dir,
                    episode_key=str(key.number),
                )
            )

    plan = Plan(
        mode="collection",
        source_root=normalize_remote_path(src_path),
        target_root=collection_dir,
        files=planned,
        warnings=warnings,
        metadata={
            "tmdb_id": tmdb_id,
            "title": collection_title,
            "poster_path": collection.get("poster_path"),
            "backdrop_path": collection.get("backdrop_path"),
            "member_posters": member_posters,
            "member_movies": member_movies,
            "mapping": explicit_map,
        },
        cleanup_files=cleanup_files,
    )
    _add_snapshot_warnings(plan)
    _canonical_root, canonical_warnings, canonical_posters = (
        _plan_canonical_batch_tree(
            [plan],
            outer_root=collection_dir,
            allow_family_boundaries=False,
            preserve_container_root=True,
        )
    )
    # The TMDB collection is a proven directory-only relation.  Its member
    # movies are identity leaves; the collection id is not a media leaf.
    plan.warnings.extend(
        warning for warning in canonical_warnings if warning not in plan.warnings
    )
    if canonical_posters:
        plan.metadata.setdefault("member_posters", {}).update(canonical_posters)
    validate_plan(alist, plan)
    return plan


def _discover_tagged_media_roots(
    alist: AListClient,
    src_path: str,
    *,
    max_depth: int = 4,
) -> list[str]:
    """Find topmost descendant directories carrying an explicit TMDB tag."""
    root = normalize_remote_path(src_path)
    found: list[str] = []

    def visit(path: str, depth: int) -> None:
        if depth > max_depth:
            return
        content = alist.try_list(path, refresh=True)
        if content is None:
            raise PlanError(f"无法读取系列子目录: {path}")
        for entry in sorted(content, key=lambda value: _collision_key(str(value.get("name") or ""))):
            name = entry.get("name")
            if not entry.get("is_dir") or not isinstance(name, str) or not name:
                continue
            child = join_remote(path, name)
            if _tmdb_hint_from_source(child) is not None:
                found.append(child)
            else:
                visit(child, depth + 1)

    visit(root, 1)
    return found


def _discover_franchise_member_roots(
    alist: AListClient,
    src_path: str,
    *,
    ignore_orphan_temp: bool = False,
) -> tuple[list[str], list[str]]:
    """Return immediate franchise members that actually contain video.

    A franchise directory is a user-owned grouping boundary.  Its immediate
    children are therefore independent works (or a work-specific bundle), not
    seasons of one arbitrarily selected TMDB show.  Explicit ``{tmdb-*}`` tags
    remain authoritative, but are no longer required.
    """
    root = normalize_remote_path(src_path)
    content = alist.try_list(root, refresh=True)
    if content is None:
        raise PlanError(f"无法读取系列父目录: {root}")
    members: list[str] = []
    skipped: list[str] = []
    for entry in sorted(
        content,
        key=lambda value: _collision_key(str(value.get("name") or "")),
    ):
        name = entry.get("name")
        if not entry.get("is_dir") or not isinstance(name, str) or not name:
            continue
        child = join_remote(root, name)
        child_files = alist.walk(
            child,
            ignore_orphan_temp=ignore_orphan_temp,
            include_bonus=True,
        )
        if any(
            Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
            for item in child_files
        ):
            members.append(child)
        else:
            skipped.append(child)

    # Some cloud providers expose a newly moved child directory before its
    # contents become visible.  A single stale empty response previously made
    # real movies look like permanent placeholders.  Retry all empty children
    # together (bounded to two refresh rounds) so a large franchise incurs a
    # fixed delay rather than one delay per directory.
    for delay in (0.2, 0.5):
        if not skipped:
            break
        time.sleep(delay)
        still_skipped: list[str] = []
        for child in skipped:
            child_files = alist.walk(
                child,
                ignore_orphan_temp=ignore_orphan_temp,
                include_bonus=True,
            )
            if any(
                Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
                for item in child_files
            ):
                members.append(child)
            else:
                still_skipped.append(child)
        skipped = still_skipped
    members.sort(key=_collision_key)
    return members, skipped


def _franchise_title_suffix_episode_overrides(
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, int], int | None]:
    """Recognize an exact ``<child title><two-digit episode>`` run.

    Asian release packs sometimes omit every separator before the episode
    number (``作品01.mp4``).  The generic parser intentionally does not treat an
    arbitrary trailing number as an episode, so recover it only inside a
    franchise child when *all* videos share the substantial immediate-child
    title and form the complete contiguous run 01..N.
    """
    videos = [
        item for item in source_files
        if Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
    ]
    if len(videos) < 3:
        return {}, None
    title_keys = list(dict.fromkeys(
        key
        for query in _franchise_member_queries(member_root)
        if len(key := _normalize_match_title(query)) >= 6
        and len(re.findall(r"[\u3400-\u9fff]", query)) >= 4
    ))
    for title_key in title_keys:
        video_numbers: list[int] = []
        for item in videos:
            stem_key = _normalize_match_title(
                Path(str(item.get("name") or "")).stem
            )
            match = re.fullmatch(rf"{re.escape(title_key)}(\d{{2,3}})", stem_key)
            if match is None:
                break
            video_numbers.append(int(match.group(1)))
        else:
            unique_numbers = sorted(set(video_numbers))
            if (
                len(unique_numbers) == len(videos)
                and unique_numbers == list(range(1, len(videos) + 1))
            ):
                overrides: dict[str, int] = {}
                for item in source_files:
                    path = str(item.get("full_path") or "")
                    if not path:
                        continue
                    stem_key = _normalize_match_title(
                        Path(str(item.get("name") or "")).stem
                    )
                    match = re.fullmatch(
                        rf"{re.escape(title_key)}(\d{{2,3}})", stem_key
                    )
                    if match is not None and 1 <= int(match.group(1)) <= len(videos):
                        overrides[path] = int(match.group(1))
                return overrides, len(videos)
    return {}, None


def _apply_franchise_title_suffix_episode_overrides(
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    overrides, _ = _franchise_title_suffix_episode_overrides(
        member_root, source_files
    )
    copied = [dict(item) for item in source_files]
    for item in copied:
        number = overrides.get(str(item.get("full_path") or ""))
        if number is None:
            continue
        item["_episode_kind_override"] = "regular"
        item["_episode_key_override"] = number
    return copied


def _franchise_member_match(
    tmdb_client: TMDBClient,
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
    *,
    target_parent: str | None = None,
) -> AutoMatch:
    hint = _tmdb_hint_from_source(member_root)
    if hint is not None:
        return _direct_tmdb_match(tmdb_client, member_root, hint)

    parent_root, name = split_remote(member_root)
    generic_additive_season = (
        "+" in unicodedata.normalize("NFKC", split_remote(parent_root)[1])
        and _is_generic_season_member_label(name)
    )
    additive_sibling_keys: set[str] = set()
    if generic_additive_season:
        parent_label = unicodedata.normalize("NFKC", split_remote(parent_root)[1])
        sibling_label = parent_label.split("+", 1)[1]
        additive_sibling_keys = {
            key
            for query in [
                sibling_label,
                *_franchise_member_queries("/" + sibling_label),
            ]
            if (key := _normalize_match_title(query))
            and _usable_release_title_query(query)
        }
    if _source_suggests_collection(member_root):
        requested_type: str | None = "collection"
    elif _has_movie_context({"name": name, "full_path": member_root}):
        requested_type = "movie"
    else:
        requested_type = _media_type_from_source_context(member_root)
        if requested_type is None and target_parent is not None:
            requested_type, _ = _media_context_from_source_and_target(
                member_root,
                target_parent,
            )

    prefer_animation = _source_is_animation_library(member_root)
    if target_parent is not None:
        _, prefer_animation = _media_context_from_source_and_target(
            member_root,
            target_parent,
        )

    episode_count = len({
        key.number
        for item in source_files
        if Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
        and (key := extract_episode_key(str(item.get("name") or ""))) is not None
        and key.kind == "regular"
        and not key.end_number
    }) or None
    if episode_count is None:
        _, episode_count = _franchise_title_suffix_episode_overrides(
            member_root, source_files
        )
    video_count = sum(
        1
        for item in source_files
        if Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
    )
    if (
        video_count == 1
        and episode_count is None
        and not re.search(r"(?:TV|电视|電視|剧集|season|第\s*\d+\s*季)", name, re.I)
    ):
        # A single unnumbered child in an explicit multi-work root may be a
        # standalone movie even when the destination category is animation.
        # Search TV and movie and retain the normal ambiguity margin instead
        # of forcing the category hint to turn it into a one-episode TV show.
        requested_type = None
    # A child containing a real run of three or more numbered episodes is a
    # TV-season bundle unless its own label explicitly says movie/film.  This
    # prevents a named OVA inside that bundle from making the entire season
    # match the OVA movie (for example five Herz disc shorts stealing the ten
    # Herz TV episodes).
    explicit_movie_member = bool(re.search(
        r"(?:剧场版|劇場版|电影|電影|movie|film)",
        name,
        re.I,
    ))
    if episode_count is not None and episode_count >= 3 and not explicit_movie_member:
        requested_type = "tv"
    queries = _franchise_member_match_queries(member_root, source_files)
    excluded_additive_sibling_ids: set[int] = set()
    if generic_additive_season:
        # Resolve the explicitly advertised work to the right of ``+`` first.
        # A generic ``第三季`` belongs to the left series, but TMDB search can
        # rank the additive sibling (for example an anime spin-off) within the
        # normal ambiguity margin.  Excluding only the independently confirmed
        # sibling identity lets the left work be evaluated on its own evidence;
        # no title is hard-coded and an ambiguous sibling remains fail-closed.
        for sibling_query in _franchise_member_queries(parent_root):
            if "+" not in unicodedata.normalize("NFKC", sibling_query):
                continue
            try:
                sibling_match, _ = auto_match_tmdb(
                    tmdb_client,
                    sibling_query,
                    media_type="tv",
                    min_confidence=0.88,
                    prefer_animation=prefer_animation,
                )
            except ScraperError:
                continue
            if sibling_match.status == "confirmed":
                excluded_additive_sibling_ids.add(sibling_match.tmdb_id)
                break
    if requested_type == "tv" and episode_count is not None and episode_count >= 3:
        # Season bundles often use only a sequel suffix in the child name;
        # retain the parent franchise title as the final TV-only lookup.
        queries = list(dict.fromkeys([
            *queries,
            *_franchise_member_queries(parent_root),
        ]))
    last_error: ScraperError | None = None
    for query in queries:
        try:
            match, _ = auto_match_tmdb(
                tmdb_client,
                query,
                media_type=requested_type,
                min_confidence=0.88,
                prefer_animation=prefer_animation,
                expected_episode_count=episode_count,
                excluded_tmdb_ids=excluded_additive_sibling_ids,
            )
            if match.status != "confirmed":
                raise PlanError(
                    f"子作品自动匹配需要人工复核: {member_root}; "
                    + ", ".join(match.decision_trace.get("blockers", []))
                )
            if generic_additive_season and additive_sibling_keys:
                candidate_title_keys = {
                    _normalize_match_title(str(title))
                    for title in [
                        match.title,
                        *(match.decision_trace.get("official_titles") or []),
                        *(match.decision_trace.get("aliases_checked") or []),
                    ]
                    if str(title).strip()
                }
                if any(
                    sibling_key in title_key
                    for sibling_key in additive_sibling_keys
                    for title_key in candidate_title_keys
                ):
                    last_error = PlanError(
                        "通用季度子目录不能匹配同级 + 右侧的附加作品: "
                        f"{member_root}; {match.media_type}/{match.tmdb_id} {match.title}"
                    )
                    continue
            if (
                requested_type == "tv"
                and episode_count is not None
                and episode_count >= 3
                and match.media_type != "tv"
            ):
                raise PlanError(
                    f"多集子目录只允许匹配 TV，拒绝整季被单个电影/OVA 候选吞并: "
                    f"{member_root}; {match.media_type}/{match.tmdb_id} {match.title}"
                )
            if (
                match.media_type == "tv"
                and episode_count is not None
                and episode_count >= 3
            ):
                # A packaging parent may advertise an additional work after
                # ``+`` (for example a main 1–9 season pack plus an anime
                # spinoff).  Title similarity alone can then select that
                # neighbouring work for an otherwise generic ``第一季`` child.
                # The child's complete contiguous episode count is hard
                # identity evidence: at least one positive official season of
                # the candidate must expose the same boundary.  A mismatch is
                # rejected before planning so cleanup items can never point at
                # a winner later discarded as an out-of-range episode.
                detail = tmdb_client.get(f"/tv/{match.tmdb_id}")
                official_counts = {
                    int(item["episode_count"])
                    for item in (detail.get("seasons") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("season_number"), int)
                    and int(item["season_number"]) > 0
                    and isinstance(item.get("episode_count"), int)
                    and int(item["episode_count"]) > 0
                }
                if official_counts and episode_count not in official_counts:
                    last_error = PlanError(
                        "多集子目录的完整连续边界与候选 TMDB 任一季度均不一致: "
                        f"{member_root}; source={episode_count}, "
                        f"{match.media_type}/{match.tmdb_id}={sorted(official_counts)}"
                    )
                    continue
            return match
        except ScraperError as exc:
            last_error = exc
    raise last_error or PlanError(f"无法识别系列子作品: {member_root}")


def _franchise_member_match_queries(
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> list[str]:
    """Build queries for one franchise child without losing its parent title.

    A two-part film is often stored as ``完整片名/01 前篇（2020）``.  The
    immediate child label is not a title and querying it alone can confidently
    match an unrelated movie.  For these generic part labels, combine the
    actual parent work title with the child part/year before consulting release
    filenames.  This is deliberately generic; no franchise title is embedded
    in the rule.
    """
    member_root = normalize_remote_path(member_root)
    parent_root, child_name = split_remote(member_root)
    direct_queries = _franchise_member_queries(member_root)
    generic_part_pattern = re.compile(
        r"\s*(?:\d{1,3}\s*[.)、_-]?\s*)?"
        r"(?:前篇|后篇|後篇|上篇|下篇|前编|后编|前編|後編|"
        r"part\s*[ivx\d]+|movie\s*[ivx\d]+)"
        r"(?:\s*[（(]?(?:19|20)\d{2}[)）]?)?\s*",
        flags=re.I,
    )
    generic_label = next(
        (
            query
            for query in reversed(direct_queries)
            if generic_part_pattern.fullmatch(unicodedata.normalize("NFKC", query))
        ),
        "",
    )
    generic_part = bool(generic_label)
    contextual_queries: list[str] = []
    generic_additive_season = False
    if generic_part:
        parent_queries = _franchise_member_queries(parent_root)
        child_query = generic_label
        for parent_query in parent_queries:
            parent_base = re.sub(
                r"\s*[（(]?(?:19|20)\d{2}\s*[-–—~～至到]\s*"
                r"(?:19|20)\d{2}[)）]?\s*"
                r"(?:前篇|上篇|前编|前編).*(?:后篇|後篇|下篇|后编|後編).*$",
                " ",
                parent_query,
                flags=re.I,
            )
            parent_base = re.sub(
                r"\s*(?:前篇|上篇|前编|前編)\s*[+＋/&、和与及]\s*"
                r"(?:后篇|後篇|下篇|后编|後編).*$",
                " ",
                parent_base,
                flags=re.I,
            )
            parent_base = re.sub(
                r"^\s*\d{1,3}\s*[.)、_-]?\s*",
                "",
                parent_base,
            )
            parent_base = re.sub(r"\s+", " ", parent_base).strip(" -_")
            if parent_base and not re.match(r"^(?:19|20)\d{2}\)?", parent_base):
                contextual_queries.append(f"{parent_base} {child_query}".strip())
    else:
        clean_parent_anchor = _clean_franchise_root_label(parent_root)
        anchor_key = _normalize_match_title(clean_parent_anchor)
        if len(anchor_key) >= 6:
            for child_query in direct_queries[:2]:
                child_key = _normalize_match_title(child_query)
                # Correct one accidental inserted character in the repeated
                # franchise prefix only when deleting it makes the immediate
                # child begin with the independently cleaned parent anchor.
                # The remainder (the member subtitle) is preserved verbatim.
                for index in range(min(len(anchor_key) + 1, len(child_key))):
                    corrected = child_key[:index] + child_key[index + 1:]
                    if corrected.startswith(anchor_key) and len(corrected) > len(anchor_key):
                        contextual_queries.append(corrected)
                        break
        parent_queries = _franchise_member_queries(parent_root)
        # If a pack label explicitly joins an additional work after ``+``, a
        # generic season child belongs to the left-hand series unless its own
        # title says otherwise.  Keep the full parent queries as fallbacks,
        # but add the left identity without the sibling-work suffix first.
        if re.search(r"(?:season|第\s*[一二三四五六七八九十0-9]+\s*季)", child_name, re.I):
            additive_parent_anchors = list(dict.fromkeys(
                anchor
                for query in parent_queries
                for anchor in [re.split(r"[+＋]", query, maxsplit=1)[0].strip()]
                if _usable_release_title_query(anchor)
            ))
            contextual_queries.extend(additive_parent_anchors)
            contextual_queries.extend(
                f"{anchor} {child_query}".strip()
                for anchor in additive_parent_anchors
                for child_query in direct_queries[:2]
            )
            generic_additive_season = (
                "+" in unicodedata.normalize("NFKC", split_remote(parent_root)[1])
                and _is_generic_season_member_label(child_name)
            )
        for parent_query in parent_queries[:2]:
            parent_base = re.sub(
                r"(?:全系列|系列合集|大合集|合集包|合集|franchise)|"
                r"\d+\s*[-–—~～至到]\s*\d+\s*(?:部|季|集)",
                " ",
                parent_query,
                flags=re.I,
            )
            parent_base = re.sub(
                r"(?:[48]k)?超清(?:2160p|1080p|720p)?|"
                r"(?:2160p|1080p|720p)?收藏版|典藏版|完整版",
                " ",
                parent_base,
                flags=re.I,
            )
            parent_base = re.sub(r"\s+", " ", parent_base).strip(" -_")
            for child_query in direct_queries[:2]:
                parent_key = _normalize_match_title(parent_base)
                child_key = _normalize_match_title(child_query)
                if (
                    len(parent_key) >= 4
                    and len(child_key) >= 3
                    and parent_key not in child_key
                ):
                    contextual_queries.append(
                        f"{parent_base} {child_query}".strip()
                    )

    release_queries: list[str] = []
    for item in source_files:
        if Path(str(item.get("name") or "")).suffix.lower() not in VIDEO_EXTS:
            continue
        release_queries.extend(_movie_queries_from_item(item))
        if len(release_queries) >= 8:
            break

    # A generic label must never be searched by itself.  It is only useful
    # after the real parent title has been attached.
    yearless_direct = [
        re.sub(
            r"\s*[（(]?(?:19|20)\d{2}(?:[.\-/]\d{1,2})?[)）]?\s*",
            " ",
            query,
        ).strip()
        for query in direct_queries
    ]
    yearless_contextual = [
        re.sub(
            r"\s*[（(]?(?:19|20)\d{2}(?:[.\-/]\d{1,2})?[)）]?\s*",
            " ",
            query,
        ).strip()
        for query in contextual_queries
    ]
    # Some TV releases name a season as ``Series 2 -Arc Title-`` while TMDB
    # stores all seasons under the base series record. Keep the full release
    # query first, then add a narrowly bounded base-title fallback; an ordinary
    # trailing number or an unpaired subtitle is not stripped.
    season_arc_bases = [
        re.sub(
            r"\s+(?:\d{1,2}|最终季|第\s*[一二三四五六七八九十]{1,3}\s*季)"
            r"\s*[-–—]\s*[^-–—]{2,}[-–—]\s*$",
            "",
            query,
        ).strip()
        for query in yearless_direct
    ]
    season_arc_bases = [
        query
        for query, original in zip(season_arc_bases, yearless_direct)
        if query and query != original
    ]
    ordered = (
        [*contextual_queries, *yearless_contextual, *release_queries]
        if generic_part
        else [
            *contextual_queries,
            *yearless_contextual,
            *direct_queries,
            *yearless_direct,
            *season_arc_bases,
            *release_queries,
        ]
        if generic_additive_season
        else [
            *direct_queries,
            *contextual_queries,
            *yearless_direct,
            *yearless_contextual,
            *season_arc_bases,
            *release_queries,
        ]
    )
    def usable_title_query(query: str) -> bool:
        return _usable_release_title_query(query)

    return list(dict.fromkeys(
        query for query in ordered if query and usable_title_query(query)
    ))


def _is_generic_season_member_label(label: str) -> bool:
    """Return true only when a child label carries no work title of its own."""
    normalized = unicodedata.normalize("NFKC", label)
    if _season_from_source("/" + normalized) is None:
        return False
    residual = re.sub(
        r"(?:season|s)\s*0*\d{1,3}|"
        r"\d{1,3}(?:st|nd|rd|th)\s+season|"
        r"第\s*(?:\d{1,3}|[一二三四五六七八九十]{1,3})\s*季",
        " ",
        normalized,
        flags=re.I,
    )
    residual = re.sub(r"[（(]\s*(?:19|20)\d{2}\s*[）)]", " ", residual)
    residual = re.sub(r"(?:全|共)?\s*\d{1,4}\s*集", " ", residual)
    residual = re.sub(
        r"\b(?:4k|8k|2160p|1080p|720p|bluray|blu-?ray|web-?dl|"
        r"webrip|x26[45]|hevc|av1|10bit)\b",
        " ",
        residual,
        flags=re.I,
    )
    residual = re.sub(
        r"(?:内封|内嵌|外挂|硬字幕|软字幕|"
        r"简英(?:双语)?|繁英(?:双语)?|中英(?:双语)?|"
        r"日英(?:双语)?|简繁(?:双语)?|双语|多语|"
        r"简中|繁中|中字|字幕)",
        " ",
        residual,
        flags=re.I,
    )
    residual = re.sub(r"[\s._\-+()（）\[\]]+", " ", residual).strip()
    return not _usable_release_title_query(residual)


def _franchise_member_official_season(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> int:
    """Use a season only when folder year and exact episode count both prove it."""
    try:
        show = tmdb_client.get(f"/tv/{tmdb_id}")
    except ApiError:
        show = {}
    title_matched_season = _season_from_series_variant(
        split_remote(member_root)[1], show
    )
    if title_matched_season is not None:
        matched_meta = next(
            (
                item for item in (show.get("seasons") or [])
                if isinstance(item, Mapping)
                and item.get("season_number") == title_matched_season
            ),
            None,
        )
        video_numbers = {
            int(item.get("_episode_key_override", key.number))
            for item in source_files
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        }
        if (
            isinstance(matched_meta, Mapping)
            and matched_meta.get("episode_count") == len(video_numbers)
            and video_numbers == set(range(1, len(video_numbers) + 1))
        ):
            return title_matched_season
    years = set(re.findall(r"(?:19|20)\d{2}", split_remote(member_root)[1]))
    episode_numbers = {
        key.number
        for item in source_files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and (key := extract_episode_key(str(item.get("name", "")))) is not None
        and key.kind == "regular"
        and not key.end_number
    }
    if len(years) != 1 or not episode_numbers:
        return 1
    if not show:
        return 1
    matches = [
        int(item["season_number"])
        for item in (show.get("seasons") or [])
        if isinstance(item, Mapping)
        and isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and int(item["season_number"]) > 0
        and item.get("episode_count") == len(episode_numbers)
        and str(item.get("air_date") or "")[:4] in years
    ]
    return matches[0] if len(matches) == 1 else 1


def _franchise_member_special_overrides(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Map an OVA child only from an exact official Season 00 run."""
    copied = [dict(item) for item in source_files]
    video_numbers = sorted({
        key.number
        for item in copied
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and (key := extract_episode_key(str(item.get("name", "")))) is not None
        and key.kind == "regular"
        and not key.end_number
    })
    if not video_numbers or video_numbers != list(range(1, len(video_numbers) + 1)):
        return copied
    try:
        specials = tmdb_client.get(f"/tv/{tmdb_id}/season/0").get("episodes") or []
    except ApiError:
        return copied
    label_queries = [
        _normalize_match_title(query)
        for query in _franchise_member_queries(member_root)
        if len(_normalize_match_title(query)) >= 5
    ]
    candidates = [
        item for item in specials
        if isinstance(item, Mapping)
        and isinstance(item.get("episode_number"), int)
        and not isinstance(item.get("episode_number"), bool)
        and any(
            query in _normalize_match_title(str(item.get("name") or ""))
            for query in label_queries
        )
    ]
    if len(candidates) != len(video_numbers):
        years = set(re.findall(r"(?:19|20)\d{2}", split_remote(member_root)[1]))
        candidates = [
            item for item in specials
            if isinstance(item, Mapping)
            and isinstance(item.get("episode_number"), int)
            and not isinstance(item.get("episode_number"), bool)
            and len(years) == 1
            and str(item.get("air_date") or "")[:4] in years
        ]
    target_numbers = sorted(int(item["episode_number"]) for item in candidates)
    if (
        len(target_numbers) != len(video_numbers)
        or target_numbers != list(range(target_numbers[0], target_numbers[-1] + 1))
    ):
        return copied
    number_map = dict(zip(video_numbers, target_numbers))
    for item in copied:
        key = extract_episode_key(str(item.get("name", "")))
        if key is None or key.number not in number_map or key.end_number:
            continue
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = number_map[key.number]
    return copied


def _attach_unique_movie_subtitles(
    movie_groups: dict[int, list[dict[str, Any]]],
    unresolved: Sequence[dict[str, Any]],
    *,
    tmdb_client: TMDBClient | None = None,
    prefer_animation: bool = False,
) -> list[dict[str, Any]]:
    """Attach proven backup companions to exactly one identified movie.

    Besides subtitles, a lower-resolution OVA/movie video may sit under a
    different wrapper tree from its already identified 4K counterpart.  Such
    a video is attached only when it has explicit movie/special context and
    its release queries identify exactly one existing movie group.
    """
    group_keys: dict[int, set[str]] = {}
    group_video_dirs: dict[int, set[str]] = {}
    group_video_stems: dict[int, set[str]] = {}
    for tmdb_id, members in movie_groups.items():
        group_keys[tmdb_id] = {
            key
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            # The first query is the concrete release title.  Later fallbacks
            # can be only the franchise name (for example ``Mushishi``), which
            # must not merge a different TV special into the one known movie.
            for query in _movie_queries_from_item(item)[:1]
            if len(key := _normalize_match_title(query)) >= 8
        }
        group_video_dirs[tmdb_id] = {
            _collision_key(split_remote(normalize_remote_path(str(item["full_path"])))[0])
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and item.get("full_path")
        }
        group_video_stems[tmdb_id] = {
            _batch_subtitle_release_stem(str(item.get("name") or ""))
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        }
    remaining: list[dict[str, Any]] = []
    for item in unresolved:
        suffix = Path(str(item.get("name", ""))).suffix.lower()
        is_subtitle = suffix in SUBTITLE_EXTS
        is_contextual_video = suffix in VIDEO_EXTS and (
            _has_movie_context(item) or _has_special_context(item)
        ) and item.get("_episode_kind_override") != "special"
        if not (is_subtitle or is_contextual_video):
            remaining.append(item)
            continue
        subtitle_keys = {
            key
            for query in _movie_queries_from_item(item)[:1]
            if len(key := _normalize_match_title(query)) >= 8
        }
        item_dir = _collision_key(
            split_remote(normalize_remote_path(str(item.get("full_path", "/"))))[0]
        )
        same_directory_matches = [
            tmdb_id
            for tmdb_id, directories in group_video_dirs.items()
            if item_dir in directories
        ]
        exact_release_matches = [
            tmdb_id
            for tmdb_id, stems in group_video_stems.items()
            if _batch_subtitle_release_stem(str(item.get("name") or "")) in stems
        ]
        if is_subtitle and len(exact_release_matches) == 1:
            movie_groups[exact_release_matches[0]].append(item)
            continue
        # A numbered backup subtitle usually belongs to a TV episode.  Do not
        # let fuzzy movie-title matching swallow an entire TV subtitle set
        # merely because a related movie is the only movie group in the same
        # franchise.  A subtitle may still accompany a movie when it shares
        # the movie video's exact directory or carries explicit movie context.
        if (
            is_subtitle
            and extract_episode_key(str(item.get("name", ""))) is not None
            and not same_directory_matches
            and not _has_movie_context(item)
        ):
            remaining.append(item)
            continue
        matches = (
            same_directory_matches
            if len(same_directory_matches) == 1
            else [
                tmdb_id
                for tmdb_id, keys in group_keys.items()
                if any(
                    subtitle_key in video_key or video_key in subtitle_key
                    for subtitle_key in subtitle_keys
                    for video_key in keys
                )
            ]
        )
        if not matches and tmdb_client is not None:
            resolved_ids: set[int] = set()
            for query in _movie_queries_from_item(item):
                try:
                    match, _ = auto_match_tmdb(
                        tmdb_client,
                        query,
                        media_type="movie",
                        min_confidence=0.88,
                        prefer_animation=prefer_animation,
                    )
                except ScraperError:
                    continue
                if (
                    match.status == "confirmed"
                    and match.media_type == "movie"
                    and (
                        not is_contextual_video
                        or _specific_movie_query_agrees_with_match(query, match)
                    )
                ):
                    resolved_ids.add(match.tmdb_id)
            matches = sorted(resolved_ids & set(movie_groups))
        if len(matches) == 1:
            movie_groups[matches[0]].append(item)
        else:
            remaining.append(item)
    return remaining


def _attach_fractional_feature_by_ass_title_to_movie_groups(
    alist: AListClient,
    tmdb_client: TMDBClient,
    movie_groups: dict[int, list[dict[str, Any]]],
    special_files: Sequence[dict[str, Any]],
    all_files: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Route a titleless ``N.5`` feature using its tiny ASS-text sidecar.

    Some disc packs number a separately catalogued film as ``20.5`` and keep
    its real title only in an exported ``.ass.txt`` companion.  Read only the
    bounded sidecar and only the ASS ``title`` style line; ordinary dialogue
    is not identity evidence.  The title must uniquely match a movie identity
    already confirmed elsewhere in the same batch.
    """
    if not movie_groups:
        return list(special_files), []
    official_keys: dict[int, set[str]] = {}
    for tmdb_id in movie_groups:
        try:
            detail = tmdb_client.get(f"/movie/{tmdb_id}")
        except ApiError:
            continue
        official_keys[tmdb_id] = {
            key
            for field in ("title", "original_title")
            if len(key := _normalize_match_title(str(detail.get(field) or ""))) >= 3
        }
    readers = getattr(alist, "read_file_bytes", None)
    if not callable(readers):
        return list(special_files), []
    by_dir: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in all_files:
        path = normalize_remote_path(str(item.get("full_path", "")))
        directory, _ = split_remote(path)
        by_dir[_collision_key(directory)].append(item)
    remaining: list[dict[str, Any]] = []
    warnings: list[str] = []
    for video in special_files:
        key = extract_episode_key(str(video.get("name", "")))
        if key is None or key.kind != "fractional":
            remaining.append(video)
            continue
        video_path = normalize_remote_path(str(video.get("full_path", "")))
        video_dir, video_name = split_remote(video_path)
        sidecars = [
            item for item in by_dir.get(_collision_key(video_dir), [])
            if str(item.get("name", "")).casefold().endswith(".ass.txt")
            and extract_episode_key(str(item.get("name", ""))) == key
            and 0 < int(item.get("size") or 0) <= 2 * 1024 * 1024
        ]
        title_keys: set[str] = set()
        for sidecar in sidecars:
            try:
                payload = readers(
                    str(sidecar.get("full_path", "")), max_bytes=2 * 1024 * 1024
                )
            except (ApiError, OSError, ValueError):
                continue
            text = ""
            for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "gb18030"):
                try:
                    text = payload.decode(encoding)
                    break
                except UnicodeError:
                    continue
            for line in text.splitlines():
                if not line.casefold().startswith("dialogue:"):
                    continue
                fields = line.split(",", 9)
                if len(fields) != 10 or fields[3].strip().casefold() != "title":
                    continue
                value = re.sub(r"\{[^}]*\}", "", fields[9]).replace(r"\N", " ")
                normalized = _normalize_match_title(value)
                if len(normalized) >= 3:
                    title_keys.add(normalized)
        matches = {
            tmdb_id
            for tmdb_id, candidates in official_keys.items()
            if any(
                source in official or official in source
                for source in title_keys
                for official in candidates
            )
        }
        if len(matches) != 1:
            remaining.append(video)
            continue
        tmdb_id = next(iter(matches))
        movie_groups[tmdb_id].append(video)
        warnings.append(
            f"{key.display} 的 ASS 文本伴侣 title 样式唯一标记为已确认的 "
            f"TMDB movie/{tmdb_id}；已按同一电影版本参与清晰度去重"
        )
    return remaining, warnings


def build_tagged_collection_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    tmdb_id: int,
    ignore_orphan_temp: bool = False,
) -> Plan:
    """Plan an existing collection whose member filenames already carry ids."""
    collection = tmdb_client.get(f"/collection/{tmdb_id}")
    title = safe_name(str(collection.get("name") or tmdb_id))
    desired_root = join_remote(parent_path, title)
    collection_root = _preserve_equivalent_source_root(src_path, desired_root)
    scanned_entries = alist.walk(src_path, ignore_orphan_temp=ignore_orphan_temp)
    scanned = _filter_media(scanned_entries)
    cleanup_files = _planned_cleanup_files(scanned_entries)
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    untagged: list[str] = []
    for item in scanned:
        hint = _tmdb_hint_from_source(str(item.get("full_path") or item.get("name") or ""))
        if hint is None:
            untagged.append(str(item.get("full_path") or item.get("name") or ""))
        else:
            groups[hint].append(dict(item))
    if untagged:
        _raise_unparsed_media(untagged, "合集成员 TMDB 编号")
    if not groups:
        raise PlanError("未找到带 {tmdb-编号} 的合集媒体文件")

    member_plans: list[Plan] = []
    for member_id, member_files in sorted(groups.items()):
        match = _direct_tmdb_match(
            tmdb_client,
            str(member_files[0].get("full_path") or member_files[0].get("name") or ""),
            member_id,
        )
        if match.media_type != "movie":
            raise PlanError(
                f"合集成员 {member_id} 被识别为 {match.media_type}，不能当作电影整理"
            )
        member_plans.append(
            build_movie_plan(
                alist,
                tmdb_client,
                src_path=src_path,
                parent_path=collection_root,
                tmdb_id=member_id,
                ignore_orphan_temp=ignore_orphan_temp,
                source_files=member_files,
                defer_validation=True,
            )
        )

    member_movies = {
        plan.target_root: {
            "tmdb_id": plan.metadata["tmdb_id"],
            "title": plan.metadata["title"],
            "year": plan.metadata["year"],
        }
        for plan in member_plans
    }
    member_posters = {
        plan.target_root: str(plan.metadata["poster_path"])
        for plan in member_plans
        if isinstance(plan.metadata.get("poster_path"), str) and plan.metadata.get("poster_path")
    }
    if isinstance(collection.get("poster_path"), str) and collection.get("poster_path"):
        member_posters[collection_root] = str(collection["poster_path"])
    canonical_root, canonical_warnings, canonical_posters = (
        _plan_canonical_batch_tree(
            member_plans,
            outer_root=collection_root,
            allow_family_boundaries=False,
        )
    )
    collection_root = canonical_root
    member_movies = {
        plan.target_root: {
            "tmdb_id": plan.metadata["tmdb_id"],
            "title": plan.metadata["title"],
            "year": plan.metadata["year"],
        }
        for plan in member_plans
    }
    member_posters = {
        plan.target_root: str(plan.metadata["poster_path"])
        for plan in member_plans
        if isinstance(plan.metadata.get("poster_path"), str)
        and plan.metadata.get("poster_path")
    }
    member_posters.update(canonical_posters)
    if isinstance(collection.get("poster_path"), str) and collection.get("poster_path"):
        member_posters[collection_root] = str(collection["poster_path"])
    result = Plan(
        mode="collection",
        source_root=normalize_remote_path(src_path),
        target_root=collection_root,
        files=[item for plan in member_plans for item in plan.files],
        problem_files=[item for plan in member_plans for item in plan.problem_files],
        warnings=list(
            dict.fromkeys(
                [
                    f"已按文件名中的 TMDB 编号识别 {len(member_plans)} 部合集电影",
                    *(
                        [f"确认执行后将删除 {len(cleanup_files)} 个明确无用的片头片尾/广告文件"]
                        if cleanup_files
                        else []
                    ),
                    *(warning for plan in member_plans for warning in plan.warnings),
                    *canonical_warnings,
                ]
            )
        ),
        metadata={
            "tmdb_id": tmdb_id,
            "title": title,
            "poster_path": collection.get("poster_path"),
            "backdrop_path": collection.get("backdrop_path"),
            "member_posters": member_posters,
            "member_movies": member_movies,
            "mapping": None,
        },
        cleanup_files=cleanup_files,
    )
    validate_plan(alist, result)
    return result


def _batch_subtitle_release_stem(name: str) -> str:
    """Return a release basename without a trailing subtitle-language tag."""
    stem = unicodedata.normalize("NFKC", Path(name).stem)
    # The video and separately exported subtitle can credit slightly
    # different collaborating groups (``[A&B]`` versus ``[A]``) while the
    # complete release payload after that credit remains byte-for-byte named.
    # A leading bracket containing letters is release-group metadata; episode
    # brackets such as ``[01]`` are deliberately retained.
    stem = re.sub(r"^(?:\[[^\]]*[A-Za-z][^\]]*\]\s*)+", "", stem)
    stem = re.sub(
        r"(?:[ ._-]+(?:zh[ ._-]?(?:cn|tw)|chs|cht|sc|tc|gb|big5|"
        r"simplified|traditional|简体?|繁体?|簡體?|繁體?))+$",
        "",
        stem,
        flags=re.IGNORECASE,
    )
    return _collision_key(stem)


def _attach_unique_batch_subtitle_companions(
    source_root: str,
    source_files: Sequence[Mapping[str, Any]],
    subplans: Sequence[Plan],
) -> tuple[list[PlannedFile], list[PlannedProblem]]:
    """Route subtitle-only batch folders by an exact, unique video basename.

    Franchise packs often put movie/spin-off subtitles in a shared backup
    directory outside the member directory that contains the video.  Member
    discovery quite correctly refuses to treat that subtitle-only directory as
    a work, but silently skipping it loses a provable companion.  Only an exact
    release basename (after a trailing language tag is removed) and one unique
    planned video destination are accepted.  Missing or ambiguous evidence is
    surfaced as a problem and the subtitle remains at source.
    """
    root = normalize_remote_path(source_root)
    handled_paths = {
        _collision_key(item.source_path)
        for plan in subplans
        for item in (*plan.files, *plan.cleanup_files, *plan.problem_files)
    }
    videos_by_stem: dict[str, list[PlannedFile]] = defaultdict(list)
    occupied_targets: set[str] = set()
    for plan in subplans:
        for planned in plan.files:
            occupied_targets.add(
                _collision_key(join_remote(planned.target_dir, planned.final_name))
            )
            if planned.media_kind != "video":
                continue
            videos_by_stem[_batch_subtitle_release_stem(planned.original_name)].append(
                planned
            )

    attached: list[PlannedFile] = []
    problems: list[PlannedProblem] = []
    for raw in source_files:
        name = str(raw.get("name") or "")
        full_path = normalize_remote_path(str(raw.get("full_path") or ""))
        if (
            raw.get("is_dir")
            or Path(name).suffix.lower() not in SUBTITLE_EXTS
            or not full_path
            or not _path_is_within(full_path, root)
            or _collision_key(full_path) in handled_paths
        ):
            continue
        destinations = {
            (
                _collision_key(video.target_dir),
                _collision_key(Path(video.final_name).stem),
            ): video
            for video in videos_by_stem.get(
                _batch_subtitle_release_stem(name), []
            )
        }
        if len(destinations) != 1:
            reason = (
                "系列批次中没有与该字幕同发行 basename 的已确认视频；"
                "保留原位待人工确认"
                if not destinations
                else "系列批次中有多个同发行 basename 视频，无法唯一确认字幕归属；"
                "保留原位待人工确认"
            )
            problems.append(PlannedProblem(source_path=full_path, reason=reason))
            continue
        video = next(iter(destinations.values()))
        extension = Path(name).suffix.lower()
        language = subtitle_language(name)
        language_suffix = f".{language}" if language else ""
        final_name = _limit_filename(
            f"{Path(video.final_name).stem}{language_suffix}{extension}"
        )
        target_path = join_remote(video.target_dir, final_name)
        target_key = _collision_key(target_path)
        if target_key in occupied_targets:
            problems.append(PlannedProblem(
                source_path=full_path,
                reason=(
                    "已确认视频的目标字幕槽已有文件，不能覆盖或猜测替换；"
                    "保留原位待人工确认"
                ),
                target_path=target_path,
            ))
            continue
        planned = _planned_file_from_entry(
            raw,
            final_name=final_name,
            target_dir=video.target_dir,
            episode_key=video.episode_key,
        )
        attached.append(planned)
        occupied_targets.add(target_key)
        handled_paths.add(_collision_key(full_path))
    return attached, problems


def build_batch_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    ignore_orphan_temp: bool = False,
    _target_root: str | None = None,
) -> Plan:
    """Combine independently identified descendants into one reviewable plan."""
    source_root = normalize_remote_path(src_path)
    target_root = (
        normalize_remote_path(_target_root)
        if _target_root is not None
        else join_remote(parent_path, _clean_franchise_root_label(source_root))
    )
    member_roots, skipped_empty = _discover_franchise_member_roots(
        alist,
        source_root,
        ignore_orphan_temp=ignore_orphan_temp,
    )
    flat_member_files: dict[str, list[dict[str, Any]]] = {}
    if len(member_roots) < 2:
        source_files = alist.walk(
            source_root,
            ignore_orphan_temp=ignore_orphan_temp,
            include_bonus=True,
        )
        flat_videos = [
            dict(item)
            for item in source_files
            if Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
            and str(item.get("full_path") or "")
            and cleanup_reason(str(item.get("name") or "")) is None
            and not is_sample(str(item.get("name") or ""))
        ]
        # Some two-film releases place both independently titled movies in
        # the parent directory. Treat each complete filename as a synthetic
        # member only inside an explicit multi-work/batch root. Every member
        # still has to independently confirm as a different TMDB movie below.
        if len(flat_videos) >= 2 and not member_roots:
            member_roots = [str(item["full_path"]) for item in flat_videos]
            flat_member_files = {
                str(item["full_path"]): [item] for item in flat_videos
            }
            skipped_empty = []
        else:
            raise PlanError("系列父目录中未找到至少两个包含视频的独立作品目录")

    subplans: list[Plan] = []
    inferred_members = 0
    flat_movie_ids: set[int] = set()
    primary_identity_candidates: set[WorkIdentity] = set()
    for member_root in member_roots:
        synthetic_flat_member = member_root in flat_member_files
        source_files = (
            flat_member_files[member_root]
            if synthetic_flat_member
            else alist.walk(
                member_root,
                ignore_orphan_temp=ignore_orphan_temp,
                include_bonus=True,
            )
        )
        try:
            nested_members = [] if synthetic_flat_member else _discover_franchise_member_roots(
                alist,
                member_root,
                ignore_orphan_temp=ignore_orphan_temp,
            )[0]
            generic_bucket = bool(re.fullmatch(
                r"(?:其它|其他|misc|miscellaneous|附加|额外)",
                split_remote(member_root)[1].strip(),
                flags=re.I,
            ))
            if len(nested_members) >= 2 and (
                _source_suggests_collection(member_root) or generic_bucket
            ):
                # The source child is usually only a packaging bucket such as
                # ``06 剧场版…4K`` or ``空之境界 1-10部 4K``.  Its wording is
                # not a library identity and must not leak into the target.
                # The recursive plan independently identifies every member;
                # The canonical work-tree planner then creates clean,
                # evidence-based family roots from confirmed metadata.
                nested_target_root = target_root
                subplans.append(
                    build_batch_plan(
                        alist,
                        tmdb_client,
                        src_path=member_root,
                        parent_path=parent_path,
                        ignore_orphan_temp=ignore_orphan_temp,
                        _target_root=nested_target_root,
                    )
                )
                inferred_members += len(nested_members)
                continue
            match = _franchise_member_match(
                tmdb_client,
                member_root,
                source_files,
                target_parent=parent_path,
            )
            if (
                match.media_type == "tv"
                and _is_generic_season_member_label(split_remote(member_root)[1])
            ):
                primary_identity_candidates.add(
                    WorkIdentity("tmdb.tv", match.tmdb_id)
                )
            if _tmdb_hint_from_source(member_root) is None:
                inferred_members += 1
            if synthetic_flat_member:
                if match.media_type != "movie":
                    raise PlanError(
                        "扁平多作品目录中的每个视频都必须独立确认成电影: "
                        f"{member_root}; 实际为 {match.media_type}/{match.tmdb_id}"
                    )
                if match.tmdb_id in flat_movie_ids:
                    raise PlanError(
                        "扁平多作品目录中的两个视频匹配到同一 TMDB 电影，"
                        f"拒绝按不同作品拆分: movie/{match.tmdb_id}"
                    )
                flat_movie_ids.add(match.tmdb_id)
            if match.media_type == "tv":
                source_files = _apply_franchise_title_suffix_episode_overrides(
                    member_root, source_files
                )
                member_season = _franchise_member_official_season(
                    tmdb_client,
                    match.tmdb_id,
                    member_root,
                    source_files,
                )
                source_files = _franchise_member_special_overrides(
                    tmdb_client,
                    match.tmdb_id,
                    member_root,
                    source_files,
                )
                subplans.append(
                    build_tv_plan_smart(
                        auto_episode_mode=True,
                        alist=alist,
                        tmdb_client=tmdb_client,
                        src_path=member_root,
                        parent_path=target_root,
                        tmdb_id=match.tmdb_id,
                        season=member_season,
                        absolute=False,
                        prefer_simplified=True,
                        allow_unmapped=False,
                        ignore_orphan_temp=ignore_orphan_temp,
                        episode_map_path=None,
                        episode_group_id=None,
                        source_files=source_files,
                        _proven_member_season=member_season != 1,
                    )
                )
            elif match.media_type == "movie":
                subplans.append(
                    build_movie_plan(
                        alist,
                        tmdb_client,
                        src_path=source_root if synthetic_flat_member else member_root,
                        parent_path=target_root,
                        tmdb_id=match.tmdb_id,
                        ignore_orphan_temp=ignore_orphan_temp,
                        source_files=source_files,
                    )
                )
            else:
                subplans.append(
                    build_tagged_collection_plan(
                        alist, tmdb_client,
                        src_path=member_root,
                        parent_path=target_root,
                        tmdb_id=match.tmdb_id,
                        ignore_orphan_temp=ignore_orphan_temp,
                    )
                    if _tmdb_hint_from_source(member_root) is not None
                    else build_collection_plan(
                        alist, tmdb_client,
                        src_path=member_root,
                        parent_path=target_root,
                        tmdb_id=match.tmdb_id,
                        mapping_path=None,
                        allow_index_mapping=True,
                        ignore_orphan_temp=ignore_orphan_temp,
                    )
                )
        except ScraperError as exc:
            raise PlanError(f"子作品规划失败 {member_root}: {exc}") from exc

    if len(primary_identity_candidates) > 1:
        raise PlanError(
            "系列容器中的通用季度目录指向多个不同 TMDB 身份，"
            "无法唯一确定 canonical 主作品"
        )
    root_identity = next(iter(primary_identity_candidates), None)
    movie_quality_warnings = _dedupe_confirmed_batch_movies(
        subplans,
        outer_root=target_root,
    )
    target_root, subseries_warnings, subseries_posters = _plan_canonical_batch_tree(
        subplans,
        outer_root=target_root,
        root_identity=root_identity,
    )
    batch_source_files = alist.walk(
        source_root,
        ignore_orphan_temp=ignore_orphan_temp,
        include_bonus=True,
    )
    attached_batch_subtitles, batch_subtitle_problems = (
        _attach_unique_batch_subtitle_companions(
            source_root,
            batch_source_files,
            subplans,
        )
    )

    member_tv: dict[str, dict[str, Any]] = {}
    member_movies: dict[str, dict[str, Any]] = {}
    member_posters: dict[str, str] = {}
    for plan in subplans:
        if plan.mode in {"tv", "mixed"}:
            series_root = str(plan.metadata.get("series_root") or plan.target_root)
            member_tv[series_root] = {
                key: plan.metadata.get(key)
                for key in (
                    "tmdb_id", "title", "original_title", "year", "poster_path", "backdrop_path",
                    "season_posters",
                )
            }
        if plan.mode == "movie":
            member_movies[plan.target_root] = {
                "tmdb_id": plan.metadata["tmdb_id"],
                "title": plan.metadata["title"],
                "year": plan.metadata["year"],
            }
            if (
                isinstance(plan.metadata.get("poster_path"), str)
                and plan.metadata.get("poster_path")
            ):
                member_posters[plan.target_root] = str(plan.metadata["poster_path"])
        if plan.mode in {"collection", "mixed", "batch"}:
            raw_tv = plan.metadata.get("member_tv")
            raw_movies = plan.metadata.get("member_movies")
            raw_posters = plan.metadata.get("member_posters")
            if isinstance(raw_tv, Mapping):
                member_tv.update({
                    str(key): dict(value)
                    for key, value in raw_tv.items()
                    if isinstance(value, Mapping)
                })
            if isinstance(raw_movies, Mapping):
                member_movies.update({str(key): dict(value) for key, value in raw_movies.items()})
            if isinstance(raw_posters, Mapping):
                member_posters.update({str(key): str(value) for key, value in raw_posters.items()})
    member_posters.update(subseries_posters)

    result = Plan(
        mode="batch",
        source_root=source_root,
        target_root=target_root,
        files=[
            *(item for plan in subplans for item in plan.files),
            *attached_batch_subtitles,
        ],
        cleanup_files=_dedupe_cleanup_files(
            item for plan in subplans for item in plan.cleanup_files
        ),
        problem_files=[
            *(item for plan in subplans for item in plan.problem_files),
            *batch_subtitle_problems,
        ],
        warnings=list(
            dict.fromkeys(
                [
                    (
                        f"已识别 {len(subplans)} 个独立电影文件；每个文件均已独立"
                        "确认不同 TMDB 电影身份，将保留系列父目录并合并审核"
                        if flat_member_files
                        else f"已识别 {len(subplans)} 个独立作品目录，将保留系列父目录并合并审核"
                    ),
                    *(
                        [
                            f"其中 {inferred_members} 个未标注 TMDB 编号的"
                            + ("电影文件" if flat_member_files else "子作品")
                            + "已独立检索确认"
                        ]
                        if inferred_members
                        else []
                    ),
                    *subseries_warnings,
                    *movie_quality_warnings,
                    *(
                        [
                            f"{len(attached_batch_subtitles)} 个独立字幕目录文件已通过"
                            "唯一同发行 basename 跟随已确认视频"
                        ]
                        if attached_batch_subtitles
                        else []
                    ),
                    *(
                        [
                            f"{len(batch_subtitle_problems)} 个独立字幕目录文件缺少"
                            "唯一视频证据，将保留原位待人工确认"
                        ]
                        if batch_subtitle_problems
                        else []
                    ),
                    *(
                        [
                            f"{sum(plan.mode == 'movie' for plan in subplans)} 部系列电影"
                            "已保留独立 TMDB 身份和各自独立电影目录"
                        ]
                        if any(plan.mode == "movie" for plan in subplans)
                        else []
                    ),
                    *(
                        [
                            "已跳过无视频的空占位目录: "
                            + ", ".join(split_remote(path)[1] for path in skipped_empty)
                        ]
                        if skipped_empty
                        else []
                    ),
                    *(warning for plan in subplans for warning in plan.warnings),
                ]
            )
        ),
        metadata={
            "title": split_remote(source_root)[1],
            "member_tv": member_tv,
            "member_movies": member_movies,
            "member_posters": member_posters,
        },
    )
    validate_plan(alist, result)
    return result


def _planned_companion_key(target_dir: str, final_name: str) -> tuple[str, str]:
    """Return the destination-level identity shared by a video and subtitles."""
    stem = Path(final_name).stem
    stem = re.sub(
        r"\.(?:zh-CN|zh-TW|en|ja)(?:\.\d+)*$|\.subtitle(?:\.\d+|\d*)$",
        "",
        stem,
        flags=re.IGNORECASE,
    )
    stem = re.sub(r" - v\d+$", "", stem, flags=re.IGNORECASE)
    return _collision_key(normalize_remote_path(target_dir)), _collision_key(stem)


def _demote_unpaired_subtitles(alist: AListClient, plan: Plan) -> None:
    """Keep destination-orphan subtitles at source and expose the exact reason.

    TMDB having an official episode is not sufficient evidence to move a lone
    subtitle.  It must accompany a video in this plan or a video that already
    exists at the exact normalized destination basename.
    """
    planned_video_keys = {
        _planned_companion_key(item.target_dir, item.final_name)
        for item in plan.files
        if item.media_kind == "video"
    }
    subtitle_items = [item for item in plan.files if item.media_kind == "subtitle"]
    target_dirs = {
        normalize_remote_path(item.target_dir)
        for item in subtitle_items
        if _planned_companion_key(item.target_dir, item.final_name)
        not in planned_video_keys
    }
    existing_video_keys: set[tuple[str, str]] = set()
    for target_dir in sorted(target_dirs):
        for entry in alist.try_list(target_dir, refresh=True) or []:
            name = str(entry.get("name", ""))
            if entry.get("is_dir") or Path(name).suffix.lower() not in VIDEO_EXTS:
                continue
            existing_video_keys.add(_planned_companion_key(target_dir, name))

    raw_evidence = plan.scan_report.get("subtitle_evidence")
    raw_evidence = raw_evidence if isinstance(raw_evidence, Mapping) else {}
    confirmed_missing = {
        _collision_key(normalize_remote_path(str(row.get("video_path"))))
        for row in (raw_evidence.get("confirmed_missing_chinese") or [])
        if isinstance(row, Mapping) and isinstance(row.get("video_path"), str)
    }
    candidate_evidence = {
        _collision_key(normalize_remote_path(str(row.get("subtitle_path")))): row
        for row in (raw_evidence.get("validated_candidates") or [])
        if isinstance(row, Mapping) and isinstance(row.get("subtitle_path"), str)
    }

    deferred_rows = plan.scan_report.setdefault("deferred_subtitles", [])
    if not isinstance(deferred_rows, list):
        raise PlanError("scan_report.deferred_subtitles 必须是数组")
    deferred_paths = {
        _collision_key(str(row.get("source_path")))
        for row in deferred_rows if isinstance(row, Mapping)
    }

    retained_cleanup: list[PlannedCleanup] = []
    for cleanup in plan.cleanup_files:
        if Path(cleanup.original_name).suffix.lower() not in SUBTITLE_EXTS:
            retained_cleanup.append(cleanup)
            continue
        key = _collision_key(cleanup.source_path)
        if key not in deferred_paths:
            deferred_rows.append({
                "source_path": cleanup.source_path,
                "action": "defer_until_exact_video_subtitle_closure",
                "reason": "initial_scrape_must_not_delete_subtitle",
            })
            deferred_paths.add(key)
    plan.cleanup_files = retained_cleanup

    retained_problems: list[PlannedProblem] = []
    for problem in plan.problem_files:
        if Path(problem.source_path).suffix.lower() not in SUBTITLE_EXTS:
            retained_problems.append(problem)
            continue
        key = _collision_key(problem.source_path)
        if key not in deferred_paths:
            deferred_rows.append({
                "source_path": problem.source_path,
                **(
                    {"planned_target_path": problem.target_path}
                    if problem.target_path else {}
                ),
                "action": "defer_until_exact_video_subtitle_closure",
                "reason": "initial_plan_subtitle_problem_deferred",
            })
            deferred_paths.add(key)
    plan.problem_files = retained_problems

    retained: list[PlannedFile] = []
    for item in plan.files:
        if item.media_kind != "subtitle":
            retained.append(item)
            continue
        key = _planned_companion_key(item.target_dir, item.final_name)
        paired_video_paths = {
            normalize_remote_path(join_remote(video.target_dir, video.final_name))
            for video in plan.files
            if video.media_kind == "video"
            and _planned_companion_key(video.target_dir, video.final_name) == key
        }
        if not paired_video_paths and key in existing_video_keys:
            paired_video_paths = {
                normalize_remote_path(str(row.get("video_path")))
                for row in (raw_evidence.get("confirmed_missing_chinese") or [])
                if isinstance(row, Mapping) and isinstance(row.get("video_path"), str)
                and _planned_companion_key(
                    split_remote(str(row["video_path"]))[0],
                    split_remote(str(row["video_path"]))[1],
                ) == key
            }
        candidate = candidate_evidence.get(_collision_key(item.source_path))
        content_proved = bool(
            isinstance(candidate, Mapping)
            and candidate.get("contains_simplified_chinese") is True
            and re.fullmatch(r"[0-9a-f]{64}", str(candidate.get("sha256") or ""))
        )
        if Path(item.original_name).suffix.lower() == ".mks":
            content_proved = bool(
                content_proved
                and re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(candidate.get("stream_probe_sha256") or "")
                    if isinstance(candidate, Mapping) else "",
                )
            )
        exact_missing = bool(paired_video_paths) and all(
            _collision_key(path) in confirmed_missing for path in paired_video_paths
        )
        if key in planned_video_keys and exact_missing and content_proved:
            retained.append(item)
            continue
        if key in planned_video_keys or key in existing_video_keys:
            retained.append(item)
            source_key = _collision_key(item.source_path)
            if source_key not in deferred_paths:
                deferred_rows.append({
                    "source_path": item.source_path,
                    "planned_target_path": join_remote(item.target_dir, item.final_name),
                    "action": "defer_until_exact_video_subtitle_closure",
                    "reason": (
                        "mks_stream_witness_required"
                        if Path(item.original_name).suffix.lower() == ".mks"
                        else "confirmed_missing_chinese_and_content_witness_required"
                    ),
                })
                deferred_paths.add(source_key)
            continue
        source_key = _collision_key(item.source_path)
        if source_key not in deferred_paths:
            deferred_rows.append({
                "source_path": item.source_path,
                "planned_target_path": join_remote(item.target_dir, item.final_name),
                "action": "defer_until_exact_video_subtitle_closure",
                "reason": "no_exact_video_pair_for_initial_scrape",
            })
            deferred_paths.add(source_key)
    plan.files = retained
    known_subtitle_sources = {
        _collision_key(item.source_path)
        for item in plan.files if item.media_kind == "subtitle"
    } | deferred_paths
    for row in alist.walk(plan.source_root):
        source_path = str(row.get("full_path") or "")
        if (
            not source_path
            or Path(source_path).suffix.lower() not in SUBTITLE_EXTS
            or _collision_key(source_path) in known_subtitle_sources
        ):
            continue
        deferred_rows.append({
            "source_path": normalize_remote_path(source_path),
            "action": "defer_until_exact_video_subtitle_closure",
            "reason": "unselected_source_subtitle",
        })
        known_subtitle_sources.add(_collision_key(source_path))


def _deferred_subtitle_source_keys(plan: Plan) -> set[str]:
    rows = plan.scan_report.get("deferred_subtitles")
    if not isinstance(rows, list):
        return set()
    return {
        _collision_key(normalize_remote_path(str(row["source_path"])))
        for row in rows
        if isinstance(row, Mapping) and isinstance(row.get("source_path"), str)
    }


def _forward_plan_files(plan: Plan) -> list[PlannedFile]:
    deferred = _deferred_subtitle_source_keys(plan)
    return [
        item for item in plan.files
        if _collision_key(item.source_path) not in deferred
    ]


def _planned_movie_tmdb_id(plan: Plan, item: PlannedFile) -> int | None:
    """Return a movie identity only when target metadata proves it."""
    candidates: list[tuple[int, int]] = []
    if plan.mode == "movie":
        tmdb_id = plan.metadata.get("tmdb_id")
        if (
            isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and tmdb_id > 0
            and _path_is_within(item.target_dir, plan.target_root)
        ):
            candidates.append((len(normalize_remote_path(plan.target_root)), tmdb_id))
    raw_movies = plan.metadata.get("member_movies")
    if isinstance(raw_movies, Mapping):
        target_path = normalize_remote_path(
            join_remote(item.target_dir, item.final_name)
        )
        for raw_root, identity in raw_movies.items():
            if not isinstance(identity, Mapping):
                continue
            tmdb_id = identity.get("tmdb_id")
            root = normalize_remote_path(str(raw_root))
            if (
                isinstance(tmdb_id, int)
                and not isinstance(tmdb_id, bool)
                and tmdb_id > 0
                and (
                    _collision_key(target_path) == _collision_key(root)
                    or _path_is_within(item.target_dir, root)
                )
            ):
                candidates.append((len(root), tmdb_id))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _plan_has_movie_tmdb_id(plan: Plan, tmdb_id: int) -> bool:
    """Return whether batch metadata contains the confirmed movie identity."""
    if plan.mode == "movie" and plan.metadata.get("tmdb_id") == tmdb_id:
        return True
    raw_movies = plan.metadata.get("member_movies")
    return isinstance(raw_movies, Mapping) and any(
        isinstance(identity, Mapping) and identity.get("tmdb_id") == tmdb_id
        for identity in raw_movies.values()
    )


def _same_concrete_movie_release(
    loser_name: str,
    loser_path: str,
    winner: PlannedFile,
) -> bool:
    """Prove two cross-root copies name the same concrete movie release.

    Batch canonical-root rebasing can intentionally move the retained file out
    of the metadata root that first proved its identity.  In that case target
    containment cannot be replayed during final validation.  Require both the
    still-present plan-level TMDB identity and substantial overlap between the
    concrete release queries instead of trusting a generic franchise label.
    """
    loser_queries = {
        key
        for query in _movie_queries_from_item(
            {"name": loser_name, "full_path": loser_path}
        )[:2]
        if len(key := _normalize_match_title(query)) >= 8
    }
    winner_queries = {
        key
        for query in _movie_queries_from_item(
            {"name": winner.original_name, "full_path": winner.source_path}
        )[:2]
        if len(key := _normalize_match_title(query)) >= 8
    }
    return any(
        left in right or right in left
        for left in loser_queries
        for right in winner_queries
    )


LIBRARY_REMEDIATION_CLEANUP_REASON = "合并系列后清理旧子条目的 NFO 与图稿"
LIBRARY_REMEDIATION_CATEGORY_ROOTS = (
    "/quark/影视/番剧",
    "/quark/影视/美剧",
    "/quark/影视/电影",
)


def _library_remediation_evidence(plan: Plan) -> Mapping[str, Any]:
    raw = plan.decision_trace.get("library_remediation")
    if not isinstance(raw, Mapping):
        raise PlanError("库内修复计划缺少 library_remediation 审计证据")
    if raw.get("version") != 1:
        raise PlanError("库内修复计划的审计证据版本不受支持")
    required_text = ("audit_report", "scope", "reviewer", "reviewed_at")
    for key in required_text:
        if not isinstance(raw.get(key), str) or not str(raw[key]).strip():
            raise PlanError(f"库内修复计划缺少审计字段: {key}")
    if not any(notice.requires_review for notice in plan.notices):
        raise PlanError("库内修复计划必须包含 requires_review 审核提示")
    return raw


def _validate_library_remediation_route(plan: Plan) -> Mapping[str, Any]:
    evidence = _library_remediation_evidence(plan)
    source_root = normalize_remote_path(plan.source_root).rstrip("/") or "/"
    target_root = normalize_remote_path(plan.target_root).rstrip("/") or "/"
    scope = normalize_remote_path(str(evidence["scope"])).rstrip("/") or "/"
    category_root = next(
        (
            root
            for root in LIBRARY_REMEDIATION_CATEGORY_ROOTS
            if _path_is_within(scope, root) and _collision_key(scope) != _collision_key(root)
        ),
        None,
    )
    if category_root is None:
        raise PlanError("库内修复 scope 必须是媒体分类目录下的具体作品目录")
    if not _path_is_within(source_root, scope) or not _path_is_within(target_root, scope):
        raise PlanError("库内修复的 source_root/target_root 超出已审核 scope")

    allowed_cleanup_roots_raw = evidence.get("allowed_cleanup_roots", [])
    if not isinstance(allowed_cleanup_roots_raw, list):
        raise PlanError("库内修复 allowed_cleanup_roots 必须是数组")
    allowed_cleanup_roots: list[str] = []
    for raw_root in allowed_cleanup_roots_raw:
        if not isinstance(raw_root, str):
            raise PlanError("库内修复 allowed_cleanup_roots 含无效路径")
        root = normalize_remote_path(raw_root).rstrip("/") or "/"
        if not _path_is_within(root, scope):
            raise PlanError("库内修复清理目录超出已审核 scope")
        allowed_cleanup_roots.append(root)

    for item in plan.cleanup_files:
        path = normalize_remote_path(item.source_path)
        suffix = Path(item.original_name).suffix.lower()
        if (
            item.reason != LIBRARY_REMEDIATION_CLEANUP_REASON
            or suffix not in {".nfo", ".jpg", ".jpeg", ".png", ".webp"}
            or not any(_path_is_within(path, root) for root in allowed_cleanup_roots)
        ):
            raise PlanError(f"库内修复包含未审核的清理项: {path}")
    return evidence


def validate_plan(
    alist: AListClient,
    plan: Plan,
    *,
    allow_library_remediation: bool = False,
) -> None:
    remediation = (
        _validate_library_remediation_route(plan)
        if allow_library_remediation
        else None
    )
    _demote_unpaired_subtitles(alist, plan)
    if not plan.files:
        if plan.problem_files:
            preview = "；".join(
                f"{item.source_path}（{item.reason}）"
                for item in plan.problem_files[:5]
            )
            raise PlanError(f"没有可安全执行的媒体；问题文件: {preview}")
        if remediation is None or not (planned_artwork(plan) or planned_nfos(plan)):
            raise PlanError("操作计划为空")

    source_root = normalize_remote_path(plan.source_root).rstrip("/") or "/"
    target_root = normalize_remote_path(plan.target_root).rstrip("/") or "/"
    if remediation is None:
        try:
            placement_for(source_root, target_root)
        except ValueError as exc:
            raise PlanError(str(exc)) from exc
    source_root_folded = _collision_key(source_root)
    target_root_folded = _collision_key(target_root)
    if remediation is None and (
        target_root_folded == source_root_folded
        or target_root_folded.startswith(source_root_folded.rstrip("/") + "/")
        or source_root_folded.startswith(target_root_folded.rstrip("/") + "/")
    ):
        raise PlanError(
            "源目录与目标目录发生重叠，可能导致旧文件夹与 Season 目录混在一起。"
            "请把 --parent 设置为与源目录完全分离的父目录。"
        )

    tv_series_roots: set[str] = set()
    if plan.mode == "mixed":
        series_root = plan.metadata.get("series_root")
        if isinstance(series_root, str):
            tv_series_roots.add(_collision_key(normalize_remote_path(series_root)))
    elif plan.mode == "batch":
        member_tv = plan.metadata.get("member_tv")
        if isinstance(member_tv, Mapping):
            tv_series_roots.update(
                _collision_key(normalize_remote_path(str(series_root)))
                for series_root in member_tv
            )
    for item in plan.files:
        if (
            item.media_kind == "video"
            and _planned_movie_tmdb_id(plan, item) is not None
            and _collision_key(normalize_remote_path(item.target_dir)) in tv_series_roots
        ):
            raise PlanError(
                "独立电影不得直接放入电视剧作品根目录："
                f"{join_remote(item.target_dir, item.final_name)}"
            )

    canonical_path_spellings: dict[str, str] = {}

    def register_path_spelling(path: str, label: str) -> None:
        key = _collision_key(path)
        previous = canonical_path_spellings.get(key)
        if previous is not None and previous != path:
            raise PlanError(
                f"计划包含 Unicode/大小写等价但拼写不同的{label}: {previous!r} 与 {path!r}"
            )
        canonical_path_spellings[key] = path

    register_path_spelling(source_root, "路径")
    register_path_spelling(target_root, "路径")

    source_paths: set[str] = set()
    source_items: dict[str, PlannedFile] = {}
    source_items_by_location: dict[tuple[str, str], PlannedFile] = {}
    final_destinations: dict[tuple[str, str], str] = {}
    per_source_final: dict[tuple[str, str], str] = {}

    for item in plan.files:
        source_path = normalize_remote_path(item.source_path)
        source_dir = normalize_remote_path(item.source_dir)
        target_dir = normalize_remote_path(item.target_dir)
        expected_dir, expected_name = split_remote(source_path)
        register_path_spelling(source_dir, "源目录")
        register_path_spelling(target_dir, "目标目录")
        original_name = _validate_remote_basename(item.original_name)
        final_name = _validate_remote_basename(item.final_name)

        if source_dir != expected_dir or original_name != expected_name:
            raise PlanError(
                f"计划中的源路径、源目录或原文件名不一致: {item.source_path}"
            )
        source_prefix = source_root_folded.rstrip("/") + "/"
        if _collision_key(source_path) != source_root_folded and not _collision_key(source_path).startswith(
            source_prefix
        ):
            raise PlanError(f"计划中的源文件不在 source_root 下: {source_path}")
        target_prefix = target_root_folded.rstrip("/") + "/"
        if _collision_key(target_dir) != target_root_folded and not _collision_key(target_dir).startswith(
            target_prefix
        ):
            raise PlanError(f"计划中的目标目录不在 target_root 下: {target_dir}")
        if remediation is None:
            try:
                placement_for(source_root, target_dir)
            except ValueError as exc:
                raise PlanError(str(exc)) from exc
        elif not _path_is_within(source_path, str(remediation["scope"])) or not _path_is_within(
            target_dir, str(remediation["scope"])
        ):
            raise PlanError("库内修复文件超出已审核 scope")

        source_key = _collision_key(source_path)
        if source_key in source_paths:
            raise PlanError(f"源文件重复出现在计划中: {source_path}")
        source_paths.add(source_key)
        source_items[source_key] = item

        location_key = (_collision_key(source_dir), _collision_key(original_name))
        if location_key in source_items_by_location:
            raise PlanError(f"源目录中存在大小写不安全的重复名称: {source_dir}/{original_name}")
        source_items_by_location[location_key] = item

        destination_key = (_collision_key(target_dir), _collision_key(final_name))
        existing_source = final_destinations.get(destination_key)
        if existing_source is not None:
            raise PlanError(
                f"目标文件名冲突: {target_dir}/{final_name}; "
                f"来源为 {existing_source} 与 {source_path}"
            )
        final_destinations[destination_key] = source_path

        rename_key = (_collision_key(source_dir), _collision_key(final_name))
        existing_source = per_source_final.get(rename_key)
        if existing_source is not None:
            raise PlanError(f"同一源目录重命名冲突: {final_name}")
        per_source_final[rename_key] = source_path

    contextual_cleanup_keys = _contextual_theme_cleanup_paths([
        {
            "name": item.original_name,
            "full_path": item.source_path,
            "is_dir": False,
        }
        for item in [*plan.files, *plan.cleanup_files]
    ])
    for item in plan.cleanup_files:
        source_path = normalize_remote_path(item.source_path)
        source_dir = normalize_remote_path(item.source_dir)
        expected_dir, expected_name = split_remote(source_path)
        register_path_spelling(source_dir, "清理目录")
        original_name = _validate_remote_basename(item.original_name)
        if source_dir != expected_dir or original_name != expected_name:
            raise PlanError(f"计划清理项的路径、目录或文件名不一致: {item.source_path}")
        source_prefix = source_root_folded.rstrip("/") + "/"
        if _collision_key(source_path) != source_root_folded and not _collision_key(source_path).startswith(
            source_prefix
        ):
            raise PlanError(f"计划清理项不在 source_root 下: {source_path}")
        source_key = _collision_key(source_path)
        if source_key in source_paths:
            raise PlanError(f"清理文件与媒体计划重复: {source_path}")
        safe_generated_cleanup = remediation is not None or (
            cleanup_reason(original_name) == item.reason
            or _contextual_cleanup_reason(
                {"name": original_name, "full_path": source_path}
            ) == item.reason
        )
        if (
            not safe_generated_cleanup
            and item.reason == "经特典目录与同集正片交叉确认的片头/片尾视频"
            and source_key in contextual_cleanup_keys
        ):
            safe_generated_cleanup = True
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 电影 movie/"
        ):
            identity_match = re.match(
                r"^同一 TMDB 电影 movie/(\d+) 已有更高清晰度版本 ",
                item.reason,
            )
            cleanup_tmdb_id = (
                int(identity_match.group(1)) if identity_match else None
            )
            loser_rank = video_resolution_rank(
                {"name": original_name, "full_path": source_path}
            )
            loser_variant = _batch_movie_source_variant_key(
                original_name,
                source_path,
            )
            safe_generated_cleanup = (
                cleanup_tmdb_id is not None
                and _plan_has_movie_tmdb_id(plan, cleanup_tmdb_id)
                and any(
                    item.reason
                    == _lower_resolution_movie_cleanup_reason(
                        winner.source_path,
                        cleanup_tmdb_id,
                    )
                    and winner.media_kind == "video"
                    and (
                        _planned_movie_tmdb_id(plan, winner) == cleanup_tmdb_id
                        or (
                            _planned_movie_tmdb_id(plan, winner) is None
                            and _same_concrete_movie_release(
                                original_name,
                                source_path,
                                winner,
                            )
                        )
                    )
                    and _batch_movie_variant_key(winner) == loser_variant
                    and (
                        winner_rank := video_resolution_rank(
                            {
                                "name": winner.original_name,
                                "full_path": winner.source_path,
                            }
                        )
                    ) > loser_rank
                    and (loser_rank > 0 or winner_rank >= 2160)
                    for winner in plan.files
                )
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号已有更高清晰度版本 "
        ):
            loser_edition = edition_tag(original_name) or edition_tag(source_path)
            loser_rank = video_resolution_rank(
                {"name": original_name, "full_path": source_path}
            )
            safe_generated_cleanup = any(
                item.reason == _lower_resolution_cleanup_reason(winner.source_path)
                and winner.media_kind == "video"
                and (
                    edition_tag(winner.original_name)
                    or edition_tag(winner.source_path)
                ) == loser_edition
                and (
                    winner_rank := video_resolution_rank(
                        {
                            "name": winner.original_name,
                            "full_path": winner.source_path,
                        }
                    )
                ) > loser_rank
                and (loser_rank > 0 or winner_rank >= 2160)
                for winner in plan.files
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号的更高清晰度版本已有对应字幕 "
        ):
            loser_rank = video_resolution_rank(
                {"name": original_name, "full_path": source_path}
            )
            loser_key = extract_episode_key(original_name)
            safe_generated_cleanup = any(
                item.reason
                == _lower_resolution_subtitle_cleanup_reason(winner.source_path)
                and winner.media_kind == "subtitle"
                and Path(winner.original_name).suffix.lower()
                == Path(original_name).suffix.lower()
                and loser_key is not None
                and extract_episode_key(winner.original_name) == loser_key
                and (
                    winner_rank := video_resolution_rank(
                        {
                            "name": winner.original_name,
                            "full_path": winner.source_path,
                        }
                    )
                ) > loser_rank
                and (loser_rank > 0 or winner_rank >= 2160)
                for winner in plan.files
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号已有同清晰度的内封/软字幕版本 "
        ):
            loser = {"name": original_name, "full_path": source_path}
            loser_edition = edition_tag(original_name) or edition_tag(source_path)
            loser_resolution = video_resolution_rank(loser)
            loser_presentation = subtitle_presentation_rank(loser)
            safe_generated_cleanup = any(
                item.reason == _burned_subtitle_cleanup_reason(winner.source_path)
                and winner.media_kind == "video"
                and (
                    edition_tag(winner.original_name)
                    or edition_tag(winner.source_path)
                ) == loser_edition
                and loser_resolution > 0
                and loser_resolution == video_resolution_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and loser_presentation < subtitle_presentation_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                for winner in plan.files
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号已有同清晰度但文件更完整的版本 "
        ):
            loser = {"name": original_name, "full_path": source_path}
            loser_edition = edition_tag(original_name) or edition_tag(source_path)
            loser_resolution = video_resolution_rank(loser)
            loser_presentation = subtitle_presentation_rank(loser)
            safe_generated_cleanup = any(
                item.reason == _same_resolution_cleanup_reason(winner.source_path)
                and winner.media_kind == "video"
                and (
                    edition_tag(winner.original_name)
                    or edition_tag(winner.source_path)
                ) == loser_edition
                and loser_resolution == video_resolution_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and loser_presentation
                == subtitle_presentation_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and item.source_size is not None
                and winner.source_size is not None
                and winner.source_size > item.source_size > 0
                for winner in plan.files
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号已有同清晰度同字幕形态的简体中文字幕版本 "
        ):
            loser = {"name": original_name, "full_path": source_path}
            loser_edition = edition_tag(original_name) or edition_tag(source_path)
            loser_resolution = video_resolution_rank(loser)
            loser_presentation = subtitle_presentation_rank(loser)
            safe_generated_cleanup = any(
                item.reason
                == _traditional_language_cleanup_reason(winner.source_path)
                and winner.media_kind == "video"
                and (
                    edition_tag(winner.original_name)
                    or edition_tag(winner.source_path)
                ) == loser_edition
                and loser_resolution
                == video_resolution_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and loser_presentation
                == subtitle_presentation_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and is_traditional_sub(source_path)
                and is_simplified_sub(winner.source_path)
                for winner in plan.files
            )
        if not safe_generated_cleanup:
            raise PlanError(f"清理文件不再符合安全规则，拒绝删除: {source_path}")
        source_paths.add(source_key)

    # 两阶段重命名发生在源目录中。提前阻止未纳入计划的文件或目录占用最终名称。
    source_listings: dict[str, list[dict[str, Any]]] = {}
    for source_dir in sorted({normalize_remote_path(item.source_dir) for item in plan.files}):
        content = alist.try_list(source_dir, refresh=True)
        if content is None:
            raise PlanError(f"无法读取源目录: {source_dir}")
        source_listings[source_dir] = content

    for item in plan.files:
        source_dir = normalize_remote_path(item.source_dir)
        final_folded = _collision_key(item.final_name)
        occupants = [
            entry
            for entry in source_listings[source_dir]
            if isinstance(entry.get("name"), str)
            and _collision_key(str(entry["name"])) == final_folded
        ]
        for occupant in occupants:
            occupant_name = str(occupant["name"])
            occupying_item = source_items_by_location.get(
                (_collision_key(source_dir), _collision_key(occupant_name))
            )
            if occupying_item is item:
                continue
            if occupying_item is not None and occupying_item.requires_rename:
                continue
            kind = "目录" if occupant.get("is_dir") else "文件"
            raise PlanError(
                f"源目录已有未被安全移开的同名{kind}: {source_dir}/{occupant_name}"
            )

    # 目标目录存在时，检查所有同名条目（包括目录）。计划内会先改为临时名的文件可放行。
    # 视频还必须检查“同规范 basename，不同扩展名”。扩展名不是
    # 内容身份：``Show - S01E01.mkv`` 与 ``Show - S01E01.mp4``
    # 依然是同一官方集。过去只检查完整文件名，会将后者静默搬入
    # 已有前者的媒体库。在没有完成跨库 ffprobe/字幕/画质比较前，
    # 安全行为是拒绝执行，而不是制造双版本。
    target_dirs = sorted({normalize_remote_path(item.target_dir) for item in plan.files})
    for target_dir in target_dirs:
        content = alist.try_list(target_dir, refresh=True)
        if content is None:
            continue
        existing_videos_by_companion: dict[tuple[str, str], list[Mapping[str, Any]]] = (
            defaultdict(list)
        )
        for entry in content:
            name = entry.get("name")
            if (
                not entry.get("is_dir")
                and isinstance(name, str)
                and Path(name).suffix.lower() in VIDEO_EXTS
            ):
                existing_videos_by_companion[
                    _planned_companion_key(target_dir, name)
                ].append(entry)
        for item in plan.files:
            if _collision_key(normalize_remote_path(item.target_dir)) != _collision_key(target_dir):
                continue
            for entry in content:
                name = entry.get("name")
                if not isinstance(name, str) or _collision_key(name) != _collision_key(item.final_name):
                    continue
                current_final_path = join_remote(target_dir, name)
                if _collision_key(current_final_path) == _collision_key(normalize_remote_path(item.source_path)):
                    continue
                occupying_item = source_items.get(_collision_key(current_final_path))
                if (
                    occupying_item is not None
                    and not entry.get("is_dir")
                    and occupying_item.requires_rename
                ):
                    continue
                kind = "目录" if entry.get("is_dir") else "文件"
                raise PlanError(f"目标目录已存在同名{kind}: {current_final_path}")
            if item.media_kind != "video":
                continue
            for entry in existing_videos_by_companion.get(
                _planned_companion_key(target_dir, item.final_name), []
            ):
                name = str(entry["name"])
                if _collision_key(name) == _collision_key(item.final_name):
                    continue
                current_path = join_remote(target_dir, name)
                if _collision_key(current_path) == _collision_key(
                    normalize_remote_path(item.source_path)
                ):
                    continue
                occupying_item = source_items.get(_collision_key(current_path))
                if occupying_item is not None and occupying_item.requires_rename:
                    continue
                raise PlanError(
                    "目标目录已存在同集不同扩展名视频，未完成跨库质量校验，"
                    f"拒绝生成重复版本: {current_path} 与 "
                    f"{join_remote(target_dir, item.final_name)}"
                )


def validate_source_state(
    alist: AListClient,
    plan: Plan,
    *,
    require_snapshot: bool = True,
    ignore_modified: bool = False,
    include_cleanup: bool = True,
) -> None:
    """执行前确认源文件名称与计划快照均未变化。"""
    by_dir: dict[str, list[PlannedFile | PlannedCleanup]] = defaultdict(list)
    cleanup_files = plan.cleanup_files if include_cleanup else []
    for item in [*plan.files, *cleanup_files]:
        by_dir[normalize_remote_path(item.source_dir)].append(item)

    for source_dir, items in by_dir.items():
        content = alist.try_list(source_dir, refresh=True)
        if content is None:
            raise PlanError(f"执行前无法读取源目录: {source_dir}")
        entries_by_key: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for entry in content:
            name = entry.get("name")
            if entry.get("is_dir") or not isinstance(name, str):
                continue
            entries_by_key[_collision_key(name)].append(entry)

        for item in items:
            matches = entries_by_key.get(_collision_key(item.original_name), [])
            if not matches:
                raise PlanError(
                    f"源目录内容在计划生成后发生变化，缺少文件: "
                    f"{source_dir}/{item.original_name}"
                )
            if len(matches) != 1:
                raise PlanError(
                    f"源目录存在 Unicode/大小写等价的多个文件，无法安全确认来源: "
                    f"{source_dir}/{item.original_name}"
                )
            actual = matches[0]
            actual_name = str(actual.get("name"))
            if actual_name != item.original_name:
                raise PlanError(
                    f"源文件名称在计划生成后发生变化: "
                    f"{source_dir}/{item.original_name} → {actual_name}"
                )

            snapshot_values = (
                (item.source_size, item.source_hash)
                if ignore_modified
                else (item.source_size, item.source_modified, item.source_hash)
            )
            snapshot_present = any(value is not None for value in snapshot_values)
            if require_snapshot and not snapshot_present:
                raise PlanError(
                    f"计划缺少源文件快照，拒绝执行: {item.source_path}。"
                    "请使用当前版本重新生成计划。"
                )

            actual_size = _entry_size_value(actual)
            actual_modified = _entry_modified_value(actual)
            actual_hash = _entry_hash_value(actual)
            comparisons = [
                ("size", item.source_size, actual_size),
                ("hash", item.source_hash, actual_hash),
            ]
            if not ignore_modified:
                comparisons.insert(
                    1, ("modified", item.source_modified, actual_modified)
                )
            changed = [
                f"{field}: {expected!r} → {observed!r}"
                for field, expected, observed in comparisons
                if expected is not None and expected != observed
            ]
            if changed:
                raise PlanError(
                    f"源文件在计划生成后发生变化，拒绝执行: {item.source_path}; "
                    + "; ".join(changed)
                )


# ---------------------------------------------------------------------------
# 计划执行与回滚
# ---------------------------------------------------------------------------


def _ensure_target_dirs(
    alist: AListClient,
    plan: Plan,
    journal: ExecutionJournal,
    journal_path: Path,
    created: list[str],
) -> None:
    directories = sorted(
        {plan.target_root, *(item.target_dir for item in plan.files)},
        key=lambda path: path.count("/"),
    )
    for directory in directories:
        existed = alist.try_list(directory, refresh=True) is not None
        record = _append_pending(
            journal,
            journal_path,
            "mkdir",
            "",
            directory,
            "existing" if existed else "create",
        )
        try:
            alist.mkdir(directory)
        except (Exception, KeyboardInterrupt):
            # 若响应在服务端成功后丢失，目录实况可帮助后续回滚。
            if not existed:
                try:
                    if alist.try_list(directory, refresh=True) is not None:
                        created.append(directory)
                        _mark_record(
                            journal, journal_path, record, "uncertain", "directory now exists"
                        )
                except Exception:
                    pass
            raise
        if not existed:
            created.append(directory)
            visible = False
            last_visibility_error: Exception | None = None
            for delay in (0.0, 0.15, 0.35, 0.75, 1.5):
                if delay:
                    time.sleep(delay)
                try:
                    if alist.try_list(directory, refresh=True) is not None:
                        visible = True
                        break
                except Exception as exc:
                    last_visibility_error = exc
            if not visible:
                message = "directory not visible after create"
                if last_visibility_error is not None:
                    message += f": {last_visibility_error}"
                _mark_record(journal, journal_path, record, "uncertain", message)
                raise ApiError(f"AList 新建目录后暂不可见: {directory}")
        _mark_record(
            journal, journal_path, record, "ok", "existing" if existed else "created"
        )


def _temporary_name(original_name: str) -> str:
    ext = Path(original_name).suffix
    digest = hashlib.sha256(original_name.encode("utf-8", errors="surrogatepass")).hexdigest()[:16]
    return f".scraper-tmp-{digest}-{uuid.uuid4().hex[:12]}{ext}"


def _recovery_name(item: PlannedFile) -> str:
    ext = Path(item.original_name).suffix
    digest = hashlib.sha256(item.source_path.encode("utf-8")).hexdigest()[:16]
    return f".scraper-recover-{digest}-{uuid.uuid4().hex[:12]}{ext}"


def _recovery_prefix(item: PlannedFile) -> str:
    digest = hashlib.sha256(item.source_path.encode("utf-8")).hexdigest()[:16]
    return f".scraper-recover-{digest}-"


def _append_pending(
    journal: ExecutionJournal,
    journal_path: Path,
    action: str,
    source: str,
    target: str,
    message: str = "",
) -> ExecutionRecord:
    record = ExecutionRecord(action, source, target, "pending", message)
    journal.records.append(record)
    journal.save(journal_path)
    return record


def _acquire_remote_lock(
    alist: AListClient,
    plan: Plan,
    journal: ExecutionJournal,
    journal_path: Path,
    lock_root: str,
    desired_root: str,
    scope: str,
) -> str:
    lock_root = normalize_remote_path(lock_root)
    desired_root = normalize_remote_path(desired_root)
    scope_key = _remote_lock_scope_key(lock_root, desired_root)
    entries = alist.try_list(lock_root, refresh=True)
    if entries is None:
        if lock_root == "/":
            entries = []
        else:
            raise PlanError(f"无法读取{scope}锁目录: {lock_root}")
    existing = [
        str(entry["name"])
        for entry in entries
        if isinstance(entry.get("name"), str) and is_scraper_lock(str(entry["name"]))
    ]
    conflicting = [
        name for name in existing
        if (key := _remote_lock_scope_from_name(name)) is None
        or key == "all"
        or scope_key == "all"
        or key == scope_key
    ]
    if conflicting:
        raise PlanError(
            f"{scope}目录已有整理锁，可能存在并发任务或未恢复任务: "
            + ", ".join(join_remote(lock_root, name) for name in conflicting)
        )

    lock_name = (
        f"{LOCK_PREFIX}v2-{scope_key}-{plan_sha256(plan)[:16]}-"
        f"{uuid.uuid4().hex[:12]}.json"
    )
    lock_path = join_remote(lock_root, lock_name)
    payload = _canonical_json_bytes(
        {
            "version": __version__,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "plan_sha256": plan_sha256(plan),
            "journal": str(journal_path),
            "scope": scope,
            "lock_root": lock_root,
        }
    )
    record = _append_pending(
        journal, journal_path, "acquire-lock", "", lock_path
    )
    try:
        alist.upload_bytes(lock_path, payload, "application/vnd.scraper-lock+json")
    except Exception:
        names = _directory_file_names(alist, lock_root)
        if not _collision_presence(names, lock_name):
            raise
    matches: list[str] = []
    conflicting_locks: list[str] = []
    # Some AList-backed providers acknowledge upload before a refreshed list
    # exposes the new object. Reconcile that bounded eventual-consistency
    # window instead of abandoning a lock which appears moments later.
    for delay in (0.0, 0.15, 0.35, 0.75, 1.5):
        if delay:
            time.sleep(delay)
        names = _directory_file_names(alist, lock_root)
        matches = _collision_presence(names, lock_name)
        conflicting_locks = sorted(
            name for name in names
            if is_scraper_lock(name)
            and (
                (key := _remote_lock_scope_from_name(name)) is None
                or key == "all"
                or scope_key == "all"
                or key == scope_key
            )
        )
        if len(matches) == 1 and conflicting_locks == [lock_name]:
            break
        if conflicting_locks and lock_name not in conflicting_locks:
            break
    else:
        matches = []

    if len(matches) != 1 or conflicting_locks != [lock_name]:
        # The name contains a random nonce and is owned by this journal. Exact
        # removal is safe even when listing has not caught up with upload yet.
        try:
            alist.remove(lock_root, [lock_name])
        except Exception:
            pass
        raise ApiError(
            f"无法确认整理锁为目录中的唯一冲突锁: {lock_path}; "
            f"own_matches={matches}, conflicting_locks={conflicting_locks}"
        )
    _mark_record(journal, journal_path, record, "ok")
    return lock_path


def _nearest_existing_lock_root(alist: AListClient, desired_root: str) -> str:
    current = normalize_remote_path(desired_root)
    while True:
        if alist.try_list(current, refresh=True) is not None:
            return current
        if current == "/":
            # AList's virtual root may not be listable in minimal test/storage
            # adapters, but it remains the only safe parent lock location.
            return current
        parent, _ = split_remote(current)
        if parent == current:
            raise PlanError(f"目标路径没有可锁定的现有父目录: {desired_root}")
        current = parent


def _remote_lock_scope_key(lock_root: str, desired_root: str) -> str:
    """Scope a parent-directory reservation to one immediate child tree."""
    lock_root = normalize_remote_path(lock_root)
    desired_root = normalize_remote_path(desired_root)
    if _collision_key(lock_root) == _collision_key(desired_root):
        return "all"
    relative = desired_root[len(lock_root.rstrip("/")):].lstrip("/")
    first_child = relative.split("/", 1)[0]
    return hashlib.sha256(_collision_key(first_child).encode("utf-8")).hexdigest()[:16]


def _remote_lock_scope_from_name(name: str) -> str | None:
    match = re.match(
        rf"^{re.escape(LOCK_PREFIX)}v2-(all|[0-9a-f]{{16}})-",
        _collision_key(name),
    )
    return match.group(1) if match else None


def _lock_roots_for_plan(alist: AListClient, plan: Plan) -> list[tuple[str, str, str]]:
    source_root = normalize_remote_path(plan.source_root)
    target_root = normalize_remote_path(plan.target_root)
    requests = {
        (source_root, source_root): "source",
        (_nearest_existing_lock_root(alist, target_root), target_root): "target",
    }
    return sorted(
        ((lock_root, desired_root, scope) for (lock_root, desired_root), scope in requests.items()),
        key=lambda item: (_collision_key(item[0]), _collision_key(item[1])),
    )


def _release_remote_lock(
    alist: AListClient,
    lock_path: str,
    journal: ExecutionJournal,
    journal_path: Path,
) -> None:
    parent, name = split_remote(lock_path)
    record = _append_pending(
        journal, journal_path, "release-lock", lock_path, ""
    )
    names = _directory_file_names(alist, parent)
    matches = _collision_presence(names, name)
    if not matches:
        _mark_record(journal, journal_path, record, "ok", "already absent")
        return
    if len(matches) != 1 or matches[0] != name:
        raise ApiError(f"整理锁名称状态不明确，拒绝删除: {lock_path}; matches={matches}")
    last_error: Exception | None = None
    # Quark can acknowledge AList's remove request before the refreshed file
    # listing converges. The lock name contains a per-journal nonce, so an
    # exact repeated removal is idempotent and cannot target another run. Give
    # the provider a bounded convergence window before rolling back media that
    # already passed the target integrity check.
    for attempt in range(6):
        try:
            alist.remove(parent, [name])
            last_error = None
        except Exception as exc:
            last_error = exc
        remaining = _collision_presence(_directory_file_names(alist, parent), name)
        if not remaining:
            _mark_record(
                journal, journal_path, record, "ok",
                "provider listing converged after retry" if attempt else None,
            )
            return
        if attempt < 5:
            time.sleep(min(0.5 * (2 ** attempt), 3.0))
    detail = f"; last_remove_error={last_error}" if last_error is not None else ""
    raise ApiError(f"整理锁删除后仍然存在: {lock_path}{detail}")


def _mark_record(
    journal: ExecutionJournal,
    journal_path: Path,
    record: ExecutionRecord,
    status: str,
    message: str | None = None,
) -> None:
    record.status = status
    if message is not None:
        record.message = message
    journal.save(journal_path)


def _directory_file_names(alist: AListClient, directory: str) -> set[str]:
    content = alist.try_list(directory, refresh=True)
    if content is None:
        return set()
    return {
        str(entry["name"])
        for entry in content
        if not entry.get("is_dir") and isinstance(entry.get("name"), str)
    }


def _directory_file_entries(
    alist: AListClient, directory: str
) -> list[Mapping[str, Any]]:
    content = alist.try_list(directory, refresh=True)
    if content is None:
        return []
    return [
        entry
        for entry in content
        if not entry.get("is_dir") and isinstance(entry.get("name"), str)
    ]


def _collision_presence(names: Iterable[str], expected: str) -> list[str]:
    key = _collision_key(expected)
    return [name for name in names if _collision_key(name) == key]


class _HybridAListAdapter(AListExactFileAdapter):
    """Add exact, idempotent directory creation for the hybrid transaction."""

    def __init__(self, backend: AListClient) -> None:
        super().__init__(backend)
        self._alist = backend

    def ensure_directory(self, path: str) -> None:
        normalized = normalize_remote_path(path)
        current = "/"
        for segment in normalized.strip("/").split("/") if normalized != "/" else []:
            parent_rows = self._alist.try_list(current, refresh=True)
            # Some exact backends represent an existing empty/root directory
            # as ``None`` until its first child is created.  Creation remains
            # fail-closed because the exact parent listing is verified below.
            parent_rows = parent_rows or []
            matches = [
                row for row in parent_rows
                if isinstance(row.get("name"), str)
                and _collision_key(str(row["name"])) == _collision_key(segment)
            ]
            if len(matches) > 1 or any(not row.get("is_dir") for row in matches):
                raise ApiError(f"事务目录存在文件或等价名称冲突: {join_remote(current, segment)}")
            next_path = join_remote(current, segment)
            if not matches:
                self._alist.mkdir(next_path)
                if self._alist.try_list(next_path, refresh=True) is None:
                    raise ApiError(f"事务目录创建后无法唯一确认: {next_path}")
            current = next_path


def _hybrid_state_root(journal_path: Path) -> Path:
    configured = os.getenv("SCRAPEFLOW_HYBRID_TRANSACTION_STATE_ROOT", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return journal_path.parent / ".hybrid-remote-transactions"


def _hybrid_batch_specs(
    alist: AListClient,
    plan: Plan,
    *,
    transaction_scope: str,
) -> tuple[list[HybridTransferSpec], dict[str, HybridTransferSpec]]:
    batch_id = "scraper-" + hashlib.sha256(
        f"{transaction_scope}\0{plan_sha256(plan)}".encode(
            "utf-8", errors="surrogatepass"
        )
    ).hexdigest()[:48]
    rollback_root = normalize_remote_path(
        os.getenv(REMOTE_ROLLBACK_ROOT_ENV, DEFAULT_ROLLBACK_ROOT).strip()
        or DEFAULT_ROLLBACK_ROOT
    )
    adapter = AListExactFileAdapter(alist)
    specs: list[HybridTransferSpec] = []
    by_source: dict[str, HybridTransferSpec] = {}
    for item in sorted(_forward_plan_files(plan), key=lambda row: _collision_key(row.source_path)):
        if item.source_dir == item.target_dir and not item.requires_rename:
            continue
        target_path = join_remote(item.target_dir, item.final_name)
        size = item.source_size
        if size is None:
            info = adapter.stat_exact(item.source_path)
            if info is None:
                raise TransactionUncertain(f"混合事务源文件不可见: {item.source_path}")
            size = info.size
        item_id = "item-" + hashlib.sha256(
            f"{item.source_path}\0{target_path}".encode(
                "utf-8", errors="surrogatepass"
            )
        ).hexdigest()[:48]
        spec = HybridTransferSpec(
            batch_id=batch_id,
            item_id=item_id,
            source_path=item.source_path,
            target_path=target_path,
            expected_size=size,
            rollback_root=rollback_root,
        )
        specs.append(spec)
        by_source[_collision_key(item.source_path)] = spec
    for item in sorted(plan.cleanup_files, key=lambda row: _collision_key(row.source_path)):
        size = item.source_size
        if size is None:
            info = adapter.stat_exact(item.source_path)
            if info is None:
                raise TransactionUncertain(
                    f"混合删除事务源文件不可见: {item.source_path}"
                )
            size = info.size
        item_id = "item-" + hashlib.sha256(
            f"delete\0{item.source_path}".encode(
                "utf-8", errors="surrogatepass"
            )
        ).hexdigest()[:48]
        spec = HybridTransferSpec(
            batch_id=batch_id,
            item_id=item_id,
            operation="delete",
            source_path=item.source_path,
            target_path=None,
            expected_size=size,
            rollback_root=rollback_root,
        )
        specs.append(spec)
        by_source[_collision_key(item.source_path)] = spec
    role_paths: dict[str, tuple[str, str]] = {}
    for spec in specs:
        role_path_pairs = [
            ("source", spec.source_path),
            ("rollback", spec.rollback_path),
            ("item-manifest", spec.remote_manifest_path),
        ]
        if spec.target_path is not None:
            role_path_pairs.append(("target", spec.target_path))
        for role, path in role_path_pairs:
            normalized = normalize_remote_path(path)
            if role in {"source", "target"} and (
                _path_is_within(normalized, rollback_root)
                or _path_is_within(rollback_root, normalized)
            ):
                raise PlanError(
                    f"用户媒体路径与事务回滚根重叠: {role}={normalized}; "
                    f"rollback_root={rollback_root}"
                )
            key = _collision_key(normalized)
            previous = role_paths.get(key)
            if previous is not None and previous != (role, normalized):
                raise PlanError(
                    "混合事务批次包含角色重叠路径: "
                    f"{previous[0]}={previous[1]} 与 {role}={normalized}"
                )
            role_paths[key] = (role, normalized)
    return specs, by_source


def _remote_transfer_spec(
    alist: AListClient,
    source_path: str,
    target_path: str,
    *,
    stage_root: Path,
    transaction_scope: str,
    expected_size: int | None,
) -> tuple[AListExactFileAdapter, RemoteFileTransferSpec]:
    adapter = AListExactFileAdapter(alist)
    transaction_id = "scraper-" + hashlib.sha256(
        f"{transaction_scope}\0{source_path}\0{target_path}".encode(
            "utf-8", errors="surrogatepass"
        )
    ).hexdigest()[:48]
    if expected_size is None:
        info = adapter.stat_exact(source_path)
        if info is not None:
            expected_size = info.size
        else:
            transaction_journal = stage_root / transaction_id / "journal.json"
            try:
                raw = json.loads(transaction_journal.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise TransactionUncertain(
                    f"源文件不可见且无法读取本机事务副本: {source_path}"
                ) from exc
            size = raw.get("size") if isinstance(raw, Mapping) else None
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                raise TransactionUncertain(
                    f"本机事务副本缺少有效大小: {source_path}"
                )
            expected_size = size
    return adapter, RemoteFileTransferSpec(
        transaction_id=transaction_id,
        source_path=source_path,
        target_path=target_path,
        expected_size=expected_size,
    )


def _prepare_remote_transfer(
    alist: AListClient,
    source_path: str,
    target_path: str,
    *,
    stage_root: Path,
    transaction_scope: str,
    expected_size: int | None,
) -> RemoteFileTransferSpec:
    adapter, spec = _remote_transfer_spec(
        alist,
        source_path,
        target_path,
        stage_root=stage_root,
        transaction_scope=transaction_scope,
        expected_size=expected_size,
    )
    prepare_remote_file_transaction(adapter, stage_root=stage_root, spec=spec)
    return spec


def _execute_remote_transfer(
    alist: AListClient,
    source_path: str,
    target_path: str,
    *,
    stage_root: Path,
    transaction_scope: str,
    expected_size: int | None,
) -> RemoteFileTransferSpec:
    adapter, spec = _remote_transfer_spec(
        alist,
        source_path,
        target_path,
        stage_root=stage_root,
        transaction_scope=transaction_scope,
        expected_size=expected_size,
    )
    run_remote_file_transaction(adapter, stage_root=stage_root, spec=spec)
    # The caller owns the work-level commit boundary.  Keeping the payload here
    # is essential: a later file, poster, NFO, cleanup, or final verification
    # failure must still have recoverable bytes for every earlier source.
    return spec


def _discard_completed_transfer_specs(
    *, stage_root: Path, specs: Iterable[RemoteFileTransferSpec],
) -> list[str]:
    """Release only the exact payloads owned by a committed work batch."""
    failures: list[str] = []
    unique: dict[str, RemoteFileTransferSpec] = {
        spec.transaction_id: spec for spec in specs
    }
    for spec in unique.values():
        try:
            discard_completed_remote_file_transaction(stage_root=stage_root, spec=spec)
        except Exception as exc:
            # Retaining a completed payload is safe.  Report it without turning
            # a fully verified work into a destructive rollback attempt.
            failures.append(f"{spec.transaction_id}: {exc}")
    return failures


def _hash_local_path(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _materialize_generated_payload(
    transaction_root: Path,
    *,
    target_path: str,
    payload: bytes,
    suffix: str,
) -> tuple[Path, str]:
    """Persist generated metadata before its first upload attempt."""
    sha256 = hashlib.sha256(payload).hexdigest()
    payload_root = transaction_root / "generated-payloads"
    payload_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    identity = hashlib.sha256(
        f"{target_path}\0{sha256}".encode("utf-8", errors="surrogatepass")
    ).hexdigest()
    path = payload_root / f"{identity}{suffix}"
    if path.exists():
        size, existing_sha256 = _hash_local_path(path)
        if size != len(payload) or existing_sha256 != sha256:
            raise LocalUploadConflict(
                f"持久元数据 payload 与计划内容不一致，拒绝覆盖: {path}"
            )
        return path, sha256
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{identity}.", suffix=".tmp", dir=payload_root,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            pass
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    size, actual_sha256 = _hash_local_path(path)
    if size != len(payload) or actual_sha256 != sha256:
        raise LocalUploadConflict(f"持久元数据 payload 写入校验失败: {path}")
    return path, sha256


def _upload_generated_payload_exact(
    alist: AListClient,
    *,
    transaction_root: Path,
    target_path: str,
    payload: bytes,
    content_type: str,
    suffix: str,
) -> str:
    """Create or accept generated metadata only after full SHA-256 proof."""
    source_path, sha256 = _materialize_generated_payload(
        transaction_root,
        target_path=target_path,
        payload=payload,
        suffix=suffix,
    )
    spec = LocalUploadSpec(
        transaction_id=deterministic_local_upload_id(source_path, target_path),
        source_path=source_path,
        target_path=target_path,
        expected_size=len(payload),
        expected_sha256=sha256,
        content_type=content_type,
    )
    result = run_local_upload_transaction(
        AListExactFileAdapter(alist),
        transaction_root=transaction_root / "generated-uploads",
        spec=spec,
    )
    if result.state != "complete" or result.sha256 != sha256:
        raise LocalUploadConflict(f"元数据目标未通过完整 SHA-256 校验: {target_path}")
    return sha256


def _move_with_reconciliation(
    alist: AListClient,
    src_dir: str,
    dst_dir: str,
    names: Sequence[str],
    *,
    stage_root: Path,
    transaction_scope: str,
    expected_sizes: Mapping[str, int | None] | None = None,
) -> list[str]:
    """Transfer files one-by-one through durable local, SHA-256 verified stages.

    This function deliberately contains no AList MOVE fallback.  An ambiguous
    upload is never repeated: the transaction either proves the exact target
    by complete read-back, or retains its local payload and fails closed.
    """
    completed: list[str] = []
    for name in names:
        source_path = join_remote(src_dir, name)
        target_path = join_remote(dst_dir, name)
        expected_size = (expected_sizes or {}).get(name)
        try:
            _execute_remote_transfer(
                alist,
                source_path,
                target_path,
                stage_root=stage_root,
                transaction_scope=transaction_scope,
                expected_size=expected_size,
            )
        except Exception as exc:
            remaining = [candidate for candidate in names if candidate not in completed]
            transaction_id = "scraper-" + hashlib.sha256(
                f"{transaction_scope}\0{source_path}\0{target_path}".encode(
                    "utf-8", errors="surrogatepass"
                )
            ).hexdigest()[:48]
            raise PartialMoveError(
                f"本机安全转移未完成: {source_path} → {target_path}; "
                f"moved={completed}, remaining={remaining}; "
                f"stage={stage_root / transaction_id}; "
                f"original={_redact_sensitive_text(str(exc))}",
                completed,
            ) from exc
        completed.append(name)
    return completed


def _remote_transaction_stage_root(journal_path: Path) -> Path:
    configured = os.getenv("SCRAPEFLOW_REMOTE_TRANSACTION_ROOT", "").strip()
    if configured:
        owner = hashlib.sha256(
            str(journal_path.parent).encode("utf-8", errors="surrogatepass")
        ).hexdigest()[:24]
        return Path(configured).expanduser().resolve() / owner
    return journal_path.parent / ".remote-file-transactions"


def _verify_final_state(alist: AListClient, plan: Plan) -> None:
    last_error: str | None = None
    for attempt in range(5):
        try:
            target_entries: dict[str, list[Mapping[str, Any]]] = {}
            for item in _forward_plan_files(plan):
                if item.target_dir not in target_entries:
                    target_entries[item.target_dir] = _directory_file_entries(
                        alist, item.target_dir
                    )
                matches = [
                    entry
                    for entry in target_entries[item.target_dir]
                    if _collision_key(str(entry["name"]))
                    == _collision_key(item.final_name)
                ]
                if len(matches) != 1:
                    raise ApiError(
                        f"执行后校验失败，目标文件数量异常: "
                        f"{item.target_dir}/{item.final_name}; "
                        f"matches={[entry.get('name') for entry in matches]}"
                    )

                actual = matches[0]
                actual_size = _entry_size_value(actual)
                actual_hash = _entry_hash_value(actual)
                identity_changes: list[str] = []
                if item.source_size is not None:
                    if actual_size is None:
                        identity_changes.append("目标端未返回 size")
                    elif actual_size != item.source_size:
                        identity_changes.append(
                            f"size: {item.source_size!r} → {actual_size!r}"
                        )
                if item.source_hash is not None:
                    if actual_hash is None:
                        identity_changes.append("目标端未返回 hash")
                    elif actual_hash != item.source_hash:
                        identity_changes.append(
                            f"hash: {item.source_hash!r} → {actual_hash!r}"
                        )
                if identity_changes:
                    raise ApiError(
                        f"执行后校验失败，目标文件身份与计划快照不一致: "
                        f"{item.target_dir}/{item.final_name}; "
                        + "; ".join(identity_changes)
                    )

            source_names: dict[str, set[str]] = {}
            for item in _forward_plan_files(plan):
                if _collision_key(item.source_dir) == _collision_key(item.target_dir):
                    continue
                if item.source_dir not in source_names:
                    source_names[item.source_dir] = _directory_file_names(alist, item.source_dir)
                if _collision_presence(source_names[item.source_dir], item.final_name):
                    raise ApiError(
                        f"执行后校验失败，文件仍留在源目录: "
                        f"{item.source_dir}/{item.final_name}"
                    )
            return
        except ApiError as exc:
            last_error = str(exc)
            if attempt == 4:
                raise
            time.sleep(0.5 * (attempt + 1))
    raise ApiError(last_error or "执行后校验失败")


def _cleanup_planned_files(
    alist: AListClient,
    plan: Plan,
    journal: ExecutionJournal,
    journal_path: Path,
    *,
    hybrid_state_root: Path,
    hybrid_by_source: Mapping[str, HybridTransferSpec],
) -> None:
    adapter = _HybridAListAdapter(alist)
    for item in plan.cleanup_files:
        record = _append_pending(
            journal,
            journal_path,
            "hybrid-delete",
            item.source_path,
            "",
            item.reason,
        )
        try:
            entries = _directory_file_entries(alist, item.source_dir)
            matches = [
                entry
                for entry in entries
                if _collision_key(str(entry.get("name") or ""))
                == _collision_key(item.original_name)
            ]
            if len(matches) != 1 or str(matches[0].get("name")) != item.original_name:
                if matches:
                    raise ApiError(
                        f"清理文件名称状态不明确，拒绝删除: {item.source_path}"
                    )
                actual = None
            else:
                actual = matches[0]
                comparisons = (
                    ("size", item.source_size, _entry_size_value(actual)),
                    ("modified", item.source_modified, _entry_modified_value(actual)),
                    ("hash", item.source_hash, _entry_hash_value(actual)),
                )
                changed = [
                    f"{name}: {expected!r} → {observed!r}"
                    for name, expected, observed in comparisons
                    if expected is not None and expected != observed
                ]
                if changed:
                    raise ApiError(
                        f"清理文件在确认后发生变化，拒绝删除: {item.source_path}; "
                        + "; ".join(changed)
                    )
            spec = hybrid_by_source.get(_collision_key(item.source_path))
            if spec is None or spec.operation != "delete":
                raise TransactionUncertain(
                    f"清理文件未纳入已封存的混合事务批次: {item.source_path}"
                )
            result = run_hybrid_transfer(
                adapter, state_root=hybrid_state_root, spec=spec,
            )
            if result.state != "complete":
                raise TransactionUncertain(
                    f"清理文件未完成可恢复删除: {item.source_path}; "
                    f"state={result.state}"
                )
            _mark_record(
                journal,
                journal_path,
                record,
                "ok",
                f"{item.reason}; hybrid_item={spec.item_id}; "
                f"sha256={result.sha256}; remote_rollback_retained",
            )
            print(f"已完成可恢复删除: {_terminal_text(item.source_path)}")
        except Exception as exc:
            _mark_record(journal, journal_path, record, "failed", str(exc))
            raise ScraperError(
                f"无用文件未能通过混合事务安全删除: "
                f"{_terminal_text(item.source_path)}: {exc}"
            ) from exc


def _source_cleanup_directories(plan: Plan) -> list[str]:
    """Return source directories deepest-first, including intermediate parents."""
    source_root = normalize_remote_path(plan.source_root)
    directories = {source_root}
    for item in [*plan.files, *plan.cleanup_files]:
        current = normalize_remote_path(item.source_dir)
        if not _path_is_within(current, source_root):
            continue
        while True:
            directories.add(current)
            if _collision_key(current) == _collision_key(source_root):
                break
            parent, _ = split_remote(current)
            if parent == current or not _path_is_within(parent, source_root):
                break
            current = parent
    return sorted(
        directories,
        key=lambda value: (value.count("/"), len(value)),
        reverse=True,
    )


def execute_plan(
    alist: AListClient,
    tmdb_client: TMDBClient | None,
    plan: Plan,
    *,
    journal_path: Path,
    skip_poster: bool,
    overwrite_poster: bool = False,
    cleanup_empty_source: bool = False,
    allow_library_remediation: bool = False,
) -> None:
    journal_path = journal_path.expanduser().resolve()
    emit_progress(
        "execution_validation", completed=0, total=len(plan.files), percent=2,
        message="正在校验计划、源文件快照与路由",
    )
    validate_plan(
        alist,
        plan,
        allow_library_remediation=allow_library_remediation,
    )
    if plan.problem_files:
        preview = "；".join(
            f"{item.source_path}（{item.reason}）"
            for item in plan.problem_files[:5]
        )
        suffix = (
            f"；另有 {len(plan.problem_files) - 5} 个问题文件"
            if len(plan.problem_files) > 5 else ""
        )
        raise PlanError(
            "计划含有未闭合的问题文件，必须保留原位并阻止执行: "
            f"{preview}{suffix}"
        )
    validate_source_state(alist, plan, require_snapshot=True)
    _reserve_output_path(journal_path)
    journal = ExecutionJournal(
        created_at=datetime.now(timezone.utc).isoformat(),
        plan=plan_to_dict(plan),
        records=[],
    )
    try:
        journal.save(journal_path)
    except Exception:
        try:
            journal_path.unlink()
        except FileNotFoundError:
            pass
        raise

    moved_items: list[PlannedFile] = []
    created_dirs: list[str] = []
    files_committed = False
    poster_preexisted = False
    pending_poster_target: str | None = None
    pending_move: tuple[PlannedFile, str, str] | None = None
    remote_lock_paths: list[str] = []
    transaction_stage_root = _remote_transaction_stage_root(journal_path)
    hybrid_state_root = _hybrid_state_root(journal_path)
    transaction_scope = f"forward-{plan_sha256(plan)}"
    hybrid_specs: list[HybridTransferSpec] = []
    hybrid_by_source: dict[str, HybridTransferSpec] = {}
    try:
        # Establish and remotely prove the complete original-source rollback
        # set before the first forward mutation.  The hybrid batch streams one
        # local file at a time and releases it only after its Quark rollback
        # copy plus immutable batch manifest have passed full SHA-256 readback.
        forward_items = [
            item for item in _forward_plan_files(plan)
            if item.source_dir != item.target_dir or item.requires_rename
        ]
        hybrid_specs, hybrid_by_source = _hybrid_batch_specs(
            alist, plan, transaction_scope=transaction_scope,
        )
        if hybrid_specs:
            record = _append_pending(
                journal, journal_path, "prepare-hybrid-batch", plan.source_root,
                hybrid_specs[0].batch_root, f"items={len(hybrid_specs)}",
            )
            prepared = prepare_hybrid_batch(
                _HybridAListAdapter(alist),
                state_root=hybrid_state_root,
                specs=hybrid_specs,
            )
            if len(prepared) != len(hybrid_specs) or any(
                result.state != "rollback_verified" for result in prepared
            ):
                raise TransactionUncertain("混合事务批次未全部进入 rollback_verified")
            _mark_record(
                journal, journal_path, record, "ok",
                "all remote rollback payloads and sealed manifest hash-verified",
            )
            prepared_by_id = {result.item_id: result for result in prepared}
            sealed_receipt = {
                "schema_version": 1,
                "kind": "hybrid_batch_sealed_receipt",
                "state_root": str(hybrid_state_root),
                "batch_id": hybrid_specs[0].batch_id,
                "rollback_root": hybrid_specs[0].rollback_root,
                "plan_sha256": plan_sha256(plan),
                "commit_authority": "local_ordinary_acceptance_only",
                "items": [
                    {
                        "operation": spec.operation,
                        "spec": spec.to_dict(),
                        "verified_size": prepared_by_id[spec.item_id].size,
                        "verified_sha256": prepared_by_id[spec.item_id].sha256,
                        "rollback_path": prepared_by_id[spec.item_id].rollback_path,
                    }
                    for spec in hybrid_specs
                ],
            }
            journal.records.append(ExecutionRecord(
                "hybrid-batch-sealed",
                plan.source_root,
                hybrid_specs[0].batch_root,
                "retained",
                json.dumps(
                    sealed_receipt,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ))
            # This is the recovery authority for failures after the first
            # forward call; persist it before locks, mkdir, or user-file writes.
            journal.save(journal_path)

        for lock_root, desired_root, scope in _lock_roots_for_plan(alist, plan):
            remote_lock_paths.append(
                _acquire_remote_lock(
                    alist, plan, journal, journal_path, lock_root, desired_root, scope
                )
            )
        emit_progress(
            "execution_locked", completed=0, total=len(plan.files), percent=10,
            message="源目录与目标目录已锁定",
        )
        _ensure_target_dirs(alist, plan, journal, journal_path, created_dirs)

        emit_progress(
            "execution_renamed", completed=0, total=len(plan.files), percent=35,
            message="最终目标路径已通过全局冲突预检",
        )

        # 同目录和跨目录都由原始路径直接绑定最终目标。
        # 目标被另一计划源占用时在封存前 fail closed，不生成临时名。
        moved_count = 0
        for item in sorted(
            forward_items,
            key=lambda value: (value.source_dir, value.target_dir, value.original_name),
        ):
            target_path = join_remote(item.target_dir, item.final_name)
            pending_move = (item, item.source_path, target_path)
            record = _append_pending(
                journal,
                journal_path,
                "move",
                item.source_dir,
                item.target_dir,
                item.final_name,
            )
            try:
                hybrid_spec = hybrid_by_source[_collision_key(item.source_path)]
                result = run_hybrid_transfer(
                    _HybridAListAdapter(alist),
                    state_root=hybrid_state_root,
                    spec=hybrid_spec,
                )
                if result.state != "complete":
                    raise TransactionUncertain(
                        f"混合事务未完成: {hybrid_spec.item_id}; state={result.state}"
                    )
            except Exception as transfer_exc:
                _mark_record(journal, journal_path, record, "failed", str(transfer_exc))
                raise
            moved_items.append(item)
            moved_count += 1
            pending_move = None
            _mark_record(journal, journal_path, record, "ok", item.final_name)
            emit_progress(
                "execution_move",
                completed=moved_count,
                total=len(plan.files),
                percent=35 + (45 * moved_count / max(1, len(plan.files))),
                message=f"已移动并核对 {moved_count}/{len(plan.files)} 个文件",
            )

        _verify_final_state(alist, plan)
        emit_progress(
            "execution_verified", completed=len(plan.files), total=len(plan.files), percent=85,
            message="目标文件完整性已通过校验",
        )
        # Keep every original payload until artwork, NFO, residual cleanup and
        # the final work-level checks have also succeeded.  Media verification
        # alone is not the completion contract.

        artwork_requests = planned_artwork(plan) if not skip_poster else []
        artwork_cache: dict[str, bytes] = {}
        for requested_target, image_path, artwork_role in artwork_requests:
            poster_target, poster_preexisted = resolve_artwork_target(
                alist, requested_target
            )
            if tmdb_client is None:
                raise ScraperError("计划包含图稿，但未提供 TMDB API Key；可使用 --skip-poster")
            if image_path not in artwork_cache:
                artwork_cache[image_path] = tmdb_client.download_poster(image_path)
            poster_data = artwork_cache[image_path]
            pending_poster_target = poster_target
            record = _append_pending(
                journal, journal_path, "upload-poster", image_path, poster_target, artwork_role
            )
            if poster_preexisted and overwrite_poster:
                # Exact upload transactions are create-or-prove.  Overwriting
                # an unverified object destroys the only extant bytes and is
                # therefore no longer an accepted compatibility behavior.
                raise LocalUploadConflict(
                    f"已有图稿必须与官方 payload 完整一致；禁止原位覆盖: {poster_target}"
                )
            poster_sha256 = _upload_generated_payload_exact(
                alist,
                transaction_root=transaction_stage_root,
                target_path=poster_target,
                payload=poster_data,
                content_type="image/jpeg",
                suffix=".jpg",
            )
            pending_poster_target = None
            _mark_record(
                journal,
                journal_path,
                record,
                "ok",
                f"exact_sha256={poster_sha256}; "
                + ("existing content proved" if poster_preexisted else "created"),
            )

        for nfo_target, nfo_data in planned_nfos(plan):
            actual_target, preexisting_nfo = resolve_artwork_target(alist, nfo_target)
            poster_preexisted = False
            pending_poster_target = actual_target
            record = _append_pending(
                journal, journal_path, "upload-nfo", "generated", actual_target
            )
            nfo_sha256 = _upload_generated_payload_exact(
                alist,
                transaction_root=transaction_stage_root,
                target_path=actual_target,
                payload=nfo_data,
                content_type="application/xml",
                suffix=".nfo",
            )
            pending_poster_target = None
            _mark_record(
                journal,
                journal_path,
                record,
                "ok",
                f"exact_sha256={nfo_sha256}; "
                + ("existing content proved" if preexisting_nfo else "created"),
            )

        _cleanup_planned_files(
            alist,
            plan,
            journal,
            journal_path,
            hybrid_state_root=hybrid_state_root,
            hybrid_by_source=hybrid_by_source,
        )

        _verify_final_state(alist, plan)
        files_committed = True
        journal.records.append(ExecutionRecord(
            "files-committed", "", plan.target_root, "ok",
            "media, generated metadata and planned cleanup verified",
        ))
        journal.save(journal_path)

        # The source lock itself would prevent a now-fileless source tree from
        # being renamed to “待删”. Release it only after target integrity and
        # planned source cleanup have completed; retain the target lock through
        # the pending-delete step.
        source_lock_root = normalize_remote_path(plan.source_root)
        for lock_path in list(reversed(remote_lock_paths)):
            if _collision_key(split_remote(lock_path)[0]) != _collision_key(source_lock_root):
                continue
            _release_remote_lock(alist, lock_path, journal, journal_path)
            remote_lock_paths.remove(lock_path)

        if cleanup_empty_source:
            for source_dir in _source_cleanup_directories(plan):
                if source_dir == "/" or _path_is_within(plan.target_root, source_dir):
                    continue
                record = _append_pending(
                    journal, journal_path, "cleanup-empty-source", source_dir, ""
                )
                try:
                    removed = alist.remove_empty_dir(source_dir)
                except Exception as exc:
                    # Media files are already committed and verified. Directory cleanup is
                    # best-effort and must not turn a successful organization into recovery.
                    _mark_record(journal, journal_path, record, "failed", str(exc))
                    print(
                        f"警告: 源空目录未能清理: {_terminal_text(source_dir)}: {exc}"
                    )
                    continue
                _mark_record(
                    journal,
                    journal_path,
                    record,
                    "ok" if removed else "skipped",
                    "removed" if removed else "directory not empty",
                )
        for lock_path in reversed(remote_lock_paths):
            _release_remote_lock(alist, lock_path, journal, journal_path)
        remote_lock_paths.clear()
        # Engine success is only a forward terminal, never ordinary-work
        # acceptance.  Keep Quark rollback payloads until Local proves the
        # complete title contract (tree/media/NFO/artwork/residual/subtitle and
        # source departure) and explicitly commits this exact sealed batch.
        for spec in hybrid_specs:
            journal.records.append(ExecutionRecord(
                "hybrid-forward-terminal",
                spec.source_path,
                spec.target_path or "",
                "retained",
                json.dumps({
                    "state_root": str(hybrid_state_root),
                    "spec": spec.to_dict(),
                    "commit_authority": "local_ordinary_acceptance_only",
                }, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            ))
        if hybrid_specs:
            journal.records.append(ExecutionRecord(
                "hybrid-batch-forward-terminal",
                plan.source_root,
                plan.target_root,
                "retained",
                json.dumps({
                    "schema_version": 1,
                    "kind": "hybrid_batch_forward_terminal",
                    "state_root": str(hybrid_state_root),
                    "batch_id": hybrid_specs[0].batch_id,
                    "rollback_root": hybrid_specs[0].rollback_root,
                    "plan_sha256": plan_sha256(plan),
                    "item_ids": [spec.item_id for spec in hybrid_specs],
                    "commit_authority": "local_ordinary_acceptance_only",
                }, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            ))
        journal.success = True
        journal.save(journal_path)
        emit_progress(
            "execution_complete", completed=len(plan.files), total=len(plan.files), percent=100,
            message="文件、元数据与事务日志均已提交",
        )
    except (Exception, KeyboardInterrupt) as exc:
        error_text = str(exc) or type(exc).__name__
        journal.records.append(ExecutionRecord("abort", "", "", "failed", error_text))

        if files_committed:
            # 文件已经通过最终校验。此时只核对可能处于不确定状态的海报，
            # 不再执行文件回滚。
            if pending_poster_target is not None:
                try:
                    poster_name = split_remote(pending_poster_target)[1]
                    poster_dir, _ = split_remote(pending_poster_target)
                    present = any(
                        _collision_key(name) == _collision_key(poster_name)
                        for name in _directory_file_names(alist, poster_dir)
                    )
                    journal.records.append(
                        ExecutionRecord(
                            "reconcile-poster",
                            "TMDB",
                            pending_poster_target,
                            "uncertain" if poster_preexisted else ("ok" if present else "failed"),
                            (
                                "overwrite result cannot be content-verified"
                                if poster_preexisted
                                else f"poster_present={present}"
                            ),
                        )
                    )
                except Exception as reconcile_exc:
                    journal.records.append(
                        ExecutionRecord(
                            "reconcile-poster",
                            "TMDB",
                            pending_poster_target,
                            "failed",
                            str(reconcile_exc),
                        )
                    )
            for lock_path in reversed(remote_lock_paths):
                try:
                    _release_remote_lock(alist, lock_path, journal, journal_path)
                except Exception as lock_exc:
                    journal.records.append(ExecutionRecord(
                        "release-lock", lock_path, "", "failed", str(lock_exc)
                    ))
            remote_lock_paths.clear()
            journal_note = f"详见 {journal_path}"
            try:
                journal.save(journal_path)
            except OSError as journal_exc:
                journal_note = f"执行日志写入失败: {journal_exc}"
            if isinstance(exc, KeyboardInterrupt):
                raise
            raise ScraperError(
                f"媒体文件已完成整理并通过校验，但图稿、NFO 或提交后记录步骤失败；"
                f"未回滚媒体文件；{journal_note}: {error_text}"
            ) from exc

        if pending_move is not None:
            pending_item, pending_source, pending_target = pending_move
            try:
                pending_spec = hybrid_by_source[_collision_key(pending_source)]
                result = run_hybrid_transfer(
                    _HybridAListAdapter(alist),
                    state_root=hybrid_state_root,
                    spec=pending_spec,
                )
                if result.state != "complete":
                    raise TransactionUncertain(
                        f"pending transfer did not reconcile complete: {result.state}"
                    )
                if pending_item not in moved_items:
                    moved_items.append(pending_item)
                journal.records.append(
                    ExecutionRecord(
                        "reconcile-move",
                        pending_source,
                        pending_target,
                        "ok",
                        f"size={result.size}; sha256={result.sha256}; full_readback=true",
                    )
                )
            except Exception as reconcile_exc:
                journal.records.append(
                    ExecutionRecord(
                        "reconcile-move",
                        pending_source,
                        pending_target,
                        "failed",
                        str(reconcile_exc),
                    )
                )

        # 先回滚已达目标的跨目录文件，再回滚同目录换名。
        for item in reversed(moved_items):
            current_path = join_remote(item.target_dir, item.final_name)
            try:
                spec = hybrid_by_source[_collision_key(item.source_path)]
                restored = restore_hybrid_transfer(
                    _HybridAListAdapter(alist),
                    state_root=hybrid_state_root,
                    spec=spec,
                )
                if restored.state != "restored":
                    raise TransactionUncertain(
                        f"混合事务恢复未完成: {spec.item_id}; state={restored.state}"
                    )
                journal.records.append(
                    ExecutionRecord(
                        "rollback-move", current_path, item.source_path, "ok",
                        f"remote_rollback={spec.rollback_path}; sha256={restored.sha256}",
                    )
                )
            except Exception as rollback_exc:
                journal.records.append(
                    ExecutionRecord(
                        "rollback-move",
                        current_path,
                        item.source_path,
                        "failed",
                        str(rollback_exc),
                    )
                )

        # 不自动删除本次流程观察为“新建”的目录。预检与 mkdir 之间存在并发窗口，
        # 无法证明一个仍为空的目录一定由本进程独占创建。保留空目录比误删其他
        # 仅清理本次执行确认过的空源目录，不扩大到媒体库的其他路径。
        for directory in sorted(created_dirs, key=lambda path: path.count("/"), reverse=True):
            journal.records.append(
                ExecutionRecord(
                    "rollback-rmdir",
                    directory,
                    "",
                    "retained",
                    "not removed automatically because directory ownership cannot be proven",
                )
            )

        for lock_path in reversed(remote_lock_paths):
            try:
                _release_remote_lock(alist, lock_path, journal, journal_path)
            except Exception as lock_exc:
                journal.records.append(ExecutionRecord(
                    "release-lock", lock_path, "", "failed", str(lock_exc)
                ))
        remote_lock_paths.clear()

        journal_note = f"详见 {journal_path}"
        try:
            journal.save(journal_path)
        except OSError as journal_exc:
            journal_note = f"执行日志写入失败: {journal_exc}"
        if isinstance(exc, KeyboardInterrupt):
            raise
        raise ScraperError(f"执行失败，已尝试回滚；{journal_note}: {error_text}") from exc


def plan_to_dict(plan: Plan) -> dict[str, Any]:
    payload = {
        "mode": plan.mode,
        "source_root": plan.source_root,
        "target_root": plan.target_root,
        "warnings": list(plan.warnings),
        "metadata": dict(plan.metadata),
        "files": [asdict(item) for item in plan.files],
        "notices": [asdict(item) for item in plan.notices],
        "decision_trace": dict(plan.decision_trace),
        "scan_report": dict(plan.scan_report),
    }
    if plan.cleanup_files:
        payload["cleanup_files"] = [asdict(item) for item in plan.cleanup_files]
    if plan.problem_files:
        payload["problem_files"] = [asdict(item) for item in plan.problem_files]
    return payload


def plan_sha256(plan: Plan | Mapping[str, Any]) -> str:
    payload = plan_to_dict(plan) if isinstance(plan, Plan) else dict(plan)
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _require_string(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise PlanError(f"计划字段 {field} 必须是字符串")
    return value


def _optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field, allow_empty=True)


def _require_remote_basename(value: Any, field: str) -> str:
    raw = _require_string(value, field)
    try:
        return _validate_remote_basename(raw)
    except ValueError as exc:
        raise PlanError(f"计划字段 {field} 不是安全文件名: {exc}") from exc


def _require_normalized_remote_path(value: Any, field: str) -> str:
    raw = _require_string(value, field)
    try:
        normalized = normalize_remote_path(raw)
    except ValueError as exc:
        raise PlanError(f"计划字段 {field} 不是安全远端路径: {exc}") from exc
    if normalized != raw:
        raise PlanError(f"计划字段 {field} 必须使用规范化绝对路径: {raw!r}")
    return normalized


def _optional_nonnegative_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PlanError(f"计划字段 {field} 必须是非负整数或 null")
    return value


def _reject_unknown_fields(raw: Mapping[str, Any], allowed: set[str], field: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise PlanError(f"计划字段 {field} 包含未知成员: {', '.join(unknown)}")


def _require_json_object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PlanError(f"计划字段 {field} 必须是对象")
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise PlanError(f"计划字段 {field} 必须是有限的 JSON 对象") from exc
    return dict(value)


def _validate_plan_metadata(mode: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    common = {"tmdb_id", "title", "original_title", "poster_path", "backdrop_path"}
    allowed_by_mode = {
        "tv": common | {"year", "season", "absolute", "season_posters", "episode_group"},
        "movie": common | {"year"},
        "collection": common | {"mapping", "member_posters", "member_movies"},
        "mixed": common | {"year", "season", "absolute", "season_posters", "episode_group", "member_posters", "member_movies", "series_root"},
        "batch": {"title", "member_tv", "member_posters", "member_movies"},
    }
    _reject_unknown_fields(raw, allowed_by_mode[mode], "metadata")

    title = _require_string(raw.get("title"), "metadata.title")
    if _has_unsafe_unicode(title):
        raise PlanError("计划字段 metadata.title 包含控制或不可见格式字符")
    if raw.get("original_title") is not None:
        original_title = _require_string(raw.get("original_title"), "metadata.original_title")
        if _has_unsafe_unicode(original_title):
            raise PlanError("计划字段 metadata.original_title 包含控制或不可见格式字符")
    if mode != "batch":
        tmdb_id = raw.get("tmdb_id")
        if isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0:
            raise PlanError("计划字段 metadata.tmdb_id 必须是正整数")

    def validate_image_path(value: Any, field: str) -> str | None:
        if value is None:
            return None
        image_path = _require_string(value, field)
        if (
            _has_unsafe_unicode(image_path)
            or not image_path.startswith("/")
            or ".." in image_path.split("/")
            or not re.fullmatch(r"/[A-Za-z0-9._/-]+", image_path)
        ):
            raise PlanError(f"计划字段 {field} 不是安全的 TMDB 图片路径")
        return image_path

    if mode != "batch":
        validate_image_path(raw.get("poster_path"), "metadata.poster_path")
        validate_image_path(raw.get("backdrop_path"), "metadata.backdrop_path")

    result = dict(raw)
    if mode == "mixed":
        result["series_root"] = _require_normalized_remote_path(
            raw.get("series_root"), "metadata.series_root"
        )
    if mode in {"tv", "movie", "mixed"}:
        year = _require_string(raw.get("year"), "metadata.year")
        if year != "未知年份" and not re.fullmatch(r"(?:19|20)\d{2}", year):
            raise PlanError("计划字段 metadata.year 必须是四位年份或 未知年份")
    if mode in {"tv", "mixed"}:
        season = raw.get("season")
        if isinstance(season, bool) or not isinstance(season, int) or season < 0:
            raise PlanError("计划字段 metadata.season 必须是非负整数")
        if type(raw.get("absolute")) is not bool:
            raise PlanError("计划字段 metadata.absolute 必须是布尔值")
        episode_group = raw.get("episode_group")
        if episode_group is not None:
            episode_group = _require_string(episode_group, "metadata.episode_group")
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", episode_group):
                raise PlanError("计划字段 metadata.episode_group 格式无效")
        if "season_posters" in raw:
            season_posters = raw.get("season_posters")
            if not isinstance(season_posters, Mapping):
                raise PlanError("计划字段 metadata.season_posters 必须是对象")
            normalized_seasons: dict[str, str] = {}
            for key, image_path in season_posters.items():
                if not isinstance(key, str) or not re.fullmatch(r"\d{1,3}", key):
                    raise PlanError("metadata.season_posters 的季度键必须是非负整数字符串")
                normalized_seasons[str(int(key))] = validate_image_path(
                    image_path, f"metadata.season_posters[{key!r}]"
                ) or ""
            result["season_posters"] = normalized_seasons
    if mode in {"collection", "mixed", "batch"}:
        mapping = raw.get("mapping")
        if mode != "batch" and mapping is not None:
            if not isinstance(mapping, Mapping):
                raise PlanError("计划字段 metadata.mapping 必须是对象或 null")
            normalized_mapping: dict[str, int] = {}
            for key, value in mapping.items():
                if not isinstance(key, (str, int)) or not str(key).isdigit():
                    raise PlanError("计划字段 metadata.mapping 的编号必须是正整数")
                number = int(str(key))
                if number <= 0 or isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                    raise PlanError("计划字段 metadata.mapping 的编号和 TMDB ID 必须是正整数")
                normalized_mapping[str(number)] = value
            result["mapping"] = normalized_mapping
        if "member_posters" in raw:
            member_posters = raw.get("member_posters")
            if not isinstance(member_posters, Mapping):
                raise PlanError("计划字段 metadata.member_posters 必须是对象")
            normalized_posters: dict[str, str] = {}
            for target_dir, image_path in member_posters.items():
                normalized_dir = _require_normalized_remote_path(
                    target_dir, "metadata.member_posters target"
                )
                normalized_posters[normalized_dir] = validate_image_path(
                    image_path, f"metadata.member_posters[{target_dir!r}]"
                ) or ""
            result["member_posters"] = normalized_posters
        if "member_movies" in raw:
            member_movies = raw.get("member_movies")
            if not isinstance(member_movies, Mapping):
                raise PlanError("计划字段 metadata.member_movies 必须是对象")
            normalized_movies: dict[str, dict[str, Any]] = {}
            for target_dir, identity in member_movies.items():
                normalized_dir = _require_normalized_remote_path(
                    target_dir, "metadata.member_movies target"
                )
                if not isinstance(identity, Mapping):
                    raise PlanError("metadata.member_movies 的条目必须是对象")
                _reject_unknown_fields(
                    identity, {"tmdb_id", "title", "year"},
                    f"metadata.member_movies[{target_dir!r}]",
                )
                member_tmdb_id = identity.get("tmdb_id")
                if (
                    isinstance(member_tmdb_id, bool)
                    or not isinstance(member_tmdb_id, int)
                    or member_tmdb_id <= 0
                ):
                    raise PlanError("metadata.member_movies.tmdb_id 必须是正整数")
                member_title = _require_string(
                    identity.get("title"), "metadata.member_movies.title"
                )
                if _has_unsafe_unicode(member_title):
                    raise PlanError("metadata.member_movies.title 包含控制或不可见格式字符")
                member_year = _require_string(
                    identity.get("year"), "metadata.member_movies.year"
                )
                if member_year != "未知年份" and not re.fullmatch(r"(?:19|20)\d{2}", member_year):
                    raise PlanError("metadata.member_movies.year 必须是四位年份或 未知年份")
                normalized_movies[normalized_dir] = {
                    "tmdb_id": member_tmdb_id,
                    "title": member_title,
                    "year": member_year,
                }
            result["member_movies"] = normalized_movies
    if mode == "batch":
        member_tv = raw.get("member_tv")
        if not isinstance(member_tv, Mapping):
            raise PlanError("计划字段 metadata.member_tv 必须是对象")
        normalized_tv: dict[str, dict[str, Any]] = {}
        allowed_tv_fields = {
            "tmdb_id", "title", "original_title", "year", "poster_path", "backdrop_path", "season_posters"
        }
        for series_root, identity in member_tv.items():
            normalized_root = _require_normalized_remote_path(
                series_root, "metadata.member_tv target"
            )
            if not isinstance(identity, Mapping):
                raise PlanError("metadata.member_tv 的条目必须是对象")
            _reject_unknown_fields(
                identity, allowed_tv_fields, f"metadata.member_tv[{series_root!r}]"
            )
            member_tmdb_id = identity.get("tmdb_id")
            if (
                isinstance(member_tmdb_id, bool)
                or not isinstance(member_tmdb_id, int)
                or member_tmdb_id <= 0
            ):
                raise PlanError("metadata.member_tv.tmdb_id 必须是正整数")
            member_title = _require_string(identity.get("title"), "metadata.member_tv.title")
            if _has_unsafe_unicode(member_title):
                raise PlanError("metadata.member_tv.title 包含控制或不可见格式字符")
            member_original_title = identity.get("original_title")
            if member_original_title is not None:
                member_original_title = _require_string(
                    member_original_title, "metadata.member_tv.original_title"
                )
                if _has_unsafe_unicode(member_original_title):
                    raise PlanError("metadata.member_tv.original_title 包含控制或不可见格式字符")
            member_year = _require_string(identity.get("year"), "metadata.member_tv.year")
            if member_year != "未知年份" and not re.fullmatch(r"(?:19|20)\d{2}", member_year):
                raise PlanError("metadata.member_tv.year 必须是四位年份或 未知年份")
            normalized_identity: dict[str, Any] = {
                "tmdb_id": member_tmdb_id,
                "title": member_title,
                "original_title": member_original_title,
                "year": member_year,
                "poster_path": validate_image_path(
                    identity.get("poster_path"), "metadata.member_tv.poster_path"
                ),
                "backdrop_path": validate_image_path(
                    identity.get("backdrop_path"), "metadata.member_tv.backdrop_path"
                ),
            }
            raw_seasons = identity.get("season_posters")
            if raw_seasons is not None:
                if not isinstance(raw_seasons, Mapping):
                    raise PlanError("metadata.member_tv.season_posters 必须是对象")
                seasons: dict[str, str] = {}
                for key, image_path in raw_seasons.items():
                    if not isinstance(key, str) or not re.fullmatch(r"\d{1,3}", key):
                        raise PlanError("metadata.member_tv.season_posters 的季度键无效")
                    seasons[str(int(key))] = validate_image_path(
                        image_path, f"metadata.member_tv.season_posters[{key!r}]"
                    ) or ""
                normalized_identity["season_posters"] = seasons
            normalized_tv[normalized_root] = normalized_identity
        result["member_tv"] = normalized_tv
    return result


def plan_from_dict(raw: Mapping[str, Any]) -> Plan:
    if not isinstance(raw, Mapping):
        raise PlanError("计划内容必须是 JSON 对象")
    _reject_unknown_fields(
        raw,
        {
            "mode", "source_root", "target_root", "warnings", "metadata", "files",
            "cleanup_files", "problem_files", "notices", "decision_trace", "scan_report",
        },
        "plan",
    )
    mode = _require_string(raw.get("mode"), "mode")
    if mode not in {"tv", "movie", "collection", "mixed", "batch"}:
        raise PlanError(f"计划字段 mode 无效: {mode!r}")
    source_root = _require_normalized_remote_path(raw.get("source_root"), "source_root")
    target_root = _require_normalized_remote_path(raw.get("target_root"), "target_root")
    warnings_raw = raw.get("warnings", [])
    metadata_raw = raw.get("metadata", {})
    files_raw = raw.get("files")
    cleanup_raw = raw.get("cleanup_files", [])
    problems_raw = raw.get("problem_files", [])
    notices_raw = raw.get("notices", [])
    decision_trace = _require_json_object(raw.get("decision_trace", {}), "decision_trace")
    scan_report = _require_json_object(raw.get("scan_report", {}), "scan_report")
    if not isinstance(warnings_raw, list) or not all(isinstance(item, str) for item in warnings_raw):
        raise PlanError("计划字段 warnings 必须是字符串数组")
    if any(_has_unsafe_unicode(item) for item in warnings_raw):
        raise PlanError("计划字段 warnings 包含控制或不可见格式字符")
    if not isinstance(metadata_raw, dict):
        raise PlanError("计划字段 metadata 必须是对象")
    metadata = _validate_plan_metadata(mode, metadata_raw)
    if not isinstance(files_raw, list):
        raise PlanError("计划字段 files 必须是数组")
    if not isinstance(cleanup_raw, list):
        raise PlanError("计划字段 cleanup_files 必须是数组")
    if not isinstance(problems_raw, list):
        raise PlanError("计划字段 problem_files 必须是数组")
    if not isinstance(notices_raw, list):
        raise PlanError("计划字段 notices 必须是数组")

    files: list[PlannedFile] = []
    allowed_file_fields = {
        "source_path", "source_dir", "original_name", "final_name", "target_dir",
        "media_kind", "episode_key", "source_size", "source_modified", "source_hash",
    }
    for index, item in enumerate(files_raw):
        if not isinstance(item, Mapping):
            raise PlanError(f"计划文件项 {index} 必须是对象")
        _reject_unknown_fields(item, allowed_file_fields, f"files[{index}]")
        media = _require_string(item.get("media_kind"), f"files[{index}].media_kind")
        if media not in {"video", "subtitle"}:
            raise PlanError(f"计划文件项 {index} media_kind 无效: {media!r}")
        files.append(
            PlannedFile(
                source_path=_require_normalized_remote_path(
                    item.get("source_path"), f"files[{index}].source_path"
                ),
                source_dir=_require_normalized_remote_path(
                    item.get("source_dir"), f"files[{index}].source_dir"
                ),
                original_name=_require_remote_basename(
                    item.get("original_name"), f"files[{index}].original_name"
                ),
                final_name=_require_remote_basename(
                    item.get("final_name"), f"files[{index}].final_name"
                ),
                target_dir=_require_normalized_remote_path(
                    item.get("target_dir"), f"files[{index}].target_dir"
                ),
                media_kind=media,
                episode_key=_optional_string(item.get("episode_key"), f"files[{index}].episode_key"),
                source_size=_optional_nonnegative_int(item.get("source_size"), f"files[{index}].source_size"),
                source_modified=_optional_string(item.get("source_modified"), f"files[{index}].source_modified"),
                source_hash=_optional_string(item.get("source_hash"), f"files[{index}].source_hash"),
            )
        )
    cleanup_files: list[PlannedCleanup] = []
    allowed_cleanup_fields = {
        "source_path", "source_dir", "original_name", "reason",
        "source_size", "source_modified", "source_hash",
    }
    for index, item in enumerate(cleanup_raw):
        if not isinstance(item, Mapping):
            raise PlanError(f"计划清理项 {index} 必须是对象")
        _reject_unknown_fields(item, allowed_cleanup_fields, f"cleanup_files[{index}]")
        reason = _require_string(item.get("reason"), f"cleanup_files[{index}].reason")
        static_cleanup_reasons = {
            "macOS AppleDouble 隐藏文件",
            "无字幕片头/片尾视频",
            "无字幕片头/片尾/光盘菜单视频",
            "经特典目录与同集正片交叉确认的片头/片尾视频",
            "发布组广告图片",
            "字体资源包",
            "特典动画广告/Animated Magia Report Commercial",
            LIBRARY_REMEDIATION_CLEANUP_REASON,
        }
        generated_cleanup_prefixes = (
            "同一 TMDB 集号已有更高清晰度版本 ",
            "同一 TMDB 集号的更高清晰度版本已有对应字幕 ",
            "同一 TMDB 集号已有同清晰度的内封/软字幕版本 ",
            "同一 TMDB 集号已有同清晰度但文件更完整的版本 ",
            "同一 TMDB 集号已有同清晰度同字幕形态的简体中文字幕版本 ",
            "同一 TMDB 电影 movie/",
        )
        if (
            reason not in static_cleanup_reasons
            and not reason.startswith(generated_cleanup_prefixes)
        ):
            raise PlanError(f"计划清理项 {index} reason 无效: {reason!r}")
        cleanup_files.append(
            PlannedCleanup(
                source_path=_require_normalized_remote_path(
                    item.get("source_path"), f"cleanup_files[{index}].source_path"
                ),
                source_dir=_require_normalized_remote_path(
                    item.get("source_dir"), f"cleanup_files[{index}].source_dir"
                ),
                original_name=_require_remote_basename(
                    item.get("original_name"), f"cleanup_files[{index}].original_name"
                ),
                reason=reason,
                source_size=_optional_nonnegative_int(
                    item.get("source_size"), f"cleanup_files[{index}].source_size"
                ),
                source_modified=_optional_string(
                    item.get("source_modified"), f"cleanup_files[{index}].source_modified"
                ),
                source_hash=_optional_string(
                    item.get("source_hash"), f"cleanup_files[{index}].source_hash"
                ),
            )
        )
    problem_files: list[PlannedProblem] = []
    allowed_problem_fields = {"source_path", "reason", "target_path"}
    for index, item in enumerate(problems_raw):
        if not isinstance(item, Mapping):
            raise PlanError(f"计划问题文件项 {index} 必须是对象")
        _reject_unknown_fields(item, allowed_problem_fields, f"problem_files[{index}]")
        reason = _require_string(item.get("reason"), f"problem_files[{index}].reason")
        if _has_unsafe_unicode(reason):
            raise PlanError(f"计划字段 problem_files[{index}].reason 包含控制或不可见格式字符")
        target_path = item.get("target_path")
        problem_files.append(
            PlannedProblem(
                source_path=_require_normalized_remote_path(
                    item.get("source_path"), f"problem_files[{index}].source_path"
                ),
                reason=reason,
                target_path=(
                    _require_normalized_remote_path(
                        target_path, f"problem_files[{index}].target_path"
                    )
                    if target_path is not None
                    else None
                ),
            )
        )
    notices: list[PlanNotice] = []
    for index, item in enumerate(notices_raw):
        if not isinstance(item, Mapping):
            raise PlanError(f"计划通知项 {index} 必须是对象")
        _reject_unknown_fields(
            item,
            {"code", "severity", "requires_review", "message", "evidence"},
            f"notices[{index}]",
        )
        code = _require_string(item.get("code"), f"notices[{index}].code")
        if not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", code):
            raise PlanError(f"计划通知项 {index} code 无效: {code!r}")
        severity = _require_string(item.get("severity"), f"notices[{index}].severity")
        if severity not in {"info", "warning", "error"}:
            raise PlanError(f"计划通知项 {index} severity 无效: {severity!r}")
        requires_review = item.get("requires_review")
        if not isinstance(requires_review, bool):
            raise PlanError(f"计划通知项 {index} requires_review 必须是布尔值")
        message = _require_string(item.get("message"), f"notices[{index}].message")
        if _has_unsafe_unicode(message):
            raise PlanError(f"计划通知项 {index} message 包含控制或不可见格式字符")
        notices.append(
            PlanNotice(
                code=code,
                severity=severity,
                requires_review=requires_review,
                message=message,
                evidence=_require_json_object(
                    item.get("evidence", {}), f"notices[{index}].evidence"
                ),
            )
        )
    return Plan(
        mode=mode,
        source_root=source_root,
        target_root=target_root,
        files=files,
        warnings=list(warnings_raw),
        metadata=metadata,
        cleanup_files=cleanup_files,
        problem_files=problem_files,
        notices=notices,
        decision_trace=decision_trace,
        scan_report=scan_report,
    )


def write_plan_json(plan: Plan, path: Path) -> str:
    validated_plan = plan_from_dict(plan_to_dict(plan))
    missing_snapshots = [
        item.source_path
        for item in [*validated_plan.files, *validated_plan.cleanup_files]
        if all(
            value is None
            for value in (item.source_size, item.source_modified, item.source_hash)
        )
    ]
    if missing_snapshots:
        preview = ", ".join(missing_snapshots[:3])
        suffix = " ..." if len(missing_snapshots) > 3 else ""
        raise PlanError(
            "计划缺少源文件快照，不能保存为可执行计划: " + preview + suffix
        )
    digest = plan_sha256(validated_plan)
    payload = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "plan_sha256": digest,
        "plan": plan_to_dict(validated_plan),
    }
    _reserve_output_path(path)
    try:
        _write_json_reserved(path, payload)
    except Exception:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise
    return digest


def load_plan_json(path: Path) -> tuple[Plan, str]:
    try:
        payload = _load_json_text(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"无法读取计划文件: {path}; {exc}") from exc
    if not isinstance(payload, dict):
        raise PlanError("计划文件顶层必须是对象")
    _reject_unknown_fields(
        payload, {"schema_version", "created_at", "plan_sha256", "plan"}, "root"
    )
    _require_string(payload.get("created_at"), "created_at")
    if payload.get("schema_version") not in SUPPORTED_PLAN_SCHEMA_VERSIONS:
        raise PlanError(
            f"计划 schema_version 不受支持: {payload.get('schema_version')!r}; "
            f"当前支持 {sorted(SUPPORTED_PLAN_SCHEMA_VERSIONS)}"
        )
    raw_plan = payload.get("plan")
    expected_digest = payload.get("plan_sha256")
    if not isinstance(raw_plan, dict) or not isinstance(expected_digest, str):
        raise PlanError("计划文件缺少 plan 或 plan_sha256")
    actual_digest = plan_sha256(raw_plan)
    if not re.fullmatch(r"[0-9a-f]{64}", expected_digest) or actual_digest != expected_digest:
        raise PlanError(
            f"计划文件 SHA-256 校验失败: expected={expected_digest!r}, actual={actual_digest}"
        )
    plan = plan_from_dict(raw_plan)
    if payload.get("schema_version") == PLAN_SCHEMA_VERSION and plan_sha256(plan) != expected_digest:
        raise PlanError("计划内容在类型解析后发生变化，拒绝执行")
    return plan, expected_digest


def load_execution_journal(
    path: Path,
) -> tuple[ExecutionJournal, str, list[dict[str, Any]]]:
    try:
        raw = _load_json_text(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"无法读取执行 journal: {path}; {exc}") from exc
    if not isinstance(raw, dict):
        raise PlanError("执行 journal 顶层必须是对象")
    _reject_unknown_fields(
        raw,
        {"created_at", "plan_sha256", "plan", "records", "success"},
        "journal",
    )
    created_at = _require_string(raw.get("created_at"), "journal.created_at")
    plan_raw = raw.get("plan")
    expected_plan_digest = raw.get("plan_sha256")
    records_raw = raw.get("records")
    if not isinstance(plan_raw, dict) or not isinstance(expected_plan_digest, str):
        raise PlanError("执行 journal 缺少 plan 或 plan_sha256")
    if plan_sha256(plan_raw) != expected_plan_digest:
        raise PlanError("执行 journal 中的计划 SHA-256 校验失败")
    plan = plan_from_dict(plan_raw)
    if not isinstance(records_raw, list):
        raise PlanError("执行 journal 字段 records 必须是数组")
    records: list[ExecutionRecord] = []
    raw_records: list[dict[str, Any]] = []
    for index, value in enumerate(records_raw):
        if not isinstance(value, dict):
            raise PlanError(f"执行 journal records[{index}] 必须是对象")
        _reject_unknown_fields(
            value, {"action", "source", "target", "status", "message"},
            f"journal.records[{index}]"
        )
        record = ExecutionRecord(
            action=_require_string(value.get("action"), f"journal.records[{index}].action"),
            source=_require_string(
                value.get("source", ""), f"journal.records[{index}].source", allow_empty=True
            ),
            target=_require_string(
                value.get("target", ""), f"journal.records[{index}].target", allow_empty=True
            ),
            status=_require_string(value.get("status"), f"journal.records[{index}].status"),
            message=_require_string(
                value.get("message", ""), f"journal.records[{index}].message", allow_empty=True
            ),
        )
        records.append(record)
        raw_records.append(dict(value))
    success = raw.get("success")
    if type(success) is not bool:
        raise PlanError("执行 journal 字段 success 必须是布尔值")
    digest = hashlib.sha256(_canonical_json_bytes(raw)).hexdigest()
    return (
        ExecutionJournal(created_at, plan_to_dict(plan), records, success),
        digest,
        raw_records,
    )


def _journal_temp_names(
    item: PlannedFile, records: Sequence[Mapping[str, Any]]
) -> set[str]:
    names: set[str] = set()
    source_key = _collision_key(item.source_path)
    final_path_key = _collision_key(join_remote(item.source_dir, item.final_name))
    for record in records:
        action = record.get("action")
        source = record.get("source")
        target = record.get("target")
        if not isinstance(source, str) or not isinstance(target, str):
            continue
        if action == "rename-temp" and _collision_key(source) == source_key:
            names.add(split_remote(target)[1])
        elif action == "rename-final" and _collision_key(target) == final_path_key:
            names.add(split_remote(source)[1])
    return names


def _journal_expected_locations(
    item: PlannedFile, records: Sequence[Mapping[str, Any]]
) -> list[tuple[str, str]]:
    expected: list[tuple[str, str]] = []
    for record in records:
        action = record.get("action")
        status = record.get("status")
        source = record.get("source")
        target = record.get("target")
        message = str(record.get("message") or "")
        if status not in {"ok", "pending", "uncertain"}:
            continue
        if action == "move" and source == item.source_dir and target == item.target_dir:
            if item.final_name in message:
                expected.append((item.target_dir, item.final_name))
        elif action == "rename-final" and isinstance(target, str):
            if _collision_key(target) == _collision_key(
                join_remote(item.source_dir, item.final_name)
            ):
                expected.append((item.source_dir, item.final_name))
        elif action == "rename-temp" and isinstance(source, str) and isinstance(target, str):
            if _collision_key(source) == _collision_key(item.source_path):
                expected.append((item.source_dir, split_remote(target)[1]))
    expected.reverse()
    expected.append((item.source_dir, item.original_name))
    return expected


def inspect_recovery_state(
    alist: AListClient,
    plan: Plan,
    records: Sequence[Mapping[str, Any]],
) -> list[RecoveryState]:
    directories = {
        normalize_remote_path(value)
        for item in plan.files
        for value in (item.source_dir, item.target_dir)
    }
    listings = {
        directory: _directory_file_entries(alist, directory)
        for directory in directories
    }
    states: list[RecoveryState] = []
    occupied_locations: set[tuple[str, str]] = set()
    for item in plan.files:
        candidates = {item.original_name, item.final_name}
        candidates.update(_journal_temp_names(item, records))
        recovery_prefix = _recovery_prefix(item)
        found: list[tuple[str, Mapping[str, Any]]] = []
        for expected_dir, expected_name in _journal_expected_locations(item, records):
            preferred = [
                (expected_dir, entry)
                for entry in listings.get(expected_dir, [])
                if _collision_key(str(entry["name"])) == _collision_key(expected_name)
            ]
            if len(preferred) == 1:
                found = preferred
                break
        if found:
            candidate_directories: set[str] = set()
        else:
            candidate_directories = {item.source_dir, item.target_dir}
        for directory in candidate_directories:
            for entry in listings.get(directory, []):
                name = str(entry["name"])
                if (
                    any(_collision_key(name) == _collision_key(value) for value in candidates)
                    or _collision_key(name).startswith(_collision_key(recovery_prefix))
                ):
                    found.append((directory, entry))
        if len(found) != 1:
            raise PlanError(
                f"恢复前无法唯一定位计划文件: {item.source_path}; "
                f"found={[join_remote(directory, str(entry['name'])) for directory, entry in found]}"
            )
        current_dir, entry = found[0]
        current_name = str(entry["name"])
        location_key = (_collision_key(current_dir), _collision_key(current_name))
        if location_key in occupied_locations:
            raise PlanError(f"恢复状态中同一文件被多个计划项占用: {current_dir}/{current_name}")
        occupied_locations.add(location_key)
        actual_size = _entry_size_value(entry)
        actual_hash = _entry_hash_value(entry)
        if item.source_size is not None and actual_size != item.source_size:
            raise PlanError(
                f"恢复前文件大小与计划不一致: {current_dir}/{current_name}; "
                f"expected={item.source_size!r}, actual={actual_size!r}"
            )
        if item.source_hash is not None and actual_hash != item.source_hash:
            raise PlanError(
                f"恢复前文件哈希与计划不一致: {current_dir}/{current_name}"
            )
        states.append(RecoveryState(item, current_dir, current_name, entry))

    planned_locations = {
        (_collision_key(state.current_dir), _collision_key(state.current_name))
        for state in states
    }
    for state in states:
        original_key = (
            _collision_key(state.item.source_dir),
            _collision_key(state.item.original_name),
        )
        if original_key in planned_locations:
            continue
        occupants = [
            entry
            for entry in listings.get(state.item.source_dir, [])
            if _collision_key(str(entry["name"])) == _collision_key(state.item.original_name)
        ]
        if occupants:
            raise PlanError(
                f"恢复目标原文件名已被计划外条目占用: "
                f"{state.item.source_dir}/{state.item.original_name}"
            )
    return states


def _ensure_recovery_source_dirs(
    alist: AListClient,
    states: Sequence[RecoveryState],
    journal: ExecutionJournal,
    journal_path: Path,
) -> None:
    """Recreate exact original parents that post-commit cleanup removed."""
    missing: set[str] = set()
    for state in states:
        current = normalize_remote_path(state.item.source_dir)
        while current != "/" and alist.try_list(current, refresh=True) is None:
            missing.add(current)
            parent, _name = split_remote(current)
            if parent == current:
                break
            current = parent
    for directory in sorted(missing, key=lambda path: path.count("/")):
        record = _append_pending(
            journal, journal_path, "recover-mkdir", "", directory,
        )
        alist.mkdir(directory)
        visible = False
        for delay in (0.0, 0.15, 0.35, 0.75, 1.5):
            if delay:
                time.sleep(delay)
            if alist.try_list(directory, refresh=True) is not None:
                visible = True
                break
        if not visible:
            _mark_record(
                journal, journal_path, record, "uncertain",
                "directory not visible after recovery create",
            )
            raise ApiError(f"AList 恢复目录新建后暂不可见: {directory}")
        _mark_record(journal, journal_path, record, "ok", "created")


def recover_execution(
    alist: AListClient,
    plan: Plan,
    records: Sequence[Mapping[str, Any]],
    *,
    recovery_journal_path: Path,
) -> None:
    states = inspect_recovery_state(alist, plan, records)
    _reserve_output_path(recovery_journal_path)
    transaction_stage_root = _remote_transaction_stage_root(recovery_journal_path)
    transaction_scope = f"recovery-{plan_sha256(plan)}"
    recovery = ExecutionJournal(
        created_at=datetime.now(timezone.utc).isoformat(),
        plan=plan_to_dict(plan),
        records=[],
    )
    try:
        recovery.save(recovery_journal_path)
    except Exception:
        try:
            recovery_journal_path.unlink()
        except FileNotFoundError:
            pass
        raise

    _ensure_recovery_source_dirs(
        alist, states, recovery, recovery_journal_path,
    )

    temporary: list[tuple[RecoveryState, str]] = []
    # Stage every recovery source before changing any user file.  This keeps
    # swap/cycle recovery possible even if a later provider request fails.
    for state in states:
        temp_name = _recovery_name(state.item)
        source_path = join_remote(state.current_dir, state.current_name)
        record = _append_pending(
            recovery,
            recovery_journal_path,
            "recover-stage-temp",
            source_path,
            join_remote(state.current_dir, temp_name),
        )
        _prepare_remote_transfer(
            alist,
            source_path,
            join_remote(state.current_dir, temp_name),
            stage_root=transaction_stage_root,
            transaction_scope=f"{transaction_scope}-temp",
            expected_size=state.item.source_size,
        )
        temporary.append((state, temp_name))
        _mark_record(
            recovery, recovery_journal_path, record, "ok", "durable local SHA-256 stage",
        )

    for state, temp_name in temporary:
        source_path = join_remote(state.current_dir, state.current_name)
        temp_path = join_remote(state.current_dir, temp_name)
        record = _append_pending(
            recovery,
            recovery_journal_path,
            "recover-rename-temp",
            source_path,
            temp_path,
        )
        _execute_remote_transfer(
            alist,
            source_path,
            temp_path,
            stage_root=transaction_stage_root,
            transaction_scope=f"{transaction_scope}-temp",
            expected_size=state.item.source_size,
        )
        _mark_record(recovery, recovery_journal_path, record, "ok")

    for state, temp_name in temporary:
        if _collision_key(state.current_dir) == _collision_key(state.item.source_dir):
            continue
        record = _append_pending(
            recovery,
            recovery_journal_path,
            "recover-move",
            state.current_dir,
            state.item.source_dir,
            temp_name,
        )
        _move_with_reconciliation(
            alist,
            state.current_dir,
            state.item.source_dir,
            [temp_name],
            stage_root=transaction_stage_root,
            transaction_scope=transaction_scope,
            expected_sizes={temp_name: state.item.source_size},
        )
        _mark_record(recovery, recovery_journal_path, record, "ok")

    for state, temp_name in temporary:
        current_path = join_remote(state.item.source_dir, temp_name)
        record = _append_pending(
            recovery,
            recovery_journal_path,
            "recover-rename-final",
            current_path,
            state.item.source_path,
        )
        _execute_remote_transfer(
            alist,
            current_path,
            state.item.source_path,
            stage_root=transaction_stage_root,
            transaction_scope=f"{transaction_scope}-final",
            expected_size=state.item.source_size,
        )
        _mark_record(recovery, recovery_journal_path, record, "ok")

    # A remote provider is allowed to rewrite mtime during our own recovery
    # rename/move operations. Size/hash and exact original location/name still
    # protect identity; comparing the pre-execution mtime here makes a fully
    # restored task look broken and prevents an idempotent retry.
    validate_source_state(
        alist,
        plan,
        require_snapshot=True,
        ignore_modified=True,
        # Cleanup rows were intentionally deleted by the original execution
        # and have no recovery copy.  Recovery is complete when every moved
        # media/subtitle item is back at its exact original path and identity;
        # requiring deleted NCOP/NCED/advertisement rows here makes a valid
        # rollback impossible after media was already committed.
        include_cleanup=False,
    )
    lock_paths = {
        str(record.get("target"))
        for record in records
        if record.get("action") == "acquire-lock"
        and isinstance(record.get("target"), str)
        and record.get("target")
    }
    for lock_path in sorted(lock_paths):
        parent, name = split_remote(lock_path)
        matches = _collision_presence(_directory_file_names(alist, parent), name)
        if len(matches) == 1 and matches[0] == name:
            record = _append_pending(
                recovery, recovery_journal_path, "recover-release-lock", lock_path, ""
            )
            alist.remove(parent, [name])
            _mark_record(recovery, recovery_journal_path, record, "ok")
        elif matches:
            raise ApiError(f"恢复后整理锁状态不明确，未删除: {lock_path}; matches={matches}")
    recovery.success = True
    recovery.save(recovery_journal_path)


def print_plan(plan: Plan) -> None:
    icons = {"tv": "📺", "movie": "🎬", "collection": "📦", "mixed": "🎞️"}
    print(f"\n{icons.get(plan.mode, '📁')} 模式: {plan.mode}")
    print(f"   源目录: {_terminal_text(plan.source_root)}")
    print(f"   目标目录: {_terminal_text(plan.target_root)}")
    print(f"   文件数: {len(plan.files)}")
    for warning in plan.warnings:
        print(f"   ⚠️  {_terminal_text(warning)}")
    print("   计划:")
    for item in plan.files:
        relative = item.source_path[len(plan.source_root) :].lstrip("/")
        source_label = relative or item.original_name
        print(
            f"      {_terminal_text(source_label)} → "
            f"{_terminal_text(item.target_dir)}/{_terminal_text(item.final_name)}"
        )


def _retain_one_subtitle_track_per_video(plan: Plan) -> None:
    """Keep one preferred external subtitle track for each planned video.

    A release may bundle several fansub translations and a second
    ``子集化字幕`` copy of every ASS file.  Moving all of them produces
    opaque ``.2/.3`` tracks in Infuse.  Preserve one deterministic, useful
    track while leaving the alternatives untouched at source.  IDX/SUB pairs
    share one target stem and are retained together as one logical track.
    """
    video_keys = {
        _planned_companion_key(item.target_dir, item.final_name)
        for item in plan.files
        if item.media_kind == "video"
    }
    tracks: dict[
        tuple[str, str],
        dict[str, list[PlannedFile]],
    ] = defaultdict(lambda: defaultdict(list))
    for item in plan.files:
        if item.media_kind != "subtitle":
            continue
        companion_key = _planned_companion_key(item.target_dir, item.final_name)
        if companion_key not in video_keys:
            continue
        track_key = _collision_key(Path(item.final_name).stem)
        tracks[companion_key][track_key].append(item)

    def track_rank(items: Sequence[PlannedFile]) -> tuple[Any, ...]:
        representative = min(items, key=lambda item: _collision_key(item.source_path))
        language = subtitle_language(representative.final_name)
        language_rank = {
            "zh-CN": 0,
            "zh-TW": 1,
            "en": 2,
            "ja": 3,
            None: 4,
        }.get(language, 5)
        source_key = unicodedata.normalize("NFKC", representative.source_path).casefold()
        subset_rank = 1 if re.search(r"(?:^|/)子集化字幕(?:/|$)", source_key) else 0
        extension_rank = {
            ".ass": 0,
            ".ssa": 1,
            ".srt": 2,
            ".vtt": 3,
            ".idx": 4,
            ".sub": 4,
            ".sup": 5,
        }.get(Path(representative.final_name).suffix.lower(), 10)
        canonical_suffix_rank = 0 if re.search(
            r"\.(?:zh-CN|zh-TW|en|ja)$|\.subtitle$",
            Path(representative.final_name).stem,
            re.I,
        ) else 1
        return (
            language_rank,
            subset_rank,
            extension_rank,
            canonical_suffix_rank,
            _collision_key(representative.source_path),
        )

    demoted_ids: set[int] = set()
    demoted: list[dict[str, Any]] = []
    for companion_key, track_groups in tracks.items():
        if len(track_groups) <= 1:
            continue
        preferred_key, preferred_items = min(
            track_groups.items(),
            key=lambda pair: track_rank(pair[1]),
        )
        preferred_path = min(
            (item.source_path for item in preferred_items),
            key=_collision_key,
        )
        for track_key, items in track_groups.items():
            if track_key == preferred_key:
                continue
            for item in items:
                demoted_ids.add(id(item))
                demoted.append({
                    "source_path": item.source_path,
                    "planned_target_path": join_remote(item.target_dir, item.final_name),
                    "action": "defer_until_exact_video_subtitle_closure",
                    "reason": "alternate_subtitle_track",
                    "preferred_source_path": preferred_path,
                })
    if not demoted_ids:
        return
    plan.files = [item for item in plan.files if id(item) not in demoted_ids]
    deferred = plan.scan_report.setdefault("deferred_subtitles", [])
    if not isinstance(deferred, list):
        raise PlanError("scan_report.deferred_subtitles 必须是数组")
    existing_deferred = {
        _collision_key(str(item.get("source_path")))
        for item in deferred if isinstance(item, Mapping)
    }
    deferred.extend(
        item for item in demoted
        if _collision_key(str(item["source_path"])) not in existing_deferred
    )
    plan.warnings.append(
        f"同一视频的多份外挂字幕仅保留 1 条首选轨道；"
        f"{len(demoted_ids)} 个备选字幕已保留在源目录"
    )


UNMAPPED_VIDEO_SUMMARY_RE = re.compile(
    r"^\d+ 个无法唯一识别的附加视频(?:已保留原位|将保留原位待人工确认)"
    r"；其余媒体在问题闭合前不得执行$"
)


def _synchronize_unmapped_video_warning(plan: Plan) -> None:
    """Keep the aggregate warning count equal to its problem-file details.

    Batch plans deduplicate identical child warnings. Two child plans each
    reporting two unresolved videos could therefore produce one misleading
    ``2 videos`` warning beside four problem rows. Rebuild this one aggregate
    from the authoritative problem list before notices are classified.
    """
    problems = [
        problem
        for problem in plan.problem_files
        if problem.reason.startswith("无法唯一识别的附加视频")
    ]
    matching_indices = [
        index
        for index, warning in enumerate(plan.warnings)
        if UNMAPPED_VIDEO_SUMMARY_RE.fullmatch(warning)
    ]
    if not problems:
        if matching_indices:
            plan.warnings = [
                warning
                for warning in plan.warnings
                if not UNMAPPED_VIDEO_SUMMARY_RE.fullmatch(warning)
            ]
            plan.notices = [
                notice
                for notice in plan.notices
                if not UNMAPPED_VIDEO_SUMMARY_RE.fullmatch(notice.message)
            ]
        return
    summary = (
        f"{len(problems)} 个无法唯一识别的附加视频"
        "将保留原位待人工确认；其余媒体在问题闭合前不得执行"
    )
    insertion_index = matching_indices[0] if matching_indices else len(plan.warnings)
    retained = [
        warning
        for warning in plan.warnings
        if not UNMAPPED_VIDEO_SUMMARY_RE.fullmatch(warning)
    ]
    retained.insert(min(insertion_index, len(retained)), summary)
    plan.warnings = retained
    # Be idempotent if evidence finalization is repeated on a deserialized plan.
    plan.notices = [
        notice
        for notice in plan.notices
        if not UNMAPPED_VIDEO_SUMMARY_RE.fullmatch(notice.message)
    ]


def finalize_plan_evidence(plan: Plan) -> None:
    """Attach stable notice codes and a compact scan report before serialization."""
    prior_deferred_subtitles = [
        dict(row)
        for row in (plan.scan_report.get("deferred_subtitles") or [])
        if isinstance(row, Mapping)
    ]
    _retain_one_subtitle_track_per_video(plan)
    current_deferred_subtitles = [
        dict(row)
        for row in (plan.scan_report.get("deferred_subtitles") or [])
        if isinstance(row, Mapping)
    ]
    _synchronize_unmapped_video_warning(plan)
    existing_messages = {notice.message for notice in plan.notices}
    for warning in plan.warnings:
        if warning in existing_messages:
            continue
        proven_safe = any(pattern.search(warning) for pattern in PROVEN_SAFE_WARNING_PATTERNS)
        evidence_extra: dict[str, Any] = {}
        complete_boundary = COMPLETE_OFFICIAL_SEASON_BOUNDARY_RE.fullmatch(warning)
        corresponding_problems = (
            [
                problem
                for problem in plan.problem_files
                if problem.reason.startswith("无法唯一识别的附加视频")
            ]
            if UNMAPPED_VIDEO_SUMMARY_RE.fullmatch(warning)
            else [
                problem for problem in plan.problem_files
                if problem.reason == warning
            ]
        )
        if complete_boundary is not None:
            season_number = int(complete_boundary.group(1))
            episode_numbers = sorted({
                int(match.group(1))
                for item in plan.files
                if item.media_kind == "video"
                and re.search(
                    rf"(?:^|/)Season\s+0*{season_number}(?:$|/)",
                    item.target_dir,
                    re.I,
                )
                and (
                    match := re.fullmatch(
                        r"E(\d{1,4})", str(item.episode_key or ""), re.I
                    )
                ) is not None
            })
            code = "complete_official_season_boundary"
            evidence_kind = "official_episode_boundary"
            proven_safe = True
            evidence_extra = {
                "season": season_number,
                "source_episode_numbers": episode_numbers,
                **(
                    {"official_episode_count": len(episode_numbers)}
                    if episode_numbers == list(range(1, len(episode_numbers) + 1))
                    else {}
                ),
                **(
                    {"tmdb_id": int(plan.metadata["tmdb_id"])}
                    if isinstance(plan.metadata.get("tmdb_id"), int)
                    and not isinstance(plan.metadata.get("tmdb_id"), bool)
                    else {}
                ),
            }
        elif corresponding_problems:
            code = "blocked_unclosed_problem_files"
            evidence_kind = "problem_files_preserved_at_source"
            proven_safe = False
            evidence_extra = {
                "source_paths": sorted(
                    {problem.source_path for problem in corresponding_problems},
                    key=_collision_key,
                ),
            }
        elif warning.startswith("确认执行后将删除"):
            proven_safe = bool(plan.cleanup_files) and all(
                _cleanup_is_generated_housekeeping(item) for item in plan.cleanup_files
            )
            code = (
                "housekeeping_cleanup"
                if proven_safe
                else "destructive_cleanup_requires_review"
            )
            evidence_kind = (
                "generated_filesystem_litter"
                if proven_safe
                else "user_file_deletion"
            )
        elif re.search(r"(?:OVA|OAV|OAD|OVBSP|特别篇|SP\d+|Season 00|合并集)", warning, re.I):
            code = "special_mapping_evidence"
            if re.search(r"(?:明确编号.*Season 00|按明确编号保留)", warning, re.I):
                evidence_kind = "explicit_sp_number_fallback"
                proven_safe = False
            elif re.search(r"(?:超出.*正片集数|溢出).*Season 00", warning, re.I):
                evidence_kind = "overflow_episode_fallback"
                proven_safe = False
            elif re.search(r"(?:官方开播时间.*完整时长|官方时间线|时间线.*时长|runtime)", warning, re.I):
                evidence_kind = "timeline_runtime_match"
            elif re.search(r"(?:官方特别篇标题|官方特别篇标签|子系列标题|多语言官方标题|标题.*唯一确认)", warning, re.I):
                evidence_kind = "official_title_match"
            elif re.search(r"(?:独立.*(?:电影|Movie)|(?:电影|Movie).*独立)", warning, re.I):
                evidence_kind = "independent_movie"
            elif re.search(r"(?:独立.*(?:TV|剧集)|(?:TV|剧集).*独立)", warning, re.I):
                evidence_kind = "independent_tv"
            else:
                evidence_kind = "official_tmdb_episode"
        elif re.search(r"(?:NFO|TMDB ID|tvshow\.nfo)", warning, re.I):
            code = "library_identity_evidence"
            evidence_kind = "existing_library_identity"
        else:
            code = "proven_safe_planning_action" if proven_safe else "planning_warning_requires_review"
            evidence_kind = "planning_rule"
        plan.notices.append(PlanNotice(
            code=code,
            severity="info" if proven_safe else "warning",
            requires_review=not proven_safe,
            message=warning,
            evidence={
                "classification": "engine_generated",
                "evidence_kind": evidence_kind,
                **evidence_extra,
                **(
                    {"mapping_source": evidence_kind}
                    if code == "special_mapping_evidence"
                    else {}
                ),
            },
        ))
    media_sources = {item.source_path for item in plan.files}
    cleanup_sources = {item.source_path for item in plan.cleanup_files}
    problem_sources = {item.source_path for item in plan.problem_files}
    resource_gaps = [
        dict(gap)
        for gap in (plan.scan_report.get("resource_gaps") or [])
        if isinstance(gap, Mapping)
    ]
    gap_files = {
        _collision_key(path)
        for gap in resource_gaps
        for path in (gap.get("files") or [])
        if isinstance(path, str)
    }
    for problem in plan.problem_files:
        if not re.search(
            r"(?:无对应视频|没有同名视频|字幕将保留原位|保留原位待人工确认)",
            problem.reason,
        ):
            continue
        if _collision_key(problem.source_path) in gap_files:
            continue
        resource_gaps.append(_resource_gap(
            "subtitle_without_video",
            split_remote(problem.source_path)[1],
            problem.reason,
            files=[problem.source_path],
        ))
        gap_files.add(_collision_key(problem.source_path))
    plan.scan_report = {
        "total_files": len(media_sources | cleanup_sources | problem_sources),
        "matched_files": len(media_sources - problem_sources),
        "skipped_files": len((cleanup_sources | problem_sources) - media_sources),
        "anomaly_files": len(problem_sources),
        "cleanup_files": len(plan.cleanup_files),
        "source_root": plan.source_root,
        "target_root": plan.target_root,
        "routing_decision": asdict(placement_for(plan.source_root, plan.target_root)),
    }
    deferred_by_source = {
        _collision_key(str(row.get("source_path"))): row
        for row in [*prior_deferred_subtitles, *current_deferred_subtitles]
        if isinstance(row.get("source_path"), str)
    }
    if deferred_by_source:
        plan.scan_report["deferred_subtitles"] = [
            deferred_by_source[key] for key in sorted(deferred_by_source)
        ]
    if resource_gaps:
        plan.scan_report["resource_gaps"] = resource_gaps


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser():
    return _package_build_parser(version=__version__, default_alist_url=DEFAULT_ALIST_URL)


def _search_tmdb(client: TMDBClient, query: str) -> None:
    print("【剧集搜索结果】")
    for item in client.get("/search/tv", query=query).get("results", [])[:5]:
        year = _extract_year(item.get("first_air_date"))
        print(f"  TMDB ID: {int(item['id']):>7d} | {year} | {_terminal_text(item.get('name', ''))}")
    print("\n【电影搜索结果】")
    for item in client.get("/search/movie", query=query).get("results", [])[:5]:
        year = _extract_year(item.get("release_date"))
        print(f"  TMDB ID: {int(item['id']):>7d} | {year} | {_terminal_text(item.get('title', ''))}")
    print("\n【合集搜索结果】")
    for item in client.get("/search/collection", query=query).get("results", [])[:5]:
        print(f"  TMDB ID: {int(item['id']):>7d} | {_terminal_text(item.get('name', ''))}")


def _normalize_match_title(value: str) -> str:
    cleaned = re.sub(
        r"^\s*[A-Za-z]\s+(?=(?:(?:4k|8k|2160p|1080p|720p|480p)\b|[\u3400-\u9fff]))",
        "",
        value,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\[[^\]]*\]|\([^)]*(?:1080|2160|720|x26|hevc)[^)]*\)", " ", cleaned)
    cleaned = re.sub(
        r"\b(?:4k|8k|2160p|1080p|720p|480p|bluray|blu-ray|web-?dl|webrip|x26[45]|hevc|av1)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"(?:19|20)\d{2}", " ", cleaned)
    return "".join(char for char in unicodedata.normalize("NFKC", cleaned).casefold() if char.isalnum())


def _title_similarity(query_key: str, title: str) -> float:
    """Score official expanded titles without weakening ambiguous short queries."""
    title_key = _normalize_match_title(title)
    similarity = difflib.SequenceMatcher(None, query_key, title_key).ratio()
    if min(len(query_key), len(title_key)) < 4:
        return similarity
    if query_key == title_key:
        return 1.0
    if title_key.startswith(query_key) or query_key.startswith(title_key):
        return max(similarity, 0.96)
    if query_key in title_key or title_key in query_key:
        return max(similarity, 0.92)
    return similarity


def _search_query_variants(query: str) -> list[str]:
    """Return bounded punctuation fallbacks for TMDB search."""
    normalized = unicodedata.normalize("NFKC", query)
    relaxed = re.sub(r"[^\w\u3400-\u9fff]+", " ", normalized)
    relaxed = re.sub(r"\s+", " ", relaxed).strip()
    variants = [query.strip()]
    # A few ingest libraries use a single Latin shelf letter directly before
    # a long CJK title (without the usual separating space).  Treat the
    # letterless form as a search fallback only: the original query remains
    # first and TMDB still has to return a high-confidence title match.  The
    # long-tail requirement deliberately excludes real short titles such as
    # ``X战警``.
    attached_shelf = re.sub(
        r"^\s*[A-Za-z](?=[\u3400-\u9fff]{6,})", "", normalized,
    ).strip()
    if attached_shelf != normalized and attached_shelf:
        variants.append(attached_shelf)
    # Release folders often encode a month as ``(2013.10)``.  A preceding
    # punctuation-normalization pass can turn that into ``(2013 10)``; remove
    # the whole parenthetical date rather than leaving a stray ``10`` that
    # changes the movie title sent to TMDB.
    without_parenthetical_date = re.sub(
        r"\s*[\uff08(](?:19|20)\d{2}(?:[.\-/\s]\d{1,2})?[)\uff09]\s*",
        " ",
        normalized,
    )
    without_parenthetical_date = re.sub(
        r"\s+", " ", without_parenthetical_date
    ).strip()
    if (
        without_parenthetical_date != normalized
        and without_parenthetical_date
        and without_parenthetical_date not in variants
    ):
        variants.append(without_parenthetical_date)
    year_source = (
        without_parenthetical_date
        if without_parenthetical_date != normalized
        else normalized
    )
    without_year = re.sub(
        r"\s*[（(]?(?:19|20)\d{2}"
        r"(?:(?:[.\-/])(?:(?:19|20)\d{2}|\d{1,2}))?[)）]?\s*",
        " ",
        year_source,
    )
    without_year = re.sub(r"\s+", " ", without_year).strip()
    if without_year != normalized and without_year and without_year not in variants:
        variants.append(without_year)
    if relaxed and relaxed not in variants:
        variants.append(relaxed)
    # TMDB Chinese localization alternates between “物语” and “故事” for
    # the same subtitle. Add one bounded word variant; normal candidate
    # scoring must still prove the work before it can be selected.
    lexical_base = without_year or without_parenthetical_date or normalized
    if "物语" in lexical_base:
        story_variant = lexical_base.replace("物语", "故事")
        if story_variant not in variants:
            variants.append(story_variant)
    elif "故事" in lexical_base:
        tale_variant = lexical_base.replace("故事", "物语")
        if tale_variant not in variants:
            variants.append(tale_variant)
    # Common Chinese release-title wording differs from TMDB by the optional
    # intensifier 神 (for example “神圣之星” vs “圣星”).  Preserve the original
    # query and add only this bounded lexical variant; never globally delete
    # 神 from unrelated titles.
    sacred_variant = re.sub(r"神圣之", "圣", relaxed or normalized)
    if sacred_variant and sacred_variant not in variants:
        variants.append(sacred_variant)
    # Bounded release aliases cover well-established short translations and
    # recurring transcription/obfuscation errors.  These are query variants,
    # never direct identities: candidate scoring, ambiguity margins and media
    # type checks remain authoritative.
    bounded_aliases = (
        (r"^\s*末日三问\s*$", "末日时在做什么？有没有空？可以来拯救吗？"),
        (r"杖与剑的魔法谭", "杖与剑的魔剑谭"),
        (r"瑞克和\s*MD", "瑞克和莫蒂"),
    )
    for pattern, replacement in bounded_aliases:
        alias = re.sub(pattern, replacement, normalized, flags=re.I).strip()
        if alias != normalized and alias and alias not in variants:
            variants.append(alias)
    parts = [part.strip() for part in re.split(r"[：:]", query) if part.strip()]
    if len(parts) > 1 and len(parts[-1]) >= 4 and parts[-1] not in variants:
        variants.append(parts[-1])
    return variants[:7]


def _cross_script_unique_match(query: str, titles: Sequence[str]) -> bool:
    query_has_latin = bool(re.search(r"[A-Za-z]", query))
    query_has_cjk = bool(re.search(r"[\u3400-\u9fff\u3040-\u30ff]", query))
    title_text = " ".join(titles)
    title_has_latin = bool(re.search(r"[A-Za-z]", title_text))
    title_has_cjk = bool(re.search(r"[\u3400-\u9fff\u3040-\u30ff]", title_text))
    return (query_has_latin and title_has_cjk and not title_has_latin) or (
        query_has_cjk and title_has_latin and not title_has_cjk
    )


def _search_item_titles(item: Mapping[str, Any], media_type: str) -> list[str]:
    fields = (
        (item.get("title"), item.get("original_title"))
        if media_type == "movie"
        else (item.get("name"), item.get("original_name"))
    )
    return [str(value) for value in fields if isinstance(value, str) and value.strip()]


def _alternative_tmdb_titles(
    client: TMDBClient, media_type: str, tmdb_id: int
) -> list[str]:
    """Fetch a bounded alias set for ambiguous TV/movie search results."""
    if media_type not in {"tv", "movie"}:
        return []
    try:
        response = client.get(f"/{media_type}/{tmdb_id}/alternative_titles")
    except ApiError:
        # Alias enrichment is optional. The original search result remains
        # usable even on older proxies that do not expose this endpoint.
        return []
    values = response.get("results" if media_type == "tv" else "titles") or []
    if not isinstance(values, list):
        return []
    aliases: list[str] = []
    seen: set[str] = set()
    for item in values[:100]:
        if not isinstance(item, Mapping):
            continue
        title = item.get("title")
        if not isinstance(title, str) or not title.strip():
            continue
        key = _normalize_match_title(title)
        if not key or key in seen:
            continue
        seen.add(key)
        aliases.append(title.strip())
    return aliases


def _query_from_source(src: str) -> str:
    name = normalize_remote_path(src).rstrip("/").rsplit("/", 1)[-1]
    # Commonly shared 86 release folders abbreviate the Chinese subtitle to
    # ``不存/ZDZQ``. Keep this bounded canonical alias instead of sending an
    # unsearchable release-code fragment to TMDB.
    if re.search(r"86.*(?:不存(?!在)|ZDZQ)", name, re.IGNORECASE):
        return "86 -不存在的战区-"
    # Library shelf labels are single Latin letters.  Most are followed by a
    # quality token (``H 4k``), while older folders may directly start with a
    # Chinese title (``R 日在校园``).
    name = re.sub(
        r"^\s*[A-Za-z]\s+(?=(?:(?:4k|8k|2160p|1080p|720p|480p)\b|[\u3400-\u9fff]))",
        "",
        name,
        flags=re.IGNORECASE,
    )
    name = re.sub(r"\[[^\]]*\]", " ", name)
    name = re.sub(r"\{(?:tmdb|imdb)-[^{}]+\}", " ", name, flags=re.IGNORECASE)
    # A season-range suffix is stripped only when everything after it is a
    # bounded release-package description.  This avoids corrupting legitimate
    # titles such as ``MS01-S03 Project`` or ``标题 收藏版的秘密``.
    season_range = re.search(
        r"(?<![A-Za-z0-9])(?:"
        r"(?:season|s)\s*\d{1,3}\s*[-–—~～至到]\s*(?:(?:season|s)\s*)?\d{1,3}"
        r"|第\s*\d{1,3}\s*[-–—~～至到]\s*\d{1,3}\s*季)",
        name,
        flags=re.IGNORECASE,
    )
    if season_range:
        release_tail = name[season_range.end():]
        release_token = (
            r"(?:全系列|系列合集|合集包|合集|收藏版|超清|"
            r"4k|8k|2160p|1080p|720p|480p|"
            r"(?:内封|内嵌|外挂)(?:中文|简中|繁中|简繁|简日双语|中字)?字幕|"
            r"附(?:\d+|一|两|二|三|四|五|六|七|八|九|十)*部剧场版)"
        )
        if re.fullmatch(rf"(?:\s*{release_token})*\s*", release_tail, flags=re.I):
            name = name[:season_range.start()]
    # Remove an actual season label, not an ``S01`` fragment embedded in a
    # product/title token such as ``MS01-S03 Project``.  A hyphen is also a
    # meaningful part of that token, so the second ``S03`` must stay intact.
    name = re.sub(
        r"(?<![A-Za-z0-9-])(?:season|s)\s*\d+(?![A-Za-z0-9])",
        " ",
        name,
        flags=re.IGNORECASE,
    )
    name = re.sub(r"第\s*\d{1,3}\s*季", " ", name)
    name = re.sub(
        r"\b(?:4k|8k|2160p|1080p|720p|480p|bluray|blu-ray|web-?dl|webrip|x26[45]|hevc|av1)\b",
        " ",
        name,
        flags=re.IGNORECASE,
    )
    name = re.sub(r"[._]+", " ", name)
    # Subtitle/language advertising is release metadata, not part of the work
    # title.  Strip only a trailing descriptor so legitimate title words in
    # the middle remain untouched.
    name = re.sub(
        r"\s*(?:(?:简体|繁体|简繁|繁简|中文|中字)?"
        r"(?:内封|内嵌|外挂|硬字幕|软字幕)(?:字幕)?"
        r"(?:\s*[+&/&]\s*(?:内封|内嵌|外挂|硬字幕|软字幕)(?:字幕)?)*)"
        r"\s*(?:4k|8k|2160p|1080p|720p|480p)?\s*[+&/&]*\s*$",
        "",
        name,
        flags=re.I,
    )
    # Quark appends a numeric collision suffix when a same-name folder is recreated.
    name = re.sub(r"\s*[（(]\d{1,3}[)）]\s*$", "", name)
    # A common source-folder typo; TMDB uses the official title 白色相簿.
    name = name.replace("白色相薄", "白色相簿")
    # The source library uses this shortened translation while TMDB exposes
    # the full official Chinese title.
    name = name.replace("最弱无败神龙", "最弱无败神装机龙")
    # Do not strip parentheses one character at a time: ``Title (2021)`` used
    # to become ``Title (2021`` because only the trailing parenthesis was at
    # the edge.  TMDB can use the balanced year as an additional signal.
    return re.sub(r"\s+", " ", name).strip(" -[]") or name


def _franchise_member_queries(src: str) -> list[str]:
    """Generate title-focused queries from verbose release-folder labels."""
    raw_name = unicodedata.normalize(
        "NFKC",
        normalize_remote_path(src).rstrip("/").rsplit("/", 1)[-1],
    )
    cleaned = re.sub(r"^\s*\d{1,3}\s*[.)、_-]?\s*", "", raw_name)
    cleaned = re.sub(r"\{(?:tmdb|imdb)-[^{}]+\}|\[[^\]]*\]", " ", cleaned, flags=re.I)
    cleaned = re.sub(
        r"\b(?:4k|8k|2160p|1080p|720p|480p|bd(?:rip)?|blu-?ray|"
        r"web-?dl|webrip|x26[45]|hevc|av1|flac|ma10p|10bit)\b",
        " ",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(
        r"(?:\s*(?:全|共)\s*|\s+)\d{1,4}\s*集.*$",
        " ",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(
        r"\s*(?:内封|内嵌|外挂|硬字幕|软字幕|简中|繁中|中字).*$",
        " ",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(r"\s+\d+\s*[-–—~～至到]\s*\d+\s*季", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_[]")
    without_movie_prefix = re.sub(
        r"^(?:剧场版|劇場版|电影|電影)\s*[：:\-—]*\s*",
        "",
        cleaned,
        flags=re.I,
    ).strip()
    without_movie_label = re.sub(
        r"(?:剧场版|劇場版|电影|電影)",
        " ",
        cleaned,
        flags=re.I,
    )
    without_movie_label = re.sub(r"\s+", " ", without_movie_label).strip()
    suffixes: list[str] = []
    punctuation_parts = [
        part.strip()
        for part in re.split(r"[-–—:：/／]+", cleaned)
        if part.strip()
    ]
    if len(punctuation_parts) > 1:
        suffixes.append(punctuation_parts[-1])
    whitespace_parts = cleaned.split(maxsplit=1)
    if (
        len(whitespace_parts) == 2
        and 1 <= len(whitespace_parts[0]) <= 8
        and len(whitespace_parts[1]) >= 2
    ):
        suffixes.append(whitespace_parts[1])
    raw_query = _query_from_source(src)
    raw_has_release_noise = bool(re.search(
        r"(?:外挂|内封|内嵌|硬字幕|软字幕|字幕组|BDRip|WEBRip|HEVC|FLAC|10bit)",
        raw_query,
        re.I,
    ))
    return list(dict.fromkeys(
        query
        for query in (
            cleaned,
            without_movie_prefix,
            without_movie_label,
            *suffixes,
            *( [] if raw_has_release_noise else [raw_query] ),
        )
        if query
    ))


def _tmdb_hint_from_source(src: str) -> int | None:
    """Return a positive TMDB id embedded in the selected directory name."""
    name = normalize_remote_path(src).rstrip("/").rsplit("/", 1)[-1]
    match = re.search(r"\{tmdb-(\d+)\}", name, flags=re.IGNORECASE)
    if not match:
        return None
    value = int(match.group(1))
    return value if value > 0 else None


def _direct_tmdb_match(client: TMDBClient, src: str, tmdb_id: int) -> AutoMatch:
    """Resolve an embedded id without fuzzy search, including cross-type ids.

    TMDB reuses numeric ids between TV and movie namespaces.  The directory
    title/year therefore selects the matching namespace, while the id itself
    remains authoritative.
    """
    query = _query_from_source(src)
    query_key = _normalize_match_title(query)
    year_match = re.search(r"(?:19|20)\d{2}", query)
    query_year = year_match.group(0) if year_match else None
    context_type = _media_type_from_source_context(src)
    collection_hint = _source_suggests_collection(src) or bool(
        re.search(r"(?:系列|series)", query, flags=re.IGNORECASE)
    )
    order = (
        ["collection", context_type, "tv", "movie"]
        if collection_hint
        else [context_type, "tv", "movie", "collection"]
    )
    candidates: list[AutoMatch] = []
    seen_types: set[str] = set()
    for candidate_type in order:
        if candidate_type not in {"tv", "movie", "collection"} or candidate_type in seen_types:
            continue
        seen_types.add(candidate_type)
        try:
            item = client.get(f"/{candidate_type}/{tmdb_id}")
        except ApiError as exc:
            if exc.status_code == 404:
                continue
            raise
        if candidate_type == "tv":
            title_fields = (item.get("name"), item.get("original_name"))
            date_value = item.get("first_air_date")
        elif candidate_type == "movie":
            title_fields = (item.get("title"), item.get("original_title"))
            date_value = item.get("release_date")
        else:
            title_fields = (item.get("name"), item.get("original_name"))
            date_value = None
        titles = [str(value) for value in title_fields if isinstance(value, str) and value]
        if not titles:
            continue
        similarity = max(
            difflib.SequenceMatcher(None, query_key, _normalize_match_title(title)).ratio()
            for title in titles
        )
        year = _extract_year(date_value)
        confidence = similarity
        if query_year and year != "未知年份":
            confidence += 0.08 if year == query_year else -0.12
        if candidate_type == context_type:
            confidence += 0.03
        if candidate_type == "collection" and collection_hint:
            confidence += 0.08
        match = AutoMatch(
            candidate_type,
            tmdb_id,
            titles[0],
            year,
            max(0.0, min(1.0, confidence)),
        )
        candidates.append(match)
        # An exact title/year match is enough to disambiguate reused ids and
        # avoids unnecessary TMDB requests for large franchise directories.
        if match.confidence >= 0.98:
            return match
    if not candidates:
        raise PlanError(f"TMDB 编号 {tmdb_id} 在电影、剧集和合集中均不存在")
    candidates.sort(key=lambda item: (-item.confidence, item.media_type))
    best = candidates[0]
    if len(candidates) > 1 and best.confidence - candidates[1].confidence < 0.08:
        raise PlanError(
            f"TMDB 编号 {tmdb_id} 同时存在于多个类型，目录名无法唯一判定: "
            + "; ".join(
                f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%})"
                for item in candidates[:3]
            )
        )
    return best


def _season_from_source(src: str) -> int | None:
    """Infer an explicit season marker without guessing from years or episode numbers."""
    name = normalize_remote_path(src).rstrip("/").rsplit("/", 1)[-1]
    # A single Latin shelf prefix before a resolution/CJK title is a library
    # bucket, not a Roman season.  Without this guard ``X 4k 作品名`` was
    # silently interpreted as Season 10 before the real child-season folders
    # were inspected.
    name = re.sub(
        r"^\s*[A-Za-z]\s+(?=(?:(?:4k|8k|2160p|1080p|720p|480p)\b|[\u3400-\u9fff]))",
        "",
        name,
        flags=re.IGNORECASE,
    )
    for pattern in (
        # Release names commonly concatenate the title and marker, e.g.
        # ``从零开始的异世界生活S03.Part2``.  ``S`` followed by digits and a
        # boundary is itself an explicit season marker; requiring a Latin
        # separator before it caused a farther, incorrect ancestor marker to
        # win in nested collections.
        r"S(?:eason)?\s*0*(\d{1,3})(?=$|[\s._\-\])])",
        r"(?:^|[\s._\-\[(])0*(\d{1,3})(?:st|nd|rd|th)\s+Season(?=$|[\s._\-\])])",
        r"第\s*0*(\d{1,3})\s*季",
    ):
        match = re.search(pattern, name, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    chinese_match = re.search(r"第\s*([一二三四五六七八九十]{1,3})\s*季", name)
    if chinese_match:
        token = chinese_match.group(1)
        digits = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
        if token == "十":
            return 10
        if token.startswith("十"):
            return 10 + digits.get(token[1:], 0)
        if token.endswith("十"):
            return digits.get(token[:-1], 0) * 10
        if "十" in token:
            tens, ones = token.split("十", 1)
            return digits.get(tens, 0) * 10 + digits.get(ones, 0)
        return digits.get(token)
    roman_match = re.search(r"(?:^|[\s._\-])([IVX]{1,4})(?=$|[\s._\-])", name, re.I)
    if roman_match:
        roman = roman_match.group(1).upper()
        values = {"I": 1, "V": 5, "X": 10}
        total = 0
        previous = 0
        for char in reversed(roman):
            value = values[char]
            total += -value if value < previous else value
            previous = max(previous, value)
        if 1 <= total <= 30:
            return total
    return None


def _explicit_release_season_episode(
    name: str,
    official_season_counts: Mapping[int, int],
) -> tuple[int, int] | None:
    """Resolve ``Title 3 - 01`` only against an official season boundary.

    A bare leading or trailing number is not enough season evidence.  This
    release convention is accepted only when both numbers form exactly one
    valid season/episode pair in the already fetched TMDB season table.
    """
    matches: set[tuple[int, int]] = set()
    for match in re.finditer(
        r"(?:^|[\s._\-\]])(?P<season>[1-9]\d{0,2})\s*\-\s*"
        r"0*(?P<episode>[1-9]\d{0,2})(?=$|[\s._\-\[])",
        unicodedata.normalize("NFKC", Path(name).name),
        flags=re.IGNORECASE,
    ):
        season_number = int(match.group("season"))
        episode_number = int(match.group("episode"))
        if 1 <= episode_number <= int(official_season_counts.get(season_number, 0)):
            matches.add((season_number, episode_number))
    return next(iter(matches)) if len(matches) == 1 else None


def _season_from_series_variant(
    source_segment: str,
    show: Mapping[str, Any],
) -> int | None:
    """Match release folders such as ``Title``, ``Title S`` and ``Title T``
    against TMDB's actual season names before treating them as file versions.

    This is deliberately exact after punctuation/spacing normalization.  A loose
    title match would turn unrelated sequel or spin-off folders into seasons of
    the current show.
    """
    source_keys = {
        _normalize_match_title(source_segment),
        _normalize_match_title(_query_from_source("/" + source_segment)),
        *(
            _normalize_match_title(token)
            for token in re.findall(r"\[([^\]]+)\]", source_segment)
        ),
        *(
            _normalize_match_title(query)
            for query in _franchise_member_queries("/" + source_segment)
        ),
    }
    source_keys.discard("")
    if not source_keys:
        return None
    matched: set[int] = set()
    for raw_season in show.get("seasons") or []:
        if not isinstance(raw_season, Mapping):
            continue
        season_number = raw_season.get("season_number")
        season_name = raw_season.get("name")
        if (
            not isinstance(season_number, int)
            or isinstance(season_number, bool)
            or season_number <= 0
            or not isinstance(season_name, str)
            or not season_name.strip()
        ):
            continue
        season_keys = {
            _normalize_match_title(season_name),
            _normalize_match_title(_query_from_source("/" + season_name)),
        }
        season_keys.discard("")
        if source_keys & season_keys:
            matched.add(season_number)
    if len(matched) == 1:
        return next(iter(matched))
    if matched:
        return None

    # Release-pack wrappers add group/codec/range noise around the official
    # season name.  Accept only the longest unique contained official name;
    # this lets ``[DBD-Raws][Mushishi Zoku Shou][01-20+SP]`` resolve to the
    # sequel season while the shorter base title also appears in the text.
    contained: list[tuple[int, int]] = []
    for raw_season in show.get("seasons") or []:
        if not isinstance(raw_season, Mapping):
            continue
        season_number = raw_season.get("season_number")
        season_name = raw_season.get("name")
        if (
            not isinstance(season_number, int)
            or isinstance(season_number, bool)
            or season_number <= 0
            or not isinstance(season_name, str)
        ):
            continue
        season_key = _normalize_match_title(season_name)
        if len(season_key) < 4:
            continue
        for source_key in source_keys:
            if season_key not in source_key:
                continue
            residual = source_key.replace(season_key, "", 1)
            # A real child-work title often starts with the parent/first-season
            # title (``约会大作战 赤黑新章``).  Containment alone must not
            # swallow that child into Season 01.  Accept a contained official
            # season name only when the remainder is release metadata rather
            # than another usable title.  Exact/bracket-cleaned season names
            # were already accepted by the stronger intersection above.
            if residual and _usable_release_title_query(residual):
                continue
            contained.append((len(season_key), season_number))
            break
    if contained:
        longest = max(length for length, _ in contained)
        longest_seasons = {
            number for length, number in contained if length == longest
        }
        if len(longest_seasons) == 1:
            return next(iter(longest_seasons))

    # Multilingual season names may translate only the franchise prefix while
    # preserving a distinctive suffix (``2wei Herz``, ``3rei``). Prefer the
    # longest unique alphanumeric signature found in the source; this prevents
    # ``2wei`` from stealing ``2wei Herz`` without using loose cross-script
    # whole-title similarity.
    source_identity = " ".join(source_keys)
    season_identity = " ".join(
        _normalize_match_title(str(item.get("name") or ""))
        for item in (show.get("seasons") or [])
        if isinstance(item, Mapping)
    )
    cross_script_identity = any(
        latin in source_identity
        and any(cjk in season_identity for cjk in cjk_aliases)
        for latin, cjk_aliases in {
            "illya": ("伊莉雅", "イリヤ"),
        }.items()
    )
    if not cross_script_identity:
        return None

    signature_matches: list[tuple[int, int]] = []
    for raw_season in show.get("seasons") or []:
        if not isinstance(raw_season, Mapping):
            continue
        season_number = raw_season.get("season_number")
        season_name = raw_season.get("name")
        if (
            not isinstance(season_number, int)
            or isinstance(season_number, bool)
            or season_number <= 0
            or not isinstance(season_name, str)
        ):
            continue
        season_key = _normalize_match_title(season_name)
        signatures = [
            token
            for token in re.findall(r"[a-z0-9]+", season_key)
            if len(token) >= 4 and re.search(r"[a-z]", token)
        ]
        for signature in signatures:
            if any(signature in source_key for source_key in source_keys):
                signature_matches.append((len(signature), season_number))
    if not signature_matches:
        # The base release often keeps only the Latin franchise name while
        # TMDB localizes that name and adds Latin signatures only to sequels
        # (Illya / 2wei / 2wei Herz / 3rei). If exactly one positive season has
        # no such suffix, it is the uniquely evidenced base season.
        base_seasons: list[int] = []
        for raw_season in show.get("seasons") or []:
            if not isinstance(raw_season, Mapping):
                continue
            season_number = raw_season.get("season_number")
            season_name = raw_season.get("name")
            if (
                not isinstance(season_number, int)
                or isinstance(season_number, bool)
                or season_number <= 0
                or not isinstance(season_name, str)
            ):
                continue
            latin_suffixes = [
                token
                for token in re.findall(
                    r"[a-z0-9]+", _normalize_match_title(season_name)
                )
                if len(token) >= 4 and re.search(r"[a-z]", token)
            ]
            if not latin_suffixes:
                base_seasons.append(season_number)
        return base_seasons[0] if len(base_seasons) == 1 else None
    longest = max(length for length, _ in signature_matches)
    longest_seasons = {
        number for length, number in signature_matches if length == longest
    }
    return next(iter(longest_seasons)) if len(longest_seasons) == 1 else None


def _source_suggests_collection(src: str) -> bool:
    name = normalize_remote_path(src).rstrip("/").rsplit("/", 1)[-1]
    if re.search(
        r"(?:\bTV\b|电视|電視|剧集|本篇).{0,40}"
        r"[+&＆/].{0,40}(?:电影|電影|剧场版|劇場版|movie|film)",
        name,
        re.I,
    ):
        return False
    # A season range describes one TV work, even when the release calls itself
    # a “合集”. Routing it to TMDB's collection namespace produces no match.
    if re.search(
        r"(?:season|s)\s*\d+\s*[-–—~～至到]\s*(?:season|s)?\s*\d+.*合集",
        name,
        re.IGNORECASE,
    ):
        return False
    return bool(re.search(
        r"(?:合集|三部曲|collection|trilogy|"
        r"\d+\s*[-–—~～至到]\s*\d+\s*(?:部|篇)|前篇.*后篇)",
        name,
        re.IGNORECASE,
    ))


def _source_suggests_batch(src: str) -> bool:
    name = normalize_remote_path(src).rstrip("/").rsplit("/", 1)[-1]
    if re.search(
        r"(?:\bTV\b|电视|電視|剧集|本篇).{0,40}"
        r"[+&＆/].{0,40}(?:电影|電影|剧场版|劇場版|movie|film)",
        name,
        re.I,
    ):
        return True
    if re.search(
        r"(?:全系列|系列合集|大合集|合集包|franchise)",
        name,
        re.IGNORECASE,
    ):
        return True
    # Two substantial CJK work labels separated by ``&`` are explicit
    # multi-work evidence.  A ``+<named variant>`` root is also eligible for
    # independent-member planning, but build_batch_plan still requires two
    # video-bearing child directories and independently confirms every TMDB
    # identity, so flat/edition-only layouts fail closed.
    cleaned = re.sub(
        r"^\s*[A-Za-z]\s+(?=(?:4k\b|[\u3400-\u9fff]))|"
        r"\b(?:4k|8k|2160p|1080p|720p)\b|"
        r"\s*(?:内封|内嵌|外挂|硬字幕|软字幕).*$",
        " ",
        unicodedata.normalize("NFKC", name),
        flags=re.I,
    )
    ampersand_parts = [part.strip() for part in re.split(r"[&＆]", cleaned)]
    if len(ampersand_parts) == 2 and all(
        len(re.findall(r"[\u3400-\u9fff]", part)) >= 4
        for part in ampersand_parts
    ):
        return True
    return bool(re.search(
        r"\+\s*[\u3400-\u9fff]{2,}(?:版|外传|外傳|剧场版|劇場版)(?:\s|$)",
        cleaned,
        re.I,
    ))


def _clean_franchise_root_label(src: str) -> str:
    """Remove shelf, release, resolution and subtitle advertising from a root."""
    name = unicodedata.normalize(
        "NFKC", normalize_remote_path(src).rstrip("/").rsplit("/", 1)[-1]
    )
    name = re.sub(
        r"^\s*[A-Za-z]\s+(?=(?:(?:4k|8k|2160p|1080p|720p)\b|[\u3400-\u9fff]))",
        "",
        name,
        flags=re.I,
    )
    name = re.sub(
        r"^\s*[【\[][^\]】]{1,12}(?:漫|剧|影|视频|动画)[】\]]\s*",
        "",
        name,
        flags=re.I,
    )
    name = re.sub(r"(?:全系列|系列合集|大合集|合集包|系列收藏)", " ", name, flags=re.I)
    name = re.sub(
        r"(?:^|[\s._+＋/&-])(?:\d+|[一二两三四五六七八九十]+)\s*部?\s*"
        r"(?:剧场版|劇場版|电影|電影|movies?|films?)(?=$|[\s._+＋/&-])",
        " ",
        name,
        flags=re.I,
    )
    name = re.sub(r"系列(?=$|[\s._+＋/&-])", " ", name, flags=re.I)
    name = re.sub(
        r"(?:[48]k\s*)?超清\s*(?:2160p|1080p|720p)?\s*收藏版|"
        r"(?:2160p|1080p|720p)?\s*(?:收藏版|典藏版)|"
        r"\b(?:4k|8k|2160p|1080p|720p)(?:\s*[+&/]\s*(?:4k|8k|2160p|1080p|720p))*\b",
        " ",
        name,
        flags=re.I,
    )
    name = re.sub(
        r"\s*(?:内封|内嵌|外挂|硬字幕|软字幕|硬字|软字|"
        r"简日|简繁|中日|双语|雙語|字幕).*$",
        "",
        name,
        flags=re.I,
    )
    name = re.sub(r"\s+", " ", name).strip(" -_+&/")
    return safe_name(name) if name else safe_name(split_remote(src)[1])


def _media_type_from_source_context(src: str) -> str | None:
    """Infer only strong media-library hints, preferring the nearest parent folder."""
    tv_hints = {
        "tv",
        "tvshow",
        "tvshows",
        "show",
        "shows",
        "series",
        "anime",
        "animation",
        "番剧",
        "电视剧",
        "剧集",
        "连续剧",
        "动漫",
        "动画",
        "电视动画",
        "国剧",
        "日剧",
        "韩剧",
        "美剧",
        "英剧",
    }
    movie_hints = {
        "movie",
        "movies",
        "film",
        "films",
        "cinema",
        "电影",
        "影片",
    }
    segments = normalize_remote_path(src).strip("/").split("/")[:-1]
    for segment in reversed(segments):
        key = re.sub(
            r"[\s._\-]+",
            "",
            unicodedata.normalize("NFKC", segment).casefold(),
        )
        if key in tv_hints:
            return "tv"
        if key in movie_hints:
            return "movie"
    return None


def _source_is_animation_library(src: str) -> bool:
    """Return true only for explicit animation-library path segments."""
    animation_hints = {"anime", "animation", "番剧", "动漫", "动画", "电视动画"}
    segments = normalize_remote_path(src).strip("/").split("/")[:-1]
    return any(
        re.sub(r"[\s._\-]+", "", unicodedata.normalize("NFKC", segment).casefold())
        in animation_hints
        for segment in segments
    )


def _media_context_from_source_and_target(
    source: str,
    target_parent: str,
) -> tuple[str | None, bool]:
    """Combine source-library and user-selected target-category evidence.

    ``_media_type_from_source_context`` intentionally ignores the leaf because
    the leaf is normally the work title.  The create UI, however, passes the
    category itself as ``--parent`` (for example ``/quark/影视/番剧``).  Append a
    synthetic work leaf so that the explicitly selected category participates
    in matching instead of being silently discarded.
    """
    target_probe = join_remote(target_parent, "__scrapeflow_work__")
    requested_type = (
        _media_type_from_source_context(source)
        or _media_type_from_source_context(target_probe)
    )
    prefer_animation = (
        _source_is_animation_library(source)
        or _source_is_animation_library(target_probe)
    )
    return requested_type, prefer_animation


def _expected_single_tv_episode_count(
    files: Sequence[Mapping[str, Any]],
) -> int | None:
    """Return a complete contiguous episode boundary, never a partial count."""
    explicit_seasons: set[int] = set()
    numbers: set[int] = set()
    for item in files:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        full_path = str(item.get("full_path", ""))
        for segment in full_path.rsplit("/", 1)[0].split("/"):
            season_number = _season_from_source("/" + segment)
            if season_number is not None and season_number > 0:
                explicit_seasons.add(season_number)
        key = extract_episode_key(str(item.get("name", "")))
        if key is not None and key.kind == "regular" and not key.end_number and key.number > 0:
            numbers.add(key.number)
    if len(explicit_seasons) > 1 or not numbers:
        return None
    maximum = max(numbers)
    return maximum if numbers == set(range(1, maximum + 1)) else None


def auto_match_tmdb(
    client: TMDBClient,
    query: str,
    *,
    media_type: str | None,
    min_confidence: float,
    prefer_animation: bool = False,
    expected_episode_count: int | None = None,
    excluded_tmdb_ids: Collection[int] | None = None,
) -> tuple[AutoMatch, list[AutoMatch]]:
    if not query.strip():
        raise PlanError("自动匹配查询为空")
    if not 0 <= min_confidence <= 1:
        raise PlanError("自动匹配最低置信度必须在 0 到 1 之间")
    query_key = _normalize_match_title(query)
    query_year_match = re.search(r"(?:19|20)\d{2}", query)
    query_year = query_year_match.group(0) if query_year_match else None
    excluded_ids = {int(value) for value in (excluded_tmdb_ids or ())}
    initial_types = [media_type] if media_type in {"tv", "movie", "collection"} else ["tv", "movie"]
    raw_candidates: list[dict[str, Any]] = []
    searched_types: list[str] = []

    def collect_type(candidate_type: str) -> None:
        if candidate_type in searched_types:
            return
        searched_types.append(candidate_type)
        search_items: list[Any] = []
        search_evidence: dict[int, tuple[str, float]] = {}
        seen_search_ids: set[int] = set()
        strongest_search_score = 0.0

        def ingest(response: Mapping[str, Any], search_query: str) -> None:
            nonlocal strongest_search_score
            for item in list(response.get("results") or [])[:10]:
                if not isinstance(item, Mapping) or isinstance(item.get("id"), bool):
                    continue
                try:
                    item_id = int(item["id"])
                except (KeyError, TypeError, ValueError):
                    continue
                titles = _search_item_titles(item, candidate_type)
                variant_key = _normalize_match_title(search_query)
                if titles:
                    variant_score = max(
                        _title_similarity(variant_key, title) for title in titles
                    )
                    strongest_search_score = max(
                        strongest_search_score,
                        variant_score,
                    )
                    previous = search_evidence.get(item_id)
                    if previous is None or variant_score > previous[1]:
                        search_evidence[item_id] = (search_query, variant_score)
                if item_id in seen_search_ids:
                    continue
                seen_search_ids.add(item_id)
                search_items.append(item)

        for search_query in _search_query_variants(query):
            response = client.get(f"/search/{candidate_type}", query=search_query)
            ingest(response, search_query)
            # An exact/expanded official title is decisive. A merely non-empty
            # response is not: punctuation and release noise can make TMDB's
            # first result set plausible but wrong.
            if strongest_search_score >= 0.98:
                break
        if not search_items:
            # A proxy/CDN can transiently cache an empty first-page response.
            # Repeating the same semantic query with an explicit page bypasses
            # that cache key while keeping the retry bounded and deterministic.
            search_query = query.strip()
            response = client.get(
                f"/search/{candidate_type}", query=search_query, page=1
            )
            ingest(response, search_query)
        for index, item in enumerate(search_items):
            if not isinstance(item, Mapping) or isinstance(item.get("id"), bool):
                continue
            try:
                tmdb_id = int(item["id"])
            except (KeyError, TypeError, ValueError):
                continue
            titles = _search_item_titles(item, candidate_type)
            if not titles:
                continue
            matched_query, title_score = search_evidence.get(
                tmdb_id, (query, max(_title_similarity(query_key, title) for title in titles))
            )
            matched_query_key = _normalize_match_title(matched_query)
            aliases = (
                _alternative_tmdb_titles(client, candidate_type, tmdb_id)
                if index < 5 else []
            )
            alias_score = max(
                (_title_similarity(matched_query_key, title) for title in aliases),
                default=0.0,
            )
            date_value = item.get(
                "first_air_date" if candidate_type == "tv" else "release_date"
            )
            year = _extract_year(date_value)
            genre_ids = item.get("genre_ids") or []
            is_animation = 16 in genre_ids if isinstance(genre_ids, list) and genre_ids else None
            actual_episode_count: int | None = None
            if expected_episode_count and candidate_type == "tv" and index < 5:
                try:
                    details = client.get(f"/tv/{tmdb_id}")
                except ApiError:
                    details = {}
                actual_count = details.get("number_of_episodes")
                if isinstance(actual_count, int) and not isinstance(actual_count, bool):
                    actual_episode_count = actual_count
            raw_candidates.append({
                "media_type": candidate_type,
                "tmdb_id": tmdb_id,
                "title": titles[0],
                "titles": titles,
                "aliases": aliases,
                "year": year,
                "title_score": title_score,
                "alias_score": alias_score,
                "cross_script": _cross_script_unique_match(matched_query, [*titles, *aliases]),
                "matched_query": matched_query,
                "is_animation": is_animation,
                "actual_episode_count": actual_episode_count,
            })

    def score(raw: Mapping[str, Any]) -> AutoMatch:
        title_score = float(raw["title_score"])
        alias_score = float(raw["alias_score"])
        evidence_score = max(title_score, alias_score)
        year_score = 0.0
        wrong_year = False
        if query_year:
            candidate_year = str(raw["year"])
            if candidate_year == query_year:
                year_score = 0.0
            elif candidate_year == "未知年份":
                year_score = -0.06
            else:
                delta = abs(int(candidate_year) - int(query_year))
                wrong_year = delta >= 2
                year_score = -0.30 if wrong_year else -0.12
        # Namespace is routing context, not title evidence. Keep it visible in
        # the trace but do not let it push a weak title over the gate.
        media_type_score = 0.0
        context_score = 0.0
        if prefer_animation and raw["media_type"] in {"tv", "movie"}:
            if raw["is_animation"] is True:
                context_score = 0.0
            elif raw["is_animation"] is False:
                # A target explicitly identified as an animation shelf is
                # strong context, not a cosmetic tie-breaker.  Keep enough
                # separation that a same-title live-action result cannot pass
                # the global ambiguity margin on title evidence alone.
                context_score = -0.12
        episode_structure_score = 0.0
        actual_count = raw["actual_episode_count"]
        if expected_episode_count and isinstance(actual_count, int):
            episode_structure_score = 0.0 if actual_count == expected_episode_count else -0.08
        confidence = max(0.0, min(1.0, evidence_score + year_score + media_type_score + context_score + episode_structure_score))
        blockers: list[str] = []
        if wrong_year:
            blockers.append("year_conflict")
        if raw["cross_script"] and alias_score < 0.88:
            blockers.append("cross_script_without_alias_evidence")
        if confidence < min_confidence:
            blockers.append("below_confidence_threshold")
        if "cross_script_without_alias_evidence" in blockers and evidence_score < 0.35:
            status = "rejected"
        elif blockers:
            status = "review"
        else:
            status = "confirmed"
        components = {
            "title_score": round(title_score, 6),
            "alias_score": round(alias_score, 6),
            "year_score": round(year_score, 6),
            "media_type_score": round(media_type_score, 6),
            "episode_structure_score": round(episode_structure_score, 6),
            "context_score": round(context_score, 6),
            "final_score": round(confidence, 6),
        }
        return AutoMatch(
            str(raw["media_type"]), int(raw["tmdb_id"]), str(raw["title"]),
            str(raw["year"]), confidence, status, components,
            {
                "query": query,
                "matched_query_variant": raw.get("matched_query", query),
                "query_year": query_year,
                "official_titles": list(raw["titles"]),
                "aliases_checked": list(raw["aliases"]),
                "blockers": blockers,
                "expected_episode_count": expected_episode_count,
                "actual_episode_count": actual_count,
            },
        )

    for initial_type in initial_types:
        collect_type(initial_type)
    candidates = [
        score(item) for item in raw_candidates
        if int(item["tmdb_id"]) not in excluded_ids
    ]
    # Namespace fallback is driven by evidence trust, not by whether TMDB happened
    # to return a non-empty result set. A plausible but unconfirmed TV hit must not
    # hide an exact movie match, and vice versa.
    if len(initial_types) == 1 and initial_types[0] in {"tv", "movie"}:
        initial_scored = [item for item in candidates if item.media_type == initial_types[0]]
        if not initial_scored or max(item.confidence for item in initial_scored) < min_confidence or not any(
            item.status == "confirmed" for item in initial_scored
        ):
            try:
                collect_type("movie" if initial_types[0] == "tv" else "tv")
            except ApiError:
                # Cross-namespace enrichment is optional when the hinted
                # namespace already yielded candidates. Its failure must not
                # convert a safe rejection into a network-error false positive.
                if not initial_scored:
                    raise
            candidates = [
                score(item) for item in raw_candidates
                if int(item["tmdb_id"]) not in excluded_ids
            ]
    candidates.sort(key=lambda item: (-item.confidence, item.media_type, item.tmdb_id))
    if not candidates:
        raise PlanError(f"TMDB 未找到自动匹配候选: {query}")
    best = candidates[0]
    if best.status == "rejected":
        preview = "; ".join(
            f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%}, {item.status})"
            for item in candidates[:3]
        )
        raise PlanError(
            "自动匹配缺少可验证的标题/别名证据，已拒绝自动选择: " + preview
        )
    if "year_conflict" in best.decision_trace.get("blockers", []):
        raise PlanError(
            f"自动匹配候选年份与源目录冲突，拒绝自动选择: "
            f"query_year={query_year}, candidate={best.media_type}/{best.tmdb_id} "
            f"{best.title} ({best.year})"
        )
    runner_up = candidates[1] if len(candidates) > 1 else None
    best_exact = max(
        float(best.score_components.get("title_score", 0.0)),
        float(best.score_components.get("alias_score", 0.0)),
    ) >= 0.999999
    runner_exact = bool(runner_up) and max(
        float(runner_up.score_components.get("title_score", 0.0)),
        float(runner_up.score_components.get("alias_score", 0.0)),
    ) >= 0.999999
    exact_title_uniquely_identifies_best = best_exact and not runner_exact
    if (
        runner_up is not None
        and best.confidence - runner_up.confidence + 1e-9 < AUTO_MATCH_MIN_MARGIN
        and not exact_title_uniquely_identifies_best
    ):
        raise PlanError(
            "自动匹配前两名证据无法区分，拒绝自动选择: "
            + "; ".join(
                f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%})"
                for item in candidates[:2]
            )
        )
    return best, candidates


def _new_alist_client(args: argparse.Namespace) -> AListClient:
    client = AListClient(
        args.alist_url,
        args.username,
        _resolve_password(args),
        timeout=args.timeout,
        retries=args.retries,
        allow_insecure_http=args.allow_insecure_http,
    )
    client.login()
    return client


def main(argv: Sequence[str] | None = None) -> int:
    # Restore interruptibility when a container parent passed SIGINT as
    # ignored. execute_plan catches KeyboardInterrupt and runs its journaled
    # rollback/reconciliation path before the process exits.
    if signal.getsignal(signal.SIGINT) == signal.SIG_IGN:
        signal.signal(signal.SIGINT, signal.default_int_handler)
    parser = _build_parser()
    args = parser.parse_args(argv)
    execute = bool(args.execute or args.no_dry_run)
    selected_match: AutoMatch | None = None

    try:
        if args.season is not None and args.season < 0:
            raise ScraperError("--season 不能小于 0")
        if args.tmdb_id is not None and args.tmdb_id <= 0:
            raise ScraperError("--id 必须是正整数")
        if args.timeout <= 0:
            raise ScraperError("--timeout 必须大于 0")
        if args.retries < 0:
            raise ScraperError("--retries 不能小于 0")
        if args.skip_poster and args.overwrite_poster:
            raise ScraperError("--skip-poster 与 --overwrite-poster 不能同时使用")

        if args.inspect_journal:
            if any(
                (
                    args.recover_journal,
                    args.execute_plan,
                    args.src,
                    args.search,
                    execute,
                    args.approve_recovery_sha256,
                    args.auto_match,
                    args.wizard,
                    args.query,
                    args.episode_map,
                    args.episode_group,
                    args.parent,
                    args.type,
                    args.tmdb_id,
                    args.skip_poster,
                    args.overwrite_poster,
                    args.cleanup_empty_source,
                )
            ):
                raise ScraperError("--inspect-journal 不能与计划、执行或恢复参数同时使用")
            journal, digest, _ = load_execution_journal(args.inspect_journal)
            print(f"Journal: {args.inspect_journal}")
            print(f"创建时间: {journal.created_at}")
            print(f"计划 SHA-256: {plan_sha256(journal.plan)}")
            print(f"Journal SHA-256: {digest}")
            print(f"执行成功: {'是' if journal.success else '否'}")
            print(f"记录数: {len(journal.records)}")
            for record in journal.records:
                print(
                    f"  {record.action}: {record.status} "
                    f"{_terminal_text(record.source)} → {_terminal_text(record.target)}"
                )
            return 0

        if args.recover_journal:
            forbidden = (
                args.execute_plan,
                args.src,
                args.parent,
                args.tmdb_id,
                args.type,
                args.search,
                args.plan_json,
                args.collection_map,
                args.episode_map,
                args.episode_group,
                args.query,
                args.approve_plan_sha256,
            )
            if any(value is not None for value in forbidden):
                raise ScraperError("--recover-journal 不能与计划生成或普通执行参数同时使用")
            if args.auto_match or args.wizard:
                raise ScraperError("--recover-journal 不能与自动匹配或 wizard 同时使用")
            if args.skip_poster or args.overwrite_poster or args.cleanup_empty_source:
                raise ScraperError("--recover-journal 不接受图稿或源目录清理参数")
            journal, digest, raw_records = load_execution_journal(args.recover_journal)
            if journal.success:
                raise ScraperError("该 journal 已标记成功，不需要恢复")
            plan = plan_from_dict(journal.plan)
            alist = _new_alist_client(args)
            states = inspect_recovery_state(alist, plan, raw_records)
            print(f"恢复来源: {args.recover_journal}")
            print(f"Journal SHA-256: {digest}")
            print(f"{RECOVERY_DIGEST_PREFIX}{digest}", flush=True)
            for state in states:
                recovery_source = join_remote(state.current_dir, state.current_name)
                print(
                    RECOVERY_ITEM_PREFIX
                    + json.dumps(
                        {"source": recovery_source, "target": state.item.source_path},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
                print(
                    f"  {_terminal_text(recovery_source)} "
                    f"→ {_terminal_text(state.item.source_path)}"
                )
            if not execute:
                print(
                    "恢复 DRY RUN：未修改远端文件。确认后添加 "
                    f"--approve-recovery-sha256 {digest} --execute。"
                )
                return 0
            approved = (args.approve_recovery_sha256 or "").lower()
            if approved != digest:
                raise ScraperError("恢复批准 SHA-256 与当前 journal 不匹配")
            recovery_path = args.journal or Path(
                f"scraper-recovery-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
            )
            recover_execution(
                alist,
                plan,
                raw_records,
                recovery_journal_path=recovery_path,
            )
            print(f"✅ 恢复完成；恢复日志: {recovery_path}")
            return 0

        if args.approve_recovery_sha256:
            raise ScraperError("--approve-recovery-sha256 只能与 --recover-journal 使用")

        if args.search:
            search_extras = (
                args.src,
                args.parent,
                args.tmdb_id,
                args.type,
                args.season,
                args.collection_map,
                args.episode_map,
                args.episode_group,
                args.query,
                args.plan_json,
                args.execute_plan,
                args.journal,
            )
            search_flags = (
                execute,
                args.absolute,
                args.allow_unmapped,
                args.prefer_simplified,
                args.allow_index_mapping,
                args.ignore_orphan_temp,
                args.skip_poster,
                args.overwrite_poster,
                args.cleanup_empty_source,
                bool(args.approve_plan_sha256),
                args.auto_match,
                args.wizard,
            )
            if any(value is not None for value in search_extras) or any(search_flags):
                raise ScraperError("--search 不能与媒体计划或执行参数同时使用")
            client = TMDBClient(
                _resolve_tmdb_key(args),
                language=args.language,
                timeout=args.timeout,
                retries=args.retries,
            )
            _search_tmdb(client, args.search)
            return 0

        if args.execute_plan:
            if not execute:
                raise ScraperError("--execute-plan 必须同时提供 --execute")
            generation_values = (
                args.src,
                args.parent,
                args.tmdb_id,
                args.type,
                args.plan_json,
                args.season,
                args.collection_map,
                args.episode_map,
                args.episode_group,
                args.query,
            )
            generation_flags = (
                args.absolute,
                args.allow_unmapped,
                args.prefer_simplified,
                args.allow_index_mapping,
                args.ignore_orphan_temp,
                args.auto_match,
                args.wizard,
            )
            if any(value is not None for value in generation_values) or any(generation_flags):
                raise ScraperError(
                    "执行已保存计划时不得同时提供源目录或计划生成参数"
                )
            plan, digest = load_plan_json(args.execute_plan)
            approved = (args.approve_plan_sha256 or "").lower()
            if not re.fullmatch(r"[0-9a-f]{64}", approved):
                raise ScraperError("--approve-plan-sha256 必须是完整的 64 位十六进制值")
            if approved != digest:
                raise ScraperError(
                    f"批准的计划 SHA-256 不匹配: approved={approved}, actual={digest}"
                )
            print_plan(plan)
            print(f"\n计划 SHA-256: {digest}")

            alist = _new_alist_client(args)
            poster_needed = bool(planned_artwork(plan)) and not args.skip_poster
            tmdb_client: TMDBClient | None = None
            if poster_needed:
                tmdb_client = TMDBClient(
                    _resolve_tmdb_key(args),
                    language=args.language,
                    timeout=args.timeout,
                    retries=args.retries,
                )
            journal_path = args.journal or Path(
                f"scraper-journal-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
            )
            execute_plan(
                alist,
                tmdb_client,
                plan,
                journal_path=journal_path,
                skip_poster=args.skip_poster,
                overwrite_poster=args.overwrite_poster,
                cleanup_empty_source=args.cleanup_empty_source,
            )
            print(f"\n✅ 完成 {len(plan.files)} 个文件；执行日志: {journal_path}")
            return 0

        if execute and args.wizard:
            raise ScraperError("--wizard 不需要也不能同时使用 --execute")
        if execute:
            raise ScraperError(
                "安全策略禁止对实时重新扫描的计划直接执行。先使用 --plan-json 保存计划，"
                "再用 --execute-plan、--approve-plan-sha256 和 --execute 执行。"
            )
        if args.approve_plan_sha256:
            raise ScraperError("--approve-plan-sha256 只能与 --execute-plan 使用")
        if (
            args.journal
            or args.skip_poster
            or args.overwrite_poster
            or args.cleanup_empty_source
        ) and not args.wizard:
            raise ScraperError(
                "--journal、--skip-poster 和 --overwrite-poster 只能用于执行已保存计划"
            )
        if not args.src:
            parser.error("生成计划必须提供 src")
        if not args.parent:
            parser.error("生成计划必须提供 --parent")
        if not args.type:
            parser.error("生成计划必须提供 --type")

        if args.tmdb_id is not None and (args.auto_match or args.type == "auto"):
            raise ScraperError("显式 --id 不能与 --auto-match 或 --type auto 同时使用")
        if args.auto_match and args.type == "collection":
            raise ScraperError("合集不能自动匹配；请明确提供合集 TMDB ID")
        if args.query and not (args.auto_match or args.type == "auto"):
            raise ScraperError("--query 只能与 --auto-match 或 --type auto 使用")
        if not 0 <= args.min_confidence <= 1:
            raise ScraperError("--min-confidence 必须在 0 到 1 之间")

        tmdb_client = TMDBClient(
            _resolve_tmdb_key(args),
            language=args.language,
            timeout=args.timeout,
            retries=args.retries,
        )
        alist: AListClient | None = None
        prebuilt_plan: Plan | None = None
        planning_started = time.monotonic()
        emit_progress(
            "planning_start", completed=0, total=0, percent=5,
            message="正在读取作品目录并查询 TMDB",
        )
        if args.auto_match or args.type == "auto":
            hinted_id = _tmdb_hint_from_source(args.src)
            if hinted_id is not None:
                match = _direct_tmdb_match(tmdb_client, args.src, hinted_id)
                selected_match = match
                args.type = match.media_type
                args.tmdb_id = match.tmdb_id
                print(
                    f"已直接使用目录中的 TMDB 编号: {match.media_type}/{match.tmdb_id} "
                    f"{_terminal_text(match.title)}"
                )
            elif args.type == "auto" and args.auto_episode_mode and _source_suggests_batch(args.src):
                alist = _new_alist_client(args)
                prebuilt_plan = build_batch_plan(
                    alist,
                    tmdb_client,
                    src_path=args.src,
                    parent_path=args.parent,
                    ignore_orphan_temp=args.ignore_orphan_temp,
                )
                print("已识别为多作品系列父目录，将生成一份合并审核计划")
            else:
                query = args.query or _query_from_source(args.src)
                requested_type = args.type if args.type in {"tv", "movie"} else None
                if (
                    args.type == "auto"
                    and args.auto_episode_mode
                    and _source_suggests_collection(args.src)
                ):
                    requested_type = "collection"
                elif args.type == "auto" and args.auto_episode_mode:
                    requested_type, _ = _media_context_from_source_and_target(
                        args.src,
                        args.parent,
                    )
                    if requested_type is not None:
                        print(f"已从目录上下文识别媒体类型: {requested_type}")
                _, prefer_animation = _media_context_from_source_and_target(
                    args.src,
                    args.parent,
                )
                expected_episode_count = None
                if requested_type == "tv":
                    if alist is None:
                        alist = _new_alist_client(args)
                    expected_episode_count = _expected_single_tv_episode_count(
                        alist.walk(
                            args.src,
                            ignore_orphan_temp=args.ignore_orphan_temp,
                        )
                    )
                match, candidates = auto_match_tmdb(
                    tmdb_client,
                    query,
                    media_type=requested_type,
                    min_confidence=args.min_confidence,
                    prefer_animation=prefer_animation,
                    expected_episode_count=expected_episode_count,
                )
                match.decision_trace["query_variants"] = _search_query_variants(query)
                match.decision_trace["top_candidates"] = [
                    {
                        "media_type": candidate.media_type,
                        "tmdb_id": candidate.tmdb_id,
                        "title": candidate.title,
                        "year": candidate.year,
                        "status": candidate.status,
                        "confidence": candidate.confidence,
                        "score_components": dict(candidate.score_components),
                    }
                    for candidate in candidates[:3]
                ]
                selected_match = match
                print("自动匹配候选:")
                for candidate in candidates[:3]:
                    print(
                        f"  {candidate.media_type}/{candidate.tmdb_id} | "
                        f"{candidate.confidence:.1%} | {_terminal_text(candidate.title)} "
                        f"({candidate.year})"
                    )
                args.type = match.media_type
                args.tmdb_id = match.tmdb_id
                print(
                    f"已选择: {match.media_type}/{match.tmdb_id} "
                    f"{_terminal_text(match.title)} ({match.confidence:.1%})"
                )
                emit_progress(
                    "planning_match", completed=1, total=1, percent=30,
                    message=f"已生成 TMDB 匹配证据：{match.status}",
                )
            if prebuilt_plan is None:
                if args.type in {"movie", "collection"}:
                    args.prefer_simplified = False
                if args.type == "collection" and args.auto_episode_mode:
                    args.allow_index_mapping = True
                    print("已识别电影合集；将按 TMDB 上映顺序生成逐项审核计划")
                if args.type == "tv" and args.season is None:
                    inferred_season = _season_from_source(args.src)
                    if inferred_season is not None:
                        args.season = inferred_season
                        print(f"已从源目录识别季度: Season {inferred_season:02d}")
        if prebuilt_plan is None and not args.tmdb_id:
            parser.error("生成计划必须提供 --id，或启用 --auto-match/--type auto")

        if prebuilt_plan is not None:
            pass
        elif args.type == "tv":
            if args.collection_map is not None or args.allow_index_mapping:
                raise ScraperError("电视剧模式不接受合集映射参数")
            if args.absolute and args.allow_unmapped:
                raise ScraperError("绝对集数模式不允许 --allow-unmapped 猜测映射")
            if args.episode_group and not args.absolute:
                raise ScraperError("--episode-group 必须与 --absolute 同时使用")
            if args.episode_group and not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.episode_group):
                raise ScraperError("--episode-group 格式无效")
        elif args.type == "movie":
            if any(
                (
                    args.season is not None,
                    args.absolute,
                    args.allow_unmapped,
                    args.prefer_simplified,
                    args.collection_map is not None,
                    args.episode_map is not None,
                    args.episode_group is not None,
                    args.allow_index_mapping,
                )
            ):
                raise ScraperError("电影模式收到不适用的剧集或合集参数")
        else:
            if any(
                (
                    args.season is not None,
                    args.absolute,
                    args.allow_unmapped,
                    args.prefer_simplified,
                    args.episode_map is not None,
                    args.episode_group is not None,
                )
            ):
                raise ScraperError("合集模式收到不适用的剧集参数")
            if args.collection_map is not None and args.allow_index_mapping:
                raise ScraperError(
                    "--collection-map 与 --allow-index-mapping 只能选择一种"
                )

        if alist is None:
            alist = _new_alist_client(args)
        if prebuilt_plan is not None:
            plan = prebuilt_plan
        elif args.type == "tv":
            plan = build_tv_plan_smart(
                auto_episode_mode=args.auto_episode_mode,
                alist=alist,
                tmdb_client=tmdb_client,
                src_path=args.src,
                parent_path=args.parent,
                tmdb_id=args.tmdb_id,
                season=args.season if args.season is not None else 1,
                absolute=args.absolute,
                prefer_simplified=args.prefer_simplified,
                allow_unmapped=args.allow_unmapped,
                ignore_orphan_temp=args.ignore_orphan_temp,
                episode_map_path=args.episode_map,
                episode_group_id=args.episode_group,
            )
        elif args.type == "movie":
            plan = build_movie_plan(
                alist,
                tmdb_client,
                src_path=args.src,
                parent_path=args.parent,
                tmdb_id=args.tmdb_id,
                ignore_orphan_temp=args.ignore_orphan_temp,
            )
        else:
            plan = build_collection_plan(
                alist,
                tmdb_client,
                src_path=args.src,
                parent_path=args.parent,
                tmdb_id=args.tmdb_id,
                mapping_path=args.collection_map,
                allow_index_mapping=args.allow_index_mapping,
                ignore_orphan_temp=args.ignore_orphan_temp,
            )

        if selected_match is not None:
            plan.decision_trace["auto_match"] = {
                "media_type": selected_match.media_type,
                "tmdb_id": selected_match.tmdb_id,
                "title": selected_match.title,
                "year": selected_match.year,
                "confidence": selected_match.confidence,
                "status": selected_match.status,
                "score_components": dict(selected_match.score_components),
                **selected_match.decision_trace,
            }
            if selected_match.status != "confirmed":
                plan.notices.append(PlanNotice(
                    code="tmdb_match_requires_review",
                    severity="warning",
                    requires_review=True,
                    message="TMDB 自动匹配存在证据冲突，必须人工核对后才能执行。",
                    evidence={
                        "status": selected_match.status,
                        "tmdb_id": selected_match.tmdb_id,
                        "media_type": selected_match.media_type,
                        "blockers": selected_match.decision_trace.get("blockers", []),
                    },
                ))
        _append_complete_tv_resource_gaps(alist, tmdb_client, plan)
        finalize_plan_evidence(plan)
        plan.scan_report["tmdb_cache"] = tmdb_client.cache_report()
        plan.scan_report["elapsed_seconds"] = round(time.monotonic() - planning_started, 3)
        emit_progress(
            "planning_complete", completed=len(plan.files), total=len(plan.files), percent=95,
            message=f"已规划 {len(plan.files)} 个媒体文件",
        )

        print_plan(plan)
        digest = plan_sha256(plan)
        print(f"\n计划 SHA-256: {digest}")
        if args.wizard:
            if not sys.stdin.isatty():
                raise ScraperError("--wizard 需要交互式终端；批处理请使用两阶段计划流程")
            plan_path = args.plan_json or Path(
                f"scraper-plan-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
            )
            written_digest = write_plan_json(plan, plan_path)
            print(f"计划已写入: {plan_path}")
            print("请先打开计划文件核对所有路径。")
            confirmation = input("粘贴完整计划 SHA-256 以执行，直接回车取消: ").strip().lower()
            if confirmation != written_digest:
                print("未执行：确认摘要不匹配或已取消。")
                return 0
            journal_path = args.journal or Path(
                f"scraper-journal-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
            )
            execute_plan(
                alist,
                tmdb_client if planned_artwork(plan) and not args.skip_poster else None,
                plan,
                journal_path=journal_path,
                skip_poster=args.skip_poster,
                overwrite_poster=args.overwrite_poster,
                cleanup_empty_source=args.cleanup_empty_source,
            )
            print(f"✅ 完成 {len(plan.files)} 个文件；执行日志: {journal_path}")
            return 0
        if args.plan_json:
            written_digest = write_plan_json(plan, args.plan_json)
            emit_progress(
                "plan_saved", completed=len(plan.files), total=len(plan.files), percent=100,
                message="可执行计划已保存并计算 SHA-256",
            )
            print(f"计划已写入: {args.plan_json}")
            print(
                "审核后执行：python3 scraper.py --execute-plan "
                f"{args.plan_json} --approve-plan-sha256 {written_digest} --execute"
            )
        else:
            print("未指定 --plan-json；该临时计划不能进入执行流程。")
        print("\n🔍 DRY RUN：未修改任何远端文件。")
        return 0
    except (ScraperError, OSError, ValueError) as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\n已取消。", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
