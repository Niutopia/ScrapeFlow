"""Strict read-only member scopes for the one-time movie audit phase.

This module deliberately models a movie stored beside unrelated movies as a
set of direct files, never as its shared parent directory.  It is not used by
the normal post-scrape title closure path.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import posixpath
from pathlib import PurePosixPath
import re
import secrets
from typing import Any, Mapping
import unicodedata

from engine.tools.audit_live_library import (
    DEFAULT_REQUIRED_SUBTITLE_LANGUAGES,
    SUBTITLE_EXTS,
    VIDEO_EXTS,
    companion_stem,
    external_subtitle_gap,
    parse_movie_nfo,
)


_FORMAL_CATEGORY_ROOTS = {
    "番剧": "/quark/影视/番剧",
    "美剧": "/quark/影视/美剧",
    "电影": "/quark/影视/电影",
}
_IMAGE_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".webp"})
_MOVIE_ARTWORK_SUFFIXES = frozenset({
    "poster", "fanart", "thumb", "landscape", "banner", "logo",
    "clearlogo", "clearart", "discart",
})


def canonical_digest(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _normalized_path(value: Any) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("电影成员路径无效")
    normalized = posixpath.normpath(value)
    if normalized != value or not normalized.startswith("/"):
        raise ValueError("电影成员路径必须是规范化绝对路径")
    return normalized


def _identity_key(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()


def _formal_category(path: str) -> str:
    matches = [
        category for category, root in _FORMAL_CATEGORY_ROOTS.items()
        if path != root and path.startswith(root + "/")
    ]
    if len(matches) != 1:
        raise ValueError("电影成员不在唯一正式媒体分类下")
    return matches[0]


def validate_movie_member_candidate(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the immutable candidate stored in a phase-2 worklist."""
    if not isinstance(value, Mapping):
        raise ValueError("电影成员候选必须是对象")
    expected = {
        "schema_version", "kind", "category", "target_stem", "parent_root",
        "video_path", "nfo_path", "tmdb_id", "title", "candidate_sha256",
    }
    if set(value) != expected:
        raise ValueError("电影成员候选字段不完整或包含未知字段")
    if value.get("schema_version") != 1 or value.get("kind") != "one_time_exact_movie_member_candidate":
        raise ValueError("电影成员候选类型无效")
    target_stem = _normalized_path(value.get("target_stem"))
    parent_root = _normalized_path(value.get("parent_root"))
    video_path = _normalized_path(value.get("video_path"))
    nfo_path = _normalized_path(value.get("nfo_path"))
    category = _formal_category(target_stem)
    tmdb_id = value.get("tmdb_id")
    title = value.get("title")
    # A movie identity may legitimately live below 番剧/美剧 as a special or
    # franchise member.  Media type comes from the sealed TMDB identity; the
    # category only has to match its one formal path boundary.
    if value.get("category") != category:
        raise ValueError("电影成员候选分类与路径不一致")
    if parent_root != posixpath.dirname(target_stem):
        raise ValueError("电影成员父目录与 target stem 不一致")
    if (
        PurePosixPath(video_path).suffix.casefold() not in VIDEO_EXTS
        or str(PurePosixPath(video_path).with_suffix("")) != target_stem
        or posixpath.dirname(video_path) != parent_root
        or nfo_path != target_stem + ".nfo"
    ):
        raise ValueError("电影成员视频/NFO 与 target stem 不是精确同名对")
    if (
        isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int)
        or tmdb_id <= 0 or not isinstance(title, str) or not title.strip()
    ):
        raise ValueError("电影成员候选缺少唯一 TMDB 身份")
    core = {
        "schema_version": 1,
        "kind": "one_time_exact_movie_member_candidate",
        "category": category,
        "target_stem": target_stem,
        "parent_root": parent_root,
        "video_path": video_path,
        "nfo_path": nfo_path,
        "tmdb_id": tmdb_id,
        "title": title.strip(),
    }
    digest = value.get("candidate_sha256")
    if not isinstance(digest, str) or not secrets.compare_digest(
        digest, canonical_digest(core),
    ):
        raise ValueError("电影成员候选 digest 无效")
    return {**core, "candidate_sha256": digest}


def seal_movie_member_candidate(value: Mapping[str, Any]) -> dict[str, Any]:
    core = {
        "schema_version": 1,
        "kind": "one_time_exact_movie_member_candidate",
        "category": value.get("category"),
        "target_stem": value.get("target_stem"),
        "parent_root": value.get("parent_root"),
        "video_path": value.get("video_path"),
        "nfo_path": value.get("nfo_path"),
        "tmdb_id": value.get("tmdb_id"),
        "title": value.get("title"),
    }
    return validate_movie_member_candidate({
        **core, "candidate_sha256": canonical_digest(core),
    })


def validate_exact_movie_member_scope(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a resolved member set without accepting a directory scope."""
    expected = {
        "schema_version", "kind", "category", "target_stem", "parent_root",
        "video_path", "nfo_path", "member_paths", "member_paths_sha256",
        "tmdb_id", "title", "candidate_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError("精确电影成员范围字段无效")
    if value.get("schema_version") != 1 or value.get("kind") != "one_time_exact_movie_member_scope":
        raise ValueError("精确电影成员范围类型无效")
    candidate = validate_movie_member_candidate({
        key: value.get(key) for key in (
            "schema_version", "category", "target_stem", "parent_root",
            "video_path", "nfo_path", "tmdb_id", "title", "candidate_sha256",
        )
        if key != "schema_version"
    } | {
        "schema_version": 1,
        "kind": "one_time_exact_movie_member_candidate",
    })
    raw_paths = value.get("member_paths")
    if not isinstance(raw_paths, list) or not raw_paths:
        raise ValueError("精确电影成员范围没有成员路径")
    paths = [_normalized_path(path) for path in raw_paths]
    identity_keys = [_identity_key(path) for path in paths]
    if (
        len(identity_keys) != len(set(identity_keys))
        or paths != sorted(paths, key=_identity_key)
    ):
        raise ValueError("精确电影成员路径必须唯一且稳定排序")
    if any(posixpath.dirname(path) != candidate["parent_root"] for path in paths):
        raise ValueError("精确电影成员不得越出共享父目录或进入子目录")
    if candidate["video_path"] not in paths or candidate["nfo_path"] not in paths:
        raise ValueError("精确电影成员缺少主视频或 NFO")
    for path in paths:
        suffix = PurePosixPath(path).suffix.casefold()
        if path in {candidate["video_path"], candidate["nfo_path"]}:
            continue
        if (
            suffix in SUBTITLE_EXTS
            and _identity_key(companion_stem(path)) == _identity_key(candidate["target_stem"])
        ) or _is_allowed_artwork(path, candidate["target_stem"]):
            continue
        raise ValueError("精确电影成员包含无法归属的路径")
    expected_digest = canonical_digest(paths)
    digest = value.get("member_paths_sha256")
    if not isinstance(digest, str) or not secrets.compare_digest(digest, expected_digest):
        raise ValueError("精确电影成员路径 digest 无效")
    return {
        "schema_version": 1,
        "kind": "one_time_exact_movie_member_scope",
        **{key: candidate[key] for key in (
            "category", "target_stem", "parent_root", "video_path", "nfo_path",
            "tmdb_id", "title", "candidate_sha256",
        )},
        "member_paths": paths,
        "member_paths_sha256": digest,
    }


def movie_scope_matches_candidate(
    scope: Mapping[str, Any], candidate: Mapping[str, Any],
) -> bool:
    try:
        validated_scope = validate_exact_movie_member_scope(scope)
        validated_candidate = validate_movie_member_candidate(candidate)
    except (TypeError, ValueError):
        return False
    return all(
        validated_scope.get(key) == validated_candidate.get(key)
        for key in (
            "category", "target_stem", "parent_root", "video_path", "nfo_path",
            "tmdb_id", "title", "candidate_sha256",
        )
    )


def _stable_entry(parent: str, row: Mapping[str, Any]) -> dict[str, Any]:
    name = row.get("name")
    if not isinstance(name, str) or not name or "/" in name or name in {".", ".."}:
        raise ValueError("电影共享父目录包含无效 AList 条目")
    return {
        "path": posixpath.join(parent, name),
        "is_dir": row.get("is_dir") is True,
        "size": row.get("size"),
        "modified": row.get("modified"),
        "hash": row.get("hash_info") or row.get("hash"),
    }


def _is_allowed_artwork(path: str, target_stem: str) -> bool:
    suffix = PurePosixPath(path).suffix.casefold()
    if suffix not in _IMAGE_EXTENSIONS:
        return False
    raw_stem = str(PurePosixPath(path).with_suffix(""))
    if _identity_key(raw_stem) == _identity_key(target_stem):
        return True
    target_name = _identity_key(PurePosixPath(target_stem).name)
    image_name = _identity_key(PurePosixPath(raw_stem).name)
    match = re.fullmatch(
        re.escape(target_name) + r"[-._ ](" + "|".join(
            sorted(map(re.escape, _MOVIE_ARTWORK_SUFFIXES))
        ) + r")",
        image_name,
    )
    return match is not None


def _looks_related(path: str, target_stem: str) -> bool:
    name = _identity_key(PurePosixPath(path).name)
    target = _identity_key(PurePosixPath(target_stem).name)
    return name == target or any(
        name.startswith(target + separator)
        for separator in (".", " ", "-", "_", "[", "(")
    )


def resolve_exact_movie_member_scope(
    alist: Any, candidate_value: Mapping[str, Any], *, tmdb: Any,
) -> dict[str, Any]:
    """Resolve one direct-file movie member and two read-only fingerprints."""
    candidate = validate_movie_member_candidate(candidate_value)
    parent = candidate["parent_root"]
    rows = alist.list(parent, refresh=True)
    if not isinstance(rows, list):
        raise ValueError("电影共享父目录没有返回条目数组")
    entries = [_stable_entry(parent, row) for row in rows if isinstance(row, Mapping)]
    if len(entries) != len(rows):
        raise ValueError("电影共享父目录包含非对象条目")
    entries.sort(key=lambda row: _identity_key(str(row["path"])))
    keys = [_identity_key(str(row["path"])) for row in entries]
    if len(keys) != len(set(keys)):
        raise ValueError("电影共享父目录存在大小写或 Unicode 别名冲突")

    target_stem = candidate["target_stem"]
    matching_videos = [
        row for row in entries
        if not row["is_dir"]
        and PurePosixPath(str(row["path"])).suffix.casefold() in VIDEO_EXTS
        and _identity_key(str(PurePosixPath(str(row["path"])).with_suffix("")))
        == _identity_key(target_stem)
    ]
    if len(matching_videos) != 1 or matching_videos[0]["path"] != candidate["video_path"]:
        raise ValueError("电影成员必须且只能有一个与 NFO 同 stem 的精确视频")
    nfo_rows = [row for row in entries if row["path"] == candidate["nfo_path"] and not row["is_dir"]]
    if len(nfo_rows) != 1:
        raise ValueError("电影成员缺少唯一同 stem NFO")

    member_entries: list[dict[str, Any]] = []
    unknown_related: list[str] = []
    for row in entries:
        path = str(row["path"])
        if row["is_dir"]:
            if _looks_related(path, target_stem):
                unknown_related.append(path)
            continue
        suffix = PurePosixPath(path).suffix.casefold()
        allowed = (
            path in {candidate["video_path"], candidate["nfo_path"]}
            or (
                suffix in SUBTITLE_EXTS
                and _identity_key(companion_stem(path)) == _identity_key(target_stem)
            )
            or _is_allowed_artwork(path, target_stem)
        )
        if allowed:
            member_entries.append(dict(row))
        elif _looks_related(path, target_stem):
            unknown_related.append(path)
    if unknown_related:
        raise ValueError(
            "电影成员存在无法唯一归属的同名/前缀文件: "
            + ", ".join(sorted(unknown_related)[:5])
        )

    nfo_payload = alist.read_file_prefix(candidate["nfo_path"], max_bytes=256 * 1024)
    if not isinstance(nfo_payload, bytes):
        raise ValueError("电影 NFO 读取未返回 bytes")
    nfo = parse_movie_nfo(nfo_payload)
    tmdb_ids = nfo.get("tmdb_ids")
    if tmdb_ids != [candidate["tmdb_id"]]:
        raise ValueError("电影成员 NFO 没有绑定候选中的唯一 TMDB 身份")
    movie = tmdb.get(f"/movie/{candidate['tmdb_id']}")
    if not isinstance(movie, Mapping):
        raise ValueError("电影成员 TMDB 身份查询未返回对象")
    returned_id = movie.get("id")
    if returned_id is not None and returned_id != candidate["tmdb_id"]:
        raise ValueError("电影成员 TMDB 查询返回了其他身份")
    release_date = str(movie.get("release_date") or "")
    tmdb_year = release_date[:4] if re.fullmatch(r"\d{4}-\d{2}-\d{2}", release_date) else ""
    nfo_year = str(nfo.get("year") or "")
    if nfo_year and tmdb_year and nfo_year != tmdb_year:
        raise ValueError("电影成员 NFO 年份与唯一 TMDB 身份不一致")
    for row in member_entries:
        if row["path"] == candidate["nfo_path"]:
            row["content_sha256"] = canonical_digest({
                "bytes": nfo_payload.decode("utf-8", errors="replace"),
            })
    member_entries.sort(key=lambda row: _identity_key(str(row["path"])))
    member_paths = [str(row["path"]) for row in member_entries]
    scope = validate_exact_movie_member_scope({
        "schema_version": 1,
        "kind": "one_time_exact_movie_member_scope",
        **{key: candidate[key] for key in (
            "category", "target_stem", "parent_root", "video_path", "nfo_path",
            "tmdb_id", "title", "candidate_sha256",
        )},
        "member_paths": member_paths,
        "member_paths_sha256": canonical_digest(member_paths),
    })
    parent_core = {"parent_root": parent, "entries": entries}
    member_core = {"target_stem": target_stem, "entries": member_entries}
    return {
        "scope": scope,
        "identity_evidence": {
            "tmdb_id": candidate["tmdb_id"],
            "nfo_title": str(nfo.get("title") or ""),
            "nfo_year": nfo_year,
            "tmdb_title": str(movie.get("title") or movie.get("original_title") or ""),
            "tmdb_year": tmdb_year,
        },
        "parent_inventory": {
            **parent_core, "inventory_sha256": canonical_digest(parent_core),
        },
        "member_inventory": {
            **member_core, "inventory_sha256": canonical_digest(member_core),
        },
    }


def scan_exact_movie_member_subtitle_inventory(
    _alist: Any,
    scope_value: Mapping[str, Any],
    *,
    required_subtitle_languages: tuple[str, ...] = DEFAULT_REQUIRED_SUBTITLE_LANGUAGES,
) -> dict[str, Any]:
    """Build title-closure inventory from an already resolved exact member."""
    scope = validate_exact_movie_member_scope(scope_value)
    video_path = scope["video_path"]
    subtitle_paths = [
        path for path in scope["member_paths"]
        if PurePosixPath(path).suffix.casefold() in SUBTITLE_EXTS
    ]
    gap = external_subtitle_gap(
        video_path, subtitle_paths,
        required_languages=required_subtitle_languages,
    )
    row = {
        "media_type": "movie",
        "title": scope["title"],
        "target_root": scope["target_stem"],
        "video_path": video_path,
    }
    missing: list[dict[str, Any]] = []
    if gap is None:
        companions = sorted({
            path for path in subtitle_paths
            if _identity_key(companion_stem(path)) == _identity_key(scope["target_stem"])
        })
        row.update({
            "status": "external_required_language_present",
            "required_languages": list(required_subtitle_languages),
            "companion_subtitles": companions,
            "candidate_subtitles": companions,
            "candidate_languages": [],
        })
    else:
        row.update({"status": "gap", **gap})
        missing.append(dict(row))
    return {
        "schema_version": 1,
        "audited_at": datetime.now(timezone.utc).isoformat(),
        "library_root": scope["target_stem"],
        "excluded_roots": [],
        "included_roots": [scope["target_stem"]],
        "member_paths": list(scope["member_paths"]),
        "member_paths_sha256": scope["member_paths_sha256"],
        "methodology": "one-time exact movie member + exact sidecar basename",
        "subtitle_policy": {
            "scope": "external_sidecar",
            "required_languages": list(required_subtitle_languages),
            "embedded_subtitle_status": "not_inspectable_from_alist_inventory",
        },
        "summary": {
            "videos": 1,
            "subtitles": len(subtitle_paths),
            "subtitle_inventory_rows": 1,
            "missing_subtitles": len(missing),
            "reason_codes": dict(Counter(
                item["reason_code"] for item in missing
            ).most_common()),
        },
        "missing_subtitles": missing,
        "subtitle_inventory": [row],
    }
