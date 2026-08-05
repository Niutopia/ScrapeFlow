"""Fail-closed acquisition planning for unmatched external subtitles.

This module deliberately stops before the formal media library.  It turns a
*complete*, digest-bound Quark-share or Torrent manifest into subtitle-only
member requests, verifies downloaded text as Simplified Chinese, and stores
the immutable bytes in a local evidence cache.  The existing subtitle
executor remains the only component allowed to create a formal companion.

Video members are identity evidence only.  They can never be selected for
download by this module.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile
from typing import Any, Callable, ContextManager, Iterable, Mapping, Sequence
import urllib.parse

from engine.tools.audit_live_library import companion_stem, episode_numbers
from engine.tools.refine_subtitle_audit import classify_subtitle_content
from engine.tools.subtitle_executor import (
    MAX_SUBTITLE_BYTES,
    MUTATING_SUBTITLE_LANE,
    VERIFICATION_SUBTITLE_LANE,
    canonical_digest,
)
from engine.scrapeflow.serialization import atomic_write_json


TEXT_EXTENSIONS = {".ass", ".srt"}
VIDEO_EXTENSIONS = {".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm"}
SOURCE_PROVIDERS = {"quark_share", "torrent"}
QUALITY_WORDS = {
    "1080p", "2160p", "720p", "4k", "uhd", "bluray", "bdrip", "webrip",
    "webdl", "web", "x264", "x265", "h264", "h265", "hevc", "av1",
    "aac", "flac", "ddp", "remux",
}
# Pure resolution numbers are never episode ordinals; a bare year is not
# either.  Anything else numeric that is not a season/quality token counts
# toward episode-ambiguity (two survivors = no deterministic ordinal).
RESOLUTION_NUMBERS = {"144", "240", "360", "480", "576", "720", "1080", "1440", "2160", "4320"}
# Season evidence markers: Sxx, Season N, Nth Season, 第N季/期, 第X季.
_SEASON_NUMBER_RE = re.compile(
    r"(?:"
    r"(?:^|[^0-9a-z])s0*(\d{1,3})(?=$|[^0-9a-z])"
    r"|(?:^|[^0-9a-z])s0*(\d{1,3})e0*(?:\d{1,4})(?=$|[^0-9a-z])"
    r"|season\s*0*(\d{1,3})"
    r"|0*(\d{1,3})(?:st|nd|rd|th)\s+season"
    r"|第\s*0*(\d{1,3})\s*[季期]"
    r"|第([一二三四五六七八九十]{1,6})[季期]"
    r")",
    re.I,
)
_CN_DIGITS = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
# Release-style episode ordinals on the paired source path.
_BRACKET_ORDINAL_RE = re.compile(r"\[0*(\d{1,4})\]")
_SPECIAL_ORDINAL_RE = re.compile(r"\b(?:sp|ova|oad|ncop|nced)\s*[.\-_]?\s*0*(\d{1,4})\b", re.I)
_EP_ORDINAL_RE = re.compile(r"(?:^|[^0-9a-z])ep\s*[.\-_]?\s*0*(\d{1,4})(?=$|[^0-9a-z])", re.I)
_E_ORDINAL_RE = re.compile(r"(?:^|[^0-9a-z])e\s*[.\-_]?\s*0*(\d{1,4})(?=$|[^0-9a-z])", re.I)
# Special-content markers that confine season-zero requests.  Latin markers
# match whole tokens only; 特典/映像 match inside Chinese tokens.
_SPECIAL_LATIN_TOKENS = {"sp", "ova", "oad", "nced", "ncop", "special", "specials", "extra", "extras"}
_SPECIAL_CHINESE_TOKENS = {"映像"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _title_key(value: str) -> str:
    return "".join(re.findall(r"[0-9a-z\u3400-\u9fff]+", value.casefold()))


def _path_parts(path: str) -> tuple[str, ...]:
    normalized = path.replace("\\", "/")
    parts = tuple(normalized.split("/"))
    if (
        not normalized or normalized.startswith("/") or len(parts) > 32
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise ValueError("source member path is unsafe")
    return parts


def _member_stem(path: str) -> str:
    suffix = PurePosixPath(path).suffix.casefold()
    if suffix in TEXT_EXTENSIONS:
        return PurePosixPath(companion_stem(path)).name.casefold()
    return PurePosixPath(path).stem.casefold()


def source_manifest_core(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: manifest[key]
        for key in (
            "schema_version", "kind", "provider", "locator", "release_name",
            "manifest_complete", "search_request_ids", "files", "acquisition",
        )
    }


def bind_source_manifest(
    *, provider: str, locator: str, release_name: str,
    search_request_ids: Sequence[str], files: Sequence[Mapping[str, Any]],
    acquisition: Mapping[str, Any],
) -> dict[str, Any]:
    """Create a canonical full-member source manifest with a self digest."""
    core = {
        "schema_version": 1,
        "kind": "subtitle_source_manifest",
        "provider": provider,
        "locator": locator,
        "release_name": release_name,
        "manifest_complete": True,
        "search_request_ids": sorted(set(search_request_ids)),
        "files": [dict(row) for row in files],
        "acquisition": dict(acquisition),
    }
    return {**core, "manifest_sha256": canonical_digest(core)}


def validate_source_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Validate completeness, digest, paths and provider-specific identities."""
    if manifest.get("schema_version") != 1 or manifest.get("kind") != "subtitle_source_manifest":
        raise ValueError("unsupported subtitle source manifest")
    if manifest.get("manifest_complete") is not True:
        raise ValueError("subtitle source manifest is not complete")
    provider = str(manifest.get("provider") or "")
    if provider not in SOURCE_PROVIDERS:
        raise ValueError("unsupported subtitle source provider")
    actual = str(manifest.get("manifest_sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", actual) or canonical_digest(source_manifest_core(manifest)) != actual:
        raise ValueError("subtitle source manifest digest mismatch")
    locator = str(manifest.get("locator") or "")
    release_name = str(manifest.get("release_name") or "").strip()
    request_ids = manifest.get("search_request_ids")
    files = manifest.get("files")
    acquisition = manifest.get("acquisition")
    if (
        not locator or not release_name or not isinstance(request_ids, list)
        or not all(isinstance(item, str) and item for item in request_ids)
        or not isinstance(files, list) or not files
        or not isinstance(acquisition, Mapping)
    ):
        raise ValueError("subtitle source manifest fields are incomplete")
    if provider == "quark_share":
        share_id = acquisition.get("share_id")
        if (
            not isinstance(share_id, str) or not share_id
            or locator != f"quark_share:{share_id}"
        ):
            raise ValueError("Quark source identity is incomplete")
    else:
        infohash = str(acquisition.get("infohash") or "").casefold()
        torrent_url = str(acquisition.get("torrent_url") or "")
        if (
            not re.fullmatch(r"[0-9a-f]{40}", infohash)
            or not torrent_url.startswith("https://")
            or locator != f"torrent:{torrent_url}"
        ):
            raise ValueError("Torrent source identity is incomplete")
    normalized_files = []
    identities: set[str | int] = set()
    paths: set[str] = set()
    for raw in files:
        if not isinstance(raw, Mapping):
            raise ValueError("subtitle source file row is invalid")
        path = str(raw.get("path") or "").replace("\\", "/")
        _path_parts(path)
        size = raw.get("size")
        if type(size) is not int or size <= 0:
            raise ValueError("subtitle source member size is invalid")
        identity: str | int | None
        if provider == "quark_share":
            identity = raw.get("file_id")
            if not isinstance(identity, str) or not identity:
                raise ValueError("Quark member lacks file_id")
        else:
            identity = raw.get("torrent_index")
            if type(identity) is not int or identity <= 0:
                raise ValueError("Torrent member lacks positive torrent_index")
        if path.casefold() in paths or identity in identities:
            raise ValueError("subtitle source manifest contains duplicate member identity")
        paths.add(path.casefold()); identities.add(identity)
        normalized_files.append({"path": path, "size": size, (
            "file_id" if provider == "quark_share" else "torrent_index"
        ): identity})
    normalized = dict(manifest)
    normalized["files"] = normalized_files
    normalized["search_request_ids"] = sorted(set(request_ids))
    return normalized


def _representative_search_titles(
    title: str, aliases: Sequence[str], *, limit: int = 2,
) -> list[str]:
    """Keep the primary title plus one script-diverse official alias."""
    values: list[str] = []
    seen: set[str] = set()
    for raw in (title, *aliases):
        value = str(raw).strip()
        key = _title_key(value)
        if not key or key in seen:
            continue
        seen.add(key)
        values.append(value)
    if len(values) <= limit:
        return values
    primary = values[0]
    primary_is_ascii = primary.isascii() and bool(re.search(r"[A-Za-z]", primary))
    if primary_is_ascii:
        diverse = next((value for value in values[1:] if not value.isascii()), None)
    else:
        latin = [
            value for value in values[1:]
            if value.isascii() and re.search(r"[A-Za-z]", value)
        ]
        diverse = min(latin, key=lambda value: (len(value), value.casefold())) if latin else None
    return [primary, diverse or values[1]][:limit]


def build_search_batches(
    requests_payload: Mapping[str, Any], selection: Mapping[str, Any],
) -> dict[str, Any]:
    """Create bounded search intents only for the current unmatched requests."""
    requests = {
        str(row.get("request_id")): row
        for row in requests_payload.get("requests", [])
        if isinstance(row, Mapping) and row.get("request_id")
    }
    failure_status = {
        str(row.get("request_id")): str(row.get("status") or "unknown")
        for row in selection.get("failures", [])
        if isinstance(row, Mapping) and row.get("request_id")
    }
    eligible_ids = {
        request_id for request_id, request in requests.items()
        if request.get("lane") == MUTATING_SUBTITLE_LANE
    }
    verification_only_ids = {
        request_id for request_id, request in requests.items()
        if request.get("lane") == VERIFICATION_SUBTITLE_LANE
    }
    unmatched_ids = set(failure_status) & eligible_ids
    search_ids = {
        request_id for request_id, status in failure_status.items()
        if request_id in eligible_ids and status == "no_verified_zh_CN_candidate"
    }
    groups: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for request_id in sorted(search_ids):
        request = requests.get(request_id)
        if request is None:
            continue
        groups[(str(request.get("target_root") or ""), str(request.get("title") or ""))].append(request)
    batches = []
    for (target_root, title), rows in sorted(groups.items()):
        request_ids = sorted(str(row["request_id"]) for row in rows)
        raw_aliases = [
            str(value).strip()
            for row in rows
            for value in (
                row.get("aliases") if isinstance(row.get("aliases"), list) else []
            )
            if isinstance(value, str) and value.strip()
        ]
        search_titles = _representative_search_titles(title, raw_aliases)
        aliases = search_titles[1:]
        episode_terms = []
        source_terms = []
        for row in rows:
            if type(row.get("season")) is int and isinstance(row.get("episodes"), list):
                episode_terms.extend(
                    f"S{int(row['season']):02d}E{int(ep):02d}"
                    for ep in row["episodes"] if type(ep) is int
                )
            for alias in row.get("source_episode_aliases") or []:
                if not isinstance(alias, Mapping):
                    continue
                season = alias.get("season")
                episode = alias.get("episode")
                titles = alias.get("series_titles")
                if (
                    type(season) is not int or season <= 0
                    or type(episode) is not int or episode <= 0
                    or not isinstance(titles, list)
                ):
                    continue
                source_terms.extend(
                    f"{str(source_title).strip()} S{season:02d}E{episode:02d}"
                    for source_title in titles[:2]
                    if isinstance(source_title, str) and source_title.strip()
                )
        base_terms = [*search_titles, PurePosixPath(target_root).name]
        query_terms = list(dict.fromkeys(
            term.strip() for term in [
                *base_terms,
                *source_terms,
                *(
                    f"{search_title} {episode}"
                    for episode in episode_terms[:24]
                    for search_title in search_titles
                ),
            ]
            if term and term.strip()
        ))[:30]
        core = {
            "target_root": target_root,
            "title": title,
            "aliases": aliases,
            "request_ids": request_ids,
            "query_terms": query_terms,
            "providers": ["quark_share", "torrent"],
            "manifest_requirement": "complete_recursive_member_listing",
            "member_policy": {
                "include_video": False,
                "video_members_are_metadata_only": True,
                "subtitle_extensions": sorted(TEXT_EXTENSIONS),
                "required_language_after_fetch": "zh-CN",
            },
        }
        batches.append({**core, "search_batch_id": canonical_digest(core)[:24]})
    ambiguity_cases = []
    for row in selection.get("failures", []):
        if not isinstance(row, Mapping) or row.get("status") != "ambiguous_verified_candidates":
            continue
        core = {
            "request_id": str(row.get("request_id") or ""),
            "candidate_ids": sorted(
                str(value) for value in row.get("candidate_ids", []) if isinstance(value, str)
            ),
            "candidate_evidence": [
                dict(value) for value in row.get("candidate_evidence", [])
                if isinstance(value, Mapping)
            ],
            "resolution": "awaiting_unique_deterministic_evidence",
        }
        ambiguity_cases.append({**core, "ambiguity_case_id": canonical_digest(core)[:24]})
    body = {
        "schema_version": 1, "kind": "subtitle_search_batches",
        "batches": batches, "ambiguity_cases": ambiguity_cases,
    }
    return {
        **body, "search_batches_sha256": canonical_digest(body),
        "summary": {
            "unmatched_requests": len(unmatched_ids),
            "search_required_requests": len(search_ids),
            "ambiguity_resolution_requests": sum(
                request_id in eligible_ids and status == "ambiguous_verified_candidates"
                for request_id, status in failure_status.items()
            ),
            "verification_only_requests": len(verification_only_ids),
            "batches": len(batches),
        },
    }


def _title_witness(request: Mapping[str, Any], *values: str) -> bool:
    haystack = _title_key(" ".join(values))
    aliases = [
        str(request.get("title") or ""), PurePosixPath(str(request.get("target_root") or "")).name,
    ]
    aliases.extend(str(item) for item in request.get("aliases", []) if isinstance(item, str))
    aliases.extend(
        str(item) for item in request.get("title_aliases", [])
        if isinstance(item, str)
    )
    return any(len(key) >= 2 and key in haystack for key in map(_title_key, aliases))


def _source_episode_alias_matches(
    request: Mapping[str, Any], season: int, episodes: set[int], *values: str,
) -> bool:
    haystack = _title_key(" ".join(values))
    for alias in request.get("source_episode_aliases") or []:
        if not isinstance(alias, Mapping):
            continue
        if alias.get("season") != season or alias.get("episode") not in episodes:
            continue
        titles = alias.get("series_titles")
        if isinstance(titles, list) and any(
            len(key) >= 2 and key in haystack
            for key in map(_title_key, (value for value in titles if isinstance(value, str)))
        ):
            return True
    return False


def _movie_identity_matches(request: Mapping[str, Any], paired_video_path: str, release: str) -> bool:
    requested = PurePosixPath(str(request.get("video_path") or "")).stem
    candidate = PurePosixPath(paired_video_path).stem
    if _title_key(requested) == _title_key(candidate):
        return True
    requested_years = set(re.findall(r"(?:19|20)\d{2}", requested))
    candidate_years = set(re.findall(r"(?:19|20)\d{2}", candidate + " " + release))
    if requested_years and requested_years != candidate_years:
        return False
    requested_tokens = {
        token for token in re.findall(r"[0-9a-z\u3400-\u9fff]+", requested.casefold())
        if token not in QUALITY_WORDS and not re.fullmatch(r"(?:19|20)\d{2}", token)
    }
    candidate_key = _title_key(candidate + " " + release)
    meaningful = [token for token in requested_tokens if len(_title_key(token)) >= 2]
    return bool(meaningful and all(_title_key(token) in candidate_key for token in meaningful))


def _chinese_number(value: str) -> int | None:
    total = 0
    current = 0
    for char in value:
        if char == "十":
            total += max(10, current * 10) if current else 10
            current = 0
        elif char in _CN_DIGITS:
            current = _CN_DIGITS[char]
        else:
            return None
    return total + current if total or current else None


def _extract_season(match: re.Match) -> int | None:
    for group in match.groups():
        if group is None:
            continue
        if group.isdigit():
            return int(group)
        return _chinese_number(group)
    return None


def _season_numbers(text: str) -> set[int]:
    """Explicit season markers (S01, Season 1, 2nd Season, 第2季) in text."""
    seasons: set[int] = set()
    for match in _SEASON_NUMBER_RE.finditer(text.casefold()):
        season = _extract_season(match)
        if season is not None and season >= 0:
            seasons.add(season)
    return seasons


def _bare_episode_candidates(name: str) -> set[int]:
    """Standalone numeric tokens of a filename that could be the ordinal.

    Season markers, decimals (5.1, 23.976), years and resolution numbers are
    removed first; any remaining embedded numbers (10bit, Ma10p, v2) are not
    standalone and are ignored.  More than one survivor means ambiguity and
    the caller must fail closed.
    """
    work = _SEASON_NUMBER_RE.sub(lambda match: " " * len(match.group(0)), name.casefold())
    work = re.sub(r"\d{1,3}(?:\.\d{1,3})+(?![0-9a-z])", " ", work)
    candidates: set[int] = set()
    for token in re.findall(r"(?<![0-9a-z])\d{1,4}(?![0-9a-z])", work):
        if token in RESOLUTION_NUMBERS or re.fullmatch(r"(?:19|20)\d{2}", token):
            continue
        candidates.add(int(token))
    return candidates


def _paired_ordinal(path: str) -> int | None:
    """Release-side episode ordinal from [NN], EP NN, E NN or a bare number."""
    name = PurePosixPath(path.replace("\\", "/")).name
    stem = PurePosixPath(name).stem
    candidates: set[int] = set()
    for regex in (_BRACKET_ORDINAL_RE, _SPECIAL_ORDINAL_RE, _EP_ORDINAL_RE, _E_ORDINAL_RE):
        candidates.update(int(value) for value in regex.findall(stem))
    candidates.update(_bare_episode_candidates(stem))
    return next(iter(candidates)) if len(candidates) == 1 else None


def _has_special_marker(path: str) -> bool:
    for component in PurePosixPath(path.replace("\\", "/")).parts:
        lowered = component.casefold()
        for token in re.findall(r"[0-9a-z\u3400-\u9fff]+", lowered):
            if (
                token in _SPECIAL_LATIN_TOKENS
                or token in _SPECIAL_CHINESE_TOKENS
                or "特典" in token
                or re.fullmatch(r"(?:sp|ova|oad|nced|ncop)\d{1,4}", token)
            ):
                return True
    return False


def _proven_season(
    paired_video_path: str, release_name: str, scope_video_paths: Sequence[str] | None,
) -> int | None:
    """Prove the season of a release-style ordinal instead of guessing it.

    The paired source path itself is the strongest evidence (a Season 01
    folder or an S01 in the name).  Without it, the manifest scope must be
    explicitly single-season: every season marker across the release name and
    all video members agrees on one value.
    """
    own = _season_numbers(paired_video_path)
    if len(own) == 1:
        return next(iter(own))
    if len(own) > 1:
        return None
    markers = set(_season_numbers(release_name))
    for video in scope_video_paths or ():
        markers.update(_season_numbers(video))
    return next(iter(markers)) if len(markers) == 1 else None


def _formal_season(request: Mapping[str, Any], fallback: int) -> int:
    season = request.get("season")
    if type(season) is int and season >= 0:
        return season
    return fallback


def _formal_episodes(request: Mapping[str, Any], fallback: set[int]) -> set[int]:
    episodes = request.get("episodes")
    if isinstance(episodes, list):
        parsed = {episode for episode in episodes if type(episode) is int}
        if parsed:
            return parsed
    return fallback


def request_matches_paired_video(
    request: Mapping[str, Any], paired_video_path: str, release_name: str,
    *, scope_video_paths: Sequence[str] | None = None,
) -> bool:
    """Match a formal TV request to one release-side video identity.

    SxxEyy names keep the original exact season/episode equality.  Release
    style ordinals ([NN], EP NN, E NN, bare number) only pair when the
    ordinal is unambiguous and the season is proven, never guessed: from the
    source path/release name itself or from a manifest whose season markers
    all agree.  Season-zero requests are confined to source paths carrying an
    explicit special-content marker (SP/OVA/特典/...).
    """
    if str(request.get("media_type") or "tv") == "movie":
        return _movie_identity_matches(request, paired_video_path, release_name)
    requested = episode_numbers(str(request.get("video_path") or ""))
    if requested is None:
        return _title_key(PurePosixPath(str(request.get("video_path") or "")).stem) == _title_key(
            PurePosixPath(paired_video_path).stem
        )
    requested_season, requested_episodes = requested
    paired = episode_numbers(paired_video_path)
    if paired is not None:
        paired_season, paired_episodes = paired
        return (
            requested_season == paired_season
            and requested_episodes == paired_episodes
            and _title_witness(request, paired_video_path, release_name)
        ) or _source_episode_alias_matches(
            request, paired_season, paired_episodes,
            paired_video_path, release_name,
        )
    ordinal = _paired_ordinal(paired_video_path)
    if ordinal is None or not _title_witness(request, paired_video_path, release_name):
        return False
    season = _formal_season(request, requested_season)
    episodes = _formal_episodes(request, requested_episodes)
    proven = _proven_season(paired_video_path, release_name, scope_video_paths)
    if proven is not None and _source_episode_alias_matches(
        request, proven, {ordinal}, paired_video_path, release_name,
    ):
        return True
    if _has_special_marker(paired_video_path):
        return season == 0 and ordinal in episodes
    if season > 0:
        return proven == season and ordinal in episodes
    return False


def _language_hint(path: str) -> int:
    value = path.casefold().replace("_", "-")
    if any(token in value for token in ("zh-cn", "chs", "gb", "sc.ass", "sc.srt", "\u7b80\u4e2d", "\u7b80体")):
        return 20
    if any(token in value for token in ("cht", "zh-tw", "tc.ass", "tc.srt", "\u7e41\u4e2d", "\u7e41体")):
        return -20
    return 0


def _acquisition_row(
    request: Mapping[str, Any], manifest: Mapping[str, Any], member: Mapping[str, Any],
    paired_video: Mapping[str, Any],
) -> dict[str, Any]:
    provider = str(manifest["provider"])
    base = {
        "request_id": request["request_id"],
        "lane": request["lane"],
        "provider": provider,
        "locator": manifest["locator"],
        "release_name": manifest["release_name"],
        "source_manifest_sha256": manifest["manifest_sha256"],
        "subtitle_member": dict(member),
        "paired_video_metadata": {"path": paired_video["path"], "size": paired_video["size"]},
        "include_video": False,
        "required_language_after_fetch": "zh-CN",
    }
    acquisition = manifest["acquisition"]
    if provider == "quark_share":
        base["transport"] = {
            "kind": "quark_fast_save",
            "share_id": acquisition.get("share_id"),
            "share_url": acquisition.get("share_url"),
            "passcode": acquisition.get("passcode", ""),
            "selected_file_ids": [member["file_id"]],
            "selected_member_paths": [member["path"]],
            "background_ready_session_required": True,
            "may_launch_or_restart_quark": False,
            "allow_ui_activation": False,
        }
    else:
        infohash = str(acquisition.get("infohash") or "").casefold()
        torrent_url = str(acquisition.get("torrent_url") or "")
        if not re.fullmatch(r"[0-9a-f]{40}", infohash) or not torrent_url.startswith("https://"):
            raise ValueError("Torrent acquisition identity is incomplete")
        base["transport"] = {
            "kind": "quark_magnet_subtitle_member",
            "magnet_url": "magnet:?xt=urn:btih:" + infohash + "&dn=" + urllib.parse.quote(
                str(manifest["release_name"]), safe="",
            ),
            "torrent_url": torrent_url,
            "selected_torrent_indices": [member["torrent_index"]],
            "selected_member_paths": [member["path"]],
            "local_fallback_kind": "torrent_subtitle_member_after_cloud_exhaustion",
            "background_ready_session_required": True,
            "may_launch_or_restart_quark": False,
            "allow_ui_activation": False,
        }
        # A source manifest is provider-supplied discovery evidence and can
        # never authorize local tier-3.  Any cloud-exhaustion proof must be
        # derived later from local durable provider journals.
    return base


def plan_subtitle_member_acquisition(
    requests_payload: Mapping[str, Any], selection: Mapping[str, Any],
    manifests: Iterable[Mapping[str, Any]], *, max_candidates_per_request: int = 3,
) -> dict[str, Any]:
    """Pair subtitle/video siblings and produce an immutable subtitle-only plan."""
    requests = {
        str(row.get("request_id")): row
        for row in requests_payload.get("requests", [])
        if isinstance(row, Mapping) and row.get("request_id")
    }
    failure_status = {
        str(row.get("request_id")): str(row.get("status") or "unknown")
        for row in selection.get("failures", [])
        if isinstance(row, Mapping) and row.get("request_id")
    }
    eligible_ids = {
        request_id for request_id, request in requests.items()
        if request.get("lane") == MUTATING_SUBTITLE_LANE
    }
    verification_only_ids = {
        request_id for request_id, request in requests.items()
        if request.get("lane") == VERIFICATION_SUBTITLE_LANE
    }
    unmatched = set(failure_status) & eligible_ids
    search_required = {
        request_id for request_id, status in failure_status.items()
        if request_id in eligible_ids and status == "no_verified_zh_CN_candidate"
    }
    candidates: dict[str, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    invalid_manifests = []
    valid_manifests = 0
    for raw_manifest in manifests:
        try:
            manifest = validate_source_manifest(raw_manifest)
        except (KeyError, TypeError, ValueError) as exc:
            invalid_manifests.append({
                "locator": str(raw_manifest.get("locator") or "") if isinstance(raw_manifest, Mapping) else "",
                "reason": str(exc),
            })
            continue
        valid_manifests += 1
        files = manifest["files"]
        videos_by_stem: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        video_paths: list[str] = []
        for member in files:
            if PurePosixPath(str(member["path"])).suffix.casefold() in VIDEO_EXTENSIONS:
                videos_by_stem[_member_stem(str(member["path"]))].append(member)
                video_paths.append(str(member["path"]))
        scoped_request_ids = set(manifest["search_request_ids"])
        for member in files:
            path = str(member["path"])
            extension = PurePosixPath(path).suffix.casefold()
            if extension not in TEXT_EXTENSIONS or int(member["size"]) > MAX_SUBTITLE_BYTES:
                continue
            paired_rows = videos_by_stem.get(_member_stem(path), [])
            if len(paired_rows) != 1:
                continue
            paired = paired_rows[0]
            for request_id in sorted(search_required & scoped_request_ids):
                request = requests.get(request_id)
                if request is None or not request_matches_paired_video(
                    request, str(paired["path"]), str(manifest["release_name"]),
                    scope_video_paths=video_paths,
                ):
                    continue
                row = _acquisition_row(request, manifest, member, paired)
                score = 100 + _language_hint(path)
                candidates[request_id].append((score, row))
    acquisitions = []
    unresolved = []
    for request_id in sorted(unmatched):
        rows = sorted(
            candidates.get(request_id, []),
            key=lambda item: (-item[0], str(item[1]["locator"]), str(item[1]["subtitle_member"]["path"])),
        )
        deduplicated = []
        seen = set()
        provider_counts: Counter[str] = Counter()
        for _, row in rows:
            key = (row["source_manifest_sha256"], row["subtitle_member"]["path"])
            if key in seen:
                continue
            provider = str(row.get("provider") or "")
            if provider_counts[provider] >= max_candidates_per_request:
                continue
            seen.add(key); deduplicated.append(row)
            provider_counts[provider] += 1
        if deduplicated:
            acquisitions.extend(deduplicated)
        else:
            request = requests.get(request_id, {})
            status = failure_status.get(request_id)
            unresolved.append({
                "request_id": request_id,
                "video_path": request.get("video_path"),
                "status": (
                    "retryable_ambiguity_resolution_required"
                    if status == "ambiguous_verified_candidates"
                    else "retryable_search_required"
                ),
                "reason": (
                    "multiple_verified_payloads_require_deterministic_resolution"
                    if status == "ambiguous_verified_candidates"
                    else "no_complete_exact_paired_source_manifest"
                ),
            })
    core = {
        "schema_version": 1,
        "kind": "subtitle_member_acquisition_plan",
        "requests_sha256": requests_payload.get("request_sha256"),
        "source_selection_sha256": selection.get("selection_sha256"),
        "acquisitions": acquisitions,
        "unresolved": unresolved,
    }
    return {
        **core,
        "plan_sha256": canonical_digest(core),
        "summary": {
            "unmatched_requests": len(unmatched),
            "search_required_requests": len(search_required),
            "ambiguity_resolution_requests": len(unmatched - search_required),
            "planned_members": len(acquisitions),
            "planned_requests": len({row["request_id"] for row in acquisitions}),
            "retryable_requests": len(unresolved),
            "verification_only_requests": len(verification_only_ids),
            "valid_complete_manifests": valid_manifests,
            "invalid_or_incomplete_manifests": len(invalid_manifests),
            "video_members_selected": 0,
        },
        "manifest_rejections": invalid_manifests,
    }


def quark_bridge_selection(item: Mapping[str, Any]) -> dict[str, Any]:
    """Adapt one safe Quark member to the existing exact fast-save bridge."""
    if (
        item.get("lane") != MUTATING_SUBTITLE_LANE
        or item.get("provider") != "quark_share"
        or item.get("include_video") is not False
    ):
        raise ValueError("not a subtitle-only Quark acquisition")
    member = item.get("subtitle_member")
    transport = item.get("transport")
    if not isinstance(member, Mapping) or not isinstance(transport, Mapping):
        raise ValueError("Quark acquisition is incomplete")
    request_id = str(item.get("request_id") or "")
    file_id = member.get("file_id")
    return {
        "provider": "quark_share",
        "release_name": item.get("release_name"),
        "selected_gap_ids": [request_id],
        "acquisition": {
            "kind": "quark_fast_save",
            "payload_kind": "subtitle_payload",
            "share_id": transport.get("share_id"),
            "share_url": transport.get("share_url"),
            "passcode": transport.get("passcode", ""),
            "file_id_by_gap": {request_id: [file_id]},
            "file_path_by_id": {str(file_id): member.get("path")},
            "file_size_by_id": {str(file_id): member.get("size")},
            "requires_share_revalidation": True,
            "include_video": False,
        },
    }


def quark_magnet_bridge_selection(item: Mapping[str, Any]) -> dict[str, Any]:
    """Adapt one safe Torrent member to Quark offline's exact subset parser."""
    if (
        item.get("lane") != MUTATING_SUBTITLE_LANE
        or item.get("provider") != "torrent"
        or item.get("include_video") is not False
    ):
        raise ValueError("not a subtitle-only Torrent acquisition")
    member = item.get("subtitle_member")
    transport = item.get("transport")
    if not isinstance(member, Mapping) or not isinstance(transport, Mapping):
        raise ValueError("Torrent acquisition is incomplete")
    request_id = str(item.get("request_id") or "")
    return {
        "provider": "quark_magnet",
        "release_name": item.get("release_name"),
        "selected_gap_ids": [request_id],
        "acquisition": {
            "kind": "quark_magnet_offline",
            "magnet_url": transport.get("magnet_url"),
            "torrent_url": transport.get("torrent_url"),
            "expected_files": [{
                "torrent_index": member.get("torrent_index"),
                "path": member.get("path"), "size": member.get("size"),
                "gap_ids": [request_id],
            }],
            "include_video": False,
        },
    }


def fetch_torrent_subtitle_member_after_cloud_exhaustion(
    item: Mapping[str, Any], *, cloud_exhaustion_proof: Mapping[str, Any],
    workspace_root: Path,
) -> bytes:
    """Fetch one exact Torrent subtitle index after permanent cloud exhaustion.

    This is a strict tier-3 fallback.  It downloads metainfo first, verifies
    BTIH/path/index/size against the digest-bound source manifest, then passes
    exactly one subtitle index to aria2.  Any non-empty video payload in the
    isolated workspace aborts the candidate.
    """
    attempts = cloud_exhaustion_proof.get("attempts")
    minimum_attempts = cloud_exhaustion_proof.get("minimum_attempts")
    if (
        item.get("lane") != MUTATING_SUBTITLE_LANE
        or item.get("provider") != "torrent" or item.get("include_video") is not False
        or cloud_exhaustion_proof.get("permanent") is not True
        or cloud_exhaustion_proof.get("provider") != "quark_magnet"
        or cloud_exhaustion_proof.get("search_complete") is not True
        or cloud_exhaustion_proof.get("all_candidates_resource_failed_or_absent") is not True
        or type(attempts) is not int or type(minimum_attempts) is not int
        or minimum_attempts <= 0 or attempts < minimum_attempts
    ):
        raise ValueError("local Torrent subtitle fallback is not permanently unlocked")
    member = item.get("subtitle_member")
    transport = item.get("transport")
    if not isinstance(member, Mapping) or not isinstance(transport, Mapping):
        raise ValueError("Torrent subtitle member acquisition is incomplete")
    path = str(member.get("path") or "").replace("\\", "/")
    _path_parts(path)
    extension = PurePosixPath(path).suffix.casefold()
    index, size = member.get("torrent_index"), member.get("size")
    selected = transport.get("selected_torrent_indices")
    torrent_url = str(transport.get("torrent_url") or "")
    if (
        extension not in TEXT_EXTENSIONS or type(index) is not int or index <= 0
        or type(size) is not int or size <= 0 or size > MAX_SUBTITLE_BYTES
        or selected != [index] or not torrent_url.startswith("https://")
    ):
        raise ValueError("Torrent selection is not an exact subtitle-only member")
    magnet_url = str(transport.get("magnet_url") or "")
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(magnet_url).query)
    match = re.fullmatch(r"urn:btih:([0-9a-fA-F]{40})", query.get("xt", [""])[0])
    if match is None:
        raise ValueError("Torrent subtitle acquisition has invalid BTIH")
    # Lazy import avoids making the subtitle planner depend on aria2/runtime at import time.
    from engine.tools.replenishment_local_adapter import _download_torrent

    workspace_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="subtitle-member-", dir=workspace_root) as directory:
        root = Path(directory)
        torrent_path = root / "candidate.torrent"
        manifest = _download_torrent(torrent_url, torrent_path, timeout=30, attempts=2)
        if str(manifest.get("infohash") or "").casefold() != match.group(1).casefold():
            raise ValueError("Torrent metainfo BTIH changed")
        files = manifest.get("files") if isinstance(manifest.get("files"), Mapping) else {}
        actual = files.get(index)
        if (
            not isinstance(actual, Mapping)
            or str(actual.get("path") or "").replace("\\", "/") != path
            or actual.get("size") != size
        ):
            raise ValueError("Torrent metainfo member differs from reviewed manifest")
        payload_root = root / "payload"; payload_root.mkdir()
        command = [
            "aria2c", "--seed-time=0", "--file-allocation=none",
            "--allow-overwrite=false", "--auto-file-renaming=false",
            "--summary-interval=0", "--console-log-level=warn",
            f"--dir={payload_root}", f"--select-file={index}", str(torrent_path),
        ]
        try:
            completed = subprocess.run(
                command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                timeout=max(60, min(3600, int(os.getenv("SCRAPEFLOW_SUBTITLE_TORRENT_TIMEOUT", "900")))),
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("subtitle-only Torrent fetch timed out") from exc
        if completed.returncode != 0:
            raise RuntimeError("subtitle-only Torrent fetch failed: " + " ".join(
                completed.stdout.splitlines()[-5:]
            )[:600])
        populated = [row for row in payload_root.rglob("*") if row.is_file() and row.stat().st_size > 0]
        videos = [row for row in populated if row.suffix.casefold() in VIDEO_EXTENSIONS]
        if videos:
            raise ValueError("subtitle-only Torrent fetch materialized a video payload")
        matches = [
            row for row in populated
            if row.stat().st_size == size
            and row.relative_to(payload_root).as_posix().endswith(path)
        ]
        if len(matches) != 1:
            raise ValueError("subtitle-only Torrent payload is not uniquely resolvable")
        payload = matches[0].read_bytes()
        if len(payload) != size:
            raise ValueError("subtitle-only Torrent payload size changed")
        return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_write_json(path, payload)


CONTENT_WITNESS_VERSION = 2


def materialize_verified_members(
    plan: Mapping[str, Any], *, approved_plan_sha256: str,
    fetch_member: Callable[[Mapping[str, Any]], bytes],
    cache_root: Path, journal_path: Path,
    item_guard: Callable[[], ContextManager[Any]] | None = None,
) -> dict[str, Any]:
    """Fetch only planned subtitle bytes and persist content-verified evidence.

    ``fetch_member`` is the transport boundary (Quark staging read or exact
    aria2 member fetch).  It receives an item whose ``include_video`` is false.
    Failed candidates and non-Chinese text stay retryable; one request succeeds
    on the first verified candidate and later candidates are skipped.
    """
    core = {
        key: plan[key] for key in (
            "schema_version", "kind", "requests_sha256", "source_selection_sha256",
            "acquisitions", "unresolved",
        )
    }
    actual = str(plan.get("plan_sha256") or "")
    if canonical_digest(core) != actual or actual != approved_plan_sha256:
        raise ValueError("subtitle acquisition plan digest is not approved")
    if any(
        not isinstance(item, Mapping)
        or item.get("lane") != MUTATING_SUBTITLE_LANE
        for item in plan.get("acquisitions", [])
    ):
        raise ValueError("subtitle acquisition plan contains a non-confirmed lane")
    journal = (
        json.loads(journal_path.read_text(encoding="utf-8"))
        if journal_path.exists() else {
            "schema_version": 1, "kind": "subtitle_member_acquisition_journal",
            "plan_sha256": actual, "status": "running", "records": [],
        }
    )
    if journal.get("plan_sha256") != actual or not isinstance(journal.get("records"), list):
        raise ValueError("existing acquisition journal belongs to another plan")
    satisfied = {
        str(row.get("request_id")) for row in journal["records"]
        if row.get("status") == "verified_zh_CN"
    }
    attempted = {
        (str(row.get("request_id")), str(row.get("source_manifest_sha256")), str(row.get("member_path")))
        for row in journal["records"]
        if (
            row.get("status") == "verified_zh_CN"
            or (
                row.get("status") == "retryable_not_verified_zh_CN"
                and row.get("content_witness_version") == CONTENT_WITNESS_VERSION
            )
        )
    }
    for item in plan.get("acquisitions", []):
        request_id = str(item.get("request_id") or "")
        member = item.get("subtitle_member")
        key = (
            request_id, str(item.get("source_manifest_sha256") or ""),
            str(member.get("path") or "") if isinstance(member, Mapping) else "",
        )
        if request_id in satisfied or key in attempted:
            continue
        with (item_guard() if item_guard is not None else nullcontext()):
            record = {
                "request_id": request_id, "source_manifest_sha256": key[1],
                "member_path": key[2], "provider": item.get("provider"),
                "include_video": False, "status": "running",
                "content_witness_version": CONTENT_WITNESS_VERSION,
            }
            journal["records"].append(record); _atomic_json(journal_path, journal)
            try:
                if item.get("include_video") is not False or not isinstance(member, Mapping):
                    raise ValueError("acquisition attempted to include video")
                extension = PurePosixPath(str(member.get("path") or "")).suffix.casefold()
                if extension not in TEXT_EXTENSIONS:
                    raise ValueError("selected member is not a supported text subtitle")
                payload = fetch_member(item)
                if not isinstance(payload, bytes) or not payload or len(payload) > MAX_SUBTITLE_BYTES:
                    raise ValueError("fetched subtitle payload size is invalid")
                if len(payload) != int(member.get("size") or -1):
                    raise ValueError("fetched subtitle differs from exact manifest size")
                evidence = classify_subtitle_content(payload[:256 * 1024], extension)
                if evidence.get("status") != "chinese":
                    record.update({"status": "retryable_not_verified_zh_CN", "content_evidence": evidence})
                else:
                    digest = hashlib.sha256(payload).hexdigest()
                    request_directory = canonical_digest({"request_id": request_id})[:24]
                    destination = cache_root / request_directory / f"{digest}{extension}"
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    if destination.exists():
                        current = destination.read_bytes()
                        if hashlib.sha256(current).hexdigest() != digest:
                            raise ValueError("immutable subtitle cache content changed")
                    else:
                        temporary = destination.with_suffix(destination.suffix + ".tmp")
                        temporary.write_bytes(payload); temporary.replace(destination)
                    record.update({
                        "status": "verified_zh_CN", "payload_sha256": digest,
                        "payload_size": len(payload), "cache_path": str(destination),
                        "content_evidence": evidence,
                    })
                    satisfied.add(request_id)
            except Exception as exc:
                record.update({"status": "retryable_fetch_failed", "error": f"{type(exc).__name__}: {exc}"})
            record["finished_at"] = _now(); _atomic_json(journal_path, journal)
    # Re-enter the pause boundary before the terminal journal write, including
    # the zero-acquisition case.  A pause requested during local planning must
    # not be followed by a misleading persisted "success".
    with (item_guard() if item_guard is not None else nullcontext()):
        latest = {}
        for row in journal["records"]:
            latest[str(row.get("request_id"))] = row
        journal["status"] = (
            "success" if len(satisfied) == len({str(row.get("request_id")) for row in plan.get("acquisitions", [])})
            else "retryable_incomplete"
        )
        journal["summary"] = dict(Counter(str(row.get("status")) for row in journal["records"]))
        journal["updated_at"] = _now(); _atomic_json(journal_path, journal)
    return journal


def build_verified_cache_selection(
    requests_payload: Mapping[str, Any], journal: Mapping[str, Any], *, cache_root: Path,
) -> dict[str, Any]:
    """Bind verified immutable cache rows back to the existing create-only executor."""
    from engine.tools.subtitle_executor import build_selection, validate_candidate

    all_requests_by_id = {
        str(row.get("request_id")): dict(row)
        for row in requests_payload.get("requests", [])
        if isinstance(row, Mapping) and isinstance(row.get("request_id"), str)
    }
    request_by_id = {
        request_id: row for request_id, row in all_requests_by_id.items()
        if row.get("lane") == MUTATING_SUBTITLE_LANE
    }
    candidates = []
    selected_requests = []
    root = cache_root.resolve()
    latest: dict[str, Mapping[str, Any]] = {}
    for row in journal.get("records", []) or []:
        if isinstance(row, Mapping) and isinstance(row.get("request_id"), str):
            latest[str(row["request_id"])] = row
    for request_id, row in sorted(latest.items()):
        if (
            row.get("status") == "verified_zh_CN"
            and request_id in all_requests_by_id
            and request_id not in request_by_id
        ):
            raise ValueError("verification-only subtitle request cannot be promoted")
        if row.get("status") != "verified_zh_CN" or request_id not in request_by_id:
            continue
        path = Path(str(row.get("cache_path") or "")).resolve()
        if path == root or root not in path.parents or path.suffix.casefold() not in TEXT_EXTENSIONS:
            raise ValueError("verified subtitle cache path escapes its root")
        payload = path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if digest != row.get("payload_sha256") or len(payload) != row.get("payload_size"):
            raise ValueError("verified subtitle cache payload changed")
        request = request_by_id[request_id]
        selected_requests.append(request)
        candidate = validate_candidate({
            "candidate_id": canonical_digest({
                "request_id": request_id, "payload_sha256": digest,
            })[:24],
            "path": str(path), "extension": path.suffix.casefold(),
            "source_kind": "local_verified_cache",
            "paired_video_path": request.get("video_path"),
        }, payload)
        candidates.append(candidate)
    request_core = {
        "schema_version": 1, "kind": "subtitle_requests",
        "requests": selected_requests,
    }
    narrowed = {**request_core, "request_sha256": canonical_digest(request_core)}
    return {"requests": narrowed, "selection": build_selection(narrowed, candidates)}
