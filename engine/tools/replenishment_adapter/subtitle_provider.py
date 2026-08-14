"""Dedicated lightweight Multi-Source Subtitle Provider with Weighted Ranking.

Decouples pure subtitle acquisition from the 3-tier PanSou/BT search pipeline.
Searches public subtitle sites natively (Assrt/Shooter, SubHD, Zimuku, A4k,
Subdog, AnimeTosho, OpenSubtitles), ranks candidates via a multi-dimensional
weighted scoring engine (language precision, format quality, episode match,
fansub group match), validates content in an isolated workspace, and stages
files into AList for safe installation by SimpleEngineRunner.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import json
import math
import os
from pathlib import Path
import posixpath
import re
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from engine.scrapeflow.subtitle_content import (
    classify_subtitle_content,
    normalize_subtitle_language,
)

PROVIDER_SUBTITLE_ASSRT = "assrt"
PROVIDER_SUBTITLE_SUBHD = "subhd"
PROVIDER_SUBTITLE_ZIMUKU = "zimuku"
PROVIDER_SUBTITLE_A4K = "a4k"
PROVIDER_SUBTITLE_SUBDOG = "subdog"
PROVIDER_SUBTITLE_ANIMETOSHO = "animetosho"
PROVIDER_SUBTITLE_OPENSUBTITLES = "opensubtitles"

PROVIDER_BASE_WEIGHTS: dict[str, float] = {
    PROVIDER_SUBTITLE_ASSRT: 100.0,
    PROVIDER_SUBTITLE_SUBHD: 95.0,
    PROVIDER_SUBTITLE_A4K: 92.0,
    PROVIDER_SUBTITLE_ZIMUKU: 90.0,
    PROVIDER_SUBTITLE_SUBDOG: 88.0,
    PROVIDER_SUBTITLE_ANIMETOSHO: 85.0,
    PROVIDER_SUBTITLE_OPENSUBTITLES: 80.0,
}

SUPPORTED_SUBTITLE_EXTENSIONS = frozenset({".ass", ".idx", ".srt", ".ssa", ".sub", ".sup", ".vtt"})
MIN_SUBTITLE_BYTES = 64
MAX_SUBTITLE_BYTES = 10 * 1024 * 1024

_EPISODE_REGEX = re.compile(r"(?i)\bS0*(\d{1,3})[ ._-]*E0*(\d{1,4})\b|第0*(\d{1,4})[集话話]|\[0*(\d{1,4})[vV\d]*\]|\bEP0*(\d{1,4})\b")
_SEASON_REGEX = re.compile(r"(?i)\bS0*(\d{1,3})\b|第0*(\d{1,3})季|\bSeason\s*0*(\d{1,3})\b")

_KNOWN_FANSUB_GROUPS = frozenset({
    "vcb-studio", "kamigami", "sweetsub", "lilith-raws", "airota", "moozzi2",
    "reinforce", "nekomoe", "dmhy", "subsplease", "erai-raws", "ani", "nc-raws",
    "lolihouse", "chobits", "sumisora", "caso", "dymy", "ktxp", "popgo",
    "mabors", "jyfan", "subpig", "yyeet", "zimuxia", "renren", "yyets",
})


class SubtitleProviderError(RuntimeError):
    """Base error for subtitle discovery and materialization."""

    failure_scope = "candidate"
    exclude_candidate = True


class SubtitleInfrastructureError(SubtitleProviderError):
    """Network, timeout, or configuration error during subtitle retrieval."""

    failure_scope = "infrastructure"
    exclude_candidate = False


def extract_fansub_groups(text: str) -> set[str]:
    """Extract known fansub / release group names from filename or title."""
    found: set[str] = set()
    cleaned = text.casefold()
    for group in _KNOWN_FANSUB_GROUPS:
        if group in cleaned:
            found.add(group)
    for match in re.finditer(r"\[([a-zA-Z0-9_\- +]+)\]", text):
        tag = match.group(1).strip().casefold()
        if tag and len(tag) <= 30:
            found.add(tag)
    return found


def extract_episode_numbers(text: str) -> set[int]:
    """Extract episode numbers from text."""
    numbers: set[int] = set()
    for match in _EPISODE_REGEX.finditer(text):
        for g in match.groups():
            if g and g.isdigit():
                numbers.add(int(g))
    return numbers


def score_subtitle_candidate(
    candidate: Mapping[str, Any],
    gap: Mapping[str, Any],
    request: Mapping[str, Any],
) -> float:
    """Compute multi-dimensional weighted score for a subtitle candidate."""
    provider = str(candidate.get("provider") or PROVIDER_SUBTITLE_ASSRT).lower()
    score = PROVIDER_BASE_WEIGHTS.get(provider, 80.0)

    cand_title = str(candidate.get("title") or "").strip()
    cand_format = str(candidate.get("format") or "srt").lower().lstrip(".")

    target_lang = normalize_subtitle_language(gap.get("subtitle_language") or "zh") or "simplified_chinese"
    video_path = str(gap.get("path") or "")
    video_filename = posixpath.basename(video_path) if video_path else ""

    # 1. Language matching weight
    title_lower = cand_title.casefold()
    if target_lang == "simplified_chinese":
        if any(k in title_lower for k in ("chs", "gb", "zh-cn", "简体", "简中", "简日", "简繁")):
            score += 50.0
        elif any(k in title_lower for k in ("中英", "双语", "chs_eng", "chs.eng")):
            score += 40.0
        elif any(k in title_lower for k in ("中字", "中文", "zh", "chinese")):
            score += 25.0
        elif any(k in title_lower for k in ("cht", "big5", "zh-tw", "繁体", "繁中")):
            score += 10.0
        else:
            score += 15.0
    elif target_lang == "traditional_chinese":
        if any(k in title_lower for k in ("cht", "big5", "zh-tw", "繁体", "繁中", "繁日")):
            score += 50.0
        elif any(k in title_lower for k in ("中英", "双语", "cht_eng", "cht.eng")):
            score += 40.0
        elif any(k in title_lower for k in ("中字", "中文", "zh", "chinese")):
            score += 25.0
        elif any(k in title_lower for k in ("chs", "gb", "zh-cn", "简体")):
            score += 10.0
        else:
            score += 15.0

    # 2. Subtitle Format weight
    if cand_format in ("ass", "ssa"):
        score += 25.0
    elif cand_format == "srt":
        score += 20.0
    elif cand_format == "vtt":
        score += 10.0
    else:
        score += 5.0

    # 3. Season / Episode Precision weight
    expected_season = gap.get("season")
    expected_episode = gap.get("episode")
    if expected_episode is not None and isinstance(expected_episode, int):
        cand_eps = extract_episode_numbers(cand_title)
        if expected_episode in cand_eps:
            score += 40.0
        elif f"e{expected_episode:02d}" in title_lower or f"ep{expected_episode:02d}" in title_lower or f"第{expected_episode}集" in title_lower:
            score += 40.0
        elif cand_eps:
            score -= 60.0
        else:
            score += 15.0

    if expected_season is not None and isinstance(expected_season, int):
        if f"s{expected_season:02d}" in title_lower or f"第{expected_season}季" in title_lower or f"season {expected_season}" in title_lower:
            score += 20.0

    # 4. Fansub / Release Group matching bonus
    if video_filename:
        video_groups = extract_fansub_groups(video_filename)
        cand_groups = extract_fansub_groups(cand_title)
        if video_groups and cand_groups and bool(video_groups & cand_groups):
            score += 35.0

    # 5. Rating / Download count bonus
    downloads = candidate.get("downloads")
    if isinstance(downloads, (int, float)) and downloads > 0:
        score += min(15.0, math.log10(downloads + 1) * 3.0)
    vote_score = candidate.get("score")
    if isinstance(vote_score, (int, float)) and vote_score > 0:
        score += min(10.0, float(vote_score) * 2.0)

    return score


class SubtitleDiscoveryService:
    """Multi-source subtitle discovery with native website support and weighted scoring."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        fetcher: Callable[[str, Mapping[str, str] | None], bytes] | None = None,
    ) -> None:
        self.enabled = enabled
        self.fetcher = fetcher

    @classmethod
    def from_env(cls) -> SubtitleDiscoveryService:
        raw_enabled = os.getenv("SCRAPEFLOW_SUBTITLE_PROVIDER_ENABLED", "1").strip().casefold()
        enabled = raw_enabled not in {"0", "false", "no", "off", "disable", "disabled"}
        return cls(enabled=enabled)

    def _http_get(self, target_url: str, headers: Mapping[str, str] | None = None, timeout: float = 8.0) -> bytes:
        if self.fetcher is not None:
            return self.fetcher(target_url, headers)
        req_headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) ScrapeFlow/4.0"}
        if headers:
            req_headers.update(headers)

        proxy = os.getenv("SCRAPEFLOW_HTTP_PROXY") or os.getenv("SCRAPEFLOW_HTTPS_PROXY")
        handlers: list[urllib.request.BaseHandler] = []
        if proxy:
            handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        opener = urllib.request.build_opener(*handlers)

        req = urllib.request.Request(target_url, headers=req_headers)
        try:
            with opener.open(req, timeout=timeout) as resp:
                return resp.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SubtitleInfrastructureError(f"字幕接口网络请求失败: {exc}") from exc

    def _search_assrt(self, title: str, season: int | None, episode: int | None, lang: str) -> list[dict[str, Any]]:
        """Search Shooter/Assrt API."""
        if not title:
            return []
        query_parts = [title]
        if season is not None and episode is not None:
            query_parts.append(f"S{season:02d}E{episode:02d}")
        elif episode is not None:
            query_parts.append(f"EP{episode:02d}")
        q = " ".join(query_parts)
        api_url = f"https://api.assrt.net/v1/sub/search?q={urllib.parse.quote(q)}"

        try:
            data = self._http_get(api_url, timeout=5.0)
            parsed = json.loads(data.decode("utf-8", errors="ignore"))
            items = (parsed.get("data") or {}).get("subs") or parsed.get("subtitles") or []
            candidates = []
            for item in items:
                if isinstance(item, Mapping):
                    sub_id = item.get("id") or item.get("sub_id")
                    file_url = item.get("url") or (f"https://api.assrt.net/v1/sub/detail?id={sub_id}" if sub_id else None)
                    if file_url:
                        candidates.append({
                            "provider": PROVIDER_SUBTITLE_ASSRT,
                            "url": str(file_url),
                            "format": str(item.get("format") or "srt").lower().lstrip("."),
                            "language": lang,
                            "title": str(item.get("native_name") or item.get("videoname") or title),
                            "downloads": int(item.get("download_count") or 0),
                            "score": float(item.get("score") or 1.0),
                        })
            return candidates
        except Exception:
            return []

    def _search_subhd(self, title: str, season: int | None, episode: int | None, lang: str) -> list[dict[str, Any]]:
        """Search SubHD subtitle site."""
        if not title:
            return []
        query_str = title
        if season is not None and episode is not None:
            query_str += f" S{season:02d}E{episode:02d}"
        url = f"https://subhd.tv/search/{urllib.parse.quote(query_str)}"
        try:
            data = self._http_get(url, timeout=5.0)
            html = data.decode("utf-8", errors="ignore")
            candidates = []
            for match in re.finditer(r'<a\s+href="(/a/\d+)"[^>]*>([^<]+)</a>', html):
                sub_path, sub_title = match.group(1), match.group(2).strip()
                fmt = "ass" if ".ass" in sub_title.lower() else ("vtt" if ".vtt" in sub_title.lower() else "srt")
                candidates.append({
                    "provider": PROVIDER_SUBTITLE_SUBHD,
                    "url": f"https://subhd.tv{sub_path}",
                    "format": fmt,
                    "language": lang,
                    "title": sub_title,
                    "downloads": 50,
                    "score": 1.0,
                })
            return candidates
        except Exception:
            return []

    def _search_zimuku(self, title: str, season: int | None, episode: int | None, lang: str) -> list[dict[str, Any]]:
        """Search Zimuku subtitle site."""
        if not title:
            return []
        query_str = title
        if season is not None and episode is not None:
            query_str += f" S{season:02d}E{episode:02d}"
        url = f"https://zimuku.org/search?q={urllib.parse.quote(query_str)}"
        try:
            data = self._http_get(url, timeout=5.0)
            html = data.decode("utf-8", errors="ignore")
            candidates = []
            for match in re.finditer(r'<a\s+href="(/detail/\d+\.html)"[^>]*title="([^"]+)"', html):
                sub_path, sub_title = match.group(1), match.group(2).strip()
                fmt = "ass" if ".ass" in sub_title.lower() else ("vtt" if ".vtt" in sub_title.lower() else "srt")
                candidates.append({
                    "provider": PROVIDER_SUBTITLE_ZIMUKU,
                    "url": f"https://zimuku.org{sub_path}",
                    "format": fmt,
                    "language": lang,
                    "title": sub_title,
                    "downloads": 30,
                    "score": 1.0,
                })
            return candidates
        except Exception:
            return []

    def _search_a4k(self, title: str, season: int | None, episode: int | None, lang: str) -> list[dict[str, Any]]:
        """Search A4K subtitle site."""
        if not title:
            return []
        query_str = title
        if season is not None and episode is not None:
            query_str += f" S{season:02d}E{episode:02d}"
        url = f"https://a4k.net/search?term={urllib.parse.quote(query_str)}"
        try:
            data = self._http_get(url, timeout=5.0)
            html = data.decode("utf-8", errors="ignore")
            candidates = []
            for match in re.finditer(r'<a\s+href="(/subtitle/[a-zA-Z0-9_\-]+)"[^>]*>([^<]+)</a>', html):
                sub_path, sub_title = match.group(1), match.group(2).strip()
                fmt = "ass" if ".ass" in sub_title.lower() else ("vtt" if ".vtt" in sub_title.lower() else "srt")
                candidates.append({
                    "provider": PROVIDER_SUBTITLE_A4K,
                    "url": f"https://a4k.net{sub_path}",
                    "format": fmt,
                    "language": lang,
                    "title": sub_title,
                    "downloads": 20,
                    "score": 1.0,
                })
            return candidates
        except Exception:
            return []

    def _search_animetosho(self, title: str, season: int | None, episode: int | None, lang: str) -> list[dict[str, Any]]:
        """Search AnimeTosho standalone subtitle attachments."""
        if not title:
            return []
        query_str = title
        if season is not None and episode is not None:
            query_str += f" S{season:02d}E{episode:02d}"
        elif episode is not None:
            query_str += f" {episode:02d}"
        url = f"https://animetosho.org/search?q={urllib.parse.quote(query_str)}&only_tor=0"
        try:
            data = self._http_get(url, timeout=5.0)
            html = data.decode("utf-8", errors="ignore")
            candidates = []
            for match in re.finditer(r'<a\s+href="(https://animetosho\.org/storage/attachments/[^"]+)"[^>]*>([^<]+)</a>', html):
                sub_url, sub_title = match.group(1), match.group(2).strip()
                ext = posixpath.splitext(sub_title)[1].lower()
                if ext in SUPPORTED_SUBTITLE_EXTENSIONS:
                    candidates.append({
                        "provider": PROVIDER_SUBTITLE_ANIMETOSHO,
                        "url": sub_url,
                        "format": ext.lstrip("."),
                        "language": lang,
                        "title": sub_title,
                        "downloads": 10,
                        "score": 1.0,
                    })
            return candidates
        except Exception:
            return []

    def _search_opensubtitles(self, tmdb_id: Any, season: int | None, episode: int | None, lang: str) -> list[dict[str, Any]]:
        """Search OpenSubtitles API by TMDB ID."""
        if not tmdb_id:
            return []
        lang_code = "zh-CN,zh-TW,zh,zho"
        url = f"https://api.opensubtitles.com/api/v1/subtitles?tmdb_id={tmdb_id}&languages={lang_code}"
        if season is not None:
            url += f"&season_number={season}"
        if episode is not None:
            url += f"&episode_number={episode}"
        try:
            data = self._http_get(url, timeout=5.0)
            parsed = json.loads(data.decode("utf-8", errors="ignore"))
            items = parsed.get("data") or []
            candidates = []
            for item in items:
                attr = item.get("attributes") if isinstance(item, Mapping) else {}
                files = attr.get("files") or []
                for f in files:
                    file_id = f.get("file_id")
                    if file_id:
                        candidates.append({
                            "provider": PROVIDER_SUBTITLE_OPENSUBTITLES,
                            "url": f"https://api.opensubtitles.com/api/v1/download/{file_id}",
                            "format": str(attr.get("format") or "srt").lower().lstrip("."),
                            "language": lang,
                            "title": str(attr.get("release") or attr.get("movie_name") or ""),
                            "downloads": int(attr.get("download_count") or 0),
                            "score": float(attr.get("ratings") or 1.0),
                        })
            return candidates
        except Exception:
            return []

    def search_gap(
        self,
        gap: Mapping[str, Any],
        request: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Search and rank subtitle candidates across all built-in sources."""
        if not self.enabled:
            return []

        media = gap.get("media") if isinstance(gap.get("media"), Mapping) else (
            request.get("media") if isinstance(request.get("media"), Mapping) else {}
        )
        title = str(media.get("title") or request.get("query") or request.get("title") or gap.get("title") or "").strip()
        tmdb_id = media.get("tmdb_id") or request.get("tmdb_id")
        season = gap.get("season") or request.get("season")
        episode = gap.get("episode")
        lang = str(gap.get("subtitle_language") or "zh").strip()
        normalized_lang = normalize_subtitle_language(lang) or "simplified_chinese"

        if not title and not tmdb_id:
            return []

        raw_candidates: list[dict[str, Any]] = []

        # 1. Shooter / Assrt API
        try:
            raw_candidates.extend(self._search_assrt(
                title=title,
                season=season if isinstance(season, int) else None,
                episode=episode if isinstance(episode, int) else None,
                lang=normalized_lang,
            ))
        except Exception:
            pass

        # 2. SubHD
        try:
            raw_candidates.extend(self._search_subhd(
                title=title,
                season=season if isinstance(season, int) else None,
                episode=episode if isinstance(episode, int) else None,
                lang=normalized_lang,
            ))
        except Exception:
            pass

        # 3. Zimuku (字幕库)
        try:
            raw_candidates.extend(self._search_zimuku(
                title=title,
                season=season if isinstance(season, int) else None,
                episode=episode if isinstance(episode, int) else None,
                lang=normalized_lang,
            ))
        except Exception:
            pass

        # 4. A4K 字幕网
        try:
            raw_candidates.extend(self._search_a4k(
                title=title,
                season=season if isinstance(season, int) else None,
                episode=episode if isinstance(episode, int) else None,
                lang=normalized_lang,
            ))
        except Exception:
            pass

        # 5. AnimeTosho attachments
        try:
            raw_candidates.extend(self._search_animetosho(
                title=title,
                season=season if isinstance(season, int) else None,
                episode=episode if isinstance(episode, int) else None,
                lang=normalized_lang,
            ))
        except Exception:
            pass

        # 6. OpenSubtitles API
        if tmdb_id:
            try:
                raw_candidates.extend(self._search_opensubtitles(
                    tmdb_id=tmdb_id,
                    season=season if isinstance(season, int) else None,
                    episode=episode if isinstance(episode, int) else None,
                    lang=normalized_lang,
                ))
            except Exception:
                pass

        if not raw_candidates:
            return []



        if not raw_candidates:
            return []

        # Multi-dimensional scoring & Deduplication
        deduped: dict[str, dict[str, Any]] = {}
        for item in raw_candidates:
            url_key = str(item.get("url") or "")
            if not url_key:
                continue
            item["gap_id"] = str(gap.get("id") or "")
            computed_score = score_subtitle_candidate(item, gap, request)
            item["weight_score"] = computed_score
            if url_key not in deduped or computed_score > deduped[url_key]["weight_score"]:
                deduped[url_key] = item

        scored_list = list(deduped.values())
        scored_list.sort(key=lambda c: float(c.get("weight_score") or 0.0), reverse=True)
        return scored_list


class SubtitleMaterializer:
    """Acquires subtitle candidates into local workspace and uploads to AList staging."""

    def __init__(
        self,
        *,
        discovery: SubtitleDiscoveryService | None = None,
        downloader: Callable[[str], bytes] | None = None,
    ) -> None:
        self.discovery = discovery or SubtitleDiscoveryService.from_env()
        self.downloader = downloader

    def _fetch_bytes(self, url: str) -> bytes:
        if self.downloader is not None:
            return self.downloader(url)
        return self.discovery._http_get(url)

    def acquire_subtitles(
        self,
        request: Mapping[str, Any],
        gaps: Sequence[Mapping[str, Any]],
        *,
        staging_root: str,
        workspace: Path,
        alist: Any,
    ) -> dict[str, Any]:
        """Download, validate, and stage subtitles for the requested missing_subtitle gaps."""
        workspace.mkdir(parents=True, exist_ok=True)
        files_out: list[dict[str, Any]] = []

        for gap in gaps:
            gap_id = str(gap.get("id") or "")
            if str(gap.get("kind") or "") != "missing_subtitle":
                continue

            candidates = self.discovery.search_gap(gap, request)
            if not candidates:
                continue

            acquired_file: dict[str, Any] | None = None
            last_err: Exception | None = None

            for candidate in candidates:
                download_url = candidate.get("url")
                if not download_url:
                    continue

                try:
                    raw_bytes = self._fetch_bytes(download_url)
                    if len(raw_bytes) < MIN_SUBTITLE_BYTES:
                        raise SubtitleProviderError(f"下载的字幕文件过小 ({len(raw_bytes)} bytes)")
                    if len(raw_bytes) > MAX_SUBTITLE_BYTES:
                        raise SubtitleProviderError(f"下载的字幕文件过大 ({len(raw_bytes)} bytes)")

                    fmt = str(candidate.get("format") or "srt").lower().lstrip(".")
                    if f".{fmt}" not in SUPPORTED_SUBTITLE_EXTENSIONS:
                        fmt = "srt"

                    target_lang = normalize_subtitle_language(gap.get("subtitle_language") or "zh") or "simplified_chinese"
                    verdict = classify_subtitle_content(raw_bytes, target_lang)
                    if str(verdict.get("status") or "").casefold() == "missing":
                        raise SubtitleProviderError(f"字幕内容未通过目标语言 ({target_lang}) 校验")

                    # Name staging file to match video stem
                    video_path = str(gap.get("path") or "")
                    video_name = posixpath.basename(video_path) if video_path else "subtitle"
                    video_stem = posixpath.splitext(video_name)[0]
                    lang_tag = "zh-CN" if target_lang == "simplified_chinese" else ("zh-TW" if target_lang == "traditional_chinese" else "zh")
                    sub_filename = f"{video_stem}.{lang_tag}.{fmt}"

                    local_sub_path = workspace / sub_filename
                    local_sub_path.write_bytes(raw_bytes)

                    staging_sub_path = f"{staging_root.rstrip('/')}/{sub_filename}"
                    if hasattr(alist, "upload_bytes"):
                        alist.upload_bytes(staging_root, sub_filename, raw_bytes)
                    elif hasattr(alist, "upload_file"):
                        alist.upload_file(staging_root, str(local_sub_path), sub_filename)
                    elif hasattr(alist, "put_file"):
                        alist.put_file(staging_sub_path, raw_bytes)
                    elif hasattr(alist, "write_file_bytes"):
                        alist.write_file_bytes(staging_sub_path, raw_bytes)

                    acquired_file = {
                        "path": staging_sub_path,
                        "size": len(raw_bytes),
                        "gap_ids": [gap_id],
                        "kind": "subtitle",
                        "provider": candidate.get("provider"),
                    }
                    break
                except SubtitleInfrastructureError:
                    raise
                except Exception as exc:
                    last_err = exc
                    continue

            if acquired_file is not None:
                files_out.append(acquired_file)

        return {
            "delivery_kind": "subtitle_delivery",
            "files": files_out,
            "provider": PROVIDER_SUBTITLE_ASSRT,
        }
