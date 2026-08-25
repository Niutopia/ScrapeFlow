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
import inspect
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
    merge_bilingual_subtitle,
    normalize_subtitle_language,
    validate_managed_subtitle_content,
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
MAX_SEARCH_PAGE_BYTES = 8 * 1024 * 1024

_SUBTITLE_CONTENT_TYPES = {
    "ass": "text/x-ass",
    "ssa": "text/x-ssa",
    "srt": "application/x-subrip",
    "vtt": "text/vtt",
    "sub": "text/plain",
    "idx": "text/plain",
    "sup": "application/octet-stream",
}

_EPISODE_REGEX = re.compile(r"(?i)\bS0*(\d{1,3})[ ._-]*E0*(\d{1,4})\b|第0*(\d{1,4})[集话話]|\[0*(\d{1,4})[vV\d]*\]|\bEP0*(\d{1,4})\b")
_SEASON_REGEX = re.compile(r"(?i)\bS0*(\d{1,3})\b|第0*(\d{1,3})季|\bSeason\s*0*(\d{1,3})\b")
_DIRECT_EPISODE_PATH_RE = re.compile(
    r"(?i)\bS0*(\d{1,3})[ ._-]*E0*(\d{1,4})\b"
)
_BARE_EPISODE_RE = re.compile(r"(?<![A-Za-z0-9])0*(\d{1,3})(?!\d)")
_SUBTITLE_PACK_RE = re.compile(
    r"(?i)(?:\b(?:complete|batch|collection|pack)\b|"
    r"\bseason\s*\d+\s*(?:complete|pack)\b|"
    r"全集|全\s*\d+\s*[集話话]|合集|合輯|整季|季包|字幕包|"
    r"压缩包|壓縮包)"
)
_SUBTITLE_RANGE_RE = re.compile(
    r"(?i)(?:S0*\d{1,3}[ ._-]*E?0*\d{1,4}|"
    r"EP?0*\d{1,4}|第0*\d{1,4}[集話话])\s*(?:-|~|至|到)\s*"
    r"(?:E?P?0*\d{1,4}|第0*\d{1,4}[集話话])"
)
_ARCHIVE_SUFFIXES = frozenset({
    ".7z", ".bz2", ".gz", ".rar", ".tar", ".tgz", ".xz", ".zip",
})
_ARCHIVE_MAGIC_PREFIXES = (
    b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08", b"Rar!\x1a\x07",
    b"7z\xbc\xaf\x27\x1c", b"\x1f\x8b",
)

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


class SubtitlePauseRequested(SubtitleProviderError):
    """A caller withdrew automatic scope before a subtitle side effect.

    This module deliberately does not import the API runtime's control
    exception.  The marker is sufficient for that runtime (and the P14
    boundary) to preserve a resumable pause rather than downgrade it to a
    provider/candidate failure.
    """

    pause_requested = True


def _pause_checkpoint(pause_requested: Callable[[], bool] | None) -> None:
    """Fail closed immediately before a subtitle provider side effect."""
    if pause_requested is None:
        return
    try:
        paused = bool(pause_requested())
    except Exception as exc:
        if getattr(exc, "pause_requested", False) is True:
            raise
        raise SubtitlePauseRequested(
            "字幕补源暂停状态不可确认，已在外部操作前停止",
        ) from exc
    if paused:
        raise SubtitlePauseRequested(
            "字幕补源已暂停或不在当前 RootJob 试运行范围",
        )


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


def _candidate_episode_numbers(text: str) -> set[int]:
    """Extract episode values without mistaking ``S01`` for episode 1."""
    numbers: set[int] = set()
    for match in _EPISODE_REGEX.finditer(text):
        # The first alternative is SxxEyy: group 1 is its season and group 2
        # is its episode.  The remaining alternatives each expose only one
        # episode group.
        if match.group(2):
            numbers.add(int(match.group(2)))
            continue
        for value in match.groups()[2:]:
            if value and value.isdigit():
                numbers.add(int(value))
    return numbers


def _subtitle_identity_key(value: object) -> str:
    return re.sub(
        r"[^a-z0-9\u3400-\u9fff]+", "", str(value or "").casefold(),
    )


def _subtitle_identity_is_trusted(
    candidate_title: str,
    gap: Mapping[str, Any],
    request: Mapping[str, Any],
) -> bool:
    """Require a meaningful work-title overlap before fetching one sidecar."""
    media = gap.get("media") if isinstance(gap.get("media"), Mapping) else (
        request.get("media") if isinstance(request.get("media"), Mapping) else {}
    )
    values = [media.get("title"), *(
        media.get("aliases") if isinstance(media.get("aliases"), list) else []
    )]
    title_key = _subtitle_identity_key(candidate_title)
    for value in values:
        key = _subtitle_identity_key(value)
        han_count = sum("\u3400" <= char <= "\u9fff" for char in key)
        if key and (len(key) >= 4 or han_count >= 2) and key in title_key:
            return True
    return False


def _gap_episode_coordinate(gap: Mapping[str, Any]) -> tuple[int | None, int | None]:
    """Read the audited coordinate, using its formal video path only as fallback."""
    season = gap.get("season")
    episode = gap.get("episode")
    parsed_season = season if type(season) is int and season >= 0 else None
    parsed_episode = episode if type(episode) is int and episode > 0 else None
    if parsed_episode is not None:
        return parsed_season, parsed_episode
    match = _DIRECT_EPISODE_PATH_RE.search(str(gap.get("path") or ""))
    if match is None:
        return parsed_season, None
    return int(match.group(1)), int(match.group(2))


def _candidate_url_is_archive(value: object) -> bool:
    if not isinstance(value, str):
        return True
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return True
    path = urllib.parse.unquote(parsed.path).casefold()
    return any(path.endswith(suffix) for suffix in _ARCHIVE_SUFFIXES)


def _candidate_url_is_direct_sidecar(value: object, expected_format: object) -> bool:
    """Require a typed direct subtitle-member URL, not an opaque download page.

    A content-disposition filename or an archive manifest is only knowable
    *after* fetching an opaque endpoint.  That is too late for the exact
    acquisition contract: a provider could already have transferred a whole
    season pack.  Restrict this lane to URLs whose member extension is visible
    up front and agrees with the provider's declared format.
    """
    if not isinstance(value, str):
        return False
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    suffix = posixpath.splitext(urllib.parse.unquote(parsed.path))[1].casefold()
    declared = str(expected_format or "").casefold().lstrip(".")
    return (
        suffix in SUPPORTED_SUBTITLE_EXTENSIONS
        and suffix.lstrip(".") == declared
    )


def _subtitle_url_exactly_matches_gap(
    value: object,
    gap: Mapping[str, Any],
) -> bool:
    """Require the direct sidecar filename to corroborate the audited episode.

    A title can be stale or copied from a release page.  For episodic gaps,
    the URL-visible member filename must independently name one—and only
    one—matching episode.  This deliberately declines opaque provider
    downloads and generic ``subtitle.srt`` links rather than guessing.
    """
    if not isinstance(value, str):
        return False
    path = urllib.parse.unquote(urllib.parse.urlsplit(value).path)
    filename = posixpath.basename(path)
    if not filename:
        return False
    if (
        _SUBTITLE_PACK_RE.search(filename) is not None
        or _SUBTITLE_RANGE_RE.search(filename) is not None
    ):
        return False
    season, episode = _gap_episode_coordinate(gap)
    if episode is None:
        # A movie gap has no episode coordinate to match.  It must therefore
        # reject—not merely fail to compare—any URL-visible TV coordinate;
        # otherwise a same-title ``Movie.S01E01.srt`` can be accepted as a
        # generic movie sidecar after the title-overlap check.
        return (
            not _candidate_episode_numbers(filename)
            and _SEASON_REGEX.search(filename) is None
        )
    advertised_episodes = _candidate_episode_numbers(filename)
    if advertised_episodes:
        if advertised_episodes != {episode}:
            return False
    else:
        bare = {int(item) for item in _BARE_EPISODE_RE.findall(filename)}
        if bare != {episode}:
            return False
    advertised_seasons = {
        int(item)
        for match in _SEASON_REGEX.finditer(filename)
        for item in match.groups()
        if item is not None
    }
    return not advertised_seasons or season is None or advertised_seasons == {season}


def _subtitle_payload_is_archive(value: bytes) -> bool:
    stripped = value.lstrip().lower()
    return (
        value.startswith(_ARCHIVE_MAGIC_PREFIXES)
        or stripped.startswith((b"<", b"{"))
    )


def subtitle_candidate_exactly_matches_gap(
    candidate: Mapping[str, Any],
    gap: Mapping[str, Any],
    request: Mapping[str, Any],
) -> bool:
    """Return whether one remote sidecar is safe to fetch for one open gap.

    Ranking is deliberately not proof.  This gate requires a work identity,
    an exact episode coordinate (where the audit has one), no pack/range
    markers, and a direct non-archive URL.  Any uncertainty leaves the gap
    open for a later exact candidate rather than downloading a season bundle.
    """
    if str(gap.get("kind") or "") != "missing_subtitle":
        return False
    title = str(candidate.get("title") or "").strip()
    declared_format = str(candidate.get("format") or "srt").casefold().lstrip(".")
    if (
        candidate.get("direct_file") is not True
        or not title
        or _candidate_url_is_archive(candidate.get("url"))
        or not _candidate_url_is_direct_sidecar(
            candidate.get("url"), declared_format,
        )
        or not _subtitle_url_exactly_matches_gap(candidate.get("url"), gap)
        or _SUBTITLE_PACK_RE.search(title) is not None
        or _SUBTITLE_RANGE_RE.search(title) is not None
        or not _subtitle_identity_is_trusted(title, gap, request)
    ):
        return False
    season, episode = _gap_episode_coordinate(gap)
    if episode is None:
        # A movie sidecar has no episode coordinate, but must independently
        # prove it is not a TV episode/season member in either its title or
        # URL.  Generic movie filenames remain allowed; only an explicit TV
        # marker is disqualifying.
        return (
            not _candidate_episode_numbers(title)
            and _SEASON_REGEX.search(title) is None
        )
    advertised_episodes = _candidate_episode_numbers(title)
    if advertised_episodes:
        if advertised_episodes != {episode}:
            return False
    else:
        # Common fansub names use ``Show - 02`` rather than S01E02.  Allow
        # only that single, delimiter-bounded ordinal after the work identity
        # gate; broad numbers or ranges never become an implicit season pack.
        bare = {int(value) for value in _BARE_EPISODE_RE.findall(title)}
        if bare != {episode}:
            return False
    advertised_seasons = {
        int(value)
        for match in _SEASON_REGEX.finditer(title)
        for value in match.groups()
        if value is not None
    }
    return not advertised_seasons or season is None or advertised_seasons == {season}


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
        # The dedicated subtitle lane is OFF by default (2026-08-16 operator
        # decision): it must be explicitly enabled.  When disabled, no search
        # request is ever made.
        raw_enabled = os.getenv("SCRAPEFLOW_SUBTITLE_PROVIDER_ENABLED", "0").strip().casefold()
        enabled = raw_enabled not in {"0", "false", "no", "off", "disable", "disabled"}
        return cls(enabled=enabled)

    def _http_get(
        self,
        target_url: str,
        headers: Mapping[str, str] | None = None,
        timeout: float = 8.0,
        max_bytes: int | None = None,
        truncation_is_infra: bool = True,
    ) -> bytes:
        if self.fetcher is not None:
            try:
                return self.fetcher(target_url, headers)
            except SubtitleInfrastructureError:
                raise
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                raise SubtitleInfrastructureError(
                    f"字幕接口网络请求失败: {exc}"
                ) from exc
        req_headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) ScrapeFlow/4.0"}
        if headers:
            req_headers.update(headers)

        proxy = os.getenv("SCRAPEFLOW_HTTP_PROXY") or os.getenv("SCRAPEFLOW_HTTPS_PROXY")
        handlers: list[urllib.request.BaseHandler] = []
        if proxy:
            handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        else:
            # Never use an ambient host proxy for subtitle fetches: an
            # implicit macOS/system proxy is environment drift, not config.
            handlers.append(urllib.request.ProxyHandler({}))
        opener = urllib.request.build_opener(*handlers)

        req = urllib.request.Request(target_url, headers=req_headers)
        try:
            with opener.open(req, timeout=timeout) as resp:
                limit = max_bytes if max_bytes is not None else MAX_SEARCH_PAGE_BYTES
                data = resp.read(limit)
                if resp.read(1):
                    if truncation_is_infra:
                        raise SubtitleInfrastructureError("字幕接口响应超过大小上限")
                    return data
                return data
        except SubtitleInfrastructureError:
            raise
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
                        fmt = str(item.get("format") or "srt").lower().lstrip(".")
                        candidates.append({
                            "provider": PROVIDER_SUBTITLE_ASSRT,
                            "url": str(file_url),
                            # A provider-supplied URL is still not enough:
                            # it must visibly name one raw subtitle member,
                            # rather than an opaque detail/download page.
                            "direct_file": _candidate_url_is_direct_sidecar(
                                file_url, fmt,
                            ),
                            "format": fmt,
                            "language": lang,
                            "title": str(item.get("native_name") or item.get("videoname") or title),
                            "downloads": int(item.get("download_count") or 0),
                            "score": float(item.get("score") or 1.0),
                        })
            return candidates
        except SubtitleInfrastructureError:
            raise
        except Exception:
            return []

    def _search_subhd(self, title: str, season: int | None, episode: int | None, lang: str) -> list[dict[str, Any]]:
        """Search SubHD subtitle site."""
        if not title:
            return []
        # SubHD matches the bare show name; appending SxxEyy yields zero
        # server-side results.  Episode-level filtering happens later in the
        # weighted scoring (extract_episode_numbers on the subtitle titles).
        query_str = title
        url = f"https://subhd.tv/search/{urllib.parse.quote(query_str)}"
        try:
            data = self._http_get(url, timeout=5.0)
            html = data.decode("utf-8", errors="ignore")
            candidates = []
            # SubHD result ids are alphanumeric now (e.g. /a/KsMHj5); the
            # anchor text carries the subtitle title.
            for match in re.finditer(
                r"<a[^>]*href=['\"]/a/([A-Za-z0-9]+)['\"][^>]*>(.*?)</a>",
                html,
            ):
                sub_id = match.group(1)
                sub_title = re.sub(r"<[^>]+>", "", match.group(2)).strip()
                if not sub_title:
                    continue
                fmt = "ass" if ".ass" in sub_title.lower() else ("vtt" if ".vtt" in sub_title.lower() else "srt")
                candidates.append({
                    "provider": PROVIDER_SUBTITLE_SUBHD,
                    "url": f"https://subhd.tv/a/{sub_id}",
                    # Search pages are useful evidence but not a subtitle
                    # member URL.  Do not materialize them until the source
                    # exposes typed direct-file metadata.
                    "direct_file": False,
                    "format": fmt,
                    "language": lang,
                    "title": sub_title,
                })
            return candidates
        except SubtitleInfrastructureError:
            raise
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
                    "direct_file": False,
                    "format": fmt,
                    "language": lang,
                    "title": sub_title,
                })
            return candidates
        except SubtitleInfrastructureError:
            raise
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
                    "direct_file": False,
                    "format": fmt,
                    "language": lang,
                    "title": sub_title,
                })
            return candidates
        except SubtitleInfrastructureError:
            raise
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
                        "direct_file": _candidate_url_is_direct_sidecar(
                            sub_url, ext.lstrip("."),
                        ),
                        "format": ext.lstrip("."),
                        "language": lang,
                        "title": sub_title,
                        "downloads": 10,
                        "score": 1.0,
                    })
            return candidates
        except SubtitleInfrastructureError:
            raise
        except Exception:
            return []

    def _search_opensubtitles(self, tmdb_id: Any, season: int | None, episode: int | None, lang: str) -> list[dict[str, Any]]:
        """Search OpenSubtitles API by TMDB ID (requires an API key)."""
        if not tmdb_id:
            return []
        api_key = os.getenv("SCRAPEFLOW_OPENSUBTITLES_API_KEY", "").strip()
        if not api_key:
            # An unconfigured source must never look like an empty search:
            # the lane stays explicitly incomplete (fail closed).
            raise SubtitleInfrastructureError("OpenSubtitles 未配置 API Key")
        lang_code = "zh-CN,zh-TW,zh,zho"
        url = f"https://api.opensubtitles.com/api/v1/subtitles?tmdb_id={tmdb_id}&languages={lang_code}"
        if season is not None:
            url += f"&season_number={season}"
        if episode is not None:
            url += f"&episode_number={episode}"
        try:
            data = self._http_get(url, timeout=5.0, headers={"Api-Key": api_key})
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
                            # This endpoint is intentionally opaque.  It may
                            # negotiate/archive a payload, so it is search
                            # evidence only until the provider exposes a
                            # typed, direct sidecar URL.
                            "direct_file": False,
                            "format": str(attr.get("format") or "srt").lower().lstrip("."),
                            "language": lang,
                            "title": str(attr.get("release") or attr.get("movie_name") or ""),
                            "downloads": int(attr.get("download_count") or 0),
                            "score": float(attr.get("ratings") or 1.0),
                        })
            return candidates
        except SubtitleInfrastructureError:
            raise
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
        infrastructure_errors: list[SubtitleInfrastructureError] = []

        # 1. Shooter / Assrt API
        try:
            raw_candidates.extend(self._search_assrt(
                title=title,
                season=season if isinstance(season, int) else None,
                episode=episode if isinstance(episode, int) else None,
                lang=normalized_lang,
            ))
        except SubtitleInfrastructureError as exc:
            infrastructure_errors.append(exc)
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
        except SubtitleInfrastructureError as exc:
            infrastructure_errors.append(exc)
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
        except SubtitleInfrastructureError as exc:
            infrastructure_errors.append(exc)
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
        except SubtitleInfrastructureError as exc:
            infrastructure_errors.append(exc)
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
        except SubtitleInfrastructureError as exc:
            infrastructure_errors.append(exc)
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
            except SubtitleInfrastructureError as exc:
                infrastructure_errors.append(exc)
            except Exception:
                pass

        if not raw_candidates:
            if infrastructure_errors:
                raise SubtitleInfrastructureError(
                    "字幕搜索源不可用，无法证明没有候选"
                ) from infrastructure_errors[0]
            return []

        # Multi-dimensional scoring & Deduplication
        deduped: dict[str, dict[str, Any]] = {}
        for item in raw_candidates:
            url_key = str(item.get("url") or "")
            if not url_key or not subtitle_candidate_exactly_matches_gap(
                item, gap, request,
            ):
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
            raw = self.downloader(url)
        else:
            # Bound the HTTP read BEFORE holding the payload in memory; an
            # oversized payload is a candidate defect, not an outage.
            raw = self.discovery._http_get(
                url,
                max_bytes=MAX_SUBTITLE_BYTES + 1,
                truncation_is_infra=False,
            )
        if len(raw) > MAX_SUBTITLE_BYTES:
            raise SubtitleProviderError("下载的字幕文件超过大小上限")
        return raw

    @staticmethod
    def _candidate_format(candidate: Mapping[str, Any]) -> str:
        """Return the direct sidecar format declared by one candidate.

        The discovery gate already rejects a URL/extension mismatch.  Keeping
        this check here too makes the byte-validation path self-contained:
        a caller that injects a discovery double cannot turn an opaque or
        unsupported payload into a staged sidecar.
        """
        fmt = str(candidate.get("format") or "").casefold().lstrip(".")
        if f".{fmt}" not in SUPPORTED_SUBTITLE_EXTENSIONS:
            raise SubtitleProviderError("字幕候选声明了不支持的格式")
        return fmt

    def _fetch_exact_candidate(
        self,
        candidate: Mapping[str, Any],
        gap: Mapping[str, Any],
        request: Mapping[str, Any],
        *,
        required_language: object,
        pause_requested: Callable[[], bool] | None = None,
    ) -> tuple[bytes, str]:
        """Fetch exactly one direct sidecar and independently prove its text.

        This is deliberately shared by the Chinese and original-language
        paths.  The optional original track is not allowed to bypass the
        episode, pack/archive, size, encoding, or language gates merely
        because it will later be embedded in a Chinese sidecar.
        """
        download_url = candidate.get("url")
        if (
            not isinstance(download_url, str)
            or not subtitle_candidate_exactly_matches_gap(candidate, gap, request)
        ):
            raise SubtitleProviderError("字幕候选未通过精确单集校验")
        _pause_checkpoint(pause_requested)
        raw_bytes = self._fetch_bytes(download_url)
        if len(raw_bytes) < MIN_SUBTITLE_BYTES:
            raise SubtitleProviderError(
                f"下载的字幕文件过小 ({len(raw_bytes)} bytes)"
            )
        if len(raw_bytes) > MAX_SUBTITLE_BYTES:
            raise SubtitleProviderError(
                f"下载的字幕文件过大 ({len(raw_bytes)} bytes)"
            )
        if _subtitle_payload_is_archive(raw_bytes):
            raise SubtitleProviderError("字幕候选是网页或压缩包，拒绝整包下载")

        fmt = self._candidate_format(candidate)
        target = normalize_subtitle_language(required_language)
        if target is None:
            raise SubtitleProviderError("字幕候选缺少可验证语言")
        verdict = classify_subtitle_content(raw_bytes, target)
        if str(verdict.get("status") or "").casefold() != "satisfied":
            # A direct provider member may already be the stronger one-file
            # Chinese+original track.  Validate it against the TMDB-bound
            # original language before allowing it through the ordinary
            # Chinese fetch lane; otherwise mixed cues correctly fail the
            # pure-language classifier below.
            direct_bilingual = self._direct_bilingual_proof(raw_bytes, fmt, request)
            if not (
                direct_bilingual.get("bilingual") is True
                and direct_bilingual.get("subtitle_language") == target
            ):
                raise SubtitleProviderError(
                    f"字幕内容未能证明目标语言 ({target})"
                )
        detected_format = str(verdict.get("format") or "").casefold()
        if detected_format and detected_format != fmt:
            raise SubtitleProviderError("字幕内容格式与候选声明不一致")
        return raw_bytes, fmt

    def _direct_bilingual_proof(
        self,
        raw_bytes: bytes,
        fmt: str,
        request: Mapping[str, Any],
    ) -> dict[str, object]:
        """Recognize a provider-returned, already merged SRT sidecar.

        Most providers expose separate Chinese/original members and are
        handled by :meth:`_try_bilingual_merge`.  A few expose one SRT whose
        cues already contain both lines.  Treat it as the stronger preference
        only when the exact bytes pass the same TMDB-bound validator used by
        ordinary planning; a filename claim or a provider language field is
        never enough.
        """
        original_language = self._tmdb_verified_original_language(request)
        if original_language is None or fmt.casefold() != "srt":
            return {"bilingual": False}
        verdict = validate_managed_subtitle_content(
            raw_bytes,
            original_language,
            declared_size=len(raw_bytes),
        )
        if (
            str(verdict.get("status") or "").casefold() != "satisfied"
            or verdict.get("preference") != 0
        ):
            return {"bilingual": False}
        code = {"japanese": "ja", "english": "en", "korean": "ko"}.get(
            original_language,
        )
        chinese_language = verdict.get("chinese_language")
        lane_marker = {
            "simplified_chinese": "zh-CN",
            "traditional_chinese": "zh-TW",
        }.get(str(chinese_language))
        if code is None or lane_marker is None:
            return {"bilingual": False}
        return {
            "bilingual": True,
            "original_language": original_language,
            "subtitle_language": chinese_language,
            "bilingual_cue_count": verdict.get("cue_count"),
            "subtitle_marker": f"{lane_marker}-bilingual-{code}",
        }

    @staticmethod
    def _direct_url_identity(value: object) -> str | None:
        """Return the stable identity of a direct member URL.

        A bilingual attempt must use two separately fetched resources.  URL
        fragments never affect an HTTP fetch, so they are ignored; scheme and
        host case are normalized to avoid treating the same source as two
        different tracks.
        """
        if not isinstance(value, str):
            return None
        parsed = urllib.parse.urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None
        return urllib.parse.urlunsplit((
            parsed.scheme.casefold(),
            parsed.netloc.casefold(),
            parsed.path,
            parsed.query,
            "",
        ))

    @staticmethod
    def _tmdb_verified_original_language(request: Mapping[str, Any]) -> str | None:
        """Read a TMDB-proven original language, never a provider assertion.

        The root pipeline is responsible for obtaining this field from the
        authoritative TMDB item before calling the subtitle provider.  A
        plain ``original_language`` value is intentionally insufficient: it
        could have come from a release title or stale local metadata.
        """
        request_media = request.get("media")
        if not isinstance(request_media, Mapping):
            return None
        if request_media.get("original_language_verified_by_tmdb") is not True:
            return None
        tmdb_id = request_media.get("tmdb_id")
        if tmdb_id in {None, ""}:
            # The proof marker is meaningful only when it remains bound to a
            # concrete TMDB identity.  Do not consult gap/request top-level
            # fields: provider input may carry mutable per-gap annotations.
            return None
        language = normalize_subtitle_language(request_media.get("original_language"))
        if language not in {"japanese", "english", "korean"}:
            # A Chinese original does not yield a useful bilingual
            # Chinese/original pair, and unknown languages cannot be
            # classified conservatively by this local classifier.
            return None
        return language

    def _try_bilingual_merge(
        self,
        *,
        chinese_candidate: Mapping[str, Any],
        chinese_bytes: bytes,
        chinese_format: str,
        gap: Mapping[str, Any],
        request: Mapping[str, Any],
        pause_requested: Callable[[], bool] | None,
    ) -> tuple[bytes, str, dict[str, object]]:
        """Return one strictly merged file, or the safe Chinese-only fallback.

        The original language is optional from the user's point of view.  A
        missing, transiently unreachable, different-format, or differently
        timed original candidate must not make a verified Chinese subtitle
        disappear.  It also must never create a second sidecar.
        """
        original_language = self._tmdb_verified_original_language(request)
        fallback: dict[str, object] = {"bilingual": False}
        if original_language is None:
            return chinese_bytes, chinese_format, fallback

        original_gap = dict(gap)
        original_gap["subtitle_language"] = original_language
        chinese_url = self._direct_url_identity(chinese_candidate.get("url"))
        try:
            _pause_checkpoint(pause_requested)
            candidates = self.discovery.search_gap(original_gap, request)
        except SubtitlePauseRequested:
            raise
        except Exception:
            # The required Chinese track is already proven.  Original-track
            # search is optional, so an outage is a safe fallback rather than
            # a reason to relabel or discard the Chinese sidecar.
            return chinese_bytes, chinese_format, fallback
        if not isinstance(candidates, list):
            return chinese_bytes, chinese_format, fallback

        for candidate in candidates:
            _pause_checkpoint(pause_requested)
            if not isinstance(candidate, Mapping):
                continue
            original_url = self._direct_url_identity(candidate.get("url"))
            if original_url is None or original_url == chinese_url:
                # Never fetch the Chinese URL again and pretend its payload
                # is an original-language track.
                continue
            try:
                if self._candidate_format(candidate) != chinese_format:
                    # The merger deliberately does not transcode or align
                    # different container formats.
                    continue
                original_bytes, original_format = self._fetch_exact_candidate(
                    candidate,
                    original_gap,
                    request,
                    required_language=original_language,
                    pause_requested=pause_requested,
                )
                if original_format != chinese_format:
                    continue
                merged = merge_bilingual_subtitle(
                    chinese_bytes,
                    original_bytes,
                    original_language,
                )
                content = merged.get("content") if isinstance(merged, Mapping) else None
                if (
                    str(merged.get("status") or "").casefold() != "satisfied"
                    or not isinstance(content, bytes)
                ):
                    continue
                chinese_language = merged.get("chinese_language")
                chinese_marker = {
                    "simplified_chinese": "zh-CN",
                    "traditional_chinese": "zh-TW",
                }.get(str(chinese_language))
                original_marker = {"japanese": "ja", "english": "en", "korean": "ko"}.get(
                    original_language,
                )
                if chinese_marker is None or original_marker is None:
                    continue
                return content, chinese_format, {
                    "bilingual": True,
                    "subtitle_language": chinese_language,
                    "original_language": original_language,
                    "original_provider": candidate.get("provider"),
                    "bilingual_cue_count": merged.get("cue_count"),
                    # This is one file's language marker, not a request to
                    # create an original-language sidecar.  It lets durable
                    # post-write audit distinguish a strictly proven merged
                    # cue body from a Chinese-only .zh-CN file after restart.
                    "subtitle_marker": f"{chinese_marker}-bilingual-{original_marker}",
                }
            except SubtitlePauseRequested:
                raise
            except SubtitleInfrastructureError:
                # Optional original-track transport is not allowed to block a
                # fully validated Chinese subtitle.  The caller still gets
                # the same one-file delivery, never a partial second track.
                return chinese_bytes, chinese_format, fallback
            except (urllib.error.URLError, TimeoutError, OSError):
                return chinese_bytes, chinese_format, fallback
            except Exception:
                # Candidate, language, or timing proof failure: try another
                # exact original candidate, then retain Chinese-only.
                continue
        return chinese_bytes, chinese_format, fallback

    @staticmethod
    def _content_type(fmt: str) -> str:
        return _SUBTITLE_CONTENT_TYPES.get(fmt.casefold().lstrip("."), "text/plain")

    @staticmethod
    def _candidate_chinese_lane(candidate: Mapping[str, Any]) -> str | None:
        """Read a conservative SC/TC claim from one search result.

        Discovery adapters stamp their *requested* language onto every row;
        it is not an assertion about a particular downloaded member.  Only an
        explicit release-title marker can narrow the lane before bytes are
        read.  Returning ``None`` lets the bounded acquire loop try an
        otherwise unlabelled candidate as SC and, only when needed, TC.
        """
        title = str(candidate.get("title") or "").casefold()
        traditional_markers = (
            "cht", "big5", "zh-tw", "zh_tw", "zh-hant", "zhhant",
            "繁体", "繁體", "繁中", "繁日", "繁英", "繁",
        )
        simplified_markers = (
            "chs", "gb", "zh-cn", "zh_cn", "zh-hans", "zhhans",
            "简体", "簡體", "简中", "簡中", "简日", "简英", "简",
        )
        has_traditional = any(marker in title for marker in traditional_markers)
        has_simplified = any(marker in title for marker in simplified_markers)
        if has_traditional and not has_simplified:
            return "traditional_chinese"
        if has_simplified and not has_traditional:
            return "simplified_chinese"
        return None

    @classmethod
    def _stage_subtitle_candidate(
        cls,
        *,
        candidate: Mapping[str, Any],
        raw_bytes: bytes,
        fmt: str,
        bilingual: Mapping[str, object],
        selected_language: str,
        gap: Mapping[str, Any],
        gap_id: str,
        staging_root: str,
        workspace: Path,
        alist: Any,
        used_staging_names: set[str],
        pause_requested: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        """Stage exactly one already-proven subtitle candidate."""
        video_path = str(gap.get("path") or "")
        video_name = posixpath.basename(video_path) if video_path else "subtitle"
        video_stem = posixpath.splitext(video_name)[0]
        lang_tag = (
            "zh-CN" if selected_language == "simplified_chinese"
            else "zh-TW" if selected_language == "traditional_chinese"
            else "zh"
        )
        if bilingual.get("bilingual") is True:
            marker = bilingual.get("subtitle_marker")
            if not isinstance(marker, str) or not marker:
                raise SubtitleProviderError("双语字幕缺少持久化语言标识")
            lang_tag = marker
        sub_filename = f"{video_stem}.{lang_tag}.{fmt}"
        # Two audited rows can point at files with the same basename (for
        # example duplicate season roots). Keep the familiar name for the
        # first row, but isolate later rows so a create-only AList PUT cannot
        # collide or make one row appear to resolve another.
        if sub_filename in used_staging_names:
            safe_gap = re.sub(r"[^a-zA-Z0-9._-]+", "-", gap_id).strip(".-")[:48] or "gap"
            sub_filename = f"{video_stem}.{safe_gap}.{lang_tag}.{fmt}"
        used_staging_names.add(sub_filename)

        local_sub_path = workspace / sub_filename
        _pause_checkpoint(pause_requested)
        local_sub_path.write_bytes(raw_bytes)

        staging_sub_path = f"{staging_root.rstrip('/')}/{sub_filename}"
        try:
            mkdir = getattr(alist, "mkdir", None)
            if callable(mkdir):
                _pause_checkpoint(pause_requested)
                mkdir(posixpath.dirname(staging_root))
                _pause_checkpoint(pause_requested)
                mkdir(staging_root)
            cls._upload_staged_file(
                alist,
                staging_root=staging_root,
                staging_sub_path=staging_sub_path,
                local_sub_path=local_sub_path,
                sub_filename=sub_filename,
                raw_bytes=raw_bytes,
                content_type=cls._content_type(fmt),
                pause_requested=pause_requested,
            )
        except SubtitlePauseRequested:
            raise
        except SubtitleInfrastructureError:
            raise
        except Exception as exc:
            raise SubtitleInfrastructureError("字幕 staging 上传失败") from exc

        return {
            "path": staging_sub_path,
            "size": len(raw_bytes),
            "gap_ids": [gap_id],
            "kind": "subtitle",
            "provider": candidate.get("provider"),
            "subtitle_language": selected_language,
            **dict(bilingual),
        }

    @staticmethod
    def _parameter_names(callable_obj: object) -> list[str]:
        try:
            return [
                parameter.name
                for parameter in inspect.signature(callable_obj).parameters.values()
            ]
        except (TypeError, ValueError):
            return []

    @classmethod
    def _upload_staged_file(
        cls,
        alist: Any,
        *,
        staging_root: str,
        staging_sub_path: str,
        local_sub_path: Path,
        sub_filename: str,
        raw_bytes: bytes,
        content_type: str,
        pause_requested: Callable[[], bool] | None = None,
    ) -> None:
        """Upload one sidecar using the production AList path contract.

        A few old in-process test doubles expose ``(remote_dir, name, data)``
        while the real client exposes ``(target_path, data, content_type)``.
        Detect the narrow legacy shape by parameter names; never send a
        directory/name tuple to the production client, where it would either
        raise or target the wrong object.
        """
        upload_bytes = getattr(alist, "upload_bytes", None)
        upload_file = getattr(alist, "upload_file", None)
        if callable(upload_bytes):
            names = cls._parameter_names(upload_bytes)
            legacy_shape = (
                len(names) >= 3
                and names[1].casefold() in {"name", "filename"}
                and names[2].casefold() in {"data", "content", "payload"}
            )
            if legacy_shape:
                _pause_checkpoint(pause_requested)
                upload_bytes(staging_root, sub_filename, raw_bytes)
            else:
                # AListClient.upload_bytes(target_path, data, content_type,
                # *, overwrite=False) is the production contract.
                _pause_checkpoint(pause_requested)
                upload_bytes(staging_sub_path, raw_bytes, content_type)
            return

        if callable(upload_file):
            names = cls._parameter_names(upload_file)
            legacy_shape = (
                len(names) >= 3
                and names[1].casefold() in {"local_path", "path"}
                and names[2].casefold() in {"name", "filename"}
            )
            if legacy_shape:
                _pause_checkpoint(pause_requested)
                upload_file(staging_root, str(local_sub_path), sub_filename)
            else:
                # The real client streams from a Path and takes the complete
                # remote target path plus an explicit MIME type.
                _pause_checkpoint(pause_requested)
                upload_file(staging_sub_path, local_sub_path, content_type)
            return

        put_file = getattr(alist, "put_file", None)
        if callable(put_file):
            _pause_checkpoint(pause_requested)
            put_file(staging_sub_path, raw_bytes)
            return
        write_file_bytes = getattr(alist, "write_file_bytes", None)
        if callable(write_file_bytes):
            _pause_checkpoint(pause_requested)
            write_file_bytes(staging_sub_path, raw_bytes)
            return
        raise SubtitleInfrastructureError("AList 客户端缺少字幕上传接口")

    def acquire_subtitles(
        self,
        request: Mapping[str, Any],
        gaps: Sequence[Mapping[str, Any]],
        *,
        staging_root: str,
        workspace: Path,
        alist: Any,
        pause_requested: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        """Download, validate, and stage subtitles for the requested missing_subtitle gaps."""
        _pause_checkpoint(pause_requested)
        workspace.mkdir(parents=True, exist_ok=True)
        files_out: list[dict[str, Any]] = []
        used_staging_names: set[str] = set()

        for gap in gaps:
            _pause_checkpoint(pause_requested)
            gap_id = str(gap.get("id") or "")
            if str(gap.get("kind") or "") != "missing_subtitle":
                continue

            # Search is read-only, but it can perform a network request; it
            # must not start a new request after a withdrawn RootJob scope.
            _pause_checkpoint(pause_requested)
            candidates = self.discovery.search_gap(gap, request)
            if not candidates:
                continue

            # Search results are usually weighted by provider/language, so a
            # valid SC candidate can appear before a lower-ranked candidate
            # that can be paired with a TMDB-proven original track.  Keep the
            # first validated Chinese fallback in memory, but inspect every
            # candidate in the preferred lane before staging it.  Only then
            # try the opposite Chinese lane (SC -> TC by default).
            # The managed-track contract has one global ranking even when a
            # legacy gap happens to name a different Chinese alias: seek a
            # proven bilingual file first, then SC, then TC.
            preferred_lane = "simplified_chinese"
            lane_order = ["simplified_chinese", "traditional_chinese"]
            fallback: tuple[Mapping[str, Any], bytes, str, dict[str, object], str] | None = None
            staged = False

            for lane in lane_order:
                for candidate in candidates:
                    _pause_checkpoint(pause_requested)
                    if not isinstance(candidate, Mapping):
                        continue
                    declared_lane = self._candidate_chinese_lane(candidate)
                    # Explicitly labelled candidates belong to one lane only;
                    # an unlabelled result may still prove either lane from
                    # its bytes, which is why it is considered in both phases.
                    if declared_lane is not None and declared_lane != lane:
                        continue
                    try:
                        raw_bytes, fmt = self._fetch_exact_candidate(
                            candidate,
                            gap,
                            request,
                            required_language=lane,
                            pause_requested=pause_requested,
                        )
                        bilingual: dict[str, object] = self._direct_bilingual_proof(
                            raw_bytes, fmt, request,
                        )
                        # Both Chinese lanes can be paired with an independently
                        # proven TMDB-original member.  A same-file bilingual
                        # result wins over an earlier SC fallback; two separate
                        # SC/TC files never become a synthetic bilingual track.
                        if bilingual.get("bilingual") is not True:
                            raw_bytes, fmt, bilingual = self._try_bilingual_merge(
                                chinese_candidate=candidate,
                                chinese_bytes=raw_bytes,
                                chinese_format=fmt,
                                gap=gap,
                                request=request,
                                pause_requested=pause_requested,
                            )
                        if bilingual.get("bilingual") is True:
                            files_out.append(self._stage_subtitle_candidate(
                                candidate=candidate,
                                raw_bytes=raw_bytes,
                                fmt=fmt,
                                bilingual=bilingual,
                                selected_language=lane,
                                gap=gap,
                                gap_id=gap_id,
                                staging_root=staging_root,
                                workspace=workspace,
                                alist=alist,
                                used_staging_names=used_staging_names,
                                pause_requested=pause_requested,
                            ))
                            staged = True
                            break
                        if fallback is None:
                            fallback = (candidate, raw_bytes, fmt, bilingual, lane)
                    except SubtitlePauseRequested:
                        raise
                    except SubtitleInfrastructureError:
                        raise
                    except (urllib.error.URLError, TimeoutError, OSError) as exc:
                        # A required Chinese candidate download failure is an
                        # infrastructure outage, not proof that this lane is
                        # exhausted.  Optional original-track failures are
                        # already converted to the Chinese-only fallback.
                        raise SubtitleInfrastructureError(
                            f"字幕下载接口不可用: {exc}"
                        ) from exc
                    except Exception:
                        # Candidate content/format/episode proof failure. Try
                        # the next independently exact sidecar.
                        continue
                if staged:
                    break

            if not staged and fallback is not None:
                candidate, raw_bytes, fmt, bilingual, selected_lane = fallback
                files_out.append(self._stage_subtitle_candidate(
                    candidate=candidate,
                    raw_bytes=raw_bytes,
                    fmt=fmt,
                    bilingual=bilingual,
                    selected_language=selected_lane,
                    gap=gap,
                    gap_id=gap_id,
                    staging_root=staging_root,
                    workspace=workspace,
                    alist=alist,
                    used_staging_names=used_staging_names,
                    pause_requested=pause_requested,
                ))

        return {
            "delivery_kind": "subtitle_delivery",
            "files": files_out,
            "provider": files_out[0].get("provider") if files_out else None,
        }
