"""Pure episode-coverage parsing shared by selection and source adapters.

This module has no Engine, Local, provider, filesystem, or network dependency.
It exists so an Engine-side provider adapter never needs to import a Local API
implementation merely to compare episode evidence.
"""

from __future__ import annotations

import re
from typing import Any
import unicodedata


EPISODE_RE = re.compile(
    r"(?<![A-Z0-9])S0*(\d{1,3})[\s._-]*E(?:P)?\s*0*(\d{1,4})(?!\d)"
    # Require an episode marker at the end of a range.  A bare title number
    # after a separator (``S00E01 - 86 - Eighty Six``) is not E86.
    r"(?:\s*[-~–—]\s*E(?:P)?\s*0*(\d{1,4})(?!\d))?",
    re.I,
)
X_EPISODE_RE = re.compile(
    r"(?<!\d)0*(\d{1,3})\s*[x×]\s*0*(\d{1,4})(?!\d)"
    r"(?:\s*[-~–—]\s*0*(\d{1,4})(?!\d))?",
    re.I,
)
SEASON_DASH_EPISODE_RE = re.compile(
    r"(?<![A-Z0-9])(?:S|Season\s+)0*(\d{1,3})\s*[-–—]\s*"
    r"(?:E(?:P)?\s*)?0*(\d{1,3})"
    r"(?:\s*[-~–—]\s*(?:E(?:P)?\s*)?0*(\d{1,3}))?(?!\d|P\b)",
    re.I,
)
# Some Chinese release groups put the season marker and a pair of episode
# ordinals in adjacent brackets, for example ``[S4][17_89]``.  The first
# ordinal is the season-local episode and the second is the whole-series
# ordinal.  This is deliberately a separate, strict matcher: a bare
# ``[17_89]`` has no season evidence and must not be assigned to a caller's
# default season.
DUAL_SEASON_BRACKET_EPISODE_RE = re.compile(
    r"\[\s*S0*(?P<season>\d{1,3})\s*\]\s*"
    r"\[\s*0*(?P<local>\d{1,4})\s*[_/]\s*0*(?P<absolute>\d{1,4})\s*\]",
    re.I,
)
# A few indexes omit the brackets around the season while retaining the
# explicit ``S`` marker (``S4[17_89]``).  Keep this form equally narrow and
# require the paired ordinal itself to be bracketed.
DUAL_SEASON_MARKED_BRACKET_EPISODE_RE = re.compile(
    r"(?<![A-Z0-9])S0*(?P<season>\d{1,3})(?![A-Z0-9])\s*"
    r"\[\s*0*(?P<local>\d{1,4})\s*[_/]\s*0*(?P<absolute>\d{1,4})\s*\]",
    re.I,
)
# Release groups also place the season marker inside the preceding title
# bracket: ``[... S4][17_89]``.  The closing bracket is part of the strict
# evidence; a bare ``S4 17_89`` remains unsupported.
DUAL_SEASON_TRAILING_BRACKET_EPISODE_RE = re.compile(
    r"(?<![A-Z0-9])S0*(?P<season>\d{1,3})\s*\]\s*"
    r"\[\s*0*(?P<local>\d{1,4})\s*[_/]\s*0*(?P<absolute>\d{1,4})\s*\]",
    re.I,
)
CHINESE_EPISODE_RE = re.compile(
    r"第\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*季"
    r"[^\n]{0,30}?第?\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话]"
    r"(?:\s*[-~–—至到]\s*第?\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话])?",
)
EPISODE_ONLY_RE = re.compile(
    r"(?<![A-Z0-9])E(?:P)?0*(\d{1,4})(?!\d)"
    r"(?:\s*[-~–—]\s*E(?:P)?\s*0*(\d{1,4})(?!\d))?",
    re.I,
)
CHINESE_EPISODE_ONLY_RE = re.compile(
    r"第\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话]"
    r"(?:\s*[-~–—至到]\s*第?\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话])?",
)
ANIME_EPISODE_RANGE_RE = re.compile(
    r"(?<!\d)\(\s*0*(\d{1,3})\s*[-~–—]\s*0*(\d{1,3})\s*\)(?!\d)",
    re.I,
)
ANIME_BRACKET_RANGE_RE = re.compile(
    r"\[\s*0*(\d{1,3})\s*[-~–—]\s*0*(\d{1,3})\s*(?:全集|全|Fin)?\s*\]",
    re.I,
)
ANIME_BRACKET_EPISODE_RE = re.compile(r"\[\s*0*(\d{1,3})\s*\]", re.I)
ANIME_DASH_EPISODE_RE = re.compile(
    r"\s[-–—]\s*0*(\d{1,3})(?=\s|\(|\[|$)", re.I,
)
SEASON_RE = re.compile(
    r"(?<![A-Z0-9])S0*(\d{1,3})(?!\d)(?!\s*E\d)"
    r"|\bSeason\s+0*(\d{1,3})\b",
    re.I,
)
SEASON_RANGE_RE = re.compile(
    r"(?<![A-Z0-9])S0*(\d{1,3})\s*[-~–—]\s*"
    r"S?0*(\d{1,3})(?!\d)(?!\s*E\d)",
    re.I,
)
CHINESE_SEASON_RE = re.compile(
    r"第\s*([一二三四五六七八九十百零〇两\d]{1,5})\s*季"
)
CHINESE_DIGITS = {
    "零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3,
    "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
}


def normalized_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return re.sub(r"[^\w\u3400-\u9fff]+", "", text)


def _parse_chinese_number(value: str) -> int | None:
    text = value.strip()
    if text.isdecimal():
        return int(text)
    if text in CHINESE_DIGITS:
        return CHINESE_DIGITS[text]
    if "百" in text:
        left, right = text.split("百", 1)
        hundreds = CHINESE_DIGITS.get(left, 1 if not left else -1)
        remainder = _parse_chinese_number(right) if right else 0
        return None if hundreds < 0 or remainder is None else hundreds * 100 + remainder
    if "十" in text:
        left, right = text.split("十", 1)
        tens = CHINESE_DIGITS.get(left, 1 if not left else -1)
        ones = CHINESE_DIGITS.get(right, 0 if not right else -1)
        return None if tens < 0 or ones < 0 else tens * 10 + ones
    return None


def _episode_ranges(value: Any) -> list[tuple[int, int, int]]:
    text = str(value or "")
    output: list[tuple[int, int, int]] = []
    # Parse canonical ``SxxEyy``/``4x17`` forms first.  The season-dash form
    # is intentionally delayed: when a strict dual label is present in the
    # same member (``S04-89 [S4][17_89]``), the dash number is the cumulative
    # ordinal and must not override the proven local ordinal.
    for pattern in (EPISODE_RE, X_EPISODE_RE):
        for match in pattern.finditer(text):
            season = int(match.group(1))
            start = int(match.group(2))
            end = int(match.group(3) or start)
            output.append((season, start, end))
    for match in CHINESE_EPISODE_RE.finditer(text):
        values = [
            _parse_chinese_number(item) if item else None
            for item in match.groups()
        ]
        season, start, end = values[0], values[1], values[2] or values[1]
        if isinstance(season, int) and isinstance(start, int) and isinstance(end, int):
            output.append((season, start, end))
    dual_matches = [
        match for pattern in (
            DUAL_SEASON_BRACKET_EPISODE_RE,
            DUAL_SEASON_MARKED_BRACKET_EPISODE_RE,
            DUAL_SEASON_TRAILING_BRACKET_EPISODE_RE,
        ) for match in pattern.finditer(text)
    ]
    dual_value: tuple[int, int, int] | None = None
    if dual_matches:
        dual_values = {
            (
                int(match.group("season")),
                int(match.group("local")),
                int(match.group("absolute")),
            )
            for match in dual_matches
        }
        # Multiple distinct dual pairs in one member are ambiguous (they can
        # be neighboring episodes, a pack label, or unrelated metadata).
        if len(dual_values) == 1:
            season, local, absolute = next(iter(dual_values))
            if (
                season > 0 and local > 0 and absolute > local
                and absolute <= 9999
            ):
                dual_value = (season, local, absolute)
                has_conflicting_canonical = any(
                    item_season == season
                    and not (item_start <= local <= item_end)
                    for item_season, item_start, item_end in output
                )
                if has_conflicting_canonical:
                    dual_value = None
    for match in SEASON_DASH_EPISODE_RE.finditer(text):
        season = int(match.group(1))
        start = int(match.group(2))
        end = int(match.group(3) or start)
        # A valid dual label supersedes only the same-season dash shorthand;
        # unrelated season-dash evidence remains available to the caller.
        if dual_value is not None and season == dual_value[0]:
            continue
        output.append((season, start, end))
    if dual_value is not None:
        output.append((dual_value[0], dual_value[1], dual_value[1]))
    return output


def expanded_episode_ids(value: Any) -> set[str]:
    output: set[str] = set()
    for season, start, end in _episode_ranges(value):
        if season < 0 or start <= 0 or end < start or end - start > 5000:
            continue
        output.update(
            f"S{season:02d}E{episode:02d}" for episode in range(start, end + 1)
        )
    return output


def season_markers(value: str) -> set[int]:
    output = {
        int(match.group(1) or match.group(2))
        for match in SEASON_RE.finditer(value)
    }
    for match in SEASON_RANGE_RE.finditer(value):
        between = value[match.end(1):match.start(2)]
        if (
            re.search(r"\s[-~–—]\s", between)
            and not re.search(r"[-~–—]\s*S", between, re.I)
        ):
            continue
        start, end = int(match.group(1)), int(match.group(2))
        if 0 < start <= end <= 999 and end - start <= 100:
            output.update(range(start, end + 1))
    for match in CHINESE_SEASON_RE.finditer(value):
        number = _parse_chinese_number(match.group(1))
        if isinstance(number, int) and number > 0:
            output.add(number)
    return output


def coverage_tokens(
    value: Any, *, default_seasons: set[int] | None = None,
) -> set[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return set()
    output: set[str] = set()
    for item in value:
        # Once a filename already contains an explicit SxxEyy token, a bare
        # numeric dash suffix is title/edition text unless it carries its own
        # episode marker.  Otherwise ``S00E01 - 86 - Eighty Six`` would still
        # gain a false S00E86 through the anime fallback below.
        explicit_episode_ids = expanded_episode_ids(item)
        output.update(explicit_episode_ids)
        has_marked_episode = bool(
            EPISODE_ONLY_RE.search(item) or CHINESE_EPISODE_ONLY_RE.search(item)
        )
        season_match = re.fullmatch(
            r"\s*(?:S0*(\d{1,3})|Season\s+0*(\d{1,3})|第\s*(\d{1,3})\s*季)\s*",
            item,
            re.I,
        )
        if season_match:
            output.add(
                f"S{int(next(group for group in season_match.groups() if group)):02d}"
            )
        intrinsic_seasons = season_markers(item)
        explicit_seasons = {
            int(match.group(1))
            for token in explicit_episode_ids
            if (match := re.fullmatch(r"S(\d+)E\d+", token))
        }
        effective_seasons = (
            explicit_seasons or intrinsic_seasons or (default_seasons or set())
        )
        if len(effective_seasons) != 1:
            continue
        season = next(iter(effective_seasons))
        # An explicit SxxEyy token already supplies the authoritative season;
        # interpreting every later bare E##/第##集 token with a request-wide
        # default can leak a Season 00 file into another season.
        if not explicit_episode_ids:
            for match in EPISODE_ONLY_RE.finditer(item):
                start = int(match.group(1))
                end = int(match.group(2) or match.group(1))
                if 0 < start <= end and end - start <= 5000:
                    output.update(
                        f"S{season:02d}E{episode:02d}"
                        for episode in range(start, end + 1)
                    )
            for match in CHINESE_EPISODE_ONLY_RE.finditer(item):
                start = _parse_chinese_number(match.group(1))
                end = _parse_chinese_number(match.group(2)) if match.group(2) else start
                if (
                    isinstance(start, int) and isinstance(end, int)
                    and 0 < start <= end and end - start <= 5000
                ):
                    output.update(
                        f"S{season:02d}E{episode:02d}"
                        for episode in range(start, end + 1)
                    )
        # The unmarked anime forms are only a fallback for names without an
        # explicit episode coordinate.  Once one exists, bracket/parenthesis
        # numbers are edition or title evidence, not extra episodes.
        if not explicit_episode_ids and not has_marked_episode:
            for pattern in (ANIME_EPISODE_RANGE_RE, ANIME_BRACKET_RANGE_RE):
                for match in pattern.finditer(item):
                    start, end = int(match.group(1)), int(match.group(2))
                    if 0 < start <= end <= 999 and end - start <= 500:
                        output.update(
                            f"S{season:02d}E{episode:02d}"
                            for episode in range(start, end + 1)
                        )
            for pattern in (ANIME_BRACKET_EPISODE_RE, ANIME_DASH_EPISODE_RE):
                for match in pattern.finditer(item):
                    episode = int(match.group(1))
                    if 0 < episode <= 999:
                        output.add(f"S{season:02d}E{episode:02d}")
    return output


__all__ = [
    "coverage_tokens", "expanded_episode_ids", "normalized_text",
    "season_markers",
]
