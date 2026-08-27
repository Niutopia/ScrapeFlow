"""Pure episode-coverage parsing shared by selection and source adapters.

This module has no Engine, Local, provider, filesystem, or network dependency.
It exists so an Engine-side provider adapter never needs to import a Local API
implementation merely to compare episode evidence.
"""

from __future__ import annotations

import re
from typing import Any
import unicodedata


_INTEGER_EPISODE_END = r"(?!\d|\.\d{1,3}(?:$|[\s._\-\[\](){}]|v\d))"


EPISODE_RE = re.compile(
    r"(?<![A-Z0-9])S0*(\d{1,3})[\s._-]*E(?:P)?\s*0*(\d{1,4})"
    + _INTEGER_EPISODE_END
    # Require an episode marker at the end of a range.  A bare title number
    # after a separator (``S00E01 - 86 - Eighty Six``) is not E86.
    + r"(?:\s*[-~–—]\s*E(?:P)?\s*0*(\d{1,4})"
    + _INTEGER_EPISODE_END + r")?",
    re.I,
)
X_EPISODE_RE = re.compile(
    r"(?<!\d)0*(\d{1,3})\s*[x×]\s*0*(\d{1,4})"
    + _INTEGER_EPISODE_END
    + r"(?:\s*[-~–—]\s*0*(\d{1,4})" + _INTEGER_EPISODE_END + r")?",
    re.I,
)
SEASON_DASH_EPISODE_RE = re.compile(
    r"(?<![A-Z0-9])(?:S|Season\s+)0*(\d{1,3})\s*[-–—]\s*"
    r"(?:E(?:P)?\s*)?0*(\d{1,3})" + _INTEGER_EPISODE_END
    + r"(?:\s*[-~–—]\s*(?:E(?:P)?\s*)?0*(\d{1,3})"
    + _INTEGER_EPISODE_END + r")?(?!P\b)",
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
    r"(?<![A-Z0-9])E(?:P)?0*(\d{1,4})" + _INTEGER_EPISODE_END
    + r"(?:\s*[-~–—]\s*E(?:P)?\s*0*(\d{1,4})"
    + _INTEGER_EPISODE_END + r")?",
    re.I,
)
# A packed name such as ``E01E02`` contains two episode markers but the
# ordinary matcher above deliberately cannot start a second match immediately
# after the final digit of the first one.  Bare-E evidence must never reduce a
# multi-episode pack to its first member, so detect this adjacent form before
# accepting one otherwise-valid ordinal.
_ADJACENT_BARE_EPISODE_MARKERS_RE = re.compile(
    r"(?<![A-Z0-9])E(?:P)?0*\d{1,4}(?:E(?:P)?0*\d{1,4})+",
    re.I,
)
CHINESE_EPISODE_ONLY_RE = re.compile(
    r"第\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话]"
    r"(?:\s*[-~–—至到]\s*第?\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话])?",
)
# A bare ``E01`` is useful evidence only when it is the entire episode
# coordinate, not a fragment of an explicit SxxEyy label or a special-release
# marker.  D/F use this narrow primitive when a one-season TMDB catalog is the
# *only* possible season proof.  Keeping it here means source reconciliation
# and later writer validation share the same episode grammar.
_BARE_REGULAR_EPISODE_SPECIAL_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"sp(?:ecial)?|ova|oav|oad|extra(?:s)?|bonus|"
    r"特别篇|特辑|花絮|映像特典"
    r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)
# A bracketed ordinal is only useful as a single-season fallback when the
# bracket itself contains digits and nothing else.  This intentionally does
# not accept ``[01v2]``, ``[01-02]`` or ``[1080p]`` as an episode coordinate.
# The caller additionally requires exactly one such bracket per primary video
# and a complete TMDB-backed run before it can invent a season.
_PURE_BRACKETED_NUMERIC_RE = re.compile(r"\[\s*(\d{1,6})\s*\]")
# A release-style member often carries no ``E``/season marker at all:
# ``[Group] Title - 01 [WebRip].mkv``.  It is not an ordinary coverage
# coordinate because the leading title can itself contain numbers or dashes.
# D/F may use it only as one member of a complete, catalog-backed run, so the
# primitive below deliberately requires one terminal `` - N`` ordinal and
# preserves the normalized title prefix for an exact sibling comparison.
# A single bounded finale word after the ordinal (``Title - 12 END``) is the
# same release convention as the bracketed ``[12 END]`` form the generic
# episode parser already accepts; it is layout metadata, not a second
# ordinal or a special-release label.
_RELEASE_DASH_EPISODE_RE = re.compile(
    r"^(?P<prefix>.+?)\s[-–—]\s*0*(?P<episode>[1-9]\d{0,2})"
    r"(?:\s+(?P<finale>END|FIN(?:AL)?|完|終|最終話))?"
    r"(?P<tail>(?:\s*(?:\[[^\[\]]+\]|\([^()]+\)))*?)\s*$",
    re.IGNORECASE,
)
_RELEASE_DASH_ORDINAL_RE = re.compile(
    r"\s[-–—]\s*0*(?P<episode>[1-9]\d{0,2})(?!\d)",
)
_RELEASE_DASH_COMPACT_SPECIAL_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"sp(?:ecial)?|ova|oav|oad|extra(?:s)?|bonus|"
    r"ncop|nced|mv|pv|trailer|teaser|promo"
    r")\s*0*\d{0,3}(?![A-Za-z0-9])"
    r"|(?:特别篇|特辑|花絮|映像特典)",
    re.IGNORECASE,
)
_RELEASE_DASH_TAIL_ORDINAL_RE = re.compile(
    r"(?:\[|\()\s*\d+\s*(?:\]|\))",
)
_RELEASE_DASH_TAIL_RANGE_RE = re.compile(
    r"(?:\[|\()\s*\d+\s*[-~–—至到]\s*\d+\s*(?:\]|\))",
)
# These labels are release-side presentation material, not an episode of the
# story.  Keep this tail-only so an ordinary title containing (say) ``ED`` is
# not globally reclassified; ``[ED]``, ``[OP2]`` and ``(MENU)`` after the dash
# ordinal are nevertheless incompatible with the narrow D/F proof.
_RELEASE_DASH_TAIL_NON_STORY_TAG_RE = re.compile(
    r"(?:\[|\()\s*(?:cm|op(?:ed)?|ed|menu)"
    r"(?=\s|[0-9._-]|\]|\))",
    re.IGNORECASE,
)
# A terminal ``E01``/``EP01`` is competing explicit coordinate evidence.  It
# must not be silently ignored merely because the earlier release-style dash
# ordinal happens to have the same number.
_RELEASE_DASH_TAIL_EXPLICIT_EPISODE_RE = re.compile(
    r"(?:\[|\()[^\]\)]*?(?<![A-Za-z0-9])E(?:P)?\s*0*[1-9]\d{0,3}",
    re.IGNORECASE,
)
# A bounded release shape used by a few anime packs is ``Title 01`` with
# optional bracketed release tags after the ordinal.  It is intentionally a
# separate grammar from ``Title - 01``: the title prefix must be digit-free,
# so ordinary planning never has to guess which number is the episode.
_RELEASE_TITLE_ORDINAL_EPISODE_RE = re.compile(
    r"^(?P<prefix>.+?)\s+0*(?P<episode>[1-9]\d{0,2})"
    r"(?P<tail>(?:\s*(?:\[[^\[\]]+\]|【[^【】]+】|\([^()]+\)|（[^（）]+）))*?)\s*$",
    re.IGNORECASE,
)
# A numeric tag may be a year, resolution, ordinal, or range.  None is
# reliable title/release metadata for the narrow ``Title 01`` proof, so keep
# this deliberately unbounded rather than silently accepting ``[2024]``.
_RELEASE_TITLE_ORDINAL_PURE_NUMBER_TAG_RE = re.compile(
    r"^\d+(?:\s*[-~–—至到]\s*\d+)?$",
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
# A decimal episode (for example ``01.5``) is a distinct source coordinate.
# It is deliberately not folded into the integer ``E01`` coverage token.  The
# Engine's planner handles fractional extras separately; acquisition matching
# must therefore fail closed instead of allowing a fractional member to
# satisfy an ordinary episode gap.
FRACTIONAL_EPISODE_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"S0*(?P<season>\d{1,3})[\s._-]*E(?:P)?\s*"
    r"|0*(?P<x_season>\d{1,3})\s*[x×]\s*"
    r"|E(?:P)?\s*"
    r")?0*(?P<whole>\d{1,3})\.(?P<fraction>\d{1,3})"
    r"(?!\d)(?=$|[\s._\-\[\](){}]|v\d)",
    re.I,
)
# Only a clearly delimited season directory may provide the season context for
# a bare ordinal such as ``Season 2/01.mkv``.  This is shared so the audit and
# provider candidate gates cannot invent different defaults.
SEASON_DIRECTORY_RE = re.compile(
    r"^(?:season|s)\s*0*(\d{1,3})$", re.I,
)
CHINESE_DIGITS = {
    "零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3,
    "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
}


def normalized_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return re.sub(r"[^\w\u3400-\u9fff]+", "", text)


def parse_chinese_number(value: str) -> int | None:
    text = value.strip()
    if text.isdecimal():
        return int(text)
    if text in CHINESE_DIGITS:
        return CHINESE_DIGITS[text]
    if "百" in text:
        left, right = text.split("百", 1)
        hundreds = CHINESE_DIGITS.get(left, 1 if not left else -1)
        remainder = parse_chinese_number(right) if right else 0
        return None if hundreds < 0 or remainder is None else hundreds * 100 + remainder
    if "十" in text:
        left, right = text.split("十", 1)
        tens = CHINESE_DIGITS.get(left, 1 if not left else -1)
        ones = CHINESE_DIGITS.get(right, 0 if not right else -1)
        return None if tens < 0 or ones < 0 else tens * 10 + ones
    return None


def episode_ranges(value: Any) -> list[tuple[int, int, int]]:
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
            parse_chinese_number(item) if item else None
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


# Private compatibility alias for older Engine-side callers.  New code should
# use ``episode_ranges``; keeping the alias avoids a needless schema/API break
# while ensuring there is still exactly one implementation.
_episode_ranges = episode_ranges
_parse_chinese_number = parse_chinese_number


def expanded_episode_ids(value: Any) -> set[str]:
    return _range_tokens(value, maximum_span=5000)


def _range_tokens(
    value: Any, *, maximum_span: int,
    include_oversized_start: bool = False,
) -> set[str]:
    output: set[str] = set()
    for season, start, end in episode_ranges(value):
        span = end - start + 1
        if season < 0 or start <= 0 or end < start:
            continue
        if span > maximum_span:
            # The audit needs to retain the explicitly proven first endpoint
            # while refusing to claim an unbounded/batch range.  Provider
            # matching leaves this disabled and rejects the whole range.
            if include_oversized_start:
                output.add(f"S{season:02d}E{start:02d}")
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
        # ``S04-89``/``S04 - 89`` is the common season-local episode
        # shorthand, not a request for every season 04 through 89.  A true
        # season range carries an explicit ``S`` on the second endpoint.  The
        # ambiguous bare-endpoint form therefore contributes only the first
        # season marker; episode_ranges() owns the episode coordinate.
        second_endpoint = value[match.end(1):match.end(2)]
        if not re.search(r"\bS\s*0*\d", second_endpoint, re.I):
            continue
        start, end = int(match.group(1)), int(match.group(2))
        if 0 < start <= end <= 999 and end - start <= 100:
            output.update(range(start, end + 1))
    for match in CHINESE_SEASON_RE.finditer(value):
        number = parse_chinese_number(match.group(1))
        if isinstance(number, int) and number > 0:
            output.add(number)
    return output


def fractional_episode_tokens(
    value: Any, *, default_seasons: set[int] | None = None,
) -> set[str]:
    """Return explicit fractional coordinates without integer coercion.

    Fractional labels are evidence for a separate Engine ``EpisodeKey`` and
    must never satisfy a normal integer episode gap.  When exactly one season
    is proven by the same filename (or by a caller's bounded default), the
    token is rendered as ``SxxEyy.f``.  Ambiguous season context is omitted.
    """
    text = str(value or "")
    matches = list(FRACTIONAL_EPISODE_RE.finditer(text))
    if not matches:
        return set()
    explicit = {
        season
        for season, _start, _end in episode_ranges(text)
        if season >= 0
    }
    explicit.update(
        int(group)
        for match in matches
        for group in (match.group("season"), match.group("x_season"))
        if group is not None
    )
    seasons = explicit or season_markers(text) or (default_seasons or set())
    if len(seasons) != 1:
        return set()
    season = next(iter(seasons))
    output: set[str] = set()
    for match in matches:
        whole = int(match.group("whole"))
        fraction = match.group("fraction").rstrip("0") or "0"
        if whole > 0 and fraction != "0":
            output.add(f"S{season:02d}E{whole:02d}.{fraction}")
    return output


def bare_regular_episode_number(value: Any) -> int | None:
    """Return one unqualified regular ``E##`` ordinal, or ``None``.

    This is deliberately much narrower than :func:`coverage_tokens`: it
    rejects a season marker anywhere in the source path, ranges, fractional
    labels, special/OVA markers, and multiple episode markers.  It therefore
    cannot itself invent a season; callers must pair it with independent
    evidence such as an operator-selected season or a fully verified TMDB
    single-season catalog.
    """
    text = str(value or "")
    if not text or _BARE_REGULAR_EPISODE_SPECIAL_RE.search(text):
        return None
    if _ADJACENT_BARE_EPISODE_MARKERS_RE.search(text):
        return None
    # Explicit SxxEyy / x-style / Chinese-season coordinates and even a bare
    # season marker are stronger, different evidence.  Do not reinterpret
    # their trailing E number as an unqualified ordinal.
    if episode_ranges(text) or season_markers(text):
        return None
    matches = list(EPISODE_ONLY_RE.finditer(text))
    if len(matches) != 1:
        return None
    match = matches[0]
    if match.group(2) is not None:
        return None
    number = int(match.group(1))
    return number if number > 0 else None


def bracketed_regular_episode_number(value: Any) -> int | None:
    """Return one unqualified pure-bracket ordinal, or ``None``.

    ``[01]`` is widespread in anime releases but is much weaker than an
    explicit ``SxxEyy`` coordinate.  It is admitted only as one member of a
    separately proved, complete single-season sequence.  The primitive itself
    rejects special labels, explicit season/episode forms, ranges and every
    basename with zero or multiple pure numeric brackets.  Four-digit values
    such as ``[1080]`` are deliberately not possible episode ordinals here;
    treating a resolution-only bracket as an episode would be unsafe.
    """
    text = str(value or "")
    if not text or _BARE_REGULAR_EPISODE_SPECIAL_RE.search(text):
        return None
    if episode_ranges(text) or season_markers(text):
        return None
    # A filename carrying a bare ``E01`` as well as ``[01]`` has competing
    # grammars.  The D/F proof must not choose one by accident simply because
    # the numeric values happen to agree.
    if bare_regular_episode_number(text) is not None:
        return None
    matches = list(_PURE_BRACKETED_NUMERIC_RE.finditer(text))
    if len(matches) != 1:
        return None
    number = int(matches[0].group(1))
    # A regular TV episode is bounded to the same three-digit range used by
    # the shared anime parser.  This makes a bare ``[1080]`` fail closed even
    # when it is the only numeric bracket in the name.
    return number if 0 < number <= 999 else None


def release_dash_regular_episode(value: Any) -> tuple[str, int] | None:
    """Return ``(title_prefix, ordinal)`` for one strict ``Title - 01`` file.

    The dash ordinal is intentionally not emitted as normal coverage: absent
    an explicit season, it cannot safely identify a TV coordinate by itself.
    The D/F single-season proof uses this narrow primitive only after it has
    shown that *every* video shares one title prefix and forms a complete
    contiguous run against the selected TV's sole positive TMDB season.

    The filename must have one (and only one) dash ordinal, a non-empty
    alphabetic title prefix, and only bracketed/parenthesized release tags
    after the ordinal.  Explicit season/episode forms, fractional values and
    special-release labels stay outside this grammar.
    """
    text = str(value or "")
    if not text or _BARE_REGULAR_EPISODE_SPECIAL_RE.search(text):
        return None
    if _RELEASE_DASH_COMPACT_SPECIAL_RE.search(text):
        return None
    if episode_ranges(text) or season_markers(text):
        return None
    name = re.split(r"[/\\]", text.rstrip("/"))[-1]
    stem, dot, _suffix = name.rpartition(".")
    if not dot:
        stem = name
    stem = stem.strip()
    if not stem:
        return None
    if FRACTIONAL_EPISODE_RE.search(stem):
        return None
    # A title may contain ordinary punctuation, but a second `` - N`` is a
    # competing ordinal (or a packed release) and must not be guessed.
    if len(_RELEASE_DASH_ORDINAL_RE.findall(stem)) != 1:
        return None
    match = _RELEASE_DASH_EPISODE_RE.fullmatch(stem)
    if match is None:
        return None
    tail = match.group("tail") or ""
    # Do not silently choose the dash number when the tail has another pure
    # ordinal such as ``Title - 01 [02]`` or ``(02)``.  Nor may a release
    # presentation tag (CM/OP/ED/MENU) or an explicit trailing E/EP coordinate
    # be smuggled through as harmless metadata.
    if (
        _RELEASE_DASH_TAIL_ORDINAL_RE.search(tail)
        or _RELEASE_DASH_TAIL_RANGE_RE.search(tail)
        or _RELEASE_DASH_TAIL_NON_STORY_TAG_RE.search(tail)
        or _RELEASE_DASH_TAIL_EXPLICIT_EPISODE_RE.search(tail)
    ):
        return None
    prefix = re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", match.group("prefix")).casefold(),
    ).strip()
    if not prefix or not any(character.isalpha() for character in prefix):
        return None
    number = int(match.group("episode"))
    return (prefix, number) if 0 < number <= 999 else None


def release_title_ordinal_regular_episode(value: Any) -> tuple[str, int] | None:
    """Return ``(title_prefix, ordinal)`` for one strict ``Title 01`` file.

    This is a D/F-only primitive.  It accepts one terminal bare ordinal and
    only bracketed/parenthesized release tags after it.  A title prefix may
    not contain another digit, a dash immediately before the ordinal, or a
    special/explicit coordinate marker.  The caller still has to prove one
    homogeneous contiguous run against the selected TMDB season.
    """
    text = str(value or "")
    if not text or _BARE_REGULAR_EPISODE_SPECIAL_RE.search(text):
        return None
    if _RELEASE_DASH_COMPACT_SPECIAL_RE.search(text):
        return None
    if episode_ranges(text) or season_markers(text):
        return None
    name = re.split(r"[/\\]", text.rstrip("/"))[-1]
    stem, dot, _suffix = name.rpartition(".")
    if not dot:
        stem = name
    stem = stem.strip()
    if FRACTIONAL_EPISODE_RE.search(stem):
        return None
    match = _RELEASE_TITLE_ORDINAL_EPISODE_RE.fullmatch(stem)
    if match is None:
        return None
    prefix = match.group("prefix") or ""
    # The dash grammar owns ``Title - 01``.  Keeping this lane disjoint
    # avoids a two-grammar D result for the same source.
    if re.search(r"[-–—]\s*$", prefix):
        return None
    # Remove only leading bracketed release-group tags.  Their contents are
    # not identity evidence; the remaining prefix is the title proof shared
    # by every member of the run.
    leading_groups: list[str] = []
    leading_rest = prefix
    while True:
        leading_match = re.match(
            r"^\s*(?P<tag>\[[^\[\]]+\]|【[^【】]+】|\([^()]+\)|（[^（）]+）)",
            leading_rest,
        )
        if leading_match is None:
            break
        # Keep only the bracketed tag.  ``group(0)`` includes optional leading
        # whitespace, which would otherwise make `` [2024]`` evade the same
        # pure-number rejection as ``[2024]``.
        leading_groups.append(leading_match.group("tag"))
        leading_rest = leading_rest[leading_match.end():]
    for group in leading_groups:
        inner = unicodedata.normalize("NFKC", group[1:-1]).strip()
        if _RELEASE_TITLE_ORDINAL_PURE_NUMBER_TAG_RE.fullmatch(inner):
            return None
        if re.search(
            r"(?<![A-Za-z0-9])E(?:P)?\s*0*[1-9]\d{0,3}|"
            r"(?<![A-Za-z0-9])(?:OVA|OAV|OAD|SP|CM|OP(?:ED)?|ED|MENU)"
            r"(?:\s*0*\d{0,3})?(?![A-Za-z0-9])",
            inner,
            re.IGNORECASE,
        ):
            return None
    prefix = re.sub(
        r"^(?:\s*(?:\[[^\[\]]+\]|【[^【】]+】|\([^()]+\)|（[^（）]+）)\s*)+",
        "",
        prefix,
    ).strip()
    prefix = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", prefix)).casefold()
    if not prefix or not any(character.isalpha() for character in prefix):
        return None
    # A second unqualified number in the title is ambiguous (``Show 2 01``),
    # as is a pure numeric/range tail or an explicit competing episode tag.
    if re.search(r"\d", prefix):
        return None
    tail = match.group("tail") or ""
    for group in re.findall(r"\[[^\[\]]+\]|【[^【】]+】|\([^()]+\)|（[^（）]+）", tail):
        inner = unicodedata.normalize("NFKC", group[1:-1]).strip()
        if _RELEASE_TITLE_ORDINAL_PURE_NUMBER_TAG_RE.fullmatch(inner):
            return None
        if re.search(r"(?<![A-Za-z0-9])E(?:P)?\s*0*[1-9]\d{0,3}", inner, re.IGNORECASE):
            return None
        if re.search(
            r"(?<![A-Za-z0-9])(?:CM|OP(?:ED)?|ED|MENU|OVA|OAV|OAD|MV|PV|TRAILER|TEASER|PROMO)(?:\s*0*\d{0,3})?(?![A-Za-z0-9])",
            inner,
            re.IGNORECASE,
        ):
            return None
    number = int(match.group("episode"))
    return (prefix, number) if 0 < number <= 999 else None


def bare_regular_episode_context_is_safe(value: Any) -> bool:
    """Return whether path context can safely accompany a bare ``E##``.

    The naked-E coordinate itself must be parsed from the media basename: a
    release-container directory such as ``E01-E06`` describes the collection,
    not each member.  Path context is still meaningful negative evidence,
    though.  A parent season/qualified coordinate or special/OVA marker means
    the file belongs to a stronger hierarchy and must not enter the strict
    single-season naked-E proof.

    Bare ranges and adjacent bare markers are intentionally *not* inspected
    here.  Those are basename-only coordinate checks in
    :func:`bare_regular_episode_number`.
    """
    text = str(value or "")
    if not text or _BARE_REGULAR_EPISODE_SPECIAL_RE.search(text):
        return False
    # ``episode_ranges`` covers explicit SxxEyy/x-style/Chinese coordinates
    # in a parent directory, while ``season_markers`` also catches a plain
    # ``Season 01`` hierarchy.  Neither parser treats a bare ``E01-E06``
    # collection directory as a qualified coordinate.
    return not episode_ranges(text) and not season_markers(text)


def coverage_tokens(
    value: Any, *, default_seasons: set[int] | None = None,
    maximum_span: int = 5000,
    include_oversized_start: bool = False,
    allow_bare_numeric: bool = False,
) -> set[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return set()
    output: set[str] = set()
    for item in value:
        # Once a filename already contains an explicit SxxEyy token, a bare
        # numeric dash suffix is title/edition text unless it carries its own
        # episode marker.  Otherwise ``S00E01 - 86 - Eighty Six`` would still
        # gain a false S00E86 through the anime fallback below.
        explicit_episode_ids = _range_tokens(
            item,
            maximum_span=maximum_span,
            include_oversized_start=include_oversized_start,
        )
        output.update(explicit_episode_ids)
        has_marked_episode = bool(
            EPISODE_ONLY_RE.search(item) or CHINESE_EPISODE_ONLY_RE.search(item)
        )
        season_match = re.fullmatch(
            r"\s*(?:S0*(\d{1,3})|Season\s+0*(\d{1,3})|第\s*([一二三四五六七八九十百零〇两\d]{1,5})\s*季)\s*",
            item,
            re.I,
        )
        if season_match:
            raw_season = next(group for group in season_match.groups() if group)
            season_number = parse_chinese_number(raw_season)
            if isinstance(season_number, int) and season_number >= 0:
                output.add(f"S{season_number:02d}")
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
                if 0 < start <= end and end - start + 1 <= maximum_span:
                    output.update(
                        f"S{season:02d}E{episode:02d}"
                        for episode in range(start, end + 1)
                    )
            for match in CHINESE_EPISODE_ONLY_RE.finditer(item):
                start = parse_chinese_number(match.group(1))
                end = parse_chinese_number(match.group(2)) if match.group(2) else start
                if (
                    isinstance(start, int) and isinstance(end, int)
                    and 0 < start <= end and end - start + 1 <= maximum_span
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
                    if 0 < start <= end <= 999 and end - start + 1 <= min(maximum_span, 500):
                        output.update(
                            f"S{season:02d}E{episode:02d}"
                            for episode in range(start, end + 1)
                        )
            for pattern in (ANIME_BRACKET_EPISODE_RE, ANIME_DASH_EPISODE_RE):
                for match in pattern.finditer(item):
                    episode = int(match.group(1))
                    if 0 < episode <= 999:
                        output.add(f"S{season:02d}E{episode:02d}")
        if allow_bare_numeric and not explicit_episode_ids and not has_marked_episode:
            # A bare ordinal is accepted only with an explicit season context:
            # either one season directory in the path or one caller-supplied
            # default.  Reject decimal/fractional labels and quality tags.
            season_from_path = next(
                (
                    int(match.group(1))
                    for part in re.split(r"[/\\]", item)[:-1]
                    if (match := SEASON_DIRECTORY_RE.fullmatch(part.strip()))
                ),
                None,
            )
            bare_season = season_from_path
            if bare_season is None and len(effective_seasons) == 1:
                bare_season = season
            if bare_season is not None:
                stem = re.split(r"[/\\]", item)[-1]
                stem = re.sub(r"\.[^.]+$", "", stem)
                if FRACTIONAL_EPISODE_RE.search(stem):
                    continue
                bare_match = re.search(
                    r"(?:^|[ ._\[(?-])0*(\d{1,4})(?:\]|$)", stem,
                )
                if bare_match:
                    episode = int(bare_match.group(1))
                    if 0 < episode <= 9999:
                        output.add(f"S{bare_season:02d}E{episode:02d}")
    return output


def audit_episode_tokens(
    value: Any, *, default_season: int | None = None,
    maximum_span: int = 24,
) -> set[tuple[int, int]]:
    """Return bounded audit coordinates using the shared coverage parser.

    A malformed oversized range keeps its explicit first endpoint as evidence
    (matching the historical audit behavior), while the rest of the range is
    rejected.  Bare ordinals may use a caller default or a ``Season N`` path,
    but fractional labels remain outside integer coverage.
    """
    defaults = {default_season} if isinstance(default_season, int) and default_season >= 0 else None
    raw = coverage_tokens(
        [str(value or "")],
        default_seasons=defaults,
        maximum_span=maximum_span,
        include_oversized_start=True,
        allow_bare_numeric=True,
    )
    output: set[tuple[int, int]] = set()
    for token in raw:
        match = re.fullmatch(r"S(\d+)E(\d+)", token)
        if match:
            output.add((int(match.group(1)), int(match.group(2))))
    return output


__all__ = [
    "audit_episode_tokens", "bare_regular_episode_context_is_safe",
    "bare_regular_episode_number", "bracketed_regular_episode_number",
    "coverage_tokens", "episode_ranges",
    "expanded_episode_ids", "fractional_episode_tokens", "normalized_text",
    "parse_chinese_number", "release_dash_regular_episode",
    "release_title_ordinal_regular_episode", "season_markers",
]
