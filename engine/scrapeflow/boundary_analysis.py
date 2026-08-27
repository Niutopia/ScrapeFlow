"""Work-boundary analysis for a source directory tree.

Given a SourceNode (built from an AList directory listing) this module infers
how many independent *WorkCandidates* live inside it and what their likely
media shape is.  It is a **pure-function module** — no TMDB calls, no network
I/O, no file reads.

Rules (applied in priority order):
  1. SEASON      — directory name matches a season-folder pattern
  2. SUBTITLE_GROUP — subtree contains only subtitle/nfo/poster files
  3. SERIES_CONTAINER — multiple titled sub-directories each containing videos
  4. MOVIE_COLLECTION — multiple sub-directories each with exactly one large video
  5. SINGLE_WORK — fallback: treat the whole root as one work
  6. UNCERTAIN   — cannot decide confidently

The boundary inference is deliberately conservative: it only promotes a
SERIES_CONTAINER when the evidence is unambiguous.  A marginal case stays
SINGLE_WORK (or UNCERTAIN) so the identity-matching phase can look at the
file names and title evidence before committing.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Sequence

from engine.scrapeflow.source_inventory import (
    SourceNode,
    collect_all_files,
    count_disc_image_files,
    count_executable_files,
    count_video_files,
    direct_video_file_count,
    has_only_subtitles,
)
from engine.scrapeflow.media_policy import DISC_IMAGE_INSPECTION_REQUIRED


# ---------------------------------------------------------------------------
# Shared season-folder regex (same evidence as replenishment_matching.py)
# ---------------------------------------------------------------------------

# Matches: "Season 1", "season01", "S1", "S01", "第1季", "第一季", …
# Deliberately does NOT match "S01E02" (episode files).
_SEASON_DIR_RE = re.compile(
    r"""
    (?:
        (?:season|s)\s*0*(\d{1,3})   # Season 1 / S01 / season01
        | 第\s*(\d{1,3}|[一二三四五六七八九十百零〇两]{1,5})\s*季
                                      # 第1季 / 第01季 / 第一季
    )$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# ASCII ``Season 01``/``S01`` roots have long been treated as structural
# siblings during B/W.  A Chinese cardinal folder (``第一季``) may likewise
# provide exact season evidence *after* C proves identity, but at an intake
# container level it can also be one member beside spin-offs and films.  Keep
# that broader Chinese form available to the later coalescing proof without
# prematurely suppressing it as an independently titled child here.
_ASCII_BARE_SEASON_DIR_RE = re.compile(
    r"(?:season|s)\s*0*\d{1,3}$",
    re.IGNORECASE,
)

# A release folder can carry its season marker together with the show and
# release name, for example ``Northwind.Show.S02.1080p``.  This is deliberately
# stricter than a free substring search: an ASCII title glued to ``S01``
# (``MS01``) and an episode marker (``S01E01``) are not season-directory
# evidence.  A preceding CJK character is allowed because it is a common
# separator-free naming form (``某剧S01``).
_DECORATED_SEASON_RE = re.compile(
    r"""
    (?:
        (?<![A-Za-z0-9])(?:season|s)\s*0*(\d{1,3})
        (?!\s*e\s*\d)(?![A-Za-z0-9])
      | 第\s*(\d{1,3}|[一二三四五六七八九十百零〇两]{1,5})\s*季
        (?!\s*(?:(?:s\s*0*\d{1,3}\s*)?e\s*0*\d{1,4}|第\s*0*\d{1,4}\s*[集话]))
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)
_SEASON_EPISODE_RE = re.compile(
    r"(?:^|[^A-Za-z0-9])S\s*0*(\d{1,3})\s*E\s*0*(\d{1,4})(?:$|[^0-9])",
    re.IGNORECASE,
)
_SEASON_SIGNATURE_NOISE_RE = re.compile(
    r"\b(?:19|20)\d{2}\b|\b(?:2160|1080|720|576|480)p\b|\b(?:4k|8k|"
    r"web-?dl|webrip|blu-?ray|bdrip|remux|x26[45]|h26[45]|hevc|av1|"
    r"10bit|8bit|aac|dts|flac)\b",
    re.IGNORECASE,
)
_YEAR_TOKEN_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")

# A number of real intake trees use a bare structural season marker with
# release packaging attached to it (for example ``第六季（2022）全10集
# 内封字幕 1080P``).  Such a directory is still a season sibling; treating
# the whole decorated label as a creative title makes C query TMDB with
# ``第六季`` and loses the parent/container evidence.  Keep this classifier
# deliberately lexical: it only returns a season number when every remaining
# token is bounded release noise, never when a work title remains.
_GENERIC_SEASON_PACKAGING_RE = re.compile(
    r"(?:19|20)\d{2}|(?:全|共)\s*\d{1,4}\s*(?:集|话|話|期)|"
    r"(?:内封|内嵌|外挂|硬字幕|软字幕|字幕|中字|简中|繁中|简繁|繁简|"
    r"简英|繁英|双语|雙語|蓝光|藍光|原盘|原盤|REMUX|BDRip|WEBRip|"
    r"WEB-?DL|HEVC|AVC|H26[45]|x26[45]|10bit|8bit|AAC|FLAC|DTS|"
    r"4K|8K|2160p|1440p|1080p|720p|576p|480p|合集|全季|全系列|"
    r"收藏版|超清|高清|发布|發布|字幕组|字幕組)",
    re.IGNORECASE,
)
_GENERIC_SEASON_MARKER_RE = re.compile(
    r"(?:^|[^A-Za-z0-9])(?:season|s)\s*0*\d{1,3}(?!\s*e\s*\d)(?![A-Za-z0-9])"
    r"|第\s*(?:\d{1,3}|[一二三四五六七八九十百零〇两]{1,5})\s*季",
    re.IGNORECASE,
)

# Minimum size (bytes) for a file to be considered a "real" movie
_MOVIE_MIN_BYTES = 200 * 1024 * 1024  # 200 MiB

# These are *directory-role* labels, not work-title aliases.  They let B/W
# distinguish a season root's own specials from a nested collection whose
# children are independently titled works.  The matching is exact after
# normalization on purpose: a title which merely contains one of these words
# remains ordinary title evidence and is never split on the name alone.
_TV_AUXILIARY_GROUP_LABELS = frozenset({
    "sp", "special", "specials", "extra", "extras", "bonus", "featurette",
    "featurettes", "behindthescenes", "interviews", "scenes", "trailers",
    "shorts", "deletedscenes", "ova", "oad", "oav", "特典", "附赠", "附贈",
    "幕后", "幕後", "特别篇", "特別篇", "花絮",
})
_FILM_COLLECTION_GROUP_LABELS = frozenset({
    "movie", "movies", "film", "films", "featurefilm", "featurefilms",
    "theatrical", "theatricals", "moviecollection", "filmcollection",
    "电影", "電影", "电影合集", "電影合集", "剧场版", "劇場版",
    "剧场电影", "劇場電影", "剧场版合集", "劇場版合集",
})


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

class DirectoryRole(str, Enum):
    SINGLE_WORK = "single_work"
    SERIES_CONTAINER = "series_container"
    SEASON = "season"
    MOVIE_COLLECTION = "movie_collection"
    SPECIAL_GROUP = "special_group"
    VERSION_GROUP = "version_group"
    SUBTITLE_GROUP = "subtitle_group"
    EXTRAS_GROUP = "extras_group"
    UNCERTAIN = "uncertain"


@dataclass(frozen=True, slots=True)
class BoundaryEvidence:
    role: DirectoryRole
    confidence: float        # 0.0 … 1.0
    reasons: tuple[str, ...]
    competing_roles: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class WorkCandidate:
    """One independent work identified inside a source directory.

    ``work_unit_id`` is deterministic (UUID5 of root_task_id + boundary_key)
    so it survives process restarts without a database.
    """

    work_unit_id: str
    boundary_key: str           # human-stable path segment or label
    source_paths: tuple[str, ...]
    display_label: str
    proposed_media_context: str  # "movie" | "tv" | "mixed" | "unknown"
    boundary_evidence: BoundaryEvidence
    # Positive season numbers explicitly claimed by a verified multi-directory
    # TV cohort.  This remains empty for ordinary candidates; it lets the
    # downstream gap ledger distinguish an actually empty declared season from
    # an unobserved season that must not be invented.
    claimed_seasons: tuple[int, ...] = ()
    # A B/W fact: this boundary contains opaque optical-disc images rather
    # than directly inspectable media files.  It is neither an identity
    # override nor permission to consume the image.
    requires_content_expansion: bool = False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_WC_NAMESPACE = uuid.UUID("b5c6d7e8-f9a0-4002-8003-a00000000002")


def _work_unit_id(root_task_id: str, boundary_key: str) -> str:
    return str(uuid.uuid5(_WC_NAMESPACE, f"{root_task_id}::{boundary_key}"))


def _is_season_dir(name: str) -> bool:
    return bool(_SEASON_DIR_RE.fullmatch(name.strip()))


def _is_ascii_bare_season_dir(name: str) -> bool:
    """Whether a folder is the unambiguous ASCII structural-season form."""
    return bool(_ASCII_BARE_SEASON_DIR_RE.fullmatch(name.strip()))


_CJK_SEASON_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}


def _parse_season_number(value: str) -> int | None:
    """Parse one bounded Arabic or Chinese season ordinal.

    Chinese numerals are accepted only as a complete 1–99 cardinal token (for
    example 一、十一、二十一).  Mixed/unknown characters fail closed rather
    than being interpreted as a title or an approximate number.
    """
    token = str(value or "").strip()
    if token.isdecimal():
        number = int(token)
        return number if number > 0 else None
    if not token or any(
        character not in _CJK_SEASON_DIGITS and character != "十"
        for character in token
    ):
        return None
    if token.count("十") > 1:
        return None
    if "十" in token:
        tens, ones = token.split("十", 1)
        if tens and (
            len(tens) != 1
            or tens not in _CJK_SEASON_DIGITS
            or _CJK_SEASON_DIGITS[tens] <= 0
        ):
            return None
        if ones and (
            len(ones) != 1
            or ones not in _CJK_SEASON_DIGITS
            or _CJK_SEASON_DIGITS[ones] <= 0
        ):
            return None
        total = (_CJK_SEASON_DIGITS[tens] if tens else 1) * 10
        total += _CJK_SEASON_DIGITS[ones] if ones else 0
        return total if total > 0 else None
    if len(token) == 1:
        number = _CJK_SEASON_DIGITS.get(token)
        return number if isinstance(number, int) and number > 0 else None
    return None


def _season_number_from_directory_name(name: str) -> int | None:
    """Return a positive explicit season marker from a directory name.

    Bare season folders use the long-standing full-match pattern.  Decorated
    release folders use the bounded pattern above, but an episode filename or
    an arbitrary ``MS01``-style token never qualifies.
    """
    stripped = name.strip()
    bare = _SEASON_DIR_RE.fullmatch(stripped)
    if bare is not None:
        value = next((part for part in bare.groups() if part is not None), None)
        if value is not None:
            return _parse_season_number(value)
    decorated = _DECORATED_SEASON_RE.search(stripped)
    if decorated is None:
        return None
    value = next((part for part in decorated.groups() if part is not None), None)
    if value is None:
        return None
    return _parse_season_number(value)


def _season_signature(name: str) -> str | None:
    """Derive a bounded common-series signature from a decorated folder name."""
    without_season = _DECORATED_SEASON_RE.sub(" ", name)
    without_season = _SEASON_SIGNATURE_NOISE_RE.sub(" ", without_season)
    normalized = re.sub(r"[^\w\u3400-\u9fff]+", " ", without_season.casefold())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def _generic_season_child(name: str) -> int | None:
    """Return the season number for a release-decorated generic season name.

    This is intentionally stricter than ``_season_number_from_directory_name``.
    A name such as ``Show S01 1080p`` retains ``Show`` and is a titled release
    branch; only a name whose non-season text is all bounded packaging noise is
    classified as a structural sibling.
    """
    season = _season_number_from_directory_name(name)
    if season is None:
        return None
    text = unicodedata.normalize("NFKC", str(name or "")).strip()

    # Brackets often contain the only title evidence (``[Attack on Titan]")
    # rather than release packaging.  Remove them only when their *entire*
    # contents are from the bounded packaging vocabulary.  Unknown, empty,
    # nested, or unbalanced brackets make this narrow classifier fail closed.
    def strip_explicit_bracketed_packaging(match: re.Match[str]) -> str:
        label = match.group(1) if match.group(1) is not None else match.group(2)
        if not label.strip() or any(token in label for token in "[]()"):
            return match.group(0)
        stripped = _GENERIC_SEASON_PACKAGING_RE.sub(" ", label)
        residual = re.sub(r"[\W_]+", "", stripped, flags=re.UNICODE)
        return " " if label.strip() and not residual else match.group(0)

    text = re.sub(r"\[([^\]]*)\]|\(([^)]*)\)", strip_explicit_bracketed_packaging, text)
    if any(token in text for token in "[]()"):
        return None
    text = _GENERIC_SEASON_MARKER_RE.sub(" ", text)
    text = _GENERIC_SEASON_PACKAGING_RE.sub(" ", text)
    residual = re.sub(r"[\W_]+", "", text, flags=re.UNICODE)
    return season if not residual else None


def _generic_season_cohort(
    node: SourceNode,
) -> tuple[tuple[SourceNode, ...], tuple[int, ...]]:
    """Find a conservative cohort of decorated structural season children.

    Every member must have a unique season number and an independently
    corroborating episode run.  The run may be explicit ``SxxExx`` or a
    complete exact ``01..N`` numeric release (the latter never becomes an
    identity query; it is only B/W ownership evidence).  This lets a root
    such as ``Show/第一季（...）/第二季（...）`` become one WorkUnit while a
    titled spin-off child remains a separate candidate.
    """
    members: list[tuple[SourceNode, int]] = []
    for child in node.children:
        season = _generic_season_child(child.name)
        if season is not None:
            members.append((child, season))
    if len(members) < 2 or len({season for _child, season in members}) != len(members):
        return (), ()
    verified: list[tuple[SourceNode, int]] = []
    for child, season in members:
        if _directory_matches_declared_season(child, season):
            verified.append((child, season))
    if len(verified) < 2 or len({season for _child, season in verified}) != len(verified):
        return (), ()
    verified.sort(key=lambda item: (item[1], item[0].path))
    return tuple(child for child, _season in verified), tuple(
        season for _child, season in verified
    )


def _directory_matches_declared_season(node: SourceNode, season: int) -> bool:
    """Require subtree episode evidence to agree with the directory marker."""
    observed: set[int] = set()
    videos = []
    for file in collect_all_files(node):
        if file.object_type != "video":
            continue
        videos.append(file)
        match = _SEASON_EPISODE_RE.search(file.name)
        if match is not None:
            observed.add(int(match.group(1)))
    if observed:
        return observed == {season}
    # Exact bare numeric releases (01.mkv…N.mkv) are valid structural
    # corroboration for a directory season, but never identity evidence.  A
    # complete contiguous run with no duplicate stems is required.
    if len(videos) < 4:
        return False
    numbers: list[int] = []
    for file in videos:
        stem = str(file.name).rsplit("/", 1)[-1]
        # Release names often append codec/group tags to a numeric ordinal
        # (``01.BluRay...mkv``).  Accept the ordinal only at the beginning
        # and require a non-digit separator; decimal/quality tokens do not
        # become episode evidence through this fallback.
        match = re.match(r"^0*([1-9]\d{0,2})(?:[._ -]|$)", stem.strip())
        if match is None:
            return False
        numbers.append(int(match.group(1)))
    ordered = sorted(set(numbers))
    return (
        len(ordered) >= 4
        and len(numbers) == len(ordered)
        and ordered == list(range(1, len(ordered) + 1))
    )


def _decorated_season_cohorts(node: SourceNode) -> list[tuple[str, tuple[SourceNode, ...], tuple[int, ...]]]:
    """Find safe same-series season cohorts among direct child directories.

    A cohort requires two or more independently verified positive seasons with
    the same normalized series signature.  A duplicate edition for one season
    invalidates that signature rather than guessing which edition to consume.
    Once a cohort exists, an adjacent same-signature empty next season may join
    it; this preserves a real declared missing season for the gap ledger
    without absorbing an arbitrary empty sibling.
    """
    grouped: dict[str, list[tuple[SourceNode, int]]] = {}
    for child in node.children:
        season = _season_number_from_directory_name(child.name)
        if season is None:
            continue
        signature = _season_signature(child.name)
        if signature is None:
            continue
        grouped.setdefault(signature, []).append((child, season))

    cohorts: list[tuple[str, tuple[SourceNode, ...], tuple[int, ...]]] = []
    for signature, members in grouped.items():
        by_season: dict[int, list[SourceNode]] = {}
        for child, season in members:
            by_season.setdefault(season, []).append(child)
        # Two editions of the same declared season are ambiguous source
        # ownership.  Do not aggregate a subset and leave the other behind.
        if any(len(children) != 1 for children in by_season.values()):
            continue

        verified = {
            season
            for season, (child,) in by_season.items()
            if _directory_matches_declared_season(child, season)
        }
        if len(verified) < 2:
            continue
        # A non-empty sibling that carries the same claimed season but whose
        # files disagree is contradictory, not an invitation to guess.
        if any(
            count_video_files(child) > 0 and season not in verified
            for season, (child,) in by_season.items()
        ):
            continue
        highest_verified = max(verified)
        included: list[tuple[SourceNode, int]] = []
        for season, (child,) in by_season.items():
            if season in verified:
                included.append((child, season))
            # An empty directory has no episode evidence.  Only the *next*
            # same-signature season after the highest verified one can be
            # carried as an explicit missing-season boundary; accepting an
            # earlier/gapped empty folder would invent episode gaps for an
            # arbitrary stale release shell.
            elif count_video_files(child) == 0 and season == highest_verified + 1:
                included.append((child, season))
        included.sort(key=lambda item: (item[1], item[0].path))
        if len({season for _child, season in included}) < 2:
            continue
        cohorts.append((
            signature,
            tuple(child for child, _season in included),
            tuple(season for _child, season in included),
        ))
    return cohorts


def _cohort_display_label(first: SourceNode) -> str:
    """Keep the operator's title evidence while removing only season noise."""
    value = _DECORATED_SEASON_RE.sub(" ", first.name)
    value = _SEASON_SIGNATURE_NOISE_RE.sub(" ", value)
    value = re.sub(r"\s+", " ", value).strip(" ._-[]()")
    return value or first.name


def _child_has_video(node: SourceNode) -> bool:
    return count_video_files(node) > 0


def _child_has_only_subtitle(node: SourceNode) -> bool:
    return has_only_subtitles(node)


def _is_titled_child(node: SourceNode) -> bool:
    """True if a child directory looks like an independent titled work.

    A titled child has videos somewhere in its subtree and its name does NOT
    match a generic season marker, a bonus/extras label, or a subtitle-only
    group.
    """
    if not _child_has_video(node):
        return False
    # A decorated release folder (``Show.S01.2160p``) becomes a season only
    # through the corroborated cohort rule below.  Before that proof exists it
    # remains a titled child, so duplicate editions cannot be silently folded
    # into one broad root claim.
    if _is_ascii_bare_season_dir(node.name):
        return False
    if _child_has_only_subtitle(node):
        return False
    # Exclude generic extra/bonus labels
    lower = node.name.strip().lower()
    if lower in {"extras", "extra", "bonus", "sp", "special", "specials",
                 "featurettes", "behind the scenes", "interviews", "scenes",
                 "trailers", "shorts", "deleted scenes", "附赠", "特典", "幕后"}:
        return False
    return True


def _has_explicit_year_evidence(node: SourceNode) -> bool:
    """Whether a residual sibling carries a bounded release/year marker.

    A generic season cohort plus an arbitrary titled branch is not enough to
    split a root: an ``Aftershow``/``Extras``-like branch may be part of the
    same unresolved container.  A direct four-digit year in the branch name
    or one of its files is a small, generic corroboration that the sibling is
    a separately released work.  This is boundary evidence only, never an
    identity match or target-path decision.
    """
    return any(
        _YEAR_TOKEN_RE.search(str(item.name or ""))
        for item in collect_all_files(node)
    ) or bool(_YEAR_TOKEN_RE.search(str(node.name or "")))


def _normalized_role_label(value: str) -> str:
    """Normalize a generic directory-role label without interpreting titles.

    The result is used only against the bounded role vocabularies above.  It
    deliberately does not strip years, release tags, or arbitrary title words;
    doing so would make a title-shaped folder look like a generic collection.
    """
    normalized = unicodedata.normalize("NFKC", value).casefold().strip()
    return re.sub(r"[\s._\-\[\](){}]+", "", normalized)


def _is_tv_auxiliary_group(node: SourceNode) -> bool:
    """Whether a direct child is a generic supplement to a season-root TV.

    ``SP``/``Extras`` may contain bonus media, but are not independently
    titled works by themselves.  They can therefore remain in the TV unit
    only when the parent root has already proved a multi-season structure.
    """
    return _normalized_role_label(node.name) in _TV_AUXILIARY_GROUP_LABELS


def _film_collection_children(node: SourceNode) -> tuple[SourceNode, ...]:
    """Return independently titled children of one generic film collection.

    A collection group is safe to split only if it has no direct video files
    and every video-bearing immediate child is itself a titled boundary.  This
    prevents a season root from silently dropping an aftershow, a loose video,
    or an opaque mixed branch while extracting a sibling movie collection.
    """
    if _normalized_role_label(node.name) not in _FILM_COLLECTION_GROUP_LABELS:
        return ()
    if direct_video_file_count(node) != 0:
        return ()
    video_children = tuple(child for child in node.children if _child_has_video(child))
    if not video_children or not all(_is_titled_child(child) for child in video_children):
        return ()
    return video_children


def _single_large_video(node: SourceNode) -> bool:
    """True if the node contains exactly one large video file (movie-shaped)."""
    all_files = collect_all_files(node)
    video_files = [f for f in all_files if f.object_type == "video"]
    if len(video_files) == 1 and video_files[0].size >= _MOVIE_MIN_BYTES:
        return True
    return False


# A flat intake root is sometimes an identity-free package containing several
# feature files (rather than one directory per feature).  Keep the lexical
# cleanup here deliberately structural: it removes release/part/episode
# notation only to decide whether two files are the same *physical title*;
# it never supplies a TMDB identity or a target path.
_DIRECT_EPISODE_MARKER_RE = re.compile(
    r"(?:^|[^A-Za-z0-9])(?:S\s*\d{1,3}\s*E\s*\d{1,4}|E(?:P)?\s*\d{1,4})"
    r"(?:$|[^0-9])",
    re.IGNORECASE,
)
_DIRECT_ORDINAL_BRACKET_RE = re.compile(
    r"(?:\[|\(|【)\s*0*\d{1,3}(?:\s*v\d+)?\s*(?:\]|\)|】)",
    re.IGNORECASE,
)
_DIRECT_PART_TOKEN_RE = re.compile(
    r"(?:^|[\s._-])(?:part|pt|disc|disk|cd|vol(?:ume)?|segment|片|碟|篇)"
    r"\s*0*\d{1,3}(?=$|[\s._-])",
    re.IGNORECASE,
)
_DIRECT_RELEASE_NOISE_RE = re.compile(
    r"(?:19|20)\d{2}|(?:2160|1440|1080|720|576|480)p|4k|8k|"
    r"(?:bdrip|blu[- ]?ray|web[- ]?dl|webrip|remux|x26[45]|h26[45]|hevc|av1|"
    r"10bit|8bit|aac|eac3|ac3|dts|flac|ma10p|hi_?10p)",
    re.IGNORECASE,
)
_DIRECT_TRAILING_ORDINAL_RE = re.compile(
    r"(?:^|[\s._-])0*[1-9]\d{0,2}$",
    re.IGNORECASE,
)


def _direct_movie_title_key(file_name: str) -> str | None:
    """Return a conservative title key for one flat video filename.

    The key is used only by the boundary splitter.  Explicit episode-shaped
    names, bare ordinals, and release-part markers fail closed so a normal TV
    episode pack or a multi-disc encode can never be mistaken for a movie
    collection.  Bracketed release tags are discarded; meaningful CJK/Latin
    title text remains for C/U to query through the ordinary matcher.
    """
    stem = unicodedata.normalize("NFKC", Path(str(file_name)).stem).strip()
    if not stem or _DIRECT_EPISODE_MARKER_RE.search(stem):
        return None
    # A bare numeric release (01.mkv) is episode/part evidence, never a title.
    if re.fullmatch(r"0*[1-9]\d{0,2}", stem):
        return None

    def bracket_replacement(match: re.Match[str]) -> str:
        content = match.group(1) or match.group(2) or match.group(3) or ""
        # Keep a bracketed creative title (for example ``[君の名は]``), but
        # discard the bounded release vocabulary and short ASCII group tags.
        if (
            not re.search(r"[\u3400-\u9fff\u3040-\u30ff]", content)
            and not re.search(r"[A-Za-z]{2,}", content)
        ):
            return " "
        if _DIRECT_RELEASE_NOISE_RE.search(content) or re.fullmatch(
            r"[A-Za-z0-9 _+.-]{1,16}", content.strip()
        ):
            return " "
        return f" {content} "

    stem = re.sub(
        r"\[([^\]]*)\]|\(([^)]*)\)|【([^】]*)】",
        bracket_replacement,
        stem,
    )
    stem = _DIRECT_PART_TOKEN_RE.sub(" ", stem)
    stem = _DIRECT_RELEASE_NOISE_RE.sub(" ", stem)
    # Common release names append a single ordinal after the title.  Strip it
    # only at the end and only after the stronger part/episode guards above.
    stem = _DIRECT_TRAILING_ORDINAL_RE.sub(" ", stem)
    stem = re.sub(r"\s+", " ", stem).strip(" ._+-")
    key = "".join(
        char.casefold()
        for char in unicodedata.normalize("NFKC", stem)
        if char.isalnum()
    )
    if not key or key.isdigit() or len(key) < 4:
        return None
    return key


def _direct_movie_title_is_substantial(file_name: str) -> bool:
    """Require meaningful title text before splitting a flat file set."""
    stem = unicodedata.normalize("NFKC", Path(str(file_name)).stem)
    cjk = re.findall(r"[\u3400-\u9fff\u3040-\u30ff]", stem)
    latin_words = re.findall(r"[A-Za-z]{2,}", stem)
    return len(cjk) >= 3 or len(latin_words) >= 2


def _split_flat_movie_files(
    node: SourceNode,
    *,
    root_task_id: str,
    root_videos: int,
) -> list[WorkCandidate] | None:
    """Split several independently titled direct feature files.

    This is a pure B/W rule for the identity-free flat-package shape.  It is
    intentionally narrower than a generic ``root_videos > 1`` rule: every
    file must be a substantial, large, non-episodic title, all normalized
    title keys must be unique, and the root may not also contain a
    video-bearing child directory.  If any proof is missing the caller keeps
    the historical whole-root boundary and C/U can park it safely.
    """
    if root_videos < 2:
        return None
    direct_videos = [
        file for file in node.files if file.object_type == "video"
    ]
    if len(direct_videos) < 2 or any(
        file.size < _MOVIE_MIN_BYTES
        or not _direct_movie_title_is_substantial(file.name)
        for file in direct_videos
    ):
        return None
    # A root that also has a video-bearing child is normally a TV/container
    # layout.  Leave it intact for the season/container rules below.
    if any(_child_has_video(child) for child in node.children):
        return None
    keys = [_direct_movie_title_key(file.name) for file in direct_videos]
    if any(key is None for key in keys):
        return None
    concrete_keys = [key for key in keys if key is not None]
    if len(set(concrete_keys)) != len(concrete_keys):
        return None

    candidates: list[WorkCandidate] = []
    reason = (
        f"根目录直接含 {len(direct_videos)} 个独立标题的大视频文件；"
        "文件名无季集/分片坐标且标题键互不重复",
    )
    for file in direct_videos:
        boundary_key = file.path
        candidates.append(WorkCandidate(
            work_unit_id=_work_unit_id(root_task_id, boundary_key),
            boundary_key=boundary_key,
            source_paths=(boundary_key,),
            display_label=Path(file.name).stem,
            proposed_media_context="movie",
            boundary_evidence=BoundaryEvidence(
                role=DirectoryRole.MOVIE_COLLECTION,
                confidence=0.86,
                reasons=reason,
                competing_roles=(DirectoryRole.SINGLE_WORK.value,),
            ),
        ))
    return candidates


def _propose_media_context(node: SourceNode) -> str:
    """Heuristic: is this more movie-shaped or TV-shaped?"""
    all_files = collect_all_files(node)
    video_files = [f for f in all_files if f.object_type == "video"]
    if not video_files:
        return "unknown"
    # One large video → movie
    if len(video_files) == 1 and video_files[0].size >= _MOVIE_MIN_BYTES:
        return "movie"
    # The node's own name is a season marker (``不死者之王 第一季`` is a
    # season-scoped candidate, not a movie-shaped standalone): a season
    # directory is TV evidence regardless of its child naming.
    if _season_number_from_directory_name(node.name) is not None:
        return "tv"
    # Multiple videos, any season dir in children → tv
    if any(_season_number_from_directory_name(c.name) is not None for c in node.children):
        return "tv"
    # More than ~4 videos → tv (episode pack)
    if len(video_files) >= 4:
        return "tv"
    # 2-3 videos with no season dir → could be short movie trilogy or OVA
    return "unknown"


def _explicit_file_claimed_seasons(node: SourceNode) -> tuple[int, ...]:
    """Return positive seasons proved by every owned video coordinate.

    A titled sibling beside a multi-season cohort is an independent WorkUnit,
    so it does not pass through ``_single_work_claimed_seasons``.  When every
    video in that exact sibling scope nevertheless carries an explicit
    ``SxxExx`` coordinate, those positive season numbers are still ordinary
    B/W ownership facts.  Persisting them lets the generic container rule
    distinguish one multi-season main identity from a one-season sibling
    without consulting a title, TMDB id, or answerbook row.

    Any unqualified video makes the proof fail closed.  Season 00 remains
    auxiliary coverage and never nominates a main TV identity by itself.
    """
    seasons: set[int] = set()
    videos = [
        file for file in collect_all_files(node)
        if file.object_type == "video"
    ]
    if not videos:
        return ()
    for file in videos:
        match = _SEASON_EPISODE_RE.search(file.name)
        if match is None:
            return ()
        season = int(match.group(1))
        if season > 0:
            seasons.add(season)
    return tuple(sorted(seasons))


def _single_work_claimed_seasons(
    node: SourceNode,
    season_children: Sequence[SourceNode],
) -> tuple[int, ...]:
    """Return only B/W-proven seasons for a rooted single TV work.

    This field is reserved for a root that *proves* a multi-season layout:
    at least two direct season directories must independently corroborate
    their ``SxxExx`` coordinates.  Ordinary nested episode files are not a
    declaration—J can derive their actual coverage from the executed plan.
    Once the layout is proven, direct root ``SxxExx`` files and a direct
    subtitle-only season may join only with exact coordinate evidence.  This
    preserves a real missing season for J without turning an arbitrary stale
    ``S99`` folder or a loose subtitle bucket into an invented gap.
    """
    direct_observed: set[int] = set()
    for file in node.files:
        if file.object_type != "video":
            continue
        match = _SEASON_EPISODE_RE.search(file.name)
        if match is not None:
            season = int(match.group(1))
            if season > 0:
                direct_observed.add(season)

    by_season: dict[int, list[SourceNode]] = {}
    for child in season_children:
        season = _season_number_from_directory_name(child.name)
        if season is not None:
            by_season.setdefault(season, []).append(child)
    verified = {
        season
        for season, children in by_season.items()
        if len(children) == 1
        and _directory_matches_declared_season(children[0], season)
    }
    if len(verified) < 2:
        return ()

    claimed = set(verified) | direct_observed
    if not claimed:
        return ()

    lower = min(claimed)
    upper = max(claimed)
    for season, children in by_season.items():
        if len(children) != 1 or season in verified:
            continue
        child_files = collect_all_files(children[0])
        subtitle_coordinates = {
            int(match.group(2))
            for file in child_files
            if file.object_type == "subtitle"
            if (match := _SEASON_EPISODE_RE.search(file.name)) is not None
            and int(match.group(1)) == season
        }
        if not subtitle_coordinates:
            continue
        if subtitle_coordinates != set(range(1, max(subtitle_coordinates) + 1)):
            continue
        if lower <= season <= upper or season == upper + 1:
            claimed.add(season)
    return tuple(sorted(claimed))


def _split_mixed_season_root(
    node: SourceNode,
    *,
    root_task_id: str,
    season_children: Sequence[SourceNode],
    root_videos: int,
) -> list[WorkCandidate] | None:
    """Split a proven season root from sibling, independently titled films.

    A common release layout has a main TV work in ``S01``/``S02``/… plus a
    generic ``Movies``/``剧场版`` branch holding separately titled films.  The
    former must remain one WorkUnit while every nested film is resolved in C
    on its own title evidence.  This function is intentionally all-or-nothing:
    any unclassified video-bearing sibling returns ``None`` so the caller
    retains the old whole-root, fail-closed boundary instead of losing source
    ownership.
    """
    if root_videos != 0 or len(season_children) < 2:
        return None
    if sum(1 for child in season_children if _child_has_video(child)) < 2:
        return None

    tv_children: list[SourceNode] = list(season_children)
    handled_video_branches = {child.path for child in season_children}
    film_candidates: list[tuple[SourceNode, DirectoryRole, tuple[str, ...]]] = []

    for child in node.children:
        if child.path in handled_video_branches:
            continue
        if _is_tv_auxiliary_group(child):
            # Generic SP/Extras folders are part of the demonstrated TV
            # layout.  Do not add a wholly empty shell as a planner scope.
            if collect_all_files(child):
                tv_children.append(child)
            if _child_has_video(child):
                handled_video_branches.add(child.path)
            continue

        nested_films = _film_collection_children(child)
        if nested_films:
            handled_video_branches.add(child.path)
            reason = (
                f"季目录旁的通用电影合集目录 '{child.name}' 含 "
                f"{len(nested_films)} 个独立标题子目录",
            )
            film_candidates.extend(
                (film, DirectoryRole.MOVIE_COLLECTION, reason)
                for film in nested_films
            )
            continue

        # A direct, independently titled one-video movie beside a proven
        # season root is equally safe to split.  Two-or-more-video branches
        # need the generic collection proof above; otherwise they remain a
        # conservative whole-root boundary.
        if _is_titled_child(child) and _single_large_video(child):
            handled_video_branches.add(child.path)
            film_candidates.append((
                child,
                DirectoryRole.MOVIE_COLLECTION,
                ("季目录旁存在单个大视频的独立标题子目录",),
            ))

    if not film_candidates:
        return None

    unhandled_video_branches = [
        child
        for child in node.children
        if _child_has_video(child) and child.path not in handled_video_branches
    ]
    if unhandled_video_branches:
        return None

    # The multi-scope TV unit owns only season and generic-SP scopes.  Its
    # claims come exclusively from the explicit season directories; D
    # revalidates those exact scopes without treating an auxiliary SP folder
    # as a season assertion.
    ordered_tv_children = sorted(
        tv_children,
        key=lambda child: (
            _season_number_from_directory_name(child.name) is None,
            _season_number_from_directory_name(child.name) or 0,
            child.path,
        ),
    )
    tv_reasons = (
        f"发现 {len(season_children)} 个季目录，且与 "
        f"{len(film_candidates)} 个独立电影/特别篇边界不重叠",
    )
    candidates: list[WorkCandidate] = [WorkCandidate(
        work_unit_id=_work_unit_id(root_task_id, f"{node.path}/@season-root"),
        boundary_key=f"{node.path}/@season-root",
        source_paths=tuple(child.path for child in ordered_tv_children),
        display_label=node.name,
        proposed_media_context="tv",
        boundary_evidence=BoundaryEvidence(
            role=DirectoryRole.SINGLE_WORK,
            confidence=0.92,
            reasons=tv_reasons,
            competing_roles=(DirectoryRole.SERIES_CONTAINER.value,),
        ),
        claimed_seasons=_single_work_claimed_seasons(node, season_children),
    )]
    for film, role, reasons in film_candidates:
        candidates.append(WorkCandidate(
            work_unit_id=_work_unit_id(root_task_id, film.path),
            boundary_key=film.path,
            source_paths=(film.path,),
            display_label=film.name,
            proposed_media_context=_propose_media_context(film),
            boundary_evidence=BoundaryEvidence(
                role=role,
                confidence=0.90,
                reasons=reasons,
                competing_roles=(DirectoryRole.SINGLE_WORK.value,),
            ),
        ))
    return candidates


# ---------------------------------------------------------------------------
# Main analyser
# ---------------------------------------------------------------------------

def analyze_boundaries(
    node: SourceNode,
    *,
    root_task_id: str = "unknown",
) -> list[WorkCandidate]:
    """Analyse ``node`` and return a list of independent WorkCandidates.

    The typical outcome is one candidate (``SINGLE_WORK``) for ordinary
    directories and multiple candidates when the root is a ``SERIES_CONTAINER``
    or ``MOVIE_COLLECTION``.

    ``root_task_id`` is only used to generate stable ``work_unit_id`` values.
    """
    # An optical-disc image may hold one title, a season, several films,
    # menus/extras, or unrelated payload.  Directory and filename heuristics
    # cannot prove that boundary.  Keep the whole supplied source scope as
    # one visible uncertain unit instead of silently dropping image files or
    # guessing an overlapping sibling split.  A future read-only image
    # expander must create a concrete inventory and re-run B/W.
    disc_image_count = count_disc_image_files(node)
    if disc_image_count:
        evidence = BoundaryEvidence(
            role=DirectoryRole.UNCERTAIN,
            confidence=1.0,
            reasons=(
                f"发现 {disc_image_count} 个光盘镜像容器；{DISC_IMAGE_INSPECTION_REQUIRED}",
            ),
            competing_roles=(),
        )
        return [WorkCandidate(
            work_unit_id=_work_unit_id(root_task_id, node.path),
            boundary_key=node.path,
            source_paths=(node.path,),
            display_label=node.name,
            proposed_media_context="unknown",
            boundary_evidence=evidence,
            requires_content_expansion=True,
        )]
    executable_count = count_executable_files(node)
    if executable_count:
        evidence = BoundaryEvidence(
            role=DirectoryRole.UNCERTAIN,
            confidence=1.0,
            reasons=(
                f"发现 {executable_count} 个伪装视频 .exe，真实媒体类型尚未只读识别",
            ),
            competing_roles=(),
        )
        return [WorkCandidate(
            work_unit_id=_work_unit_id(root_task_id, node.path),
            boundary_key=node.path,
            source_paths=(node.path,),
            display_label=node.name,
            proposed_media_context="unknown",
            boundary_evidence=evidence,
            requires_content_expansion=True,
        )]

    # --- Rule 1: Season directory ---------------------------------------
    if _season_number_from_directory_name(node.name) is not None:
        evidence = BoundaryEvidence(
            role=DirectoryRole.SEASON,
            confidence=0.95,
            reasons=(f"目录名 '{node.name}' 匹配季目录模式",),
            competing_roles=(),
        )
        return [WorkCandidate(
            work_unit_id=_work_unit_id(root_task_id, node.path),
            boundary_key=node.path,
            source_paths=(node.path,),
            display_label=node.name,
            proposed_media_context="tv",
            boundary_evidence=evidence,
        )]

    # --- Rule 2: Subtitle-only group ------------------------------------
    if has_only_subtitles(node):
        all_files = collect_all_files(node)
        if all_files:
            evidence = BoundaryEvidence(
                role=DirectoryRole.SUBTITLE_GROUP,
                confidence=0.90,
                reasons=("目录下只有字幕/NFO/海报文件",),
                competing_roles=(),
            )
            return [WorkCandidate(
                work_unit_id=_work_unit_id(root_task_id, node.path),
                boundary_key=node.path,
                source_paths=(node.path,),
                display_label=node.name,
                proposed_media_context="unknown",
                boundary_evidence=evidence,
            )]

    titled_children = [c for c in node.children if _is_titled_child(c)]
    bare_season_children = [
        child for child in node.children if _is_ascii_bare_season_dir(child.name)
    ]
    season_children = [
        child
        for child in node.children
        if _season_number_from_directory_name(child.name) is not None
    ]
    root_videos = direct_video_file_count(node)

    # Identity-free flat movie packages (two or more independently titled
    # feature files directly under the intake root) need one exact WorkUnit
    # per file before C/U.  Keep this ahead of the broad single-work fallback;
    # the helper is deliberately conservative and returns ``None`` for TV
    # episode/part shapes or any mixed video-bearing subtree.
    flat_movie_split = _split_flat_movie_files(
        node,
        root_task_id=root_task_id,
        root_videos=root_videos,
    )
    if flat_movie_split is not None:
        return flat_movie_split

    # --- Rule 1a: season root plus independent film/special collection ---
    # This must happen before decorated-season cohorts and the broad
    # single-work fallback.  It never relies on a title lookup: only explicit
    # season folders, exact generic collection-role labels, and disjoint file
    # tree ownership can produce the split.
    mixed_season_split = _split_mixed_season_root(
        node,
        root_task_id=root_task_id,
        season_children=season_children,
        root_videos=root_videos,
    )
    if mixed_season_split is not None:
        return mixed_season_split

    # --- Rule 1b: decorated *generic* season siblings ------------------
    # Uploaders often add year/count/codec text to an otherwise structural
    # ``第一季``/``Season 02`` directory.  Group only the members whose exact
    # episode runs corroborate their season numbers; independently titled
    # children (for example a spin-off in the same user container) remain
    # separate WorkUnits and are never absorbed by this rule.
    generic_children, generic_seasons = _generic_season_cohort(node)
    generic_cohort_unresolved = False
    if generic_children and root_videos == 0:
        grouped_paths = {child.path for child in generic_children}
        auxiliary_children = tuple(
            child
            for child in node.children
            if child.path not in grouped_paths
            and _is_tv_auxiliary_group(child)
            and collect_all_files(child)
        )
        residual_video_children = [
            child
            for child in node.children
            if _child_has_video(child)
            and child.path not in grouped_paths
            and child.path not in {item.path for item in auxiliary_children}
        ]
        # A remaining season-marked branch which failed the exact proof is a
        # contradiction, not a reason to silently drop it.  Other titled
        # branches are valid sibling works and are retained below.
        contradictory_seasons = [
            child
            for child in residual_video_children
            if _season_number_from_directory_name(child.name) is not None
        ]
        # Do not split a generic season cohort merely because an arbitrary
        # titled branch exists (``Aftershow`` is a common same-container
        # example).  A residual branch needs an additional release/year
        # corroboration before it becomes a sibling WorkUnit; otherwise the
        # conservative whole-root rule below retains ownership together.
        residual_is_provably_titled = all(
            _is_titled_child(child) and _has_explicit_year_evidence(child)
            for child in residual_video_children
        )
        if not contradictory_seasons and residual_is_provably_titled:
            ordered_main = tuple(sorted(
                (*generic_children, *auxiliary_children),
                key=lambda child: (
                    _season_number_from_directory_name(child.name) is None,
                    _season_number_from_directory_name(child.name) or 0,
                    child.path,
                ),
            ))
            reasons = (
                f"发现 {len(generic_seasons)} 个带发布噪声但经集号验证的结构季目录",
            )
            # A pure multi-season work owns the real intake boundary.  Use a
            # synthetic key only when independently proved sibling works also
            # live below that intake; in that case the key prevents the main
            # unit from claiming the siblings' source objects.  Applying the
            # synthetic key unconditionally leaked an internal implementation
            # marker into ordinary single-work boundaries and obscured their
            # whole-root ownership.
            main_boundary = (
                f"{node.path}/@generic-season-root"
                if residual_video_children
                else node.path
            )
            candidates = [WorkCandidate(
                work_unit_id=_work_unit_id(root_task_id, main_boundary),
                boundary_key=main_boundary,
                source_paths=tuple(child.path for child in ordered_main),
                display_label=node.name,
                proposed_media_context="tv",
                boundary_evidence=BoundaryEvidence(
                    role=DirectoryRole.SINGLE_WORK,
                    confidence=0.93,
                    reasons=reasons,
                    competing_roles=(DirectoryRole.SERIES_CONTAINER.value,),
                ),
                claimed_seasons=generic_seasons,
            )]
            for child in residual_video_children:
                candidates.append(WorkCandidate(
                    work_unit_id=_work_unit_id(root_task_id, child.path),
                    boundary_key=child.path,
                    source_paths=(child.path,),
                    display_label=child.name,
                    proposed_media_context=_propose_media_context(child),
                    boundary_evidence=BoundaryEvidence(
                        role=DirectoryRole.SERIES_CONTAINER,
                        confidence=0.85,
                        reasons=(
                            "结构季目录之外存在独立有标题视频子目录",
                        ),
                        competing_roles=(DirectoryRole.MOVIE_COLLECTION.value,),
                    ),
                    claimed_seasons=_explicit_file_claimed_seasons(child),
                ))
            return candidates
        # A verified generic cohort exists, but at least one sibling is not
        # independently proven.  Keep the whole root together and suppress
        # the broader decorated-cohort rule below; otherwise an ``Aftershow``
        # branch could be split merely because the season labels are noisy.
        generic_cohort_unresolved = True

    # --- Rule 1c: decorated sibling season cohort -----------------------
    # Some uploaders put every season in a release-named sibling directory,
    # rather than a bare ``Season 01`` directory.  Recognise it only when the
    # directory and its video filenames corroborate each other.  The returned
    # candidates claim only their exact sibling paths, so unrelated aftershows
    # and root-level resource files remain outside the TV unit's ownership.
    cohorts = _decorated_season_cohorts(node)
    if cohorts and root_videos == 0 and not generic_cohort_unresolved:
        claimed_paths = {
            child.path
            for _signature, children, _seasons in cohorts
            for child in children
        }
        # A remaining video-bearing bare season folder would otherwise be
        # unowned.  Leave the entire root to the conservative fallback rather
        # than silently dropping it from the cohort.
        residual_video_children = [
            child for child in node.children
            if _child_has_video(child) and child.path not in claimed_paths
        ]
        residual_titled = [child for child in residual_video_children if _is_titled_child(child)]
        if len(residual_titled) == len(residual_video_children):
            candidates: list[WorkCandidate] = []
            for signature, children, seasons in cohorts:
                boundary_key = f"{node.path}/@season-cohort/{signature}"
                candidates.append(WorkCandidate(
                    work_unit_id=_work_unit_id(root_task_id, boundary_key),
                    boundary_key=boundary_key,
                    source_paths=tuple(child.path for child in children),
                    display_label=_cohort_display_label(children[0]),
                    proposed_media_context="tv",
                    boundary_evidence=BoundaryEvidence(
                        role=DirectoryRole.SINGLE_WORK,
                        confidence=0.93,
                        reasons=(
                            f"发现 {len(seasons)} 个同签名装饰季目录，且目录季号与 SxxExx 文件一致",
                        ),
                        competing_roles=(),
                    ),
                    claimed_seasons=seasons,
                ))
            if residual_titled:
                reasons = (
                    f"发现 {len(residual_titled)} 个未归入季度 cohort 的有名字子目录",
                )
                for child in residual_titled:
                    candidates.append(WorkCandidate(
                        work_unit_id=_work_unit_id(root_task_id, child.path),
                        boundary_key=child.path,
                        source_paths=(child.path,),
                        display_label=child.name,
                        proposed_media_context=_propose_media_context(child),
                        boundary_evidence=BoundaryEvidence(
                            role=DirectoryRole.SERIES_CONTAINER,
                            confidence=0.85,
                            reasons=reasons,
                            competing_roles=(DirectoryRole.MOVIE_COLLECTION.value,),
                        ),
                        claimed_seasons=_explicit_file_claimed_seasons(child),
                    ))
            return candidates

    # --- Rule 3: Series container (multiple titled sub-works) -----------
    # Condition: ≥2 titled children, no season dirs at root level, no root
    # videos that would suggest the root itself is a single work.  When every
    # titled child is movie-shaped (exactly one large video), the container is
    # a MOVIE_COLLECTION instead of a generic series container.
    if (
        len(titled_children) >= 2
        and len(bare_season_children) == 0
        and root_videos == 0
        and not generic_cohort_unresolved
    ):
        reasons = [
            f"发现 {len(titled_children)} 个包含视频的有名字子目录",
        ]
        competing: list[str] = []
        movie_shaped = [c for c in titled_children if _single_large_video(c)]
        all_movie_shaped = len(movie_shaped) == len(titled_children)
        role = (
            DirectoryRole.MOVIE_COLLECTION
            if all_movie_shaped
            else DirectoryRole.SERIES_CONTAINER
        )
        if all_movie_shaped:
            reasons.append("每个子目录各含单个大视频文件（电影合集特征）")
        else:
            competing.append(DirectoryRole.MOVIE_COLLECTION.value)

        evidence = BoundaryEvidence(
            role=role,
            confidence=0.85,
            reasons=tuple(reasons),
            competing_roles=tuple(competing),
        )
        candidates: list[WorkCandidate] = []
        for child in titled_children:
            child_context = _propose_media_context(child)
            # A titled child whose own name carries a season marker (``不死者
            # 王者 第一季``) is an explicit B/W season fact: D's default-season
            # derivation can use it even when the child's filenames are bare
            # bracket ordinals that the unqualified grammar skips.
            child_season = _season_number_from_directory_name(child.name)
            child_claimed = (child_season,) if child_season is not None else ()
            candidates.append(WorkCandidate(
                work_unit_id=_work_unit_id(root_task_id, child.path),
                boundary_key=child.path,
                source_paths=(child.path,),
                display_label=child.name,
                proposed_media_context=child_context,
                boundary_evidence=BoundaryEvidence(
                    role=role,
                    confidence=0.85,
                    reasons=tuple(reasons),
                    competing_roles=tuple(competing),
                ),
                claimed_seasons=child_claimed,
            ))
        return candidates

    # --- Rule 4: Movie collection (multiple subdirs, each one large video)
    if (
        len(bare_season_children) == 0
        and root_videos == 0
        and len(node.children) >= 2
        and all(_single_large_video(c) for c in node.children)
        and not generic_cohort_unresolved
    ):
        evidence = BoundaryEvidence(
            role=DirectoryRole.MOVIE_COLLECTION,
            confidence=0.80,
            reasons=(
                f"{len(node.children)} 个子目录各含一个大视频文件（电影合集特征）",
            ),
            competing_roles=(),
        )
        candidates = []
        for child in node.children:
            candidates.append(WorkCandidate(
                work_unit_id=_work_unit_id(root_task_id, child.path),
                boundary_key=child.path,
                source_paths=(child.path,),
                display_label=child.name,
                proposed_media_context="movie",
                boundary_evidence=evidence,
            ))
        return candidates

    # --- Rule 5: Single work (default) ----------------------------------
    # Root has videos directly, or a single titled child, or season subdirs
    # (multi-season single TV show).
    total_videos = count_video_files(node)
    if total_videos > 0 or season_children:
        reasons: list[str] = []
        if root_videos > 0:
            reasons.append(f"根目录直接含 {root_videos} 个视频文件")
        if season_children:
            reasons.append(
                f"发现 {len(season_children)} 个季目录（多季单作品）"
            )
        if len(titled_children) == 1:
            reasons.append("仅有一个有名字的子目录含视频")
        evidence = BoundaryEvidence(
            role=DirectoryRole.SINGLE_WORK,
            confidence=0.75,
            reasons=tuple(reasons) if reasons else ("默认单作品",),
            competing_roles=(),
        )
        return [WorkCandidate(
            work_unit_id=_work_unit_id(root_task_id, node.path),
            boundary_key=node.path,
            source_paths=(node.path,),
            display_label=node.name,
            proposed_media_context=_propose_media_context(node),
            boundary_evidence=evidence,
            claimed_seasons=_single_work_claimed_seasons(node, season_children),
        )]

    # --- Rule 6: Uncertain ----------------------------------------------
    evidence = BoundaryEvidence(
        role=DirectoryRole.UNCERTAIN,
        confidence=0.30,
        reasons=("无法从目录结构确定作品边界",),
        competing_roles=(),
    )
    return [WorkCandidate(
        work_unit_id=_work_unit_id(root_task_id, node.path),
        boundary_key=node.path,
        source_paths=(node.path,),
        display_label=node.name,
        proposed_media_context="unknown",
        boundary_evidence=evidence,
    )]
