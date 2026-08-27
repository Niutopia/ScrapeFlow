"""TMDB identity matching and source-context normalization.

This module owns query cleanup, media-context inference and confidence-scored
TMDB selection. It does not plan names, seasons or destination trees: those
remain under the Engine runtime and consume only a confirmed
``AutoMatch`` result from this boundary.
"""

from __future__ import annotations

import difflib
import re
import unicodedata
from pathlib import Path
from typing import Any, Collection, Mapping, Sequence, TYPE_CHECKING

from .errors import ApiError, PlanError
from .data.release_lexicon import (
    BIDIRECTIONAL_LEXICAL_VARIANTS,
    CROSS_SCRIPT_SEASON_ALIASES,
    LEXICAL_VARIANTS,
    QUERY_VARIANT_ALIASES,
    SOURCE_NAME_CORRECTIONS,
    SOURCE_QUERY_OVERRIDES,
)
from .models import AutoMatch
from .remote_paths import (
    join_remote,
    normalize_remote_path,
    safe_name,
    split_remote,
)

if TYPE_CHECKING:
    from .core import TMDBClient
    from .work_units import IdentityEvidence


AUTO_MATCH_MIN_MARGIN = 0.08

# These are physical-release labels which can describe a separately
# catalogued short TV work.  ``SP``/``SPECIAL`` stay useful planning context,
# but are too broad to select a different TMDB identity automatically.  OVA,
# OAV and OAD are bounded enough to require corresponding *official TMDB*
# evidence when the source is a complete numbered physical-release run.
_PHYSICAL_SPECIAL_IDENTITY_MARKERS = frozenset({"OVA", "OAV", "OAD"})

# A folder that says only ``Season 02``/``第二季`` identifies a structural
# position, never a creative work.  C/U may still use it together with the
# exact B-snapshot parent title, but it must not consume the bounded query
# budget ahead of that title or a title-bearing representative filename.
_GENERIC_SEASON_IDENTITY_LABEL_RE = re.compile(
    r"^\s*(?:"
    r"(?:season|s)\s*0*\d{1,3}"
    r"|第\s*(?:\d{1,3}|[一二三四五六七八九十百零〇两]{1,5})\s*季"
    r"|(?:[一二三四五六七八九十百零〇两]{1,5})\s*季"
    r")\s*$",
    re.IGNORECASE,
)

_COORDINATE_ONLY_IDENTITY_LABEL_RE = re.compile(
    r"^\s*(?:"
    r"S\s*0*\d{1,3}\s*E\s*0*\d{1,4}"
    r"|E\s*0*\d{1,4}"
    r"|第\s*0*\d{1,4}\s*[集话話期]"
    r"|\[\s*0*\d{1,4}\s*\]"
    r")\s*$",
    re.IGNORECASE,
)


def _is_generic_season_identity_label(value: object) -> bool:
    """Return true only for a bare directory-style season label."""
    return bool(
        _GENERIC_SEASON_IDENTITY_LABEL_RE.fullmatch(str(value or ""))
    )


def _is_non_structural_identity_evidence(value: object) -> bool:
    """Whether a C/U label contains a usable work-title clue, not a coordinate."""
    query = _REPRESENTATIVE_MEDIA_SUFFIX_RE.sub("", str(value or "")).strip()
    return bool(
        query
        and not _is_generic_season_identity_label(query)
        and not _COORDINATE_ONLY_IDENTITY_LABEL_RE.fullmatch(query)
        and _usable_release_title_query(query)
    )


def _extract_year(value: Any) -> str:
    text = str(value or "")
    match = re.match(r"^(19|20)\d{2}(?:$|[-/])", text)
    return match.group(0)[:4] if match else "未知年份"


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


def _search_language(query: str) -> str | None:
    """Use TMDB's zh-CN localization for CJK queries so a Chinese release label
    matches the localized name instead of the English one.  Non-CJK queries keep
    the client default language."""
    if re.search(r"[\u3400-\u9fff\u3040-\u30ff]", query or ""):
        return "zh-CN"
    return None


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
    # Bounded lexical variants come from the release lexicon (data, not
    # logic).  Normal candidate scoring must still prove the work before any
    # variant can be selected; these spellings never grant an identity.
    lexical_base = without_year or without_parenthetical_date or normalized
    for old, new in BIDIRECTIONAL_LEXICAL_VARIANTS:
        if old in lexical_base:
            variant = lexical_base.replace(old, new)
            if variant not in variants:
                variants.append(variant)
        elif new in lexical_base:
            variant = lexical_base.replace(new, old)
            if variant not in variants:
                variants.append(variant)
    for old, new in LEXICAL_VARIANTS:
        base = lexical_base if old in lexical_base else (relaxed or normalized)
        variant = base.replace(old, new)
        if variant and variant not in variants:
            variants.append(variant)
    # Bounded release aliases cover well-established short translations and
    # recurring transcription/obfuscation errors.  These are query variants,
    # never direct identities: candidate scoring, ambiguity margins and media
    # type checks remain authoritative.
    for pattern, replacement in QUERY_VARIANT_ALIASES:
        alias = re.sub(pattern, replacement, normalized, flags=re.I).strip()
        if alias != normalized and alias and alias not in variants:
            variants.append(alias)
    parts = [part.strip() for part in re.split(r"[：:]", query) if part.strip()]
    if len(parts) > 1 and len(parts[-1]) >= 4 and parts[-1] not in variants:
        variants.append(parts[-1])
    return variants[:7]


_BOUNDARY_TRAILING_QUALITY_TAIL_RE = re.compile(
    r"(?:[\s._+\-]*[【\[(（]?\s*)"
    r"(?:4k|8k|2160p|1440p|1080p|720p|576p|480p|"
    r"bluray|blu-?ray|web-?dl|webrip|x26[45]|h26[45]|hevc|av1|"
    r"10bit|8bit|aac|flac|dts)"
    r"(?:\s*[】\])）])?\s*$",
    re.IGNORECASE,
)
_BOUNDARY_TRAILING_BATCH_COUNT_RE = re.compile(
    r"(?:全|共)\s*\d{1,4}\s*(?:集|话|話|期)\s*$",
)

# Half-width bracket groups in a release label are either packaging (group
# name, codec, resolution, subtitle language, episode span) or title
# evidence: a bracket-only release carries the work title inside them
# (``[DBD-Raws][大剑][1080P]``), and a bracketed Latin alias is legitimate
# cross-script evidence (``[Meaningful Show]``).  Discard only groups that
# match the bounded packaging vocabulary or carry no letters at all.
_HALF_WIDTH_BRACKET_PACKAGING_RE = re.compile(
    r"(?:全集|合集|全系列|特典|映像|花絮|扫图|图集|字幕|简繁|繁简|简中|繁中|中字|"
    r"内封|内嵌|外挂|双语|国语|粤语|台配|美版|日版|台版|港版|"
    r"(?:19|20)\d{2}|"
    r"\d{1,4}\s*[-~]\s*\d{1,4}|全\s*\d{1,4}\s*集|"
    r"(?:BD|DVD|WEB|BDrip|TV)?[\s._-]*(?:1080|2160|720|480)[pP]|"
    r"BD(?:rip)?|WEB[-]?DL|WEBRip|Remux|"
    r"(?:hi[\s._-]*)?10[pP]|8bit|10bit|HEVC|AVC|AV1|x26[45]|h26[45]|"
    r"FLAC|AAC|DDP|DTS|MKV|MP4|Fin)",
    re.IGNORECASE,
)


def _unwrap_half_width_title_brackets(text: str) -> str:
    """Keep bracketed titles/aliases; drop bounded release packaging tags.

    Anime release folders put the work title inside half-width brackets
    (``[DBD-Raws][大剑][01-26TV全集+特典映像][1080P]...``).  A blanket strip
    would lose the only title evidence; unwrapping every bracket would push
    group/codec tags into the query.  A group survives only when it carries
    letters and none of the packaging vocabulary matches; a group with a CJK
    title is unwrapped in place so its text joins the query.  A single-token
    Latin bracket is a release-group tag (``[DBD-Raws]``), while a
    multi-word Latin bracket is ordinary alias evidence
    (``[Meaningful Show]``) and stays verbatim.
    """

    def replacement(match: re.Match[str]) -> str:
        content = match.group(1).strip()
        if not content:
            return " "
        if _HALF_WIDTH_BRACKET_PACKAGING_RE.search(content):
            return " "
        if not re.search(r"[㐀-鿿぀-ヿ]", content):
            if re.fullmatch(r"[^\s]+", content):
                return " "
            return match.group(0)
        return f" {content} "

    return re.sub(r"\[([^\]]*)\]", replacement, text)


def _clean_boundary_identity_query(value: str) -> str:
    """Derive a bounded CJK release-label query without rewriting the source.

    The raw boundary label remains the first persisted and queried value.  This
    helper only adds a second query variant for the narrow, common shape of a
    separated one-letter shelf prefix plus a CJK title followed by release
    metadata.  It deliberately removes neither arbitrary bracketed text nor
    title words: those can be real aliases and remain subject to the normal
    cross-script guard.  It also does not inspect filenames or derive an
    episode/season coordinate.
    """
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    text = re.sub(
        r"^\s*[A-Za-z]\s+(?=[\u3400-\u9fff\u3040-\u30ff])",
        "",
        text,
    )
    # A leading full-width genre bucket (``【美剧】``/``【番剧】``) is
    # source routing metadata, not part of the work title.  Keep this list
    # deliberately bounded and anchored so arbitrary title brackets remain
    # ordinary evidence.
    text = re.sub(
        r"^\s*【(?:美剧|欧美剧|英剧|韩剧|日剧|国产剧|港剧|台剧|番剧|动画|动漫|电影|纪录片)】\s*",
        "",
        text,
    )
    # Full-width release brackets (【4K】/【日语中字】/【类型：…】/【全 N 集】)
    # are packaging metadata; drop them, then unwrap a remaining bracket so a
    # title wrapped as 【Title】 survives the query.  Half-width ``[]`` stays
    # untouched here (it can be a real alias).
    text = re.sub(
        r"【[^】]*(?:4k|8k|2160p|1080p|720p|480p|字幕|中字|类型|全\s*\d+\s*集|内封|内嵌|外挂)[^】]*】",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"【([^】]*)】", r" \1 ", text)
    # Half-width brackets need the opposite split: a bracket-only release
    # carries the work title inside them, so unwrap CJK title groups and drop
    # the packaging ones instead of leaving the raw group tags in the query.
    text = _unwrap_half_width_title_brackets(text)
    # Release metadata (year / count / subtitle / quality) is not part of the
    # work title.  Strip it unconditionally so a title-bearing folder such as
    # ``钢之炼金术师（2003）全51集 1080P`` or ``有意义中文剧名（2024）全12集``
    # matches its TMDB title.  The year is preserved in ``IdentityEvidence.years``
    # for scoring; only the query is cleaned here.
    text = re.sub(r"[（(]\s*(?:19|20)\d{2}(?:\s*[.\-/]\s*(?:(?:19|20)\d{2}|\d{1,2}))?\s*[)）]", " ", text)
    # A trailing parenthetical release-group/technical suffix (for example
    # ``（DBD&HKG&X2字幕组 - BDRip HEVC-10bit FLAC）`` or
    # ``（AMZN.WEB-DL.AVC.DDP.2.0）``) is packaging, not title words.  The
    # latter shape is common for otherwise title-bearing CJK folders whose
    # files are bare ordinals; leaving it attached makes TMDB receive a
    # release fingerprint instead of the actual title and can exhaust the
    # bounded query variants before the exact title is tried.
    text = re.sub(
        r"[（(][^）)]*(?:字幕组|压制组|压制|BDRip|BDRIP|Blu-?Ray|"
        r"AMZN|WEB[- .]?DL|WEBRip|REMUX|AVC|H26[45]|x26[45]|HEVC|"
        r"FLAC|AAC|DDP|DTS(?:-HD)?|TrueHD|EAC3|AC3|10bit|8bit)"
        r"[^）)]*[)）]\s*$",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(?:全|共)\s*\d{1,4}\s*(?:集|话|話|期)", " ", text)
    text = re.sub(r"\+\s*(?:OVA|OAV|OAD|SP)(?:\s*\+)?", " ", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?:内封|内嵌|外挂|简体|繁体|简英|简中|简日|繁中|繁日|简繁|中英|中日|日英|双语|硬字幕|软字幕|中文字幕)(?:字幕)?",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"(?:超清|收藏版|4k|8k|2160p|1440p|1080p|720p|576p|480p|"
        r"blu-?ray|bdrip|web-?dl|webrip|x26[45]|h26[45]|hevc|av1|"
        r"hi[\s._-]*10p|ma10p|10[\s._-]*bit|8bit)",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    # A trailing release size (110(1).2G / 共75G) and a duplicate marker ``(1)``
    # are packaging, not title words.  Drop the marker without inserting a
    # space so the numeric size stays a single token for the next pass.
    text = re.sub(r"\(\d+\)", "", text)
    text = re.sub(r"(?:共|约)?\s*\d+(?:\.\d+)?\s*[GT]B?\s*$", " ", text)
    # A leading numeric/volume prefix is only source ordering, and only when it
    # precedes a CJK title (``01 寒蝉鸣泣之时``).
    text = re.sub(r"^\s*0*\d{1,3}[\s._\-]+(?=[\u3400-\u9fff])", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    # A trailing season label (第二季/第 2 季/Season 2) is source layout, not
    # part of the work title.  Strip it so a season subdir whose name repeats
    # the parent title (排球少年第二季) can still match the parent TMDB entry.
    text = re.sub(
        r"[\s_-]*(?:第\s*[0-9一二三四五六七八九十百]+\s*季|season\s*0*\d+)\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    # Release containers also encode a season span (``1-3季`` or
    # ``S01-S03``).  It is layout evidence, not a title token; strip only a
    # trailing, explicitly bounded span.
    text = re.sub(
        r"[\s._-]*(?:\d{1,3}\s*[-~至]\s*\d{1,3}\s*季|"
        r"S\s*0*\d{1,3}\s*[-~至]\s*S?\s*0*\d{1,3})\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    # Chinese release labels commonly put dots between every title
    # character (``金.斯.敦.市.长``).  Collapse only separators surrounded by
    # CJK characters; punctuation in an actual title remains untouched.
    text = re.sub(
        r"(?<=[\u3400-\u9fff\u3040-\u30ff])[\s._+\-]+(?=[\u3400-\u9fff\u3040-\u30ff])",
        "",
        text,
    )
    # A batch count and a quality tag can appear in either tail order.  Bound
    # this cleanup to a few passes so malformed labels cannot be over-cleaned.
    for _ in range(3):
        before = text
        text = _BOUNDARY_TRAILING_QUALITY_TAIL_RE.sub("", text)
        text = _BOUNDARY_TRAILING_BATCH_COUNT_RE.sub("", text)
        text = text.rstrip(" ._+-")
        if text == before:
            break
    # A bracket group whose inner tokens were all stripped above must not
    # survive as an empty pair of delimiters in the final query.
    return re.sub(r"\[\s*\]", " ", text).strip()


def _script_evidence_text(value: str) -> str:
    """Remove known release tails before classifying title scripts.

    Intake folders may begin with a single Latin bucket letter followed by a
    CJK title (for example ``B 某剧``).  That routing marker is not a Latin
    title alias and must not turn an otherwise same-script CJK match into a
    cross-script match.  The same bounded cleaner also omits tail-only quality
    and batch-count metadata, while preserving arbitrary bracketed text and
    actual Latin title words for the cross-script guard.
    """
    return _clean_boundary_identity_query(value)


def _cross_script_unique_match(query: str, titles: Sequence[str]) -> bool:
    query_text = _script_evidence_text(query)
    query_has_latin = bool(re.search(r"[A-Za-z]", query_text))
    query_has_cjk = bool(re.search(r"[\u3400-\u9fff\u3040-\u30ff]", query_text))
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
    # Archive preprocessing keeps the original archive basename as its
    # task-staging leaf.  The extension is transport metadata, not a TMDB
    # title token, so strip only the closed archive suffix set before the
    # regular release-name cleanup below.
    name = re.sub(r"\.part\d+\.rar\s*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\.(?:zip|7z|rar|001|r0\d)\s*$", "", name, flags=re.IGNORECASE)
    # Regex-guarded canonical queries for abbreviated release folder labels
    # come from the release lexicon (data, not logic).
    for pattern, replacement in SOURCE_QUERY_OVERRIDES:
        if re.search(pattern, name, re.IGNORECASE):
            return replacement
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
    # Full-width release brackets (【…】) are common in Chinese packaging:
    # metadata brackets carry quality/subtitle/genre/count tokens and are
    # dropped, while a remaining bracket (usually the title wrapper) is
    # unwrapped so the title inside is kept.
    name = re.sub(
        r"【[^】]*(?:4k|8k|2160p|1080p|720p|480p|字幕|中字|类型|全\s*\d+\s*集|内封|内嵌|外挂)[^】]*】",
        " ",
        name,
        flags=re.IGNORECASE,
    )
    name = re.sub(r"【([^】]*)】", r" \1 ", name)
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
    # Exact source-name corrections from the release lexicon (data, not logic).
    for wrong, correct in SOURCE_NAME_CORRECTIONS:
        name = name.replace(wrong, correct)
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
    # valid query and still has to pass the normal confidence/ambiguity thresholds.
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


_REPRESENTATIVE_MEDIA_SUFFIX_RE = re.compile(
    r"\.(?:mkv|mp4|m4v|avi|mov|wmv|flv|webm|ts|m2ts)$",
    re.IGNORECASE,
)


def _usable_representative_identity_query(value: str) -> bool:
    """Reject representative media names that are only an episode ordinal.

    ``IdentityEvidence`` is durable and can be constructed by older callers,
    so filtering naked numeric names only while extracting evidence is not
    sufficient.  This second check keeps a legacy ``01.mp4`` representative
    from becoming a movie search query; other release-name checks remain in
    :func:`_usable_release_title_query`.
    """
    query = _REPRESENTATIVE_MEDIA_SUFFIX_RE.sub("", str(value or "")).strip()
    return bool(query) and _usable_release_title_query(query)


def _title_from_representative_episode_filename(value: str) -> str | None:
    """Extract a bounded work-title query from an explicit episode filename.

    A directory label may be an opaque release package while its video files
    retain the actual title.  ``Show.Name.S02E03`` is useful identity evidence,
    but sending the episode suffix to TMDB often produces no search result.
    Some standalone specials and short companion series use ``Show.Name.E01``
    or ``Show.Name.E01-E06`` instead of a season-qualified marker.  Those are
    equally explicit only when a usable title precedes the marker.  A bare
    ordinal such as ``01`` is never promoted into a title query.
    """
    stem = Path(value).stem.strip(" ._-")
    marker = re.search(
        r"(?:^|[\s._\-\[\](){}])S\s*\d{1,3}\s*E\s*\d{1,4}"
        r"(?=$|[\s._\-\[\](){}])",
        stem,
        flags=re.IGNORECASE,
    )
    if marker is None:
        marker = re.search(
            r"(?:^|[\s._\-\[\](){}])E\s*\d{1,4}"
            r"(?:\s*[\-–—~～]\s*E\s*\d{1,4})?"
            r"(?=$|[\s._\[\](){}])",
            stem,
            flags=re.IGNORECASE,
        )
    if marker is None:
        return None
    title = stem[:marker.start()].strip(" ._-")
    return title if _usable_release_title_query(title) else None


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
        for latin, cjk_aliases in CROSS_SCRIPT_SEASON_ALIASES.items()
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
    # A trailing season range describes package coverage, not the creative
    # container title. Keep it structurally explicit and suffix-bound so a
    # genuine token such as ``MS01-S03 Project`` remains untouched.
    name = re.sub(
        r"\s*(?:(?:season|s)\s*0*\d{1,3}\s*[-–—~～至到]\s*"
        r"(?:(?:season|s)\s*)?0*\d{1,3}"
        r"|第\s*0*\d{1,3}\s*[-–—~～至到]\s*0*\d{1,3}\s*季)"
        r"\s*(?:全季|全系列|系列合集|合集包|合集)?\s*$",
        " ",
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
        "欧美剧",
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
    """Combine the source library and automatically selected target category.

    ``_media_type_from_source_context`` intentionally ignores the leaf because
    the leaf is normally the work title. Append a synthetic work leaf so that
    the selected category participates in matching instead of being discarded.
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


def bounded_auto_match_candidate_rows(
    candidates: Sequence[AutoMatch], *, limit: int = 5,
) -> list[dict[str, Any]]:
    """Project scored candidates into a bounded, JSON-only evidence list."""
    return [
        {
            "media_type": item.media_type,
            "tmdb_id": item.tmdb_id,
            "title": item.title,
            "year": item.year,
            "confidence": item.confidence,
            "status": item.status,
        }
        for item in list(candidates)[:limit]
    ]


class AutoMatchAmbiguityError(PlanError):
    """A safe rejection that still carries bounded candidate evidence.

    The matcher refuses to choose automatically, but the scored candidates
    remain useful read-only evidence: the reconciliation U-node exposes them
    so the operator can confirm one identity instead of researching TMDB by
    hand.  This is deliberately not a fallback selection mechanism.
    """

    def __init__(self, message: str, *, candidates: Sequence[AutoMatch]) -> None:
        super().__init__(message)
        self.candidates = bounded_auto_match_candidate_rows(candidates)


def _score_identity_candidate(
    raw: Mapping[str, Any],
    *,
    query_years: Collection[str],
    parent_labels: Sequence[str] = (),
    prefer_animation: bool,
    expected_episode_count: int | None,
    min_confidence: float,
    trace_extra: Mapping[str, Any],
    special_markers: Collection[str] = (),
    special_episode_count: int | None = None,
) -> AutoMatch:
    """Score one raw candidate row with the single shared evidence policy.

    This is the ONLY scoring implementation for both matcher entries
    (query-based and IdentityEvidence-based); the two public functions differ
    only in how they gather candidate rows.
    """
    title_score = float(raw["title_score"])
    alias_score = float(raw["alias_score"])
    evidence_score = max(title_score, alias_score)

    parent_bonus = 0.0
    candidate_all_titles = [
        str(value).casefold() for value in [*raw["titles"], *raw["aliases"]]
    ]
    for parent in parent_labels:
        parent_norm = parent.strip().casefold()
        if parent_norm and any(
            parent_norm in title for title in candidate_all_titles
        ):
            parent_bonus = max(parent_bonus, 0.04)

    year_score = 0.0
    wrong_year = False
    if query_years:
        candidate_year = str(raw["year"])
        if candidate_year in query_years:
            year_score = 0.0
        elif candidate_year == "未知年份":
            year_score = -0.06
        else:
            try:
                candidate_year_int = int(candidate_year)
                deltas = [
                    abs(candidate_year_int - int(year))
                    for year in query_years
                    if year.isdigit()
                ]
                minimum_delta = min(deltas) if deltas else 0
                wrong_year = minimum_delta >= 2
                year_score = -0.30 if wrong_year else -0.12
            except ValueError:
                year_score = -0.06

    # The media namespace is routing context, not title evidence.  Keep it
    # visible in the trace but never let it push a weak title over the
    # threshold.
    media_type_score = 0.0
    context_score = 0.0
    if prefer_animation and raw["media_type"] in {"tv", "movie"}:
        if raw["is_animation"] is True:
            context_score = 0.0
        elif raw["is_animation"] is False:
            # An animation-shelf target is strong context: keep enough
            # separation that a same-title live-action result cannot pass the
            # global ambiguity margin on title evidence alone.
            context_score = -0.12

    episode_structure_score = 0.0
    actual_count = raw["actual_episode_count"]
    if expected_episode_count and isinstance(actual_count, int):
        episode_structure_score = (
            0.0 if actual_count == expected_episode_count else -0.08
        )

    # An explicit physical OVA/OAV/OAD run can be a separately catalogued TV
    # work. It still does not assert a TMDB season: automatic selection needs
    # both a matching official positive-season count and an official marker in
    # the candidate's title, season label, or episode title. This blocks a
    # coincidentally named regular parent from swallowing the short work.
    special_marker_score = 0.0
    special_evidence_required = bool(
        special_episode_count
        and set(special_markers) & _PHYSICAL_SPECIAL_IDENTITY_MARKERS
    )
    official_special_hits = {
        str(value).upper()
        for value in (raw.get("official_special_marker_hits") or ())
    }
    official_special_count_match = bool(raw.get("official_special_count_match"))
    special_detail_checked = bool(raw.get("special_detail_checked"))
    if special_evidence_required:
        if not special_detail_checked:
            special_marker_score = -0.20
        elif official_special_hits and official_special_count_match:
            special_marker_score = 0.20
        elif official_special_hits:
            special_marker_score = -0.30
        elif official_special_count_match:
            special_marker_score = -0.18
        else:
            special_marker_score = -0.30

    confidence = max(
        0.0,
        min(
            1.0,
            evidence_score
            + parent_bonus
            + year_score
            + media_type_score
            + context_score
            + episode_structure_score,
        ),
    )
    confidence = max(0.0, min(1.0, confidence + special_marker_score))
    blockers: list[str] = []
    if wrong_year:
        blockers.append("year_conflict")
    if raw["cross_script"] and alias_score < 0.88:
        blockers.append("cross_script_without_alias_evidence")
    strict_naked_numeric_video_run = bool(
        raw.get("strict_naked_numeric_video_run")
    )
    naked_numeric_cjk_release_eligible = bool(
        raw.get("naked_numeric_cjk_release_eligible")
    )
    if strict_naked_numeric_video_run:
        # A bare ``01`` … ``N`` file run is intentionally not title or
        # coordinate evidence.  Every such source shape is guarded.  Only the
        # narrow CJK release-label fallback can proceed, and then only when
        # the cleaned boundary itself has an exact same-script official
        # title/alias, its explicit year agrees exactly, and the normal
        # ambiguity margin remains in force below.
        if not naked_numeric_cjk_release_eligible:
            blockers.append("naked_numeric_requires_meaningful_cjk_year_boundary")
        if not bool(raw.get("naked_numeric_clean_boundary_query_sent")):
            blockers.append("naked_numeric_requires_sent_clean_boundary_query")
        if not bool(raw.get("naked_numeric_same_script_exact_title_or_alias")):
            blockers.append("naked_numeric_requires_same_script_exact_title_or_alias")
        if not bool(raw.get("naked_numeric_exact_year")):
            blockers.append("naked_numeric_requires_exact_year")
        if raw["media_type"] != "tv":
            blockers.append("naked_numeric_requires_tv_candidate")
    if confidence < min_confidence:
        blockers.append("below_confidence_threshold")
    if special_evidence_required:
        if not special_detail_checked:
            blockers.append("physical_special_official_evidence_unavailable")
        elif not official_special_hits:
            blockers.append("physical_special_marker_not_officially_proven")
        elif not official_special_count_match:
            blockers.append("physical_special_episode_count_mismatch")
    status = "rejected" if blockers else "confirmed"
    components = {
        "title_score": round(title_score, 6),
        "alias_score": round(alias_score, 6),
        "parent_bonus": round(parent_bonus, 6),
        "year_score": round(year_score, 6),
        "media_type_score": round(media_type_score, 6),
        "episode_structure_score": round(episode_structure_score, 6),
        "special_marker_score": round(special_marker_score, 6),
        "context_score": round(context_score, 6),
        "final_score": round(confidence, 6),
    }
    return AutoMatch(
        str(raw["media_type"]),
        int(raw["tmdb_id"]),
        str(raw["title"]),
        str(raw["year"]),
        confidence,
        status,
        components,
        {
            **dict(trace_extra),
            "official_titles": list(raw["titles"]),
            "aliases_checked": list(raw["aliases"]),
            "blockers": blockers,
            "expected_episode_count": expected_episode_count,
            "actual_episode_count": actual_count,
            "physical_special_markers": sorted(str(x) for x in special_markers),
            "physical_special_episode_count": special_episode_count,
            "official_special_marker_hits": sorted(official_special_hits),
            "official_special_count_match": official_special_count_match,
            "special_detail_checked": special_detail_checked,
            "strict_naked_numeric_video_run": strict_naked_numeric_video_run,
            "naked_numeric_cjk_release_eligible": naked_numeric_cjk_release_eligible,
            "naked_numeric_clean_boundary_query_sent": bool(
                raw.get("naked_numeric_clean_boundary_query_sent")
            ),
            "naked_numeric_same_script_exact_title_or_alias": bool(
                raw.get("naked_numeric_same_script_exact_title_or_alias")
            ),
            "naked_numeric_exact_year": bool(raw.get("naked_numeric_exact_year")),
            "naked_numeric_boundary_years": list(
                raw.get("naked_numeric_boundary_years") or ()
            ),
            "requires_normal_margin": strict_naked_numeric_video_run,
        },
    )


def _select_auto_match(
    candidates: Sequence[AutoMatch],
    *,
    query_label: str,
    year_label: str,
) -> tuple[AutoMatch, list[AutoMatch]]:
    """Apply the shared final-selection policy (margin + exact-title rules)."""
    ordered = sorted(
        list(candidates),
        key=lambda item: (-item.confidence, item.media_type, item.tmdb_id),
    )
    if not ordered:
        raise PlanError(f"TMDB 未找到自动匹配候选: {query_label}")
    best = ordered[0]
    if best.status != "confirmed":
        preview = "; ".join(
            f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%}, {item.status})"
            for item in ordered[:3]
        )
        raise AutoMatchAmbiguityError(
            "自动匹配缺少可验证的标题/别名证据，已拒绝自动选择: " + preview,
            candidates=ordered,
        )
    if "year_conflict" in best.decision_trace.get("blockers", []):
        raise AutoMatchAmbiguityError(
            f"自动匹配候选年份与源目录冲突，拒绝自动选择: "
            f"{year_label}, candidate={best.media_type}/{best.tmdb_id} "
            f"{best.title} ({best.year})",
            candidates=ordered,
        )
    runner_up = ordered[1] if len(ordered) > 1 else None
    best_exact = max(
        float(best.score_components.get("title_score", 0.0)),
        float(best.score_components.get("alias_score", 0.0)),
    ) >= 0.999999
    runner_exact = bool(runner_up) and max(
        float(runner_up.score_components.get("title_score", 0.0)),
        float(runner_up.score_components.get("alias_score", 0.0)),
    ) >= 0.999999
    exact_title_uniquely_identifies_best = (
        best_exact
        and not runner_exact
        and not bool(best.decision_trace.get("requires_normal_margin"))
    )
    if (
        runner_up is not None
        and best.confidence - runner_up.confidence + 1e-9
        < AUTO_MATCH_MIN_MARGIN
        and not exact_title_uniquely_identifies_best
    ):
        raise AutoMatchAmbiguityError(
            "自动匹配前两名证据无法区分，拒绝自动选择: "
            + "; ".join(
                f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%})"
                for item in ordered[:2]
            ),
            candidates=ordered,
        )
    return best, ordered


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
            response = client.get(
                f"/search/{candidate_type}",
                query=search_query,
                language=_search_language(search_query),
            )
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
                f"/search/{candidate_type}",
                query=search_query,
                page=1,
                language=_search_language(search_query),
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
        return _score_identity_candidate(
            raw,
            query_years={query_year} if query_year else set(),
            parent_labels=(),
            prefer_animation=prefer_animation,
            expected_episode_count=expected_episode_count,
            min_confidence=min_confidence,
            trace_extra={
                "query": query,
                "matched_query_variant": raw.get("matched_query", query),
                "query_year": query_year,
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
    return _select_auto_match(
        candidates,
        query_label=query,
        year_label=f"query_year={query_year}",
    )


def _physical_special_marker_key(value: object) -> str | None:
    """Normalize the narrow physical-release vocabulary for evidence checks."""
    marker = str(value or "").strip().upper()
    if marker == "OAV":
        return "OVA"
    if marker in _PHYSICAL_SPECIAL_IDENTITY_MARKERS:
        return marker
    return None


def _official_physical_special_markers(values: Collection[object]) -> set[str]:
    """Return OVA/OAD labels literally present in official TMDB text."""
    output: set[str] = set()
    for value in values:
        text = unicodedata.normalize("NFKC", str(value or "")).upper()
        for marker in re.findall(r"(?<![A-Z])(OVA|OAV|OAD)(?![A-Z])", text):
            key = _physical_special_marker_key(marker)
            if key is not None:
                output.add(key)
    return output


def _tmdb_physical_special_candidate_evidence(
    client: object,
    *,
    tmdb_id: int,
    aliases: Collection[object],
    source_markers: Collection[str],
    source_episode_count: int | None,
) -> dict[str, object]:
    """Read bounded official TMDB evidence for a physical-special TV child.

    The result never creates an identity on its own.  It is merely supplied to
    the normal scorer once an ordinary TMDB search has already produced this
    candidate.  A detail/season failure is recorded as unavailable so C/U
    fails closed rather than falling back to a parent show's title similarity.
    """
    requested_markers = {
        key
        for value in source_markers
        if (key := _physical_special_marker_key(value)) is not None
    }
    result: dict[str, object] = {
        "special_detail_checked": False,
        "official_special_marker_hits": (),
        "official_special_count_match": False,
        "official_special_season": None,
    }
    if not requested_markers or not source_episode_count:
        return result
    getter = getattr(client, "get", None)
    if not callable(getter):
        return result
    try:
        detail = getter(f"/tv/{tmdb_id}")
    except ApiError:
        return result
    except Exception:
        return result
    if not isinstance(detail, Mapping):
        return result
    result["special_detail_checked"] = True
    official_texts: list[object] = [
        *aliases,
        detail.get("name"),
        detail.get("original_name"),
    ]
    positives: list[tuple[int, int, str]] = []
    raw_seasons = detail.get("seasons")
    if not isinstance(raw_seasons, list):
        return result
    for season in raw_seasons:
        if not isinstance(season, Mapping):
            return result
        number = season.get("season_number")
        count = season.get("episode_count")
        if (
            isinstance(number, bool)
            or not isinstance(number, int)
            or isinstance(count, bool)
            or not isinstance(count, int)
            or number < 0
            or count < 0
        ):
            return result
        if number > 0 and count > 0:
            positives.append((number, count, str(season.get("name") or "")))
            official_texts.append(season.get("name"))
    # A separate short work must itself have exactly one published positive
    # season.  A multi-season parent which happens to have an N-episode season
    # is not sufficient proof for a physical OAD/OVA child.
    if len(positives) != 1 or positives[0][1] != source_episode_count:
        result["official_special_marker_hits"] = tuple(sorted(
            _official_physical_special_markers(official_texts) & requested_markers
        ))
        return result
    season_number = positives[0][0]
    marker_hits = _official_physical_special_markers(official_texts)
    # The show's official title is normally enough.  Some TMDB records label
    # only their episodes, so make one bounded season request for the sole
    # matching season and look at official episode titles too.
    if not (marker_hits & requested_markers):
        try:
            season_payload = getter(f"/tv/{tmdb_id}/season/{season_number}")
        except ApiError:
            season_payload = None
        except Exception:
            season_payload = None
        if isinstance(season_payload, Mapping):
            episodes = season_payload.get("episodes")
            if isinstance(episodes, list) and len(episodes) == source_episode_count:
                for episode in episodes:
                    if not isinstance(episode, Mapping):
                        marker_hits = set()
                        break
                    official_texts.extend((
                        episode.get("name"),
                        episode.get("original_name"),
                    ))
                marker_hits = _official_physical_special_markers(official_texts)
    result.update({
        "official_special_marker_hits": tuple(sorted(marker_hits & requested_markers)),
        "official_special_count_match": True,
        "official_special_season": season_number,
    })
    return result


def physical_special_candidate_evidence(
    client: object,
    *,
    tmdb_id: int,
    aliases: Collection[object] = (),
    source_markers: Collection[str],
    source_episode_count: int | None,
) -> dict[str, object]:
    """Public C/D helper for the shared formal OVA/OAD proof grammar."""
    return _tmdb_physical_special_candidate_evidence(
        client,
        tmdb_id=tmdb_id,
        aliases=aliases,
        source_markers=source_markers,
        source_episode_count=source_episode_count,
    )


def auto_match_from_evidence(
    client: TMDBClient,
    evidence: IdentityEvidence,
    *,
    min_confidence: float = 0.70,
    prefer_animation: bool = False,
    excluded_tmdb_ids: Collection[int] | None = None,
) -> tuple[AutoMatch, list[AutoMatch]]:
    """Match a WorkUnit's IdentityEvidence against TMDB.

    Evaluates boundary labels, parent container clues, representative names,
    years, and structural episode counts to select and score candidates.
    """
    if not evidence.boundary_label.strip():
        raise PlanError("IdentityEvidence boundary_label 为空")
    if not 0 <= min_confidence <= 1:
        raise PlanError("自动匹配最低置信度必须在 0 到 1 之间")

    excluded_ids = {int(value) for value in (excluded_tmdb_ids or ())}
    target_media_type = evidence.media_shape if evidence.media_shape in {"tv", "movie"} else None
    initial_types = [target_media_type] if target_media_type else ["tv", "movie"]

    # Gather search queries in prioritized order.
    candidate_queries: list[str] = []

    # Direct boundary label, plus a narrowly cleaned CJK release variant.
    # Preserve the raw label for auditability and normal TMDB behavior; the
    # clean value is only an additional query, never a persisted rewrite.
    boundary_label = evidence.boundary_label.strip()
    clean_boundary_query = _clean_boundary_identity_query(boundary_label)
    clean_boundary_is_cjk = bool(
        re.search(r"[\u3400-\u9fff\u3040-\u30ff]", clean_boundary_query)
    )
    strict_naked_numeric_guard = bool(evidence.strict_naked_numeric_video_run)
    naked_numeric_cjk_release_eligible = bool(
        evidence.naked_numeric_cjk_release_eligible
    )
    generic_season_boundary = _is_generic_season_identity_label(boundary_label)
    if generic_season_boundary and not any(
        _is_non_structural_identity_evidence(value)
        for value in (
            *evidence.parent_labels,
            *evidence.representative_names,
            *evidence.normalized_titles,
            *evidence.aliases,
        )
    ):
        raise PlanError("纯季目录缺少父容器或代表媒体标题证据")
    # Parent-combination queries can consume the six-query budget.  For the
    # narrow strict bare-number fallback, the cleaned boundary is the only
    # title proof allowed, so put it first and later prove it was dispatched.
    if (
        strict_naked_numeric_guard
        and naked_numeric_cjk_release_eligible
        and clean_boundary_is_cjk
        and clean_boundary_query
    ):
        candidate_queries.append(clean_boundary_query)

    if generic_season_boundary:
        # ``第一季``/``Season 02`` is only the source layout.  Give the
        # user-owned container and a title-bearing media sample priority over
        # that structural leaf, otherwise the six-query cap can make TMDB
        # confidently select an unrelated show named "第二季".  These remain
        # normal TMDB search queries and ordinary scoring/ambiguity checks;
        # the parent never injects an identity.
        for parent in evidence.parent_labels:
            p_clean = parent.strip()
            if not p_clean:
                continue
            candidate_queries.append(p_clean)
            candidate_queries.append(f"{p_clean} {boundary_label}")
            candidate_queries.append(f"{p_clean}/{boundary_label}")
        for representative in evidence.representative_names:
            if _is_generic_season_identity_label(representative):
                continue
            title_query = _title_from_representative_episode_filename(
                representative
            )
            if title_query:
                candidate_queries.append(title_query)
            elif _is_non_structural_identity_evidence(representative):
                candidate_queries.append(representative)

    # Combined parent + boundary queries are useful ordinary evidence, but
    # must not crowd out the strict bare-number proof above.
    for parent in evidence.parent_labels:
        p_clean = parent.strip()
        if p_clean and boundary_label:
            candidate_queries.append(f"{p_clean} {boundary_label}")
            candidate_queries.append(f"{p_clean}/{boundary_label}")

    # A generic season label (``第一季``/``第二季``/``Season 02``) is source
    # layout, not a work title.  It must never be dispatched as a standalone
    # query: TMDB would otherwise confidently select an unrelated show whose
    # title merely contains that label (``中国 第二季``).  Only the parent
    # title and its combined variants identify the work.
    if not generic_season_boundary:
        candidate_queries.append(boundary_label)
    if (
        clean_boundary_is_cjk
        and clean_boundary_query
        and clean_boundary_query != boundary_label
        and not generic_season_boundary
    ):
        candidate_queries.append(clean_boundary_query)

    # 3. A representative filename with an explicit ``SxxExx`` marker may
    # carry a clean title even when the boundary is a release-package label.
    # Put that derived query ahead of further noisy variants so it remains
    # inside the bounded search budget.
    for representative in evidence.representative_names:
        title_query = _title_from_representative_episode_filename(representative)
        if title_query:
            candidate_queries.append(title_query)

    # 4. Normalized titles and combined with parent
    for t in evidence.normalized_titles:
        for parent in evidence.parent_labels:
            if parent.strip() and t.strip():
                candidate_queries.append(f"{parent.strip()} {t.strip()}")
        candidate_queries.append(t)

    # 5. Aliases and raw representative names
    for a in evidence.aliases:
        for parent in evidence.parent_labels:
            if parent.strip() and a.strip():
                candidate_queries.append(f"{parent.strip()} {a.strip()}")
        candidate_queries.append(a)
    for r in evidence.representative_names:
        if _usable_representative_identity_query(r):
            candidate_queries.append(r)

    # Deduplicate while preserving order
    seen_q: set[str] = set()
    search_queries: list[str] = []
    for q in candidate_queries:
        norm_q = q.strip()
        if norm_q and norm_q not in seen_q:
            seen_q.add(norm_q)
            search_queries.append(norm_q)

    # ``EpisodePattern.total_episodes`` is a set of observed episode ordinals.
    # For an absolute multi-season release, E01 appears in every season, so
    # that number is not the show's aggregate TMDB episode count.  Avoid
    # turning structurally valid multi-season evidence into a false penalty;
    # single-season evidence remains useful for disambiguation.
    expected_episode_count = (
        evidence.episode_pattern.total_episodes
        if (
            evidence.episode_pattern
            and evidence.episode_pattern.total_episodes > 0
            and evidence.media_shape == "tv"
            and len(evidence.episode_pattern.season_numbers) <= 1
        )
        else None
    )
    # A complete numbered physical OVA/OAV/OAD run is intentionally separate
    # from ``expected_episode_count``. The latter describes ordinary episode
    # structure; the former only becomes identity evidence after TMDB confirms
    # the candidate's own official short-work shape.
    physical_special_markers = tuple(sorted({
        str(marker).upper()
        for marker in evidence.special_markers
        if _physical_special_marker_key(marker) is not None
    }))
    physical_special_episode_count = (
        evidence.special_episode_count
        if evidence.special_numbered_run_complete
        and evidence.special_episode_count is not None
        and set(physical_special_markers) & _PHYSICAL_SPECIAL_IDENTITY_MARKERS
        else None
    )

    query_years = {str(y) for y in evidence.years}
    # ``IdentityEvidence.years`` deliberately aggregates boundary, parent and
    # filename years for ordinary matching.  A bare numeric run is much more
    # fragile: its exact-year gate must be anchored to the work boundary
    # itself, never accidentally satisfied by a container or release-file
    # date.
    boundary_years = set(
        re.findall(r"(?<!\d)(?:19|20)\d{2}(?!\d)", evidence.boundary_label)
    )
    clean_boundary_key = (
        _normalize_match_title(clean_boundary_query)
        if clean_boundary_is_cjk
        else ""
    )
    raw_candidates: list[dict[str, Any]] = []
    searched_types: list[str] = []

    def collect_type(candidate_type: str) -> None:
        if candidate_type in searched_types:
            return
        searched_types.append(candidate_type)
        search_items: list[Any] = []
        seen_search_ids: set[int] = set()
        sent_queries: list[str] = []
        seen_sent_queries: set[str] = set()
        sent_clean_boundary_queries: list[str] = []
        seen_sent_clean_boundary_queries: set[str] = set()

        def record_sent_query(query: str, *, from_clean_boundary: bool) -> None:
            value = str(query or "").strip()
            if not value:
                return
            if value not in seen_sent_queries:
                seen_sent_queries.add(value)
                sent_queries.append(value)
            if (
                from_clean_boundary
                and value not in seen_sent_clean_boundary_queries
            ):
                seen_sent_clean_boundary_queries.add(value)
                sent_clean_boundary_queries.append(value)

        def ingest(response: Mapping[str, Any]) -> None:
            for item in list(response.get("results") or [])[:10]:
                if not isinstance(item, Mapping) or isinstance(item.get("id"), bool):
                    continue
                try:
                    item_id = int(item["id"])
                except (KeyError, TypeError, ValueError):
                    continue
                if item_id in seen_search_ids:
                    continue
                seen_search_ids.add(item_id)
                search_items.append(item)

        # Search top candidate queries
        for sq in search_queries[:6]:
            for variant in _search_query_variants(sq):
                record_sent_query(
                    variant,
                    from_clean_boundary=(sq == clean_boundary_query),
                )
                response = client.get(
                    f"/search/{candidate_type}",
                    query=variant,
                    language=_search_language(variant),
                )
                ingest(response)

        if not search_items and search_queries:
            first_q = search_queries[0]
            record_sent_query(
                first_q,
                from_clean_boundary=(first_q == clean_boundary_query),
            )
            response = client.get(
                f"/search/{candidate_type}",
                query=first_q,
                page=1,
                language=_search_language(first_q),
            )
            ingest(response)

        sent_query_keys = [
            _normalize_match_title(query)
            for query in sent_queries
            if _normalize_match_title(query)
        ]

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

            # Calculate title score only from queries actually sent to TMDB.
            title_score = max(
                (_title_similarity(k, title) for k in sent_query_keys for title in titles),
                default=0.0,
            )

            aliases = (
                _alternative_tmdb_titles(client, candidate_type, tmdb_id)
                if index < 5 else []
            )
            evidence_titles = [*titles, *aliases]
            # The boundary can be a CJK release-package label while a
            # representative filename supplies an exact original-language
            # title.  The cross-script guard must evaluate the query that
            # actually earned the strongest title/alias evidence, not always
            # the boundary label.  Relax that guard only for an exact
            # same-script title match: a merely prefix-similar release string
            # must still supply an official alias.
            matched_query = max(
                sent_queries,
                key=lambda query: max(
                    (
                        _title_similarity(_normalize_match_title(query), title)
                        for title in evidence_titles
                    ),
                    default=0.0,
                ),
                default=evidence.boundary_label,
            )
            matched_query_score = max(
                (
                    _title_similarity(_normalize_match_title(matched_query), title)
                    for title in evidence_titles
                ),
                default=0.0,
            )
            boundary_cross_script = _cross_script_unique_match(
                evidence.boundary_label, evidence_titles
            )
            exact_same_script_evidence = (
                matched_query_score >= 0.999999
                and not _cross_script_unique_match(matched_query, evidence_titles)
            )
            strict_clean_boundary_query = max(
                sent_clean_boundary_queries,
                key=lambda query: max(
                    (
                        _title_similarity(_normalize_match_title(query), title)
                        for title in evidence_titles
                    ),
                    default=0.0,
                ),
                default="",
            )
            strict_clean_boundary_query_score = max(
                (
                    _title_similarity(
                        _normalize_match_title(strict_clean_boundary_query),
                        title,
                    )
                    for title in evidence_titles
                ),
                default=0.0,
            )
            strict_clean_boundary_same_script_exact = (
                bool(strict_clean_boundary_query)
                and strict_clean_boundary_query_score >= 0.999999
                and not _cross_script_unique_match(
                    strict_clean_boundary_query,
                    evidence_titles,
                )
            )
            alias_score = max(
                (_title_similarity(k, title) for k in sent_query_keys for title in aliases),
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
            physical_special_evidence: Mapping[str, object] = {}
            if (
                physical_special_episode_count
                and candidate_type == "tv"
                and index < 5
            ):
                physical_special_evidence = physical_special_candidate_evidence(
                    client,
                    tmdb_id=tmdb_id,
                    aliases=aliases,
                    source_markers=physical_special_markers,
                    source_episode_count=physical_special_episode_count,
                )
            raw_candidates.append({
                "media_type": candidate_type,
                "tmdb_id": tmdb_id,
                "title": titles[0],
                "titles": titles,
                "aliases": aliases,
                "year": year,
                "title_score": title_score,
                "alias_score": alias_score,
                "cross_script": boundary_cross_script and not exact_same_script_evidence,
                "matched_query": matched_query,
                "is_animation": is_animation,
                "actual_episode_count": actual_episode_count,
                "strict_naked_numeric_video_run": strict_naked_numeric_guard,
                "naked_numeric_cjk_release_eligible": naked_numeric_cjk_release_eligible,
                "naked_numeric_clean_boundary_query_sent": bool(
                    sent_clean_boundary_queries
                ),
                "naked_numeric_same_script_exact_title_or_alias": (
                    bool(clean_boundary_key)
                    and _normalize_match_title(strict_clean_boundary_query)
                    == clean_boundary_key
                    and strict_clean_boundary_same_script_exact
                ),
                "naked_numeric_exact_year": (
                    bool(boundary_years) and year in boundary_years
                ),
                "naked_numeric_boundary_years": tuple(sorted(boundary_years)),
                **physical_special_evidence,
            })

    def score(raw: Mapping[str, Any]) -> AutoMatch:
        return _score_identity_candidate(
            raw,
            query_years=query_years,
            parent_labels=evidence.parent_labels,
            prefer_animation=prefer_animation,
            expected_episode_count=expected_episode_count,
            min_confidence=min_confidence,
            special_markers=physical_special_markers,
            special_episode_count=physical_special_episode_count,
            trace_extra={
                "query": evidence.boundary_label,
                "matched_query_variant": raw.get(
                    "matched_query", evidence.boundary_label
                ),
                "query_years": sorted(query_years),
                "work_unit_id": evidence.work_unit_id,
            },
        )

    for initial_type in initial_types:
        collect_type(initial_type)
    candidates = [
        score(item) for item in raw_candidates
        if int(item["tmdb_id"]) not in excluded_ids
    ]
    if len(initial_types) == 1 and initial_types[0] in {"tv", "movie"}:
        initial_scored = [item for item in candidates if item.media_type == initial_types[0]]
        if not initial_scored or max(item.confidence for item in initial_scored) < min_confidence or not any(
            item.status == "confirmed" for item in initial_scored
        ):
            try:
                collect_type("movie" if initial_types[0] == "tv" else "tv")
            except ApiError:
                if not initial_scored:
                    raise
            candidates = [
                score(item) for item in raw_candidates
                if int(item["tmdb_id"]) not in excluded_ids
            ]
    return _select_auto_match(
        candidates,
        query_label=evidence.boundary_label,
        year_label=f"query_years={sorted(query_years)}",
    )


__all__ = [
    "AUTO_MATCH_MIN_MARGIN",
    "AutoMatchAmbiguityError",
    "bounded_auto_match_candidate_rows",
    "_extract_year",
    "_normalize_match_title",
    "_title_similarity",
    "_search_query_variants",
    "_clean_boundary_identity_query",
    "_script_evidence_text",
    "_cross_script_unique_match",
    "_search_item_titles",
    "_alternative_tmdb_titles",
    "_query_from_source",
    "_franchise_member_queries",
    "_tmdb_hint_from_source",
    "_direct_tmdb_match",
    "_season_from_source",
    "_explicit_release_season_episode",
    "_usable_release_title_query",
    "_usable_representative_identity_query",
    "_season_from_series_variant",
    "_source_suggests_collection",
    "_source_suggests_batch",
    "_clean_franchise_root_label",
    "_media_type_from_source_context",
    "_source_is_animation_library",
    "_media_context_from_source_and_target",
    "auto_match_tmdb",
    "auto_match_from_evidence",
    "physical_special_candidate_evidence",
]
