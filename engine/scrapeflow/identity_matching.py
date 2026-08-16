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
        # the trace but do not let it push a weak title over the threshold.
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
        status = "rejected" if blockers else "confirmed"
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
    if best.status != "confirmed":
        preview = "; ".join(
            f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%}, {item.status})"
            for item in candidates[:3]
        )
        raise AutoMatchAmbiguityError(
            "自动匹配缺少可验证的标题/别名证据，已拒绝自动选择: " + preview,
            candidates=candidates,
        )
    if "year_conflict" in best.decision_trace.get("blockers", []):
        raise AutoMatchAmbiguityError(
            f"自动匹配候选年份与源目录冲突，拒绝自动选择: "
            f"query_year={query_year}, candidate={best.media_type}/{best.tmdb_id} "
            f"{best.title} ({best.year})",
            candidates=candidates,
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
        and best.confidence - runner_up.confidence + 1e-9
        < AUTO_MATCH_MIN_MARGIN
        and not exact_title_uniquely_identifies_best
    ):
        raise AutoMatchAmbiguityError(
            "自动匹配前两名证据无法区分，拒绝自动选择: "
            + "; ".join(
                f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%})"
                for item in candidates[:2]
            ),
            candidates=candidates,
        )
    return best, candidates


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

    # Gather search queries in prioritized order
    candidate_queries: list[str] = []

    # 1. Combined parent + boundary queries first if parent labels exist (e.g. "Fate Zero", "Fate/Zero")
    for parent in evidence.parent_labels:
        p_clean = parent.strip()
        b_clean = evidence.boundary_label.strip()
        if p_clean and b_clean:
            candidate_queries.append(f"{p_clean} {b_clean}")
            candidate_queries.append(f"{p_clean}/{b_clean}")

    # 2. Direct boundary label
    candidate_queries.append(evidence.boundary_label)

    # 3. Normalized titles and combined with parent
    for t in evidence.normalized_titles:
        for parent in evidence.parent_labels:
            if parent.strip() and t.strip():
                candidate_queries.append(f"{parent.strip()} {t.strip()}")
        candidate_queries.append(t)

    # 4. Aliases and representative names
    for a in evidence.aliases:
        for parent in evidence.parent_labels:
            if parent.strip() and a.strip():
                candidate_queries.append(f"{parent.strip()} {a.strip()}")
        candidate_queries.append(a)
    for r in evidence.representative_names:
        candidate_queries.append(r)

    # Deduplicate while preserving order
    seen_q: set[str] = set()
    search_queries: list[str] = []
    for q in candidate_queries:
        norm_q = q.strip()
        if norm_q and norm_q not in seen_q:
            seen_q.add(norm_q)
            search_queries.append(norm_q)

    expected_episode_count = (
        evidence.episode_pattern.total_episodes
        if evidence.episode_pattern and evidence.episode_pattern.total_episodes > 0 and evidence.media_shape == "tv"
        else None
    )

    query_years = {str(y) for y in evidence.years}
    raw_candidates: list[dict[str, Any]] = []
    searched_types: list[str] = []

    search_query_keys = [_normalize_match_title(sq) for sq in search_queries if _normalize_match_title(sq)]

    def collect_type(candidate_type: str) -> None:
        if candidate_type in searched_types:
            return
        searched_types.append(candidate_type)
        search_items: list[Any] = []
        seen_search_ids: set[int] = set()

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
                response = client.get(f"/search/{candidate_type}", query=variant)
                ingest(response)

        if not search_items and search_queries:
            first_q = search_queries[0]
            response = client.get(f"/search/{candidate_type}", query=first_q, page=1)
            ingest(response)

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

            # Calculate title score against all query variants
            title_score = max(
                (_title_similarity(k, title) for k in search_query_keys for title in titles),
                default=0.0,
            )

            aliases = (
                _alternative_tmdb_titles(client, candidate_type, tmdb_id)
                if index < 5 else []
            )
            alias_score = max(
                (_title_similarity(k, title) for k in search_query_keys for title in aliases),
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
                "cross_script": _cross_script_unique_match(evidence.boundary_label, [*titles, *aliases]),
                "matched_query": evidence.boundary_label,
                "is_animation": is_animation,
                "actual_episode_count": actual_episode_count,
            })

    def score(raw: Mapping[str, Any]) -> AutoMatch:
        title_score = float(raw["title_score"])
        alias_score = float(raw["alias_score"])
        evidence_score = max(title_score, alias_score)

        parent_bonus = 0.0
        candidate_all_titles = [str(t).lower() for t in raw["titles"]] + [str(a).lower() for a in raw["aliases"]]
        for p in evidence.parent_labels:
            p_norm = p.strip().lower()
            if p_norm and any(p_norm in ct for ct in candidate_all_titles):
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
                    cand_y_int = int(candidate_year)
                    deltas = [abs(cand_y_int - int(qy)) for qy in query_years if qy.isdigit()]
                    min_delta = min(deltas) if deltas else 0
                    wrong_year = min_delta >= 2
                    year_score = -0.30 if wrong_year else -0.12
                except ValueError:
                    year_score = -0.06

        media_type_score = 0.0
        context_score = 0.0
        if prefer_animation and raw["media_type"] in {"tv", "movie"}:
            if raw["is_animation"] is True:
                context_score = 0.0
            elif raw["is_animation"] is False:
                context_score = -0.12

        episode_structure_score = 0.0
        actual_count = raw["actual_episode_count"]
        if expected_episode_count and isinstance(actual_count, int):
            episode_structure_score = 0.0 if actual_count == expected_episode_count else -0.08

        confidence = max(0.0, min(1.0, evidence_score + parent_bonus + year_score + media_type_score + context_score + episode_structure_score))
        blockers: list[str] = []
        if wrong_year:
            blockers.append("year_conflict")
        if raw["cross_script"] and alias_score < 0.88:
            blockers.append("cross_script_without_alias_evidence")
        if confidence < min_confidence:
            blockers.append("below_confidence_threshold")

        status = "rejected" if blockers else "confirmed"
        components = {
            "title_score": round(title_score, 6),
            "alias_score": round(alias_score, 6),
            "parent_bonus": round(parent_bonus, 6),
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
                "query": evidence.boundary_label,
                "matched_query_variant": raw.get("matched_query", evidence.boundary_label),
                "query_years": sorted(query_years),
                "official_titles": list(raw["titles"]),
                "aliases_checked": list(raw["aliases"]),
                "blockers": blockers,
                "expected_episode_count": expected_episode_count,
                "actual_episode_count": actual_count,
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

    candidates.sort(key=lambda item: (-item.confidence, item.media_type, item.tmdb_id))
    if not candidates:
        raise PlanError(f"TMDB 未找到自动匹配候选: {evidence.boundary_label}")
    best = candidates[0]
    if best.status != "confirmed":
        preview = "; ".join(
            f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%}, {item.status})"
            for item in candidates[:3]
        )
        raise AutoMatchAmbiguityError(
            "自动匹配缺少可验证的标题/别名证据，已拒绝自动选择: " + preview,
            candidates=candidates,
        )
    if "year_conflict" in best.decision_trace.get("blockers", []):
        raise AutoMatchAmbiguityError(
            f"自动匹配候选年份与源目录冲突，拒绝自动选择: "
            f"query_years={sorted(query_years)}, candidate={best.media_type}/{best.tmdb_id} "
            f"{best.title} ({best.year})",
            candidates=candidates,
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
        and best.confidence - runner_up.confidence + 1e-9
        < AUTO_MATCH_MIN_MARGIN
        and not exact_title_uniquely_identifies_best
    ):
        raise AutoMatchAmbiguityError(
            "自动匹配前两名证据无法区分，拒绝自动选择: "
            + "; ".join(
                f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%})"
                for item in candidates[:2]
            ),
            candidates=candidates,
        )
    return best, candidates


__all__ = [
    "AUTO_MATCH_MIN_MARGIN",
    "AutoMatchAmbiguityError",
    "bounded_auto_match_candidate_rows",
    "_extract_year",
    "_normalize_match_title",
    "_title_similarity",
    "_search_query_variants",
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
    "_season_from_series_variant",
    "_source_suggests_collection",
    "_source_suggests_batch",
    "_clean_franchise_root_label",
    "_media_type_from_source_context",
    "_source_is_animation_library",
    "_media_context_from_source_and_target",
    "auto_match_tmdb",
    "auto_match_from_evidence",
]
