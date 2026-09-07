#!/usr/bin/env python3
"""Replenishment source searchers (extracted 2026-09-07).

Split from ``_replenishment_local_adapter_impl`` along its natural seam: the
per-source search lane implementations — Nyaa/Mikan/DMHY/TokyoTosho/
AnimeTosho/BitSearch/Knaben/ACG/SubsPlease — plus the candidate
construction, gap→file mapping, swarm observation, and manifest-safety
predicates they share.  The impl module remains the facade and re-exports
every exported name, so external ``adapter._symbol`` consumers and the test
suite see no change.  Import direction is strictly downward
(common ← terms ← searchers ← impl facade).
"""

from __future__ import annotations

import base64
import hashlib
import html
import html as html_module
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

from engine.tools.replenishment_common import (
    _AnchorParser,
    _DynamicSearchResult,
    _MAGNET_BOOTSTRAP_TRACKERS,
    _acg_http_opener,
    _bencode,
    _base32_infohash,
    _bounded_seconds,
    _direct_download_env,
    _download_torrent,
    _fetch_bytes,
    _is_ordinary_primary_video_path,
    _is_supplemental_video_path,
    _magnet_metadatas_batch,
    _network_failure_code,
    _nyaa_torrent_mirror_url,
    _pause_checkpoint,
    _safe_infrastructure_failure_types,
    _torrent_manifest,
    ReplenishmentCandidateError,
    ReplenishmentInfrastructureError,
    ReplenishmentPauseRequested,
    MagnetMetadataUnavailable,
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
)
from engine.scrapeflow.provider_capabilities import (
    ACQUISITION_TORRENT,
    PROVIDER_LOCAL_MAGNET,
)
from engine.scrapeflow.replenishment_matching import (
    coverage_tokens as _coverage_tokens,
    normalized_text as _normalized_text,
    expanded_episode_ids as _expanded_episode_ids,
    season_markers as _season_markers,
)
from engine.scrapeflow.media_policy import VIDEO_EXTENSIONS as _VIDEO_EXTENSIONS
from engine.tools.replenishment_search_terms import (
    _DMHY_MAX_QUERY_TERMS,
    _DMHY_MAX_QUERY_TERM_LENGTH,
    _NYAA_MAX_FEED_ROWS,
    _NYAA_MAX_LOGICAL_QUERY_TERMS,
    _NYAA_MAX_MANIFEST_INSPECTIONS,
    _NYAA_MAX_QUERY_TERMS,
    _NYAA_MAX_QUERY_TERM_LENGTH,
    _NYAA_MAX_RSS_ROWS_PER_QUERY,
    _S00_TITLE_PREFLIGHT_GENERIC_KEYS,
    _S00_TITLE_PREFLIGHT_KEY_LIMIT,
    _S00_TITLE_PREFLIGHT_LIMIT,
    _animetosho_release_priority,
    _animetosho_requested_gap_tokens,
    _source_episode_release_priority,
    _nyaa_search_terms,
    _compact_dynamic_search_terms,
    _explicit_episode_search_terms,
    _identity_query_bases,
    _nyaa_logical_search_terms,
    _nyaa_release_priority,
    _nyaa_request_cursor,
    _nyaa_request_fingerprint,
    _nyaa_safe_query_term,
    _optional_episode_title_search_terms,
    _positive_requested_seasons,
    _release_year_conflict,
    _requested_episode_targets,
)

# ---- search-term construction: extracted module, re-exported ----
def _torrent_candidate_variants(
    request: Mapping[str, Any], release_name: str, torrent_url: str,
    manifest: Mapping[str, Any], *, include_local: bool = True,
    swarm: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return only the exact local-Torrent candidate.

    AList's offline-download API cannot carry aria2's ``select-file`` option
    and fetches the torrent URL again after ScrapeFlow has inspected it.  It
    therefore cannot prove that it will acquire only the selected members.
    Automatic replenishment deliberately has no AList projection; the local
    Torrent materializer is the sole torrent lane and passes exact member
    indexes to aria2.
    """
    local = _torrent_candidate(
        request, release_name, torrent_url, manifest, swarm=swarm,
    )
    if local is None:
        return []
    return [local] if include_local else []
def _torrent_candidate(
    request: Mapping[str, Any], release_name: str, torrent_url: str,
    manifest: Mapping[str, Any],
    *,
    swarm: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    gaps = [gap for gap in request.get("gaps") or [] if isinstance(gap, Mapping)]
    has_subtitle_gaps = any(gap.get("kind") == "missing_subtitle" for gap in gaps)
    has_media_gaps = any(gap.get("kind") != "missing_subtitle" for gap in gaps)
    allowed = (
        SUBTITLE_EXTENSIONS if has_subtitle_gaps and not has_media_gaps
        else VIDEO_EXTENSIONS | SUBTITLE_EXTENSIONS if has_subtitle_gaps
        else VIDEO_EXTENSIONS
    )
    if _release_year_conflict(request, release_name):
        return None
    gap_map, file_coverage = _gap_file_map(
        request, release_name, manifest, allowed_payload_extensions=allowed,
    )
    if not gap_map:
        return None
    # A Chinese sidecar for a newly selected media member is optional and
    # separately proven.  It never appears in ``file_index_by_gap`` (that
    # field means an audited gap is directly satisfied), so the runtime can
    # keep companion installation out of the normal missing-subtitle lane.
    companion_map = _new_media_companion_subtitle_map(request, manifest, gap_map)
    indices = sorted({
        index
        for values in [*gap_map.values(), *companion_map.values()]
        for index in values
    })
    files = manifest["files"]
    # Preserve both the complete manifest total and the exact member subset.
    # The local aria2 path receives the subset through ``--select-file``;
    # keeping both receipts prevents a future caller from confusing a small
    # selected-file total with the source torrent's whole-pack size.
    download_bytes = 0
    for index, row in files.items():
        if type(index) is not int or index <= 0 or not isinstance(row, Mapping):
            return None
        size = row.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            return None
        download_bytes += size
    if download_bytes <= 0:
        return None
    selected_download_bytes = sum(int(files[index]["size"]) for index in indices)
    if selected_download_bytes <= 0:
        return None
    candidate_paths = [str(files[index]["path"]) for index in indices]
    quality_text = " ".join([release_name, *candidate_paths[:20]]).casefold()
    resolution = (
        "2160p" if "2160" in quality_text or "4k" in quality_text
        else "1080p" if "1080" in quality_text
        else "720p" if "720" in quality_text else "unknown"
    )
    candidate: dict[str, Any] = {
        "provider": PROVIDER_LOCAL_MAGNET,
        "release_name": release_name,
        "resolution": resolution,
        "availability": "metadata_verified",
        "locator": f"torrent:{torrent_url}",
        "files": candidate_paths,
        "file_coverage": sorted(file_coverage),
        "infohash": manifest["infohash"],
        "acquisition": {
            "kind": ACQUISITION_TORRENT, "url": torrent_url,
            "file_index_by_gap": gap_map,
            "file_size_by_index": {str(index): int(files[index]["size"]) for index in indices},
            "file_path_by_index": {str(index): str(files[index]["path"]) for index in indices},
            "download_bytes": download_bytes,
            "manifest_member_count": len(files),
            "selected_member_count": len(indices),
            "selected_download_bytes": selected_download_bytes,
            **({
                "companion_subtitle_index_by_media_gap": companion_map,
            } if companion_map else {}),
        },
    }
    if isinstance(swarm, Mapping):
        # Optional liveness evidence is additive and never substitutes for
        # the identity/manifest/coverage checks above.
        candidate.update({
            key: swarm[key]
            for key in ("seeders", "leechers")
            if key in swarm
        })
        if "observed_at" in swarm:
            candidate["swarm_observed_at"] = swarm["observed_at"]
    return candidate
def _episode_source_alias_ids(gap: Mapping[str, Any]) -> set[str]:
    """Return canonical source coordinates explicitly declared for one gap."""
    result: set[str] = set()
    for alias in gap.get("source_episode_aliases") or []:
        if not isinstance(alias, Mapping):
            continue
        season = alias.get("season")
        episode = alias.get("episode")
        if type(season) is int and season >= 0 and type(episode) is int and episode > 0:
            result.add(f"S{season:02d}E{episode:02d}")
    return result
def _primary_episode_manifest_member_is_safe(
    gap: Mapping[str, Any], path: str,
) -> bool:
    """Validate one candidate member before binding it to an episode gap.

    The adapter can use a title/semantic alias for Season 00 specials, so an
    explicit source coordinate is not mandatory.  When a member *does* carry
    coordinates, however, it may carry exactly one coordinate and it must be
    the requested coordinate or an explicitly audited source alias.  This
    blocks ranges and cross-season files that happen to contain the requested
    title.  Supplemental paths are always rejected, even when their title
    text matches the official episode title.
    """
    if not _is_ordinary_primary_video_path(path):
        return False
    declared = _expanded_episode_ids(path)
    gap_id = str(gap.get("id") or "")
    if not declared or declared == {gap_id}:
        return True
    return declared <= _episode_source_alias_ids(gap)
def _sanitize_episode_gap_mapping(
    gaps: Iterable[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    mapping: Mapping[str, list[int]],
    file_coverage: Iterable[str],
) -> tuple[dict[str, list[int]], set[str]]:
    """Fail closed for ambiguous primary-video bindings.

    ``_gap_file_map`` has several evidence lanes (canonical coordinates,
    source aliases and Season 00 titles).  They intentionally return all
    matching manifest indices so the caller can audit ambiguity.  Before a
    candidate is serialized, each exact episode gap is reduced only when it
    has one distinct ordinary video; one manifest member may not satisfy two
    different episode gaps.  Invalid rows are removed, allowing a pack to
    remain a safe partial candidate for other independently proven gaps.
    """
    rows = manifest.get("files") if isinstance(manifest.get("files"), Mapping) else {}
    safe = {str(key): list(value) for key, value in mapping.items()}
    coverage = {str(value) for value in file_coverage}
    episode_rows: dict[str, int] = {}
    invalid: set[str] = set()
    for gap in gaps:
        if str(gap.get("kind") or "") != "missing_episode":
            continue
        gap_id = str(gap.get("id") or "")
        if not re.fullmatch(r"S\d{2,3}E\d{2,4}", gap_id):
            continue
        values = safe.get(gap_id)
        if not isinstance(values, list) or len(values) != 1 or type(values[0]) is not int:
            if values is not None:
                invalid.add(gap_id)
            continue
        index = values[0]
        row = rows.get(index)
        path = row.get("path") if isinstance(row, Mapping) else None
        if not isinstance(path, str) or not _primary_episode_manifest_member_is_safe(gap, path):
            invalid.add(gap_id)
            continue
        episode_rows[gap_id] = index

    # A range or a shared source can otherwise be serialized once and replayed
    # against several selected gaps.  Reject every gap involved in a collision.
    owners: dict[int, list[str]] = {}
    for gap_id, index in episode_rows.items():
        owners.setdefault(index, []).append(gap_id)
    for owner_ids in owners.values():
        if len(owner_ids) > 1:
            invalid.update(owner_ids)
    for gap_id in invalid:
        safe.pop(gap_id, None)
        coverage.discard(gap_id)
    return safe, coverage
def _subtitle_path_matches_language(path: str, requested: object) -> bool:
    """Require ordinary release-name language evidence for subtitle gaps.

    This is ordinary release-name filtering. It only stops an
    obvious English/Japanese sidecar being attached to a Chinese-subtitle gap;
    an ambiguous release remains a normal automatic search miss and lets the
    next provider candidate be tried.
    """
    language = str(requested or "").strip().casefold()
    if not language:
        return True
    marker = re.sub(r"[^a-z0-9\u3400-\u9fff]+", " ", path.casefold())
    if any(token in language for token in ("zh", "中文", "chinese", "简", "繁")):
        return any(token in marker for token in ("zh", "zho", "chi", "chs", "cht", "中文", "简", "繁"))
    if any(token in language for token in ("en", "english", "英文", "英语")):
        return any(token in marker for token in (" en ", "eng", "english", "英文", "英语"))
    if any(token in language for token in ("ja", "japanese", "日文", "日语")):
        return any(token in marker for token in (" ja ", "jpn", "japanese", "日文", "日语"))
    return True
_SUBTITLE_LANGUAGE_SUFFIX_RE = re.compile(
    r"(?:zho|chi|chs|cht|zh|eng|jpn|ja|en|中文|简中|簡中|简体|繁体|"
    r"chinese|english|japanese|subtitle|subtitles)$",
    re.I,
)
def _subtitle_path_has_trusted_request_identity(
    path: str, request: Mapping[str, Any] | None,
) -> bool:
    """Require an identity-bearing work alias in a bare-ordinal sidecar path.

    ``Show - 02 [CHS].ass`` is a common subtitle spelling, but the ordinal
    alone is unsafe in a shared request: it could be a sibling franchise or a
    completely unrelated show.  This fallback therefore only accepts a
    request title/alias that is visibly present in the subtitle member's own
    filename.  A containing release folder is not enough: a pack can include
    an unrelated sidecar beneath an otherwise correctly titled root.  Short
    aliases are deliberately ignored unless they contain at least two Han
    characters; generic tokens such as ``86`` or ``SP`` are not trustworthy
    work identity.
    """
    if not isinstance(request, Mapping):
        return False
    media = request.get("media")
    if not isinstance(media, Mapping):
        return False
    aliases = media.get("aliases")
    values = aliases if isinstance(aliases, list) else [media.get("title")]
    normalized_stem = _normalized_text(Path(path).stem)
    if not normalized_stem:
        return False
    for value in values:
        key = _normalized_text(value)
        han_count = sum("\u3400" <= char <= "\u9fff" for char in key)
        if (len(key) >= 4 or han_count >= 2) and key in normalized_stem:
            return True
    return False
def _explicit_subtitle_seasons(value: object) -> set[int]:
    """Return every explicit season claim carried by one provider string.

    ``season_markers`` covers standalone ``S02``/``第2季`` spellings, while
    ``expanded_episode_ids`` covers a canonical coordinate such as
    ``S02E07``.  Treat both as authoritative negative evidence during the
    bare-ordinal fallback: a sidecar cannot silently inherit the audited
    video's season when it already says it belongs to another one.
    """
    text = str(value or "")
    seasons = set(_season_markers(text))
    for episode_id in _expanded_episode_ids(text):
        match = re.fullmatch(r"S(\d{2,3})E\d{2,4}", episode_id)
        if match is not None:
            seasons.add(int(match.group(1)))
    return seasons
def _subtitle_path_matches_audited_video(
    path: str,
    gap: Mapping[str, Any],
    *,
    request: Mapping[str, Any] | None = None,
    release_name: str | None = None,
) -> bool:
    """Return whether one subtitle member identifies the audited video.

    A subtitle candidate is untrusted provider evidence.  Its language marker
    alone is not enough to attach it to a formal-library file: a season pack
    may contain many same-language members.  Prefer the canonical episode
    coordinate when the audited video exposes one.  For a normal TV episode,
    accept a common bare ordinal only when the same subtitle path visibly
    carries a trusted request work identity.  For movie/opaque names, retain
    the conservative stem match after removing one trailing language marker.
    Ambiguous names stay a search miss and are retried through the normal
    subtitle lane.
    """
    if not isinstance(path, str) or not path:
        return False
    video_path = gap.get("path") if isinstance(gap, Mapping) else None
    if not isinstance(video_path, str) or not video_path.startswith("/"):
        return False
    if not _subtitle_path_matches_language(path, gap.get("subtitle_language")):
        return False
    target_episodes = _expanded_episode_ids(video_path)
    member_episodes = _expanded_episode_ids(path)
    if target_episodes:
        # Exact episode identity is required.  A range/whole-season subtitle
        # must not be guessed as the sidecar for one episode.  The narrowly
        # scoped ordinal fallback below deliberately excludes S00 specials,
        # ranges, and members that carry a conflicting explicit coordinate.
        if len(target_episodes) != 1:
            return False
        target = next(iter(target_episodes))
        match = re.fullmatch(r"S(\d{2,3})E\d{2,4}", target)
        if match is None:
            return False
        season = int(match.group(1))
        # A provider path/release may carry ``第2季 ep 7`` rather than a
        # canonical SxxEyy coordinate.  It is still an explicit season
        # assertion and must be allowed only when it agrees with the audited
        # target.  This check is deliberately before the exact-coordinate
        # fast path too: contradictory embedded metadata is unsafe.
        explicit_seasons = _explicit_subtitle_seasons(path)
        if release_name:
            explicit_seasons.update(_explicit_subtitle_seasons(release_name))
        if any(candidate_season != season for candidate_season in explicit_seasons):
            return False
        if target_episodes == member_episodes:
            return True
        if member_episodes or season <= 0:
            return False
        if not _subtitle_path_has_trusted_request_identity(path, request):
            return False
        # ``coverage_tokens`` understands ordinary anime spellings such as
        # ``Show - 02 [CHS].ass`` and ``Show [02][CHS].ass``.  The local
        # adapter also recognizes the common ``Show 02 [CHS].ass`` spelling;
        # mirror that narrow, trailing-ordinal fallback here.  Requiring one
        # and only one coordinate prevents a pack/range from satisfying an
        # individual audited video.
        member_coverage = _coverage_tokens([path], default_seasons={season})
        plain = ANIME_PLAIN_EPISODE_RE.search(Path(path).stem)
        if plain:
            episode = int(plain.group(1))
            if 0 < episode <= 999:
                member_coverage.add(f"S{season:02d}E{episode:02d}")
        return member_coverage == {target}
    target_stem = _normalized_text(Path(video_path).stem)
    member_stem = _normalized_text(Path(path).stem)
    if not target_stem or not member_stem:
        return False
    member_core = _SUBTITLE_LANGUAGE_SUFFIX_RE.sub("", member_stem)
    return bool(member_core) and member_core == target_stem
_COMPANION_LANGUAGE_SUFFIX_RE = re.compile(
    r"(?i)(?:[ ._\-\[\]()]+(?:zh|zho|chi|chs|cht|中文|简中|簡中|"
    r"简体|繁体|繁體|chinese))+$"
)
def _companion_member_core(path: str) -> str:
    """Normalize a provider member stem after only a Chinese suffix removal."""
    return _normalized_text(_COMPANION_LANGUAGE_SUFFIX_RE.sub("", Path(path).stem))
def _companion_members_share_trusted_identity(
    request: Mapping[str, Any], video_path: str, subtitle_path: str,
) -> bool:
    """Require exact stems or a substantial request alias in both members."""
    video_core = _companion_member_core(video_path)
    subtitle_core = _companion_member_core(subtitle_path)
    if video_core and video_core == subtitle_core:
        return True
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    aliases = media.get("aliases") if isinstance(media.get("aliases"), list) else [media.get("title")]
    video_key = _normalized_text(video_path)
    subtitle_key = _normalized_text(subtitle_path)
    for value in aliases:
        alias = _normalized_text(value)
        han_count = sum("\u3400" <= char <= "\u9fff" for char in alias)
        if alias and (len(alias) >= 4 or han_count >= 2) and alias in video_key and alias in subtitle_key:
            return True
    return False
def _new_media_companion_subtitle_map(
    request: Mapping[str, Any], manifest: Mapping[str, Any],
    gap_map: Mapping[str, list[int]],
) -> dict[str, list[int]]:
    """Return only one-to-one Chinese sidecars for selected new media.

    This is intentionally narrower than the ordinary ``missing_subtitle``
    selector.  A companion has no pre-existing formal video to anchor it, so
    it must have an explicit Chinese marker, an identical single episode
    coordinate (when coordinates are present), and exact stem/work identity.
    Ambiguous companions are simply omitted; they never make the video
    candidate unsafe and a later audit can open the dedicated subtitle lane.
    """
    files = manifest.get("files") if isinstance(manifest.get("files"), Mapping) else {}
    gap_rows = {
        str(gap.get("id")): gap
        for gap in request.get("gaps") or []
        if isinstance(gap, Mapping)
        and str(gap.get("kind") or "") != "missing_subtitle"
        and isinstance(gap.get("id"), str) and gap.get("id")
    }
    output: dict[str, list[int]] = {}
    used_indices: set[int] = set()
    for gap_id, gap in gap_rows.items():
        video_indices = gap_map.get(gap_id)
        if not isinstance(video_indices, list) or len(video_indices) != 1:
            continue
        video_index = video_indices[0]
        video_row = files.get(video_index)
        video_path = video_row.get("path") if isinstance(video_row, Mapping) else None
        if type(video_index) is not int or not isinstance(video_path, str):
            continue
        video_ids = _expanded_episode_ids(video_path)
        # A missing episode's video already passed the primary-map guard.  Do
        # not broaden the companion lane to a range or a mismatched source.
        if len(video_ids) > 1:
            continue
        matching: list[int] = []
        for index, row in files.items():
            if type(index) is not int or not isinstance(row, Mapping):
                continue
            path = row.get("path")
            if (
                not isinstance(path, str)
                or Path(path).suffix.casefold() not in SUBTITLE_EXTENSIONS
                or not _subtitle_path_matches_language(path, "zh")
                or _is_supplemental_video_path(path)
            ):
                continue
            subtitle_ids = _expanded_episode_ids(path)
            if video_ids or subtitle_ids:
                if len(video_ids) != 1 or subtitle_ids != video_ids:
                    continue
                # For a canonical audited episode the provider member must
                # explicitly describe that same episode.  Season-00 source
                # aliases intentionally remain companion-free unless their
                # opaque stems match exactly below.
                if re.fullmatch(r"S\d{2,3}E\d{2,4}", gap_id) and video_ids != {gap_id}:
                    continue
            if not _companion_members_share_trusted_identity(request, video_path, path):
                continue
            matching.append(index)
        if len(matching) == 1 and matching[0] not in used_indices:
            output[gap_id] = matching
            used_indices.add(matching[0])
    return output
def _optional_semantic_keys(value: str) -> set[str]:
    keys = {
        f"past:{int(match.group(1))}"
        for match in OPTIONAL_PAST_ARC_RE.finditer(value)
        if 0 < int(match.group(1)) <= 999
    }
    for match in OPTIONAL_NEWLYWED_RE.finditer(value):
        ordinal = int(match.group(1) or 1)
        if 0 < ordinal <= 999:
            keys.add(f"newlywed:{ordinal}")
    return keys
def _infohash_aliases(value: Any) -> set[str]:
    """Return equivalent lowercase hex and base32 torrent infohash forms."""
    raw = str(value or "").strip().casefold()
    if not raw:
        return set()
    aliases = {raw}
    if re.fullmatch(r"[0-9a-f]{40}", raw):
        aliases.add(_base32_infohash(raw))
    elif re.fullmatch(r"[a-z2-7]{32}", raw):
        import base64
        try:
            aliases.add(base64.b32decode(raw.upper()).hex())
        except ValueError:
            pass
    return {item for item in aliases if item}
def _locator_infohash_aliases(values: Iterable[Any]) -> set[str]:
    """Extract normalized BTIH identities carried by persisted locators."""
    aliases: set[str] = set()
    for value in values:
        locator = str(value or "").strip()
        if not locator:
            continue
        prefix, separator, payload = locator.partition(":")
        if prefix.casefold() != "torrent" or not separator:
            continue
        aliases.update(_infohash_aliases(payload))
        match = re.search(
            r"(?i)(?:urn:)?btih:([0-9a-f]{40}|[a-z2-7]{32})\b", payload,
        )
        if match:
            aliases.update(_infohash_aliases(match.group(1)))
    return aliases
def _local_torrent_available(request: Mapping[str, Any]) -> bool:
    """Allow the configured local Torrent search lane."""
    del request
    return True
def _gap_file_map(
    request: Mapping[str, Any], release_name: str, manifest: Mapping[str, Any],
    *, allowed_payload_extensions: frozenset[str] = VIDEO_EXTENSIONS,
) -> tuple[dict[str, list[int]], set[str]]:
    raw_gaps = request.get("gaps")
    if not isinstance(raw_gaps, list) or any(
        not isinstance(gap, Mapping) for gap in raw_gaps
    ):
        return {}, set()
    gaps = [dict(gap) for gap in raw_gaps]
    if any(
        str(gap.get("kind") or "") not in {
            "missing_episode", "missing_season", "missing_media", "missing_subtitle",
        }
        for gap in gaps
    ):
        return {}, set()
    subtitle_gaps = [gap for gap in gaps if gap.get("kind") == "missing_subtitle"]
    if subtitle_gaps:
        if any(gap.get("kind") != "missing_subtitle" for gap in gaps):
            # Explicit subtitle repairs are a sidecar-only provider lane.
            # Companion subtitles remain supported by the media-only branch
            # below, but an audited subtitle gap must never share a torrent
            # manifest or materializer request with video gaps.
            return {}, set()
        # A subtitle gap is an explicit audit coordinate.  It may share one
        # provider request only with other subtitle gaps. Never let a
        # language-only member become a guessed sidecar for another episode.
        subtitle_candidates: dict[str, list[tuple[int, int]]] = {}
        for gap in subtitle_gaps:
            gap_id = str(gap.get("id") or "")
            if not gap_id:
                continue
            matches: list[tuple[int, int]] = []
            for index, row in (manifest.get("files") or {}).items():
                if type(index) is not int or not isinstance(row, Mapping):
                    continue
                path = str(row.get("path") or "").replace("\\", "/")
                size = row.get("size")
                if (
                    not path or Path(path).suffix.casefold() not in SUBTITLE_EXTENSIONS
                    or isinstance(size, bool) or not isinstance(size, int) or size <= 0
                    or not _subtitle_path_matches_audited_video(
                        path,
                        gap,
                        request=request,
                        release_name=release_name,
                    )
                ):
                    continue
                # Prefer an explicitly marked language/episode member, then
                # keep the manifest index as deterministic tie-breaker.
                score = 0
                if _subtitle_path_matches_language(path, gap.get("subtitle_language")):
                    score += 10
                if _expanded_episode_ids(path):
                    score += 5
                matches.append((score, index))
            if matches:
                subtitle_candidates[gap_id] = sorted(matches, key=lambda item: (-item[0], item[1]))

        # Match the most constrained gap first so one broad candidate cannot
        # consume the only member belonging to a narrower target.
        available_indices = {
            index
            for values in subtitle_candidates.values()
            for _score, index in values
        }
        subtitle_mapping: dict[str, list[int]] = {}
        for gap_id in sorted(subtitle_candidates, key=lambda item: (len(subtitle_candidates[item]), item)):
            selected = next(
                (
                    (score, index) for score, index in subtitle_candidates[gap_id]
                    if index in available_indices
                ),
                None,
            )
            if selected is None:
                continue
            _score, index = selected
            subtitle_mapping[gap_id] = [index]
            available_indices.discard(index)

        # Pure subtitle requests remain a subtitle-only materializer lane.
        return subtitle_mapping, set(subtitle_mapping)
    movie_gaps = [gap for gap in gaps if gap.get("kind") == "missing_media"]
    if movie_gaps:
        # The automatic movie lane is deliberately narrow: one known movie
        # root, one largest ordinary video from a verified Torrent manifest.
        # The Engine child still re-identifies and names it after staging.
        # Do not accidentally use this path for a mixed TV request or for
        # extras/sample files that happen to be videos.
        if len(movie_gaps) != len(gaps):
            return {}, set()
        candidates: list[tuple[int, int]] = []
        for index, row in (manifest.get("files") or {}).items():
            if type(index) is not int or not isinstance(row, Mapping):
                continue
            path = str(row.get("path") or "")
            size = row.get("size")
            if (
                not path
                or Path(path).suffix.casefold() not in allowed_payload_extensions
                or isinstance(size, bool)
                or not isinstance(size, int)
                or size <= 0
            ):
                continue
            stem = Path(path).stem
            if re.search(
                r"(?i)(?:^|[ ._\-])(sample|trailer|teaser|featurette|behind[ ._\-]?the[ ._\-]?scenes|menu|pv|cm)(?:$|[ ._\-])",
                stem,
            ):
                continue
            candidates.append((size, index))
        if not candidates:
            return {}, set()
        # A full movie file dominates normal Torrent payloads.  Ties stay
        # deterministic by file index; multipart movies deliberately remain
        # unsupported here rather than guessing which parts form one film.
        _size, selected_index = max(candidates, key=lambda item: (item[0], -item[1]))
        mapping = {
            str(gap.get("id") or ""): [selected_index]
            for gap in movie_gaps
            if str(gap.get("id") or "")
        }
        return mapping, set(mapping)
    optional_discovery = (
        isinstance(request.get("rules"), Mapping)
        and request["rules"].get("optional_discovery_only") is True
    )
    request_seasons = {
        int(gap["season"]) for gap in gaps
        if isinstance(gap.get("season"), int)
        and (int(gap["season"]) > 0 or (optional_discovery and int(gap["season"]) == 0))
    }
    # TMDB Season 00 numbers often do not equal the source release's OVA/OAD
    # ordinal.  For example, TMDB S00E07 may be titled ``Darkness OVA#1``.
    # Treat that explicit title ordinal as an alias only for the named gap;
    # never infer it from a generic query-group marker such as ``OVA``.
    optional_gap_aliases: dict[str, str] = {}
    optional_gap_semantics: dict[str, set[str]] = {}
    optional_gap_official_titles: dict[str, set[str]] = {}
    optional_source_aliases: dict[
        str, list[tuple[str, list[tuple[str, set[str]]]]]
    ] = {}
    # A request label normally has the shape ``<series> S00E##``.  It is a
    # coordinate, not an episode title.  Treating its residual series name as
    # a title causes every member of a complete-series Torrent to become
    # evidence for the one missing special.  Only an explicit episode title
    # (or its authoritative aliases) may power this semantic matching lane.
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    optional_series_identity_titles = {
        normalized
        for value in [
            media.get("title"),
            *(media.get("aliases") if isinstance(media.get("aliases"), list) else []),
        ]
        if isinstance(value, str)
        and (normalized := _normalized_text(value))
    }
    normalized_release = _normalized_text(release_name)
    if optional_discovery:
        for gap in gaps:
            gap_id = str(gap.get("id") or "")
            if not re.fullmatch(r"S00E\d{2,4}", gap_id):
                continue
            descriptors = [
                str(gap.get(key) or "") for key in ("season_name", "label")
                if isinstance(gap.get(key), str)
            ]
            semantic_keys = {
                key for descriptor in descriptors
                for key in _optional_semantic_keys(descriptor)
            }
            if semantic_keys:
                optional_gap_semantics[gap_id] = semantic_keys
            official_titles = {
                normalized
                for descriptor in (
                    str(gap.get("title") or ""),
                    *(
                        str(value) for value in gap.get("title_aliases") or []
                        if isinstance(value, str)
                    ),
                )
                if (normalized := _normalized_text(
                    re.sub(re.escape(gap_id), " ", descriptor, count=1, flags=re.I)
                ))
                and len(normalized) >= 4
                and normalized not in {
                    "特别篇", "特別篇", "special", "specials",
                }
                # A generic series identity is not episode-specific proof,
                # whether it came from a stale label fallback or matches the
                # provider release identity verbatim.
                and normalized not in optional_series_identity_titles
                and normalized != normalized_release
            }
            if official_titles:
                optional_gap_official_titles[gap_id] = official_titles
            for alias in gap.get("source_episode_aliases") or []:
                if not isinstance(alias, Mapping):
                    continue
                source_season = alias.get("season")
                source_episode = alias.get("episode")
                series_titles = [
                    (
                        _normalized_text(value),
                        {
                            word for word in re.findall(
                                r"[a-z0-9]+", value.casefold(),
                            )
                            if len(word) >= 3
                            and word not in {"from", "starting", "season"}
                        },
                    )
                    for value in alias.get("series_titles") or []
                    if isinstance(value, str) and _normalized_text(value)
                ]
                if (
                    type(source_season) is int and source_season > 0
                    and type(source_episode) is int and source_episode > 0
                    and series_titles
                ):
                    optional_source_aliases.setdefault(gap_id, []).append((
                        f"S{source_season:02d}E{source_episode:02d}",
                        series_titles,
                    ))
            source_ordinals = {
                int(match.group(1))
                for descriptor in descriptors
                for match in OPTIONAL_EPISODE_RE.finditer(descriptor)
                if 0 < int(match.group(1)) <= 999
            }
            if len(source_ordinals) == 1:
                optional_gap_aliases[gap_id] = (
                    f"S00E{next(iter(source_ordinals)):02d}"
                )
    release_seasons = _season_markers(release_name)
    semantic_rules: list[tuple[str, int]] = []
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or not isinstance(group.get("season"), int):
            continue
        names = group.get("season_names") if isinstance(group.get("season_names"), list) else []
        semantic_rules.extend(
            (key, int(group["season"])) for name in names
            if (key := _normalized_text(name)) and len(key) >= 2
        )
    if optional_discovery:
        semantic_rules.extend(
            (key, 0)
            for value in _optional_series_title_search_terms(request, maximum=12)
            if (key := _normalized_text(value)) and len(key) >= 4
        )
    semantic_matches = [
        (len(key), season) for key, season in semantic_rules if key in normalized_release
    ]
    longest_semantic = max((length for length, _season in semantic_matches), default=0)
    semantic_seasons = {
        season for length, season in semantic_matches if length == longest_semantic
    }
    if len(release_seasons) == 1:
        default_seasons = release_seasons & request_seasons
    elif len(semantic_seasons) == 1:
        default_seasons = semantic_seasons & request_seasons
    elif len(request_seasons) == 1 and (
        next(iter(request_seasons)) == 1 or request_seasons <= semantic_seasons
    ):
        default_seasons = request_seasons
    else:
        default_seasons = set()
    episode_indices: dict[str, list[int]] = {}
    # Season 00 has no safe request-wide numeric fallback.  Keep intrinsic
    # S00E syntax separate so a release-level ``S00`` marker plus ordinary
    # ``[01]``..``[24]`` members cannot be promoted into arbitrary specials.
    optional_explicit_episode_indices: dict[str, list[int]] = {}
    # Release-local OVA/OAD/SP ordinals are not TMDB Season 00 identities.
    # Keep them separate from canonical SxxEyy tokens so ``OVA1`` cannot
    # silently satisfy TMDB ``S00E01``.  They become eligible only through an
    # explicit ordinal alias present in that exact official gap title.
    optional_ordinal_indices: dict[str, list[int]] = {}
    optional_semantic_indices: dict[str, list[int]] = {}
    optional_official_title_indices: dict[str, list[int]] = {}
    optional_source_alias_indices: dict[str, list[int]] = {}
    manifest_has_optional_container = bool(
        optional_discovery and any(
            isinstance(row, Mapping)
            and OPTIONAL_CONTAINER_RE.search(
                str(row.get("path") or "").replace("\\", "/")
            )
            for row in (manifest.get("files") or {}).values()
        )
    )
    for index, row in (manifest.get("files") or {}).items():
        if type(index) is not int or not isinstance(row, Mapping):
            continue
        path = str(row.get("path") or "")
        normalized_path = path.replace("\\", "/").casefold()
        basename = Path(path).stem
        if (
            Path(path).suffix.casefold() not in allowed_payload_extensions
            or (
                not optional_discovery
                and any(part in normalized_path for part in ("/menu/", "/sps/", "/specials/", "/extras/"))
            )
            or re.search(
                r"(?i)(?:^|[\[\s._-])(NCOP|NCED|MENU|PV|CM|TRAILER)(?:[\]\s._-]|$)",
                basename,
            )
        ):
            continue
        # Release CRC groups such as ``[E6FB5CBC]`` otherwise look like an
        # ``E6`` episode token.  Remove only exact eight-hex groups before
        # extracting episode syntax; retain the original path for verification.
        clean_path = re.sub(r"\[[0-9A-Fa-f]{8}\](?=\.[^.]+$|$)", "", path)
        clean_basename = Path(clean_path).stem
        normalized_file = _normalized_text(clean_path)
        normalized_source_identity = _normalized_text(f"{release_name} {clean_path}")
        source_tokens = _expanded_episode_ids(clean_path) | _coverage_tokens(
            [clean_path], default_seasons=_season_markers(release_name),
        )
        source_words = set(re.findall(r"[a-z0-9]+", f"{release_name} {clean_path}".casefold()))
        for gap_id, aliases in optional_source_aliases.items():
            for source_token, series_titles in aliases:
                if source_token not in source_tokens:
                    continue
                identity_matches = False
                for series_title, title_words in series_titles:
                    if series_title in normalized_source_identity:
                        identity_matches = True
                        break
                    if len(title_words) >= 2 and title_words <= source_words:
                        identity_matches = True
                        break
                if identity_matches:
                    optional_source_alias_indices.setdefault(gap_id, []).append(index)
                    break
        file_in_optional_container = bool(
            optional_discovery
            and OPTIONAL_CONTAINER_RE.search(clean_path.replace("\\", "/"))
        )
        file_optional_semantics = (
            _optional_semantic_keys(clean_path) if optional_discovery else set()
        )
        if (
            optional_discovery and not file_optional_semantics
            and OPTIONAL_PAST_ARC_NAME_RE.search(clean_path)
        ):
            # A file named only OAD02 below a 过去篇 directory may use either
            # the global OAD number or the arc-local number.  Suppress numeric
            # fallback until the path itself states 过去篇02 (or equivalent).
            file_optional_semantics = {"past:ambiguous"}
        file_optional_tokens = {
            f"S00E{int(match.group(1)):02d}"
            for match in OPTIONAL_EPISODE_RE.finditer(clean_basename)
            if 0 < int(match.group(1)) <= 999
        } if optional_discovery and not file_optional_semantics else set()
        has_requested_optional_alias = bool(
            file_optional_tokens & set(optional_gap_aliases.values())
        )
        file_semantic_matches = [
            (len(key), season) for key, season in semantic_rules if key in normalized_file
        ]
        longest_file_semantic = max(
            (length for length, _season in file_semantic_matches), default=0,
        )
        file_semantic_seasons = {
            season for length, season in file_semantic_matches
            if length == longest_file_semantic
        }
        # Keep intrinsic path evidence separate from the requested seasons.
        # Intersecting first used to erase an explicit ``Season 2`` marker for
        # an S01-only request and then silently fall back to that request's
        # unique season.  A multi-season pack consequently mapped both
        # ``Clannad - 20`` and ``Clannad After Story - 20`` to S01E20.
        intrinsic_path_seasons = {
            season for season in _season_markers(clean_path)
            if season > 0 or (optional_discovery and season == 0)
        }
        if has_requested_optional_alias:
            # An OAD may physically live under the source release's ``S3``
            # directory while TMDB owns it in Season 00.  The explicit OVA
            # ordinal from the official gap title is stronger evidence than
            # that packaging parent, and is scoped to this exact request.
            file_default_seasons = {0}
        elif len(intrinsic_path_seasons) == 1:
            explicit_season = intrinsic_path_seasons
            if not explicit_season <= request_seasons:
                continue
            # A longest semantic match is also explicit evidence.  Conflicting
            # path/name identities are unsafe rather than a reason to prefer
            # the single requested season.
            if (
                len(file_semantic_seasons) == 1
                and file_semantic_seasons != explicit_season
            ):
                continue
            file_default_seasons = explicit_season
        elif intrinsic_path_seasons:
            # Multiple season markers in one file path do not identify which
            # season owns a naked episode ordinal.  A unique semantic title may
            # disambiguate it only when it agrees with both the path and the
            # requested season set; otherwise fail closed.
            if (
                len(file_semantic_seasons) != 1
                or not file_semantic_seasons <= intrinsic_path_seasons
                or not file_semantic_seasons <= request_seasons
            ):
                continue
            file_default_seasons = file_semantic_seasons
        elif len(file_semantic_seasons) == 1:
            if not file_semantic_seasons <= request_seasons:
                continue
            file_default_seasons = file_semantic_seasons
        elif file_semantic_seasons:
            # Semantic season evidence exists but is ambiguous.  Do not erase
            # it by degrading to the release/request default.
            continue
        else:
            file_default_seasons = (
                set()
                if (
                    optional_discovery
                    and manifest_has_optional_container
                    and 0 in default_seasons
                    and not file_in_optional_container
                )
                else default_seasons
            )
        explicit_path_tokens = _expanded_episode_ids(clean_path)
        tokens = explicit_path_tokens | _coverage_tokens(
            [clean_path], default_seasons=file_default_seasons,
        )
        if len(file_default_seasons) == 1:
            plain = ANIME_PLAIN_EPISODE_RE.search(clean_basename)
            if plain:
                episode = int(plain.group(1))
                if 0 < episode <= 999:
                    tokens.add(f"S{next(iter(file_default_seasons)):02d}E{episode:02d}")
        if (
            optional_discovery
            and OPTIONAL_RETROSPECTIVE_COLLECTION_RE.search(clean_path)
            and not OPTIONAL_EXPLICIT_S00_RE.search(clean_path)
        ):
            # A retrospective collection commonly numbers its own entries
            # (for example ``精选集03 ... 回想篇第03话``).  Those local
            # ordinals are neither TMDB Season 00 identities nor source OVA
            # ordinals, and must not satisfy a coincidentally numbered S00
            # gap.  Explicit S00 syntax remains authoritative; explicit
            # OVA/OAD/SP ordinals and named official arc semantics are kept in
            # their dedicated maps below.
            tokens = {token for token in tokens if not token.startswith("S00E")}
        for token in tokens:
            episode_indices.setdefault(token, []).append(index)
        if optional_discovery:
            for token in explicit_path_tokens:
                if token.startswith("S00E"):
                    optional_explicit_episode_indices.setdefault(token, []).append(index)
        for token in file_optional_tokens:
            optional_ordinal_indices.setdefault(token, []).append(index)
        for semantic_key in file_optional_semantics:
            optional_semantic_indices.setdefault(semantic_key, []).append(index)
        if optional_discovery:
            for gap_id, official_titles in optional_gap_official_titles.items():
                if any(title in normalized_file for title in official_titles):
                    optional_official_title_indices.setdefault(gap_id, []).append(index)
    mapping: dict[str, list[int]] = {}
    file_coverage: set[str] = set()
    for gap in gaps:
        gap_id = str(gap.get("id") or "")
        if re.fullmatch(r"S\d{2,3}E\d{2,4}", gap_id):
            official_title_indices = set(
                optional_official_title_indices.get(gap_id) or []
            )
            if official_title_indices:
                mapping[gap_id] = sorted(official_title_indices)
                file_coverage.add(gap_id)
                continue
            source_alias_indices = set(
                optional_source_alias_indices.get(gap_id) or []
            )
            if source_alias_indices:
                mapping[gap_id] = sorted(source_alias_indices)
                file_coverage.add(gap_id)
                continue
            gap_semantics = optional_gap_semantics.get(gap_id) or set()
            semantic_indices = {
                index for semantic_key in gap_semantics
                for index in optional_semantic_indices.get(semantic_key) or []
            }
            if semantic_indices:
                mapping[gap_id] = sorted(semantic_indices)
                file_coverage.add(gap_id)
                continue
            if gap_semantics:
                # A named arc in TMDB (for example 过去篇01) must not fall
                # back to a coincidental global OAD number from another arc.
                continue
            direct_indices = set(
                (optional_explicit_episode_indices if optional_discovery else episode_indices)
                .get(gap_id) or []
            )
            alias_id = optional_gap_aliases.get(gap_id)
            alias_indices = (
                set(optional_ordinal_indices.get(alias_id) or [])
                if alias_id else set()
            )
            # If both the TMDB number and explicit source OVA ordinal exist but
            # identify different objects, the share is ambiguous and this gap
            # must remain unresolved.
            if direct_indices and alias_indices and direct_indices != alias_indices:
                continue
            matched_indices = direct_indices or alias_indices
            if matched_indices:
                mapping[gap_id] = sorted(matched_indices)
                file_coverage.add(gap_id)
                continue
        if gap.get("kind") != "missing_season":
            continue
        season = gap.get("season")
        expected = gap.get("expected_episode_count")
        if type(season) is not int or type(expected) is not int or expected <= 0:
            continue
        episode_ids = [f"S{season:02d}E{episode:02d}" for episode in range(1, expected + 1)]
        if all(episode_indices.get(item) for item in episode_ids):
            mapping[gap_id] = sorted({
                index for item in episode_ids for index in episode_indices[item]
            })
            file_coverage.update(episode_ids)
    return _sanitize_episode_gap_mapping(gaps, manifest, mapping, file_coverage)
def _search_nyaa(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    results: dict[str, tuple[str, str, dict[str, Any] | None]] = {}
    query_attempts = 0
    query_responses = 0
    logical_terms = _nyaa_search_terms(
        request, maximum=_NYAA_MAX_LOGICAL_QUERY_TERMS,
    )
    # A full logical-term buffer may be a prefix of a larger malformed or
    # unusually wide identity/gap set.  It is still safe to rotate through
    # that finite buffer, but never sufficient to certify a no-resource
    # result for coordinates that did not fit.
    logical_schedule_truncated = (
        len(logical_terms) >= _NYAA_MAX_LOGICAL_QUERY_TERMS
    )
    fingerprint = _nyaa_request_fingerprint(request, logical_terms)
    cursor = _nyaa_request_cursor(request, fingerprint, len(logical_terms))
    revalidate_exhausted = cursor["exhausted"] is True and bool(logical_terms)
    term_start = (
        max(len(logical_terms) - 1, 0)
        if revalidate_exhausted else int(cursor["term_index"])
    )
    terms = logical_terms[term_start:term_start + _NYAA_MAX_QUERY_TERMS]
    response_window_incomplete = False
    feed_window_incomplete = False
    manifest_window_incomplete = False
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    # Only a successfully parsed, non-covering manifest becomes a durable
    # negative receipt.  Download failures and RSS/metainfo mismatches stay
    # uncached and therefore retryable.
    reviewed_hashes = _locator_infohash_aliases(
        request.get("reviewed_torrent_miss_locators") or [],
    )
    preexcluded_hashes: set[str] = set()
    reviewed_miss_locators: set[str] = set()
    infrastructure_failure_types: dict[str, int] = {}
    infrastructure_failures = 0
    requested_gap_tokens = _animetosho_requested_gap_tokens(request)
    candidate_coverage: set[str] = set()
    partial_candidate_seen = False

    def rss_infohash(item: ET.Element) -> str:
        for child in item:
            if child.tag.rsplit("}", 1)[-1].casefold() == "infohash":
                value = str(child.text or "").strip().casefold()
                if _infohash_aliases(value):
                    return value
        return ""

    def rss_count(item: ET.Element, *names: str) -> int | None:
        wanted = {name.casefold() for name in names}
        for child in item:
            tag = child.tag.rsplit("}", 1)[-1].casefold()
            if tag not in wanted:
                continue
            return _swarm_count(str(child.text or "").strip())
        return None

    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://nyaa.si/?page=rss&c=1_2&f=0&q=" + urllib.parse.quote(term)
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
            root = ET.fromstring(page)
            if root.tag.rsplit("}", 1)[-1].casefold() != "rss":
                raise ValueError("Nyaa RSS 根节点无效")
            channel = next(
                (
                    child for child in root
                    if child.tag.rsplit("}", 1)[-1].casefold() == "channel"
                ),
                None,
            )
            if channel is None:
                raise ValueError("Nyaa RSS 缺少 channel")
        except (OSError, RuntimeError, ValueError, ET.ParseError) as exc:
            infrastructure_failures += 1
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        items = [
            child for child in channel
            if child.tag.rsplit("}", 1)[-1].casefold() == "item"
        ]
        # Nyaa currently returns at most 75 entries and exposes no reliable
        # page/offset receipt.  A full response can therefore be a truncated
        # provider window; inspect it for candidates, but never use it as a
        # no-resource proof.  A provider regression beyond that bound is
        # likewise read only within the finite window below.
        if len(items) >= _NYAA_MAX_RSS_ROWS_PER_QUERY:
            response_window_incomplete = True
        for item in items[:_NYAA_MAX_RSS_ROWS_PER_QUERY]:
            release_name = str(item.findtext("title") or "").strip()
            torrent_url = str(item.findtext("link") or "").strip()
            feed_infohash = rss_infohash(item)
            swarm = _swarm_payload(
                rss_count(item, "seeders", "seeds"),
                rss_count(item, "leechers", "leeches"),
                observed_at=_now_swarm_observation(),
            )
            locator = f"torrent:{torrent_url}"
            aliases = _infohash_aliases(feed_infohash)
            if aliases and aliases & (excluded_hashes | reviewed_hashes):
                preexcluded_hashes.add(feed_infohash)
                continue
            if (
                release_name
                and torrent_url.startswith("https://nyaa.si/download/")
                and locator not in existing_locators
            ):
                results.setdefault(torrent_url, (release_name, feed_infohash, swarm))
        if len(results) > _NYAA_MAX_FEED_ROWS:
            # Keep the globally best bounded window, rather than the first
            # rows received.  This is only an inspection optimization; the
            # dropped rows keep the source explicitly incomplete.
            feed_window_incomplete = True
            retained = sorted(
                results.items(),
                key=lambda row: _nyaa_release_priority(request, row[1][0]),
            )[:_NYAA_MAX_FEED_ROWS]
            results.clear()
            results.update(retained)

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    processed = 0
    ranked_results = sorted(
        results.items(),
        key=lambda item: _nyaa_release_priority(
            request, item[1][0],
        ),
    )
    manifest_results = ranked_results[:_NYAA_MAX_MANIFEST_INSPECTIONS]
    if len(manifest_results) < len(ranked_results):
        manifest_window_incomplete = True
    for position, (torrent_url, (release_name, feed_infohash, swarm)) in enumerate(
        manifest_results,
    ):
        if time.monotonic() >= deadline:
            manifest_window_incomplete = True
            break
        with tempfile.TemporaryDirectory(prefix="scrapeflow-nyaa-") as directory:
            try:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=1,
                )
            except Exception as exc:
                infrastructure_failures += 1
                code = _network_failure_code(exc)
                infrastructure_failure_types[code] = (
                    infrastructure_failure_types.get(code, 0) + 1
                )
                continue
        processed += 1
        manifest_aliases = _infohash_aliases(manifest["infohash"])
        feed_aliases = _infohash_aliases(feed_infohash)
        if (
            manifest_aliases & (excluded_hashes | reviewed_hashes)
            or feed_aliases & (excluded_hashes | reviewed_hashes)
        ):
            continue
        if feed_aliases and not manifest_aliases & feed_aliases:
            # The RSS receipt and downloaded metainfo disagree.  It is not a
            # verified non-covering release, so keep the source fail-closed
            # rather than caching a negative resource result.
            infrastructure_failures += 1
            infrastructure_failure_types["source_error"] = (
                infrastructure_failure_types.get("source_error", 0) + 1
            )
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_available(request),
            swarm=swarm,
        )
        if variants:
            candidates.extend(variants)
            for variant in variants:
                if not isinstance(variant, Mapping):
                    continue
                coverage = variant.get("file_coverage")
                if isinstance(coverage, (list, tuple, set)):
                    candidate_coverage.update(
                        str(value) for value in coverage
                        if isinstance(value, str)
                    )
            if requested_gap_tokens and requested_gap_tokens <= candidate_coverage:
                # Candidate coverage makes further metainfo reads unnecessary
                # for this pass, but it does not prove the provider window
                # exhausted.  Leave source health explicitly incomplete.
                if position + 1 < len(ranked_results):
                    manifest_window_incomplete = True
                break
            # A partial candidate is useful to the selector, but retaining
            # this term window is necessary if its later acquisition fails:
            # unselected siblings from the same RSS evidence remain possible.
            partial_candidate_seen = True
        else:
            miss_locator = f"torrent:{manifest['infohash']}"
            resource_failed_locators.append(miss_locator)
            if manifest_aliases:
                reviewed_miss_locators.add(miss_locator)
                reviewed_hashes.update(manifest_aliases)
    if partial_candidate_seen:
        manifest_window_incomplete = True
    window_fully_reviewed = bool(
        terms
        and query_attempts == len(terms)
        and query_responses == query_attempts
        and not response_window_incomplete
        and not feed_window_incomplete
        and not manifest_window_incomplete
        and processed == len(ranked_results)
        and infrastructure_failures == 0
    )
    next_term_index = (
        min(len(logical_terms), term_start + len(terms))
        if window_fully_reviewed else term_start
    )
    logical_schedule_complete = bool(
        window_fully_reviewed and next_term_index >= len(logical_terms)
    )
    # A capped logical schedule can be retried, but cannot ever claim that
    # omitted terms were searched.  Restart its bounded cycle rather than
    # persisting a misleading ``exhausted`` receipt.
    if logical_schedule_complete and logical_schedule_truncated:
        next_term_index = 0
    cursor_exhausted = bool(
        logical_schedule_complete and not logical_schedule_truncated
    )
    query_cursor = {
        "fingerprint": fingerprint,
        "term_index": next_term_index,
        "page": 1,
        "exhausted": cursor_exhausted,
    }
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            cursor_exhausted
            and window_fully_reviewed
            and not logical_schedule_truncated
        ),
        resource_failed_locators=resource_failed_locators,
        reviewed_torrent_miss_locators=sorted(reviewed_miss_locators),
        infrastructure_failures=infrastructure_failures,
        infrastructure_failure_types=infrastructure_failure_types,
        preexcluded_count=len(preexcluded_hashes),
        query_cursor=query_cursor,
    )
def _mikan_search_terms(request: Mapping[str, Any]) -> list[str]:
    """Prefer broad title terms for Season 00, then verify exact metadata."""
    terms = _compact_dynamic_search_terms(request, maximum=3)
    gaps = [gap for gap in request.get("gaps") or [] if isinstance(gap, Mapping)]
    if not any(gap.get("season") == 0 for gap in gaps):
        return terms
    # Season 00 packs are commonly titled ``Specials`` rather than ``S00``.
    # Keep one bounded, non-numeric official alias in the source query set so
    # a manifest such as ``... 01-23 + Specials`` can still be verified by its
    # exact S00 member paths below.  Candidate selection never trusts this
    # broad query alone.
    bare = _optional_bare_alias_terms(request, maximum=1)
    broad = [
        re.sub(r"\s+S00(?:E\d+(?:-E?\d+)?)?$", "", term, flags=re.I).strip()
        for term in terms
    ]
    return list(dict.fromkeys([*bare, *filter(None, broad), *terms]))[:3]
def _optional_bare_alias_terms(
    request: Mapping[str, Any], *, maximum: int = 2,
) -> list[str]:
    """Return a tiny safe fallback set for Season 00 bundle discovery.

    A bare title is only used for read-only provider discovery.  The normal
    title identity and exact manifest-to-gap checks remain mandatory before a
    download can be selected.  Prefer alphabetic official aliases because
    torrent indexes often use their English/Japanese release title even when
    the local NFO uses a translated title; reject numeric-only aliases such
    as ``86`` because they are too broad for an unattended search.
    """
    if maximum < 1:
        return []
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    raw = media.get("aliases") if isinstance(media.get("aliases"), list) else []
    values = [*raw, media.get("title")]
    ranked: list[tuple[int, int, int, str]] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        if not isinstance(value, str):
            continue
        text = value.strip()
        key = _normalized_text(text)
        if len(key) < 4 or key.isdecimal() or key in seen:
            continue
        seen.add(key)
        normalized = unicodedata.normalize("NFKC", text).casefold()
        # A release-index-friendly Latin alias first; fall back to the local
        # title without inventing any new transliteration.
        latin_rank = 0 if re.search(r"[a-z]", normalized) else 1
        ranked.append((latin_rank, -len(key), index, text))
    return [row[3] for row in sorted(ranked)[:maximum]]
def _optional_series_title_search_terms(
    request: Mapping[str, Any], *, maximum: int = 4,
) -> list[str]:
    """Extract bounded Season 00 sub-series names from exact episode titles."""
    if maximum < 1:
        return []
    titles = [
        str(value).strip()
        for group in request.get("query_groups") or []
        if isinstance(group, Mapping) and group.get("season") == 0
        for value in group.get("episode_titles") or []
        if isinstance(value, str) and value.strip()
    ]
    output: list[str] = []
    seen: set[str] = set()
    for title in titles:
        series = re.sub(
            r"(?i)\s+\d{1,2}(?:st|nd|rd|th)(?:\s+season)?\b.*$",
            "", title,
        ).strip(" -:：")
        if series == title and " - " in title and title.count(":") >= 2:
            series = title.rsplit(":", 1)[0].strip(" -:：")
        variants = [series]
        leading_tag = re.match(r"^[A-Za-z]{1,12}[:：]\s*(.+)$", series)
        if leading_tag:
            # DMHY treats the colon-bearing form as a very broad query for
            # some titles (hundreds of unrelated rows).  The suffix remains
            # an exact substring of release names and is the safer identity.
            variants = [leading_tag.group(1).strip()]
        for value in variants:
            key = re.sub(r"\W+", "", value).casefold()
            if len(key) < 3 or key in seen:
                continue
            seen.add(key)
            output.append(value)
            if len(output) >= maximum:
                return output
    return output
def _specific_s00_title_terms(
    request: Mapping[str, Any], *, maximum: int = 4,
) -> list[str]:
    """Rank specific Season 00 names ahead of generic episode labels.

    AnimeTosho gives the local adapter only a small query budget.  TMDB may
    expose generic labels (``Episode 5``/``第5話``) alongside the actual OVA
    or movie name; spending the budget on those labels hides the release
    title that indexes use.  This helper only changes read-only discovery
    order.  Candidate identity and exact manifest coverage remain mandatory.
    """
    if maximum < 1:
        return []
    raw = _optional_series_title_search_terms(request, maximum=max(16, maximum * 4))
    specific: list[str] = []
    for value in raw:
        normalized = unicodedata.normalize("NFKC", value).strip()
        if re.fullmatch(
            r"(?i)(?:episode|ep\.?|ova|oad|special)\s*\d{1,3}",
            normalized,
        ) or re.fullmatch(r"第\s*\d{1,3}\s*[话話集回]", normalized):
            continue
        specific.append(value)
    # Latin/romanized names are the most common AnimeTosho release spelling;
    # keep all remaining authoritative names as fallback within the bound.
    latin = [value for value in specific if re.search(r"[A-Za-z]", value)]
    non_latin = [value for value in specific if not re.search(r"[A-Za-z]", value)]
    return list(dict.fromkeys([*latin, *non_latin]))[:maximum]
def _broad_identity_alias_terms(
    request: Mapping[str, Any], *, maximum: int = 1,
) -> list[str]:
    """Return a tiny broad fallback made only from confirmed identity names.

    Some indexes (notably AnimeTosho) ignore an ``S01E13`` query even when a
    bare series query returns the corresponding release.  Keep one short
    Latin/romanized TMDB alias available for that case.  The fallback is only
    discovery input: the manifest-to-gap identity and exact episode checks
    still gate every candidate, so a broad result cannot authorize a write.
    """
    if maximum < 1:
        return []
    output: list[str] = []
    seen: set[str] = set()
    for value in _identity_query_bases(request, prefer_latin_aliases=True):
        key = _normalized_text(value)
        if len(key) < 4 or key.isdecimal() or key in seen:
            continue
        seen.add(key)
        output.append(value)
        if len(output) >= maximum:
            break
    return output
def _animetosho_search_terms(
    request: Mapping[str, Any], *, maximum: int = 4,
) -> list[str]:
    """Build AnimeTosho's bounded, exact-first query list."""
    if maximum < 1:
        return []
    source_episode = _source_episode_search_terms(request, maximum=1)
    # The live Gap-ledger bridge carries only C/TMDB aliases and exact gap
    # coordinates, not legacy ``query_groups``.  Reserve one slot for a broad
    # authoritative alias: AnimeTosho can return a release for ``Mashle`` but
    # return nothing for an otherwise precise ``Mashle S01E13`` query.
    broad_alias = _broad_identity_alias_terms(request, maximum=1)
    # A one-slot caller still receives an exact query.  Reserve a broad slot
    # only when the bounded provider window has room for both lanes.
    exact_window = (
        maximum - len(broad_alias)
        if broad_alias and maximum > 1
        else maximum
    )
    exact_episode = _explicit_episode_search_terms(
        request,
        maximum=exact_window,
        prefer_latin_aliases=True,
        interleave_aliases=True,
    )
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    local_title = str(media.get("title") or "").strip()
    local_bare = [local_title] if local_title else []
    focused = _specific_s00_title_terms(request, maximum=maximum)
    gaps = [gap for gap in request.get("gaps") or [] if isinstance(gap, Mapping)]
    optional_bare = (
        _optional_bare_alias_terms(request, maximum=1)
        if any(gap.get("season") == 0 for gap in gaps)
        else []
    )
    priority = list(dict.fromkeys([
        *focused, *source_episode, *exact_episode,
    ]))
    # Keep exact/specific terms first, then force the bounded broad fallback
    # into this same request window even when focused metadata is present.
    terms = priority[:exact_window]
    if len(terms) < maximum:
        terms.extend(value for value in broad_alias if value not in terms)
    terms.extend(value for value in [
        *local_bare, *optional_bare,
        *_compact_dynamic_search_terms(request, maximum=maximum),
    ] if value not in terms)
    return terms[:maximum]
def _dmhy_search_terms(request: Mapping[str, Any]) -> list[str]:
    """Build a bounded DMHY query set from confirmed identity evidence.

    DMHY returns an HTTP error for overlong RSS keywords.  A rejected long
    alias must not consume one of the four query slots: filter each generated
    term first, then continue with shorter aliases and exact Gap coordinates.
    The request's ``media.title``/``media.aliases`` are the only title inputs;
    source paths, web-search labels, and legacy generated query strings are
    deliberately excluded from this provider's search lane.
    """
    maximum = _DMHY_MAX_QUERY_TERMS
    bases = _identity_query_bases(request, prefer_latin_aliases=True)
    seasons = _positive_requested_seasons(request, maximum=8)
    targets = _requested_episode_targets(request)

    # Keep at least one season query whenever a positive season is known, and
    # reserve the remainder for exact gap coordinates.  With two missing
    # episodes this yields two season terms followed by the first two exact
    # terms, while a whole-season gap still receives the full season window.
    exact_reserve = min(
        max(0, maximum - 1),
        len(targets),
    ) if seasons else maximum
    season_window = max(0, maximum - exact_reserve)
    season_terms = [
        f"{base} S{season}"
        for base in bases
        for season in seasons
    ]
    exact_terms = _explicit_episode_search_terms(
        request,
        maximum=max(maximum * 4, maximum),
        prefer_latin_aliases=True,
        # Base-major order fills adjacent requested episodes with the
        # shortest confirmed alias before trying a translated/long alias.
        interleave_aliases=False,
    )

    output: list[str] = []
    seen: set[str] = set()

    def add(values: Iterable[str]) -> None:
        for value in values:
            normalized = _dmhy_safe_query_term(value)
            if normalized is None:
                continue
            key = _normalized_text(normalized)
            if not key or key in seen:
                continue
            seen.add(key)
            output.append(normalized)
            if len(output) >= maximum:
                return

    add(season_terms[:season_window])
    add(exact_terms)
    add(season_terms[season_window:])
    # A bare confirmed alias is a final read-only fallback for a request that
    # has no episode coordinates (or whose exact terms were all too long).
    add(bases)
    return output[:maximum]
def _dmhy_safe_query_term(value: object) -> str | None:
    """Normalize one identity-derived DMHY keyword and reject unsafe length."""
    if not isinstance(value, str):
        return None
    normalized = unicodedata.normalize("NFKC", value)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized or len(normalized) > _DMHY_MAX_QUERY_TERM_LENGTH:
        return None
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in normalized):
        return None
    return normalized
def _source_episode_search_terms(
    request: Mapping[str, Any], *, maximum: int = 2,
) -> list[str]:
    """Build precise release-season terms from verified local aliases."""
    output: list[str] = []
    for gap in request.get("gaps") or []:
        if not isinstance(gap, Mapping):
            continue
        for alias in gap.get("source_episode_aliases") or []:
            if not isinstance(alias, Mapping) or type(alias.get("season")) is not int:
                continue
            season = int(alias["season"])
            if season <= 0:
                continue
            for title in alias.get("series_titles") or []:
                if not isinstance(title, str):
                    continue
                words = [
                    word for word in re.findall(r"[A-Za-z0-9]+", title)
                    if word.casefold() not in {"from", "starting", "season"}
                ]
                compact = " ".join(dict.fromkeys(words)).strip()
                if len(words) < 3 or not compact:
                    continue
                term = f"{compact} S{season}"
                if term not in output:
                    output.append(term)
                    if len(output) >= maximum:
                        return output
    return output
def _verified_s00_title_preflight_keys(
    request: Mapping[str, Any], *, maximum: int = 8,
) -> tuple[str, ...]:
    """Return bounded exact S00 title evidence for raw-result ordering.

    The adapter sees this evidence only after the audit has produced an
    ``optional_discovery_only`` request.  In particular, the small S00 movie
    enrichment path has already bound its aliases to the requested TV TMDB
    identity.  Keep the raw-result use deliberately narrower than candidate
    acceptance: titles here merely choose which Torrent metainfo to inspect
    first; identity, manifest and coverage checks remain below that boundary.
    """
    if maximum < 1:
        return ()
    rules = request.get("rules")
    if not (
        isinstance(rules, Mapping)
        and rules.get("optional_discovery_only") is True
    ):
        return ()

    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    identity_keys = {
        _normalized_text(value).replace("_", "")
        for value in [
            media.get("title"),
            *(media.get("aliases") if isinstance(media.get("aliases"), list) else []),
        ]
        if isinstance(value, str) and _normalized_text(value)
    }
    output: list[str] = []
    seen: set[str] = set()
    for gap in request.get("gaps") or []:
        if not (
            isinstance(gap, Mapping)
            and gap.get("kind") == "missing_episode"
            and gap.get("season") == 0
        ):
            continue
        values = [
            gap.get("title"),
            *(gap.get("title_aliases") if isinstance(gap.get("title_aliases"), list) else []),
        ]
        for value in values:
            if not isinstance(value, str):
                continue
            key = _normalized_text(value).replace("_", "")
            if (
                len(key) < 4
                or len(key) > _S00_TITLE_PREFLIGHT_KEY_LIMIT
                or key in seen
                or key in identity_keys
                or key in _S00_TITLE_PREFLIGHT_GENERIC_KEYS
                or re.fullmatch(
                    r"(?i)(?:episode|ep|ova|oad|special|sp)\d{0,3}", key,
                )
                or re.fullmatch(r"第\d{1,3}[话話集回]", key)
            ):
                continue
            seen.add(key)
            output.append(key)
            if len(output) >= maximum:
                return tuple(output)
    return tuple(output)
def _prioritize_verified_s00_title_rows(
    request: Mapping[str, Any], rows: Iterable[Any], *,
    release_name: Callable[[Any], Any],
    maximum: int = _S00_TITLE_PREFLIGHT_LIMIT,
) -> list[Any]:
    """Promote only a few exact S00-title raw rows before preflight.

    Provider feeds can return 32 superficially valid rows.  Downloading the
    first 28 Torrent metainfo files can consume the shared 45-second deadline
    before the verified movie-name row is reached.  This is an ordering-only
    optimization: it neither manufactures a candidate nor relaxes the later
    identity, file-manifest, or gap-coverage gates.  Rows not promoted retain
    their original source order and all rows remain subject to the existing
    global raw-result cap.
    """
    ordered = list(rows)
    if maximum < 1 or not ordered:
        return ordered
    title_keys = _verified_s00_title_preflight_keys(request)
    if not title_keys:
        return ordered

    scored: list[tuple[int, int]] = []
    for index, row in enumerate(ordered):
        normalized_release = _normalized_text(release_name(row)).replace("_", "")
        strength = max(
            (len(key) for key in title_keys if key in normalized_release),
            default=0,
        )
        if strength:
            scored.append((-strength, index))
    if not scored:
        return ordered

    selected = sorted(scored)[:maximum]
    promoted = [ordered[index] for _strength, index in selected]
    promoted_indexes = {index for _strength, index in selected}
    return [
        *promoted,
        *(row for index, row in enumerate(ordered) if index not in promoted_indexes),
    ]
def _swarm_count(value: Any) -> int | None:
    """Normalize a public seed/leecher count without coercing junk."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 <= value <= 1_000_000_000 else None
    if isinstance(value, str) and re.fullmatch(r"\d{1,10}", value.strip()):
        parsed = int(value.strip())
        return parsed if parsed <= 1_000_000_000 else None
    return None
def _swarm_epoch_iso(value: Any) -> str:
    if isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        if value != value or value <= 0 or value > 4_102_444_800:
            return ""
        return datetime.fromtimestamp(value, timezone.utc).isoformat().replace(
            "+00:00", "Z",
        )
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if re.fullmatch(r"\d{1,10}", text):
            return _swarm_epoch_iso(int(text))
        return text
    return ""
def _swarm_payload(
    seeders: Any,
    leechers: Any = None,
    *,
    observed_at: Any,
) -> dict[str, Any] | None:
    seeds = _swarm_count(seeders)
    observed = _swarm_epoch_iso(observed_at)
    # An explicit observation time is required.  Missing/invalid timestamps
    # remain neutral in selector ranking rather than being guessed fresh.
    if seeds is None or not observed:
        return None
    payload: dict[str, Any] = {"seeders": seeds, "observed_at": observed}
    parsed_leechers = _swarm_count(leechers)
    if parsed_leechers is not None:
        payload["leechers"] = parsed_leechers
    return payload
def _now_swarm_observation() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
def _search_mikan(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    """Search Mikan's public RSS and validate every selected Torrent manifest."""
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    results: dict[str, str] = {}
    query_attempts = 0
    query_responses = 0
    terms = _mikan_search_terms(request)
    hit_cap = False
    preexcluded = 0
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    requested_seasons = {
        int(gap["season"]) for gap in request.get("gaps") or []
        if isinstance(gap, Mapping) and isinstance(gap.get("season"), int)
    }
    resource_failed_locators: set[str] = set()
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = (
            "https://mikanani.me/RSS/Search?searchstr="
            + urllib.parse.quote(term)
        )
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
            root = ET.fromstring(page)
        except (OSError, RuntimeError, ValueError, ET.ParseError):
            continue
        query_responses += 1
        for item in root.findall("./channel/item"):
            release_name = str(item.findtext("title") or "").strip()
            enclosure = item.find("enclosure")
            torrent_url = (
                str(enclosure.attrib.get("url") or "").strip()
                if enclosure is not None else ""
            )
            parsed = urllib.parse.urlsplit(torrent_url)
            locator = f"torrent:{torrent_url}"
            if not (
                release_name
                and parsed.scheme == "https"
                and parsed.hostname == "mikanani.me"
                and parsed.username is None and parsed.password is None
                and parsed.path.startswith("/Download/")
                and parsed.path.casefold().endswith(".torrent")
                and not parsed.query and not parsed.fragment
            ):
                continue
            if locator in existing_locators:
                preexcluded += 1
                continue
            release_seasons = _season_markers(release_name)
            if (
                requested_seasons and release_seasons
                and release_seasons.isdisjoint(requested_seasons)
            ):
                resource_failed_locators.add(locator)
                continue
            results.setdefault(torrent_url, release_name)
            if len(results) >= 32:
                hit_cap = True
                break
        if hit_cap:
            break

    candidates: list[dict[str, Any]] = []
    infrastructure_failures = 0
    processed = 0
    for torrent_url, release_name in results.items():
        if time.monotonic() >= deadline:
            break
        with tempfile.TemporaryDirectory(prefix="scrapeflow-mikan-") as directory:
            try:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=1,
                )
            except Exception:
                infrastructure_failures += 1
                continue
        processed += 1
        if _infohash_aliases(manifest["infohash"]) & excluded_hashes:
            resource_failed_locators.add(f"torrent:{torrent_url}")
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_available(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.update({
                f"torrent:{manifest['infohash']}",
                f"torrent:{torrent_url}",
            })
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms)
            and query_responses == query_attempts and not hit_cap
            and processed == len(results) and infrastructure_failures == 0
        ),
        resource_failed_locators=sorted(resource_failed_locators),
        infrastructure_failures=infrastructure_failures,
        preexcluded_count=preexcluded,
    )
def _search_dmhy(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    """Search DMHY's public RSS and verify its per-release Torrent metainfo."""
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    def official_detail_url(value: str) -> str:
        parsed = urllib.parse.urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname != "share.dmhy.org"
            or parsed.username is not None or parsed.password is not None
            or parsed.port is not None
            or not parsed.path.startswith("/topics/view/")
            or not parsed.path.casefold().endswith(".html")
            or parsed.query or parsed.fragment
        ):
            return ""
        return urllib.parse.urlunsplit(
            ("https", "share.dmhy.org", parsed.path, "", "")
        )

    def official_torrent_url(detail_url: str, href: str) -> str:
        absolute = urllib.parse.urljoin(detail_url, href)
        parsed = urllib.parse.urlsplit(absolute)
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname != "dl.dmhy.org"
            or parsed.username is not None or parsed.password is not None
            or parsed.port is not None
            or not parsed.path.casefold().endswith(".torrent")
            or parsed.query or parsed.fragment
        ):
            return ""
        return urllib.parse.urlunsplit(
            ("https", "dl.dmhy.org", parsed.path, "", "")
        )

    terms = _dmhy_search_terms(request)
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    results: dict[str, tuple[str, str]] = {}
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    preexcluded_hashes: set[str] = set()
    infrastructure_failure_types: dict[str, int] = {}
    infrastructure_failures = 0

    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://share.dmhy.org/topics/rss/rss.xml?" + urllib.parse.urlencode({
            "keyword": term,
        })
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
            root = ET.fromstring(page)
        except (OSError, RuntimeError, ValueError, ET.ParseError) as exc:
            infrastructure_failures += 1
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        for item in root.findall("./channel/item"):
            release_name = str(item.findtext("title") or "").strip()
            detail_url = official_detail_url(str(item.findtext("link") or "").strip())
            enclosure = item.find("enclosure")
            magnet_url = (
                str(enclosure.attrib.get("url") or "").strip()
                if enclosure is not None else ""
            )
            match = re.search(
                r"(?i)(?:urn:)?btih:([0-9a-f]{40}|[a-z2-7]{32})\b",
                magnet_url,
            )
            feed_infohash = match.group(1).casefold() if match else ""
            aliases = _infohash_aliases(feed_infohash)
            if aliases and aliases & excluded_hashes:
                preexcluded_hashes.add(feed_infohash)
                continue
            if not release_name or not detail_url:
                continue
            if detail_url not in results and len(results) >= 32:
                hit_cap = True
                break
            results.setdefault(detail_url, (release_name, feed_infohash))
        if hit_cap:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    processed = 0
    ranked_results = _prioritize_verified_s00_title_rows(
        request, results.items(),
        release_name=lambda item: item[1][0],
    )
    for detail_url, (release_name, feed_infohash) in ranked_results:
        if time.monotonic() >= deadline:
            break
        try:
            detail_page = _fetch_bytes(
                detail_url, max_bytes=2 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
            parser = _AnchorParser()
            parser.feed(detail_page.decode("utf-8", "replace"))
            torrent_urls = sorted({
                url for anchor in parser.anchors
                if (url := official_torrent_url(
                    detail_url, str(anchor.get("href") or "").strip(),
                ))
            })
            if len(torrent_urls) != 1:
                raise ValueError("DMHY detail page lacks one verified Torrent link")
            torrent_url = torrent_urls[0]
            with tempfile.TemporaryDirectory(prefix="scrapeflow-dmhy-") as directory:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=1,
                )
        except (OSError, RuntimeError, ValueError, ET.ParseError) as exc:
            infrastructure_failures += 1
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        processed += 1
        manifest_aliases = _infohash_aliases(manifest["infohash"])
        feed_aliases = _infohash_aliases(feed_infohash)
        if (
            manifest_aliases & excluded_hashes
            or (feed_aliases and not manifest_aliases & feed_aliases)
        ):
            resource_failed_locators.append(
                f"torrent:{manifest['infohash']}"
            )
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_available(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.append(
                f"torrent:{manifest['infohash']}"
            )
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms)
            and query_responses == query_attempts and not hit_cap
            and processed == len(results) and infrastructure_failures == 0
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=infrastructure_failures,
        infrastructure_failure_types=infrastructure_failure_types,
        preexcluded_count=len(preexcluded_hashes),
    )
def _search_tokyotosho(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    """Search Tokyo Toshokan's official HTML results without browser UI.

    TokyoTosho exposes a magnet immediately before the corresponding Torrent
    link.  The BTIH is used to discard already-seen releases before the
    32-row processing cap, so old releases cannot occupy every search round.
    """
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    base_url = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_URL",
        "https://tokyo-tosho.net",
    ).strip().rstrip("/")
    parsed_base = urllib.parse.urlsplit(base_url)
    if (
        parsed_base.scheme != "https"
        or parsed_base.hostname not in {
            "tokyo-tosho.net", "tokyotosho.info", "www.tokyotosho.info",
            "tokyotosho.se", "www.tokyotosho.se",
        }
        or parsed_base.username is not None or parsed_base.password is not None
        or parsed_base.port is not None
        or parsed_base.path not in {"", "/"}
        or parsed_base.query or parsed_base.fragment
    ):
        raise ValueError(
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_URL must be an official HTTPS origin"
        )

    # TokyoTosho rows are often indexed as ``S4 - 17`` (or just ``S4`` with
    # the episode in the release title), while exact ``S04E17`` queries can
    # miss the active SubsPlease row.  Keep two exact terms, then add the
    # bounded season aliases used by DMHY.  Manifest identity/coverage checks
    # remain mandatory, so broad discovery cannot authorize a wrong release.
    terms = list(dict.fromkeys([
        *_compact_dynamic_search_terms(request, maximum=4)[:2],
        *_dmhy_search_terms(request),
    ]))[:6]
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    results: dict[str, tuple[str, str]] = {}
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    preexcluded_hashes: set[str] = set()
    infrastructure_failure_types: dict[str, int] = {}

    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = base_url + "/search.php?" + urllib.parse.urlencode({
            "terms": term, "searchName": "true", "searchComment": "true",
        })
        parser = _AnchorParser()
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=8 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        parser.feed(page.decode("utf-8", "replace"))
        pending_infohash = ""
        for anchor in parser.anchors:
            href = str(anchor.get("href") or "").strip()
            if href.casefold().startswith("magnet:"):
                match = re.search(
                    r"(?i)(?:urn:)?btih:([0-9a-f]{40}|[a-z2-7]{32})\b", href,
                )
                pending_infohash = match.group(1).casefold() if match else ""
                continue
            absolute_url = urllib.parse.urljoin(base_url + "/", href)
            parsed_url = urllib.parse.urlsplit(absolute_url)
            nyaa_mirror_url = _nyaa_torrent_mirror_url(absolute_url)
            if (
                not (
                    parsed_url.scheme == "https"
                    and parsed_url.hostname
                    and parsed_url.path.casefold().endswith(".torrent")
                )
                and nyaa_mirror_url is None
            ):
                continue
            release_name = str(anchor.get("text") or "").strip()
            infohash = pending_infohash
            pending_infohash = ""
            aliases = _infohash_aliases(infohash)
            # A TokyoTosho row gives us an adjacent BTIH.  Do not turn an
            # otherwise ordinary web link into an acquisition candidate unless
            # that cryptographic identity was present in the feed.
            if not aliases:
                continue
            locator = f"torrent:{absolute_url}"
            if aliases and aliases & excluded_hashes:
                preexcluded_hashes.add(infohash)
                continue
            if locator in existing_locators or not release_name:
                continue
            if absolute_url not in results and len(results) >= 32:
                hit_cap = True
                break
            results.setdefault(absolute_url, (release_name, infohash))
        if hit_cap:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    processed = 0
    ranked_results = sorted(
        results.items(),
        key=lambda item: _source_episode_release_priority(
            request, item[1][0],
        ),
    )
    for torrent_url, (release_name, feed_infohash) in ranked_results:
        if time.monotonic() >= deadline:
            break
        processed += 1
        with tempfile.TemporaryDirectory(prefix="scrapeflow-tokyotosho-") as directory:
            try:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=1,
                )
            except Exception:
                aliases = _infohash_aliases(feed_infohash)
                resource_failed_locators.append(
                    f"torrent:{sorted(aliases)[0]}"
                    if aliases else f"torrent:{torrent_url}"
                )
                continue
        manifest_aliases = _infohash_aliases(manifest["infohash"])
        expected_aliases = _infohash_aliases(feed_infohash)
        if not expected_aliases or not (manifest_aliases & expected_aliases):
            # The feed BTIH and fetched metainfo must agree.  This also makes
            # the narrowly-scoped Nyaa transport fallback safe: it may only
            # supply the exact torrent the TokyoTosho row advertised.
            resource_failed_locators.append(
                f"torrent:{sorted(expected_aliases)[0]}"
                if expected_aliases else f"torrent:{torrent_url}"
            )
            continue
        if manifest_aliases & excluded_hashes:
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_available(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.append(
                f"torrent:{manifest['infohash']}"
            )
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms) and query_responses == query_attempts
            and not hit_cap and processed == len(results)
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=0,
        infrastructure_failure_types=infrastructure_failure_types,
        preexcluded_count=len(preexcluded_hashes),
    )
_ANIMETOSHO_MAX_PAGES_PER_RUN = 4
_ANIMETOSHO_MAX_PAGE = 256
_ANIMETOSHO_MAX_ROWS_PER_PAGE = 512
def _animetosho_request_fingerprint(
    request: Mapping[str, Any], terms: Sequence[str],
) -> str:
    """Fingerprint only the confirmed identity and exact open-gap query.

    The feed cursor is deliberately tied to the request that produced it.
    Directory names, provider URLs and web-search titles never enter this
    payload.  If TMDB identity, aliases, coordinates, or the deterministic
    term list changes, a stale page cursor is ignored and discovery restarts
    from page one.
    """
    media = request.get("media")
    media = media if isinstance(media, Mapping) else {}
    media_payload = {
        "media_type": media.get("media_type"),
        "tmdb_id": media.get("tmdb_id"),
        "title": media.get("title"),
        "original_title": media.get("original_title"),
        "aliases": [
            value for value in (media.get("aliases") or [])
            if isinstance(value, str)
        ][:40],
    }
    gap_payload: list[dict[str, Any]] = []
    for raw in request.get("gaps") or []:
        if not isinstance(raw, Mapping):
            continue
        episodes = sorted({
            int(value) for value in (raw.get("episodes") or [])
            if type(value) is int and value > 0
        })
        gap_payload.append({
            "id": raw.get("id"),
            "kind": raw.get("kind"),
            "season": raw.get("season"),
            "episodes": episodes,
        })
    payload = {
        "provider": "animetosho",
        "media": media_payload,
        "gaps": sorted(gap_payload, key=lambda row: (
            str(row.get("id") or ""), str(row.get("kind") or ""),
            int(row.get("season")) if type(row.get("season")) is int else -1,
            row.get("episodes") or [],
        )),
        "terms": list(terms),
    }
    encoded = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
def _animetosho_request_cursor(
    request: Mapping[str, Any], fingerprint: str, term_count: int,
) -> dict[str, Any]:
    """Read a bounded source cursor; malformed/mismatched state restarts."""
    raw: object = None
    cursors = request.get("search_cursors")
    if isinstance(cursors, Mapping):
        for key, value in cursors.items():
            normalized = re.sub(r"[^a-z0-9]+", "", str(key).casefold())
            if normalized == "animetosho":
                raw = value
                break
    if raw is None:
        raw = request.get("animetosho_query_cursor")
    if not isinstance(raw, Mapping):
        return {"fingerprint": fingerprint, "term_index": 0, "page": 1, "exhausted": False}
    raw_fingerprint = raw.get("fingerprint")
    term_index = raw.get("term_index")
    page = raw.get("page")
    exhausted = raw.get("exhausted")
    if (
        raw_fingerprint != fingerprint
        or not isinstance(raw_fingerprint, str)
        or not re.fullmatch(r"[a-f0-9]{64}", raw_fingerprint)
        or type(term_index) is not int
        or term_index < 0
        or term_index > max(term_count, 0)
        or type(page) is not int
        or page < 1
        or page > _ANIMETOSHO_MAX_PAGE
        or type(exhausted) is not bool
    ):
        return {"fingerprint": fingerprint, "term_index": 0, "page": 1, "exhausted": False}
    return {
        "fingerprint": fingerprint,
        "term_index": term_index,
        "page": page,
        "exhausted": exhausted,
    }
def _search_animetosho(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    """Search AnimeTosho's JSON feed with a bounded page continuation.

    AnimeTosho returns up to 75 rows for each ``page``.  The old adapter
    stopped after the first 32 unique rows, which made broad aliases (for
    example ``Mashle``) permanently miss later pages.  We inspect a bounded
    number of complete pages per run and persist the next exact page cursor;
    a page is advanced only after every manifest on it was either safely
    excluded or validated as non-covering.  HTTP/JSON/metainfo failures keep
    the cursor on that page and mark the source infrastructure-incomplete.
    """
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    # Keep the existing exact-first terms, then append confirmed bare aliases
    # so a broad release spelling is reached by later cursor windows without
    # using a directory name or a web-search result.
    terms = list(_animetosho_search_terms(request, maximum=4))
    for value in _identity_query_bases(request, prefer_latin_aliases=True):
        if value and value not in terms:
            terms.append(value)
        if len(terms) >= 8:
            break
    fingerprint = _animetosho_request_fingerprint(request, terms)
    cursor = _animetosho_request_cursor(request, fingerprint, len(terms))
    revalidate_exhausted = cursor.get("exhausted") is True and bool(terms)
    # A completed receipt is revalidated with the final term's last empty
    # page instead of fabricating query counters.  If the provider gained a
    # new row since the previous pass, discovery resumes from that page; if
    # it remains empty, the new run has truthful query/response evidence.
    term_index = (
        max(len(terms) - 1, 0) if revalidate_exhausted
        else int(cursor["term_index"])
    )
    page = int(cursor["page"])

    existing_hashes = _locator_infohash_aliases(existing_locators)
    reviewed_hashes = _locator_infohash_aliases(
        request.get("reviewed_torrent_miss_locators") or [],
    )
    results: dict[str, tuple[str, str, dict[str, Any] | None]] = {}
    candidates: list[dict[str, Any]] = []
    requested_gap_tokens = _animetosho_requested_gap_tokens(request)
    candidate_coverage: set[str] = set()
    partial_candidate_seen = False
    resource_failed_locators: list[str] = []
    reviewed_miss_locators: set[str] = set()
    query_attempts = 0
    query_responses = 0
    infrastructure_failures = 0
    pages_completed = 0
    cursor_blocked = False
    last_empty_page: int | None = None

    while (
        term_index < len(terms)
        and pages_completed < _ANIMETOSHO_MAX_PAGES_PER_RUN
        and time.monotonic() < deadline
    ):
        term = terms[term_index]
        url = "https://feed.animetosho.org/json?" + urllib.parse.urlencode({
            "q": term,
            "page": page,
        })
        query_attempts += 1
        try:
            raw_payload = _fetch_bytes(
                url, max_bytes=8 * 1024 * 1024,
                timeout=request_timeout(), attempts=2,
            )
            payload = json.loads(raw_payload)
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            # A page that cannot be fetched or parsed is not an empty page.
            # Leave the exact cursor in place and force a same-tier retry.
            infrastructure_failures += 1
            cursor_blocked = True
            break
        if not isinstance(payload, list) or len(payload) > _ANIMETOSHO_MAX_ROWS_PER_PAGE:
            infrastructure_failures += 1
            cursor_blocked = True
            break
        query_responses += 1

        page_results: dict[str, tuple[str, str, dict[str, Any] | None]] = {}
        for row in payload:
            if not isinstance(row, Mapping):
                continue
            release_name = str(row.get("title") or "").strip()
            torrent_url = str(row.get("torrent_url") or "").strip()
            infohash = str(row.get("info_hash") or "").strip().casefold()
            feed_hashes = _infohash_aliases(infohash)
            swarm = _swarm_payload(
                row.get("seeders"),
                row.get("leechers"),
                # ``tracker_updated`` is the only explicit swarm observation
                # time in this feed; publication time is not a liveness fact.
                observed_at=row.get("tracker_updated"),
            )
            locator = f"torrent:{torrent_url}"
            if (
                release_name
                and torrent_url.startswith("https://storage.animetosho.org/torrent/")
                and locator not in existing_locators
                and not (feed_hashes & (existing_hashes | reviewed_hashes))
            ):
                page_results.setdefault(torrent_url, (release_name, infohash, swarm))

        ranked_results = sorted(
            page_results.items(),
            key=lambda item: _animetosho_release_priority(
                request, item[1][0],
            ),
        )
        ranked_results = _prioritize_verified_s00_title_rows(
            request, ranked_results,
            release_name=lambda item: item[1][0],
        )
        page_failed = False
        for torrent_url, (release_name, feed_infohash, swarm) in ranked_results:
            if time.monotonic() >= deadline:
                # The page was only partially reviewed.  Do not advance it;
                # successful misses collected above remain safe to persist.
                page_failed = True
                break
            feed_hashes = _infohash_aliases(feed_infohash)
            if feed_hashes & (existing_hashes | reviewed_hashes):
                continue
            with tempfile.TemporaryDirectory(prefix="scrapeflow-animetosho-") as directory:
                try:
                    manifest = _download_torrent(
                        torrent_url, Path(directory) / "candidate.torrent",
                        timeout=request_timeout(), attempts=2,
                    )
                except Exception:
                    infrastructure_failures += 1
                    page_failed = True
                    continue
            manifest_hashes = _infohash_aliases(manifest.get("infohash"))
            # If the feed advertises a hash, the fetched metainfo must agree.
            # A mismatch is not a proven non-covering candidate and therefore
            # must remain an infrastructure failure rather than being cached.
            if feed_hashes and not (feed_hashes & manifest_hashes):
                infrastructure_failures += 1
                page_failed = True
                continue
            if manifest_hashes & (existing_hashes | reviewed_hashes):
                continue
            variants = _torrent_candidate_variants(
                request, release_name, torrent_url, manifest,
                include_local=_local_torrent_available(request),
                swarm=swarm,
            )
            if variants:
                candidates.extend(variants)
                for variant in variants:
                    if not isinstance(variant, Mapping):
                        continue
                    coverage = variant.get("file_coverage")
                    if isinstance(coverage, (list, tuple, set)):
                        candidate_coverage.update(
                            str(value) for value in coverage if isinstance(value, str)
                        )
                if requested_gap_tokens and requested_gap_tokens <= candidate_coverage:
                    # Stop only once the validated candidate union covers all
                    # current coordinates.  The page is intentionally left
                    # blocked: it was not fully reviewed, so its cursor must
                    # not advance or be marked exhausted.
                    page_failed = True
                    break
                # A partial candidate is useful to the selector even when it
                # cannot close every gap.  Keep the page cursor blocked so a
                # later request (after reconciliation or candidate rejection)
                # can revisit the remaining rows without a false miss cache.
                partial_candidate_seen = True
            else:
                # This is the only negative fact safe to carry across runs:
                # the complete torrent metainfo was fetched and validated,
                # then proved not to cover the current exact gaps.
                manifest_infohash = str(manifest.get("infohash") or "").casefold()
                if _infohash_aliases(manifest_infohash):
                    miss_locator = f"torrent:{manifest_infohash}"
                    reviewed_miss_locators.add(miss_locator)
                    reviewed_hashes.update(_infohash_aliases(manifest_infohash))
                    resource_failed_locators.append(miss_locator)
        if page_failed:
            cursor_blocked = True
            break

        if partial_candidate_seen:
            # Every row happened to finish before the deadline, but this page
            # still yielded only partial coverage.  Retain its cursor rather
            # than pretending the remaining coordinates were searched by a
            # candidate that cannot satisfy them.
            cursor_blocked = True
            break

        # A valid empty page closes the current term; non-empty pages advance
        # by one.  No 32-row cap remains: the page itself is the bounded unit.
        if payload:
            page += 1
            if page > _ANIMETOSHO_MAX_PAGE:
                cursor_blocked = True
                break
        else:
            last_empty_page = page
            term_index += 1
            page = 1
        pages_completed += 1

    exhausted = bool(
        terms and term_index >= len(terms) and not cursor_blocked
    )
    cursor_page = (
        last_empty_page if exhausted and last_empty_page is not None else page
    )
    query_cursor = {
        "fingerprint": fingerprint,
        "term_index": min(max(term_index, 0), len(terms)),
        "page": min(max(cursor_page, 1), _ANIMETOSHO_MAX_PAGE),
        "exhausted": exhausted,
    }
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=exhausted and infrastructure_failures == 0,
        resource_failed_locators=resource_failed_locators,
        reviewed_torrent_miss_locators=sorted(reviewed_miss_locators),
        infrastructure_failures=infrastructure_failures,
        query_cursor=query_cursor,
    )
_BITSEARCH_ENDPOINT = "https://bitsearch.to/search?q="
_BITSEARCH_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) ScrapeFlow/4.0"
)
_BITSEARCH_MAX_ROWS = 16
_BITSEARCH_SITE_TAG = re.compile(r"(?i)^\s*[\[{]?\s*bitsearch(?:\.to)?\s*[\]}]?\s*[-–—: ]?\s*")
def _search_index_opener() -> urllib.request.OpenerDirector:
    """Proxy-aware opener for blocked search indexes.

    Search indexes are the one lane allowed to use the configured HTTP proxy
    (tracker announces and peer traffic must stay direct — see the aria2
    invocation).  Compose injects that proxy as the standard ``HTTP_PROXY``
    name translated from ``SCRAPEFLOW_HTTP_PROXY``; accept either spelling.
    With neither configured, stay explicitly direct rather than inheriting
    an ambient host proxy by accident.
    """
    proxy = (
        os.getenv("SCRAPEFLOW_HTTP_PROXY")
        or os.getenv("SCRAPEFLOW_HTTPS_PROXY")
        or os.getenv("HTTP_PROXY")
        or os.getenv("HTTPS_PROXY")
        or ""
    ).strip()
    if proxy:
        return urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy}),
        )
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))
def _general_index_search_terms(request: Mapping[str, Any], *, maximum: int = 6) -> list[str]:
    """Season-level then bare-title terms for the general-purpose index.

    Unlike the anime indexes this source indexes season packs, so per-episode
    queries add nothing; a season token plus the confirmed title is the
    provider-native grammar.
    """
    if maximum < 1:
        return []
    bases: list[str] = []
    seen: set[str] = set()
    for value in _identity_query_bases(request, prefer_latin_aliases=True):
        term = _nyaa_safe_query_term(value)
        if term is None:
            continue
        key = _normalized_text(term)
        if not key or key in seen:
            continue
        seen.add(key)
        bases.append(term)
        if len(bases) >= 4:
            break
    if not bases:
        return []
    terms: list[str] = []
    dedup: set[str] = set()
    for season in _positive_requested_seasons(request, maximum=4):
        for base in bases[:2]:
            value = f"{base} S{season:02d}"
            key = _normalized_text(value)
            if not key or key in dedup:
                continue
            dedup.add(key)
            terms.append(value)
    for base in bases[:2]:
        key = _normalized_text(base)
        if key and key not in dedup:
            dedup.add(key)
            terms.append(base)
    return terms[:maximum]
def _bitsearch_page_rows(page: str) -> dict[str, str]:
    """Full-infohash rows with a clean release title per hash.

    Only the page's own magnet hrefs are trusted for the infohash.  The
    ``/download/torrent/`` links carry just a 12-character hash prefix and
    have been observed serving a *different* torrent than their advertised
    hash, so they are used purely as the clean-title source, never as the
    acquisition document.
    """
    rows: dict[str, str] = {}
    for match in re.finditer(r'href="(magnet:\?xt[^"]+)"', page):
        href = html_module.unescape(match.group(1))
        info = re.search(r"urn:btih:([0-9a-fA-F]{40})", href)
        if info is None:
            continue
        name_match = re.search(r"[?&]dn=([^&]*)", href)
        dn = (
            urllib.parse.unquote_plus(name_match.group(1))
            if name_match else ""
        )
        rows.setdefault(info.group(1).casefold(), _BITSEARCH_SITE_TAG.sub("", dn).strip())
    for match in re.finditer(
        r'/download/torrent/([0-9A-Fa-f]{12,40})\?title=([^"]+)"', page,
    ):
        prefix = match.group(1).casefold()[:12]
        title = html_module.unescape(match.group(2)).strip()
        if not title:
            continue
        for infohash in rows:
            if infohash.startswith(prefix):
                rows[infohash] = title
                break
    return rows
def _general_index_row_relevant(title: str, request: Mapping[str, Any]) -> bool:
    """Cheap name prefilter before any DHT metadata resolution."""
    if not title:
        return False
    normalized = _normalized_text(title)
    bases = [
        _normalized_text(value)
        for value in _identity_query_bases(request, prefer_latin_aliases=True)
        if _normalized_text(value)
    ]
    if bases and not any(base in normalized for base in bases):
        return False
    return not _release_year_conflict(request, title)
def _search_bitsearch(
    request: Mapping[str, Any], existing_locators: set[str], *,
    deadline: float | None = None,
) -> _DynamicSearchResult:
    """Search the general-purpose BitSearch index for TV/movie works.

    Row discovery is plain HTTP; per-row metadata is resolved from the swarm
    via DHT (bounded batch), never from the index's own download endpoint.
    """
    deadline = deadline or time.monotonic() + _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT", 45, 10, 300,
    )
    metadata_budget = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_GENERAL_INDEX_METADATA_TIMEOUT", 200, 60, 600,
    )

    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(15, remaining))

    terms = _general_index_search_terms(request)
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    infrastructure_failures = 0
    infrastructure_failure_types: dict[str, int] = {}
    rows: dict[str, str] = {}
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    reviewed_hashes = _locator_infohash_aliases(
        request.get("reviewed_torrent_miss_locators") or [],
    )
    preexcluded_count = 0
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = _BITSEARCH_ENDPOINT + urllib.parse.quote_plus(term)
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
                opener=_search_index_opener(),
                # The index's WAF rejects non-browser agents with a 500.
                user_agent=_BITSEARCH_USER_AGENT,
            ).decode("utf-8", "replace")
        except (OSError, RuntimeError, ValueError) as exc:
            infrastructure_failures += 1
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        for infohash, title in _bitsearch_page_rows(page).items():
            if infohash in excluded_hashes or infohash in reviewed_hashes:
                preexcluded_count += 1
                continue
            if not _general_index_row_relevant(title, request):
                continue
            rows.setdefault(infohash, title)
            if len(rows) >= _BITSEARCH_MAX_ROWS:
                hit_cap = True
                break
        if hit_cap:
            break

    magnets: dict[str, str] = {}
    for infohash, title in rows.items():
        dn = urllib.parse.quote(title or infohash)
        magnet = f"magnet:?xt=urn:btih:{infohash}&dn={dn}"
        if "&tr=" not in magnet:
            magnet = magnet + "&tr=" + "&tr=".join(_MAGNET_BOOTSTRAP_TRACKERS)
        magnets[infohash] = magnet

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    with tempfile.TemporaryDirectory(prefix="scrapeflow-bitsearch-") as directory:
        scratch = Path(directory)
        manifests, dht_unresolved = _magnet_metadatas_batch(
            list(magnets.values()), scratch, timeout=metadata_budget,
        )
        if dht_unresolved:
            # A cold DHT window dropped rows the index did return; count it
            # as infrastructure so the lane reads "window not finished"
            # instead of silently "no candidates".
            infrastructure_failures += 1
            infrastructure_failure_types["dht_window_cold"] = (
                infrastructure_failure_types.get("dht_window_cold", 0) + 1
            )
        for infohash, title in rows.items():
            manifest = manifests.get(infohash)
            if manifest is None:
                # No swarm metadata within the bounded DHT window.  This is
                # a resource miss (dead/unseeded), not an outage.
                resource_failed_locators.append(f"torrent:{infohash}")
                continue
            variants = _torrent_candidate_variants(
                request, title or (manifest.get("root") or ""),
                magnets[infohash], manifest,
                include_local=_local_torrent_available(request),
            )
            if variants:
                candidates.extend(variants)
            else:
                resource_failed_locators.append(f"torrent:{infohash}")
    processed = len(candidates) + len(resource_failed_locators)
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms
            and query_attempts == len(terms)
            and query_responses == query_attempts
            and not hit_cap
            and processed == len(rows)
            and infrastructure_failures == 0
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failure_types=infrastructure_failure_types,
        preexcluded_count=preexcluded_count,
    )
_KNABEN_ENDPOINT = "https://knaben.org/search/?q="
def _knaben_page_rows(page: str) -> dict[str, str]:
    """Full-infohash rows with the release title from knaben.org.

    Knaben is a meta-index: its anchors pair the release title with a magnet
    that already carries its own tracker list, so the DHT metadata pass gets
    announce endpoints for free.
    """
    rows: dict[str, str] = {}
    for match in re.finditer(
        r'<a title="([^"]+)" href="(magnet:\?xt=urn:btih:([0-9a-fA-F]{40})[^"]*)"',
        page,
    ):
        title = html_module.unescape(match.group(1)).strip()
        infohash = match.group(3).casefold()
        rows.setdefault(infohash, title)
    return rows
def _search_knaben(
    request: Mapping[str, Any], existing_locators: set[str], *,
    deadline: float | None = None,
) -> _DynamicSearchResult:
    """Search the knaben.org meta-index for TV/movie works.

    Same shape as the BitSearch lane: HTTP row discovery through the search
    proxy, then DHT-anchored metadata resolution; never a downloaded
    ``.torrent`` from the index itself.
    """
    deadline = deadline or time.monotonic() + _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT", 45, 10, 300,
    )
    metadata_budget = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_GENERAL_INDEX_METADATA_TIMEOUT", 200, 60, 600,
    )

    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(15, remaining))

    terms = _general_index_search_terms(request)
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    infrastructure_failures = 0
    infrastructure_failure_types: dict[str, int] = {}
    rows: dict[str, str] = {}
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    reviewed_hashes = _locator_infohash_aliases(
        request.get("reviewed_torrent_miss_locators") or [],
    )
    preexcluded_count = 0
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = _KNABEN_ENDPOINT + urllib.parse.quote_plus(term)
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
                opener=_search_index_opener(),
                user_agent=_BITSEARCH_USER_AGENT,
            ).decode("utf-8", "replace")
        except (OSError, RuntimeError, ValueError) as exc:
            infrastructure_failures += 1
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        for infohash, title in _knaben_page_rows(page).items():
            if infohash in excluded_hashes or infohash in reviewed_hashes:
                preexcluded_count += 1
                continue
            if not _general_index_row_relevant(title, request):
                continue
            rows.setdefault(infohash, title)
            if len(rows) >= _BITSEARCH_MAX_ROWS:
                hit_cap = True
                break
        if hit_cap:
            break

    magnets: dict[str, str] = {}
    for infohash, title in rows.items():
        dn = urllib.parse.quote(title or infohash)
        magnets[infohash] = f"magnet:?xt=urn:btih:{infohash}&dn={dn}"

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    with tempfile.TemporaryDirectory(prefix="scrapeflow-knaben-") as directory:
        scratch = Path(directory)
        # Knaben magnets carry their own trackers; the batch helper keeps
        # them and only appends the bootstrap list when none are present.
        manifests, dht_unresolved = _magnet_metadatas_batch(
            [
                _magnet_with_trackers(magnets[infohash])
                for infohash in rows
            ],
            scratch,
            timeout=metadata_budget,
        )
        if dht_unresolved:
            infrastructure_failures += 1
            infrastructure_failure_types["dht_window_cold"] = (
                infrastructure_failure_types.get("dht_window_cold", 0) + 1
            )
        for infohash, title in rows.items():
            manifest = manifests.get(infohash)
            if manifest is None:
                resource_failed_locators.append(f"torrent:{infohash}")
                continue
            variants = _torrent_candidate_variants(
                request, title or (manifest.get("root") or ""),
                magnets[infohash], manifest,
                include_local=_local_torrent_available(request),
            )
            if variants:
                candidates.extend(variants)
            else:
                resource_failed_locators.append(f"torrent:{infohash}")
    processed = len(candidates) + len(resource_failed_locators)
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms
            and query_attempts == len(terms)
            and query_responses == query_attempts
            and not hit_cap
            and processed == len(rows)
            and infrastructure_failures == 0
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failure_types=infrastructure_failure_types,
        preexcluded_count=preexcluded_count,
    )
def _magnet_with_trackers(magnet_url: str) -> str:
    """Append the bootstrap tracker list only when a magnet carries none."""
    if "&tr=" in magnet_url:
        return magnet_url
    return magnet_url + "&tr=" + "&tr=".join(_MAGNET_BOOTSTRAP_TRACKERS)
def _search_acg(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float | None = None,
) -> list[dict[str, Any]]:
    deadline = deadline or time.monotonic() + _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT", 45, 10, 300,
    )

    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    result_pages: dict[str, str] = {}
    query_attempts = 0
    query_responses = 0
    infrastructure_failure_types: dict[str, int] = {}
    terms = _compact_dynamic_search_terms(request, maximum=4)
    acg_opener = _acg_http_opener()
    hit_cap = False
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://acg.rip/?term=" + urllib.parse.quote(term)
        parser = _AnchorParser()
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
                opener=acg_opener,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        parser.feed(page.decode("utf-8", "replace"))
        for anchor in parser.anchors:
            match = re.fullmatch(r"(?:https://acg\.rip)?/t/(\d+)", anchor["href"])
            if match and anchor["text"]:
                result_pages.setdefault(match.group(1), anchor["text"])
            if len(result_pages) >= 16:
                hit_cap = True
                break
        if len(result_pages) >= 16:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    infrastructure_failures = 0
    processed = 0
    for torrent_id, release_name in list(result_pages.items())[:16]:
        if time.monotonic() >= deadline:
            break
        torrent_url = f"https://acg.rip/t/{torrent_id}.torrent"
        locator = f"torrent:{torrent_url}"
        if locator in existing_locators:
            continue
        with tempfile.TemporaryDirectory(prefix="scrapeflow-acg-") as directory:
            torrent_path = Path(directory) / "candidate.torrent"
            try:
                manifest = _download_torrent(
                    torrent_url, torrent_path,
                    timeout=request_timeout(), attempts=1,
                    opener=acg_opener,
                )
            except Exception as exc:
                infrastructure_failures += 1
                code = _network_failure_code(exc)
                infrastructure_failure_types[code] = (
                    infrastructure_failure_types.get(code, 0) + 1
                )
                continue
        processed += 1
        if f"torrent:{manifest['infohash']}" in existing_locators:
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_available(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.append(f"torrent:{manifest['infohash']}")
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms) and query_responses == query_attempts
            and not hit_cap and processed == len(result_pages)
            and infrastructure_failures == 0
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=infrastructure_failures,
        infrastructure_failure_types=infrastructure_failure_types,
    )
def _subsplease_magnet_manifest(
    magnet_url: str,
) -> tuple[str, dict[str, Any]] | None:
    """Return a safe single-file manifest carried by a SubsPlease magnet."""
    if not isinstance(magnet_url, str) or not magnet_url.startswith("magnet:?"):
        return None
    if len(magnet_url) > 32_768:
        return None
    query = urllib.parse.parse_qs(
        urllib.parse.urlsplit(magnet_url).query, keep_blank_values=False,
    )
    xt = query.get("xt", [""])[0]
    match = re.fullmatch(
        r"urn:btih:([0-9a-fA-F]{40}|[A-Z2-7a-z2-7]{32})", str(xt),
    )
    names = query.get("dn") or []
    sizes = query.get("xl") or []
    if match is None or len(names) != 1 or len(sizes) != 1:
        return None
    name = str(names[0]).replace("\\", "/")
    try:
        size = int(sizes[0])
    except (TypeError, ValueError):
        return None
    if (
        not name or name.startswith("/") or len(name.encode("utf-8")) > 1_024
        or any(part in {"", ".", ".."} for part in name.split("/"))
        or Path(name).suffix.casefold() not in VIDEO_EXTENSIONS
        or size <= 0
    ):
        return None
    aliases = _infohash_aliases(match.group(1))
    infohash = next((value for value in aliases if len(value) == 40), "")
    if not re.fullmatch(r"[0-9a-f]{40}", infohash):
        return None
    return infohash, {
        "root": name, "infohash": infohash,
        "files": {1: {"path": name, "size": size}},
    }
def _search_subsplease(
    request: Mapping[str, Any], existing_locators: set[str], *,
    deadline: float | None = None,
) -> _DynamicSearchResult:
    """Search SubsPlease's public API without treating it as an archive."""
    deadline = deadline or time.monotonic() + _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT", 45, 10, 300,
    )

    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    terms = _compact_dynamic_search_terms(request, maximum=4)
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    infrastructure_failure_types: dict[str, int] = {}
    magnets: dict[str, tuple[str, str, dict[str, Any]]] = {}
    preexcluded_count = 0
    resource_failed_locators: list[str] = []
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://subsplease.org/api/?" + urllib.parse.urlencode({
            "f": "search", "tz": "UTC", "s": term,
        })
        query_attempts += 1
        try:
            payload = json.loads(_fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            ).decode("utf-8"))
            if payload == []:
                # The API uses an empty JSON array for a successful zero-hit
                # search, while non-empty results are keyed objects.
                payload = {}
            if not isinstance(payload, Mapping):
                raise ValueError("SubsPlease response is not an object")
        except (OSError, RuntimeError, UnicodeDecodeError, ValueError,
                json.JSONDecodeError) as exc:
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        # The endpoint currently caps broad searches at 30 releases.  Such a
        # page can still contribute candidates, but cannot prove exhaustion.
        if len(payload) >= 30:
            hit_cap = True
        for release_name, release in payload.items():
            if not isinstance(release_name, str) or not isinstance(release, Mapping):
                continue
            downloads = release.get("downloads")
            if not isinstance(downloads, list):
                continue
            for download in downloads:
                if not isinstance(download, Mapping):
                    continue
                magnet_url = download.get("magnet")
                parsed = _subsplease_magnet_manifest(magnet_url)
                if parsed is None:
                    continue
                infohash, manifest = parsed
                aliases = _infohash_aliases(infohash)
                if any(
                    f"torrent:{alias}" in existing_locators
                    for alias in aliases
                ):
                    preexcluded_count += 1
                    continue
                magnets.setdefault(infohash, (release_name, str(magnet_url), manifest))
                if len(magnets) >= 16:
                    hit_cap = True
                    break
            if len(magnets) >= 16:
                break
        if len(magnets) >= 16:
            break

    candidates: list[dict[str, Any]] = []
    for infohash, (release_name, magnet_url, manifest) in magnets.items():
        local = _torrent_candidate(
            request, release_name, "https://subsplease.org/api/", manifest,
        )
        if local is None:
            resource_failed_locators.append(f"torrent:{infohash}")
            continue
        # SubsPlease exposes a magnet without a separately verifiable torrent
        # URL.  It is retained as search evidence only, never dispatched.
        del magnet_url
        resource_failed_locators.append(f"torrent:{infohash}")
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms)
            and query_responses == query_attempts and not hit_cap
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failure_types=infrastructure_failure_types,
        preexcluded_count=preexcluded_count,
    )


ANIME_PLAIN_EPISODE_RE = re.compile(
    r"(?:^|[\s._-])0*(\d{1,3})(?=\s*(?:\[[^\]]+\]\s*)*$)", re.I,
)
OPTIONAL_EPISODE_RE = re.compile(
    r"(?i)(?:OVA|OAD|SP|SPECIAL|PICTURE[\s._-]*DRAMA|PLAY)\s*#?0*(\d{1,3})"
)
OPTIONAL_CONTAINER_RE = re.compile(
    r"(?i)(?:^|/)(?:SP|SPECIALS?|OVA|OAD|EXTRAS?)(?:/|$)"
)
OPTIONAL_PAST_ARC_RE = re.compile(r"(?:过去|過去)篇\s*0*(\d{1,3})", re.I)
OPTIONAL_PAST_ARC_NAME_RE = re.compile(r"(?:过去|過去)篇", re.I)
OPTIONAL_NEWLYWED_RE = re.compile(r"新婚篇(?:\s*0*(\d{1,3}))?", re.I)
OPTIONAL_RETROSPECTIVE_COLLECTION_RE = re.compile(
    r"(?:精选集|精選集|总集篇|總集篇|総集編|総集篇|"
    r"回想篇|回顾篇|回顧篇|回顾集|回顧集|"
    r"\b(?:recap|digest|compilation|retrospective)\b)",
    re.I,
)
OPTIONAL_EXPLICIT_S00_RE = re.compile(r"(?i)(?<![A-Z0-9])S00[ ._-]*E0*\d{1,4}\b")
