#!/usr/bin/env python3
"""Local catalog + aria2 + AList replenishment adapter.

Search is read-only and returns only candidates bound to the request TMDB ID.
Acquisition downloads the selected torrent files into an isolated workspace,
verifies exact file indices and sizes, uploads canonical episode names into the
unscraped AList root, and verifies the remote rows.  Successful local staging
is retained until the coordinator proves formal-library convergence.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import posixpath
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Iterable, Mapping
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
import zlib

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scraper import AListClient, ApiError, ScraperError, join_remote, split_remote
from engine.scrapeflow.provider_capabilities import (
    ACTIVE_PROVIDERS,
    ACQUISITION_TORRENT,
    PROVIDER_LOCAL_MAGNET,
    candidate_capability_error,
    provider_capability_snapshot,
)
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.video_admission import (
    VideoAdmissionError,
    probe_local_video_stream,
)


class ReplenishmentDeliveryError(RuntimeError):
    """The verified payload is reusable; only cloud delivery failed."""

    failure_scope = "delivery"
    reusable_candidate = True
    exclude_candidate = False

    def __init__(self, message: str, *, stage: str = "delivery") -> None:
        super().__init__(message)
        self.failure_stage = stage


class ReplenishmentCandidateError(RuntimeError):
    """The selected release itself failed identity, manifest, or acquisition checks."""

    failure_scope = "candidate"
    reusable_candidate = False
    exclude_candidate = True

    def __init__(
        self, message: str, *, stage: str = "candidate_acquire",
        candidate: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_stage = stage
        self.candidate = {
            key: candidate.get(key)
            for key in ("provider", "release_name", "locator", "infohash")
            if candidate is not None and candidate.get(key) is not None
        }


class ReplenishmentInfrastructureError(RuntimeError):
    """Local capacity, dependencies, or orchestration failed without blaming a release."""

    failure_scope = "infrastructure"
    reusable_candidate = False
    exclude_candidate = False

    def __init__(self, message: str, *, stage: str = "infrastructure") -> None:
        super().__init__(message)
        self.failure_stage = stage


class ReplenishmentPauseRequested(RuntimeError):
    """A caller withdrew its RootJob scope before a local provider effect."""

    pause_requested = True


def _pause_checkpoint(pause_requested: Callable[[], bool] | None) -> None:
    """Fail closed immediately before a provider-owned external operation."""
    if pause_requested is None:
        return
    try:
        paused = bool(pause_requested())
    except Exception as exc:
        if getattr(exc, "pause_requested", False) is True:
            raise
        raise ReplenishmentPauseRequested(
            "补源暂停状态不可确认，已在外部操作前停止",
        ) from exc
    if paused:
        raise ReplenishmentPauseRequested(
            "补源已暂停或不在当前 RootJob 试运行范围",
        )


@contextmanager
def _workspace_lease(root: Path, workspace_key: str):
    """Prevent two retries from mutating one deterministic workspace at once."""
    lock_dir = root / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{workspace_key}.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReplenishmentInfrastructureError(
                "相同补源工作区已有获取进程在运行",
                stage="orchestration_concurrency",
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _search_capacity_lease(root: Path):
    """Bound API-heavy search subprocesses across restored scheduler threads."""
    try:
        slots = int(os.getenv("SCRAPEFLOW_REPLENISHMENT_SEARCH_WORKERS", "2"))
    except ValueError:
        slots = 2
    slots = max(1, min(8, slots))
    state_root = os.getenv("SCRAPEFLOW_STATE_DIR", "").strip()
    lock_dir = (
        Path(state_root) / ".replenishment-search-locks"
        if state_root and Path(state_root).is_absolute()
        else root / ".search-locks"
    )
    lock_dir.mkdir(parents=True, exist_ok=True)
    acquired = None
    try:
        while acquired is None:
            for index in range(slots):
                handle = (lock_dir / f"slot-{index}.lock").open("a+", encoding="utf-8")
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    handle.close()
                    continue
                acquired = handle
                break
            if acquired is None:
                time.sleep(0.25)
        yield
    finally:
        if acquired is not None:
            fcntl.flock(acquired.fileno(), fcntl.LOCK_UN)
            acquired.close()


def _selection_workspace_key(selection_wrapper: Mapping[str, Any]) -> str:
    selection = selection_wrapper.get("selection")
    rows = selection.get("selections") if isinstance(selection, Mapping) else []
    identities = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, Mapping):
            continue
        acquisition = row.get("acquisition")
        gap_map = acquisition.get("file_index_by_gap") if isinstance(acquisition, Mapping) else {}
        companion_map = (
            acquisition.get("companion_subtitle_index_by_media_gap")
            if isinstance(acquisition, Mapping) else {}
        )
        file_id_map = acquisition.get("file_id_by_gap") if isinstance(acquisition, Mapping) else {}
        indices = sorted({
            int(value) for values in (gap_map.values() if isinstance(gap_map, Mapping) else [])
            if isinstance(values, list) for value in values if type(value) is int
        })
        indices = sorted(set(indices) | {
            int(value)
            for values in (companion_map.values() if isinstance(companion_map, Mapping) else [])
            if isinstance(values, list) for value in values if type(value) is int
        })
        file_ids = sorted({
            str(value) for values in (
                file_id_map.values() if isinstance(file_id_map, Mapping) else []
            )
            for value in (
                [values] if isinstance(values, str)
                else values if isinstance(values, list) else []
            )
            if isinstance(value, str) and value
        })
        identity = {
            "infohash": str(row.get("infohash") or "").casefold(),
            "locator": (
                "" if row.get("infohash") else str(row.get("locator") or "")
            ),
            "indices": indices,
        }
        # Include file IDs when a selection provides them so retries reuse the
        # same workspace for the same payload members.
        if file_ids:
            identity["file_ids"] = file_ids
        identities.append(identity)
    encoded = json.dumps(identities, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    # This key is only a short, stable directory name for retry reuse; it is
    # not used to validate payload content.
    return f"{zlib.crc32(encoded.encode('utf-8')):08x}"


from engine.scrapeflow.replenishment_matching import (
    coverage_tokens as _coverage_tokens,
    expanded_episode_ids as _expanded_episode_ids,
    normalized_text as _normalized_text,
    season_markers as _season_markers,
)
from engine.scrapeflow.media_quality import (
    is_production_test_media_path,
    minimum_video_bytes,
    video_size_is_admissible,
)
from engine.scrapeflow.media_policy import (
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
)


# Compatibility names are intentionally kept local because this adapter's
# public helpers accept an ``allowed_payload_extensions`` default.
MAX_TORRENT_BYTES = 8 * 1024 * 1024
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

# A provider manifest is untrusted evidence.  These members can carry an
# episode-looking token while being an opening/ending, preview, sample,
# bonus, scan or other supplemental payload.  They must never be selected as
# the one primary video for an audited ``missing_episode`` gap.  Keep the
# expression delimiter-aware so ordinary words such as ``Extraordinary`` do
# not accidentally become a rejection, while still failing closed for the
# common directory and release-name spellings.
_SUPPLEMENTAL_VIDEO_PATH_RE = re.compile(
    r"(?i)(?:^|[/\\\s._\-\[\](){}])"
    r"(?:bonus(?:es)?|extra(?:s)?|sample(?:s)?|scan(?:s)?|"
    r"menu|preview(?:s)?|trailer(?:s)?|teaser(?:s)?|featurette(?:s)?|"
    r"behind[ ._\-]*the[ ._\-]*scenes|"
    r"ncop|nced|pv|cm|creditless|op|ed)"
    r"(?=$|[/\\\s._\-\[\](){}])"
)


def _is_supplemental_video_path(path: str) -> bool:
    """Return whether a manifest path is recognizably non-primary media."""
    normalized = str(path or "").replace("\\", "/")
    return bool(_SUPPLEMENTAL_VIDEO_PATH_RE.search(normalized))


def _is_ordinary_primary_video_path(path: str) -> bool:
    """Check path shape which remains meaningful after selection serialization."""
    return (
        Path(path).suffix.casefold() in VIDEO_EXTENSIONS
        and not _is_supplemental_video_path(path)
        and len(_expanded_episode_ids(path)) <= 1
    )


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


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON 根节点需要是对象: {path}")
    return value


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


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_write_json(path, value, sort_keys=True)


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


def _catalog_path() -> Path | None:
    raw = os.getenv("SCRAPEFLOW_REPLENISHMENT_CATALOG", "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_file():
        raise ValueError(f"补源候选目录不存在: {path}")
    return path


def _local_torrent_available(request: Mapping[str, Any]) -> bool:
    """Allow the configured local Torrent search lane."""
    del request
    return True


def _dynamic_search_timeout_seconds(request: Mapping[str, Any]) -> int:
    del request
    return _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT", 45, 10, 300,
    )


def _search(request: Mapping[str, Any]) -> dict[str, Any]:
    """Search exact local Torrent sources only; never register a cloud bridge."""
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    tmdb_id = media.get("tmdb_id")
    if type(tmdb_id) is not int or tmdb_id <= 0:
        return {"version": 1, "candidates": [], "message": "请求缺少 TMDB 身份"}

    existing_rows = request.get("excluded_candidates")
    existing_rows = existing_rows if isinstance(existing_rows, list) else []
    existing_locators = {
        str(row.get("locator"))
        for row in existing_rows
        if isinstance(row, Mapping) and row.get("locator")
    }
    warnings: list[str] = []
    output: list[dict[str, Any]] = []
    catalog_ready = False
    try:
        catalog_path = _catalog_path()
        if catalog_path is not None:
            catalog = _load(catalog_path)
            projects = catalog.get("projects") if isinstance(catalog.get("projects"), Mapping) else {}
            project = projects.get(str(tmdb_id))
            raw = project.get("candidates") if isinstance(project, Mapping) else []
            if isinstance(raw, list):
                catalog_ready = True
                for row in raw:
                    if not isinstance(row, Mapping):
                        continue
                    candidate = dict(row)
                    if str(candidate.get("provider") or "") in ACTIVE_PROVIDERS:
                        output.extend(_catalog_torrent_candidate_variants(candidate))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        warnings.append(f"已核验候选目录不可用: {type(exc).__name__}")

    source_specs: list[tuple[str, Callable[..., Any], bool, str, str]] = [
        ("AnimeTosho", _search_animetosho, True,
         "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH", "0"),
        ("TokyoTosho", _search_tokyotosho, True,
         "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH", "0"),
        ("SubsPlease", _search_subsplease, False,
         "SCRAPEFLOW_REPLENISHMENT_SUBSPLEASE_SEARCH", "0"),
        ("Mikan", _search_mikan, False,
         "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH", "0"),
        ("DMHY", _search_dmhy, False,
         "SCRAPEFLOW_REPLENISHMENT_DMHY_SEARCH", "0"),
        # Nyaa remains an optional provider in normal deployments, but the
        # explicit switch lets isolated checks disable every network source.
        ("Nyaa", _search_nyaa, False,
         "SCRAPEFLOW_REPLENISHMENT_NYAA_SEARCH", "1"),
        ("ACG", _search_acg, True,
         "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH", "1"),
    ]
    deadline = time.monotonic() + _dynamic_search_timeout_seconds(request)
    telemetry: dict[str, Any] = {}
    for label, searcher, required, env_name, default_enabled in source_specs:
        if os.getenv(env_name, default_enabled).strip().casefold() in {
            "0", "false", "no", "off", "",
        }:
            continue
        try:
            result = searcher(
                request,
                existing_locators,
                deadline=deadline,
            )
            rows = [dict(row) for row in result if isinstance(row, Mapping)]
            rows = [
                row for row in rows
                if str(row.get("provider") or "") in ACTIVE_PROVIDERS
                and candidate_capability_error(row) is None
            ]
            output.extend(row for row in rows if str(row.get("locator") or "") not in existing_locators)
            telemetry[label] = {
                "query_attempts": int(getattr(result, "query_attempts", 0)),
                "query_responses": int(getattr(result, "query_responses", 0)),
                "source_exhausted": bool(getattr(result, "source_exhausted", False)),
                "resource_failed_locators": [
                    str(value) for value in getattr(result, "resource_failed_locators", [])
                    if str(value).startswith("torrent:")
                ],
                "infrastructure_failures": int(getattr(result, "infrastructure_failures", 0)),
                "required": required,
            }
        except Exception as exc:
            telemetry[label] = {
                "source_exhausted": False, "required": required,
                "infrastructure_failures": 1,
                "error_type": type(exc).__name__,
            }
            warnings.append(f"{label} 搜索不可用: {type(exc).__name__}")

    deduplicated: dict[tuple[str, str], dict[str, Any]] = {}
    for row in output:
        provider = str(row.get("provider") or "")
        locator = str(row.get("locator") or "")
        if provider not in ACTIVE_PROVIDERS or not locator:
            continue
        if candidate_capability_error(row) is not None:
            continue
        deduplicated.setdefault((provider, locator), row)
    required_rows = [
        value for value in telemetry.values()
        if isinstance(value, Mapping) and value.get("required") is True
    ]
    search_complete = bool(
        (catalog_ready or output)
        and all(row.get("source_exhausted") is True for row in required_rows)
        and all(int(row.get("infrastructure_failures") or 0) == 0 for row in required_rows)
    ) if required_rows else catalog_ready
    return {
        "version": 1,
        "catalog_verified_at": None,
        "candidates": list(deduplicated.values()),
        "excluded_candidate_count": len(existing_rows),
        "warnings": warnings,
        "lane_status": provider_capability_snapshot(),
        "provider_capabilities": provider_capability_snapshot(),
        "source_telemetry": telemetry,
        "search_complete": search_complete,
        "active_search_lane": "magnet_torrent",
    }


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[dict[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a" or self._href is not None:
            return
        href = dict(attrs).get("href")
        if isinstance(href, str):
            self._href = href
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() == "a" and self._href is not None:
            self.anchors.append({"href": self._href, "text": " ".join(self._text).strip()})
            self._href = None
            self._text = []


class _DynamicSearchResult(list[dict[str, Any]]):
    """Candidates plus enough telemetry to distinguish empty from unreachable."""

    def __init__(
        self, values: list[dict[str, Any]], *, query_attempts: int, query_responses: int,
        source_exhausted: bool = False,
        resource_failed_locators: list[str] | None = None,
        infrastructure_failures: int = 0,
        infrastructure_failure_types: Mapping[str, int] | None = None,
        preexcluded_count: int = 0,
    ) -> None:
        super().__init__(values)
        self.query_attempts = query_attempts
        self.query_responses = query_responses
        self.source_exhausted = bool(source_exhausted)
        self.resource_failed_locators = list(resource_failed_locators or [])
        self.infrastructure_failures = max(0, int(infrastructure_failures))
        self.infrastructure_failure_types = {
            str(key): max(0, int(value))
            for key, value in (infrastructure_failure_types or {}).items()
            if value
        }
        self.preexcluded_count = max(0, int(preexcluded_count))


def _network_failure_code(exc: BaseException) -> str:
    """Return a stable, non-sensitive code for source-health telemetry."""
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in chain:
        chain.append(current)
        reason = current.reason if isinstance(current, urllib.error.URLError) else None
        current = reason if isinstance(reason, BaseException) else (
            current.__cause__ or current.__context__
        )
    for error in chain:
        if isinstance(error, urllib.error.HTTPError):
            return f"http_{error.code}"
        if isinstance(error, ConnectionRefusedError):
            return "connection_refused"
        if isinstance(error, ConnectionResetError):
            return "connection_reset"
        if isinstance(error, socket.gaierror):
            return "dns_failure"
        if isinstance(error, (TimeoutError, socket.timeout)):
            return "timeout"
        if isinstance(error, ssl.SSLError):
            return "tls_failure"
    return type(chain[-1] if chain else exc).__name__

def _fetch_bytes(
    url: str, *, max_bytes: int, timeout: int = 60, attempts: int = 4,
    opener: Any | None = None,
) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "ScrapeFlow/1.0"})
    last_error: Exception | None = None
    # When no explicit per-source proxy opener is supplied, stay direct:
    # urllib's default opener would inherit an ambient host proxy.
    direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for attempt in range(attempts):
        try:
            open_request = opener.open if opener is not None else direct_opener.open
            with open_request(request, timeout=timeout) as response:
                data = response.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise ValueError("HTTP 响应超过大小上限")
            return data
        except (OSError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(min(2 ** attempt, 4))
    raise RuntimeError(f"HTTP 读取失败: {type(last_error).__name__}") from last_error


def _acg_http_opener() -> Any | None:
    """Use an explicit per-source proxy without changing AList traffic."""
    value = os.getenv("SCRAPEFLOW_REPLENISHMENT_ACG_PROXY", "").strip()
    if not value:
        return None
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname or parsed.port is None
        or parsed.username is not None or parsed.password is not None
        or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
    ):
        raise ValueError("SCRAPEFLOW_REPLENISHMENT_ACG_PROXY must be a credential-free HTTP proxy URL")
    return urllib.request.build_opener(urllib.request.ProxyHandler({
        "http": value, "https": value,
    }))


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


def _optional_episode_title_search_terms(
    request: Mapping[str, Any],
) -> list[str]:
    """Keep a bounded set of exact S00 names in provider query budgets.

    Dynamic providers accept only a few queries per pass.  Prefer one exact
    TMDB title for each release season marker (for example ``3rd season`` and
    ``4th season``) before filling with other episode titles.  The later
    recursive file-manifest check remains authoritative for gap coverage.
    """
    rules = request.get("rules")
    if not (
        isinstance(rules, Mapping)
        and rules.get("optional_discovery_only") is True
    ):
        return []
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    aliases = [
        str(value).strip() for value in media.get("aliases") or []
        if isinstance(value, str) and value.strip()
    ]
    title = str(media.get("title") or "").strip()
    base = next(iter(dict.fromkeys([*aliases, title])), "")
    if not base:
        return []
    exact_titles: list[str] = []
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or group.get("season") != 0:
            continue
        exact_titles.extend(
            str(value).strip() for value in group.get("episode_titles") or []
            if isinstance(value, str) and value.strip()
        )
    marked: list[str] = []
    remaining: list[str] = []
    seen_markers: set[str] = set()
    for value in dict.fromkeys(exact_titles):
        marker = re.search(
            r"(?i)\b(\d{1,2})(?:st|nd|rd|th)?\s+season\b", value,
        )
        marker_key = marker.group(1) if marker else ""
        if marker_key and marker_key not in seen_markers:
            seen_markers.add(marker_key)
            marked.append(value)
        else:
            remaining.append(value)
    return [f"{base} {value}" for value in [*marked, *remaining][:4]]


def _dynamic_search_terms(request: Mapping[str, Any]) -> list[str]:
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    aliases = [
        str(value).strip() for value in media.get("aliases") or []
        if isinstance(value, str) and value.strip()
    ]
    title = str(media.get("title") or "").strip()
    bases = list(dict.fromkeys([*aliases, title]))
    focused: list[str] = []
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or not isinstance(group.get("season"), int):
            continue
        season = int(group["season"])
        names = [
            str(value).strip() for value in group.get("season_names") or []
            if isinstance(value, str) and value.strip()
        ]
        for base in bases[:4]:
            focused.append(f"{base} {names[0]}" if names else f"{base} S{season:02d}")
            focused.append(f"{base} S{season:02d}")
    generated = [
        str(value).strip() for value in request.get("search_queries") or []
        if isinstance(value, str)
        and re.search(r"(?i)(?:S\d|\d+x\d|Season\s+\d|第.+季|完结篇|Darkness)", value)
    ]
    episode_focused = _optional_episode_title_search_terms(request)
    terms: list[str] = []
    seen: set[str] = set()
    # Exact S00 episode titles must survive the provider query ceiling; broad
    # season syntax follows them and still finds complete packs.  Focused
    # terms must precede bare aliases.  With many exact TMDB
    # alternative titles, letting bases consume the 12-term budget prevented
    # later official English names from receiving an S00 query at all.
    for value in [*episode_focused, *focused, *generated, *bases]:
        key = re.sub(r"\W+", "", value).casefold()
        if len(key) < 2 or key in seen:
            continue
        seen.add(key)
        terms.append(value)
        if len(terms) >= 12:
            break
    return terms


def _explicit_episode_search_terms(
    request: Mapping[str, Any], *, maximum: int = 8,
) -> list[str]:
    """Build a small, identity-scoped query set for exact requested episodes.

    A TMDB record can contribute many aliases.  Broad ``S00`` queries for the
    first few aliases used to consume every provider query slot before the
    exact ``S00E##`` form was tried.  Keep the exact token independent of the
    broader generated list so a verified single-episode release remains
    discoverable without trusting a bare series pack.
    """
    if maximum < 1:
        return []
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    title = str(media.get("title") or "").strip()
    aliases = [
        str(value).strip() for value in media.get("aliases") or []
        if isinstance(value, str) and value.strip()
    ]
    # Keep the local/library title and TMDB's declared alias order.  The
    # generic bare-alias helper ranks long English descriptions first, which
    # is sensible for broad discovery but can bury the short canonical alias
    # that a Torrent index actually uses (for example ``Kaichou wa
    # Maid-sama!``).
    bases = list(dict.fromkeys([title, *aliases]))
    targets: list[tuple[int, int]] = []
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or not isinstance(group.get("season"), int):
            continue
        season = int(group["season"])
        episodes = group.get("episodes")
        if not isinstance(episodes, list):
            continue
        for episode in episodes:
            if type(episode) is int and 1 <= episode <= 9999:
                targets.append((season, episode))
    if not targets:
        for gap in request.get("gaps") or []:
            if not isinstance(gap, Mapping) or not isinstance(gap.get("season"), int):
                continue
            season = int(gap["season"])
            episode = gap.get("episode")
            if type(episode) is int and 1 <= episode <= 9999:
                targets.append((season, episode))
    output: list[str] = []
    seen: set[str] = set()
    for base in bases:
        for season, episode in targets:
            value = f"{base} S{season:02d}E{episode:02d}"
            key = re.sub(r"\W+", "", value).casefold()
            if key in seen:
                continue
            seen.add(key)
            output.append(value)
            if len(output) >= maximum:
                return output
    return output


def _compact_dynamic_search_terms(
    request: Mapping[str, Any], *, maximum: int = 4,
) -> list[str]:
    """Prefer season/episode-specific queries over redundant bare aliases."""
    if maximum < 1:
        return []
    terms = _dynamic_search_terms(request)
    episode_focused = _optional_episode_title_search_terms(request)
    marked_episode_focused = [
        value for value in episode_focused
        if re.search(
            r"(?i)\b\d{1,2}(?:st|nd|rd|th)?\s+season\b", value,
        )
    ]
    # Keep enough authoritative aliases to reach the original/romanized title
    # even when TMDB returns a long multilingual list.  The final query cap is
    # still ``maximum``; this only changes which exact terms get the slots.
    exact_episode_all = _explicit_episode_search_terms(
        request, maximum=max(8, maximum * 3),
    )
    def latin_alias_term(value: str) -> bool:
        base = re.sub(
            r"(?i)\s+S\d{1,3}E\d{1,4}(?:-E?\d{1,4})?\s*$", "", value,
        )
        return bool(re.search(r"[A-Za-z]", base))

    exact_episode = [
        value for value in exact_episode_all if latin_alias_term(value)
    ] + [
        value for value in exact_episode_all if not latin_alias_term(value)
    ]
    exact_season = [
        value for value in terms if re.search(r"(?i)\bS\d{2}\b", value)
    ]
    broad_season = [
        value for value in terms
        if re.search(r"(?i)(?:\bS\d{1,2}\b|\bSeason\s+\d+\b|\b\d+x\d|\u7b2c.+\u5b63)", value)
    ]
    output: list[str] = []
    seen: set[str] = set()
    for value in [
        *marked_episode_focused,
        *exact_episode,
        *episode_focused,
        *exact_season,
        *broad_season,
        *terms,
    ]:
        key = re.sub(r"\W+", "", value).casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
        if len(output) >= maximum:
            break
    return output


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


def _catalog_torrent_candidate_variants(
    candidate: Mapping[str, Any], *, include_local: bool = True,
) -> list[dict[str, Any]]:
    """Return the exact local-Torrent variant for one catalog candidate."""
    local = dict(candidate)
    acquisition = local.get("acquisition")
    if (
        str(local.get("provider") or "").strip().casefold() == PROVIDER_LOCAL_MAGNET
        and isinstance(acquisition, Mapping)
        and str(acquisition.get("kind") or "").strip().casefold() == ACQUISITION_TORRENT
        and candidate_capability_error(local) is None
    ):
        return [local] if include_local else []
    return []


def _search_nyaa(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    results: dict[str, tuple[str, str, dict[str, Any] | None]] = {}
    query_attempts = 0
    query_responses = 0
    terms = _compact_dynamic_search_terms(request, maximum=3)
    hit_cap = False
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    preexcluded_hashes: set[str] = set()

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
        except (OSError, RuntimeError, ValueError, ET.ParseError):
            continue
        query_responses += 1
        for item in root.findall("./channel/item"):
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
            if aliases and aliases & excluded_hashes:
                preexcluded_hashes.add(feed_infohash)
                continue
            if (
                release_name
                and torrent_url.startswith("https://nyaa.si/download/")
                and locator not in existing_locators
            ):
                results.setdefault(torrent_url, (release_name, feed_infohash, swarm))
            if len(results) >= 32:
                hit_cap = True
                break
        if len(results) >= 32:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    infrastructure_failures = 0
    processed = 0
    ranked_results = sorted(
        results.items(),
        key=lambda item: _source_episode_release_priority(
            request, item[1][0],
        ),
    )
    for torrent_url, (release_name, feed_infohash, swarm) in ranked_results:
        if time.monotonic() >= deadline:
            break
        with tempfile.TemporaryDirectory(prefix="scrapeflow-nyaa-") as directory:
            try:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=1,
                )
            except Exception:
                infrastructure_failures += 1
                continue
        processed += 1
        manifest_aliases = _infohash_aliases(manifest["infohash"])
        if (
            manifest_aliases & excluded_hashes
            or _infohash_aliases(feed_infohash) & excluded_hashes
        ):
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_available(request),
            swarm=swarm,
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
            and not hit_cap and processed == len(results) and infrastructure_failures == 0
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=infrastructure_failures,
        preexcluded_count=len(preexcluded_hashes),
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


def _animetosho_search_terms(
    request: Mapping[str, Any], *, maximum: int = 4,
) -> list[str]:
    """Build AnimeTosho's bounded, exact-first query list."""
    if maximum < 1:
        return []
    source_episode = _source_episode_search_terms(request, maximum=1)
    exact_episode = _explicit_episode_search_terms(request, maximum=2)
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
    return list(dict.fromkeys([
        *focused, *source_episode, *exact_episode, *local_bare,
        *optional_bare, *_compact_dynamic_search_terms(request, maximum=maximum),
    ]))[:maximum]


def _dmhy_search_terms(request: Mapping[str, Any]) -> list[str]:
    """Prefer Season 00 sub-series names that DMHY release titles retain."""
    focused = _specific_s00_title_terms(request, maximum=4)
    # Some active Chinese indexes encode a regular-season episode as a pair
    # such as ``[S4][17_89]`` rather than ``S04E17``.  A bounded season query
    # lets the index return that release; the manifest parser still requires
    # the explicit dual-number evidence before any candidate can be selected.
    season_terms: list[str] = []
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    raw_bases = [
        str(value).strip()
        for value in [media.get("title"), *(media.get("aliases") or [])]
        if isinstance(value, str) and value.strip()
    ]
    # Query English/romanized aliases before the local display title: DMHY
    # release names normally retain those spellings.  This changes discovery
    # only; the title identity check still uses every authoritative alias.
    bases = [
        value for _index, value in sorted(
            enumerate(dict.fromkeys(raw_bases)),
            key=lambda item: (
                0 if re.search(r"[A-Za-z]", item[1]) else 1,
                item[0],
            ),
        )
    ]
    seasons: list[int] = []
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or type(group.get("season")) is not int:
            continue
        season = int(group["season"])
        if season > 0 and season not in seasons:
            seasons.append(season)
    for base in bases:
        for season in seasons:
            season_terms.append(f"{base} S{season}")
            if len(season_terms) >= 4:
                break
        if len(season_terms) >= 4:
            break
    fallback = _mikan_search_terms(request)
    return list(dict.fromkeys([*focused, *season_terms, *fallback]))[:4]


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


def _source_episode_release_priority(
    request: Mapping[str, Any], release_name: str,
) -> int:
    """Prioritize metadata rows that can contain a verified local alias."""
    release_tokens = _expanded_episode_ids(release_name) | _coverage_tokens(
        [release_name], default_seasons=_season_markers(release_name),
    )
    release_words = set(re.findall(r"[a-z0-9]+", release_name.casefold()))
    normalized_release = _normalized_text(release_name)
    for gap in request.get("gaps") or []:
        if not isinstance(gap, Mapping):
            continue
        for alias in gap.get("source_episode_aliases") or []:
            if not isinstance(alias, Mapping):
                continue
            source_season = alias.get("season")
            source_episode = alias.get("episode")
            if type(source_season) is not int or type(source_episode) is not int:
                continue
            token = f"S{source_season:02d}E{source_episode:02d}"
            if token not in release_tokens:
                continue
            for title in alias.get("series_titles") or []:
                if not isinstance(title, str):
                    continue
                normalized_title = _normalized_text(title)
                title_words = {
                    word for word in re.findall(r"[a-z0-9]+", title.casefold())
                    if len(word) >= 3
                    and word not in {"from", "starting", "season"}
                }
                if (
                    normalized_title and normalized_title in normalized_release
                    or len(title_words) >= 2 and title_words <= release_words
                ):
                    return 0
    return 1


_S00_TITLE_PREFLIGHT_LIMIT = 4
_S00_TITLE_PREFLIGHT_KEY_LIMIT = 256
_S00_TITLE_PREFLIGHT_GENERIC_KEYS = frozenset({
    "special", "specials", "movie", "ova", "oad", "sp", "extra",
    "bonus", "episode", "episodes", "特别篇", "特別篇", "剧场版",
    "劇場版", "映像特典",
})


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
    infrastructure_failures = 0
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


def _search_animetosho(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    """Search AnimeTosho's official JSON feed for old anime torrents."""
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    terms = _animetosho_search_terms(request, maximum=4)
    results: dict[str, tuple[str, str, dict[str, Any] | None]] = {}
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://feed.animetosho.org/json?q=" + urllib.parse.quote(term)
        query_attempts += 1
        try:
            payload = json.loads(_fetch_bytes(
                url, max_bytes=8 * 1024 * 1024,
                timeout=request_timeout(), attempts=2,
            ))
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, list):
            continue
        query_responses += 1
        for row in payload:
            if not isinstance(row, Mapping):
                continue
            release_name = str(row.get("title") or "").strip()
            torrent_url = str(row.get("torrent_url") or "").strip()
            infohash = str(row.get("info_hash") or "").strip().casefold()
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
                and (
                    not re.fullmatch(r"[0-9a-f]{40}", infohash)
                    or f"torrent:{infohash}" not in existing_locators
                )
            ):
                results.setdefault(torrent_url, (release_name, infohash, swarm))
            if len(results) >= 32:
                hit_cap = True
                break
        if hit_cap:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    infrastructure_failures = 0
    processed = 0
    ranked_results = sorted(
        results.items(),
        key=lambda item: _source_episode_release_priority(
            request, item[1][0],
        ),
    )
    ranked_results = _prioritize_verified_s00_title_rows(
        request, ranked_results,
        release_name=lambda item: item[1][0],
    )
    for torrent_url, (release_name, feed_infohash, swarm) in ranked_results:
        if time.monotonic() >= deadline:
            break
        with tempfile.TemporaryDirectory(prefix="scrapeflow-animetosho-") as directory:
            try:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=2,
                )
            except Exception:
                infrastructure_failures += 1
                continue
        processed += 1
        if (
            f"torrent:{manifest['infohash']}" in existing_locators
            or (
                feed_infohash
                and f"torrent:{feed_infohash}" in existing_locators
            )
        ):
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_available(request),
            swarm=swarm,
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.append(f"torrent:{manifest['infohash']}")
    source_exhausted = bool(
        terms and query_attempts == len(terms) and query_responses == query_attempts
        and not hit_cap and processed == len(results) and infrastructure_failures == 0
    )
    return _DynamicSearchResult(
        candidates, query_attempts=query_attempts,
        query_responses=query_responses, source_exhausted=source_exhausted,
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=infrastructure_failures,
    )


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


class _BDecoder:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def parse(self, offset: int = 0) -> tuple[Any, int]:
        data = self.data
        marker = data[offset:offset + 1]
        if marker == b"i":
            end = data.index(b"e", offset)
            return int(data[offset + 1:end]), end + 1
        if marker == b"l":
            values: list[Any] = []
            offset += 1
            while data[offset:offset + 1] != b"e":
                value, offset = self.parse(offset)
                values.append(value)
            return values, offset + 1
        if marker == b"d":
            values: dict[bytes, Any] = {}
            offset += 1
            while data[offset:offset + 1] != b"e":
                key, offset = self.parse(offset)
                value, offset = self.parse(offset)
                if not isinstance(key, bytes):
                    raise ValueError("torrent 字典键格式无效")
                values[key] = value
            return values, offset + 1
        separator = data.index(b":", offset)
        size = int(data[offset:separator])
        start = separator + 1
        return data[start:start + size], start + size


def _bencode(value: Any) -> bytes:
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii") + b"e"
    if isinstance(value, bytes):
        return str(len(value)).encode("ascii") + b":" + value
    if isinstance(value, list):
        return b"l" + b"".join(_bencode(item) for item in value) + b"e"
    if isinstance(value, dict):
        return b"d" + b"".join(_bencode(key) + _bencode(item) for key, item in value.items()) + b"e"
    raise TypeError(type(value))


def _torrent_manifest(data: bytes) -> dict[str, Any]:
    meta, offset = _BDecoder(data).parse()
    if offset != len(data) or not isinstance(meta, dict) or not isinstance(meta.get(b"info"), dict):
        raise ValueError("torrent 元数据格式无效")
    info = meta[b"info"]
    root = info.get(b"name")
    if not isinstance(root, bytes):
        raise ValueError("torrent 缺少名称")
    files: dict[int, dict[str, Any]] = {}
    if isinstance(info.get(b"files"), list):
        for index, row in enumerate(info[b"files"], start=1):
            if not isinstance(row, dict) or not isinstance(row.get(b"path"), list):
                raise ValueError("torrent 文件清单格式无效")
            parts = row[b"path"]
            if not all(isinstance(part, bytes) for part in parts):
                raise ValueError("torrent 文件路径格式无效")
            files[index] = {
                "path": "/".join(part.decode("utf-8", "replace") for part in parts),
                "size": row.get(b"length"),
            }
    else:
        files[1] = {"path": root.decode("utf-8", "replace"), "size": info.get(b"length")}
    return {
        "root": root.decode("utf-8", "replace"),
        "infohash": hashlib.sha1(_bencode(info)).hexdigest(),
        "files": files,
    }


def _nyaa_torrent_mirror_url(url: str) -> str | None:
    """Return the tightly-bounded HTTPS mirror URL for a canonical Nyaa link.

    TokyoTosho's authorized release rows use Nyaa's ``/view/<id>/torrent``
    endpoint, but that origin can be unavailable from a deployment even when
    TokyoTosho itself is reachable.  This helper intentionally accepts no
    arbitrary host, query, credential, port, or path: the caller may only
    retry the same numeric torrent ID through the fixed HTTPS mirror.
    """
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"nyaa.si", "www.nyaa.si"}
        or parsed.username is not None or parsed.password is not None
        or parsed.port is not None or parsed.query or parsed.fragment
    ):
        return None
    match = re.fullmatch(
        r"/(?:view/(?P<view>[1-9]\d*)/torrent|download/(?P<download>[1-9]\d*)\.torrent)",
        parsed.path,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    torrent_id = match.group("view") or match.group("download")
    return f"https://nyaa.land/view/{torrent_id}/torrent"


def _repair_nyaa_land_torrent_comment(data: bytes, mirror_url: str) -> bytes:
    """Repair only Nyaa Land's known top-level comment-length rewrite.

    The mirror changes ``nyaa.si`` to ``nyaa.land`` in its top-level comment
    but leaves the original bencode byte length.  Its ``info`` dictionary is
    unchanged, yet standard decoders rightly reject the malformed wrapper.
    Restrict the repair to one exact comment before ``info`` so no hashed
    payload byte can be altered; callers still verify the resulting infohash
    against the BTIH advertised by TokyoTosho.
    """
    parsed = urllib.parse.urlsplit(mirror_url)
    match = re.fullmatch(r"/view/([1-9]\d*)/torrent", parsed.path)
    if (
        parsed.scheme != "https" or parsed.hostname != "nyaa.land"
        or parsed.username is not None or parsed.password is not None
        or parsed.port is not None or parsed.query or parsed.fragment
        or match is None
    ):
        return data
    torrent_id = match.group(1).encode("ascii")
    original_comment = b"https://nyaa.si/view/" + torrent_id
    mirrored_comment = b"https://nyaa.land/view/" + torrent_id
    malformed = (
        b"7:comment" + str(len(original_comment)).encode("ascii")
        + b":" + mirrored_comment
    )
    corrected = (
        b"7:comment" + str(len(mirrored_comment)).encode("ascii")
        + b":" + mirrored_comment
    )
    comment_offset = data.find(malformed)
    info_offset = data.find(b"4:info")
    if (
        comment_offset < 0 or data.count(malformed) != 1
        or info_offset < 0 or comment_offset >= info_offset
    ):
        return data
    return data.replace(malformed, corrected, 1)


def _download_torrent(
    url: str, destination: Path, *, timeout: int = 60, attempts: int = 4,
    opener: Any | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    if not url.startswith("https://"):
        raise ValueError("torrent 地址需要使用 HTTPS")
    try:
        _pause_checkpoint(pause_requested)
        data = _fetch_bytes(
            url, max_bytes=MAX_TORRENT_BYTES, timeout=timeout,
            attempts=attempts, opener=opener,
        )
    except ReplenishmentPauseRequested:
        raise
    except (OSError, RuntimeError, TimeoutError):
        mirror_url = _nyaa_torrent_mirror_url(url)
        if mirror_url is None:
            raise
        _pause_checkpoint(pause_requested)
        data = _fetch_bytes(
            mirror_url, max_bytes=MAX_TORRENT_BYTES, timeout=timeout,
            attempts=attempts, opener=opener,
        )
        data = _repair_nyaa_land_torrent_comment(data, mirror_url)
    manifest = _torrent_manifest(data)
    _pause_checkpoint(pause_requested)
    destination.write_bytes(data)
    return manifest


def _selected_companion_indices(
    selection: Mapping[str, Any], *, selected_gap_ids: set[str],
) -> dict[int, list[str]]:
    """Read narrowly-scoped new-media subtitle companion bindings.

    Companion indices are additional download members, not gap fulfilments:
    they must not be folded into ``by_index`` or a subtitle could masquerade
    as a missing-episode payload.  This parser is repeated during manifest
    verification so a hand-edited persisted selection cannot turn an
    arbitrary subtitle into a child sidecar.
    """
    acquisition = selection.get("acquisition")
    if not isinstance(acquisition, Mapping):
        raise ValueError("选中候选缺少 torrent 获取说明")
    raw_map = acquisition.get("companion_subtitle_index_by_media_gap")
    if raw_map is None:
        return {}
    if not isinstance(raw_map, Mapping):
        raise ValueError("伴随字幕索引映射无效")
    gap_map = acquisition.get("file_index_by_gap")
    if not isinstance(gap_map, Mapping):
        raise ValueError("选中候选缺少集号到 torrent 文件索引映射")
    path_map = acquisition.get("file_path_by_index")
    if not isinstance(path_map, Mapping):
        raise ValueError("伴随字幕缺少 torrent 文件路径映射")
    primary_indices = {
        index for values in gap_map.values() if isinstance(values, list)
        for index in values if type(index) is int and index > 0
    }
    result: dict[int, list[str]] = {}
    used_indices: set[int] = set()
    for raw_gap_id, values in raw_map.items():
        gap_id = str(raw_gap_id)
        primary = gap_map.get(gap_id)
        if (
            not gap_id
            or gap_id.startswith("missing_subtitle:")
            or not isinstance(primary, list)
            or len(primary) != 1
            or type(primary[0]) is not int
            or primary[0] <= 0
            or not isinstance(values, list)
            or len(values) != 1
            or type(values[0]) is not int
            or values[0] <= 0
        ):
            raise ValueError("伴随字幕没有唯一媒体 gap/manifest 绑定")
        index = values[0]
        path = path_map.get(str(index), path_map.get(index))
        if (
            index in primary_indices
            or index in used_indices
            or not isinstance(path, str)
            or Path(path).suffix.casefold() not in SUBTITLE_EXTENSIONS
            or _is_supplemental_video_path(path)
        ):
            raise ValueError("伴随字幕 manifest 成员无效或重复")
        # A candidate can advertise a companion for a gap which the selector
        # did not choose this round.  Do not download it, but validate its
        # shape above so a persisted unknown pseudo-gap cannot hide here.
        if gap_id in selected_gap_ids:
            result[index] = [gap_id]
            used_indices.add(index)
    return result


def _selected_indices(selection: Mapping[str, Any]) -> tuple[set[int], dict[int, list[str]]]:
    acquisition = selection.get("acquisition")
    if not isinstance(acquisition, Mapping) or acquisition.get("kind") != "torrent":
        raise ValueError("选中候选缺少 torrent 获取说明")
    gap_map = acquisition.get("file_index_by_gap")
    if not isinstance(gap_map, Mapping):
        raise ValueError("选中候选缺少集号到 torrent 文件索引映射")
    # Companion sidecars belonged to the retired legacy media flow.  The
    # current RootJob subtitle channel obtains and proves one merged bilingual
    # file independently, so a direct Torrent invocation must fail closed if
    # a serialized old companion map slips past its caller.  Current callers
    # strip it before reaching this lower boundary; rejecting here protects
    # manual/recovery callers too, before aria2 sees an extra index.
    if acquisition.get("companion_subtitle_index_by_media_gap") is not None:
        raise ValueError("媒体磁力补源不接受伴随字幕成员")
    by_index: dict[int, list[str]] = {}
    for gap_id in selection.get("selected_gap_ids") or []:
        values = gap_map.get(gap_id)
        if not isinstance(values, list) or not values:
            raise ValueError(f"缺口没有 torrent 文件索引: {gap_id}")
        for value in values:
            if type(value) is not int or value <= 0:
                raise ValueError(f"torrent 文件索引无效: {gap_id}")
            by_index.setdefault(value, []).append(str(gap_id))
    if not by_index:
        raise ValueError("选中候选没有需要获取的文件")
    path_map = acquisition.get("file_path_by_index")
    if not isinstance(path_map, Mapping):
        raise ValueError("选中候选缺少 torrent 文件路径映射")

    def is_subtitle_gap_id(gap_id: str) -> bool:
        """Recognize the durable subtitle coordinates accepted by this adapter.

        The compact selector keeps only ids at this boundary.  Do not infer a
        media kind from an extension after aria2 has already started: a
        non-subtitle coordinate must prove an ordinary video member *before*
        it is serialized into ``--select-file``.  Both legacy and RootJob
        ledger ids are accepted for the standalone subtitle lane.
        """
        return (
            gap_id.startswith("missing_subtitle:")
            or "::missing_subtitle::" in gap_id
        )

    # Every selected primary member is checked here, before preflight or
    # aria2.  The prior episode-only guard left movie/season/manual rows able
    # to map a subtitle (or another non-media member) as a primary payload.
    # A mixed media/subtitle binding is equally unsafe: it would let a video
    # delivery masquerade as a sidecar or vice versa.
    for index, gap_ids in by_index.items():
        path = path_map.get(str(index), path_map.get(index))
        if not isinstance(path, str):
            raise ValueError(f"torrent 文件缺少路径映射: {index}")
        subtitle_gaps = [gap_id for gap_id in gap_ids if is_subtitle_gap_id(gap_id)]
        media_gaps = [gap_id for gap_id in gap_ids if not is_subtitle_gap_id(gap_id)]
        if subtitle_gaps and media_gaps:
            raise ValueError("torrent 文件不能同时绑定媒体与字幕缺口")
        if media_gaps:
            if (
                not _is_ordinary_primary_video_path(path)
                or len(media_gaps) != 1
            ):
                raise ValueError(
                    f"媒体缺口的 torrent 文件不唯一或非正片: {media_gaps[0]}"
                )
        elif Path(path).suffix.casefold() not in SUBTITLE_EXTENSIONS:
            raise ValueError(f"字幕缺口的 torrent 文件不是字幕: {subtitle_gaps[0]}")
    # Exact episode gaps are never batch members.  A selected candidate must
    # serialize one ordinary video for each one, and no video may be replayed
    # against several gaps after a restart/manual edit.
    episode_gap_ids = {
        gap_id for gap_id in (str(value) for value in selection.get("selected_gap_ids") or [])
        if re.fullmatch(r"S\d{2,3}E\d{2,4}", gap_id)
    }
    for gap_id in episode_gap_ids:
        values = gap_map.get(gap_id)
        if not isinstance(values, list) or len(values) != 1 or type(values[0]) is not int:
            raise ValueError(f"episode gap 没有唯一 torrent 视频: {gap_id}")
        index = values[0]
        path = path_map.get(str(index), path_map.get(index))
        if (
            not isinstance(path, str)
            or not _is_ordinary_primary_video_path(path)
            or len(by_index.get(index, [])) != 1
        ):
            raise ValueError(f"episode gap 的 torrent 视频不唯一或非正片: {gap_id}")
    return set(by_index), by_index


def _verify_manifest(selection: Mapping[str, Any], manifest: Mapping[str, Any]) -> tuple[set[int], dict[int, list[str]]]:
    indices, by_index = _selected_indices(selection)
    acquisition = selection["acquisition"]
    expected_hash = str(selection.get("infohash") or "").casefold()
    actual_hash = str(manifest.get("infohash") or "").casefold()
    if expected_hash and expected_hash not in {actual_hash, _base32_infohash(actual_hash)}:
        raise ValueError("torrent infohash 与候选目录不一致")
    files = manifest.get("files") if isinstance(manifest.get("files"), Mapping) else {}
    size_map = acquisition.get("file_size_by_index") if isinstance(acquisition.get("file_size_by_index"), Mapping) else {}
    path_map = acquisition.get("file_path_by_index") if isinstance(acquisition.get("file_path_by_index"), Mapping) else {}
    for index in indices:
        row = files.get(index)
        if not isinstance(row, Mapping):
            raise ValueError(f"torrent 不含目录声明的文件索引: {index}")
        expected_size = size_map.get(str(index), size_map.get(index))
        if type(expected_size) is not int or expected_size <= 0 or row.get("size") != expected_size:
            raise ValueError(f"torrent 文件大小与目录不一致: {index}")
        expected_path = path_map.get(str(index), path_map.get(index))
        if not isinstance(expected_path, str) or expected_path != row.get("path"):
            raise ValueError(f"torrent 文件路径与目录不一致: {index}")
    return indices, by_index


def _base32_infohash(hex_hash: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", hex_hash):
        return ""
    import base64
    return base64.b32encode(bytes.fromhex(hex_hash)).decode("ascii").rstrip("=").casefold()


def _direct_download_env(base: Mapping[str, str]) -> dict[str, str]:
    """Clone an environment with every HTTP proxy removed.

    Search indexes may need the proxy; tracker announces and BT peer
    traffic are direct connections and must never inherit it (aria2 reads
    the ``http_proxy`` environment family for its HTTP tracker requests).
    """
    cleaned = dict(base)
    for key in ("http_proxy", "https_proxy", "all_proxy", "no_proxy",
                "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
        cleaned.pop(key, None)
    return cleaned


def _payload_is_complete(
    payload_dir: Path, acquisition: Mapping[str, Any], indices: set[int],
) -> bool:
    """Trust a retained payload only after aria2 finished and every file matches."""
    if not payload_dir.is_dir() or any(payload_dir.rglob("*.aria2")):
        return False
    size_map = acquisition.get("file_size_by_index")
    path_map = acquisition.get("file_path_by_index")
    if not isinstance(size_map, Mapping) or not isinstance(path_map, Mapping):
        return False
    try:
        for index in indices:
            _find_download(
                payload_dir,
                str(path_map[str(index)]),
                int(size_map[str(index)]),
            )
    except (KeyError, TypeError, ValueError):
        return False
    return True


def _assert_payload_has_no_incomplete_markers(payload_dir: Path) -> None:
    """Fail before upload if aria2 left any incomplete-file marker behind."""
    marker = next(payload_dir.rglob("*.aria2"), None) if payload_dir.is_dir() else None
    if marker is not None:
        raise ReplenishmentCandidateError(
            f"aria2 未完成文件仍在补源 payload: {marker.name}",
            stage="candidate_payload_validation",
        )


def _preflight(
    selection_wrapper: Mapping[str, Any], workspace: Path,
    *, resume_workspace: Path | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    bundle = selection_wrapper.get("selection")
    selections = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(selections, list) or not selections:
        raise ReplenishmentInfrastructureError(
            "选择文件缺少 selections", stage="artifact_validation",
        )
    acquisition_kinds = {
        str(row.get("acquisition", {}).get("kind") or "")
        for row in selections if isinstance(row, Mapping)
        and isinstance(row.get("acquisition"), Mapping)
    }
    if acquisition_kinds != {"torrent"}:
        raise ReplenishmentInfrastructureError(
            "选择包含非 Torrent acquisition；本地适配器失败关闭",
            stage="unsupported_provider_lane",
        )
    if shutil.which("aria2c") is None:
        raise ReplenishmentInfrastructureError(
            "运行环境缺少 aria2c", stage="local_dependency",
        )
    _pause_checkpoint(pause_requested)
    workspace.mkdir(parents=True, exist_ok=True)
    selected_bytes = 0
    selected_files = 0
    reusable_bytes = 0
    verified: list[dict[str, Any]] = []
    for offset, selection in enumerate(selections, start=1):
        _pause_checkpoint(pause_requested)
        if not isinstance(selection, Mapping):
            raise ValueError("selection 项格式无效")
        acquisition = selection.get("acquisition")
        url = acquisition.get("url") if isinstance(acquisition, Mapping) else None
        if not isinstance(url, str):
            raise ValueError("选中候选缺少 torrent URL")
        torrent_path = workspace / f"candidate-{offset:02d}.torrent"
        try:
            if pause_requested is None:
                manifest = _download_torrent(url, torrent_path)
            else:
                manifest = _download_torrent(
                    url,
                    torrent_path,
                    pause_requested=pause_requested,
                )
            indices, _by_index = _verify_manifest(selection, manifest)
        except ReplenishmentPauseRequested:
            raise
        except (OSError, ValueError, RuntimeError, urllib.error.URLError) as exc:
            raise ReplenishmentCandidateError(
                str(exc), stage="candidate_preflight", candidate=selection,
            ) from exc
        files = manifest["files"]
        byte_count = sum(int(files[index]["size"]) for index in indices)
        selected_bytes += byte_count
        selected_files += len(indices)
        if resume_workspace is not None and _payload_is_complete(
            resume_workspace / f"download-{offset:02d}" / "payload",
            acquisition,
            indices,
        ):
            reusable_bytes += byte_count
        verified.append({
            "release_name": selection.get("release_name"),
            "torrent": url,
            "infohash": manifest["infohash"],
            "selected_indices": sorted(indices),
            "selected_files": len(indices),
            "selected_bytes": byte_count,
            "torrent_path": str(torrent_path),
            "manifest": manifest,
        })
    free = shutil.disk_usage(workspace).free
    remaining_bytes = selected_bytes - reusable_bytes
    required = int(remaining_bytes * 1.15) + 1024 ** 3
    if free < required:
        raise ReplenishmentInfrastructureError(
            f"补源暂存空间不足: required={required}, free={free}",
            stage="local_capacity",
        )
    return {
        "status": "verified",
        "selected_files": selected_files,
        "selected_bytes": selected_bytes,
        "reusable_bytes": reusable_bytes,
        "remaining_bytes": remaining_bytes,
        "free_bytes": free,
        "required_bytes": required,
        "candidates": verified,
    }


def _alist_client() -> AListClient:
    base_url = os.getenv("ALIST_URL", "http://127.0.0.1:5244")
    username = os.getenv("ALIST_USERNAME", "")
    password = os.getenv("ALIST_PASSWORD", "")
    if not username or not password:
        raise ValueError("AList 上传凭据未配置")
    client = AListClient(
        base_url, username, password, timeout=60, retries=4,
        allow_insecure_http=base_url.startswith("http://alist:") or base_url.startswith("http://127.0.0.1"),
    )
    client.login()
    return client


def _selection_acquisition_kind(selection: Mapping[str, Any]) -> str:
    acquisition = selection.get("acquisition")
    return str(acquisition.get("kind") or "") if isinstance(acquisition, Mapping) else ""


def _wrapper_for_selections(
    wrapper: Mapping[str, Any], selections: list[dict[str, Any]],
) -> dict[str, Any]:
    output = dict(wrapper)
    bundle = dict(wrapper.get("selection") or {})
    bundle["selections"] = selections
    output["selection"] = bundle
    return output


def _preflight_dispatch(
    wrapper: Mapping[str, Any], workspace: Path, *, resume_workspace: Path | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    bundle = wrapper.get("selection")
    rows = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise ReplenishmentInfrastructureError(
            "选择文件缺少 selections", stage="artifact_validation",
        )
    if any(
        _selection_acquisition_kind(row) != "torrent"
        or candidate_capability_error(row) is not None
        for row in rows
    ):
        raise ReplenishmentInfrastructureError(
            "本地适配器只接受可执行的 magnet/torrent selection",
            stage="artifact_validation",
        )
    _pause_checkpoint(pause_requested)
    arguments = (
        _wrapper_for_selections(wrapper, [dict(row) for row in rows]),
        workspace / "torrent",
    )
    keyword_arguments = {
        "resume_workspace": resume_workspace / "torrent" if resume_workspace else None,
    }
    if pause_requested is not None:
        keyword_arguments["pause_requested"] = pause_requested
    return _preflight(*arguments, **keyword_arguments)


def _ffprobe_archive_video(path: Path) -> dict[str, Any]:
    """Verify one retained payload through the shared bounded admission."""
    try:
        return probe_local_video_stream(path)
    except VideoAdmissionError as exc:
        if exc.infrastructure:
            raise ReplenishmentInfrastructureError(
                f"视频准入环境不可用: {exc.reason}",
                stage="local_dependency",
            ) from exc
        raise ReplenishmentCandidateError(
            f"视频流准入失败: {path.name}: {exc.reason}",
            stage="candidate_payload",
        ) from exc


def _verify_video_payload(
    path: Path,
    expected_size: int,
    selection: Mapping[str, Any],
) -> None:
    """Apply local-video admission before any AList staging upload.

    Torrent manifest size checks only prove that aria2 received the bytes the
    torrent advertised.  They do not prove that a tiny fixture, HTML error
    page, or other non-video payload renamed to ``.mkv`` is safe to expose to
    the Engine.  The formal writer repeats the byte guard as a final boundary;
    this earlier local check keeps rejected payloads out of remote staging.
    """
    if not video_size_is_admissible(expected_size):
        raise ReplenishmentCandidateError(
            "候选视频小于正式库准入下限 "
            f"{minimum_video_bytes()} bytes: {path.name}",
            stage="candidate_payload_validation",
            candidate=selection,
        )
    _ffprobe_archive_video(path)


def _acquire_dispatch(
    wrapper: Mapping[str, Any],
    workspace: Path,
    *,
    automatic: bool = True,
    client: AListClient | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Dispatch only exact Torrent selections to the local materializer."""
    bundle = wrapper.get("selection")
    rows = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise ReplenishmentInfrastructureError(
            "选择文件缺少 selections", stage="artifact_validation",
        )
    if any(
        _selection_acquisition_kind(row) != "torrent"
        or candidate_capability_error(row) is not None
        for row in rows
    ):
        raise ReplenishmentInfrastructureError(
            "本地适配器拒绝不可执行 provider/acquisition；没有云端 fallback",
            stage="artifact_validation",
        )
    if not automatic:
        raise ReplenishmentInfrastructureError(
            "补源必须由自动调度器创建任务 staging",
            stage="automatic_route_required",
        )
    _pause_checkpoint(pause_requested)
    return _acquire(
        wrapper,
        workspace,
        client=client,
        pause_requested=pause_requested,
    )


def _safe_name(value: str, *, limit: int = 180) -> str:
    value = re.sub(r"[\\/:*?\"<>|\x00-\x1f]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip(" .")
    return (value or "补源文件")[:limit].rstrip(" .")


def _verify_remote_uploads(
    client: AListClient,
    remote_root: str,
    uploaded: list[dict[str, Any]],
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> None:
    """Wait for cloud-backed AList listings to expose committed uploads.

    AList's upload endpoint can return before a provider refresh exposes the
    new row and its final size.  Treating that short visibility window as a
    failed torrent discards a fully downloaded candidate and causes an
    unnecessary retry, so poll the refreshed directory for a bounded period
    before declaring the acquisition failed.
    """
    timeout = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_ARRIVAL_TIMEOUT", 120, 10, 600,
    )
    deadline = time.monotonic() + timeout
    last_files: dict[str, int] = {}
    while True:
        _pause_checkpoint(pause_requested)
        rows = client.list(remote_root, refresh=True)
        last_files = {
            str(row.get("name")): int(row.get("size") or 0)
            for row in rows if not row.get("is_dir")
        }
        if all(last_files.get(row["remote_name"]) == row["size"] for row in uploaded):
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            missing = [
                row["remote_name"] for row in uploaded
                if last_files.get(row["remote_name"]) != row["size"]
            ]
            raise ValueError(f"AList 到盘核验失败: {', '.join(missing[:3])}")
        time.sleep(min(5.0, remaining))


def _remote_upload_matches(
    client: AListClient, remote_root: str, remote_name: str, size: int,
) -> bool:
    rows = client.try_list(remote_root, refresh=True) or []
    return any(
        not row.get("is_dir")
        and str(row.get("name") or "") == remote_name
        and int(row.get("size") or 0) == size
        for row in rows
    )


def _automatic_upload(
    client: AListClient,
    remote_root: str,
    row: dict[str, Any],
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> None:
    """Upload one file from task-owned staging and verify its exact size."""
    remote_name = str(row["remote_name"])
    source = Path(row["source"]).resolve()
    size = int(row["size"])
    if not source.is_file() or source.stat().st_size != size:
        raise ReplenishmentCandidateError(
            f"自动补源本地文件大小无效: {source}", stage="candidate_payload_validation",
        )
    target = join_remote(remote_root, remote_name)
    _pause_checkpoint(pause_requested)
    existing = client.exact_file_info(target)
    if existing is not None:
        existing_size = int(existing.get("size") or 0)
        if existing_size == size:
            return
        raise ReplenishmentDeliveryError(
            f"自动补源 staging 已有不同大小文件: {target}", stage="delivery_upload",
        )
    content_type = {
        ".srt": "application/x-subrip",
        ".ass": "text/x-ass",
        ".ssa": "text/x-ssa",
        ".vtt": "text/vtt",
    }.get(source.suffix.casefold(), "application/octet-stream")
    _pause_checkpoint(pause_requested)
    client.upload_file(target, source, content_type)


def _find_download(payload: Path, relative_path: str, size: int) -> Path:
    suffix = relative_path.replace("\\", "/")
    basename = Path(relative_path).name
    candidates = [
        path for path in payload.rglob("*")
        if path.is_file() and path.name == basename
        and path.as_posix().endswith(suffix) and path.stat().st_size == size
    ]
    if len(candidates) != 1:
        raise ValueError(f"下载文件定位结果异常: {relative_path}; matches={len(candidates)}")
    return candidates[0]


def _ensure_automatic_staging_root(
    client: AListClient,
    remote_parent: str,
    remote_root: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> None:
    """Create the known task-owned staging path one level at a time."""
    parent = remote_parent.rstrip("/")
    root = remote_root.rstrip("/")
    if not parent or not root.startswith(parent + "/"):
        raise ReplenishmentInfrastructureError(
            "自动补源 staging 路径超出受管父目录", stage="staging_root",
        )
    job_root = posixpath.dirname(root)
    if job_root != parent and not job_root.startswith(parent + "/"):
        raise ReplenishmentInfrastructureError(
            "自动补源任务 staging 父目录无效", stage="staging_root",
        )
    for directory in dict.fromkeys(
        path for path in (parent, job_root, root) if path
    ):
        _pause_checkpoint(pause_requested)
        client.mkdir(directory)


def _acquire(
    selection_wrapper: Mapping[str, Any],
    workspace: Path,
    *,
    client: AListClient | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    request = selection_wrapper.get("request") if isinstance(selection_wrapper.get("request"), Mapping) else {}
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    tmdb_id = media.get("tmdb_id")
    title = _safe_name(str(media.get("title") or f"TMDB-{tmdb_id}"), limit=60)
    remote_parent = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_UNSCRAPED_ROOT",
        "/quark/影视/ScrapeFlow/补源",
    ).rstrip("/")
    automatic_parent = selection_wrapper.get("automatic_staging_parent")
    if automatic_parent is not None:
        if (
            not isinstance(automatic_parent, str)
            or not automatic_parent.startswith("/")
            or posixpath.normpath(automatic_parent) != automatic_parent
            or posixpath.basename(automatic_parent) != "补源"
        ):
            raise ReplenishmentInfrastructureError(
                "自动补源 staging 父目录无效", stage="staging_root",
            )
        remote_parent = automatic_parent
    workspace_key = _selection_workspace_key(selection_wrapper)
    remote_name = _safe_name(f"ScrapeFlow补源-{tmdb_id}-{title}-{workspace_key}")
    automatic_staging = selection_wrapper.get("automatic_staging_root")
    if automatic_staging is not None and (not isinstance(automatic_staging, str) or not automatic_staging):
        raise ReplenishmentInfrastructureError(
            "自动补源缺少受管 staging_root", stage="staging_root",
        )
    remote_root = str(
        automatic_staging
        or join_remote(remote_parent, remote_name)
    )
    expected_prefix = remote_parent.rstrip("/") + "/"
    if (
        not remote_root.startswith(expected_prefix)
        or posixpath.normpath(remote_root) != remote_root
        or remote_root == remote_parent.rstrip("/")
    ):
        raise ReplenishmentInfrastructureError(
            "自动补源 staging_root 超出受管根", stage="staging_root",
        )
    if is_production_test_media_path(remote_root):
        raise ReplenishmentInfrastructureError(
            "保留的生产 E2E 测试 staging 路径不可用于补源",
            stage="staging_root",
        )
    uploaded: list[dict[str, Any]] = []
    raw_request_rows = request.get("gaps")
    if not isinstance(raw_request_rows, list) or any(
        not isinstance(gap, Mapping) for gap in raw_request_rows
    ):
        raise ReplenishmentCandidateError(
            "补源请求 gap 证据无效，拒绝下载",
            stage="lane_validation",
        )
    request_rows = [dict(gap) for gap in raw_request_rows]
    if not request_rows:
        raise ReplenishmentCandidateError(
            "补源请求没有可执行 gap",
            stage="lane_validation",
        )
    request_kinds = {str(gap.get("kind") or "") for gap in request_rows}
    if request_kinds - {
        "missing_episode", "missing_season", "missing_media", "missing_subtitle",
    }:
        raise ReplenishmentCandidateError(
            "补源请求包含不支持的 gap 类型",
            stage="lane_validation",
        )
    if "missing_subtitle" in request_kinds and request_kinds - {"missing_subtitle"}:
        raise ReplenishmentCandidateError(
            "字幕缺口必须使用独立 sidecar 补源请求",
            stage="lane_validation",
        )
    # ``client`` may be injected by the automatic coordinator so the staging
    # upload and its later Engine readback use the same AList session.  Do not
    # overwrite it below with a second client.
    payload_verified = False
    delivery_stage = "candidate_preflight"
    try:
        request_gap_kinds = {
            str(gap.get("id")): str(gap.get("kind") or "")
            for gap in request.get("gaps") or []
            if isinstance(gap, Mapping) and isinstance(gap.get("id"), str)
            and gap.get("id")
        }
        _pause_checkpoint(pause_requested)
        if pause_requested is None:
            preflight = _preflight(
                selection_wrapper, workspace / "preflight", resume_workspace=workspace,
            )
        else:
            preflight = _preflight(
                selection_wrapper,
                workspace / "preflight",
                resume_workspace=workspace,
                pause_requested=pause_requested,
            )
        bundle = selection_wrapper["selection"]
        for offset, selection in enumerate(bundle["selections"], start=1):
            candidate_dir = workspace / f"download-{offset:02d}"
            payload_dir = candidate_dir / "payload"
            _pause_checkpoint(pause_requested)
            payload_dir.mkdir(parents=True, exist_ok=True)
            verified = preflight["candidates"][offset - 1]
            manifest = verified["manifest"]
            torrent_path = Path(verified["torrent_path"])
            try:
                indices, by_index = _verify_manifest(selection, manifest)
            except ValueError as exc:
                raise ReplenishmentCandidateError(
                    str(exc), stage="candidate_manifest", candidate=selection,
                ) from exc
            acquisition = selection["acquisition"]
            try:
                companion_by_index = _selected_companion_indices(
                    selection,
                    selected_gap_ids={
                        str(value) for value in selection.get("selected_gap_ids") or []
                    },
                )
            except ValueError as exc:
                raise ReplenishmentCandidateError(
                    str(exc), stage="candidate_manifest", candidate=selection,
                ) from exc
            if _payload_is_complete(payload_dir, acquisition, indices):
                print(
                    f"[replenishment] 复用已验证下载 {offset}/{len(bundle['selections'])}: "
                    f"{selection.get('release_name')}",
                    flush=True,
                )
            else:
                command = [
                    "aria2c", "--seed-time=0", "--file-allocation=none",
                    "--allow-overwrite=true", "--auto-file-renaming=false",
                    "--summary-interval=60", "--console-log-level=notice",
                    # In mainland deployments the HTTP proxy exists for the
                    # blocked search indexes only; tracker announces and peer
                    # traffic must stay direct.  DHT/PEX/LPD give the swarm a
                    # chance even when every tracker is unreachable.
                    "--enable-dht=true", "--enable-peer-exchange=true",
                    "--bt-enable-lpd=true",
                    f"--bt-stop-timeout={_bounded_seconds('SCRAPEFLOW_REPLENISHMENT_BT_IDLE_TIMEOUT', 600, 60, 3600)}",
                    f"--dir={payload_dir}", f"--select-file={','.join(str(i) for i in sorted(indices))}",
                    str(torrent_path),
                ]
                print(f"[replenishment] 下载候选 {offset}/{len(bundle['selections'])}: {selection.get('release_name')}", flush=True)
                try:
                    _pause_checkpoint(pause_requested)
                    completed = subprocess.run(
                        command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        timeout=_bounded_seconds(
                            "SCRAPEFLOW_REPLENISHMENT_TORRENT_TIMEOUT", 21600, 300, 86400,
                        ),
                        env=_direct_download_env(os.environ),
                    )
                except subprocess.TimeoutExpired as exc:
                    raise ReplenishmentCandidateError(
                        "aria2c 下载超过总时限", stage="candidate_download",
                        candidate=selection,
                    ) from exc
                if completed.returncode != 0:
                    tail = " ".join(completed.stdout.splitlines()[-8:])[:1200]
                    raise ReplenishmentCandidateError(
                        f"aria2c 下载失败: {tail}", stage="candidate_download",
                        candidate=selection,
                    )
            _assert_payload_has_no_incomplete_markers(payload_dir)
            size_map = acquisition["file_size_by_index"]
            path_map = acquisition["file_path_by_index"]
            for index in sorted(indices):
                expected_size = int(size_map[str(index)])
                relative_path = str(path_map[str(index)])
                try:
                    source = _find_download(payload_dir, relative_path, expected_size)
                except ValueError as exc:
                    raise ReplenishmentCandidateError(
                        str(exc), stage="candidate_payload_validation", candidate=selection,
                    ) from exc
                gaps = sorted(set(by_index.get(index, [])))
                companion_for = sorted(set(companion_by_index.get(index, [])))
                if companion_for and gaps:
                    raise ReplenishmentCandidateError(
                        "伴随字幕与直接 gap 绑定冲突",
                        stage="candidate_payload_validation", candidate=selection,
                    )
                if companion_for:
                    if len(companion_for) != 1:
                        raise ReplenishmentCandidateError(
                            "伴随字幕没有唯一媒体 gap 绑定",
                            stage="candidate_payload_validation", candidate=selection,
                        )
                    paired_gap_id = companion_for[0]
                    paired_indices = acquisition.get("file_index_by_gap", {}).get(paired_gap_id)
                    if (
                        not isinstance(paired_indices, list) or len(paired_indices) != 1
                        or type(paired_indices[0]) is not int or paired_indices[0] <= 0
                    ):
                        raise ReplenishmentCandidateError(
                            "伴随字幕缺少唯一正片 manifest 索引",
                            stage="candidate_payload_validation", candidate=selection,
                        )
                    paired_video_index = paired_indices[0]
                    paired_video_path = path_map.get(str(paired_video_index))
                    extension = source.suffix.casefold()
                    if (
                        extension not in SUBTITLE_EXTENSIONS
                        or _is_supplemental_video_path(relative_path)
                        or not isinstance(paired_video_path, str)
                    ):
                        raise ReplenishmentCandidateError(
                            f"伴随字幕不是可配对的字幕格式: {source.name}",
                            stage="candidate_payload_validation", candidate=selection,
                        )
                    remote_file = _safe_name(
                        f"{paired_gap_id} - {source.stem}", limit=170,
                    ) + extension
                    uploaded.append({
                        "gap_ids": [paired_gap_id],
                        "companion_for_gap_ids": [paired_gap_id],
                        "paired_video_index": paired_video_index,
                        "paired_video_source_name": paired_video_path,
                        "source_name": relative_path, "provider_path": relative_path,
                        "manifest_index": index, "source": source,
                        "remote_name": remote_file, "size": expected_size,
                        "kind": "subtitle", "subtitle_language": "zh",
                    })
                    continue
                gap_prefix = "+".join(gaps)
                extension = source.suffix.casefold()
                kinds = {request_gap_kinds.get(gap_id) for gap_id in gaps}
                if not gaps or None in kinds or "" in kinds:
                    raise ReplenishmentCandidateError(
                        "选中文件缺少受审计缺口绑定",
                        stage="candidate_payload_validation",
                        candidate=selection,
                    )
                if kinds == {"missing_subtitle"}:
                    allowed_extensions = SUBTITLE_EXTENSIONS
                elif "missing_subtitle" not in kinds:
                    allowed_extensions = VIDEO_EXTENSIONS
                else:
                    # A manifest member must be either a video move or one
                    # exact subtitle sidecar.  Sharing it across both lanes
                    # would defeat the formal-library pairing proof.
                    raise ReplenishmentCandidateError(
                        "选中文件同时绑定媒体和字幕缺口",
                        stage="candidate_payload_validation",
                        candidate=selection,
                    )
                if extension not in allowed_extensions:
                    raise ReplenishmentCandidateError(
                        f"选中文件不是本次缺口支持的媒体格式: {source.name}",
                        stage="candidate_payload_validation",
                        candidate=selection,
                    )
                if extension in VIDEO_EXTENSIONS:
                    _verify_video_payload(source, expected_size, selection)
                remote_file = _safe_name(f"{gap_prefix} - {source.stem}", limit=170) + extension
                uploaded.append({
                    "gap_ids": gaps, "source": source, "remote_name": remote_file,
                    "size": expected_size, "manifest_index": index,
                    "source_name": relative_path, "provider_path": relative_path,
                    "kind": "subtitle" if extension in SUBTITLE_EXTENSIONS else "video",
                })
        if not uploaded:
            raise ReplenishmentCandidateError(
                "没有可上传的补源媒体或字幕",
                stage="candidate_payload_validation",
            )
        payload_verified = True
        delivery_stage = "delivery_connect"
        _pause_checkpoint(pause_requested)
        client = client or _alist_client()
        login = getattr(client, "login", None)
        if callable(login) and not getattr(client, "token", None):
            _pause_checkpoint(pause_requested)
            login()
        delivery_stage = "delivery_prepare"
        if pause_requested is None:
            _ensure_automatic_staging_root(client, remote_parent, remote_root)
        else:
            _ensure_automatic_staging_root(
                client,
                remote_parent,
                remote_root,
                pause_requested=pause_requested,
            )
        has_video = any(row.get("kind") == "video" for row in uploaded)
        has_subtitle = any(row.get("kind") == "subtitle" for row in uploaded)
        # A mixed provider candidate must never expose its subtitle members
        # to the child Engine planner.  The planner is deliberately strict
        # about subtitle companions and could otherwise turn a sidecar for an
        # already-existing episode into a child-plan problem.  Keep both
        # subroots inside the one task-owned attempt so final cleanup remains
        # bounded, but hand only ``media`` to the child.
        mixed_delivery = has_video and has_subtitle
        media_staging_root = join_remote(remote_root, "media") if mixed_delivery else remote_root
        subtitle_staging_root = join_remote(remote_root, "subtitles") if mixed_delivery else remote_root
        if mixed_delivery:
            _pause_checkpoint(pause_requested)
            client.mkdir(media_staging_root)
            _pause_checkpoint(pause_requested)
            client.mkdir(subtitle_staging_root)
        for row in uploaded:
            row["delivery_root"] = (
                subtitle_staging_root if row.get("kind") == "subtitle"
                else media_staging_root
            )
        delivery_stage = "delivery_upload"
        for offset, row in enumerate(uploaded, start=1):
            print(f"[replenishment] 上传 {offset}/{len(uploaded)}: {row['remote_name']}", flush=True)
            if pause_requested is None:
                _automatic_upload(client, str(row["delivery_root"]), row)
            else:
                _automatic_upload(
                    client,
                    str(row["delivery_root"]),
                    row,
                    pause_requested=pause_requested,
                )
        delivery_stage = "delivery_visibility"
        grouped_uploads: dict[str, list[dict[str, Any]]] = {}
        for row in uploaded:
            delivery_root = str(row["delivery_root"])
            grouped_uploads.setdefault(delivery_root, []).append(row)
        for delivery_root, rows in grouped_uploads.items():
            if pause_requested is None:
                _verify_remote_uploads(client, delivery_root, rows)
            else:
                _verify_remote_uploads(
                    client,
                    delivery_root,
                    rows,
                    pause_requested=pause_requested,
                )
        delivery_files: list[dict[str, Any]] = []
        for row in uploaded:
            delivery_files.append({
                "path": join_remote(str(row["delivery_root"]), str(row["remote_name"])),
                "size": int(row["size"]),
                "gap_ids": list(row["gap_ids"]),
                "kind": str(row["kind"]),
            })
        # Both local bytes and the remote task tree remain retry/reconcile
        # evidence.  The coordinator owns their eventual cleanup after a
        # formal readback and targeted audit prove the gap disappeared.
        return {
            "lane": "magnet",
            "attempt_id": posixpath.basename(remote_root.rstrip("/")),
            "staging_root": remote_root,
            "files": delivery_files,
        }
    except BaseException as exc:
        if getattr(exc, "pause_requested", False) is True:
            raise
        delivery_failure = payload_verified and isinstance(exc, Exception)
        # Never recursively remove the deterministic remote delivery root on
        # an ambiguous failure. It may already contain a verified object from
        # this or an earlier attempt. A later retry inspects the exact objects
        # in place; cleanup remains limited to this attempt's staging prefix.
        if delivery_failure:
            # Keep both the verified local payload and any exact-size remote
            # objects.  The deterministic workspace/remote names make the next
            # retry skip the torrent and already committed uploads.
            if isinstance(exc, ReplenishmentDeliveryError):
                raise
            raise ReplenishmentDeliveryError(str(exc), stage=delivery_stage) from exc
        # Capacity/dependency/orchestration failures do not invalidate bytes
        # retained by a previous attempt. Candidate failures do.
        if not isinstance(exc, ReplenishmentInfrastructureError):
            _pause_checkpoint(pause_requested)
            shutil.rmtree(workspace, ignore_errors=True)
        raise


def _bounded_seconds(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError(f"{name} 需要是整数秒")
    value = int(raw)
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} 需要在 {minimum}–{maximum} 秒之间")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ScrapeFlow 本地补源适配器")
    subparsers = parser.add_subparsers(dest="action", required=True)
    search = subparsers.add_parser("search")
    search.add_argument("--request", type=Path, required=True)
    search.add_argument("--output", type=Path, required=True)
    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--selection", type=Path, required=True)
    preflight.add_argument("--output", type=Path, required=True)
    acquire = subparsers.add_parser("acquire")
    acquire.add_argument("--selection", type=Path, required=True)
    acquire.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    if args.action == "search":
        request = _load(args.request)
        root = Path(os.getenv(
            "SCRAPEFLOW_REPLENISHMENT_DOWNLOAD_DIR",
            "/var/tmp/scrapeflow/replenishment",
        ))
        with _search_capacity_lease(root):
            result = _search(request)
    else:
        wrapper = _load(args.selection)
        root = Path(os.getenv("SCRAPEFLOW_REPLENISHMENT_DOWNLOAD_DIR", "/var/tmp/scrapeflow/replenishment"))
        workspace = (
            root / f"preflight-{uuid.uuid4().hex}"
            if args.action == "preflight"
            else root / f"acquire-{_selection_workspace_key(wrapper)}"
        )
        try:
            if args.action == "preflight":
                result = _preflight_dispatch(wrapper, workspace)
            else:
                with _workspace_lease(root, _selection_workspace_key(wrapper)):
                    result = _acquire_dispatch(wrapper, workspace, automatic=True)
        except Exception as exc:
            if args.action == "acquire":
                _atomic_json(args.output, {
                    "status": "failed",
                    "failure": {
                        "scope": str(getattr(exc, "failure_scope", "infrastructure")),
                        "stage": str(getattr(exc, "failure_stage", "unclassified")),
                        "reusable_candidate": bool(
                            getattr(exc, "reusable_candidate", False)
                        ),
                        "exclude_candidate": bool(
                            getattr(exc, "exclude_candidate", False)
                        ),
                        "candidate": dict(getattr(exc, "candidate", {}) or {}),
                        "workspace": str(workspace),
                        "retained_workspace": workspace.exists(),
                        "workspace_key": _selection_workspace_key(wrapper),
                        "message": str(exc),
                    },
                })
            raise
    _atomic_json(args.output, result)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ReplenishmentDeliveryError as exc:
        print("[replenishment] failure_scope=delivery reusable_candidate=true exclude_candidate=false", flush=True)
        print(f"补源适配器失败: {exc}", flush=True)
        raise SystemExit(1)
    except ReplenishmentCandidateError as exc:
        print("[replenishment] failure_scope=candidate reusable_candidate=false exclude_candidate=true", flush=True)
        print(f"补源适配器失败: {exc}", flush=True)
        raise SystemExit(1)
    except (OSError, ValueError, RuntimeError, ApiError, json.JSONDecodeError) as exc:
        print("[replenishment] failure_scope=infrastructure reusable_candidate=false exclude_candidate=false", flush=True)
        print(f"补源适配器失败: {exc}", flush=True)
        raise SystemExit(1)
