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
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Sequence

from engine.scrapeflow.source_inventory import (
    SourceNode,
    collect_all_files,
    count_video_files,
    direct_video_file_count,
    has_only_subtitles,
)


# ---------------------------------------------------------------------------
# Shared season-folder regex (same evidence as replenishment_matching.py)
# ---------------------------------------------------------------------------

# Matches: "Season 1", "season01", "S1", "S01", "第1季", "第01季", …
# Deliberately does NOT match "S01E02" (episode files).
_SEASON_DIR_RE = re.compile(
    r"""
    (?:
        (?:season|s)\s*0*(\d{1,3})   # Season 1 / S01 / season01
        | 第\s*0*(\d{1,3})\s*季       # 第1季 / 第01季
    )$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Minimum size (bytes) for a file to be considered a "real" movie
_MOVIE_MIN_BYTES = 200 * 1024 * 1024  # 200 MiB


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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_WC_NAMESPACE = uuid.UUID("b5c6d7e8-f9a0-4002-8003-a00000000002")


def _work_unit_id(root_task_id: str, boundary_key: str) -> str:
    return str(uuid.uuid5(_WC_NAMESPACE, f"{root_task_id}::{boundary_key}"))


def _is_season_dir(name: str) -> bool:
    return bool(_SEASON_DIR_RE.fullmatch(name.strip()))


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
    if _is_season_dir(node.name):
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


def _single_large_video(node: SourceNode) -> bool:
    """True if the node contains exactly one large video file (movie-shaped)."""
    all_files = collect_all_files(node)
    video_files = [f for f in all_files if f.object_type == "video"]
    if len(video_files) == 1 and video_files[0].size >= _MOVIE_MIN_BYTES:
        return True
    return False


def _propose_media_context(node: SourceNode) -> str:
    """Heuristic: is this more movie-shaped or TV-shaped?"""
    all_files = collect_all_files(node)
    video_files = [f for f in all_files if f.object_type == "video"]
    if not video_files:
        return "unknown"
    # One large video → movie
    if len(video_files) == 1 and video_files[0].size >= _MOVIE_MIN_BYTES:
        return "movie"
    # Multiple videos, any season dir in children → tv
    if any(_is_season_dir(c.name) for c in node.children):
        return "tv"
    # More than ~4 videos → tv (episode pack)
    if len(video_files) >= 4:
        return "tv"
    # 2-3 videos with no season dir → could be short movie trilogy or OVA
    return "unknown"


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
    # --- Rule 1: Season directory ---------------------------------------
    if _is_season_dir(node.name):
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
    season_children = [c for c in node.children if _is_season_dir(c.name)]
    root_videos = direct_video_file_count(node)

    # --- Rule 3: Series container (multiple titled sub-works) -----------
    # Condition: ≥2 titled children, no season dirs at root level, no root
    # videos that would suggest the root itself is a single work.  When every
    # titled child is movie-shaped (exactly one large video), the container is
    # a MOVIE_COLLECTION instead of a generic series container.
    if (
        len(titled_children) >= 2
        and len(season_children) == 0
        and root_videos == 0
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
            ))
        return candidates

    # --- Rule 4: Movie collection (multiple subdirs, each one large video)
    if (
        len(season_children) == 0
        and root_videos == 0
        and len(node.children) >= 2
        and all(_single_large_video(c) for c in node.children)
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
