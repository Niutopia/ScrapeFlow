"""WorkUnit domain models, identity evidence, and lifecycle records.

A WorkUnit represents a single distinct creative work discovered within an
IntakeSource (or RootJob). While a single RootJob might contain a series container
with multiple works (e.g. Fate/Zero, Fate/stay night), each WorkUnit has its own:
  - IdentityEvidence (names, years, episode structure, parent container clues)
  - TMDB identity match (AutoMatch)
  - Reconciliation outcome (new_work, merge_existing, duplicate, gap)
  - Lifecycle state (pending -> confirmed / uncertain / failed)

All classes are immutable/frozen dataclasses with serialization helpers and atomic
file persistence.
"""

from __future__ import annotations

import json
import re
import tempfile
import unicodedata
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from engine.scrapeflow.boundary_analysis import WorkCandidate
from engine.scrapeflow.source_inventory import (
    SourceFile,
    SourceNode,
    collect_all_files,
)


# ---------------------------------------------------------------------------
# Episode & Title Patterns
# ---------------------------------------------------------------------------

_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")

_SEASON_EPISODE_RE = re.compile(
    r"""
    (?:^|[^A-Za-z0-9])
    S\s*0*(\d{1,3})\s*E\s*0*(\d{1,4})
    (?:$|[^0-9])
    """,
    re.IGNORECASE | re.VERBOSE,
)

_STANDALONE_EPISODE_RE = re.compile(
    r"""
    (?:^|[^A-Za-z0-9])
    (?:EP?|第|E)\s*0*(\d{1,4})\s*(?:[话話集期]|$|[^0-9])
    """,
    re.IGNORECASE | re.VERBOSE,
)

_BRACKETED_EPISODE_RE = re.compile(
    r"""
    \[\s*0*(\d{1,4})\s*(?:v\d+)?\s*\]
    """,
    re.IGNORECASE | re.VERBOSE,
)

_SPECIAL_KEYWORD_RE = re.compile(
    r"""
    (?:^|[\s._\-\[(])
    (?:SP|SPECIAL|OVA|OAV|OAD|EXTRA|EXTRAS|TOKUTEN|特典|特别篇|特辑)
    (?:[\s._\-)\]]|0*(\d{1,3})|$)
    """,
    re.IGNORECASE | re.VERBOSE,
)

_KNOWN_RESOLUTIONS = frozenset({480, 576, 720, 1080, 2160, 4320})
_KNOWN_CODECS = frozenset({"264", "265", "x264", "x265", "h264", "h265", "hevc", "av1", "10bit", "8bit"})


# ---------------------------------------------------------------------------
# EpisodePattern
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class EpisodePattern:
    """Structural summary of episodes and seasons detected in source files."""

    season_numbers: tuple[int, ...]
    episode_numbers: tuple[int, ...]
    total_episodes: int
    has_specials: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "season_numbers": list(self.season_numbers),
            "episode_numbers": list(self.episode_numbers),
            "total_episodes": self.total_episodes,
            "has_specials": self.has_specials,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> EpisodePattern:
        return cls(
            season_numbers=tuple(int(x) for x in raw.get("season_numbers") or ()),
            episode_numbers=tuple(int(x) for x in raw.get("episode_numbers") or ()),
            total_episodes=int(raw.get("total_episodes", 0)),
            has_specials=bool(raw.get("has_specials", False)),
        )


def extract_episode_pattern(
    files_or_node: Sequence[SourceFile | str] | SourceNode,
) -> EpisodePattern | None:
    """Extract regular seasons/episodes and specials from files or directory node."""
    if isinstance(files_or_node, SourceNode):
        all_files = collect_all_files(files_or_node)
        filenames = [f.name for f in all_files if f.object_type == "video"]
    else:
        filenames = [
            f.name if isinstance(f, SourceFile) else str(f)
            for f in files_or_node
        ]

    if not filenames:
        return None

    seasons: set[int] = set()
    episodes: set[int] = set()
    has_specials = False

    for name in filenames:
        # Check special indicators
        if _SPECIAL_KEYWORD_RE.search(name):
            has_specials = True

        # Check SxxExx
        se_match = _SEASON_EPISODE_RE.search(name)
        if se_match:
            s_num = int(se_match.group(1))
            e_num = int(se_match.group(2))
            if s_num == 0:
                has_specials = True
            else:
                seasons.add(s_num)
                episodes.add(e_num)
            continue

        # Check standalone episode pattern (e.g. EP01, 第01集, E01)
        ep_match = _STANDALONE_EPISODE_RE.search(name)
        if ep_match:
            val = int(ep_match.group(1))
            if val not in _KNOWN_RESOLUTIONS and str(val) not in _KNOWN_CODECS:
                episodes.add(val)
                seasons.add(1)
            continue

        # Check bracketed numbers: [01], [12]
        for b_match in _BRACKETED_EPISODE_RE.finditer(name):
            val = int(b_match.group(1))
            if val not in _KNOWN_RESOLUTIONS and str(val) not in _KNOWN_CODECS and not (1900 <= val <= 2099):
                episodes.add(val)
                seasons.add(1)

    if not episodes and not seasons and not has_specials:
        return None

    sorted_seasons = tuple(sorted(seasons))
    sorted_episodes = tuple(sorted(episodes))
    total_episodes = len(sorted_episodes)

    return EpisodePattern(
        season_numbers=sorted_seasons,
        episode_numbers=sorted_episodes,
        total_episodes=total_episodes,
        has_specials=has_specials,
    )


# ---------------------------------------------------------------------------
# IdentityEvidence
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class IdentityEvidence:
    """Aggregated naming and structural clues used for TMDB matching."""

    work_unit_id: str
    boundary_label: str
    parent_labels: tuple[str, ...]
    representative_names: tuple[str, ...]
    normalized_titles: tuple[str, ...]
    years: tuple[int, ...]
    episode_pattern: EpisodePattern | None
    media_shape: str   # "movie" | "tv" | "mixed" | "unknown"
    aliases: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "work_unit_id": self.work_unit_id,
            "boundary_label": self.boundary_label,
            "parent_labels": list(self.parent_labels),
            "representative_names": list(self.representative_names),
            "normalized_titles": list(self.normalized_titles),
            "years": list(self.years),
            "episode_pattern": self.episode_pattern.as_dict() if self.episode_pattern else None,
            "media_shape": self.media_shape,
            "aliases": list(self.aliases),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> IdentityEvidence:
        ep_raw = raw.get("episode_pattern")
        return cls(
            work_unit_id=str(raw["work_unit_id"]),
            boundary_label=str(raw["boundary_label"]),
            parent_labels=tuple(str(x) for x in raw.get("parent_labels") or ()),
            representative_names=tuple(str(x) for x in raw.get("representative_names") or ()),
            normalized_titles=tuple(str(x) for x in raw.get("normalized_titles") or ()),
            years=tuple(int(x) for x in raw.get("years") or ()),
            episode_pattern=EpisodePattern.from_dict(ep_raw) if isinstance(ep_raw, Mapping) else None,
            media_shape=str(raw.get("media_shape", "unknown")),
            aliases=tuple(str(x) for x in raw.get("aliases") or ()),
        )


def _clean_noise_tags(text: str) -> str:
    """Remove common release group bracket tags, resolutions, and codecs."""
    cleaned = re.sub(r"\[[^\]]*\]|\([^)]*(?:1080|2160|720|x26|hevc)[^)]*\)", " ", text)
    cleaned = re.sub(
        r"\b(?:4k|8k|2160p|1080p|720p|480p|bluray|blu-ray|web-?dl|webrip|x26[45]|hevc|av1|10bit|aac|flac|dts)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def extract_identity_evidence(
    candidate: WorkCandidate,
    node: SourceNode | None = None,
    *,
    parent_labels: Sequence[str] = (),
) -> IdentityEvidence:
    """Build IdentityEvidence from candidate boundary analysis and file node."""
    years: set[int] = set()

    # Extract years from candidate label and parent labels
    for y_str in _YEAR_RE.findall(candidate.display_label):
        years.add(int(y_str))
    for p_label in parent_labels:
        for y_str in _YEAR_RE.findall(p_label):
            years.add(int(y_str))

    representative_names: list[str] = [candidate.display_label]
    aliases: list[str] = []

    # Extract title subparts (e.g. "Fate/Zero", "Fate - Stay Night")
    for sep in [" - ", " / ", "／", "：", ":"]:
        if sep in candidate.display_label:
            parts = [p.strip() for p in candidate.display_label.split(sep) if p.strip()]
            if len(parts) > 1:
                aliases.extend(parts)

    episode_pattern: EpisodePattern | None = None
    if node is not None:
        video_files = [f for f in collect_all_files(node) if f.object_type == "video"]
        if video_files:
            episode_pattern = extract_episode_pattern(video_files)
            for f in video_files[:5]:
                for y_str in _YEAR_RE.findall(f.name):
                    years.add(int(y_str))
                cleaned_name = _clean_noise_tags(Path(f.name).stem)
                if cleaned_name and cleaned_name not in representative_names:
                    representative_names.append(cleaned_name)

    # Derive normalized titles
    normalized_titles: list[str] = []
    base_clean = _clean_noise_tags(candidate.display_label)
    if base_clean:
        normalized_titles.append(base_clean)

    # Clean without year
    without_year = _YEAR_RE.sub(" ", base_clean)
    without_year = re.sub(r"\s+", " ", without_year).strip(" -_()（）[]【】")
    if without_year and without_year not in normalized_titles:
        normalized_titles.append(without_year)

    return IdentityEvidence(
        work_unit_id=candidate.work_unit_id,
        boundary_label=candidate.display_label,
        parent_labels=tuple(parent_labels),
        representative_names=tuple(representative_names),
        normalized_titles=tuple(normalized_titles),
        years=tuple(sorted(years)),
        episode_pattern=episode_pattern,
        media_shape=candidate.proposed_media_context,
        aliases=tuple(sorted(set(aliases))),
    )


# ---------------------------------------------------------------------------
# WorkUnitRecord
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True, slots=True)
class WorkUnitRecord:
    """Persistent execution state for one distinct creative work."""

    work_unit_id: str
    root_task_id: str
    boundary_key: str
    source_paths: tuple[str, ...]
    source_revision: int
    role: str
    identity_status: str  # "pending" | "confirmed" | "uncertain" | "failed"
    identity: dict[str, Any] | None
    candidate_identities: tuple[dict[str, Any], ...] = ()
    reconciliation_outcome: str | None = None
    matched_work_root: str | None = None
    attention: str | None = None
    updated_at: str = field(default_factory=_now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "work_unit_id": self.work_unit_id,
            "root_task_id": self.root_task_id,
            "boundary_key": self.boundary_key,
            "source_paths": list(self.source_paths),
            "source_revision": self.source_revision,
            "role": self.role,
            "identity_status": self.identity_status,
            "identity": dict(self.identity) if self.identity else None,
            "candidate_identities": [dict(c) for c in self.candidate_identities],
            "reconciliation_outcome": self.reconciliation_outcome,
            "matched_work_root": self.matched_work_root,
            "attention": self.attention,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> WorkUnitRecord:
        ident = raw.get("identity")
        cand_list = raw.get("candidate_identities") or ()
        return cls(
            work_unit_id=str(raw["work_unit_id"]),
            root_task_id=str(raw["root_task_id"]),
            boundary_key=str(raw["boundary_key"]),
            source_paths=tuple(str(x) for x in raw.get("source_paths") or ()),
            source_revision=int(raw.get("source_revision", 1)),
            role=str(raw.get("role", "single_work")),
            identity_status=str(raw.get("identity_status", "pending")),
            identity=dict(ident) if isinstance(ident, Mapping) else None,
            candidate_identities=tuple(dict(c) for c in cand_list if isinstance(c, Mapping)),
            reconciliation_outcome=str(raw["reconciliation_outcome"]) if raw.get("reconciliation_outcome") else None,
            matched_work_root=str(raw["matched_work_root"]) if raw.get("matched_work_root") else None,
            attention=str(raw["attention"]) if raw.get("attention") else None,
            updated_at=str(raw.get("updated_at") or _now()),
        )


def create_work_units_from_candidates(
    candidates: Sequence[WorkCandidate],
    root_task_id: str,
    *,
    source_revision: int = 1,
) -> list[WorkUnitRecord]:
    """Convert boundary WorkCandidates into initial pending WorkUnitRecords."""
    now_str = _now()
    records: list[WorkUnitRecord] = []
    for cand in candidates:
        record = WorkUnitRecord(
            work_unit_id=cand.work_unit_id,
            root_task_id=root_task_id,
            boundary_key=cand.boundary_key,
            source_paths=cand.source_paths,
            source_revision=source_revision,
            role=cand.boundary_evidence.role.value,
            identity_status="pending",
            identity=None,
            candidate_identities=(),
            reconciliation_outcome=None,
            matched_work_root=None,
            attention=None,
            updated_at=now_str,
        )
        records.append(record)
    return records


# ---------------------------------------------------------------------------
# Atomic Persistence
# ---------------------------------------------------------------------------

def _records_path(state_dir: Path, root_task_id: str) -> Path:
    return state_dir / f"work_units_{root_task_id}.json"


def save_work_unit_records(
    state_dir: Path,
    root_task_id: str,
    records: Sequence[WorkUnitRecord],
) -> None:
    """Atomically persist work unit records for a root task."""
    state_dir.mkdir(parents=True, exist_ok=True)
    target = _records_path(state_dir, root_task_id)
    payload = json.dumps(
        [r.as_dict() for r in records],
        indent=2,
        ensure_ascii=False,
    )
    with tempfile.NamedTemporaryFile("w", dir=state_dir, delete=False, encoding="utf-8") as tmp:
        tmp.write(payload)
        tmp.flush()
        tmp_path = Path(tmp.name)
    tmp_path.replace(target)


def load_work_unit_records(
    state_dir: Path,
    root_task_id: str,
) -> list[WorkUnitRecord]:
    """Load persisted work unit records for a root task; return empty if absent."""
    path = _records_path(state_dir, root_task_id)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [WorkUnitRecord.from_dict(item) for item in data if isinstance(item, Mapping)]
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return []


__all__ = [
    "EpisodePattern",
    "extract_episode_pattern",
    "IdentityEvidence",
    "extract_identity_evidence",
    "WorkUnitRecord",
    "create_work_units_from_candidates",
    "save_work_unit_records",
    "load_work_unit_records",
]
