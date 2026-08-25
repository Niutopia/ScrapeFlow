"""J-node domain: per-work-unit Gap ledger with AcquisitionAttempt records.

A Gap binds one precise season/episode (or media/subtitle) coordinate to its
``work_unit_id`` (contract rule J).  AcquisitionAttempt rows record every
replenishment try against that gap.  The ledger is a single atomic JSON file
per root task: ``gap_ledger_<root_task_id>.json``.

This module performs no network I/O and no writes outside the local state
root.  Closing a gap is an explicit audit-proven transition — callers may only
call ``close_gap`` after the coordinate has been verified in the formal
library.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from .serialization import atomic_write_json

GAP_KINDS = ("missing_episode", "missing_season", "missing_media", "missing_subtitle")
ATTEMPT_STATUSES = (
    "submitted",
    "candidate_failed",
    "infrastructure",
    "in_doubt",
    "closed",
)
MAX_ATTEMPTS_PER_GAP = 200
MAX_GAPS_PER_LEDGER = 20_000
_MAX_STAGED_PATHS_PER_ATTEMPT = 64

_TOKEN_RE = re.compile(r"^S(\d+)E(\d+)$")


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def gap_token(season: int, episode: int) -> str:
    return f"S{season:02d}E{episode:02d}"


def parse_gap_token(value: object) -> tuple[int, int] | None:
    if not isinstance(value, str):
        return None
    match = _TOKEN_RE.fullmatch(value)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


@dataclass(frozen=True, slots=True)
class AcquisitionAttempt:
    """One bounded replenishment try against one gap."""

    attempt_id: str
    provider: str
    tier: str | None
    locator: str | None
    status: str
    external_task_id: str | None = None
    staged_paths: tuple[str, ...] = ()
    error: str | None = None
    recorded_at: str = field(default_factory=_now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "provider": self.provider,
            "tier": self.tier,
            "locator": self.locator,
            "status": self.status,
            "external_task_id": self.external_task_id,
            "staged_paths": list(self.staged_paths),
            "error": self.error,
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "AcquisitionAttempt":
        status = str(raw.get("status", ""))
        if status not in ATTEMPT_STATUSES:
            raise ValueError(f"invalid attempt status: {status!r}")
        staged = raw.get("staged_paths") or []
        paths = tuple(
            str(value) for value in staged
            if isinstance(value, str) and value
        )[: _MAX_STAGED_PATHS_PER_ATTEMPT]
        return cls(
            attempt_id=str(raw["attempt_id"]),
            provider=str(raw["provider"]),
            tier=str(raw["tier"]) if raw.get("tier") else None,
            locator=str(raw["locator"]) if raw.get("locator") else None,
            status=status,
            external_task_id=(
                str(raw["external_task_id"]) if raw.get("external_task_id") else None
            ),
            staged_paths=paths,
            error=str(raw["error"]) if raw.get("error") else None,
            recorded_at=str(raw.get("recorded_at") or _now()),
        )


@dataclass(frozen=True, slots=True)
class Gap:
    """One precise work-unit coordinate that is missing from the library."""

    gap_id: str
    root_task_id: str
    work_unit_id: str
    kind: str
    media_type: str
    tmdb_id: int
    season: int | None
    episodes: tuple[int, ...]
    subtitle_path: str | None
    subtitle_language: str | None
    status: str
    attempts: tuple[AcquisitionAttempt, ...] = ()
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)

    def __post_init__(self) -> None:
        if self.kind not in GAP_KINDS:
            raise ValueError(f"invalid gap kind: {self.kind!r}")
        if self.status not in {"open", "closed"}:
            raise ValueError(f"invalid gap status: {self.status!r}")
        if not self.gap_id or not self.root_task_id or not self.work_unit_id:
            raise ValueError("gap id and task ids are required")
        if isinstance(self.tmdb_id, bool) or not isinstance(self.tmdb_id, int) or self.tmdb_id <= 0:
            raise ValueError("tmdb_id must be a positive integer")

    def as_dict(self) -> dict[str, Any]:
        return {
            "gap_id": self.gap_id,
            "root_task_id": self.root_task_id,
            "work_unit_id": self.work_unit_id,
            "kind": self.kind,
            "media_type": self.media_type,
            "tmdb_id": self.tmdb_id,
            "season": self.season,
            "episodes": list(self.episodes),
            "subtitle_path": self.subtitle_path,
            "subtitle_language": self.subtitle_language,
            "status": self.status,
            "attempts": [attempt.as_dict() for attempt in self.attempts],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Gap":
        return cls(
            gap_id=str(raw["gap_id"]),
            root_task_id=str(raw["root_task_id"]),
            work_unit_id=str(raw["work_unit_id"]),
            kind=str(raw["kind"]),
            media_type=str(raw["media_type"]),
            tmdb_id=int(raw["tmdb_id"]),
            season=int(raw["season"]) if raw.get("season") is not None else None,
            episodes=tuple(int(value) for value in (raw.get("episodes") or ())),
            subtitle_path=str(raw["subtitle_path"]) if raw.get("subtitle_path") else None,
            subtitle_language=(
                str(raw["subtitle_language"]) if raw.get("subtitle_language") else None
            ),
            status=str(raw.get("status", "open")),
            attempts=tuple(
                AcquisitionAttempt.from_dict(attempt)
                for attempt in (raw.get("attempts") or [])
                if isinstance(attempt, Mapping)
            )[: MAX_ATTEMPTS_PER_GAP],
            created_at=str(raw.get("created_at") or _now()),
            updated_at=str(raw.get("updated_at") or _now()),
        )


def _ledger_path(state_root: Path, root_task_id: str) -> Path:
    return state_root / f"gap_ledger_{root_task_id}.json"


def load_gap_ledger(state_root: Path, root_task_id: str) -> list[Gap]:
    try:
        raw = json.loads(
            _ledger_path(state_root, root_task_id).read_text(encoding="utf-8")
        )
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    output: list[Gap] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        try:
            gap = Gap.from_dict(item)
        except (KeyError, TypeError, ValueError):
            continue
        output.append(gap)
        if len(output) > MAX_GAPS_PER_LEDGER:
            break
    return output


def _load_gap_ledger_strict(state_root: Path, root_task_id: str) -> list[Gap]:
    """Read one ledger without treating corruption as an empty ledger.

    The public/tolerant loader is useful for dashboard aggregation over old
    local state.  A J-step registration is different: it is about to make a
    durable claim that missing coordinates were recorded, so an unreadable or
    malformed existing ledger must stop that claim rather than be silently
    replaced by an empty list.
    """
    path = _ledger_path(state_root, root_task_id)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("缺口账本无法读取") from exc
    if not isinstance(raw, list):
        raise ValueError("缺口账本格式无效")
    output: list[Gap] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("缺口账本包含无效条目")
        try:
            output.append(Gap.from_dict(item))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("缺口账本包含无法验证的条目") from exc
        if len(output) > MAX_GAPS_PER_LEDGER:
            raise ValueError("缺口账本超过安全上限")
    return output


def save_gap_ledger(
    state_root: Path,
    root_task_id: str,
    gaps: Sequence[Gap],
) -> None:
    atomic_write_json(
        _ledger_path(state_root, root_task_id),
        [gap.as_dict() for gap in gaps],
        allow_nan=False,
    )


def _episode_gap_id(work_unit_id: str, token: str) -> str:
    return f"{work_unit_id}::missing_episode::{token}"


def discover_episode_gaps(
    state_root: Path,
    root_task_id: str,
    work_unit_id: str,
    *,
    media_type: str,
    tmdb_id: int,
    expected_by_season: Mapping[int, Sequence[int]],
    actual_tokens: Sequence[str],
) -> list[Gap]:
    """Register precise episode gaps and persist the updated ledger.

    ``expected_by_season`` is the official published catalog for the work;
    ``actual_tokens`` are the ``SxxEyy`` coordinates the library already
    holds.  Only missing coordinates become open gaps; already-closed gaps are
    preserved untouched and every open gap is idempotent across retries.
    """
    actual: set[tuple[int, int]] = set()
    for token in actual_tokens:
        coordinate = parse_gap_token(token)
        if coordinate is not None:
            actual.add(coordinate)
    ledger = _load_gap_ledger_strict(state_root, root_task_id)
    existing = {
        gap.gap_id: gap
        for gap in ledger
        if gap.work_unit_id == work_unit_id
    }
    # A coordinate is a root-level fact: two units of the same series (e.g.
    # the container unit and one season unit) must never each register the
    # same (media_type, tmdb_id, season, episode) as separate open rows.
    open_coordinates = {
        (gap.media_type, gap.tmdb_id, gap.season, int(episode))
        for gap in ledger
        if gap.kind == "missing_episode"
        and gap.status == "open"
        and isinstance(gap.episodes, (list, tuple))
        for episode in gap.episodes
        if isinstance(episode, int) and not isinstance(episode, bool)
    }
    missing: list[tuple[int, int]] = []
    for season, episodes in expected_by_season.items():
        if isinstance(season, bool) or not isinstance(season, int) or season < 0:
            continue
        for episode in episodes:
            if (
                isinstance(episode, bool)
                or not isinstance(episode, int)
                or episode <= 0
            ):
                continue
            if (season, episode) in actual:
                continue
            if (media_type, tmdb_id, season, episode) in open_coordinates:
                continue
            missing.append((season, episode))
    for coordinate in missing:
        token = gap_token(*coordinate)
        gap_id = _episode_gap_id(work_unit_id, token)
        if gap_id in existing:
            continue
        ledger.append(Gap(
            gap_id=gap_id,
            root_task_id=root_task_id,
            work_unit_id=work_unit_id,
            kind="missing_episode",
            media_type=media_type,
            tmdb_id=tmdb_id,
            season=coordinate[0],
            episodes=(coordinate[1],),
            subtitle_path=None,
            subtitle_language=None,
            status="open",
        ))
    save_gap_ledger(state_root, root_task_id, ledger)
    # A local write is not accepted until an exact strict readback proves all
    # pre-existing and newly-added IDs survived.  This keeps J fail-closed on
    # a damaged filesystem/ledger instead of returning an indistinguishable
    # empty gap list.
    readback = _load_gap_ledger_strict(state_root, root_task_id)
    expected_by_id = {gap.gap_id: gap.as_dict() for gap in ledger}
    actual_by_id = {gap.gap_id: gap.as_dict() for gap in readback}
    if actual_by_id != expected_by_id:
        raise ValueError("缺口账本写后回读不一致")
    return [gap for gap in readback if gap.work_unit_id == work_unit_id]


def register_subtitle_gap(
    state_root: Path,
    root_task_id: str,
    work_unit_id: str,
    *,
    media_type: str,
    tmdb_id: int,
    subtitle_path: str,
    subtitle_language: str,
) -> Gap:
    """Register one open subtitle gap (dedicated subtitle lane schema)."""
    if not subtitle_path or not subtitle_language:
        raise ValueError("subtitle gap requires a video path and language")
    gap_id = f"{work_unit_id}::missing_subtitle::{subtitle_language}"
    ledger = load_gap_ledger(state_root, root_task_id)
    for gap in ledger:
        if gap.gap_id == gap_id:
            return gap
    gap = Gap(
        gap_id=gap_id,
        root_task_id=root_task_id,
        work_unit_id=work_unit_id,
        kind="missing_subtitle",
        media_type=media_type,
        tmdb_id=tmdb_id,
        season=None,
        episodes=(),
        subtitle_path=subtitle_path,
        subtitle_language=subtitle_language,
        status="open",
    )
    ledger.append(gap)
    save_gap_ledger(state_root, root_task_id, ledger)
    return gap


def record_attempt(
    state_root: Path,
    root_task_id: str,
    gap_id: str,
    *,
    attempt_id: str,
    provider: str,
    tier: str | None,
    locator: str | None,
    status: str,
    external_task_id: str | None = None,
    staged_paths: Sequence[str] = (),
    error: str | None = None,
) -> Gap:
    """Append one AcquisitionAttempt to a gap and persist the ledger.

    An attempt never closes a gap: closure is an audit-proven transition via
    ``close_gap``.
    """
    if status not in ATTEMPT_STATUSES:
        raise ValueError(f"invalid attempt status: {status!r}")
    ledger = load_gap_ledger(state_root, root_task_id)
    for index, gap in enumerate(ledger):
        if gap.gap_id != gap_id:
            continue
        attempts = list(gap.attempts)[: MAX_ATTEMPTS_PER_GAP - 1]
        attempts.append(AcquisitionAttempt(
            attempt_id=attempt_id,
            provider=provider,
            tier=tier,
            locator=locator,
            status=status,
            external_task_id=external_task_id,
            staged_paths=tuple(str(path) for path in staged_paths if isinstance(path, str)),
            error=error,
        ))
        ledger[index] = replace(gap, attempts=tuple(attempts), updated_at=_now())
        save_gap_ledger(state_root, root_task_id, ledger)
        return ledger[index]
    raise KeyError(f"gap 不存在: {gap_id}")


def close_gap(
    state_root: Path,
    root_task_id: str,
    gap_id: str,
    *,
    note: str = "",
) -> Gap:
    """Mark one gap closed after audit proof; idempotent."""
    ledger = load_gap_ledger(state_root, root_task_id)
    for index, gap in enumerate(ledger):
        if gap.gap_id != gap_id:
            continue
        if gap.status == "closed":
            return gap
        ledger[index] = replace(
            gap, status="closed", updated_at=_now(),
        )
        save_gap_ledger(state_root, root_task_id, ledger)
        return ledger[index]
    raise KeyError(f"gap 不存在: {gap_id}")


__all__ = [
    "AcquisitionAttempt",
    "ATTEMPT_STATUSES",
    "Gap",
    "GAP_KINDS",
    "close_gap",
    "discover_episode_gaps",
    "gap_token",
    "load_gap_ledger",
    "parse_gap_token",
    "record_attempt",
    "register_subtitle_gap",
    "save_gap_ledger",
]
