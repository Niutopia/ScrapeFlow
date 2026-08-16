"""P14: translate the new-path Gap ledger into runtime replenishment requests.

The new architecture (``IntakeSource -> RootJob -> WorkUnit``) registers gaps in
``gap_ledger_<root_task_id>.json`` (see ``engine.scrapeflow.gap_ledger``), while
the automatic replenishment runtime and its adapters already accept a legacy
request shape built by ``local.scrapeflow_api.replenishment`` from a scan
report.  This module is the bridge between the two: it turns the durable Gap
ledger into the runtime request shape and wires the existing search + selection
boundaries without touching either legacy path.

Request field contract (one request per ``(media_type, tmdb_id)`` identity):

    {
        "tier": "magnet",                      # local Torrent lane
        "media": {
            "tmdb_id": <int>,                  # positive TMDB id
            "media_type": "movie" | "tv",      # gap ledger media_type
            "title": <str>,                    # work unit identity.title
            "original_title": <str>,           # identity original_title ("" if absent)
            "aliases": [<str>, ...],           # title + original_title + identity.aliases
        },
        "gaps": [
            {
                "id": <str>,                   # gap_id for media/subtitle;
                                               # S{season:02d}E{ep:02d}(-E{ep:02d}) for episodes;
                                               # S{season:02d} for a whole season
                "kind": "missing_episode" | "missing_season"
                        | "missing_media" | "missing_subtitle",
                "season": <int> | None,
                "episodes": [<int>, ...],      # episode ordinals; [] for non-episode kinds
                "title": <str>,                # media title for media/subtitle; "" otherwise
                # subtitle lane only:
                "path": <str>,                 # target video path (subtitle_path)
                "subtitle_language": <str>,
            },
            ...
        ],
        "excluded_candidates": [],             # always empty here; the runtime owns
                                               # durable exclusion memory
        # "rules" is deliberately NOT set.  The runtime selection boundary does
        # not need the legacy optional-discovery/quality-ladder rule block.
    }

``rules`` is omitted so that the request is a data projection of the Gap ledger
rather than a re-import of legacy plan policy.  The selection boundary
(``select_replenishment_candidates``) is written to accept this minimal shape:
``media.tmdb_id`` for identity, ``media.aliases`` (falling back to
``media.title``) for the title gate, and ``gaps[].id``/``kind``/``season`` for
coverage.  ``media.aliases`` must never be an empty list, because the selector
treats an explicit empty list as "no alias evidence" and rejects every
candidate; this module therefore always seeds it with ``title``.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from engine.scrapeflow.gap_ledger import Gap, load_gap_ledger
from engine.scrapeflow.work_units import load_work_unit_records
from engine.tools.replenishment_adapter.search import search as _default_search

from .replenishment import select_replenishment_candidates
from .replenishment_tiers import TIER_LOCAL_MAGNET


def _nonempty_strings(values: Sequence[Any], *, limit: int = 40) -> list[str]:
    """Deduplicate trimmed non-empty strings, preserving first-seen order."""
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        output.append(text)
        if len(output) >= limit:
            break
    return output


def _episode_gap_id(season: int, episodes: Sequence[int]) -> str | None:
    """Render one contiguous episode coordinate as the canonical SxxEyy form."""
    ordered = sorted({int(item) for item in episodes if isinstance(item, int) and item > 0})
    if not ordered:
        return None
    if len(ordered) == 1:
        return f"S{season:02d}E{ordered[0]:02d}"
    return f"S{season:02d}E{ordered[0]:02d}-E{ordered[-1]:02d}"


def _work_unit_identities(
    state_root: Path, root_task_id: str,
) -> tuple[dict[str, Mapping[str, Any]], dict[tuple[str, int], Mapping[str, Any]]]:
    """Index confirmed work-unit identities by unit id and by TMDB identity."""
    identity_by_unit: dict[str, Mapping[str, Any]] = {}
    identity_by_key: dict[tuple[str, int], Mapping[str, Any]] = {}
    for record in load_work_unit_records(state_root, root_task_id):
        identity = record.identity
        if not isinstance(identity, Mapping):
            continue
        identity_by_unit[record.work_unit_id] = identity
        media_type = str(identity.get("media_type") or "").strip().casefold()
        tmdb_id = identity.get("tmdb_id")
        if (
            media_type in {"movie", "tv"}
            and isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and tmdb_id > 0
        ):
            identity_by_key.setdefault((media_type, tmdb_id), identity)
    return identity_by_unit, identity_by_key


def _gap_row(gap: Gap, *, title: str) -> dict[str, Any] | None:
    """Project one ledger Gap into the runtime gap row; ``None`` if unusable."""
    kind = gap.kind
    if kind == "missing_episode":
        if not isinstance(gap.season, int) or isinstance(gap.season, bool) or gap.season < 0:
            return None
        episodes = tuple(
            sorted({
                int(item) for item in gap.episodes
                if isinstance(item, int) and not isinstance(item, bool) and item > 0
            })
        )
        gap_id = _episode_gap_id(gap.season, episodes)
        if gap_id is None:
            return None
        return {
            "id": gap_id,
            "kind": kind,
            "season": gap.season,
            "episodes": list(episodes),
            "title": "",
        }
    if kind == "missing_season":
        if not isinstance(gap.season, int) or isinstance(gap.season, bool) or gap.season < 0:
            return None
        return {
            "id": f"S{gap.season:02d}",
            "kind": kind,
            "season": gap.season,
            "episodes": [],
            "title": "",
        }
    if kind == "missing_media":
        return {
            "id": gap.gap_id,
            "kind": kind,
            "season": None,
            "episodes": [],
            "title": title,
        }
    if kind == "missing_subtitle":
        row: dict[str, Any] = {
            "id": gap.gap_id,
            "kind": kind,
            "season": None,
            "episodes": [],
            "title": title,
        }
        if gap.subtitle_path:
            row["path"] = gap.subtitle_path
        if gap.subtitle_language:
            row["subtitle_language"] = gap.subtitle_language
        return row
    return None


def gap_ledger_requests(
    state_root: str | Path,
    root_task_id: str,
) -> list[dict[str, Any]]:
    """Build one runtime request per open-gap ``(media_type, tmdb_id)`` identity.

    Open gaps are read from ``gap_ledger_<root_task_id>.json``; the work title,
    original title and aliases are read from each confirmed work unit's
    ``identity`` in ``work_units_<root_task_id>.json``.  Closed gaps and gaps
    whose kind cannot be projected are skipped.  The result is deterministic:
    identities sort by ``(media_type, tmdb_id)`` and gaps by
    ``(work_unit_id, kind, gap_id)``.
    """
    state_root = Path(state_root)
    identities_by_unit, identities_by_key = _work_unit_identities(
        state_root, root_task_id,
    )

    grouped: dict[tuple[str, int], list[Gap]] = defaultdict(list)
    for gap in load_gap_ledger(state_root, root_task_id):
        if gap.status != "open":
            continue
        key = (str(gap.media_type).strip().casefold(), gap.tmdb_id)
        grouped[key].append(gap)

    requests: list[dict[str, Any]] = []
    for (media_type, tmdb_id) in sorted(grouped):
        identity = identities_by_key.get((media_type, tmdb_id))
        if identity is None:
            # The identity projection may lack a canonical (media_type, tmdb_id)
            # row (for example an operator override).  Fall back to the gap's
            # own work unit identity before giving up on a title.
            for gap in grouped[(media_type, tmdb_id)]:
                candidate = identities_by_unit.get(gap.work_unit_id)
                if isinstance(candidate, Mapping):
                    identity = candidate
                    break
        identity = identity if isinstance(identity, Mapping) else {}

        title = str(identity.get("title") or "").strip()
        original_title = str(identity.get("original_title") or "").strip()
        raw_aliases = identity.get("aliases")
        aliases = _nonempty_strings([
            title,
            original_title,
            *(raw_aliases if isinstance(raw_aliases, list) else []),
        ])

        gap_rows: list[dict[str, Any]] = []
        for gap in sorted(
            grouped[(media_type, tmdb_id)],
            key=lambda item: (item.work_unit_id, item.kind, item.gap_id),
        ):
            row = _gap_row(gap, title=title)
            if row is None:
                continue
            gap_rows.append(row)
        if not gap_rows:
            continue

        requests.append({
            "tier": TIER_LOCAL_MAGNET,
            "media": {
                "tmdb_id": tmdb_id,
                "media_type": media_type,
                "title": title,
                "original_title": original_title,
                "aliases": aliases,
            },
            "gaps": gap_rows,
            "excluded_candidates": [],
        })
    return requests


def gap_ledger_selection(
    state_root: str | Path,
    root_task_id: str,
    request: Mapping[str, Any],
    *,
    search_runner: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run search + selection for one bridged request and return the bundle.

    ``search_runner`` is injectable for tests (the ``search`` module also
    exposes ``ReplenishmentSearchService(runner=...)`` for the same purpose).
    When omitted, the real ``engine.tools.replenishment_adapter.search.search``
    boundary runs.

    ``state_root``/``root_task_id`` are accepted for symmetry with
    ``gap_ledger_requests`` and are reserved for the integration phase, where
    the caller will record ``AcquisitionAttempt`` rows and close gaps after the
    staging re-audit proves a coordinate present:

        # integration-phase hook (NOT wired here):
        #   engine.scrapeflow.gap_ledger.record_attempt(
        #       state_root, root_task_id, gap_id, ...)
        #   engine.scrapeflow.gap_ledger.close_gap(
        #       state_root, root_task_id, gap_id)

    The returned bundle maps ``selected_gap_ids`` back to ledger ``gap_id`` via
    the ``SxxEyy`` token (episodes) or the whole ``gap_id`` (media/subtitle);
    the integrator resolves those back through the gap ledger to close gaps.
    """
    del state_root, root_task_id  # reserved for the integration-phase hooks above

    runner = search_runner if search_runner is not None else _default_search
    result = runner(request)
    if not isinstance(result, Mapping):
        raise TypeError("补源搜索结果必须是对象")
    raw_candidates = result.get("candidates")
    candidates = raw_candidates if isinstance(raw_candidates, list) else []
    return select_replenishment_candidates(request, candidates)


__all__ = [
    "gap_ledger_requests",
    "gap_ledger_selection",
]
