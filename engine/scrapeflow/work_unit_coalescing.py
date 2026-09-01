"""Narrow C-stage coalescing for proven sibling TV-season WorkUnits.

Boundary analysis normally emits one WorkUnit for a whole multi-season work.
Some conservative release trees must instead begin as siblings so that C/U
can confirm their identities independently.  Once C proves that sibling
season directories are the *same* TV identity, this module may combine them
before D/F.  It is deliberately narrow and pure: no TMDB, AList, planner,
writer, or local-state I/O occurs here.

Every prospective merge is fail-closed.  In particular, it requires exact
disjoint source ownership, one distinct explicit season marker per sibling,
and no file-level ``SxxExx`` marker contradicting its directory season.  A
post-D/E/F/J record, or any unit with an acceptance receipt, is never
rewritten.
"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Mapping, Sequence

from .boundary_analysis import _SEASON_EPISODE_RE, _season_number_from_directory_name
from .media_naming import edition_tag
from .source_inventory import (
    SourceNode,
    build_source_inventory,
    collect_all_files,
    iter_source_nodes,
)
from .work_units import WorkUnitRecord


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True, slots=True)
class _SeasonSibling:
    """One exact, validated season-scope record eligible for grouping."""

    record: WorkUnitRecord
    tmdb_id: int
    parent_path: str
    source_path: str
    season: int
    # An explicit edition tag on the folder label (``… 第一季 新编集版``) marks an
    # alternate cut of that season, not a competing season sibling.  The smart
    # planner already understands ``{edition-New Edit}``; it just needs the cut
    # to arrive inside the same WorkUnit as the season it re-cuts.
    edition: str | None = None


def _confirmed_tv_tmdb_id(record: WorkUnitRecord) -> int | None:
    """Return a C-confirmed TV identity key, otherwise ``None``."""
    identity = record.identity
    if (
        record.identity_status != "confirmed"
        or record.requires_content_expansion
        or not isinstance(identity, Mapping)
        or str(identity.get("media_type") or "") != "tv"
    ):
        return None
    raw_tmdb_id = identity.get("tmdb_id")
    if isinstance(raw_tmdb_id, bool) or not isinstance(raw_tmdb_id, int):
        return None
    return raw_tmdb_id if raw_tmdb_id > 0 else None


def _is_untouched(record: WorkUnitRecord) -> bool:
    """Whether a C-confirmed unit has no D/E/F/G/H/J state to replace."""
    return (
        record.writer_job_id is None
        and record.reconciliation_outcome is None
        and record.matched_work_root is None
        and record.reconciliation_evidence is None
        and record.lane_status is None
        and record.lane_detail is None
        and not record.uncovered_tokens
        and record.gap_status is None
        and record.gap_detail is None
        and record.attention is None
    )


def _valid_identity_season(record: WorkUnitRecord, season: int) -> bool:
    """Allow an optional C/U season only when it corroborates the scope."""
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    selected = identity.get("season")
    if selected is None:
        return True
    return (
        isinstance(selected, int)
        and not isinstance(selected, bool)
        and selected == season
    )


def _valid_claims(record: WorkUnitRecord, season: int) -> bool:
    """A sibling may claim only its own explicit directory season."""
    claimed = tuple(record.claimed_seasons)
    return not claimed or claimed == (season,)


def _has_conflicting_file_season(node: SourceNode, season: int) -> bool:
    """Reject a scope whose video filenames contradict its season directory."""
    for source_file in collect_all_files(node):
        if source_file.object_type != "video":
            continue
        for match in _SEASON_EPISODE_RE.finditer(source_file.name):
            if int(match.group(1)) != season:
                return True
    return False


def _season_sibling(
    record: WorkUnitRecord,
    *,
    nodes_by_path: Mapping[str, SourceNode],
) -> _SeasonSibling | None:
    """Validate one record as a single, explicitly-marked season sibling."""
    tmdb_id = _confirmed_tv_tmdb_id(record)
    if tmdb_id is None or not _is_untouched(record) or len(record.source_paths) != 1:
        return None
    source_path = str(record.source_paths[0]).rstrip("/")
    if not source_path:
        return None
    node = nodes_by_path.get(source_path)
    if node is None:
        return None
    season = _season_number_from_directory_name(posixpath.basename(source_path))
    if season is None:
        return None
    if not _valid_identity_season(record, season) or not _valid_claims(record, season):
        return None
    if _has_conflicting_file_season(node, season):
        return None
    return _SeasonSibling(
        record=record,
        tmdb_id=tmdb_id,
        parent_path=posixpath.dirname(source_path) or "/",
        source_path=source_path,
        season=season,
        edition=edition_tag(posixpath.basename(source_path)),
    )


def _scopes_overlap(left: str, right: str) -> bool:
    """Whether two normalized remote directory scopes overlap."""
    return (
        left == right
        or left.startswith(right + "/")
        or right.startswith(left + "/")
    )


def _group_has_exclusive_scopes(
    group: Sequence[_SeasonSibling],
    all_records: Sequence[WorkUnitRecord],
) -> bool:
    """Require the group to own all of its exact season scopes exclusively."""
    group_ids = {item.record.work_unit_id for item in group}
    group_scopes = [item.source_path for item in group]
    if len(group_scopes) != len(set(group_scopes)):
        return False
    for index, left in enumerate(group_scopes):
        if any(_scopes_overlap(left, right) for right in group_scopes[index + 1:]):
            return False
    for record in all_records:
        if record.work_unit_id in group_ids:
            continue
        for external_scope_raw in record.source_paths:
            external_scope = str(external_scope_raw).rstrip("/")
            if not external_scope:
                return False
            if any(_scopes_overlap(scope, external_scope) for scope in group_scopes):
                return False
    return True


def _other_same_identity_sibling_blocks_group(
    group: Sequence[_SeasonSibling],
    all_records: Sequence[WorkUnitRecord],
) -> bool:
    """Do not merge a subset when a same-ID sibling is not equally provable."""
    group_ids = {item.record.work_unit_id for item in group}
    tmdb_id = group[0].tmdb_id
    parent_path = group[0].parent_path
    for record in all_records:
        if record.work_unit_id in group_ids or _confirmed_tv_tmdb_id(record) != tmdb_id:
            continue
        # A multi-scope record can contain a sibling season path.  It must
        # block the new coalescing pass rather than leave one same-identity
        # season behind with a competing unit boundary.
        if any(
            (posixpath.dirname(str(scope).rstrip("/")) or "/") == parent_path
            for scope in record.source_paths
        ):
            return True
    return False


def _merge_group(group: Sequence[_SeasonSibling]) -> WorkUnitRecord:
    """Create one canonical multi-season record from an all-safe group."""
    ordered = tuple(sorted(
        group,
        key=lambda item: (
            item.season,
            item.source_path,
            item.record.boundary_key,
            item.record.work_unit_id,
        ),
    ))
    canonical = ordered[0].record
    # A per-sibling manual season is correct before coalescing, but a merged
    # multi-season unit must not carry one arbitrary season as a global F
    # request default.  The exact source paths and their claims now supply
    # the bounded per-season evidence.
    identity = dict(canonical.identity or {})
    identity.pop("season", None)
    return replace(
        canonical,
        source_paths=tuple(item.source_path for item in ordered),
        role="single_work",
        # An alternate cut shares its base season, so the merged claim set is
        # the ordinary seasons only — a repeated number would read as two
        # different seasons to every downstream consumer.
        claimed_seasons=tuple(
            sorted({item.season for item in ordered if item.edition is None})
        ),
        media_context="tv",
        identity=identity,
        reconciliation_outcome=None,
        matched_work_root=None,
        reconciliation_evidence=None,
        writer_job_id=None,
        lane_status=None,
        lane_detail=None,
        uncovered_tokens=(),
        gap_status=None,
        gap_detail=None,
        attention=None,
        updated_at=_now(),
    )


def coalesce_confirmed_tv_season_work_units(
    records: Sequence[WorkUnitRecord],
    snapshot: Mapping[str, object] | None,
    *,
    acceptance_work_unit_ids: Sequence[str] = (),
) -> list[WorkUnitRecord]:
    """Coalesce only fully proved, untouched same-TV sibling season units.

    ``snapshot`` is the persisted B/W inventory, not a fresh provider read;
    C has just resolved the identities from that exact inventory.  A caller
    supplies every WorkUnit with any H acceptance row.  The function leaves
    all unsafe, ambiguous, or post-effect records unchanged and returns a
    new list only when at least one valid group was merged.
    """
    original = list(records)
    if len(original) < 2 or not isinstance(snapshot, Mapping):
        return original
    root = snapshot.get("root")
    rows = snapshot.get("rows")
    if not isinstance(root, str) or not root or not isinstance(rows, list):
        return original
    try:
        inventory = build_source_inventory(rows, root)
    except (TypeError, ValueError):
        return original
    nodes_by_path = {
        node.path.rstrip("/"): node
        for node in iter_source_nodes(inventory)
    }
    accepted = {str(work_unit_id) for work_unit_id in acceptance_work_unit_ids}

    candidates: list[_SeasonSibling] = []
    for record in original:
        if record.work_unit_id in accepted:
            continue
        sibling = _season_sibling(record, nodes_by_path=nodes_by_path)
        if sibling is not None:
            candidates.append(sibling)

    groups: dict[tuple[int, str], list[_SeasonSibling]] = {}
    for sibling in candidates:
        groups.setdefault((sibling.tmdb_id, sibling.parent_path), []).append(sibling)

    replacements: dict[str, WorkUnitRecord] = {}
    discarded: set[str] = set()
    for group in groups.values():
        if len(group) < 2:
            continue
        base = [item for item in group if item.edition is None]
        editions = [item for item in group if item.edition is not None]
        if len(base) < 2:
            # Without at least two ordinary seasons there is no broadcast run to
            # coalesce; a lone season plus its alternate cut keeps the historical
            # per-unit boundaries.
            continue
        if len({item.season for item in base}) != len(base):
            continue
        base_seasons = {item.season for item in base}
        if any(item.season not in base_seasons for item in editions):
            # An alternate cut of a season this group does not own is not proof
            # of anything; fail closed rather than widen the scope.
            continue
        if len({(item.season, item.edition) for item in editions}) != len(editions):
            continue
        if len({item.record.source_revision for item in group}) != 1:
            continue
        if any(item.record.work_unit_id in accepted for item in group):
            continue
        if not _group_has_exclusive_scopes(group, original):
            continue
        if _other_same_identity_sibling_blocks_group(group, original):
            continue
        merged = _merge_group(group)
        replacements[merged.work_unit_id] = merged
        discarded.update(
            item.record.work_unit_id
            for item in group
            if item.record.work_unit_id != merged.work_unit_id
        )

    if not replacements:
        return original
    output: list[WorkUnitRecord] = []
    for record in original:
        if record.work_unit_id in discarded:
            continue
        output.append(replacements.get(record.work_unit_id, record))
    return output


__all__ = ["coalesce_confirmed_tv_season_work_units"]
