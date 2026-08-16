"""C/U composition: per-work-unit TMDB identity resolution and overrides.

Each WorkUnit of a root task is resolved independently against TMDB using the
structured ``IdentityEvidence`` built from the B snapshot.  An ambiguous or
unprovable unit is parked as ``uncertain`` **without blocking its siblings**
(contract rule 4).  A user confirmation is persisted as a durable override on
the same ledger, so a retry never asks the same question twice.

This module performs no writes outside the local state root and no formal
library operations.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .boundary_analysis import (
    BoundaryEvidence,
    DirectoryRole,
    WorkCandidate,
)
from .errors import PlanError
from .identity_matching import (
    AutoMatchAmbiguityError,
    auto_match_from_evidence,
    bounded_auto_match_candidate_rows,
)
from .root_boundaries import load_source_snapshot
from .source_inventory import SourceNode, build_source_inventory
from .work_units import (
    IdentityEvidence,
    WorkUnitRecord,
    extract_identity_evidence,
    load_work_unit_records,
    save_work_unit_records,
)

DEFAULT_MIN_CONFIDENCE = 0.70


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iter_nodes(node: SourceNode) -> list[SourceNode]:
    output = [node]
    for child in node.children:
        output.extend(_iter_nodes(child))
    return output


def _parent_labels(root: SourceNode, unit: SourceNode) -> tuple[str, ...]:
    """Return ancestor container labels from the snapshot root to the unit."""
    root_path = root.path.rstrip("/")
    unit_path = unit.path.rstrip("/")
    if unit_path == root_path:
        return ()
    try:
        relative = unit_path[len(root_path):].lstrip("/")
    except ValueError:
        return ()
    parts = relative.split("/")[:-1]
    return tuple(parts)


def _identity_projection(best: Any) -> dict[str, Any]:
    return {
        "media_type": best.media_type,
        "tmdb_id": best.tmdb_id,
        "title": best.title,
        "year": best.year,
        "confidence": best.confidence,
        "score_components": dict(best.score_components or {}),
        "decision_trace": dict(best.decision_trace or {}),
    }


def resolve_work_unit_identities(
    tmdb_client: object,
    state_root: Path,
    root_task_id: str,
    *,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    prefer_animation: bool = False,
) -> list[WorkUnitRecord]:
    """Resolve every pending WorkUnit independently and persist the ledger.

    Already ``confirmed`` records — including durable operator overrides — are
    left untouched, so retries are idempotent and never re-ask a question the
    user already answered.  Infrastructure failures (``ApiError``) propagate
    so the caller can apply its normal retry policy; evidence-level failures
    park the single unit as ``uncertain``.
    """
    records = load_work_unit_records(state_root, root_task_id)
    snapshot = load_source_snapshot(state_root, root_task_id)
    if not records or snapshot is None:
        return records
    node = build_source_inventory(snapshot["rows"], snapshot["root"])
    nodes_by_path = {candidate.path: candidate for candidate in _iter_nodes(node)}
    updated: list[WorkUnitRecord] = []
    for record in records:
        if record.identity_status != "pending":
            updated.append(record)
            continue
        subnode = nodes_by_path.get(record.boundary_key)
        if subnode is None and record.source_paths:
            subnode = nodes_by_path.get(record.source_paths[0])
        if subnode is None:
            # The boundary disappeared since B; keep the unit pending and let
            # the next snapshot pass rebuild the ledger.
            updated.append(record)
            continue
        candidate = WorkCandidate(
            work_unit_id=record.work_unit_id,
            boundary_key=record.boundary_key,
            source_paths=record.source_paths,
            display_label=subnode.name,
            proposed_media_context=record.media_context,
            boundary_evidence=BoundaryEvidence(
                role=DirectoryRole(record.role),
                confidence=1.0,
                reasons=(),
                competing_roles=(),
            ),
        )
        evidence: IdentityEvidence = extract_identity_evidence(
            candidate,
            subnode,
            parent_labels=_parent_labels(node, subnode),
        )
        try:
            best, candidates = auto_match_from_evidence(
                tmdb_client,
                evidence,
                min_confidence=min_confidence,
                prefer_animation=prefer_animation,
            )
        except AutoMatchAmbiguityError as exc:
            updated.append(replace(
                record,
                identity_status="uncertain",
                identity=None,
                candidate_identities=tuple(exc.candidates),
                attention=str(exc),
                updated_at=_now(),
            ))
            continue
        except PlanError as exc:
            updated.append(replace(
                record,
                identity_status="uncertain",
                identity=None,
                candidate_identities=(),
                attention=str(exc),
                updated_at=_now(),
            ))
            continue
        updated.append(replace(
            record,
            identity_status="confirmed",
            identity=_identity_projection(best),
            candidate_identities=tuple(bounded_auto_match_candidate_rows(candidates)),
            attention=None,
            updated_at=_now(),
        ))
    save_work_unit_records(state_root, root_task_id, updated)
    return updated


def apply_work_unit_override(
    state_root: Path,
    root_task_id: str,
    work_unit_id: str,
    *,
    media_type: str,
    tmdb_id: int,
    season: int | None = None,
) -> WorkUnitRecord:
    """Persist a durable operator identity confirmation for one work unit.

    Only ``media_type + tmdb_id`` (and an optional season) are accepted — the
    minimal confirmation surface the contract allows.  The override survives
    every later resolve pass because ``resolve_work_unit_identities`` never
    downgrades a confirmed record.
    """
    if media_type not in {"movie", "tv"}:
        raise ValueError("media_type 必须是 movie 或 tv")
    if isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0:
        raise ValueError("tmdb_id 必须是正整数")
    if season is not None and (
        isinstance(season, bool) or not isinstance(season, int) or season <= 0
    ):
        raise ValueError("season 必须是正整数或 None")
    records = load_work_unit_records(state_root, root_task_id)
    for index, record in enumerate(records):
        if record.work_unit_id != work_unit_id:
            continue
        identity = {
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "season": season,
            "source": "operator_override",
            "title": None,
            "year": None,
            "confidence": 1.0,
        }
        records[index] = replace(
            record,
            identity_status="confirmed",
            identity=identity,
            # A new identity invalidates any decision computed for the old
            # one; the next D pass must re-evaluate the unit, and any
            # finished E lane must re-run for the new identity.
            reconciliation_outcome=None,
            matched_work_root=None,
            uncovered_tokens=(),
            lane_status=None,
            lane_detail=None,
            attention=None,
            updated_at=_now(),
        )
        save_work_unit_records(state_root, root_task_id, records)
        return records[index]
    raise KeyError(f"work unit 不存在: {work_unit_id}")


__all__ = [
    "DEFAULT_MIN_CONFIDENCE",
    "apply_work_unit_override",
    "resolve_work_unit_identities",
]
