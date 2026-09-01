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
import re
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
from .media_policy import DISC_IMAGE_INSPECTION_REQUIRED
from .root_boundaries import load_source_snapshot
from .source_inventory import (
    SourceNode,
    build_scoped_source_node,
    build_source_inventory,
    collect_all_files,
    has_disc_image_files,
    iter_source_nodes,
)
from .work_units import (
    IdentityEvidence,
    WorkUnitRecord,
    extract_identity_evidence,
    load_work_unit_records,
    save_work_unit_records,
)

DEFAULT_MIN_CONFIDENCE = 0.70


# A generic season leaf is a structural label, not work-title evidence.  The
# CJK form is deliberately included because an intake container often uses
# ``第一季``/``第二季`` alongside a meaningful parent title.  Keep this local
# to C/U retry provenance: B/W remains responsible for ownership boundaries.
_GENERIC_SEASON_LABEL_RE = re.compile(
    r"^\s*(?:"
    r"(?:season|s)\s*0*\d{1,3}"
    r"|第\s*(?:\d{1,3}|[一二三四五六七八九十百零〇两]{1,5})\s*季"
    r"|(?:[一二三四五六七八九十百零〇两]{1,5})\s*季"
    r")\s*$",
    re.IGNORECASE,
)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parent_labels(root: SourceNode, unit: SourceNode) -> tuple[str, ...]:
    """Return ancestor container labels from the snapshot root to the unit."""
    root_path = root.path.rstrip("/")
    unit_path = unit.path.rstrip("/")
    if unit_path == root_path:
        return ()
    if not unit_path.startswith(root_path + "/"):
        return ()
    relative = unit_path[len(root_path):].lstrip("/")
    parts = relative.split("/")[:-1]
    # ``root`` is itself the user-owned intake container.  Omitting its name
    # made a direct child named only ``第一季``/``Season 02`` query TMDB as a
    # work title by itself.  It is formal B-snapshot evidence, so retain it
    # ahead of intermediate ancestors for every strict descendant.  A work
    # rooted at the intake directory still has no parent clue.
    labels = [root.name, *parts]
    return tuple(label for label in labels if str(label).strip())


def _common_parent_labels(root: SourceNode, units: tuple[SourceNode, ...]) -> tuple[str, ...]:
    """Return only parent labels shared by every exact source subtree."""
    if not units:
        return ()
    labels = [_parent_labels(root, unit) for unit in units]
    common: list[str] = []
    for parts in zip(*labels):
        if len(set(parts)) != 1:
            break
        common.append(parts[0])
    return tuple(common)


def _source_nodes_by_path(root: SourceNode) -> dict[str, SourceNode]:
    """Index directory nodes plus exact-file virtual nodes from one snapshot.

    B/W may assign a flat feature file as the complete source scope of a
    WorkUnit.  C still needs the same parent-label calculation used for
    directory scopes; materializing a one-file node here keeps that evidence
    path generic and avoids falling back to the whole intake directory.
    """
    indexed = {
        candidate.path: candidate for candidate in iter_source_nodes(root)
    }
    for source_file in collect_all_files(root):
        indexed.setdefault(
            source_file.path,
            SourceNode(
                path=source_file.path,
                name=source_file.name,
                files=(source_file,),
                children=(),
                depth=max(
                    0,
                    root.depth
                    + source_file.path.count("/")
                    - root.path.count("/"),
                ),
            ),
        )
    return indexed


def _is_generic_season_label(value: object) -> bool:
    """Whether a persisted query is only a structural season leaf."""
    return bool(_GENERIC_SEASON_LABEL_RE.fullmatch(str(value or "")))


def _automatic_identity_needs_parent_context_recheck(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> bool:
    """Detect an automatic match earned from parent-side evidence alone.

    Two shapes qualify.  Older C/U records can be ``confirmed`` solely
    because TMDB returned a plausible result for ``第一季``/``Season 02``;
    that is not a durable identity proof when the exact B snapshot shows the
    source is really a child of a titled intake container.  Newer records
    also mark when the earning query was the total-miss parent escalation —
    a recovery round whose ancestor label (a franchise-bundle root
    collapsing to a bare prefix) can select the wrong franchise sibling.
    Re-evaluate only on an explicit retry, only before a writer exists, and
    never replace an operator's confirmation.  This is provenance repair,
    not a title or TMDB-ID rule.
    """
    identity = record.identity if isinstance(record.identity, dict) else {}
    if (
        record.identity_status != "confirmed"
        or record.writer_job_id is not None
        or identity.get("source") == "operator_override"
    ):
        return False
    trace = identity.get("decision_trace")
    if not isinstance(trace, dict):
        return False
    # Newer records retain the actual query that earned the strongest title
    # evidence.  Reopen only when that variant itself is a bare season leaf;
    # the durable boundary label may still be generic after a valid parent or
    # representative-media query confirms the work.
    matched_variant = trace.get("matched_query_variant")
    stale_query = (
        _is_generic_season_label(matched_variant)
        if isinstance(matched_variant, str) and matched_variant.strip()
        else _is_generic_season_label(trace.get("query"))
    )
    # A total-miss parent escalation is a recovery round: the earning query
    # was not the boundary's own evidence but an ancestor container label,
    # which a generic franchise-bundle root collapses into a bare prefix
    # that can select the wrong franchise sibling.  Such a confirmation is
    # not durable against matcher fixes — an explicit retry must re-run C.
    stale_query = stale_query or bool(trace.get("matched_query_via_parent_escalation"))
    if not stale_query:
        return False
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return False
    try:
        root = build_source_inventory(snapshot["rows"], snapshot["root"])
        nodes_by_path = _source_nodes_by_path(root)
        scoped_units = tuple(
            nodes_by_path[path]
            for path in record.source_paths
            if path in nodes_by_path
        )
    except (KeyError, TypeError, ValueError):
        return False
    return bool(_common_parent_labels(root, scoped_units))


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


def _park_for_disc_image(record: WorkUnitRecord) -> WorkUnitRecord:
    """Persist a non-bypassable C/U stop for an opaque disc-image boundary."""
    if (
        record.requires_content_expansion
        and record.identity_status == "uncertain"
        and record.identity is None
        and record.reconciliation_outcome is None
        and record.attention == DISC_IMAGE_INSPECTION_REQUIRED
    ):
        return record
    return replace(
        record,
        requires_content_expansion=True,
        media_context="unknown",
        identity_status="uncertain",
        identity=None,
        candidate_identities=(),
        reconciliation_outcome=None,
        matched_work_root=None,
        reconciliation_evidence=None,
        uncovered_tokens=(),
        lane_status=None,
        lane_detail=None,
        gap_status=None,
        gap_detail=None,
        attention=(record.attention or DISC_IMAGE_INSPECTION_REQUIRED),
        updated_at=_now(),
    )


def _snapshot_scope_has_disc_image(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> bool:
    """Read only the B snapshot to protect an identity override from bypass."""
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return False
    try:
        root = build_source_inventory(snapshot["rows"], snapshot["root"])
        scoped = build_scoped_source_node(
            root,
            record.source_paths,
            boundary_key=record.boundary_key,
            display_label=record.display_label,
        )
    except (KeyError, TypeError, ValueError):
        # A source scope which cannot be proven is not evidence that an ISO
        # disappeared.  The ordinary resolver will park it on the next pass.
        return False
    return has_disc_image_files(scoped)


def _pending_physical_special_recheck(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> bool:
    """Whether a retry must revisit an unwritten legacy special selection.

    A persisted C result from before physical OAD/OVA evidence existed may
    have selected a regular parent by title similarity, then stalled at D.
    Do not rewrite completed work or an operator override.  For the narrow
    no-writer/uncertain-D state, rebuild *only* current B evidence and reopen
    C when it now proves a complete numbered physical-special run.  The next
    resolver still uses TMDB formally; this helper merely avoids pinning a
    stale automatic choice forever.
    """
    identity = record.identity if isinstance(record.identity, dict) else {}
    if (
        record.identity_status != "confirmed"
        or record.writer_job_id is not None
        or record.reconciliation_outcome not in {None, "uncertain"}
        or identity.get("source") == "operator_override"
    ):
        return False
    trace = identity.get("decision_trace")
    if isinstance(trace, dict) and (
        bool(trace.get("official_special_count_match"))
        and bool(trace.get("official_special_marker_hits"))
    ):
        return False
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return False
    try:
        root = build_source_inventory(snapshot["rows"], snapshot["root"])
        scoped = build_scoped_source_node(
            root,
            record.source_paths,
            boundary_key=record.boundary_key,
            display_label=record.display_label,
        )
        role = DirectoryRole(record.role)
    except (KeyError, TypeError, ValueError):
        return False
    evidence = extract_identity_evidence(
        WorkCandidate(
            work_unit_id=record.work_unit_id,
            boundary_key=record.boundary_key,
            source_paths=record.source_paths,
            display_label=record.display_label or scoped.name,
            proposed_media_context=record.media_context,
            boundary_evidence=BoundaryEvidence(
                role=role,
                confidence=1.0,
                reasons=(),
                competing_roles=(),
            ),
            requires_content_expansion=record.requires_content_expansion,
        ),
        scoped,
    )
    return bool(
        evidence.special_numbered_run_complete
        and evidence.special_episode_count
        and evidence.special_markers
    )


def _automatic_identity_needs_junk_movie_recheck(record: WorkUnitRecord) -> bool:
    """Whether a retry must re-resolve an automatic undated-movie identity.

    The matcher now refuses movie rows TMDB never dated or timed (search row
    without a date AND detail with neither release year nor runtime).  An
    older automatic confirmation whose accepted identity is exactly that
    shape — an ``unknown year`` movie — was accepted from junk catalogue
    data, so an explicit retry must re-resolve it under the current guards.
    An operator's confirmation is durable, and the writer carrier is kept:
    once C re-resolves, G retires the superseded carrier through the
    identity-mismatch rule.  This is provenance repair on the accepted
    evidence shape, not a title or TMDB-ID rule.
    """
    identity = record.identity if isinstance(record.identity, dict) else {}
    if (
        record.identity_status != "confirmed"
        or identity.get("source") == "operator_override"
        or str(identity.get("media_type")) != "movie"
        or str(identity.get("year") or "") != "未知年份"
    ):
        return False
    return True


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
    left untouched unless the exact B snapshot proves that the unit owns an
    opaque disc image.  An identity override is not a content-inspection
    override.  Infrastructure failures (``ApiError``) propagate so the caller
    can apply its normal retry policy; evidence-level failures park the single
    unit as ``uncertain``.
    """
    records = load_work_unit_records(state_root, root_task_id)
    snapshot = load_source_snapshot(state_root, root_task_id)
    if not records or snapshot is None:
        return records
    node = build_source_inventory(snapshot["rows"], snapshot["root"])
    nodes_by_path = _source_nodes_by_path(node)
    updated: list[WorkUnitRecord] = []
    for record in records:
        try:
            subnode = build_scoped_source_node(
                node,
                record.source_paths,
                boundary_key=record.boundary_key,
                display_label=record.display_label,
            )
        except ValueError as exc:
            # The boundary disappeared since B; keep the unit pending and let
            # the operator explicitly rebuild the ledger.  Do not fall back to
            # the first path: that would silently narrow a multi-season work.
            updated.append(replace(
                record,
                identity_status="uncertain",
                identity=None,
                candidate_identities=(),
                attention=f"来源边界无法按快照证明: {exc}",
                updated_at=_now(),
            ))
            continue
        # Re-inspect even a durable identity override.  An override confirms
        # a TMDB identity, not the opaque contents of an optical-disc image;
        # leaving a confirmed legacy record untouched here could otherwise
        # let it reach D/E/F after a policy upgrade.
        if record.requires_content_expansion or has_disc_image_files(subnode):
            updated.append(_park_for_disc_image(record))
            continue
        if record.identity_status != "pending":
            updated.append(record)
            continue
        scoped_units = tuple(
            nodes_by_path[path]
            for path in record.source_paths
            if path in nodes_by_path
        )
        candidate = WorkCandidate(
            work_unit_id=record.work_unit_id,
            boundary_key=record.boundary_key,
            source_paths=record.source_paths,
            display_label=record.display_label or subnode.name,
            proposed_media_context=record.media_context,
            boundary_evidence=BoundaryEvidence(
                role=DirectoryRole(record.role),
                confidence=1.0,
                reasons=(),
                competing_roles=(),
            ),
            requires_content_expansion=record.requires_content_expansion,
        )
        evidence: IdentityEvidence = extract_identity_evidence(
            candidate,
            subnode,
            parent_labels=_common_parent_labels(node, scoped_units),
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
        form_conflict = _boundary_media_form_conflict(record, best)
        if form_conflict is not None:
            updated.append(replace(
                record,
                identity_status="uncertain",
                identity=None,
                candidate_identities=tuple(
                    bounded_auto_match_candidate_rows(candidates)
                ),
                attention=form_conflict,
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


def _boundary_media_form_conflict(
    record: WorkUnitRecord,
    best: object,
) -> str | None:
    """Refuse an auto-confirm that contradicts a *structurally proven* form.

    ``DirectoryRole.MOVIE_COLLECTION`` is never a heuristic guess: only the
    conservative film splitters emit it, and each one demands a generic
    film-collection role label plus a substantial standalone per-film title
    (and, where a label cannot carry that proof, an explicit year or
    theatrical-form marker).  A unit carrying that role therefore *is* a
    feature boundary, so a winning ``tv`` candidate means the query was
    answered by an ancestor's series name rather than by this film.

    ``[AI-Raws] Fullmetal Alchemist the Movie The Sacred Star of Milos …``
    inside ``剧场版`` scored 1.0 against the parent show ``tv/31911`` on title
    text plus a parent bonus, with a zero movie-form alignment score.  Letting
    that confirm would hand a 22 GB feature to the TV work's planner.  Parking
    it keeps the sibling episode body moving and asks a human for the one fact
    the evidence cannot supply.
    """
    if record.role != DirectoryRole.MOVIE_COLLECTION.value:
        return None
    media_type = getattr(best, "media_type", None)
    if media_type != "tv":
        return None
    tmdb_id = getattr(best, "tmdb_id", None)
    title = getattr(best, "title", None) or ""
    return (
        "B/W 已证明这是电影合集内的独立正片边界，但自动匹配给出的是剧集身份"
        f" tv/{tmdb_id} {title}；拒绝自动确认，需人工确认电影身份"
    )


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
    minimal confirmation surface the contract allows.  It cannot confirm the
    opaque contents of a disc image, which remains parked until B/W has a
    read-only expanded inventory.
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
        if (
            record.requires_content_expansion
            or _snapshot_scope_has_disc_image(state_root, root_task_id, record)
        ):
            raise ValueError(DISC_IMAGE_INSPECTION_REQUIRED)
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
            reconciliation_evidence=None,
            uncovered_tokens=(),
            lane_status=None,
            lane_detail=None,
            gap_status=None,
            gap_detail=None,
            attention=None,
            updated_at=_now(),
        )
        save_work_unit_records(state_root, root_task_id, records)
        return records[index]
    raise KeyError(f"work unit 不存在: {work_unit_id}")


def requeue_uncertain_work_units(
    state_root: Path,
    root_task_id: str,
) -> list[WorkUnitRecord]:
    """Re-open C/U, D/U, or parked J records for one explicit RootJob retry.

    Automatic identity uncertainty is durable so the scheduler never spins on
    the same evidence.  An operator's explicit retry is the safe point to
    evaluate that evidence again after a generic matcher, library-index, or
    configuration fix.  A J attention/failed marker is likewise reopened
    only by this explicit operator action, so its completed writer carrier
    can retry the local catalog/ledger check without moving media. Confirmed
    identities (including durable overrides) and completed work otherwise
    remain untouched.
    """
    records = load_work_unit_records(state_root, root_task_id)
    if not records:
        return records
    changed = False
    updated: list[WorkUnitRecord] = []
    for record in records:
        if record.requires_content_expansion:
            # An identity/D/J retry is not content inspection.  Keep this
            # unit parked until a read-only expander produces a new B/W
            # snapshot without opaque image containers.
            updated.append(record)
            continue
        if _automatic_identity_needs_parent_context_recheck(
            state_root,
            root_task_id,
            record,
        ):
            # A pre-fix automatic match was accepted from a structural season
            # leaf alone.  The exact B snapshot now supplies its parent and
            # representative-media evidence, so an explicit retry must go
            # through C again before D can retain the old result.
            updated.append(replace(
                record,
                identity_status="pending",
                identity=None,
                candidate_identities=(),
                reconciliation_outcome=None,
                matched_work_root=None,
                reconciliation_evidence=None,
                uncovered_tokens=(),
                lane_status=None,
                lane_detail=None,
                gap_status=None,
                gap_detail=None,
                attention=None,
                updated_at=_now(),
            ))
            changed = True
            continue
        if record.identity_status == "uncertain":
            updated.append(replace(
                record,
                identity_status="pending",
                identity=None,
                candidate_identities=(),
                reconciliation_outcome=None,
                matched_work_root=None,
                reconciliation_evidence=None,
                uncovered_tokens=(),
                lane_status=None,
                lane_detail=None,
                gap_status=None,
                gap_detail=None,
                attention=None,
                updated_at=_now(),
            ))
            changed = True
            continue
        if _pending_physical_special_recheck(state_root, root_task_id, record):
            updated.append(replace(
                record,
                identity_status="pending",
                identity=None,
                candidate_identities=(),
                reconciliation_outcome=None,
                matched_work_root=None,
                reconciliation_evidence=None,
                uncovered_tokens=(),
                lane_status=None,
                lane_detail=None,
                gap_status=None,
                gap_detail=None,
                attention=None,
                updated_at=_now(),
            ))
            changed = True
            continue
        if _automatic_identity_needs_junk_movie_recheck(record):
            # A pre-guard automatic match accepted a movie row TMDB never
            # dated or timed.  Re-run C under the current release-evidence
            # guards; the writer carrier is deliberately preserved so G can
            # retire it through the identity-mismatch rule after the
            # re-resolution.
            updated.append(replace(
                record,
                identity_status="pending",
                identity=None,
                candidate_identities=(),
                reconciliation_outcome=None,
                matched_work_root=None,
                reconciliation_evidence=None,
                uncovered_tokens=(),
                lane_status=None,
                lane_detail=None,
                gap_status=None,
                gap_detail=None,
                attention=None,
                updated_at=_now(),
            ))
            changed = True
            continue
        if record.identity_status == "confirmed" and record.reconciliation_outcome == "uncertain":
            updated.append(replace(
                record,
                reconciliation_outcome=None,
                matched_work_root=None,
                uncovered_tokens=(),
                # An operator-issued retry is also the explicit safe point
                # to retry J after a catalog/evidence attention.  It never
                # discards a completed writer carrier or re-plans media.
                gap_status=None,
                gap_detail=None,
                attention=None,
                updated_at=_now(),
            ))
            changed = True
            continue
        if record.gap_status in {"attention", "failed"}:
            updated.append(replace(
                record,
                gap_status=None,
                gap_detail=None,
                attention=None,
                updated_at=_now(),
            ))
            changed = True
            continue
        updated.append(record)
    if changed:
        save_work_unit_records(state_root, root_task_id, updated)
    return updated


__all__ = [
    "DEFAULT_MIN_CONFIDENCE",
    "apply_work_unit_override",
    "requeue_uncertain_work_units",
    "resolve_work_unit_identities",
]
