"""B→C bridge: expand parked disc-image scopes into task-owned staging.

B/W parks any scope that holds optical-disc images behind
``DISC_IMAGE_INSPECTION_REQUIRED`` because no directory heuristic can prove
what is inside an image.  This module is that missing read-only expander:

1. match the scope's TMDB identity with the same evidence extractor C uses;
2. parse the scope's explicit season marker;
3. fetch the season roster from TMDB;
4. probe the images and prove the playlist→episode mapping with the
   engine's own evidence (order-preserving duration DP, else a filed
   operator ruling that still cannot contradict the discs' own durations);
5. transfer the proven mappings into task-owned remote staging, resumably;
6. replace the parked record with ordinary records re-derived from the
   staged tree, carrying the expansion provenance, and merge the staged
   tree into the persisted B snapshot.

A scope that cannot be proven stays parked with its attention reason; a
transfer error parks that scope without touching its siblings.  Nothing is
ever written from an unproven mapping, and the intake ingress keeps owning
the original images until terminal consumption.
"""

from __future__ import annotations

import posixpath
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .disc_expansion import (
    DiscExpansionError,
    DiscExpansionExecutor,
    PlaylistCandidate,
    ScopeExpansionPlan,
    ScopeMappingRuling,
    apply_scope_mapping_ruling,
    collect_playlist_candidates,
    derive_scope_expansion,
)
from .disc_image import DiscImageError, probe_disc_image_via_alist
from .identity_matching import _season_from_source, auto_match_from_evidence
from .root_boundaries import (
    persist_root_boundary_analysis,
    walk_source_rows,
)
from .serialization import atomic_write_json
from .source_inventory import build_scoped_source_node, build_source_inventory
from .source_objects import (
    SourceManifest,
    SourceObjectValidationError,
    validate_unique_source_object_ownership,
)
from .boundary_analysis import analyze_boundaries
from .work_units import (
    WorkUnitRecord,
    create_work_units_from_candidates,
    extract_identity_evidence,
)


class DiscExpansionBridgeError(RuntimeError):
    """A bridge-level failure that parks one scope without touching siblings."""


class DiscExpansionPauseRequested(RuntimeError):
    """The expansion stopped at a pause boundary; state stays resumable."""


def expansion_staging_root(media_root: str, root_task_id: str) -> str:
    """Return the task-owned remote staging root for disc expansion.

    Mirrors the archive lane's ``{media_root}/ScrapeFlow/归档/<job>`` layout:
    expansion is the third task-owned staging marker beside ``补源`` and
    ``归档``.
    """
    normalized = str(media_root).rstrip("/")
    if not normalized.startswith("/"):
        raise ValueError("media_root 必须是绝对路径")
    return f"{normalized}/ScrapeFlow/展开/{root_task_id}"


def disc_rulings_path(state_root: Path, root_task_id: str) -> Path:
    """Return the operator ruling store for one root task."""
    return state_root / f"disc-rulings-{root_task_id}.json"


def load_disc_rulings(
    state_root: Path,
    root_task_id: str,
) -> dict[str, ScopeMappingRuling]:
    """Load filed operator rulings keyed by their source scope.

    A malformed entry fails closed: an operator ruling is a strong artifact
    and may never be silently skipped.
    """
    path = disc_rulings_path(state_root, root_task_id)
    try:
        import json

        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise DiscExpansionBridgeError(
            f"光盘展开裁决存储损坏: {path}; {exc}"
        ) from exc
    if not isinstance(raw, list):
        raise DiscExpansionBridgeError(f"光盘展开裁决存储损坏: {path}")
    rulings: dict[str, ScopeMappingRuling] = {}
    for entry in raw:
        ruling = ScopeMappingRuling.from_mapping(entry)
        if ruling.scope_path in rulings:
            raise DiscExpansionBridgeError(
                f"同一来源范围存在多条光盘展开裁决: {ruling.scope_path}"
            )
        rulings[ruling.scope_path] = ruling
    return rulings


def save_disc_ruling(
    state_root: Path,
    root_task_id: str,
    ruling: ScopeMappingRuling,
) -> None:
    """Append one validated operator ruling to the task's ruling store."""
    import json

    path = disc_rulings_path(state_root, root_task_id)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raw = []
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise DiscExpansionBridgeError(
            f"光盘展开裁决存储损坏: {path}; {exc}"
        ) from exc
    if not isinstance(raw, list):
        raise DiscExpansionBridgeError(f"光盘展开裁决存储损坏: {path}")
    kept = [
        entry
        for entry in raw
        if not (
            isinstance(entry, Mapping)
            and entry.get("scope_path") == ruling.scope_path
        )
    ]
    kept.append(ruling.as_dict())
    atomic_write_json(path, kept, allow_nan=False)


def fetch_season_roster(
    tmdb_client: Any,
    tmdb_id: int,
    season: int,
) -> "SeasonEpisodeRoster | None":
    """Fetch one season's episode roster from TMDB.

    A season with no episodes returns ``None`` (nothing to prove against).
    An episode without a published runtime keeps ``None`` so the mapping
    proof fails closed on it.
    """
    from .disc_expansion import SeasonEpisodeRoster

    payload = tmdb_client.get(f"/tv/{int(tmdb_id)}/season/{int(season)}")
    episodes_raw = payload.get("episodes")
    if not isinstance(episodes_raw, list) or not episodes_raw:
        return None
    episodes: list[tuple[int, int | None]] = []
    for entry in episodes_raw:
        if not isinstance(entry, Mapping):
            raise DiscExpansionBridgeError("TMDB 季集清单条目格式异常")
        number = entry.get("episode_number")
        if isinstance(number, bool) or not isinstance(number, int):
            raise DiscExpansionBridgeError("TMDB 季集清单缺少集号")
        runtime = entry.get("runtime")
        if isinstance(runtime, bool) or not isinstance(runtime, int):
            runtime = None
        episodes.append((number, runtime))
    episodes.sort(key=lambda item: item[0])
    return SeasonEpisodeRoster(season=int(season), episodes=tuple(episodes))


def _common_ancestor(left: str, right: str) -> str:
    left_parts = [part for part in left.split("/") if part]
    right_parts = [part for part in right.split("/") if part]
    shared = 0
    for a, b in zip(left_parts, right_parts):
        if a != b:
            break
        shared += 1
    if shared == 0:
        raise DiscExpansionBridgeError(
            f"来源根与展开 staging 无公共祖先: {left} vs {right}"
        )
    return "/" + "/".join(left_parts[:shared])


def _scope_identity(
    tmdb_client: Any,
    scoped_node: Any,
    record: WorkUnitRecord,
    *,
    inventory_root: Any,
    season: int | None,
    prefer_animation: bool,
    min_confidence: float,
) -> tuple[int, str]:
    """Match one parked scope's TMDB identity from its boundary evidence.

    Reuses the C lane's evidence extractor and auto matcher — including its
    parent-label escalation over the snapshot inventory — so the bridge can
    never confirm an identity C itself would not reach.  A scope whose own
    path carries a season marker is TV-shaped by that evidence alone: a
    movie identity cannot own a season, so the match stays inside the tv
    type instead of tying against same-named films.
    """
    from .boundary_analysis import BoundaryEvidence, DirectoryRole, WorkCandidate
    from .unit_identity import _common_parent_labels, _useful_parent_labels

    candidate = WorkCandidate(
        work_unit_id=record.work_unit_id,
        boundary_key=record.boundary_key,
        source_paths=record.source_paths,
        display_label=record.display_label or scoped_node.name,
        proposed_media_context=(
            "tv" if season is not None else record.media_context
        ),
        boundary_evidence=BoundaryEvidence(
            role=DirectoryRole(record.role),
            confidence=1.0,
            reasons=(),
            competing_roles=(),
        ),
        requires_content_expansion=True,
    )
    parent_labels = _useful_parent_labels(
        record,
        candidate.display_label,
        _common_parent_labels(inventory_root, (scoped_node,)),
    )
    evidence = extract_identity_evidence(
        candidate, scoped_node, parent_labels=parent_labels
    )
    best, _candidates = auto_match_from_evidence(
        tmdb_client,
        evidence,
        min_confidence=min_confidence,
        prefer_animation=prefer_animation,
    )
    identity = {
        "media_type": best.media_type,
        "tmdb_id": best.tmdb_id,
        "title": best.title,
    }
    if best.media_type != "tv":
        raise DiscExpansionBridgeError(
            f"光盘镜像范围匹配到非剧集身份: {best.title}"
        )
    return int(best.tmdb_id), str(best.title)


def validate_unit_scopes_for_root(
    ingress: str,
    media_root: str,
    root_task_id: str,
    record: WorkUnitRecord,
) -> tuple[str, ...]:
    """Validate unit scopes against the ingress, or expansion staging.

    An ordinary unit's scopes must descend from the intake ingress.  A unit
    born from the disc-expansion bridge instead owns scopes inside the
    task-derived expansion staging root — the only other tree the root job
    may legitimately read media from.  That root is re-derived from the media
    root and task id, never trusted from the record payload, and it is
    checked first: an ingress that happens to sit above the media root must
    not widen an expansion-born record's reach into another task's staging.
    """
    from .source_inventory import validate_source_scope

    if record.disc_expansion is not None:
        staging_root = expansion_staging_root(media_root, root_task_id)
        return validate_source_scope(staging_root, record.source_paths)
    return validate_source_scope(ingress, record.source_paths)


def _park_with(
    record: WorkUnitRecord,
    reason: str,
) -> WorkUnitRecord:
    return replace(
        record,
        identity_status="uncertain",
        identity=None,
        candidate_identities=(),
        attention=reason,
    )


def _probe_scope_candidates(
    alist: Any,
    scoped_node: Any,
) -> list[PlaylistCandidate]:
    """Probe every disc image in the scope and collect playlist candidates."""
    from .source_inventory import collect_all_files

    images = [
        f
        for f in collect_all_files(scoped_node)
        if f.object_type == "disc_image"
    ]
    if not images:
        raise DiscExpansionBridgeError("范围内没有光盘镜像文件")
    candidates: list[PlaylistCandidate] = []
    for image in sorted(images, key=lambda f: f.path):
        info = alist.exact_file_info(image.path)
        inventory = probe_disc_image_via_alist(alist, image.path)
        candidates.extend(
            collect_playlist_candidates(
                image.path,
                inventory,
                image_size=info["size"],
                image_version=info["version"],
            )
        )
    if not candidates:
        raise DiscExpansionBridgeError(
            "镜像内没有可展开的正片 playlist，裁决无事可裁"
        )
    return candidates


def _synthetic_dir_row(path: str) -> dict[str, Any]:
    """A minimal directory row for the merged B snapshot.

    ``build_scoped_source_node`` matches scopes by exact path, so a staged
    scope directory needs its own row even though the walk itself only
    returns children.  The synthetic row mirrors the shape of the walk's
    directory entries.
    """
    normalized = path.rstrip("/")
    return {
        "path": "",
        "virtual_path": normalized,
        "name": posixpath.basename(normalized),
        "size": 0,
        "is_dir": True,
        "full_path": normalized,
    }


def _merge_snapshot(
    snapshot: Mapping[str, Any],
    staged_rows: Sequence[Mapping[str, Any]],
    staging_root: str,
) -> dict[str, Any]:
    """Merge walked staging rows into the persisted B snapshot.

    The merged root is the common ancestor of the ingress root and the
    task's staging root, so every row stays a real provider path and every
    record scope — ingress or staged — validates against the same snapshot.
    """
    merged_root = _common_ancestor(str(snapshot["root"]), staging_root)
    rows = list(snapshot.get("rows") or [])
    rows.extend(dict(row) for row in staged_rows)
    merged: dict[str, Any] = {"root": merged_root, "rows": rows}
    snapshot_id = f"{merged_root}:expansion:{len(rows)}"
    try:
        exact = SourceManifest.from_listing_rows(
            rows, root_path=merged_root, snapshot_id=snapshot_id,
        )
        merged["source_manifest"] = exact.as_dict()
    except SourceObjectValidationError as exc:
        merged["source_manifest_error"] = str(exc)
    return merged


def expand_root_disc_images(
    alist: Any,
    tmdb_client: Any,
    state_root: Path,
    root_task_id: str,
    records: Sequence[WorkUnitRecord],
    snapshot: Mapping[str, Any],
    *,
    media_root: str,
    prefer_animation: bool = False,
    min_confidence: float = 0.70,
    pause_requested: Callable[[], bool] | None = None,
    executor: DiscExpansionExecutor | None = None,
    roster_loader: Callable[[Any, int, int], Any] = fetch_season_roster,
) -> tuple[list[WorkUnitRecord], bool]:
    """Run the B→C disc expansion pass over one root's parked records.

    Returns the new ledger plus a flag telling whether any record changed.
    The persisted snapshot is rewritten only when at least one scope was
    converted, and every sibling record stays byte-identical.
    """
    parked = [
        record
        for record in records
        if record.requires_content_expansion and record.disc_expansion is None
    ]
    if not parked:
        return list(records), False

    staging_root = expansion_staging_root(media_root, root_task_id)
    rulings = load_disc_rulings(state_root, root_task_id)
    if executor is None:
        executor = DiscExpansionExecutor(
            alist,
            state_dir=str(state_root / f"disc-expansion-{root_task_id}"),
            local_buffer_dir=str(state_root / "expansion-buffers"),
        )

    inventory_node = build_source_inventory(
        snapshot.get("rows") or [], str(snapshot["root"]),
    )

    updated = [r for r in records if r not in parked]
    changed = False
    staged_rows: list[Mapping[str, Any]] = []
    for record in parked:
        if len(record.source_paths) != 1:
            updated.append(_park_with(
                record, "多来源范围的光盘镜像单元不能自动展开，请人工处理",
            ))
            continue
        scope = str(record.source_paths[0]).rstrip("/")
        try:
            scoped_node = build_scoped_source_node(
                inventory_node,
                [scope],
                boundary_key=record.boundary_key,
                display_label=record.display_label,
            )
        except ValueError as exc:
            updated.append(_park_with(
                record, f"光盘镜像范围无法按快照证明: {exc}",
            ))
            continue
        try:
            updated.append(_expand_one_scope(
                alist,
                tmdb_client,
                record,
                scoped_node,
                inventory_root=inventory_node,
                scope=scope,
                staging_root=staging_root,
                rulings=rulings,
                executor=executor,
                roster_loader=roster_loader,
                prefer_animation=prefer_animation,
                min_confidence=min_confidence,
                pause_requested=pause_requested,
                staged_rows=staged_rows,
            ))
            changed = True
        except DiscExpansionBridgeError as exc:
            updated.append(_park_with(record, str(exc)))
        except DiscExpansionError as exc:
            updated.append(_park_with(record, f"光盘展开传输失败: {exc}"))
        except DiscImageError as exc:
            # One unreadable or unrecognized image parks its own scope; its
            # siblings keep expanding.  A probe failure is not a root-level
            # failure, and the park reason keeps the evidence visible.
            updated.append(_park_with(record, f"光盘镜像探测失败: {exc}"))
    if changed:
        persist_root_boundary_analysis(
            state_root,
            root_task_id,
            _merge_snapshot(snapshot, staged_rows, staging_root),
            updated,
        )
    return updated, changed


def _expand_one_scope(
    alist: Any,
    tmdb_client: Any,
    record: WorkUnitRecord,
    scoped_node: Any,
    *,
    inventory_root: Any,
    scope: str,
    staging_root: str,
    rulings: Mapping[str, ScopeMappingRuling],
    executor: DiscExpansionExecutor,
    roster_loader: Callable[[Any, int, int], Any],
    prefer_animation: bool,
    min_confidence: float,
    pause_requested: Callable[[], bool] | None,
    staged_rows: list[Mapping[str, Any]],
) -> WorkUnitRecord:
    """Expand one parked scope; returns the replacement record.

    Raises ``DiscExpansionBridgeError`` with a park reason when the scope
    cannot be proven right now.
    """
    season = _season_from_source(scope)
    if season is None:
        raise DiscExpansionBridgeError(
            "光盘镜像范围没有可解析的季标记，无法选择 TMDB 集清单"
        )
    tmdb_id, title = _scope_identity(
        tmdb_client,
        scoped_node,
        record,
        inventory_root=inventory_root,
        season=season,
        prefer_animation=prefer_animation,
        min_confidence=min_confidence,
    )
    roster = roster_loader(tmdb_client, tmdb_id, season)
    if roster is None:
        raise DiscExpansionBridgeError(
            f"TMDB 第 {season} 季没有集清单，无法证明映射"
        )
    candidates = _probe_scope_candidates(alist, scoped_node)
    plan: ScopeExpansionPlan | None = derive_scope_expansion(
        scope_path=scope,
        season=season,
        roster=roster,
        candidates=candidates,
        staging_root=staging_root,
        work_name=title,
    )
    if not plan.proven:
        ruling = rulings.get(scope)
        if ruling is None:
            raise DiscExpansionBridgeError(
                f"光盘映射无法从镜像自身证明: {plan.attention}"
            )
        plan = apply_scope_mapping_ruling(
            scope_path=scope,
            season=season,
            roster=roster,
            candidates=candidates,
            ruling=ruling,
            staging_root=staging_root,
            work_name=title,
        )
    if not plan.mappings:
        raise DiscExpansionBridgeError("展开计划没有可传输的映射")

    # Transfer every proven mapping; the executor is resumable, so an
    # interrupted expansion re-enters here without re-reading gigabytes.
    by_clip = {}
    for mapping in plan.mappings:
        state = executor.load_state(mapping)
        if state is not None and state.status == "completed":
            continue
        if callable(pause_requested) and pause_requested():
            raise DiscExpansionPauseRequested(
                "光盘展开在暂停边界停止，保持可恢复"
            )
        candidate = mapping.candidate
        image = candidate.image_path
        if image not in by_clip:
            inventory = probe_disc_image_via_alist(alist, image)
            by_clip[image] = {
                f.inner_path.casefold(): f for f in inventory.inner_files
            }
        inner = by_clip[image].get(candidate.clip_inner_path.casefold())
        if inner is None or inner.size != candidate.clip_size:
            raise DiscExpansionBridgeError(
                f"镜像结构与展开声明不一致: {image} {candidate.clip_inner_path}"
            )
        executor.execute_mapping(mapping, inner_file=inner)

    members = []
    for mapping in plan.mappings:
        state = executor.load_state(mapping)
        if state is None or state.status != "completed":
            raise DiscExpansionBridgeError(
                f"展开状态缺失: S{mapping.season:02d}E{mapping.episode:02d}"
            )
        members.append({
            "episode": mapping.episode,
            "staged_path": mapping.target_path,
            "image_path": mapping.candidate.image_path,
            "playlist": mapping.candidate.playlist_inner_path,
            "clip": mapping.candidate.clip_inner_path,
            "clip_size": mapping.candidate.clip_size,
            "duration": mapping.candidate.duration_seconds,
            "output_bytes": state.output_bytes,
            "md5": state.md5,
            "sha1": state.sha1,
        })

    staged_scope = posixpath.join(staging_root, posixpath.basename(scope))
    rows = walk_source_rows(alist, staged_scope)
    if not rows:
        raise DiscExpansionBridgeError(
            f"展开 staging 树不可见: {staged_scope}"
        )
    # The walk lists children only; the converted unit's new source_path IS
    # the staged scope, so the merged snapshot must carry that directory row
    # itself (plus any ancestor between the merged root and the scope) or
    # build_scoped_source_node cannot find the scope and C parks the unit
    # with 来源范围在 B 快照中不存在.
    rows.insert(0, _synthetic_dir_row(staged_scope))
    staged_rows.extend(rows)
    staged_node = build_source_inventory(rows, staged_scope)
    candidates_new = analyze_boundaries(staged_node, root_task_id=record.root_task_id)
    if not candidates_new:
        raise DiscExpansionBridgeError("展开 staging 树没有可分析的作品单元")
    new_records = create_work_units_from_candidates(
        candidates_new,
        record.root_task_id,
        source_revision=record.source_revision,
    )
    provenance = {
        "basis": plan.basis,
        "tmdb_id": tmdb_id,
        "season": season,
        "work_name": title,
        "source_scope": scope,
        "staging_scope": staged_scope,
        "skipped_playlists": list(plan.skipped_playlists),
        "members": members,
    }
    stamped = [
        replace(
            record_new,
            disc_expansion=dict(provenance),
            expanded_from_scopes=(scope,),
        )
        for record_new in new_records
    ]
    if len(stamped) != 1:
        # A staged season tree that splits into several units would still be
        # correct, but it has never been observed; keep the shape explicit.
        raise DiscExpansionBridgeError(
            f"展开 staging 树分析出 {len(stamped)} 个单元，预期 1 个"
        )
    return stamped[0]


__all__ = [
    "DiscExpansionBridgeError",
    "disc_rulings_path",
    "expansion_staging_root",
    "expand_root_disc_images",
    "fetch_season_roster",
    "load_disc_rulings",
    "save_disc_ruling",
]
