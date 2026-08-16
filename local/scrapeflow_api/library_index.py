"""D-node composition: three-shelf LibraryIndex and five-way reconciliation.

Builds a read-only index of confirmed works across the three formal shelves
(电影 / 番剧 / 美剧) from NFO identities and episode-coordinate coverage, then
reconciles each confirmed WorkUnit of a root task into exactly one of the five
contract outcomes: ``uncertain`` / ``merge_existing`` / ``existing_gap`` /
``duplicate_complete`` / ``new_work``.

The index is always queried across **all three shelves**: an existing work in
a different shelf is inherited, never duplicated (contract rule D).  A
conflicting multi-shelf presence fails closed as ``uncertain``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping, Sequence

from engine.scrapeflow.media_policy import is_video_filename
from engine.scrapeflow.replenishment_matching import audit_episode_tokens
from engine.scrapeflow.root_boundaries import load_source_snapshot
from engine.scrapeflow.source_inventory import SourceNode, build_source_inventory
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)

from .simple_library_audit import _read_nfo_identity

FORMAL_SHELF_SEGMENTS: tuple[str, ...] = ("电影", "番剧", "美剧")
SHELF_BY_SEGMENT: dict[str, str] = {
    "电影": "movie",
    "番剧": "anime",
    "美剧": "us_tv",
}

MAX_INDEX_WORKS = 2_000
MAX_INDEX_FILES = 200_000
MAX_WORK_DIRECTORIES = 500

OUTCOMES = (
    "uncertain",
    "merge_existing",
    "existing_gap",
    "duplicate_complete",
    "new_work",
)


@dataclass(frozen=True)
class IndexedWork:
    """One confirmed work found in one formal shelf."""

    media_type: str  # "movie" | "tv"
    tmdb_id: int
    shelf: str  # movie | anime | us_tv
    work_root: str
    title: str = ""
    year: str = ""
    episode_tokens: frozenset[str] = frozenset()


@dataclass(frozen=True)
class ReconciliationDecision:
    """The D1 five-way verdict for one WorkUnit."""

    outcome: str
    shelf: str | None
    work_root: str | None
    reasons: tuple[str, ...] = ()
    # Precise coordinate evidence consumed by the E2 lane:
    # ``new_tokens`` are input coordinates the existing work lacks;
    # ``uncovered_tokens`` are known gaps the input does not cover.
    new_tokens: frozenset[str] = frozenset()
    uncovered_tokens: frozenset[str] = frozenset()


@dataclass(frozen=True)
class LibraryIndex:
    works: tuple[IndexedWork, ...]

    def entries_for(self, media_type: str, tmdb_id: int) -> tuple[IndexedWork, ...]:
        return tuple(
            work
            for work in self.works
            if work.media_type == media_type and work.tmdb_id == tmdb_id
        )


def _safe_child_name(value: object) -> str | None:
    if not isinstance(value, str) or not value or value in {".", ".."}:
        return None
    if "/" in value or "\\" in value or "\x00" in value:
        return None
    return value


def build_library_index(alist: object, media_root: str) -> LibraryIndex:
    """Walk the three formal shelves and index NFO-confirmed works.

    A work without a readable, unambiguous NFO identity is not indexable and
    simply does not participate in cross-shelf dedup (fail closed toward
    ``new_work`` — the writer's no-overwrite gate stays the final guard).
    """
    listing = getattr(alist, "list", None)
    if not callable(listing):
        raise ValueError("AList client lacks list()")
    works: list[IndexedWork] = []
    file_count = 0
    for segment in FORMAL_SHELF_SEGMENTS:
        shelf = SHELF_BY_SEGMENT[segment]
        root = f"{str(media_root).rstrip('/')}/{segment}"
        try:
            rows = listing(root, refresh=True)
        except TypeError:
            rows = listing(root)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, Mapping) or row.get("is_dir") is not True:
                continue
            name = _safe_child_name(row.get("name"))
            if name is None:
                continue
            work_root = f"{root}/{name}"
            identity: dict[str, object] | None = None
            tokens: set[str] = set()
            stack = [work_root]
            directory_count = 0
            while stack:
                current = stack.pop()
                directory_count += 1
                if directory_count > MAX_WORK_DIRECTORIES:
                    raise ValueError(f"作品目录深度/数量超过索引上限: {work_root}")
                try:
                    items = listing(current, refresh=True)
                except TypeError:
                    items = listing(current)
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, Mapping):
                        continue
                    child_name = _safe_child_name(item.get("name"))
                    if child_name is None:
                        continue
                    child_path = current.rstrip("/") + "/" + child_name
                    if item.get("is_dir") is True:
                        stack.append(child_path)
                        continue
                    file_count += 1
                    if file_count > MAX_INDEX_FILES:
                        raise ValueError(
                            f"正式库索引文件数超过安全上限 {MAX_INDEX_FILES}"
                        )
                    lowered = child_name.casefold()
                    if lowered in {"tvshow.nfo", "movie.nfo"}:
                        identity = _read_nfo_identity(alist, child_path)
                    elif lowered.endswith(".nfo"):
                        # The movie planner names the root NFO
                        # "<title> (year).nfo"; recognise it by its XML root
                        # (<movie>/<tvshow> with a tmdbid) instead of the
                        # filename, and keep episode/special NFOs out.
                        parsed = _read_nfo_identity(alist, child_path)
                        if parsed is not None and parsed.get("tmdb_id"):
                            identity = parsed
                    elif is_video_filename(child_name):
                        for season, episode in audit_episode_tokens(child_name):
                            tokens.add(f"S{season:02d}E{episode:02d}")
            if identity is None:
                continue
            try:
                tmdb_id = int(identity["tmdb_id"])
                media_type = str(identity["media_type"])
            except (KeyError, TypeError, ValueError):
                continue
            works.append(IndexedWork(
                media_type=media_type,
                tmdb_id=tmdb_id,
                shelf=shelf,
                work_root=work_root,
                title=str(identity.get("title") or ""),
                year=str(identity.get("year") or ""),
                episode_tokens=frozenset(tokens),
            ))
            if len(works) > MAX_INDEX_WORKS:
                raise ValueError(f"正式库作品数超过索引上限 {MAX_INDEX_WORKS}")
    return LibraryIndex(tuple(works))


def decide_reconciliation(
    index: LibraryIndex,
    *,
    media_type: str,
    tmdb_id: int,
    unit_tokens: frozenset[str] = frozenset(),
    known_gap_tokens: frozenset[str] = frozenset(),
) -> ReconciliationDecision:
    """Compute the D1 verdict with the contract priority order.

    Priority: uncertain (conflict) → merge_existing (new media) →
    existing_gap (confirmed gaps, no new media) → duplicate_complete →
    new_work (absent from all three shelves).
    """
    matches = index.entries_for(media_type, tmdb_id)
    if not matches:
        return ReconciliationDecision(
            "new_work", None, None, ("三个正式库均无该身份，视为全新作品",),
        )
    roots = {(work.shelf, work.work_root) for work in matches}
    if len(roots) > 1:
        return ReconciliationDecision(
            "uncertain", None, None,
            ("同一身份存在于多个货架/作品根，无法安全判定",),
        )
    shelf, work_root = next(iter(roots))
    existing_tokens: set[str] = set()
    for work in matches:
        existing_tokens.update(work.episode_tokens)
    new_tokens = set(unit_tokens) - existing_tokens
    if new_tokens:
        return ReconciliationDecision(
            "merge_existing", shelf, work_root,
            (f"输入包含 {len(new_tokens)} 个既有作品没有的新媒体",),
            new_tokens=frozenset(new_tokens),
        )
    uncovered = set(known_gap_tokens) - existing_tokens - set(unit_tokens)
    if uncovered:
        return ReconciliationDecision(
            "existing_gap", shelf, work_root,
            (f"既有作品存在 {len(uncovered)} 个确认缺口，当前输入不含对应内容",),
            uncovered_tokens=frozenset(uncovered),
        )
    return ReconciliationDecision(
        "duplicate_complete", shelf, work_root,
        ("输入媒体已全部存在于正式库",),
    )


def _iter_nodes(node: SourceNode) -> list[SourceNode]:
    output = [node]
    for child in node.children:
        output.extend(_iter_nodes(child))
    return output


def _unit_episode_tokens(node: SourceNode | None) -> frozenset[str]:
    if node is None:
        return frozenset()
    tokens: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        for file in current.files:
            if file.object_type != "video":
                continue
            for season, episode in audit_episode_tokens(file.name):
                tokens.add(f"S{season:02d}E{episode:02d}")
        stack.extend(current.children)
    return frozenset(tokens)


def reconcile_root_work_units(
    alist: object,
    media_root: str,
    state_root: Any,
    root_task_id: str,
    *,
    known_gap_tokens_by_identity: Mapping[tuple[str, int], Sequence[str]] | None = None,
) -> list[WorkUnitRecord]:
    """Run the D step for one root task and persist each unit's decision.

    Only ``confirmed`` units without an existing decision are re-evaluated, so
    retries are idempotent and durable overrides stay authoritative.
    """
    records = load_work_unit_records(state_root, root_task_id)
    snapshot = load_source_snapshot(state_root, root_task_id)
    if not records or snapshot is None:
        return records
    index = build_library_index(alist, media_root)
    node = build_source_inventory(snapshot["rows"], snapshot["root"])
    nodes_by_path = {candidate.path: candidate for candidate in _iter_nodes(node)}
    known = known_gap_tokens_by_identity or {}
    updated: list[WorkUnitRecord] = []
    for record in records:
        if record.identity_status != "confirmed" or record.reconciliation_outcome is not None:
            updated.append(record)
            continue
        identity = record.identity or {}
        try:
            media_type = str(identity["media_type"])
            tmdb_id = int(identity["tmdb_id"])
            decision = decide_reconciliation(
                index,
                media_type=media_type,
                tmdb_id=tmdb_id,
                unit_tokens=_unit_episode_tokens(
                    nodes_by_path.get(record.source_paths[0])
                    if record.source_paths else None
                ),
                known_gap_tokens=frozenset(
                    str(token) for token in known.get((media_type, tmdb_id), ())
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            decision = ReconciliationDecision(
                "uncertain", None, None, (f"单元身份记录无效: {exc}",),
            )
        updated.append(replace(
            record,
            reconciliation_outcome=decision.outcome,
            matched_work_root=decision.work_root,
            uncovered_tokens=(
                tuple(sorted(decision.uncovered_tokens))
                if decision.outcome == "existing_gap"
                else ()
            ),
            attention=(
                "; ".join(decision.reasons)
                if decision.outcome == "uncertain"
                else None
            ),
        ))
    save_work_unit_records(state_root, root_task_id, updated)
    return updated


__all__ = [
    "FORMAL_SHELF_SEGMENTS",
    "IndexedWork",
    "LibraryIndex",
    "OUTCOMES",
    "ReconciliationDecision",
    "SHELF_BY_SEGMENT",
    "build_library_index",
    "decide_reconciliation",
    "reconcile_root_work_units",
]
