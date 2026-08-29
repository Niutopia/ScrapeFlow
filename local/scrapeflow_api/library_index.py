"""D-node composition: three-shelf LibraryIndex and five-way reconciliation.

Builds a read-only index of confirmed works across the three formal shelves
(电影 / 番剧 / 欧美剧) from NFO identities and episode-coordinate coverage, then
reconciles each confirmed WorkUnit of a root task into exactly one of the five
contract outcomes: ``uncertain`` / ``merge_existing`` / ``existing_gap`` /
``duplicate_complete`` / ``new_work``.

The index is always queried across **all three shelves**: an existing work in
a different shelf is inherited, never duplicated (contract rule D).  A
conflicting multi-shelf presence fails closed as ``uncertain``.
"""

from __future__ import annotations

import posixpath
import re
import unicodedata
from dataclasses import dataclass, replace
from typing import Any, Callable, Mapping, Sequence

from engine.scrapeflow.boundary_analysis import (
    _SEASON_EPISODE_RE,
    _season_number_from_directory_name,
)
from engine.scrapeflow.core import (
    _pending_special_release_ordinal,
    _special_arc_title_key,
    special_season_window_candidates,
)
from engine.scrapeflow.identity_matching import (
    AUTO_MATCH_MIN_MARGIN,
    _clean_boundary_identity_query,
    _normalize_match_title,
    _official_physical_special_markers,
    _physical_special_marker_key,
    _season0_marker_and_ordinal,
    _title_similarity,
    physical_special_candidate_evidence,
)
from engine.scrapeflow.media_policy import (
    DISC_IMAGE_INSPECTION_REQUIRED,
    is_video_filename,
)
from engine.scrapeflow.replenishment_matching import (
    FRACTIONAL_EPISODE_RE,
    audit_episode_tokens,
    bare_regular_episode_context_is_safe,
    bare_regular_episode_number,
    bracketed_regular_episode_number,
    release_dash_regular_episode,
    release_title_ordinal_regular_episode,
)
from engine.scrapeflow.residual_policy import BONUS_DIRECTORY_SEGMENT_RE
from engine.scrapeflow.root_boundaries import load_source_snapshot, walk_source_rows
from engine.scrapeflow.source_inventory import (
    SourceNode,
    SourceFile,
    build_scoped_source_node,
    build_source_inventory,
    collect_all_files,
    iter_source_nodes,
)
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    is_physical_special_video_file,
    load_work_unit_records,
    physical_special_marker_evidence,
    physical_special_stem_ordinal,
    save_work_unit_records,
)

from .library_metadata import read_nfo_identity

FORMAL_SHELF_SEGMENTS: tuple[str, ...] = ("电影", "番剧", "欧美剧")
SHELF_BY_SEGMENT: dict[str, str] = {
    "电影": "movie",
    "番剧": "anime",
    "欧美剧": "us_tv",
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

_BARE_EPISODE_EVIDENCE_KIND = "tmdb_single_positive_season_bare_episodes"
_BRACKETED_EPISODE_EVIDENCE_KIND = (
    "tmdb_single_positive_season_bracketed_episodes"
)
_NAKED_NUMERIC_EPISODE_EVIDENCE_KIND = (
    "tmdb_single_positive_season_naked_numeric"
)
_RELEASE_DASH_EPISODE_EVIDENCE_KIND = (
    "tmdb_single_positive_season_release_dash_episodes"
)
_RELEASE_TITLE_ORDINAL_EPISODE_EVIDENCE_KIND = (
    "tmdb_single_positive_season_title_ordinal_episodes"
)
_PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND = (
    "tmdb_single_positive_season_physical_special"
)
_SINGLE_SEASON_EPISODE_EVIDENCE_KINDS = frozenset({
    _BARE_EPISODE_EVIDENCE_KIND,
    _BRACKETED_EPISODE_EVIDENCE_KIND,
    _NAKED_NUMERIC_EPISODE_EVIDENCE_KIND,
    _RELEASE_DASH_EPISODE_EVIDENCE_KIND,
    _RELEASE_TITLE_ORDINAL_EPISODE_EVIDENCE_KIND,
    _PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND,
})


def single_season_episode_evidence_label(evidence_kind: str) -> str:
    """Return the user-facing name of one finite D/F proof grammar."""
    if evidence_kind == _BRACKETED_EPISODE_EVIDENCE_KIND:
        return "纯方括号集号"
    if evidence_kind == _NAKED_NUMERIC_EPISODE_EVIDENCE_KIND:
        return "裸数字集号"
    if evidence_kind == _RELEASE_DASH_EPISODE_EVIDENCE_KIND:
        return "发行组短横线集号"
    if evidence_kind == _RELEASE_TITLE_ORDINAL_EPISODE_EVIDENCE_KIND:
        return "同标题裸序号集号"
    if evidence_kind == _PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND:
        return "完整 OAD/OVA/OAV 集号"
    return "裸 E"


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
class SingleSeasonEpisodeProof:
    """A D-derived, revalidatable season proof for an unqualified run.

    This is deliberately not an identity override.  It records only the
    evidence shape that was already proved against the current B snapshot and
    TMDB catalog, so F can repeat that same read-only proof before it sends a
    season to the planner.  ``evidence_kind`` is deliberately narrow: it
    distinguishes the parsing grammar F must revalidate, rather than turning
    a D result into a generic or user-injectable season override.
    """

    tmdb_id: int
    season: int
    episode_count: int
    episode_tokens: tuple[str, ...]
    evidence_kind: str = _BARE_EPISODE_EVIDENCE_KIND
    season_boundaries: tuple[tuple[int, int], ...] = ()

    def as_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "kind": self.evidence_kind,
            "tmdb_id": self.tmdb_id,
            "season": self.season,
            "episode_count": self.episode_count,
            "episode_tokens": list(self.episode_tokens),
        }
        if self.season_boundaries:
            result["season_boundaries"] = [
                [int(season), int(count)] for season, count in self.season_boundaries
            ]
        return result

    @classmethod
    def from_dict(cls, value: object) -> "SingleSeasonEpisodeProof | None":
        if not isinstance(value, Mapping):
            return None
        evidence_kind = value.get("kind")
        if (
            not isinstance(evidence_kind, str)
            or evidence_kind not in _SINGLE_SEASON_EPISODE_EVIDENCE_KINDS
        ):
            return None
        tmdb_id = _positive_season(value.get("tmdb_id"))
        episode_count = _positive_season(value.get("episode_count"))
        # Season 00 is a legal proof season, but only for the physical-special
        # grammar: an unqualified ``E01`` run never proves Season 00 ownership.
        raw_season = value.get("season")
        if (
            isinstance(raw_season, int)
            and not isinstance(raw_season, bool)
            and raw_season == 0
            and evidence_kind == _PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND
        ):
            season = 0
        else:
            season = _positive_season(raw_season)
        raw_tokens = value.get("episode_tokens")
        if (
            tmdb_id is None
            or season is None
            or episode_count is None
            or not isinstance(raw_tokens, Sequence)
            or isinstance(raw_tokens, (str, bytes, bytearray))
        ):
            return None
        tokens = tuple(str(token) for token in raw_tokens)
        boundaries: tuple[tuple[int, int], ...] = ()
        raw_boundaries = value.get("season_boundaries")
        if raw_boundaries is not None:
            if (
                not isinstance(raw_boundaries, Sequence)
                or isinstance(raw_boundaries, (str, bytes, bytearray))
            ):
                return None
            parsed: list[tuple[int, int]] = []
            for pair in raw_boundaries:
                if (
                    not isinstance(pair, Sequence)
                    or isinstance(pair, (str, bytes, bytearray))
                    or len(pair) != 2
                ):
                    return None
                season_num = _positive_season(pair[0])
                count = _positive_season(pair[1])
                if season_num is None or count is None:
                    return None
                parsed.append((season_num, count))
            if sum(count for _s, count in parsed) != episode_count:
                return None
            boundaries = tuple(parsed)
        if boundaries:
            expected = tuple(
                f"S{season_number:02d}E{episode:02d}"
                for season_number, count in boundaries
                for episode in range(1, count + 1)
            )
        elif evidence_kind == _PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND:
            # A named-arc Season 00 run proves the release ordinals map onto
            # an official window that may start anywhere (``S00E08``/``E09``),
            # so only the token grammar and count are checked here.  The
            # persisted receipt is re-proved against the live catalog before
            # F uses it, which is what actually pins the window.
            expected = None
        else:
            expected = tuple(
                f"S{season:02d}E{episode:02d}"
                for episode in range(1, episode_count + 1)
            )
        if expected is not None and tokens != expected:
            return None
        if expected is None:
            prefix = f"S{season:02d}E"
            ordinals: set[int] = set()
            for token in tokens:
                suffix = token[len(prefix):]
                if not token.startswith(prefix) or not suffix.isdigit():
                    return None
                ordinal = int(suffix)
                if ordinal <= 0 or ordinal in ordinals:
                    return None
                ordinals.add(ordinal)
            if len(ordinals) != episode_count:
                return None
        return cls(
            tmdb_id,
            season,
            episode_count,
            tokens,
            evidence_kind=str(evidence_kind),
            season_boundaries=boundaries,
        )


# Kept as a source-compatible name for persisted bare-E evidence and callers
# added before bracketed single-season proof existed.  New code should refer
# to the grammar-neutral type above.
BareEpisodeSeasonProof = SingleSeasonEpisodeProof


@dataclass(frozen=True)
class _TmdbSingleRegularSeasonEvidence:
    """TMDB proof for one regular season and optional auxiliary specials.

    Season 00 is not a source season inference.  It is retained only as an
    auxiliary shape check so a published specials bucket does not make an
    otherwise unique regular season look ambiguous.  A same-sized Season 00
    remains ambiguous and is rejected by the reader below.
    """

    season: int
    regular_episode_count: int
    specials_episode_count: int | None = None
    # Episodes beyond ``regular_episode_count`` in the source run (``1..N``
    # where N > the declared season count) that land in Season 00.
    overflow_episode_count: int = 0


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


def _normalise_nfo_identity(value: object) -> dict[str, object] | None:
    """Return the narrow identity projection accepted by the index."""
    if not isinstance(value, Mapping):
        return None
    raw_id = value.get("tmdb_id")
    if isinstance(raw_id, bool):
        return None
    try:
        tmdb_id = int(raw_id)
    except (TypeError, ValueError):
        return None
    media_type = str(value.get("media_type") or "")
    if tmdb_id <= 0 or media_type not in {"movie", "tv"}:
        return None
    return {
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "title": str(value.get("title") or ""),
        "year": str(value.get("year") or ""),
    }


def _direct_directory_identity(
    alist: object,
    directory: str,
    items: Sequence[Mapping[str, object]],
) -> dict[str, object] | None:
    """Read an unambiguous work identity from NFOs directly in one directory.

    Every NFO is parsed by XML root rather than filename, so a titled movie
    NFO is recognised while episode-level NFOs remain outside the work index.
    Multiple *different* valid identities in the same directory are not
    arbitrarily selected.
    """
    identities: dict[tuple[str, int], dict[str, object]] = {}
    for item in items:
        if item.get("is_dir") is True:
            continue
        name = _safe_child_name(item.get("name"))
        if name is None or not name.casefold().endswith(".nfo"):
            continue
        nfo_path = posixpath.join(directory, name)
        # The normal writer emits one episode NFO beside every TV video.  It
        # cannot be a work root, and reading thousands of such sidecars would
        # turn the selected-root D step into a library-wide XML audit.  Keep
        # standard root NFO names eligible; for titled sidecars, an explicit
        # episode coordinate in the path is enough to exclude them.
        if (
            name.casefold() not in {"tvshow.nfo", "movie.nfo"}
            and audit_episode_tokens(nfo_path)
        ):
            continue
        parsed = _normalise_nfo_identity(
            read_nfo_identity(alist, nfo_path)
        )
        if parsed is None:
            continue
        key = (str(parsed["media_type"]), int(parsed["tmdb_id"]))
        identities.setdefault(key, parsed)
    return next(iter(identities.values())) if len(identities) == 1 else None


def _nearest_identity_root(
    directory: str,
    *,
    top_level_root: str,
    identities: Mapping[str, Mapping[str, object]],
) -> str | None:
    """Find the closest NFO-confirmed work root owning a nested file."""
    current = directory.rstrip("/") or "/"
    stop = top_level_root.rstrip("/") or "/"
    while True:
        if current in identities:
            return current
        if current == stop:
            return None
        parent = posixpath.dirname(current) or "/"
        if parent == current:
            return None
        current = parent


def build_library_index(alist: object, media_root: str) -> LibraryIndex:
    """Walk the three formal shelves and index every NFO-confirmed work root.

    A top-level shelf directory can be a user-facing series container.  Its
    own NFO remains one indexed work, while a nested directory carrying a
    different readable work NFO becomes a separate indexed work.  Episode
    coverage is attributed to the closest such NFO directory, never copied
    into the parent container.  This keeps nested films/spinoffs discoverable
    without letting their identity or media coverage overwrite the container.
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
            top_level_root = posixpath.join(root, name)
            directory_items: dict[str, list[Mapping[str, object]]] = {}
            stack = [top_level_root]
            directory_count = 0
            while stack:
                current = stack.pop()
                directory_count += 1
                if directory_count > MAX_WORK_DIRECTORIES:
                    raise ValueError(f"作品目录深度/数量超过索引上限: {top_level_root}")
                # Only the three shelf roots refresh; nested listings use the
                # provider cache.  The writer already performs a fresh exact
                # readback for content it writes.
                try:
                    raw_items = listing(current)
                except TypeError:
                    raw_items = listing(current)
                if not isinstance(raw_items, list):
                    continue
                items: list[Mapping[str, object]] = []
                for item in raw_items:
                    if not isinstance(item, Mapping):
                        continue
                    child_name = _safe_child_name(item.get("name"))
                    if child_name is None:
                        continue
                    items.append(item)
                    child_path = posixpath.join(current, child_name)
                    if item.get("is_dir") is True:
                        stack.append(child_path)
                        continue
                    file_count += 1
                    if file_count > MAX_INDEX_FILES:
                        raise ValueError(
                            f"正式库索引文件数超过安全上限 {MAX_INDEX_FILES}"
                        )
                directory_items[current] = items

            identities = {
                directory: identity
                for directory, items in directory_items.items()
                if (identity := _direct_directory_identity(alist, directory, items))
                is not None
            }
            if not identities:
                continue
            tokens_by_root: dict[str, set[str]] = {
                directory: set() for directory in identities
            }
            for directory, items in directory_items.items():
                owner = _nearest_identity_root(
                    directory,
                    top_level_root=top_level_root,
                    identities=identities,
                )
                if owner is None:
                    continue
                tokens = tokens_by_root[owner]
                for item in items:
                    if item.get("is_dir") is True:
                        continue
                    child_name = _safe_child_name(item.get("name"))
                    if child_name is None or not is_video_filename(child_name):
                        continue
                    child_path = posixpath.join(directory, child_name)
                    for season, episode in audit_episode_tokens(child_path):
                        tokens.add(f"S{season:02d}E{episode:02d}")
            for work_root, identity in identities.items():
                works.append(IndexedWork(
                    media_type=str(identity["media_type"]),
                    tmdb_id=int(identity["tmdb_id"]),
                    shelf=shelf,
                    work_root=work_root,
                    title=str(identity.get("title") or ""),
                    year=str(identity.get("year") or ""),
                    episode_tokens=frozenset(tokens_by_root[work_root]),
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


def _resumable_consumed_source_decision(
    alist: object,
    snapshot: Mapping[str, object],
    state_root: object,
    root_task_id: str,
    record: WorkUnitRecord,
    index: LibraryIndex,
    *,
    media_type: str,
    tmdb_id: int,
    label: str,
) -> ReconciliationDecision | None:
    """Recognize a source this root's own interrupted write consumed.

    A write may move every planned media object and then fail during the
    artifact (NFO/poster) phase, before its internal carrier was persisted.
    On retry the fresh source no longer matches the B snapshot, so no
    episode grammar can be re-proven and the ordinary proof path would park
    the unit forever.  Continuation back into ``new_work`` is warranted
    exactly when:

    - the unit's last acceptance record FAILED (a never-started unit has
      nothing to continue; a completed unit never re-enters this path),
    - the fresh source really drifted (something was consumed), and
    - the formal library now holds an entry for this exact identity —
      the interrupted write did reach the library.

    Safety does not depend on this verdict alone: F hands the executor the
    consumed snapshot objects only, and the executor's no-overwrite matrix
    turns each into an exact ``already_present`` byte readback or fails
    hard.  A continuation can therefore never create a wrong library
    object; it only completes readback and regenerates artifacts.
    """
    from .unit_execution import load_work_acceptance

    acceptance = {
        row.work_unit_id: row
        for row in load_work_acceptance(state_root, root_task_id)
    }
    previous = acceptance.get(record.work_unit_id)
    if previous is None or previous.outcome != "failed":
        return None
    if _fresh_scopes_match_snapshot(alist, snapshot, record):
        # The source is intact: ordinary re-evaluation applies.
        return None
    if not index.entries_for(media_type, tmdb_id):
        return None
    return ReconciliationDecision(
        "new_work", None, None,
        (
            f"来源已被本任务的中断写入消耗（{label}无法从空源重新证明）；"
            "正式库已持有该身份，续接补完剩余对象",
        ),
    )


def _positive_season(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    return None


def _default_season_for_record(record: WorkUnitRecord) -> int | None:
    """Return only an evidence-backed default for an unqualified label.

    A manual C/U confirmation may explicitly select a season.  Otherwise a
    B/W cohort can provide a default only when it proved exactly one positive
    season.  A multi-season cohort never borrows its first season for an
    unqualified filename.
    """
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    override = _positive_season(identity.get("season"))
    if override is not None:
        return override
    claimed = tuple(
        season
        for season in record.claimed_seasons
        if _positive_season(season) is not None
    )
    unique = tuple(sorted(set(claimed)))
    return unique[0] if len(unique) == 1 else None


def _unit_episode_tokens(
    node: SourceNode | None,
    *,
    default_season: int | None = None,
) -> frozenset[str]:
    """Return source coordinates without inventing a season for bare files."""
    if node is None:
        return frozenset()
    tokens: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        for file in current.files:
            if file.object_type != "video":
                continue
            # The full path carries a real ``Season 02`` directory context
            # when one exists.  Passing only ``file.name`` loses that proof.
            for season, episode in audit_episode_tokens(
                file.path,
                default_season=default_season,
            ):
                tokens.add(f"S{season:02d}E{episode:02d}")
        stack.extend(current.children)
    return frozenset(tokens)


def _has_video(node: SourceNode) -> bool:
    return any(file.object_type == "video" for file in collect_all_files(node))


_SPECIAL_MARKER_RE = re.compile(r"OVBSP|OVA|OAV|OAD")


def _season_scoped_special_run_tokens(
    scoped_node: SourceNode,
    *,
    tmdb_client: object | None,
    tmdb_id: int,
    season: int | None,
) -> frozenset[str]:
    """Derive S00 coordinates for a season-scoped same-marker OVA run.

    A physical release can ship one season's bonus videos as a same-marker
    run (``[12(OVA)]``/``[13(OVA)]``) whose filenames carry no episode
    grammar.  The season identity plus the official timeline still yields
    unambiguous coordinates: every video carries a special marker and one
    distinct release ordinal, and the season's official S00 window holds
    exactly as many slots as the run has videos.  The bijection fixes the
    token set without pairing individual files — F pairs them by release
    order when writing.  Anything looser stays fail-closed (empty set), so
    the unit keeps its manual-confirmation surface.
    """
    if tmdb_client is None or season is None or season <= 0:
        return frozenset()
    getter = getattr(tmdb_client, "get", None)
    if not callable(getter):
        return frozenset()
    videos = [
        file
        for file in collect_all_files(scoped_node)
        if file.object_type == "video"
    ]
    if not videos:
        return frozenset()
    ordinals: list[int] = []
    for file in videos:
        normalized = unicodedata.normalize("NFKC", file.name).upper()
        if not _SPECIAL_MARKER_RE.search(normalized):
            # A non-special video in scope breaks the run bijection.
            return frozenset()
        ordinal = _pending_special_release_ordinal({"name": file.name})
        if ordinal is None:
            return frozenset()
        ordinals.append(ordinal)
    if len(set(ordinals)) != len(ordinals):
        return frozenset()
    try:
        show = getter(f"/tv/{tmdb_id}")
        season_data = getter(f"/tv/{tmdb_id}/season/{season}")
        specials_data = getter(f"/tv/{tmdb_id}/season/0")
    except Exception:
        return frozenset()
    if not isinstance(show, Mapping) or not isinstance(season_data, Mapping):
        return frozenset()
    if not isinstance(specials_data, Mapping):
        specials_data = {"episodes": []}
    window = special_season_window_candidates(
        show, season_data, specials_data, season
    )
    if len(window) != len(videos):
        return frozenset()
    return frozenset(f"S00E{key.number:02d}" for key in window)


def _scope_season_number(path: str) -> int | None:
    """Use the same bounded season-directory parser as B/W."""
    return _season_number_from_directory_name(posixpath.basename(path))


def _subtitle_coordinates_for_season(node: SourceNode, season: int) -> frozenset[int] | None:
    """Return exact subtitle coordinates for one declared no-video season.

    A rooted WorkUnit may own one source root rather than one path per season.
    An empty child directory is not enough to manufacture a missing TMDB
    season: its subtitle members must still carry one unambiguous, contiguous
    ``SxxEyy`` sequence matching the child directory's explicit season marker.
    This repeats the narrow B/W fact at D rather than trusting a loose folder
    name or a stale persisted claim.
    """
    episodes: set[int] = set()
    for file in collect_all_files(node):
        if file.object_type != "subtitle":
            continue
        match = _SEASON_EPISODE_RE.search(file.name)
        if match is None:
            return None
        file_season, episode = int(match.group(1)), int(match.group(2))
        if file_season != season or episode <= 0 or episode in episodes:
            return None
        episodes.add(episode)
    if not episodes or episodes != set(range(1, max(episodes) + 1)):
        return None
    return frozenset(episodes)


def _root_direct_video_seasons(node: SourceNode) -> frozenset[int]:
    """Return only explicit seasons carried by videos directly at one root."""
    seasons: set[int] = set()
    for file in node.files:
        if file.object_type != "video":
            continue
        match = _SEASON_EPISODE_RE.search(file.name)
        if match is not None:
            season = int(match.group(1))
            if season > 0:
                seasons.add(season)
    return frozenset(seasons)


def _video_coordinates_match_declared_season(node: SourceNode, season: int) -> bool:
    """Require every video in a marked directory to corroborate its season."""
    observed: set[int] = set()
    for file in collect_all_files(node):
        if file.object_type != "video":
            continue
        match = _SEASON_EPISODE_RE.search(file.name)
        if match is not None:
            observed.add(int(match.group(1)))
    return observed == {season}


def _scope_videos_corroborate_season(node: SourceNode, season: int) -> bool:
    """Require a scope's videos to corroborate one declared season.

    Unlike the child-directory rule above, this reads coordinates through
    the shared coverage parser, so a release grammar that splits the season
    token from the bracketed episode ordinal (``S2 [01]``) still carries its
    explicit season.  Only files that state a season count; tokenless videos
    neither corroborate nor contradict.
    """
    observed: set[int] = set()
    for file in collect_all_files(node):
        if file.object_type != "video":
            continue
        for file_season, _episode in audit_episode_tokens(file.path):
            observed.add(file_season)
    return observed == {season}


def _declared_empty_seasons_in_root_scope(
    root: SourceNode,
    claimed: tuple[int, ...],
) -> tuple[int, ...] | None:
    """Revalidate B/W claims when one WorkUnit owns its whole source root.

    A single-root TV can legitimately contain several decorated season
    directories plus direct files for a later season.  ``source_paths`` is
    intentionally just that owned root, so pairing it positionally with every
    claimed season would reject an otherwise proven boundary.  Rebuild the
    minimal directory-to-season proof from the persisted B snapshot instead:
    every claimed season must be represented by exactly one direct, explicitly
    marked child or by direct qualified video; a no-video child additionally
    needs its contiguous matching subtitle sequence.
    """
    by_season: dict[int, list[SourceNode]] = {}
    for child in root.children:
        season = _scope_season_number(child.path)
        if season is not None:
            by_season.setdefault(season, []).append(child)
    direct_video_seasons = _root_direct_video_seasons(root)
    empty: list[int] = []
    for season in claimed:
        children = by_season.get(season, [])
        if len(children) > 1:
            return None
        if len(children) == 1:
            if season in direct_video_seasons:
                # A rooted source cannot prove two competing physical layouts
                # for one season.  B/W may only claim an unambiguous owner.
                return None
            child = children[0]
            if _has_video(child):
                # A named season directory with video must itself corroborate
                # the same season.  Do not let a mismarked release subtree
                # validate an unrelated persisted claim.
                if not _video_coordinates_match_declared_season(child, season):
                    return None
                continue
            if _subtitle_coordinates_for_season(child, season) is None:
                return None
            empty.append(season)
            continue
        if season not in direct_video_seasons:
            return None
    return tuple(empty)


def _declared_empty_seasons(
    record: WorkUnitRecord,
    nodes_by_path: Mapping[str, SourceNode],
) -> tuple[int, ...] | None:
    """Return proved empty claimed seasons, or ``None`` when linkage is weak.

    ``claimed_seasons`` are a B/W boundary fact.  They can drive a catalog
    request only if each is still linked one-to-one to a snapshot subtree
    carrying the same explicit directory season marker.  This avoids treating
    arbitrary empty folders as missing episodes.
    """
    claimed = tuple(record.claimed_seasons)
    if not claimed:
        return ()
    if (
        any(_positive_season(season) is None for season in claimed)
        or tuple(sorted(set(claimed))) != claimed
    ):
        return None
    # A multi-scope season cohort persists one exact source directory per
    # claimed season.  A proven mixed boundary may additionally own a generic
    # SP/Extras scope; it is not a season assertion and must not make the
    # season proof positional.  Keep the season-directory linkage strict,
    # including a truly empty final season such as an unpopulated Season 11.
    if len(record.source_paths) == 1:
        root_path = record.source_paths[0].rstrip("/")
        if record.boundary_key.rstrip("/") != root_path:
            return None
        root = nodes_by_path.get(root_path)
        if root is None:
            return None
        # A unit whose whole scope is one explicitly marked season
        # directory is season-scoped, not root-scoped: its own directory
        # marker is the directory-to-season linkage.  The videos must still
        # corroborate that season through the shared coverage parser, which
        # also reads release grammars that split the token from the ordinal
        # (``S2 [01]``).  Nothing claimed can be empty in that shape.
        scope_season = _scope_season_number(root_path)
        if scope_season is not None:
            if claimed != (scope_season,) or not _scope_videos_corroborate_season(
                root, scope_season
            ):
                return None
            return ()
        return _declared_empty_seasons_in_root_scope(root, claimed)

    season_scopes: dict[int, str] = {}
    for source_path in record.source_paths:
        season = _scope_season_number(source_path)
        if season is None:
            continue
        if season in season_scopes:
            return None
        season_scopes[season] = source_path
    if tuple(sorted(season_scopes)) != claimed:
        return None
    empty: list[int] = []
    for season in claimed:
        source_path = season_scopes[season]
        scope = nodes_by_path.get(source_path.rstrip("/"))
        if scope is None or _scope_season_number(source_path) != season:
            return None
        if not _has_video(scope):
            empty.append(season)
    return tuple(empty)


def _catalog_tokens_for_seasons(
    episode_catalog: Callable[[Mapping[str, object]], object] | None,
    *,
    tmdb_id: int,
    seasons: Sequence[int],
) -> frozenset[str] | None:
    """Read exact published coordinates for declared empty seasons.

    Catalog errors, absent seasons and malformed episode rows are all evidence
    failures rather than a reason to consume a source as duplicate media.
    """
    if not seasons:
        return frozenset()
    if not callable(episode_catalog):
        return None
    try:
        payload = episode_catalog({"media_type": "tv", "tmdb_id": tmdb_id})
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    output: set[str] = set()
    for season in seasons:
        rows = payload.get(season)
        if (
            not isinstance(rows, Sequence)
            or isinstance(rows, (str, bytes, bytearray))
            or not rows
        ):
            return None
        season_tokens: set[str] = set()
        for row in rows:
            if not isinstance(row, Mapping):
                return None
            catalog_season = _positive_season(row.get("season_number"))
            episode = _positive_season(row.get("episode_number"))
            if catalog_season != season or episode is None:
                return None
            season_tokens.add(f"S{season:02d}E{episode:02d}")
        if not season_tokens:
            return None
        output.update(season_tokens)
    return frozenset(output)


_OWNED_SEASON_TOKEN_RE = re.compile(r"^S(\d{2,})E\d{2,}$")


def _owned_season_catalog_gap_tokens(
    episode_catalog: Callable[[Mapping[str, object]], object] | None,
    index: LibraryIndex,
    *,
    media_type: str,
    tmdb_id: int,
) -> frozenset[str] | None:
    """Catalog episodes missing from the library work's already-owned seasons.

    Only seasons the library already covers participate: an unowned season
    is not a gap claim this shape may invent.  A season without catalog rows
    proves nothing either way and contributes no tokens, but a catalog that
    cannot be read at all proves nothing, so the caller keeps its fail-closed
    verdict instead of silently consuming the source.
    """
    existing_tokens: set[str] = set()
    for work in index.entries_for(media_type, tmdb_id):
        existing_tokens.update(work.episode_tokens)
    owned_seasons: set[int] = set()
    for token in existing_tokens:
        match = _OWNED_SEASON_TOKEN_RE.match(str(token))
        if match is not None:
            owned_seasons.add(int(match.group(1)))
    if not owned_seasons:
        return frozenset()
    if not callable(episode_catalog):
        return None
    try:
        payload = episode_catalog({"media_type": "tv", "tmdb_id": tmdb_id})
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    output: set[str] = set()
    for season in owned_seasons:
        rows = payload.get(season)
        if (
            not isinstance(rows, Sequence)
            or isinstance(rows, (str, bytes, bytearray))
            or not rows
        ):
            continue
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            episode = _positive_season(row.get("episode_number"))
            if episode is None:
                continue
            token = f"S{season:02d}E{episode:02d}"
            if token not in existing_tokens:
                output.add(token)
    return frozenset(output)


def _residual_only_reconciliation(
    node: SourceNode | None,
    index: LibraryIndex,
    *,
    episode_catalog: Callable[[Mapping[str, object]], object] | None,
    media_type: str,
    tmdb_id: int,
    known_gap_tokens: frozenset[str],
) -> ReconciliationDecision | None:
    """Classify an extras-only confirmed TV unit whose work already exists.

    A source whose every video is proven never-written residual media
    (theme/menu/commercial/bonus-directory) has no story coordinates to
    contribute, and the planner would never write any of those files, so
    parking the unit uncertain forever adds no safety.  When the formal
    library already holds this identity the unit is instead classified by
    that identity: catalog episodes missing from the library's already-owned
    seasons register as gaps (existing_gap), otherwise the source is a pure
    duplicate (duplicate_complete).

    Fractional and unnumbered-special videos are deliberately outside this
    vocabulary — they are story media with their own mapping paths, so
    consuming them without a write would lose media.  A library without this
    identity also stays uncertain: an extras-only source can never found a
    new work root.
    """
    if node is None or media_type != "tv":
        return None
    videos = [
        file for file in collect_all_files(node) if file.object_type == "video"
    ]
    if not videos:
        return None
    if not all(_is_proven_non_story_residual_video(file) for file in videos):
        return None
    if not index.entries_for(media_type, tmdb_id):
        return None
    owned_gap_tokens = _owned_season_catalog_gap_tokens(
        episode_catalog,
        index,
        media_type=media_type,
        tmdb_id=tmdb_id,
    )
    if owned_gap_tokens is None:
        return None
    return decide_reconciliation(
        index,
        media_type=media_type,
        tmdb_id=tmdb_id,
        unit_tokens=frozenset(),
        known_gap_tokens=known_gap_tokens | owned_gap_tokens,
    )


def _scope_row_fingerprint(
    rows: Sequence[Mapping[str, object]],
) -> frozenset[tuple[str, bool, int, str]] | None:
    """Return the B/F source-object tuple, rejecting malformed duplicates."""
    output: set[tuple[str, bool, int, str]] = set()
    paths: set[str] = set()
    for row in rows:
        full_path = str(row.get("full_path") or "").rstrip("/")
        if not full_path or full_path in paths:
            return None
        paths.add(full_path)
        raw_size = row.get("size")
        try:
            size = int(raw_size or 0)
        except (TypeError, ValueError):
            return None
        if isinstance(raw_size, bool) or size < 0:
            return None
        output.add((
            full_path,
            row.get("is_dir") is True,
            size,
            str(row.get("modified") or ""),
        ))
    return frozenset(output)


def _path_below_any_scope(path: str, scopes: Sequence[str]) -> bool:
    return any(path.startswith(scope.rstrip("/") + "/") for scope in scopes)


def _fresh_scope_is_directory(alist: object, scope: str) -> bool:
    """Prove an empty source scope still exists as the exact directory."""
    return _fresh_scope_row(alist, scope, require_directory=True) is not None


def _fresh_scope_row(
    alist: object,
    scope: str,
    *,
    require_directory: bool = False,
) -> Mapping[str, object] | None:
    """Return the live provider row for one exact scope path, if present.

    Directory scopes (whole-folder boundaries) and file scopes (flat-split
    boundaries that own exact media files) are both legitimate WorkUnit
    scopes.  The caller states which shape it needs; a vanished or
    shape-shifted object returns ``None`` so the proof fails closed.
    """
    listing = getattr(alist, "list", None)
    if not callable(listing):
        return None
    parent = posixpath.dirname(scope.rstrip("/")) or "/"
    name = posixpath.basename(scope.rstrip("/"))
    try:
        rows = listing(parent, refresh=True)
    except TypeError:
        try:
            rows = listing(parent)
        except Exception:
            return None
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, Mapping) or row.get("name") != name:
            continue
        if require_directory and row.get("is_dir") is not True:
            continue
        return row
    return None


def _fresh_scopes_match_snapshot(
    alist: object,
    snapshot: Mapping[str, object],
    record: WorkUnitRecord,
) -> bool:
    """Require the exact WorkUnit scopes to equal the B snapshot right now.

    A D-derived unqualified-episode proof is only safe for the media objects
    B/W actually classified.  A source object may have appeared, vanished, or
    changed while C queried TMDB, so F must never inherit this proof without a
    fresh directory existence check and the same path/type/size/version tuple.
    A scope may be a whole directory (folder boundary) or an exact file
    (flat-split boundary); file scopes are proven from their parent listing.
    """
    scopes = tuple(path.rstrip("/") for path in record.source_paths)
    if not scopes:
        return False
    raw_snapshot_rows = snapshot.get("rows")
    if not isinstance(raw_snapshot_rows, list):
        return False
    fresh: list[Mapping[str, object]] = []
    try:
        for scope in scopes:
            directory_row = _fresh_scope_row(alist, scope, require_directory=True)
            if directory_row is not None:
                fresh.extend(walk_source_rows(alist, scope))
                continue
            # A flat-split boundary owns exact files, not a directory.  The
            # snapshot's own rows decide the expected shape: a scope whose
            # snapshot row is a file must still exist as that exact file.
            scope_row = _fresh_scope_row(alist, scope)
            if scope_row is None or scope_row.get("is_dir") is True:
                return False
            fresh.append(dict(scope_row, full_path=scope))
    except Exception:
        return False
    expected: list[Mapping[str, object]] = []
    for scope in scopes:
        scope_rows = [
            row
            for row in raw_snapshot_rows
            if isinstance(row, Mapping)
            and str(row.get("full_path") or "").rstrip("/") == scope
        ]
        if len(scope_rows) == 1 and scope_rows[0].get("is_dir") is not True:
            # A flat-split file scope: the snapshot row is the exact file the
            # fresh side proves from its parent listing.
            expected.append(scope_rows[0])
            continue
        # A directory scope (or the snapshot walk root itself, which never
        # carries its own row): the expected rows are strictly below the
        # scope, mirroring ``walk_source_rows`` children-only semantics.
        expected.extend(
            row
            for row in raw_snapshot_rows
            if isinstance(row, Mapping)
            and _path_below_any_scope(
                str(row.get("full_path") or "").rstrip("/"), (scope,)
            )
        )
    return _scope_row_fingerprint(expected) == _scope_row_fingerprint(fresh)


def _strict_bare_episode_number_for_file(file: SourceFile) -> int | None:
    """Read a naked-E coordinate without mistaking its container for media.

    A release can place ``Show.E01.mkv`` beneath an ``E01-E06`` directory.
    The directory describes the batch and must not turn each file into a
    range.  Conversely, a parent ``Season 01``/``S01E01`` or ``OVA``/``SP``
    is real hierarchy evidence and keeps the strict no-context proof closed.
    """
    if not bare_regular_episode_context_is_safe(file.path):
        return None
    # Source paths are POSIX provider paths.  Parsing the basename, rather
    # than the whole path, intentionally confines bare ranges and packed
    # episode markers to the media member itself.
    return bare_regular_episode_number(posixpath.basename(file.path.rstrip("/")))


def _movie_shaped_child_paths(node: SourceNode) -> set[str]:
    """Descendant directories that are movie-shaped (exactly one large video).

    A titled release folder holding exactly one large video is an
    independent film, not a member of the integer TV run — whether it sits
    beside the root (``剧场版 代号：白/``) or one level deeper inside the
    movie-labeled branch (``剧场版 代号：白/[某字幕组]/film.mp4``).  Its
    files must be omitted from the regular single-season proof so the film
    cannot break the ``[01]..[N]`` run of the rooted TV work.
    """
    paths: set[str] = set()
    stack = list(node.children)
    while stack:
        child = stack.pop()
        # A flat-split multi-file scope aggregates virtual one-FILE nodes as
        # children.  Every such node trivially holds "exactly one large
        # video", but it is an owned episode member, not an independent
        # film; a movie-shaped DIRECTORY owns its video strictly inside it.
        child_is_directory = child.children or (
            all(
                video.path.rstrip("/").startswith(str(child.path).rstrip("/") + "/")
                for video in collect_all_files(child)
                if video.object_type == "video"
            )
        )
        videos = [f for f in collect_all_files(child) if f.object_type == "video"]
        if (
            child_is_directory
            and len(videos) == 1
            and videos[0].size >= _MOVIE_SHAPED_MIN_BYTES
        ):
            paths.add(str(child.path).rstrip("/"))
            continue
        stack.extend(child.children)
    return paths


def _season_qualified_video(file: SourceFile) -> bool:
    """Whether a video sits under an explicit season-marked directory.

    A video inside ``01.第一季/``, ``Season 2/``, or ``S03/`` derives its
    coordinate from that directory hierarchy: it is not an *unqualified*
    release ordinal, and a season-organized multi-release container (root
    release plus per-season release folders, 间谍过家家 shape) must not run
    its season-qualified members through the strict unqualified proofs —
    their duplicate ordinals across releases would fail a proof that never
    applied to them.

    An explicit ``SxxExx`` in the *filename* deliberately does NOT qualify:
    a qualified name mixed into the same directory as unqualified files is
    the ambiguous mixed shape the proofs must keep failing closed on.
    """
    path = str(file.path or "")
    segments = [segment for segment in path.split("/") if segment]
    return any(
        _season_number_from_directory_name(segment) is not None
        for segment in segments[:-1]
    )


_BRACKET_ZERO_ORDINAL_RE = re.compile(r"\[\s*0+\s*\]")


def _bracket_zero_prologue_video(file: SourceFile) -> bool:
    """Whether a video's only coordinate is a bracketed ``[00]`` prologue.

    ``[00]`` is the special-season coordinate (S00), never a member of the
    integer ``1..N`` regular run; the shared bracket grammar deliberately
    rejects it, which would otherwise poison the whole run as an
    unparseable primary video.  Unlike an SP/OVA marker there is no special
    label to key a family on, so the exclusion is by the coordinate itself.
    """
    basename = posixpath.basename(file.path.rstrip("/"))
    if not _BRACKET_ZERO_ORDINAL_RE.search(basename):
        return False
    # A file that also carries a positive ordinal elsewhere keeps its
    # ordinary classification.
    positive = bracketed_regular_episode_number(basename)
    return positive is None or positive > 0


def _regular_episode_primary_videos(
    node: SourceNode | None,
) -> list[SourceFile] | None:
    """Primary regular-episode videos, omitting only a complete SP run.

    NCOP/NCED and fractional videos are always omitted from the integer
    regular run.  A physical-special family (OAD/OVA/OAV/SP) is omitted only
    when it itself forms one complete, unique ``1..M`` run; an incomplete or
    ambiguous special keeps the proof fail-closed (returns ``None``).  A
    movie-shaped sibling subtree is likewise omitted so an independent film
    beside the rooted TV work does not invalidate the TV proof.

    An ``SP`` marker inside an already-proven non-story context (a theme
    video, a bonus/menu/commercial directory, or a theme label) is release
    naming for that asset, not an independent physical-special ordinal
    (``[SP01] NCOP [02 [ Type-A ]]`` inside ``NCOP&ED/``).  Such files are
    excluded from the special family before the completeness requirement;
    the family check only governs SP-marked files that could otherwise be
    story episodes.
    """
    if node is None:
        return None
    videos = [
        file for file in collect_all_files(node) if file.object_type == "video"
    ]
    special = [
        file
        for file in videos
        if is_physical_special_video_file(file)
        and not (
            _is_known_non_story_theme_video(file)
            or _is_bonus_directory_video(file)
            or _is_menu_video(file)
            or _is_commercial_video(file)
        )
    ]
    if special:
        _markers, _numbers, count, complete = physical_special_marker_evidence(
            special
        )
        if not complete or count is None:
            return None
    special_paths = {file.path for file in special}
    movie_paths = _movie_shaped_child_paths(node)
    regular = [
        file
        for file in videos
        if file.path not in special_paths
        and not _is_non_regular_episode_video(file)
        and not _season_qualified_video(file)
        and not _bracket_zero_prologue_video(file)
        and not any(
            file.path == path or file.path.startswith(path + "/")
            for path in movie_paths
        )
    ]
    return regular if regular else None


def _strict_bare_episode_numbers(node: SourceNode | None) -> tuple[int, ...] | None:
    """Return exactly E01..EN when *every* source video proves one bare E."""
    videos = _regular_episode_primary_videos(node)
    if not videos:
        return None
    numbers = [_strict_bare_episode_number_for_file(file) for file in videos]
    if any(number is None for number in numbers):
        return None
    concrete = [int(number) for number in numbers if number is not None]
    if len(set(concrete)) != len(concrete):
        return None
    ordered = tuple(sorted(concrete))
    if ordered != tuple(range(1, len(concrete) + 1)):
        return None
    return ordered


def _strict_naked_numeric_episode_number(file: SourceFile) -> int | None:
    """Read an exact numeric video stem (``01.mp4``) without guessing.

    A numeric stem is weaker than ``E01`` and is therefore admitted only by
    the complete single-season proof below.  Any season/range/special marker
    in the full path closes this lane; suffixes such as ``01.1080p`` and
    duplicate/version labels do not match the exact stem grammar.
    """
    path = str(file.path or "").rstrip("/")
    if not path or not bare_regular_episode_context_is_safe(path):
        return None
    name = posixpath.basename(path)
    stem, dot, _suffix = name.rpartition(".")
    if not dot:
        return None
    match = re.fullmatch(r"0*([1-9]\d{0,2})", stem.strip())
    if match is None:
        return None
    number = int(match.group(1))
    return number if 0 < number <= 999 else None


def _strict_naked_numeric_episode_numbers(
    node: SourceNode | None,
) -> tuple[int, ...] | None:
    """Return exactly numeric ``01`` … ``N`` primary videos, or ``None``."""
    videos = _regular_episode_primary_videos(node)
    if not videos:
        return None
    numbers = [_strict_naked_numeric_episode_number(file) for file in videos]
    if any(number is None for number in numbers):
        return None
    concrete = [int(number) for number in numbers if number is not None]
    if len(set(concrete)) != len(concrete):
        return None
    ordered = tuple(sorted(concrete))
    if ordered != tuple(range(1, len(concrete) + 1)):
        return None
    return ordered


def _strict_release_dash_episode_signature_for_file(
    file: SourceFile,
) -> tuple[str, int] | None:
    """Read one release-style ``Title - 01`` member without inventing S01."""
    path = str(file.path or "").rstrip("/")
    if not path or not bare_regular_episode_context_is_safe(path):
        return None
    return release_dash_regular_episode(posixpath.basename(path))


def _strict_release_dash_episode_members(
    node: SourceNode | None,
) -> tuple[tuple[str, int], ...] | None:
    """Return exact ``(source_path, ordinal)`` members of one dash run.

    Unlike the older bare/bracket proofs, this grammar excludes no videos by
    *file shape*: the unqualified dash ordinal cannot safely distinguish a
    regular episode from an OVA, trailer, NCOP/NCED, or a second title.
    Every candidate video therefore has to carry the same normalized title
    prefix and one unique ordinal.  A video inside a dedicated bonus
    directory (``EXTRA/``, ``PV/``, ``特典映像/``) is still excluded by that
    directory context — the context is strong evidence independent of the
    ordinal grammar, exactly like the bracketed/bare proofs.
    """
    if node is None:
        return None
    videos = [
        file
        for file in collect_all_files(node)
        if file.object_type == "video"
        and not _is_bonus_directory_video(file)
    ]
    if not videos:
        return None
    signatures = [
        _strict_release_dash_episode_signature_for_file(file)
        for file in videos
    ]
    if any(signature is None for signature in signatures):
        return None
    concrete = [
        signature for signature in signatures if signature is not None
    ]
    prefixes = {prefix for prefix, _number in concrete}
    if len(prefixes) != 1:
        return None
    numbers = [number for _prefix, number in concrete]
    if len(set(numbers)) != len(numbers):
        return None
    ordered = tuple(sorted(numbers))
    if ordered != tuple(range(1, len(numbers) + 1)):
        return None
    members = tuple(sorted(
        (str(file.path).rstrip("/"), number)
        for file, (_prefix, number) in zip(videos, concrete)
    ))
    if len({path for path, _number in members}) != len(members):
        return None
    return members


def _strict_release_dash_episode_numbers(
    node: SourceNode | None,
) -> tuple[int, ...] | None:
    """Return the contiguous release-dash ordinal run, or ``None``."""
    members = _strict_release_dash_episode_members(node)
    if members is None:
        return None
    return tuple(sorted(number for _path, number in members))


def _strict_release_title_ordinal_episode_signature_for_file(
    file: SourceFile,
) -> tuple[str, int] | None:
    """Read one strict ``Title 01`` release member."""
    path = str(file.path or "").rstrip("/")
    if not path or not bare_regular_episode_context_is_safe(path):
        return None
    return release_title_ordinal_regular_episode(posixpath.basename(path))


def _strict_release_title_ordinal_episode_members(
    node: SourceNode | None,
) -> tuple[tuple[str, int], ...] | None:
    """Return one homogeneous, contiguous ``Title 01`` source run."""
    if node is None:
        return None
    videos = [
        file for file in collect_all_files(node)
        if file.object_type == "video"
    ]
    if not videos:
        return None
    signatures = [
        _strict_release_title_ordinal_episode_signature_for_file(file)
        for file in videos
    ]
    # This grammar excludes no video: a trailer, special, duplicate, or
    # second prefix therefore invalidates the complete proof.
    if any(signature is None for signature in signatures):
        return None
    concrete = [signature for signature in signatures if signature is not None]
    prefixes = {prefix for prefix, _number in concrete}
    if len(prefixes) != 1:
        return None
    numbers = [number for _prefix, number in concrete]
    if len(set(numbers)) != len(numbers):
        return None
    ordered = tuple(sorted(numbers))
    if ordered != tuple(range(1, len(ordered) + 1)):
        return None
    members = tuple(sorted(
        (str(file.path).rstrip("/"), number)
        for file, (_prefix, number) in zip(videos, concrete)
    ))
    if len({path for path, _number in members}) != len(members):
        return None
    return members


def _strict_release_title_ordinal_episode_numbers(
    node: SourceNode | None,
) -> tuple[int, ...] | None:
    members = _strict_release_title_ordinal_episode_members(node)
    if members is None:
        return None
    return tuple(sorted(number for _path, number in members))


def release_title_ordinal_episode_source_ordinals(
    node: SourceNode | None,
) -> dict[str, int] | None:
    """Expose the exact D/F source-key proof for ``Title 01`` runs."""
    members = _strict_release_title_ordinal_episode_members(node)
    return dict(members) if members is not None else None


def release_dash_episode_source_ordinals(
    node: SourceNode | None,
) -> dict[str, int] | None:
    """Expose the exact D/F release-dash source-key proof.

    F uses this only after it has re-run
    :func:`prove_single_season_episode_evidence`.  It turns the same strict
    source members into planner overrides, preventing title digits such as
    ``The 100 - 01`` from becoming the engine's source key ``E100``.
    """
    members = _strict_release_dash_episode_members(node)
    return dict(members) if members is not None else None


def _unqualified_episode_videos(node: SourceNode) -> list[SourceFile]:
    """Videos that drive the strict unqualified-episode grammars.

    Season-qualified videos (an explicit ``SxxExx`` name or a season-marked
    parent directory) already carry their coordinates; the unqualified
    grammars exist only for genuinely unqualified release ordinals.
    """
    return [
        file
        for file in collect_all_files(node)
        if file.object_type == "video"
        and not _season_qualified_video(file)
    ]


def _contains_bare_regular_episode(node: SourceNode | None) -> bool:
    if node is None:
        return False
    return any(
        _strict_bare_episode_number_for_file(file) is not None
        for file in _unqualified_episode_videos(node)
    )


def _contains_naked_numeric_episode(node: SourceNode | None) -> bool:
    if node is None:
        return False
    return any(
        _strict_naked_numeric_episode_number(file) is not None
        for file in _unqualified_episode_videos(node)
    )


def _contains_release_dash_episode(node: SourceNode | None) -> bool:
    if node is None:
        return False
    return any(
        _strict_release_dash_episode_signature_for_file(file) is not None
        for file in _unqualified_episode_videos(node)
    )


def _contains_release_title_ordinal_episode(node: SourceNode | None) -> bool:
    if node is None:
        return False
    return any(
        _strict_release_title_ordinal_episode_signature_for_file(file) is not None
        for file in _unqualified_episode_videos(node)
    )


_KNOWN_NON_STORY_THEME_MARKER_RE = re.compile(
    r"\[\s*(?:(?:NC)?(?:OP|ED)(?:\s*(?:\d+|v\d+))?)\s*\]",
    re.IGNORECASE,
)

# A ``[Menu]``/``[Menu01]`` label is a disc menu (光盘菜单), never a member of
# the integer regular run, so it is omitted from the single-season proof.
_MENU_MARKER_RE = re.compile(
    r"\[\s*MENU(?:\s*\d+)?\s*\]",
    re.IGNORECASE,
)

# A ``[CM]``/``[TV-CM]`` label is a commercial/preview, never a story episode.
_COMMERCIAL_MARKER_RE = re.compile(
    r"\[\s*(?:TV-)?CM(?:\s*\d+)?\s*\]",
    re.IGNORECASE,
)

# A dedicated bonus directory is strong context that every video inside it is
# a non-story extra, exactly like an ``NCOP&ED`` directory: a release puts
# bare ordinals there (``PV/[01]``, ``特典映像/[01]``, ``menu/[Menu01]``)
# that must be omitted from the integer regular-run proof instead of
# colliding with the real episodes.  The earlier fail-closed ruling covered
# a ``[PV]`` filename *label* in a mixed directory; a directory named
# ``PV``/``特典映像``/``Bonus`` is different, stronger evidence.  The
# vocabulary itself lives in the shared engine residual policy so B/W, D,
# and F can never disagree about what a bonus directory means.
_NON_STORY_THEME_DIRECTORY_RE = BONUS_DIRECTORY_SEGMENT_RE

# One video file at or above this size in a titled sibling is treated as an
# independent film (movie-shaped) rather than a member of the TV episode run.
# Kept consistent with ``boundary_analysis._MOVIE_MIN_BYTES``.
_MOVIE_SHAPED_MIN_BYTES = 200 * 1024 * 1024

# An unnumbered physical-special marker (``[OAD]``/``[OVA]``/``[OAV]``/``[SP]``)
# is a named special, not a member of the integer ``1..N`` regular run.  It is
# omitted from the regular single-season proof exactly like NCOP/NCED are.
_UNNUMBERED_SPECIAL_MARKER_RE = re.compile(
    r"(?<![A-Za-z])(?:OVA|OAV|OAD|SP)(?![A-Za-z])",
    re.IGNORECASE,
)


def _is_known_non_story_theme_video(file: SourceFile) -> bool:
    """Whether a video is one explicitly identified non-story OP/ED asset.

    ``[OP]``/``[ED]`` (with or without ``NC`` and an optional number) are
    opening/ending theme markers, so they are omitted from the regular-episode
    proof.  An ``MV``/``PV`` label is not a reliable media role, so it keeps
    the proof fail-closed until B/W can place it independently.
    """
    basename = posixpath.basename(str(file.path or "").rstrip("/"))
    return bool(_KNOWN_NON_STORY_THEME_MARKER_RE.search(basename))


def _is_bonus_directory_video(file: SourceFile) -> bool:
    """Whether a video sits inside a recognized non-story OP/ED directory.

    A path segment such as ``NCOP&ED``/``OP&ED``/``NCED`` is strong context
    that every video inside is a bonus (opening/ending/menu), not a story
    episode, even when the file's own basename is only ``[MV]``/``[Menu]``.
    """
    return bool(_NON_STORY_THEME_DIRECTORY_RE.search(str(file.path or "")))


def _is_non_regular_episode_video(file: SourceFile) -> bool:
    """Whether a video is outside the integer regular-episode run."""
    return (
        _is_known_non_story_theme_video(file)
        or _is_fractional_episode_video(file)
        or _is_bonus_directory_video(file)
        or _is_unnumbered_special_video(file)
        or _is_menu_video(file)
        or _is_commercial_video(file)
    )


def _is_proven_non_story_residual_video(file: SourceFile) -> bool:
    """Whether a video is proven never-written residual media.

    This is the D-side subset of the non-story vocabulary whose members the
    planner never writes as story media: theme videos, disc menus,
    commercials and bonus-directory residents.  Fractional and
    unnumbered-special videos are excluded — they are story coordinates with
    their own mapping paths, so consuming them without a write would lose
    media.
    """
    return (
        _is_known_non_story_theme_video(file)
        or _is_bonus_directory_video(file)
        or _is_menu_video(file)
        or _is_commercial_video(file)
    )


def _is_fractional_episode_video(file: SourceFile) -> bool:
    """Whether a video carries a fractional episode label (``[11.5]``).

    A fractional special is a distinct source coordinate (handled by the
    special/fractional mapping), not a member of the integer ``1..N`` regular
    run.  It must therefore be omitted from the regular single-season proof
    instead of invalidating it, exactly like NCOP/NCED are.
    """
    basename = posixpath.basename(str(file.path or "").rstrip("/"))
    return bool(FRACTIONAL_EPISODE_RE.search(basename))


def _is_unnumbered_special_video(file: SourceFile) -> bool:
    """Whether a video carries an unnumbered OAD/OVA/OAV/SP marker.

    A bare ``[OAD]``/``[OVA]``/``[OAV]``/``[SP]`` is a named special (no
    ordinal), not a member of the integer regular run, so it must be omitted
    from the single-season proof like NCOP/NCED rather than invalidating it.
    """
    basename = posixpath.basename(str(file.path or "").rstrip("/"))
    return bool(_UNNUMBERED_SPECIAL_MARKER_RE.search(basename))


def _is_menu_video(file: SourceFile) -> bool:
    """Whether a video carries a disc-menu ``[Menu]``/``[MenuNN]`` label."""
    basename = posixpath.basename(str(file.path or "").rstrip("/"))
    return bool(_MENU_MARKER_RE.search(basename))


def _is_commercial_video(file: SourceFile) -> bool:
    """Whether a video carries a ``[CM]``/``[TV-CM]`` commercial label."""
    basename = posixpath.basename(str(file.path or "").rstrip("/"))
    return bool(_COMMERCIAL_MARKER_RE.search(basename))


def _strict_bracketed_episode_number_for_file(file: SourceFile) -> int | None:
    """Read one pure ``[01]`` ordinal without erasing hierarchy evidence.

    ``[00]`` is a special coordinate (S00), never a member of the integer
    ``1..N`` regular run; it is rejected here so a single prologue file
    cannot invalidate an otherwise complete bracket run.
    """
    if not bare_regular_episode_context_is_safe(file.path):
        return None
    number = bracketed_regular_episode_number(
        posixpath.basename(file.path.rstrip("/"))
    )
    if number is not None and number <= 0:
        return None
    return number


def _strict_bracketed_episode_members(
    node: SourceNode | None,
) -> tuple[tuple[str, int], ...] | None:
    """Return exact ``(source_path, ordinal)`` members of one ``[01]`` run.

    This is the member-level view of :func:`_strict_bracketed_episode_numbers`
    used by the D/F proof chain: F turns the same strict members into an
    explicit episode map, so every planner override stays tied to one
    revalidated source file instead of a bare ordinal.
    """
    videos = _regular_episode_primary_videos(node)
    if not videos:
        return None
    members: list[tuple[str, int]] = []
    for file in videos:
        number = _strict_bracketed_episode_number_for_file(file)
        if number is None:
            return None
        members.append((str(file.path).rstrip("/"), int(number)))
    numbers = [number for _path, number in members]
    if len(set(numbers)) != len(numbers):
        return None
    ordered = tuple(sorted(numbers))
    if ordered != tuple(range(1, len(numbers) + 1)):
        return None
    if len({path for path, _number in members}) != len(members):
        return None
    return tuple(sorted(members))


def _strict_bracketed_episode_numbers(
    node: SourceNode | None,
) -> tuple[int, ...] | None:
    """Return exactly ``[01]..[N]`` from every primary source video.

    NCOP/NCED/fractional and a complete OAD/OVA/OAV/SP family are omitted from
    the integer run.  A lone OVA, special, trailer, duplicate encode, or a file
    with ambiguous brackets still invalidates the entire proof rather than
    being ignored.
    """
    members = _strict_bracketed_episode_members(node)
    if members is None:
        return None
    return tuple(sorted(number for _path, number in members))


def bracketed_episode_source_ordinals(
    node: SourceNode | None,
) -> dict[str, int] | None:
    """Expose the exact D/F source-key proof for ``[01]`` runs.

    F uses this only after re-running
    :func:`prove_single_season_episode_evidence`: the strict members become
    the planner's explicit episode map, so an enclosing movie-label directory
    (``剧场版``) cannot hijack a proved bracketed episode run.
    """
    members = _strict_bracketed_episode_members(node)
    return dict(members) if members is not None else None


def _contains_bracketed_regular_episode(node: SourceNode | None) -> bool:
    if node is None:
        return False
    return any(
        not _is_non_regular_episode_video(file)
        and _strict_bracketed_episode_number_for_file(file) is not None
        for file in _unqualified_episode_videos(node)
    )


def _single_season_episode_numbers(
    node: SourceNode | None,
    *,
    evidence_kind: str,
) -> tuple[int, ...] | None:
    """Dispatch one persisted proof grammar to its strict source check."""
    if evidence_kind == _BARE_EPISODE_EVIDENCE_KIND:
        return _strict_bare_episode_numbers(node)
    if evidence_kind == _BRACKETED_EPISODE_EVIDENCE_KIND:
        return _strict_bracketed_episode_numbers(node)
    if evidence_kind == _NAKED_NUMERIC_EPISODE_EVIDENCE_KIND:
        return _strict_naked_numeric_episode_numbers(node)
    if evidence_kind == _RELEASE_DASH_EPISODE_EVIDENCE_KIND:
        return _strict_release_dash_episode_numbers(node)
    if evidence_kind == _RELEASE_TITLE_ORDINAL_EPISODE_EVIDENCE_KIND:
        return _strict_release_title_ordinal_episode_numbers(node)
    return None


def _title_ordinal_prefix_matches_record(
    prefix: str,
    record: WorkUnitRecord,
) -> bool:
    """Require automatic C evidence to agree with the filename title prefix.

    Operator overrides intentionally skip this auxiliary check: their public
    confirmation surface contains only ``media_type + tmdb_id`` and existing
    tests/operations must remain valid.  Automatic identities carry the
    boundary label and the matcher decision trace, which are sufficient for a
    normalized exact alias check without introducing another matcher.  A
    containment match would let a longer, different release title borrow a
    short TMDB title as identity evidence.
    """
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    if str(identity.get("source") or "") == "operator_override":
        return True
    prefix_key = _normalize_match_title(prefix)
    if not prefix_key:
        return False
    candidates: list[object] = [record.display_label, identity.get("title")]
    trace = identity.get("decision_trace")
    if isinstance(trace, Mapping):
        for trace_key in ("official_titles", "aliases_checked"):
            aliases = trace.get(trace_key)
            if (
                isinstance(aliases, Sequence)
                and not isinstance(aliases, (str, bytes, bytearray))
            ):
                candidates.extend(aliases)
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        text = candidate.strip()
        if not text:
            continue
        cleaned = _clean_boundary_identity_query(text)
        key = _normalize_match_title(cleaned)
        if key and prefix_key == key:
            return True
    return False


def _season_runtime_profile(
    tmdb_client: object | None,
    tmdb_id: int,
    season: int,
) -> list[int | None] | None:
    """Read one season's published per-episode runtimes, fail-closed."""
    getter = getattr(tmdb_client, "get", None)
    if not callable(getter):
        return None
    try:
        payload = getter(f"/tv/{tmdb_id}/season/{season}")
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    rows = payload.get("episodes")
    if not isinstance(rows, list) or not rows:
        return None
    runtimes: list[int | None] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return None
        value = row.get("runtime")
        if value is None:
            runtimes.append(None)
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        runtimes.append(value)
    return runtimes


def _same_sized_specials_resolved_by_runtimes(
    tmdb_client: object | None,
    *,
    tmdb_id: int,
    season: int,
    specials_count: int,
    regular_count: int,
) -> bool:
    """Resolve a same-sized specials bucket via published runtimes.

    When TMDB's specials bucket declares exactly as many episodes as the
    source run, the counts alone cannot tell a regular season from the
    specials.  The two buckets' published runtime profiles can: a uniformly
    short specials bucket (every episode under the contract's 18-minute
    episode threshold) against a uniformly full-length regular season
    (every episode at or above it) cannot be the same content (轮回七次
    shape: 12 one-minute Picture Dramas beside twelve 24-minute episodes).
    Missing, partial, or overlapping runtime data keeps the fail-closed
    verdict.
    """
    special_runtimes = _season_runtime_profile(tmdb_client, tmdb_id, 0)
    regular_runtimes = _season_runtime_profile(tmdb_client, tmdb_id, season)
    if (
        special_runtimes is None
        or regular_runtimes is None
        or len(special_runtimes) != specials_count
        or len(regular_runtimes) != regular_count
        or not special_runtimes
        or not regular_runtimes
    ):
        return False
    return all(
        value is not None and value < 18 for value in special_runtimes
    ) and all(value is not None and value >= 18 for value in regular_runtimes)


def _single_positive_tmdb_season(
    tmdb_client: object | None,
    *,
    tmdb_id: int,
    episode_count: int,
) -> _TmdbSingleRegularSeasonEvidence | None:
    """Read show detail that proves one regular season.

    TMDB keeps announced, not-yet-released seasons in the detail response
    with ``episode_count == 0``.  Those empty future placeholders do not
    create an episode coordinate and are safe to ignore here.  A published
    Season 00 is auxiliary metadata, not a coordinate for an unqualified
    source run: it is accepted only when the regular season is unique and
    the special count differs from the source count.  A same-sized specials
    bucket remains indistinguishable from the source and fails closed.
    """
    getter = getattr(tmdb_client, "get", None)
    if not callable(getter):
        return None
    try:
        show = getter(f"/tv/{tmdb_id}")
    except Exception:
        return None
    if not isinstance(show, Mapping):
        return None
    raw_seasons = show.get("seasons")
    if not isinstance(raw_seasons, list) or not raw_seasons:
        return None
    positives: list[tuple[int, int]] = []
    specials_count: int | None = None
    for row in raw_seasons:
        if not isinstance(row, Mapping):
            return None
        raw_season = row.get("season_number")
        raw_count = row.get("episode_count")
        if (
            isinstance(raw_season, bool)
            or not isinstance(raw_season, int)
            or isinstance(raw_count, bool)
            or not isinstance(raw_count, int)
            or raw_season < 0
            or raw_count < 0
        ):
            return None
        if raw_season == 0:
            # Specials have no source coordinate in this proof.  Keep their
            # count only to reject the genuinely ambiguous same-sized case.
            if raw_count > 0:
                if specials_count is not None:
                    return None
                specials_count = raw_count
            continue
        if raw_count == 0:
            # Future/announced season placeholder; the episode catalog will
            # independently confirm all currently published coordinates.
            continue
        positives.append((raw_season, raw_count))
    matching = [
        (season, count)
        for season, count in positives
        if count == episode_count
    ]
    # A complete ``1..N`` run proves one published season when exactly one
    # positive season declares that N, even on a multi-season show (a
    # container child owns only that season).  A run equal to the sum of
    # several seasons is handled by the merged-season evidence instead.
    if len(matching) == 1:
        season, declared_count = matching[0]
        overflow_count = 0
    else:
        # A ``1..N`` run whose tail (N - declared) exactly equals the published
        # specials bucket proves the regular season plus an overflow tail that
        # lands in Season 00 (日在校园 1..14 = 12 regular + 2 OVA).  Only a
        # unique such season is accepted.
        overflow = [
            (season, count)
            for season, count in positives
            if count < episode_count
            and specials_count is not None
            and episode_count - count == specials_count
        ]
        if len(overflow) != 1:
            return None
        season, declared_count = overflow[0]
        overflow_count = episode_count - declared_count
    if specials_count == episode_count and not _same_sized_specials_resolved_by_runtimes(
        tmdb_client,
        tmdb_id=tmdb_id,
        season=season,
        specials_count=specials_count,
        regular_count=declared_count,
    ):
        return None
    total_seasons = show.get("number_of_seasons")
    if total_seasons is not None and (
        isinstance(total_seasons, bool)
        or not isinstance(total_seasons, int)
        or total_seasons < 1
    ):
        return None
    total_episodes = show.get("number_of_episodes")
    if total_episodes is not None and (
        isinstance(total_episodes, bool)
        or not isinstance(total_episodes, int)
        or total_episodes < episode_count
    ):
        return None
    return _TmdbSingleRegularSeasonEvidence(
        season=season,
        regular_episode_count=declared_count,
        specials_episode_count=specials_count,
        overflow_episode_count=overflow_count,
    )


@dataclass(frozen=True)
class _TmdbMergedSeasonEvidence:
    """TMDB proof for a whole-series counter spanning multiple seasons.

    A release that keeps counting ``01..N`` across seasons (``01..24`` season
    1, ``25..48`` season 2) is one complete unqualified run whose N equals the
    sum of every positive published season.  ``boundaries`` is the ordered
    ``(season_number, episode_count)`` list that lets F split the run.
    """

    boundaries: tuple[tuple[int, int], ...]


def _merged_multi_season_evidence(
    tmdb_client: object | None,
    *,
    tmdb_id: int,
    episode_count: int,
) -> _TmdbMergedSeasonEvidence | None:
    """Prove a whole-series counter spans every published positive season."""
    getter = getattr(tmdb_client, "get", None)
    if not callable(getter):
        return None
    try:
        show = getter(f"/tv/{tmdb_id}")
    except Exception:
        return None
    if not isinstance(show, Mapping):
        return None
    raw_seasons = show.get("seasons")
    if not isinstance(raw_seasons, list) or not raw_seasons:
        return None
    positives: list[tuple[int, int]] = []
    for row in raw_seasons:
        if not isinstance(row, Mapping):
            return None
        raw_season = row.get("season_number")
        raw_count = row.get("episode_count")
        if (
            isinstance(raw_season, bool)
            or not isinstance(raw_season, int)
            or isinstance(raw_count, bool)
            or not isinstance(raw_count, int)
            or raw_season < 0
            or raw_count < 0
        ):
            return None
        if raw_season > 0 and raw_count > 0:
            positives.append((raw_season, raw_count))
    if len(positives) < 2:
        return None
    positives.sort(key=lambda pair: pair[0])
    if sum(count for _season, count in positives) != episode_count:
        return None
    return _TmdbMergedSeasonEvidence(tuple(positives))


def _partial_season_prefix_evidence(
    tmdb_client: object | None,
    *,
    tmdb_id: int,
    episode_count: int,
) -> tuple[int, int] | None:
    """Prove a contiguous ``1..N`` run is the prefix of the sole positive
    season when ``N < season episode_count``.

    A not-yet-finished release legitimately has fewer source episodes than the
    published season total.  The proof only accepts the unique positive season
    (a multi-season show stays ambiguous) and returns ``(season, total)``; the
    uncovered tail ``E(N+1)..E(total)`` is left for the ordinary J gap
    discovery against the TMDB episode catalog, never treated as complete.
    """
    getter = getattr(tmdb_client, "get", None)
    if not callable(getter):
        return None
    try:
        show = getter(f"/tv/{tmdb_id}")
    except Exception:
        return None
    if not isinstance(show, Mapping):
        return None
    raw_seasons = show.get("seasons")
    if not isinstance(raw_seasons, list) or not raw_seasons:
        return None
    positives: list[tuple[int, int]] = []
    for row in raw_seasons:
        if not isinstance(row, Mapping):
            return None
        raw_season = row.get("season_number")
        raw_count = row.get("episode_count")
        if (
            isinstance(raw_season, bool)
            or not isinstance(raw_season, int)
            or isinstance(raw_count, bool)
            or not isinstance(raw_count, int)
            or raw_season < 0
            or raw_count < 0
        ):
            return None
        if raw_season > 0 and raw_count > 0:
            positives.append((raw_season, raw_count))
    if len(positives) != 1:
        return None
    season, total = positives[0]
    if total <= episode_count:
        return None
    return (season, total)


def prove_single_season_episode_evidence(
    alist: object,
    state_root: Any,
    root_task_id: str,
    record: WorkUnitRecord,
    *,
    evidence_kind: str,
    episode_catalog: Callable[[Mapping[str, object]], object] | None,
    tmdb_client: object | None,
) -> SingleSeasonEpisodeProof | None:
    """Prove one strict unqualified TV run is a complete TMDB season.

    ``evidence_kind`` selects a deliberately finite parsing grammar (bare E,
    pure brackets, exact naked numeric stems, or a homogeneous release-dash
    title prefix).  Every allowed grammar still requires a fresh B snapshot
    match, all required source videos to form one unique contiguous 1..N run,
    and TMDB detail/catalog to prove exactly one positive season of the same
    N.  It is reusable by D and F so a D proof cannot silently turn into an
    unverified writer default.
    """
    if evidence_kind not in _SINGLE_SEASON_EPISODE_EVIDENCE_KINDS:
        return None
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    if record.identity_status != "confirmed" or str(identity.get("media_type")) != "tv":
        return None
    raw_tmdb_id = identity.get("tmdb_id")
    if isinstance(raw_tmdb_id, bool):
        return None
    try:
        tmdb_id = int(raw_tmdb_id)
    except (TypeError, ValueError):
        return None
    if tmdb_id <= 0:
        return None
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None or not _fresh_scopes_match_snapshot(alist, snapshot, record):
        return None
    try:
        root = build_source_inventory(snapshot["rows"], snapshot["root"])
        scoped = build_scoped_source_node(
            root,
            record.source_paths,
            boundary_key=record.boundary_key,
            display_label=record.display_label,
        )
    except (KeyError, TypeError, ValueError):
        return None
    if evidence_kind == _RELEASE_TITLE_ORDINAL_EPISODE_EVIDENCE_KIND:
        members = _strict_release_title_ordinal_episode_members(scoped)
        if not members:
            return None
        first_prefix: str | None = None
        for path, _number in members:
            parsed = release_title_ordinal_regular_episode(posixpath.basename(path))
            if parsed is not None:
                first_prefix = parsed[0]
                break
        if first_prefix is None or not _title_ordinal_prefix_matches_record(
            first_prefix, record,
        ):
            return None
    episode_numbers = _single_season_episode_numbers(
        scoped,
        evidence_kind=evidence_kind,
    )
    if episode_numbers is None:
        return None
    season_evidence = _single_positive_tmdb_season(
        tmdb_client,
        tmdb_id=tmdb_id,
        episode_count=len(episode_numbers),
    )
    if season_evidence is None:
        merged = _merged_multi_season_evidence(
            tmdb_client,
            tmdb_id=tmdb_id,
            episode_count=len(episode_numbers),
        )
        if merged is None:
            # A partial-season prefix is only sound for a pure regular run.
            # Any fractional or physical-special (SP/OAD/OVA) video mixed in
            # means the source carries ambiguous coordinates, so fail closed
            # instead of reading it as a short regular season.
            if any(
                _is_fractional_episode_video(file)
                or _is_unnumbered_special_video(file)
                or is_physical_special_video_file(file)
                for file in collect_all_files(scoped)
                if file.object_type == "video"
            ):
                return None
            prefix = _partial_season_prefix_evidence(
                tmdb_client,
                tmdb_id=tmdb_id,
                episode_count=len(episode_numbers),
            )
            if prefix is None or not callable(episode_catalog):
                return None
            season, _total = prefix
            count = len(episode_numbers)
            return SingleSeasonEpisodeProof(
                tmdb_id=tmdb_id,
                season=season,
                episode_count=count,
                episode_tokens=tuple(
                    f"S{season:02d}E{episode:02d}"
                    for episode in range(1, count + 1)
                ),
                evidence_kind=evidence_kind,
            )
        if not callable(episode_catalog):
            return None
        boundaries = merged.boundaries
        merged_tokens = tuple(
            f"S{season:02d}E{episode:02d}"
            for season, count in boundaries
            for episode in range(1, count + 1)
        )
        return SingleSeasonEpisodeProof(
            tmdb_id=tmdb_id,
            season=boundaries[0][0],
            episode_count=len(episode_numbers),
            episode_tokens=merged_tokens,
            evidence_kind=evidence_kind,
            season_boundaries=boundaries,
        )
    if not callable(episode_catalog):
        return None
    season = season_evidence.season
    regular_count = season_evidence.regular_episode_count
    overflow_count = season_evidence.overflow_episode_count
    try:
        payload = episode_catalog({"media_type": "tv", "tmdb_id": tmdb_id})
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    if any(
        isinstance(key, bool) or not isinstance(key, int)
        for key in payload
    ):
        return None
    # A multi-season show's catalog legitimately carries every season; the
    # proof only needs the one target season to be present and to match the
    # complete ``1..N`` run.  Extra published seasons do not invalidate it.
    if season not in payload:
        return None
    rows = payload.get(season)
    if (
        not isinstance(rows, Sequence)
        or isinstance(rows, (str, bytes, bytearray))
        or len(rows) != regular_count
    ):
        return None
    catalog_numbers: list[int] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return None
        raw_season = row.get("season_number")
        number = _positive_season(row.get("episode_number"))
        if raw_season != season or number is None:
            return None
        catalog_numbers.append(number)
    expected_numbers = tuple(range(1, regular_count + 1))
    if tuple(sorted(catalog_numbers)) != expected_numbers or len(set(catalog_numbers)) != len(catalog_numbers):
        return None
    if 0 in payload:
        special_rows = payload.get(0)
        special_count = season_evidence.specials_episode_count
        if (
            special_count is None
            or not isinstance(special_rows, Sequence)
            or isinstance(special_rows, (str, bytes, bytearray))
            or len(special_rows) != special_count
        ):
            return None
        special_numbers: list[int] = []
        for row in special_rows:
            if not isinstance(row, Mapping):
                return None
            if row.get("season_number") != 0:
                return None
            number = _positive_season(row.get("episode_number"))
            if number is None:
                return None
            special_numbers.append(number)
        if (
            tuple(sorted(special_numbers)) != tuple(range(1, special_count + 1))
            or len(set(special_numbers)) != len(special_numbers)
        ):
            return None
    tokens = tuple(
        f"S{season:02d}E{episode:02d}"
        for episode in episode_numbers[:regular_count]
    ) + tuple(
        f"S00E{index:02d}"
        for index in range(1, overflow_count + 1)
    )
    return SingleSeasonEpisodeProof(
        tmdb_id=tmdb_id,
        season=season,
        episode_count=len(episode_numbers),
        episode_tokens=tokens,
        evidence_kind=evidence_kind,
    )


_ARC_LABEL_SPECIAL_MARKER_RE = re.compile(
    r"(?<![A-Z])(?:OVA|OAV|OAD|SP|SPECIAL)(?![A-Z])",
    re.IGNORECASE,
)


def _named_arc_season00_run(
    published: Mapping[int, str],
    *,
    published_years: Mapping[int, int],
    run_length: int,
    boundary_label: str,
    source_years: Sequence[int],
) -> tuple[int, ...] | None:
    """Return the one official Season 00 window titled as the source arc.

    A physical special released as ``银魂 爱染香篇 [01][02]`` is catalogued as
    part-titled Season 00 episodes of the parent show.  The boundary label
    minus its physical marker words must be a concrete arc name (at least four
    identity characters, so a bare franchise title never qualifies), every
    episode of the window must carry that arc name in its official title, and
    the official air years must sit inside the source release-year window.
    Exactly one such window may exist, or the best must beat the runner-up by
    the global ambiguity margin.
    """
    if run_length < 2 or not source_years:
        return None
    label_text = _ARC_LABEL_SPECIAL_MARKER_RE.sub(
        " ", str(boundary_label or "")
    )
    label_key = _special_arc_title_key(label_text)
    if len(label_key) < 4:
        return None
    episode_scores: dict[int, float] = {}
    for number, title in published.items():
        title_key = _special_arc_title_key(title)
        if not title_key:
            continue
        score = _title_similarity(label_key, title_key)
        if label_key in title_key:
            score = max(score, 0.95)
        elif len(title_key) >= 4 and title_key in label_key:
            score = max(score, 0.95)
        if score >= 0.90:
            episode_scores[number] = score
    ranked: list[tuple[float, tuple[int, ...]]] = []
    for start in sorted(episode_scores):
        window = tuple(range(start, start + run_length))
        if not all(number in episode_scores for number in window):
            continue
        years = [published_years.get(number) for number in window]
        if any(year is None for year in years):
            continue
        if any(
            min(abs(int(year) - int(source)) for source in source_years) > 1
            for year in years
            if year is not None
        ):
            continue
        # The weakest episode is the safety boundary; the mean only breaks
        # ties between otherwise fully qualifying windows.
        scores = [episode_scores[number] for number in window]
        ranked.append(
            (
                min(scores) * 0.8 + (sum(scores) / len(scores)) * 0.2,
                window,
            )
        )
    ranked.sort(key=lambda row: (-row[0], row[1]))
    if not ranked:
        return None
    if len(ranked) > 1 and ranked[0][0] - ranked[1][0] < AUTO_MATCH_MIN_MARGIN:
        return None
    return ranked[0][1]


def _published_season0_episodes(
    episode_catalog: Callable[[Mapping[str, object]], object],
    tmdb_id: int,
) -> tuple[dict[int, str], dict[int, int]] | None:
    """Load the parent show's published Season 00 from the episode catalog.

    The published catalog is the authoritative coordinate source; the show
    detail only proves the Season 00 shape exists.  An episode without a
    parsable air date simply carries no year.
    """
    try:
        payload = episode_catalog({"media_type": "tv", "tmdb_id": tmdb_id})
    except Exception:
        return None
    if not isinstance(payload, Mapping) or 0 not in payload:
        return None
    rows = payload.get(0)
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return None
    published: dict[int, str] = {}
    published_years: dict[int, int] = {}
    for row in rows:
        if not isinstance(row, Mapping) or row.get("season_number") != 0:
            return None
        number = _positive_season(row.get("episode_number"))
        if number is None:
            return None
        published[number] = str(row.get("name") or "")
        air_date = str(row.get("air_date") or "")
        air_year_match = re.match(r"(\d{4})-", air_date)
        if air_year_match:
            published_years[number] = int(air_year_match.group(1))
    if not published:
        return None
    return published, published_years


_TITLED_SPECIAL_OFFICIAL_PREFIX_RE = re.compile(
    r"^\s*(?:"
    r"(?:第\s*[0-9一二三四五六七八九十]+\s*季\s*)?(?:OVA|OAV|OAD|SP|SPECIAL)"
    r"\s*0*\d{0,3}"
    r"|第\s*[0-9一二三四五六七八九十]+\s*季"
    r")[\s：:．.・\-—－]*",
    re.IGNORECASE,
)


def _titled_single_season00_episode(
    published: Mapping[int, str],
    published_years: Mapping[int, int],
    *,
    boundary_label: str,
    parent_title: str,
    source_years: Sequence[int] = (),
) -> int | None:
    """Return the one published Season 00 episode titled as the source label.

    The boundary label minus its physical marker words and the parent show's
    own title must be a concrete arc name (at least four identity characters),
    and must match exactly one published Season 00 episode title — whose own
    ``OVA1：``-style ordinal prefixes are stripped before comparison — at the
    global high-confidence threshold, beating the runner-up by the global
    ambiguity margin.  A known official air year outside the source release
    years disqualifies the candidate.
    """
    label_text = _ARC_LABEL_SPECIAL_MARKER_RE.sub(
        " ", str(boundary_label or "")
    )
    label_key = _special_arc_title_key(label_text)
    parent_key = _special_arc_title_key(str(parent_title or ""))
    if parent_key:
        label_key = label_key.replace(parent_key, "")
    if len(label_key) < 4:
        return None
    episode_scores: dict[int, float] = {}
    for number, title in published.items():
        title_key = _special_arc_title_key(
            _TITLED_SPECIAL_OFFICIAL_PREFIX_RE.sub("", str(title or ""))
        )
        if len(title_key) < 4:
            continue
        score = _title_similarity(label_key, title_key)
        if label_key in title_key or title_key in label_key:
            score = max(score, 0.95)
        if score < 0.90:
            continue
        if source_years:
            year = published_years.get(number)
            if year is not None and min(
                abs(int(year) - int(source)) for source in source_years
            ) > 1:
                continue
        episode_scores[number] = score
    ranked = sorted(episode_scores.items(), key=lambda row: (-row[1], row[0]))
    if not ranked:
        return None
    if len(ranked) > 1 and ranked[0][1] - ranked[1][1] < AUTO_MATCH_MIN_MARGIN:
        return None
    return ranked[0][0]


def _titled_single_season00_evidence(
    episode_catalog: Callable[[Mapping[str, object]], object] | None,
    *,
    tmdb_id: int,
    boundary_label: str,
    parent_title: str,
    source_years: Sequence[int] = (),
    video_names: Sequence[str] = (),
) -> SingleSeasonEpisodeProof | None:
    """Prove a single unnumbered titled special as the parent's Season 00.

    This is the ordinal-free branch of the shared physical-special grammar:
    the confirmed identity is a regular show whose published Season 00 holds
    the special, and a concrete arc title — not a release ordinal, which does
    not exist — is the only coordinate evidence.  The boundary label is the
    primary anchor; every video stem in scope is a further anchor, because a
    release bundle whose directory name mixes sibling positions and packaging
    metadata (``03 OVA：黑色的铁碎牙（2008）内封+外挂字幕 1080P``) can leave
    the arc title readable only in the file names.  All anchors must agree on
    exactly one published Season 00 episode: two different winning episodes,
    or none, stay unproven.
    """
    if not callable(episode_catalog):
        return None
    loaded = _published_season0_episodes(episode_catalog, tmdb_id)
    if loaded is None:
        return None
    published, published_years = loaded
    labels = [str(boundary_label or "")]
    labels.extend(str(name or "") for name in video_names if name)
    matched: dict[int, None] = {}
    for label in labels:
        number = _titled_single_season00_episode(
            published,
            published_years,
            boundary_label=label,
            parent_title=parent_title,
            source_years=source_years,
        )
        if number is not None:
            matched.setdefault(number, None)
    if len(matched) != 1:
        return None
    number = next(iter(matched))
    return SingleSeasonEpisodeProof(
        tmdb_id=tmdb_id,
        season=0,
        episode_count=1,
        episode_tokens=(f"S00E{number:02d}",),
        evidence_kind=_PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND,
    )


def _season00_physical_special_evidence(
    official: Mapping[str, object],
    *,
    episode_catalog: Callable[[Mapping[str, object]], object] | None,
    tmdb_id: int,
    source_markers: tuple[str, ...],
    source_numbers: tuple[int, ...],
    boundary_label: str = "",
    source_years: tuple[int, ...] = (),
) -> SingleSeasonEpisodeProof | None:
    """Prove a complete OVA/OAD run is the parent show's official Season 00.

    This is the parent-identity branch of the shared physical-special grammar:
    the confirmed identity is a regular multi-season show whose published
    Season 00 holds its specials.  Three bounded official evidence classes may
    map the release ordinals to ``S00E01..S00EN``:

    * every source ordinal is named by the official Season 00 episode title
      of that number (``OVA2 PINTO`` for source ordinal 2);
    * the source run covers the *complete* published Season 00 and the
      official Season 00 text (season name or episode titles) carries the
      same physical marker family;
    * a named-arc run: the boundary label is a concrete arc name (not the
      parent franchise title itself), and exactly one consecutive official
      Season 00 window of the same length carries that arc name in every
      episode title (``银魂 爱染香篇 前篇``/``后篇`` for source ``[01]``/``[02]``)
      with air years inside the source release-year window.  The release-local
      ordinals then map onto that official window, which may start anywhere
      in Season 00 (``S00E08``/``S00E09``), and must beat the runner-up window
      by the global ambiguity margin.

    Anything looser stays ``None``: a partial run without per-ordinal title
    proof can never be positioned by release ordinals alone.
    """
    if not isinstance(official, Mapping) or not callable(episode_catalog):
        return None
    if official.get("special_detail_checked") is not True:
        return None
    requested_markers = {
        key
        for value in source_markers
        if (key := _physical_special_marker_key(value)) is not None
    }
    if not requested_markers or not source_numbers:
        return None
    count = len(source_numbers)
    expected_numbers = tuple(range(1, count + 1))
    if source_numbers != expected_numbers:
        return None
    official_numbers_raw = official.get("official_season0_numbers")
    official_titles_raw = official.get("official_season0_titles")
    if (
        not isinstance(official_numbers_raw, Sequence)
        or isinstance(official_numbers_raw, (str, bytes, bytearray))
        or not official_numbers_raw
        or not isinstance(official_titles_raw, Sequence)
        or isinstance(official_titles_raw, (str, bytes, bytearray))
        or len(official_titles_raw) != len(official_numbers_raw)
    ):
        return None
    official_numbers = tuple(official_numbers_raw)
    official_titles = tuple(official_titles_raw)
    # The published catalog is the authoritative coordinate source; the show
    # detail only proves the Season 00 shape exists.
    loaded = _published_season0_episodes(episode_catalog, tmdb_id)
    if loaded is None:
        return None
    published, published_years = loaded
    # Evidence class 1: each source ordinal is named by the official Season 00
    # title of that number under the shared marker grammar.
    ordinal_named = True
    for number in source_numbers:
        title = published.get(number)
        marker, ordinal = _season0_marker_and_ordinal(title)
        if marker not in requested_markers or ordinal != number:
            ordinal_named = False
            break
    # Evidence class 2: the run covers the complete published Season 00 and
    # the official Season 00 text names the physical marker family.
    count_match = False
    if sorted(published) == list(range(1, len(published) + 1)) and len(published) == count:
        count_match = bool(
            _official_physical_special_markers(official_titles) & requested_markers
        )
    if not ordinal_named and not count_match:
        # Evidence class 3: a named-arc run.  A physical special released as
        # ``银魂 爱染香篇 [01][02]`` is officially catalogued as part-titled
        # Season 00 episodes of the parent show.  Exactly one consecutive
        # official window of the source length, every episode officially
        # titled with the boundary arc name and aired in the source's release
        # year window, positions the release-local ordinals.
        named_run = _named_arc_season00_run(
            published,
            published_years=published_years,
            run_length=count,
            boundary_label=boundary_label,
            source_years=source_years,
        )
        if named_run is None:
            return None
        return SingleSeasonEpisodeProof(
            tmdb_id=tmdb_id,
            season=0,
            episode_count=count,
            episode_tokens=tuple(
                f"S00E{number:02d}" for number in named_run
            ),
            evidence_kind=_PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND,
        )
    return SingleSeasonEpisodeProof(
        tmdb_id=tmdb_id,
        season=0,
        episode_count=count,
        episode_tokens=tuple(f"S00E{number:02d}" for number in expected_numbers),
        evidence_kind=_PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND,
    )


def prove_physical_special_single_season_evidence(
    alist: object,
    state_root: Any,
    root_task_id: str,
    record: WorkUnitRecord,
    *,
    episode_catalog: Callable[[Mapping[str, object]], object] | None,
    tmdb_client: object | None,
) -> SingleSeasonEpisodeProof | None:
    """Prove a physical OAD/OVA run without guessing its target season.

    The source must be a complete single-family physical-special run.  Two
    TMDB shapes then turn the release ordinals into coordinates:

    * a separately catalogued work: the selected identity itself has exactly
      one positive season of the same size and an official matching physical
      marker;
    * the parent show itself (operator override or C): its published Season
      00 officially holds these specials, proven either by per-ordinal
      official special titles or by a complete count match with an official
      marker (``_season00_physical_special_evidence``).

    A third source shape carries no ordinals at all: exactly one unnumbered
    marker-bearing video whose boundary label is a concrete arc title
    (``夏目友人帐 和猫老师的第一次跑腿``).  Only the parent-identity Season 00
    branch can prove it, and only through that arc title matching exactly one
    published Season 00 episode (``_titled_single_season00_evidence``).

    In all cases only the proved source SP keys become coordinates for D/F;
    a release ordinal is never silently rewritten to an unrelated Season 00.
    """
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    if record.identity_status != "confirmed" or str(identity.get("media_type")) != "tv":
        return None
    raw_tmdb_id = identity.get("tmdb_id")
    if isinstance(raw_tmdb_id, bool):
        return None
    try:
        tmdb_id = int(raw_tmdb_id)
    except (TypeError, ValueError):
        return None
    if tmdb_id <= 0:
        return None
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None or not _fresh_scopes_match_snapshot(alist, snapshot, record):
        return None
    try:
        root = build_source_inventory(snapshot["rows"], snapshot["root"])
        scoped = build_scoped_source_node(
            root,
            record.source_paths,
            boundary_key=record.boundary_key,
            display_label=record.display_label,
        )
    except (KeyError, TypeError, ValueError):
        return None
    markers, numbers, count, complete = physical_special_marker_evidence(scoped)
    # The named-arc Season 00 proof is anchored to the release label and the
    # explicit release years, never to a bare ordinal guess.
    source_year_tokens = set(
        re.findall(r"(?<!\d)(?:19|20)\d{2}(?!\d)", str(record.display_label or " "))
    )
    for file in collect_all_files(scoped):
        if file.object_type == "video":
            source_year_tokens.update(
                re.findall(r"(?<!\d)(?:19|20)\d{2}(?!\d)", file.name)
            )
    source_years = tuple(sorted(int(y) for y in source_year_tokens))
    if not (complete and count is not None and markers and numbers):
        # A single unnumbered marker-bearing video (``夏目友人帐
        # 和猫老师的第一次跑腿`` around one OVA file) carries no release
        # ordinal at all, so no run grammar can position it.  Its only
        # bounded coordinate proof is the concrete arc title itself matching
        # exactly one published Season 00 episode of the parent show.
        # The same holds when the scope-level ordinal parse is fed only by an
        # ancestor directory name (``03 OVA：…`` sibling positions inside a
        # numbered bundle) and every video's own stem is ordinal-free, and
        # when several ordinal-free stems are alternate encodes of one
        # release (hardsub mp4 beside a softsub mkv): the stems share every
        # arc anchor, so they can only prove that one episode together or
        # not at all.
        if markers:
            videos = [
                file
                for file in collect_all_files(scoped)
                if file.object_type == "video"
            ]
            stem_ordinals = [
                physical_special_stem_ordinal(file.name) for file in videos
            ]
            if all(value is None for value in stem_ordinals):
                return _titled_single_season00_evidence(
                    episode_catalog,
                    tmdb_id=tmdb_id,
                    boundary_label=record.display_label,
                    parent_title=str(identity.get("title") or ""),
                    source_years=source_years,
                    video_names=tuple(
                        posixpath.splitext(file.name)[0] for file in videos
                    ),
                )
        return None
    official = physical_special_candidate_evidence(
        tmdb_client,
        tmdb_id=tmdb_id,
        source_markers=markers,
        source_episode_count=count,
    )
    if not (
        official.get("special_detail_checked") is True
        and official.get("official_special_count_match") is True
        and official.get("official_special_marker_hits")
    ):
        return _season00_physical_special_evidence(
            official,
            episode_catalog=episode_catalog,
            tmdb_id=tmdb_id,
            source_markers=markers,
            source_numbers=numbers,
            boundary_label=record.display_label,
            source_years=source_years,
        )
    season = _positive_season(official.get("official_special_season"))
    if season is None or not callable(episode_catalog):
        return None
    try:
        payload = episode_catalog({"media_type": "tv", "tmdb_id": tmdb_id})
    except Exception:
        return None
    if not isinstance(payload, Mapping) or set(payload) != {season}:
        return None
    rows = payload.get(season)
    if (
        not isinstance(rows, Sequence)
        or isinstance(rows, (str, bytes, bytearray))
        or len(rows) != count
    ):
        return None
    catalog_numbers: list[int] = []
    for row in rows:
        if not isinstance(row, Mapping) or row.get("season_number") != season:
            return None
        number = _positive_season(row.get("episode_number"))
        if number is None:
            return None
        catalog_numbers.append(number)
    expected_numbers = tuple(range(1, count + 1))
    if tuple(sorted(catalog_numbers)) != expected_numbers or len(set(catalog_numbers)) != count:
        return None
    return SingleSeasonEpisodeProof(
        tmdb_id=tmdb_id,
        season=season,
        episode_count=count,
        episode_tokens=tuple(
            f"S{season:02d}E{episode:02d}"
            for episode in expected_numbers
        ),
        evidence_kind=_PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND,
    )


def prove_bare_episode_single_season(
    alist: object,
    state_root: Any,
    root_task_id: str,
    record: WorkUnitRecord,
    *,
    episode_catalog: Callable[[Mapping[str, object]], object] | None,
    tmdb_client: object | None,
) -> SingleSeasonEpisodeProof | None:
    """Compatibility wrapper for the strict naked-``E##`` proof."""
    return prove_single_season_episode_evidence(
        alist,
        state_root,
        root_task_id,
        record,
        evidence_kind=_BARE_EPISODE_EVIDENCE_KIND,
        episode_catalog=episode_catalog,
        tmdb_client=tmdb_client,
    )


def prove_bracketed_episode_single_season(
    alist: object,
    state_root: Any,
    root_task_id: str,
    record: WorkUnitRecord,
    *,
    episode_catalog: Callable[[Mapping[str, object]], object] | None,
    tmdb_client: object | None,
) -> SingleSeasonEpisodeProof | None:
    """Prove a strict pure-bracket ``[01]`` run as one TMDB season."""
    return prove_single_season_episode_evidence(
        alist,
        state_root,
        root_task_id,
        record,
        evidence_kind=_BRACKETED_EPISODE_EVIDENCE_KIND,
        episode_catalog=episode_catalog,
        tmdb_client=tmdb_client,
    )


def reconcile_root_work_units(
    alist: object,
    media_root: str,
    state_root: Any,
    root_task_id: str,
    *,
    known_gap_tokens_by_identity: Mapping[tuple[str, int], Sequence[str]] | None = None,
    episode_catalog: Callable[[Mapping[str, object]], object] | None = None,
    tmdb_client: object | None = None,
) -> list[WorkUnitRecord]:
    """Run the D step for one root task and persist each unit's decision.

    Only ``confirmed`` units without an existing decision are re-evaluated, so
    retries are idempotent and durable overrides stay authoritative.  One
    exception: a unit whose last acceptance FAILED is re-evaluated, because a
    failed run (e.g. "target already exists") may prove the previous verdict
    was computed against a stale library view.
    """
    records = load_work_unit_records(state_root, root_task_id)
    snapshot = load_source_snapshot(state_root, root_task_id)
    if not records or snapshot is None:
        return records
    from .unit_execution import load_work_acceptance
    acceptance = {
        result.work_unit_id: result
        for result in load_work_acceptance(state_root, root_task_id)
    }
    index = build_library_index(alist, media_root)
    node = build_source_inventory(snapshot["rows"], snapshot["root"])
    nodes_by_path = {
        candidate.path.rstrip("/"): candidate
        for candidate in iter_source_nodes(node)
    }
    known = known_gap_tokens_by_identity or {}
    updated: list[WorkUnitRecord] = []
    for record in records:
        if record.requires_content_expansion:
            # Do not let a stale/forged D verdict reach an E lane that could
            # archive, hold, or merge an opaque disc image.  C/U normally
            # establishes this fact; D repeats the barrier defensively.
            updated.append(replace(
                record,
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
                attention=DISC_IMAGE_INSPECTION_REQUIRED,
            ))
            continue
        if record.reconciliation_outcome is not None:
            previous = acceptance.get(record.work_unit_id)
            if previous is not None and previous.outcome == "failed":
                # Re-evaluate only while the source still equals the B
                # snapshot.  A partial write may have legitimately consumed
                # the source media before an infrastructure failure (e.g. an
                # artifact upload timeout); in that state the fresh source
                # can no longer prove any episode grammar, and discarding a
                # durable verdict would deadlock the retry.  F's
                # already-present readback then completes the remaining
                # artifacts.  The stale-library-view case this branch was
                # built for (e.g. "target already exists") leaves the source
                # untouched, so it still re-evaluates.
                if _fresh_scopes_match_snapshot(alist, snapshot, record):
                    record = replace(
                        record,
                        reconciliation_outcome=None,
                        matched_work_root=None,
                        reconciliation_evidence=None,
                        uncovered_tokens=(),
                    )
                else:
                    updated.append(record)
                    continue
            else:
                updated.append(record)
                continue
        if record.identity_status != "confirmed":
            updated.append(record)
            continue
        identity = record.identity or {}
        season_proof: SingleSeasonEpisodeProof | None = None
        try:
            media_type = str(identity["media_type"])
            raw_tmdb_id = identity["tmdb_id"]
            if isinstance(raw_tmdb_id, bool):
                raise ValueError("TMDB ID 无效")
            tmdb_id = int(raw_tmdb_id)
            if media_type not in {"movie", "tv"} or tmdb_id <= 0:
                raise ValueError("媒体身份类型或 TMDB ID 无效")
            scoped_node = build_scoped_source_node(
                node,
                record.source_paths,
                boundary_key=record.boundary_key,
                display_label=record.display_label,
            )
            default_season = _default_season_for_record(record)
            unit_tokens = _unit_episode_tokens(
                scoped_node,
                default_season=default_season,
            )
            physical_special_proof = (
                prove_physical_special_single_season_evidence(
                    alist,
                    state_root,
                    root_task_id,
                    record,
                    episode_catalog=episode_catalog,
                    tmdb_client=tmdb_client,
                )
                if (
                    media_type == "tv"
                    and _has_video(scoped_node)
                    and default_season is None
                )
                else None
            )
            has_bare_episode = _contains_bare_regular_episode(scoped_node)
            has_bracketed_episode = _contains_bracketed_regular_episode(scoped_node)
            has_naked_numeric_episode = _contains_naked_numeric_episode(scoped_node)
            has_release_dash_episode = _contains_release_dash_episode(scoped_node)
            has_release_title_ordinal_episode = _contains_release_title_ordinal_episode(
                scoped_node
            )
            if physical_special_proof is not None:
                season_proof = physical_special_proof
                unit_tokens = frozenset(season_proof.episode_tokens)
                decision = None
            elif (
                media_type == "tv"
                and _has_video(scoped_node)
                and default_season is None
                and (
                    has_bare_episode
                    or has_bracketed_episode
                    or has_naked_numeric_episode
                    or has_release_dash_episode
                    or has_release_title_ordinal_episode
                )
            ):
                # A mixed root (bare E01 beside [02], S01E03/SP/unknown
                # video) must not let one fragment manufacture Sxx tokens for
                # the rest.  Only one complete, fresh, catalog-backed proof
                # grammar may supply the coordinates used by D.  Exact naked
                # numeric stems (01.mp4…N.mp4) and homogeneous
                # ``Title - 01`` release runs are separate grammars; neither
                # becomes evidence merely because a title was found.
                grammar_count = sum(
                    (
                        has_bare_episode,
                        has_bracketed_episode,
                        has_naked_numeric_episode,
                        has_release_dash_episode,
                        has_release_title_ordinal_episode,
                    )
                )
                if grammar_count != 1:
                    decision = ReconciliationDecision(
                        "uncertain", None, None,
                        (
                            "TV 来源混合了不同的无季号集号格式；"
                            "不能安全判定重复",
                        ),
                    )
                else:
                    if has_bare_episode:
                        evidence_kind = _BARE_EPISODE_EVIDENCE_KIND
                    elif has_bracketed_episode:
                        evidence_kind = _BRACKETED_EPISODE_EVIDENCE_KIND
                    elif has_naked_numeric_episode:
                        evidence_kind = _NAKED_NUMERIC_EPISODE_EVIDENCE_KIND
                    elif has_release_title_ordinal_episode:
                        evidence_kind = _RELEASE_TITLE_ORDINAL_EPISODE_EVIDENCE_KIND
                    else:
                        evidence_kind = _RELEASE_DASH_EPISODE_EVIDENCE_KIND
                    season_proof = prove_single_season_episode_evidence(
                        alist,
                        state_root,
                        root_task_id,
                        record,
                        evidence_kind=evidence_kind,
                        episode_catalog=episode_catalog,
                        tmdb_client=tmdb_client,
                    )
                    if season_proof is None:
                        evidence_label = single_season_episode_evidence_label(
                            evidence_kind
                        )
                        decision = (
                            _resumable_consumed_source_decision(
                                alist,
                                snapshot,
                                state_root,
                                root_task_id,
                                record,
                                index,
                                media_type=media_type,
                                tmdb_id=tmdb_id,
                                label=f"{evidence_label}证据",
                            )
                            or ReconciliationDecision(
                                "uncertain", None, None,
                                (
                                    f"TV {evidence_label}未能证明为完整唯一的 "
                                    "TMDB 正季；不能安全判定重复",
                                ),
                            )
                        )
                    else:
                        unit_tokens = frozenset(season_proof.episode_tokens)
                        decision = None
            else:
                decision = None
            if decision is not None:
                pass
            elif media_type == "tv" and _has_video(scoped_node) and not unit_tokens:
                decision = _resumable_consumed_source_decision(
                    alist,
                    snapshot,
                    state_root,
                    root_task_id,
                    record,
                    index,
                    media_type=media_type,
                    tmdb_id=tmdb_id,
                    label="季集坐标",
                )
                if decision is None:
                    decision = _residual_only_reconciliation(
                        scoped_node,
                        index,
                        episode_catalog=episode_catalog,
                        media_type=media_type,
                        tmdb_id=tmdb_id,
                        known_gap_tokens=frozenset(
                            str(token)
                            for token in known.get((media_type, tmdb_id), ())
                        ),
                    )
                if decision is None:
                    # A season-scoped same-marker OVA run carries no episode
                    # grammar, but the season identity plus the official
                    # timeline can still prove its S00 coordinates.  The
                    # verdict is computed inline (not persisted as a season
                    # proof receipt) because the coordinates describe Season
                    # 00 while the identity season stays the run's own
                    # season.
                    special_run_tokens = _season_scoped_special_run_tokens(
                        scoped_node,
                        tmdb_client=tmdb_client,
                        tmdb_id=tmdb_id,
                        season=default_season,
                    )
                    if special_run_tokens:
                        decision = decide_reconciliation(
                            index,
                            media_type=media_type,
                            tmdb_id=tmdb_id,
                            unit_tokens=special_run_tokens,
                            known_gap_tokens=frozenset(
                                str(token)
                                for token in known.get((media_type, tmdb_id), ())
                            ),
                        )
                    else:
                        decision = ReconciliationDecision(
                            "uncertain", None, None,
                            ("TV 来源视频缺少可证明的季集坐标，不能安全判定重复",),
                        )
            else:
                empty_seasons = (
                    _declared_empty_seasons(record, nodes_by_path)
                    if media_type == "tv"
                    else ()
                )
                if empty_seasons is None:
                    decision = ReconciliationDecision(
                        "uncertain", None, None,
                        ("声明季与来源目录无法一一核对，不能安全推导空季缺口",),
                    )
                else:
                    catalog_tokens = _catalog_tokens_for_seasons(
                        episode_catalog,
                        tmdb_id=tmdb_id,
                        seasons=empty_seasons,
                    )
                    if catalog_tokens is None:
                        decision = ReconciliationDecision(
                            "uncertain", None, None,
                            ("声明的空季缺少可核对的官方季集证据",),
                        )
                    else:
                        decision = decide_reconciliation(
                            index,
                            media_type=media_type,
                            tmdb_id=tmdb_id,
                            unit_tokens=unit_tokens,
                            known_gap_tokens=(
                                frozenset(
                                    str(token)
                                    for token in known.get((media_type, tmdb_id), ())
                                )
                                | catalog_tokens
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
            reconciliation_evidence=(
                season_proof.as_dict() if season_proof is not None else None
            ),
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
    "BareEpisodeSeasonProof",
    "SingleSeasonEpisodeProof",
    "IndexedWork",
    "LibraryIndex",
    "OUTCOMES",
    "ReconciliationDecision",
    "SHELF_BY_SEGMENT",
    "build_library_index",
    "bracketed_episode_source_ordinals",
    "decide_reconciliation",
    "prove_bracketed_episode_single_season",
    "prove_bare_episode_single_season",
    "prove_physical_special_single_season_evidence",
    "prove_single_season_episode_evidence",
    "reconcile_root_work_units",
    "release_dash_episode_source_ordinals",
    "release_title_ordinal_episode_source_ordinals",
    "single_season_episode_evidence_label",
]
