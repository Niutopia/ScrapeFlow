"""F/G/H composition: plan and execute confirmed new_work units.

Each ``new_work`` WorkUnit drives the existing planner through one internal
``EngineJob`` carrier and is written by the single ``SimplePlanExecutor`` with
its exact readback (the legacy G/H machinery is reused unchanged).  The result
is recorded as a typed ``WorkAcceptanceResult`` persisted per root task.

Units whose reconciliation is not ``new_work`` are reported as skipped and are
never written here: merge/duplicate/gap handling belongs to the D/E/J lanes,
not to the new-work writer.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
import json
from pathlib import Path
import posixpath
import re
import unicodedata
import uuid
from typing import Any, Callable, Mapping, Sequence

from engine.scrapeflow.boundary_analysis import (
    _SEASON_EPISODE_RE,
    _is_season_dir,
    _season_number_from_directory_name,
)
from engine.scrapeflow.media_policy import (
    DISC_IMAGE_INSPECTION_REQUIRED,
    is_disc_image_filename,
    is_video_filename,
)
from engine.scrapeflow.root_boundaries import (
    load_source_manifest,
    load_source_snapshot,
    walk_source_rows,
)
from engine.scrapeflow.source_objects import (
    SourceManifest,
    SourceObjectValidationError,
)
from engine.scrapeflow.source_inventory import (
    build_scoped_source_node,
    build_source_inventory,
    validate_source_scope,
)
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.gap_ledger import (
    discover_episode_gaps,
    load_gap_ledger,
    parse_gap_token,
)
from engine.scrapeflow.identity_matching import _clean_franchise_root_label
from engine.scrapeflow.replenishment_matching import audit_episode_tokens
from collections import defaultdict

from engine.scrapeflow.remote_paths import provider_safe_basename
from engine.scrapeflow.target_shelf import target_root_for_shelf
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    physical_special_marker_evidence,
    save_work_unit_records,
)

from .redaction import redact_error
from .library_index import (
    SingleSeasonEpisodeProof,
    _BRACKETED_EPISODE_EVIDENCE_KIND,
    _PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND,
    _QUOTED_ORDINAL_EPISODE_EVIDENCE_KIND,
    _RELEASE_DASH_EPISODE_EVIDENCE_KIND,
    _RELEASE_TITLE_ORDINAL_EPISODE_EVIDENCE_KIND,
    bracketed_episode_source_ordinals,
    prove_physical_special_single_season_evidence,
    prove_single_season_episode_evidence,
    quoted_ordinal_episode_source_ordinals,
    release_dash_episode_source_ordinals,
    release_title_ordinal_episode_source_ordinals,
    single_season_episode_evidence_label,
)
from .simple_engine_runner import (
    EngineJob,
    EnginePauseRequested,
    EngineRequest,
    SimpleEngineRunner,
    _safe_remote_path,  # noqa: PLC2701 - validates D-locked work roots
)
from .tmdb_episode_catalog import TmdbEpisodeCatalog


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _acceptance_path(state_root: Path, root_task_id: str) -> Path:
    return state_root / f"work_acceptance_{root_task_id}.json"


_TERMINAL_CARRIER_PHASES = frozenset({
    "failed", "failed_planning", "failed_write", "failed_verification",
    "failed_cleanup", "failed_archive", "failed_provider", "failed_identity",
    "cancelled", "planned",
})


def _is_owned_internal_carrier(
    carrier: EngineJob,
    root_task_id: str,
) -> bool:
    """Whether an Engine carrier belongs exclusively to this RootJob.

    A ``retry_wait`` carrier is normally a stale terminal/retry fact.  The
    one exception is an internal WorkUnit carrier left by an interrupted
    formal write: its durable plan is the only safe source of truth for exact
    readback and a no-overwrite continuation.  Never infer that ownership
    from an id or path; both markers are persisted when the unit carrier is
    created.
    """
    summary = carrier.summary if isinstance(carrier.summary, Mapping) else {}
    return (
        summary.get("internal_child") is True
        and summary.get("root_job_id") == root_task_id
    )


class GapDiscoveryAttention(RuntimeError):
    """J cannot prove expected coordinates and must park the WorkUnit."""


class GapLedgerPersistenceError(RuntimeError):
    """J could not durably record/read back its gap-ledger result."""


class ContainerMetadataAttention(RuntimeError):
    """A container root lacks safe representative metadata evidence."""


def _retire_stale_unit_carrier(runner: SimpleEngineRunner, carrier_id: str) -> None:
    """Remove a terminal unit carrier so a retry re-plans from current state.

    Only internal unit carriers (``internal_child``) in terminal/planned
    phases are ever removed; the root ledger, gap ledger and formal library
    are never touched here.
    """
    try:
        carrier = runner.get_job(carrier_id)
    except Exception:
        return
    summary = carrier.summary if isinstance(carrier.summary, Mapping) else {}
    if summary.get("internal_child") is not True:
        return
    if carrier.phase not in _TERMINAL_CARRIER_PHASES:
        return
    path = runner._job_path(carrier_id)  # noqa: SLF001 - carrier composition
    if path.exists():
        path.unlink()


def _carrier_plan_identity_matches(
    plan: object,
    identity: Mapping[str, object],
) -> bool:
    """Whether an executed carrier's durable plan still proves this record.

    A receipt rollback or an operator reconfirmation can change a record's
    confirmed identity after its carrier already executed.  The plan's own
    metadata is the only durable evidence of the identity that write used;
    comparing identities (never names, paths, or job ids) keeps the rule
    generic.  Missing evidence never proves a mismatch, so the carrier is
    preserved in that case.
    """
    if not isinstance(plan, Mapping):
        return True
    metadata = plan.get("metadata")
    if not isinstance(metadata, Mapping):
        return True
    carrier_tmdb = metadata.get("tmdb_id")
    record_tmdb = identity.get("tmdb_id")
    if (
        isinstance(carrier_tmdb, int) and not isinstance(carrier_tmdb, bool)
        and isinstance(record_tmdb, int) and not isinstance(record_tmdb, bool)
        and carrier_tmdb != record_tmdb
    ):
        return False
    mode = str(plan.get("mode") or "")
    media_type = str(identity.get("media_type") or "")
    if mode == "movie" and media_type == "tv":
        return False
    if mode == "tv" and media_type == "movie":
        return False
    return True


def _retire_superseded_unit_carrier(
    runner: SimpleEngineRunner,
    carrier_id: str,
) -> None:
    """Remove an executed unit carrier whose write the record superseded.

    ``_retire_stale_unit_carrier`` deliberately never removes an executed
    carrier because ``executed`` is the durable Engine write fact.  The one
    sanctioned exception is a carrier whose durable plan identity no longer
    matches the record's confirmed identity: a receipt rollback (or an
    operator reconfirmation) retired that write, so the ``executed`` fact
    describes an object the formal library no longer holds.  Removing the
    local JSON frees the deterministic carrier id for the corrected
    re-plan; the rollback receipt remains the audit trail.
    """
    try:
        carrier = runner.get_job(carrier_id)
    except Exception:
        return
    summary = carrier.summary if isinstance(carrier.summary, Mapping) else {}
    if summary.get("internal_child") is not True:
        return
    if carrier.phase != "executed":
        return
    path = runner._job_path(carrier_id)  # noqa: SLF001 - carrier composition
    if path.exists():
        path.unlink()


def _unit_job_id(work_unit_id: str) -> str:
    return f"unit-{work_unit_id}"


def _mark_internal_carrier(
    runner: SimpleEngineRunner,
    carrier: EngineJob,
    root_task_id: str,
) -> EngineJob:
    """Tag one unit carrier so it never surfaces as a second public task.

    ``internal_child``/``root_job_id`` are the stock internal-carrier markers
    the provider lane already uses; no new summary field is introduced.
    """
    summary = dict(carrier.summary)
    if summary.get("internal_child") is True and summary.get("root_job_id") == root_task_id:
        return carrier
    summary["internal_child"] = True
    summary["root_job_id"] = root_task_id
    marked = replace(carrier, summary=summary, updated_at=_now())
    atomic_write_json(
        runner._job_path(carrier.id),  # noqa: SLF001 - carrier composition
        marked.as_dict(),
        allow_nan=False,
    )
    return marked


@dataclass(frozen=True)
class WorkAcceptanceResult:
    """Typed post-write verification for one work unit (H step)."""

    work_unit_id: str
    outcome: str  # accepted | failed | skipped
    writer_job_id: str | None
    phase: str
    target_root: str
    planned_files: int
    error: str | None
    recorded_at: str
    # The durable plan receipt of a FAILED attempt: the exact source→target
    # →size mapping the interrupted writer was executing.  The failed
    # carrier is retired, so this receipt is the only record of what the
    # interrupted write was doing; a later retry continues from it instead
    # of inferring consumption from the mutated provider source.
    planned_receipt: tuple[Mapping[str, Any], ...] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "work_unit_id": self.work_unit_id,
            "outcome": self.outcome,
            "writer_job_id": self.writer_job_id,
            "phase": self.phase,
            "target_root": self.target_root,
            "planned_files": self.planned_files,
            "error": self.error,
            "recorded_at": self.recorded_at,
            "planned_receipt": (
                [dict(item) for item in self.planned_receipt]
                if self.planned_receipt is not None
                else None
            ),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "WorkAcceptanceResult":
        receipt_raw = raw.get("planned_receipt")
        receipt: tuple[Mapping[str, Any], ...] | None = None
        if isinstance(receipt_raw, list):
            receipt = tuple(
                dict(item)
                for item in receipt_raw
                if isinstance(item, Mapping)
            )
        return cls(
            work_unit_id=str(raw["work_unit_id"]),
            outcome=str(raw.get("outcome", "failed")),
            writer_job_id=(
                str(raw["writer_job_id"]) if raw.get("writer_job_id") else None
            ),
            phase=str(raw.get("phase", "")),
            target_root=str(raw.get("target_root", "")),
            planned_files=int(raw.get("planned_files", 0)),
            error=str(raw["error"]) if raw.get("error") else None,
            recorded_at=str(raw.get("recorded_at") or _now()),
            planned_receipt=receipt,
        )


def save_work_acceptance(
    state_root: Path,
    root_task_id: str,
    results: Sequence[WorkAcceptanceResult],
) -> None:
    atomic_write_json(
        _acceptance_path(state_root, root_task_id),
        [result.as_dict() for result in results],
        allow_nan=False,
    )


def load_work_acceptance(
    state_root: Path,
    root_task_id: str,
) -> list[WorkAcceptanceResult]:
    try:
        raw = json.loads(
            _acceptance_path(state_root, root_task_id).read_text(encoding="utf-8")
        )
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    output: list[WorkAcceptanceResult] = []
    for item in raw:
        if isinstance(item, Mapping):
            try:
                output.append(WorkAcceptanceResult.from_dict(item))
            except (KeyError, TypeError, ValueError):
                continue
    return output


_ABSOLUTE_BRACKET_RE = re.compile(r"\[0*(\d{1,4})\]", re.IGNORECASE)
_SE_TOKEN_RE = re.compile(r"S0*\d{1,3}E0*\d{1,4}", re.IGNORECASE)
_BARE_SEASON_RE = re.compile(r"^S0*(\d{1,3})$", re.IGNORECASE)
_LEADING_INDEX_RE = re.compile(r"^\d{1,3}[.、．\-]\s*")
_BRACKET_GROUP_RE = re.compile(
    r"\[[^\]]*\]|【[^】]*】|"
    r"\([^)]*(?:1080p|2160p|4k|x26[45]|hevc|avc|bdrip|web-?dl|bluray|remux)[^)]*\)",
    re.IGNORECASE,
)
_RESOLUTION_TOKEN_RE = re.compile(
    r"\b(1080p|2160p|4k|720p|480p|x26[45]|hevc|avc|bdrip|web-?dl|bluray|remux|dts|aac|flac|ma10p)\b",
    re.IGNORECASE,
)
_JUNK_DIGIT_RUN_RE = re.compile(r"\d{2,}")


def _clean_container_name(value: object) -> str | None:
    """Strip release noise from a container folder name (bounded).

    The operator's own folder name is the primary container name; only
    obvious noise (release groups, resolutions, leading indexes, filler
    symbols, digit runs) is removed.  A name that is still garbage after
    cleaning yields ``None`` so the caller falls back to TMDB evidence.
    """
    name = str(value or "").strip()
    if not name:
        return None
    name = _BRACKET_GROUP_RE.sub(" ", name)
    name = _LEADING_INDEX_RE.sub(" ", name)
    # Reuse the Engine's bounded franchise-root normalizer so RootJob
    # execution cannot leak alphabetical shelf prefixes (``W 五等分``),
    # collection/package labels (``全系列``/``S01-S03合集``), or
    # subtitle advertising into the formal library.  The earlier local-only
    # cleaner handled bracketed groups and resolutions but silently preserved
    # exactly those intake labels, which is how polluted container roots were
    # produced.
    name = _clean_franchise_root_label("/" + name.strip())
    name = _RESOLUTION_TOKEN_RE.sub(" ", name)
    name = re.sub(r"[#@！!]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip(" ._-/\\")
    if len(name) < 2 or _JUNK_DIGIT_RUN_RE.search(name):
        return None
    return name


def _record_source_scopes(
    runner: SimpleEngineRunner,
    root_job: EngineJob,
    record: WorkUnitRecord,
) -> tuple[str, ...]:
    """Return the exact non-overlapping intake subtrees owned by a unit."""
    ingress = str(runner._job_ingress_source(root_job)).rstrip("/")  # noqa: SLF001
    try:
        return validate_source_scope(ingress, record.source_paths)
    except ValueError as exc:
        raise ValueError(f"单元来源所有权无效: {exc}") from exc


def _require_fresh_source_scope_directories(
    runner: SimpleEngineRunner,
    scopes: tuple[str, ...],
) -> None:
    """Prove every claimed boundary is still the same exact remote object.

    A recursive AList listing may return ``[]`` for both an empty directory
    and a path that disappeared.  That ambiguity is especially dangerous for
    a declared empty season in a multi-directory cohort: its B snapshot is
    also empty, so a row-fingerprint comparison alone would otherwise let a
    deleted claimed scope pass into F/G.  Parent-listing proof is deliberately
    required for *every* scope, including ordinary single-source units.
    """
    for scope in scopes:
        kind = runner._remote_entry_kind(scope)  # noqa: SLF001 - exact AList proof
        if kind not in {"directory", "file"}:
            raise ValueError(
                f"来源范围已不再是可证明目录或文件 ({kind}): {scope}；"
                "请保持暂停并重建边界"
            )


def _fresh_exact_file_scope_row(
    runner: SimpleEngineRunner,
    scope: str,
) -> dict[str, Any]:
    """Read one exact file scope through its parent listing.

    ``AList.list(file)`` is not a portable file read (some drivers return an
    empty directory-like response), so the parent/name row is the authoritative
    existence and byte observation.  This keeps flat movie WorkUnits pinned to
    one object and never widens them to the containing directory.
    """
    parent, name = posixpath.split(scope.rstrip("/"))
    listing = getattr(runner.alist, "list", None)
    if not parent or not name or not callable(listing):
        raise ValueError(f"无法读取来源文件父目录: {scope}")
    try:
        rows = listing(parent, refresh=True)
    except TypeError:
        rows = listing(parent)
    except Exception as exc:
        raise ValueError(f"无法读取来源文件父目录: {scope}") from exc
    if not isinstance(rows, list):
        raise ValueError(f"来源文件父目录列表无效: {scope}")
    matches = [
        row for row in rows
        if isinstance(row, Mapping) and row.get("name") == name
    ]
    if len(matches) != 1 or matches[0].get("is_dir") is True:
        raise ValueError(f"来源文件范围已不存在或类型变化: {scope}")
    raw = dict(matches[0])
    raw["full_path"] = scope
    return raw


def _fresh_scope_rows(
    runner: SimpleEngineRunner,
    scopes: tuple[str, ...],
) -> tuple[dict[str, Any], ...]:
    """Freshly list directory scopes and exact-file scopes as one row set."""
    rows: list[dict[str, Any]] = []
    for scope in scopes:
        kind = runner._remote_entry_kind(scope)  # noqa: SLF001
        if kind == "directory":
            rows.extend(walk_source_rows(runner.alist, scope))
        elif kind == "file":
            rows.append(_fresh_exact_file_scope_row(runner, scope))
        else:
            raise ValueError(
                f"来源范围无法 fresh 证明 ({kind}): {scope}；"
                "不是可证明目录或文件"
            )
    return tuple(rows)


def _scope_kind_map(
    runner: SimpleEngineRunner,
    scopes: tuple[str, ...],
) -> dict[str, str]:
    """Return one exact provider kind for each declared scope."""
    kinds: dict[str, str] = {}
    for scope in scopes:
        kind = runner._remote_entry_kind(scope)  # noqa: SLF001
        if kind not in {"directory", "file"}:
            raise ValueError(
                f"来源范围无法 fresh 证明 ({kind}): {scope}；"
                "不是可证明目录或文件"
            )
        kinds[scope] = kind
    return kinds


def _path_in_scoped_objects(
    path: str,
    scope_kinds: Mapping[str, str],
    *,
    include_scope: bool = False,
) -> bool:
    """Test one object path against mixed directory/file scopes."""
    for scope, kind in scope_kinds.items():
        if kind == "file":
            if path == scope:
                return True
        elif path.startswith(scope + "/") or (include_scope and path == scope):
            return True
    return False


def _path_in_scope(path: str, scopes: tuple[str, ...], *, include_scope: bool = True) -> bool:
    return any(
        (path == scope if include_scope else False) or path.startswith(scope + "/")
        for scope in scopes
    )


def _scope_row_fingerprint(rows: Sequence[Mapping[str, Any]]) -> set[tuple[str, bool, int, str]]:
    """Stable source-object identity tuple: path, type, size, version."""
    fingerprint: set[tuple[str, bool, int, str]] = set()
    for row in rows:
        full_path = str(row.get("full_path") or "").rstrip("/")
        if not full_path:
            raise ValueError("来源快照包含缺少路径的条目")
        is_dir = row.get("is_dir") is True
        try:
            size = int(row.get("size") or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"来源快照条目大小无效: {full_path}") from exc
        fingerprint.add((full_path, is_dir, size, str(row.get("modified") or "")))
    return fingerprint


def _fresh_scoped_source_files(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    root_job: EngineJob,
    *,
    require_single_scope_manifest: bool = False,
) -> tuple[tuple[str, ...], tuple[Mapping[str, object], ...]]:
    """Freshly re-list an exact WorkUnit boundary and reject snapshot drift.

    The B snapshot proves the original object boundary; the manifest passed to
    F is created from a new read instead of reusing that stale listing.  A new,
    missing, resized, or moved object must trigger an explicit paused B/W
    rebuild, never be silently widened to the whole ingress root.  Ordinary
    one-directory WorkUnits only need the directory-existence check; callers
    with a proof whose parser must be pinned (release-dash) set
    ``require_single_scope_manifest`` to hand the exact fresh manifest to F.
    """
    scopes = _record_source_scopes(runner, root_job, record)
    scope_kinds = _scope_kind_map(runner, scopes)
    if len(scopes) <= 1 and not require_single_scope_manifest:
        return scopes, ()
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        raise ValueError("缺少 B/W 来源快照，无法证明多来源边界")
    expected_rows = [
        row
        for row in snapshot["rows"]
        if isinstance(row, Mapping)
        and _path_in_scoped_objects(
            str(row.get("full_path") or "").rstrip("/"),
            scope_kinds,
            include_scope=False,
        )
    ]
    fresh_rows = list(_fresh_scope_rows(runner, scopes))
    expected_fingerprint = _scope_row_fingerprint(expected_rows)
    fresh_fingerprint = _scope_row_fingerprint(fresh_rows)
    if expected_fingerprint != fresh_fingerprint:
        raise ValueError("来源范围已在 B/W 快照后变化；请保持暂停并重建边界")

    walk = getattr(runner.alist, "walk", None)
    manifest: list[Mapping[str, object]] = []
    if require_single_scope_manifest:
        # Keep the normal planner-side walker as a read-only preflight (it
        # rejects locks, unsafe entries and orphan temps), but do not let its
        # convenience filtering become the D/F handoff.  The release-dash
        # parser is enabled only for an exact source set, so hand the planner
        # the full second fresh listing instead.
        if callable(walk):
            for scope in scopes:
                if scope_kinds[scope] != "directory":
                    continue
                try:
                    walk(scope, refresh=True)
                except TypeError:
                    walk(scope)
        post_walk_rows = list(_fresh_scope_rows(runner, scopes))
        if _scope_row_fingerprint(post_walk_rows) != fresh_fingerprint:
            raise ValueError(
                "fresh 来源清单在快照核验后变化；请保持暂停并重建边界"
            )
        manifest.extend(
            dict(row)
            for row in post_walk_rows
            if row.get("is_dir") is not True
        )
    elif callable(walk):
        for scope in scopes:
            if scope_kinds[scope] == "file":
                manifest.append(_fresh_exact_file_scope_row(runner, scope))
                continue
            try:
                scoped_files = walk(scope, refresh=True)
            except TypeError:
                scoped_files = walk(scope)
            if not isinstance(scoped_files, list):
                raise ValueError("AList 多来源文件清单无效")
            if not any(is_video_filename(str(row.get("name") or "")) for row in scoped_files if isinstance(row, Mapping)):
                try:
                    scoped_files = walk(scope, refresh=True, include_bonus=True)
                except TypeError:
                    scoped_files = walk(scope)
            manifest.extend(
                dict(row)
                for row in scoped_files
                if isinstance(row, Mapping) and row.get("is_dir") is not True
            )
    else:
        # Test doubles and narrow AList adapters may expose only list(); the
        # fresh boundary walk remains correct, although it lacks the normal
        # extra-directory pruning performed by AListClient.walk.
        manifest.extend(dict(row) for row in fresh_rows if row.get("is_dir") is not True)
    seen: set[str] = set()
    validated: list[Mapping[str, object]] = []
    for raw in manifest:
        path = str(raw.get("full_path") or "").rstrip("/")
        if not _path_in_scope(path, scopes):
            raise ValueError("fresh 多来源清单包含范围外对象")
        if path in seen:
            raise ValueError("fresh 多来源清单包含重复对象")
        seen.add(path)
        validated.append(raw)
    manifest_fingerprint = _scope_row_fingerprint(validated)
    if require_single_scope_manifest:
        # Unlike the ordinary multi-scope helper, the release-dash branch may
        # not omit a member after the proof.  Equality catches both a source
        # that vanished before the manifest was formed and one introduced
        # while the planner's normal walker was doing its safety preflight.
        fresh_file_fingerprint = _scope_row_fingerprint(
            [row for row in fresh_rows if row.get("is_dir") is not True]
        )
        if manifest_fingerprint != fresh_file_fingerprint:
            raise ValueError(
                "fresh 来源清单在快照核验后变化；请保持暂停并重建边界"
            )
    # ``walk_source_rows`` above and the planner-shaped ``walk`` may be two
    # distinct remote reads.  A file introduced between them must not enter
    # a normal multi-scope F manifest merely because it sits below a valid
    # directory.  Every handed-off object therefore has to be one of the
    # exact fresh snapshot objects that already matched B/W.
    elif not manifest_fingerprint.issubset(fresh_fingerprint):
        raise ValueError(
            "fresh 来源清单在快照核验后变化；请保持暂停并重建边界"
        )
    return scopes, tuple(validated)


def _snapshot_scope_kinds(
    scopes: tuple[str, ...],
    *,
    state_root: Path,
    root_task_id: str,
) -> dict[str, str] | None:
    """Derive scope kinds from the B snapshot when the provider cannot answer.

    A consumed-source continuation may find its source directories removed
    entirely, so ``_scope_kind_map``'s fresh provider probe fails.  The B
    snapshot is the durable record of each scope's kind; a scope that exists
    there only as a file was a one-file WorkUnit.
    """
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return None
    rows = snapshot.get("rows")
    if not isinstance(rows, list):
        return None
    by_path = {
        str(row.get("full_path") or "").rstrip("/"): row
        for row in rows
        if isinstance(row, Mapping)
    }
    kinds: dict[str, str] = {}
    for scope in scopes:
        row = by_path.get(scope.rstrip("/"))
        if row is None:
            return None
        kinds[scope] = "directory" if row.get("is_dir") is True else "file"
    return kinds


def _consumed_source_snapshot_rows(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    scope_kinds: Mapping[str, str],
    fresh_rows: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, object], ...] | None:
    """Recognize a source this root's own interrupted write consumed.

    A write may move every planned media object and then fail during the
    artifact (NFO/poster) phase, before its internal carrier was ever
    persisted.  The retry then finds a partly emptied source: fresh
    manifests can prove nothing and the durable verdict may be gone.
    Continuation is safe exactly when both hold:

    - the unit's last acceptance record FAILED (a completed unit never
      re-enters F, and a never-started unit has nothing to continue), and
    - at least one snapshot media object in the unit's scope is absent
      fresh (something was actually consumed).

    The continuation manifest is exactly the consumed set: snapshot file
    rows in scope that no longer exist fresh.  The planner rebuilds the
    identical plan for them (same source names and sizes), and the
    executor's no-overwrite matrix turns each already-moved file into an
    exact ``already_present`` byte readback before regenerating artifacts.
    Objects that still exist fresh (residuals, unplanned extras) are not
    handed over, so they can never widen the plan.
    """
    acceptance = {
        row.work_unit_id: row
        for row in load_work_acceptance(state_root, root_task_id)
    }
    previous = acceptance.get(record.work_unit_id)
    if previous is None or previous.outcome != "failed":
        return None
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return None
    rows = snapshot.get("rows")
    if not isinstance(rows, list):
        return None
    # The failed attempt's persisted plan receipt is the exact consumed set:
    # every snapshot file object the interrupted writer was executing.  It
    # does not depend on what the provider source looks like now, so a
    # mid-move interruption (some objects still present) resumes precisely —
    # present objects move normally, already-moved objects read back.
    receipt_paths: set[str] | None = None
    if previous.planned_receipt is not None:
        receipt_paths = {
            str(item.get("source_path") or "").rstrip("/")
            for item in previous.planned_receipt
            if str(item.get("source_path") or "").strip()
        }
        if not receipt_paths:
            receipt_paths = None
    fresh_paths = {
        str(row.get("full_path") or "").rstrip("/")
        for row in fresh_rows
        if row.get("is_dir") is not True
    }
    consumed: list[Mapping[str, object]] = []
    for row in rows:
        if not isinstance(row, Mapping) or row.get("is_dir") is True:
            continue
        path = str(row.get("full_path") or "").rstrip("/")
        if not _path_in_scoped_objects(path, scope_kinds, include_scope=True):
            continue
        if receipt_paths is not None:
            if path in receipt_paths:
                consumed.append(dict(row))
            continue
        if path in fresh_paths:
            continue
        consumed.append(dict(row))
    if not consumed:
        # Nothing was consumed and no receipt exists: this is ordinary
        # drift, not a resumable interrupted write.
        return None
    return tuple(consumed)


def _fresh_exact_source_manifest(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    root_job: EngineJob,
) -> tuple[tuple[str, ...], tuple[Mapping[str, object], ...]]:
    """Re-prove every object in a single-scope WorkUnit before F.

    ``SourceManifest`` is stricter than a directory existence check: it
    rejects a new, removed, renamed, resized, or provider-version-changed
    object.  A source snapshot made by an older release may not carry the
    sidecar; retain the established scoped fallback in that compatibility
    case, but all newly created B/W snapshots use the exact proof.
    """
    expected = load_source_manifest(state_root, root_task_id)
    if expected is None:
        return _fresh_scoped_source_files(
            runner, state_root, root_task_id, record, root_job,
            require_single_scope_manifest=True,
        )
    scopes = _record_source_scopes(runner, root_job, record)
    scope_kinds = _scope_kind_map(runner, scopes)
    rows = list(_fresh_scope_rows(runner, scopes))
    # WorkUnit scopes partition a source tree, while the root-level manifest
    # also contains siblings owned by other units/residuals.  Filter only by
    # the declared exact boundary before comparing; an added item in a
    # sibling unit cannot spuriously invalidate this unit, but any changed
    # object inside its scope remains fatal.
    expected_objects = tuple(
        obj for obj in expected.objects
        if _path_in_scoped_objects(
            obj.path, scope_kinds, include_scope=False,
        )
    )
    fresh_rows = [
        row for row in rows
        if _path_in_scoped_objects(
            str(row.get("full_path") or "").rstrip("/"),
            scope_kinds,
            include_scope=False,
        )
    ]
    try:
        declared = SourceManifest(
            expected.snapshot_id,
            expected.root_path,
            expected_objects,
        )
        fresh = SourceManifest.from_listing_rows(
            fresh_rows,
            root_path=expected.root_path,
            snapshot_id=f"fresh:{root_task_id}:{uuid.uuid4().hex}",
        )
        declared.require_fresh_match(fresh)
    except SourceObjectValidationError as exc:
        consumed = _consumed_source_snapshot_rows(
            runner, state_root, root_task_id, record, scope_kinds, rows,
        )
        if consumed is not None:
            # This root's own interrupted write consumed the whole source
            # before its carrier persisted.  Continue from the B snapshot;
            # the executor's no-overwrite matrix re-reads every moved file
            # exactly and only regenerates the missing artifacts.
            return scopes, consumed
        raise ValueError(
            "精确来源对象清单已漂移或无效；请保持暂停并重建边界"
        ) from exc
    # Planner-facing AList clients commonly perform their own read-only walk
    # before parsing.  Re-read after that hook as well: a source created while
    # the hook ran must not enter an otherwise pinned WorkUnit manifest.
    walk = getattr(runner.alist, "walk", None)
    if callable(walk):
        try:
            for scope in scopes:
                if scope_kinds[scope] != "directory":
                    continue
                try:
                    walk(scope, refresh=True)
                except TypeError:
                    walk(scope)
            post_walk_rows = list(_fresh_scope_rows(runner, scopes))
            post_walk = SourceManifest.from_listing_rows(
                post_walk_rows,
                root_path=expected.root_path,
                snapshot_id=f"post-walk:{root_task_id}:{uuid.uuid4().hex}",
            )
            fresh.require_fresh_match(post_walk)
            fresh_rows = post_walk_rows
        except SourceObjectValidationError as exc:
            raise ValueError(
                "fresh 来源清单在快照核验后变化；请保持暂停并重建边界"
            ) from exc
    # Directories are part of the B/W provenance but never planner members.
    return scopes, tuple(
        dict(row) for row in fresh_rows if row.get("is_dir") is not True
    )


_SPECIAL_RELATION_MARKER_RE = re.compile(
    r"(?i)(?<![a-z0-9])(?:oad|ova|oav|special|specials|sp|extra|extras)"
    r"(?![a-z0-9])|特典|特别篇|特別篇|番外|花絮"
)
_SEASON_RELATION_MARKER_RE = re.compile(
    r"(?i)(?:season|series|s)\s*0*\d{1,3}|第\s*[0-9一二三四五六七八九十百零〇两]+\s*季"
)
_RELATION_NOISE_RE = re.compile(
    r"(?i)(?:19|20)\d{2}|(?:2160|1080|720|576|480)p|4k|8k|web[- ]?dl|blu[- ]?ray|"
    r"remux|x26[45]|h26[45]|hevc|av1|10bit|8bit|aac|dts|flac"
)


def _identity_relation_values(record: WorkUnitRecord) -> tuple[str, ...]:
    """Return only durable C/TMDB title evidence for family placement.

    Placement is deliberately based on the persisted formal identity result,
    not an external search result or a guessed directory name.  Older records
    may not have ``official_titles``/``aliases_checked``; their projected TMDB
    title is still valid evidence, while the boundary label remains a bounded
    last resort for synthetic/legacy records.
    """
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    trace = identity.get("decision_trace")
    values: list[str] = []
    for key in ("title", "original_title"):
        value = identity.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    if isinstance(trace, Mapping):
        for key in ("official_titles", "aliases_checked"):
            raw = trace.get(key)
            if isinstance(raw, (list, tuple)):
                values.extend(
                    str(value).strip()
                    for value in raw
                    if isinstance(value, str) and value.strip()
                )
    # A persisted operator override contains no title by contract.  Keeping
    # its boundary label allows tests/old ledgers to produce a deterministic
    # *candidate* relation, but it is never stronger than formal title data.
    if not values and record.display_label.strip():
        values.append(record.display_label.strip())
    return tuple(dict.fromkeys(values))


def _relation_title_key(value: str) -> str:
    """Normalize a formal title for conservative parent-family comparison."""
    text = unicodedata.normalize("NFKC", value).casefold()
    text = _SPECIAL_RELATION_MARKER_RE.sub(" ", text)
    text = _SEASON_RELATION_MARKER_RE.sub(" ", text)
    text = _RELATION_NOISE_RE.sub(" ", text)
    # ``isalnum`` retains CJK/Kana while discarding punctuation and release
    # separators.  Do not transliterate: cross-script aliases are already
    # present in TMDB's formal ``aliases_checked`` evidence.
    return "".join(char for char in text if char.isalnum())


def _is_physical_special_record(record: WorkUnitRecord) -> bool:
    """Whether B/C prove this unit is an auxiliary release.

    ``MV`` is intentionally *not* treated as a special marker here.  It may be
    a genuine movie/feature; only an explicit OAD/OVA/SP/extra marker or the
    structural special role can trigger family nesting.
    """
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    role = str(record.role or "").casefold()
    if role in {"special_group", "extras_group"}:
        return True
    # Two or more distinct positive claimed seasons are B/W's structural
    # proof of a regular multi-season work (verified season directories or
    # explicit SxxExx coordinates on every owned video).  Bundled OVA/SP
    # marker files inside such a cohort are extra coverage of the same work,
    # not evidence that the whole unit is an auxiliary release: the demotion
    # would hand the container's main-TV slot to an unrelated sibling and
    # plan the regular seasons under that sibling's work root.  A single
    # claimed season stays demotable because the boundary can derive it from
    # a season word in the directory name alone (``第二季 OVA``).
    if sum(
        1
        for season in record.claimed_seasons
        if isinstance(season, int) and not isinstance(season, bool) and season > 0
    ) >= 2:
        return False
    # A D-proved single positive season carries the same structural weight as
    # a claimed season: a split-season release (``某科学的超电磁炮`` S /
    # ``某科学的超电磁炮 T`` directories, each with a complete proved
    # bracketed run of one official season) is a regular season of the parent
    # work, not an auxiliary release.  Demoting every part would leave no
    # main-TV candidate and chain the seasons under each other's work roots.
    # (Read the proof directly: the proved-seasons helper gates on
    # main-TV eligibility, which itself asks this very question.)
    proof = SingleSeasonEpisodeProof.from_dict(record.reconciliation_evidence)
    if (
        proof is not None
        and proof.tmdb_id == identity.get("tmdb_id")
        and proof.season > 0
    ):
        return False
    # A boundary whose whole directory name is a structural season marker
    # (``第一季``/``Season 02``) is B/W's proof of a regular season part.
    # One bundled OVA/SP file inside such a season (``第二季/[14(OVA)]``)
    # must not demote the whole unit to an auxiliary release: that would
    # drop the split-season cohort's only main-TV candidate and re-plan the
    # regular seasons under an unrelated container root.  A decorated label
    # (``第二季 OVA``, where the marker word belongs to the release's own
    # title) still fails this full-match test and keeps the demotion.
    for scope in record.source_paths:
        if _is_season_dir(posixpath.basename(str(scope).rstrip("/"))):
            return False
    trace = identity.get("decision_trace")
    if isinstance(trace, Mapping):
        for key in ("physical_special_markers", "official_special_marker_hits"):
            raw = trace.get(key)
            if isinstance(raw, (list, tuple)) and any(
                isinstance(item, str) and item.strip() for item in raw
            ):
                return True
    primary_titles: list[str] = []
    title = identity.get("title")
    original_title = identity.get("original_title")
    if isinstance(title, str):
        primary_titles.append(title)
    if isinstance(original_title, str):
        primary_titles.append(original_title)
    if isinstance(trace, Mapping) and isinstance(trace.get("official_titles"), (list, tuple)):
        primary_titles.extend(
            value for value in trace["official_titles"] if isinstance(value, str)
        )
    for value in primary_titles:
        if _SPECIAL_RELATION_MARKER_RE.search(value):
            return True
    # Boundary paths are structural evidence only after C confirmed an
    # identity.  This catches labels such as ``Show OAD`` in older ledgers.
    return bool(
        _SPECIAL_RELATION_MARKER_RE.search(record.display_label or "")
        or any(
            _SPECIAL_RELATION_MARKER_RE.search(posixpath.basename(path.rstrip("/")))
            for path in record.source_paths
        )
    )


def _relation_score(child: WorkUnitRecord, parent: WorkUnitRecord) -> int:
    """Score one candidate parent using formal title/alias containment."""
    child_keys = {
        key for value in _identity_relation_values(child)
        if (key := _relation_title_key(value))
    }
    parent_keys = {
        key for value in _identity_relation_values(parent)
        if (key := _relation_title_key(value))
    }
    best = 0
    for child_key in child_keys:
        for parent_key in parent_keys:
            if child_key == parent_key:
                best = max(best, 100 + len(parent_key))
                continue
            shorter = min(len(child_key), len(parent_key))
            if shorter < 4:
                continue
            if child_key.startswith(parent_key) or parent_key.startswith(child_key):
                best = max(best, 50 + shorter)
    return best


def _special_parent_record(
    record: WorkUnitRecord,
    records: Sequence[WorkUnitRecord],
) -> tuple[WorkUnitRecord | None, bool]:
    """Return ``(unique_parent, ambiguous)`` for one special unit.

    A special whose confirmed TV identity equals a regular sibling's identity
    is the same work's Season 00 release: that sibling is the unique parent,
    stronger than any title containment.  Otherwise the parent must be proved
    by formal TMDB title/alias evidence alone.
    """
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    own_tmdb_id = identity.get("tmdb_id")
    same_identity_parent: WorkUnitRecord | None = None
    candidates: dict[int, WorkUnitRecord] = {}
    for candidate in records:
        if candidate.work_unit_id == record.work_unit_id:
            # A unit is never its own parent; the old same-id exclusion only
            # guarded against self-matching, not against a real sibling.
            continue
        candidate_identity = candidate.identity if isinstance(candidate.identity, Mapping) else {}
        if str(candidate_identity.get("media_type") or "") != "tv":
            continue
        tmdb_id = candidate_identity.get("tmdb_id")
        if (
            isinstance(tmdb_id, bool)
            or not isinstance(tmdb_id, int)
            or tmdb_id <= 0
            or _is_physical_special_record(candidate)
        ):
            continue
        if (
            str(identity.get("media_type") or "") == "tv"
            and isinstance(own_tmdb_id, int)
            and not isinstance(own_tmdb_id, bool)
            and own_tmdb_id > 0
            and tmdb_id == own_tmdb_id
        ):
            same_identity_parent = same_identity_parent or candidate
            continue
        candidates.setdefault(tmdb_id, candidate)
    if same_identity_parent is not None:
        return same_identity_parent, False
    scored = [
        (score, candidate)
        for candidate in candidates.values()
        if (score := _relation_score(record, candidate)) > 0
    ]
    if not scored:
        return None, False
    scored.sort(key=lambda item: (-item[0], item[1].work_unit_id))
    top_score = scored[0][0]
    top = [candidate for score, candidate in scored if score == top_score]
    if len(top) != 1:
        return None, True
    return top[0], False


def _record_tmdb_title(record: WorkUnitRecord) -> str:
    """Choose the canonical TMDB title used by the existing planner."""
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    title = identity.get("title")
    if isinstance(title, str) and title.strip():
        return title.strip()
    trace = identity.get("decision_trace")
    if isinstance(trace, Mapping):
        official = trace.get("official_titles")
        if isinstance(official, (list, tuple)):
            for value in official:
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return record.display_label.strip() or "work"


def _canonical_target_child(parent: str, record: WorkUnitRecord) -> str:
    """Build the planner's deterministic desired child root for a unit."""
    title = _record_tmdb_title(record)
    try:
        name = provider_safe_basename(title)
    except Exception:
        name = provider_safe_basename(
            _clean_container_name(title) or f"work-{(record.identity or {}).get('tmdb_id', 'unknown')}"
        )
    return f"{parent.rstrip('/')}/{name}"


def _container_layout_targets(
    runner: SimpleEngineRunner,
    root_job: EngineJob,
    records: Sequence[WorkUnitRecord],
) -> dict[str, dict[str, object]]:
    """Return the generic expected parent/target for every WorkUnit.

    For a root containing multiple confirmed TV identities, the intake folder
    is a pure container: each regular TV identity is a direct child.  An
    OAD/OVA/SP unit is nested under exactly one regular TV parent only when
    formal TMDB title/alias evidence proves that family relation.  A missing
    or tied relation is marked ``uncertain`` so callers can stop that unit
    without putting it under an arbitrary sibling.

    The returned mapping is intentionally stable and side-effect free; the
    main layout planning path uses it to place units under their family parent.
    """
    ordered, container_parent, main_tmdb = _container_plan(runner, root_job, list(records))
    shelf_root = target_root_for_shelf(
        runner.library_root, str(root_job.target_shelf or "anime")
    )
    by_tmdb: dict[int, WorkUnitRecord] = {}
    for record in ordered:
        if _eligible_main_tv_record(record):
            identity = record.identity or {}
            tmdb_id = identity.get("tmdb_id")
            by_tmdb.setdefault(tmdb_id, record)

    # Sub-series grouping (operator ruling 2026-08-30, tree confirmed
    # 2026-08-30): inside a pure franchise container, works sharing a zh-CN
    # title prefix (命运之夜, 命运／冠位指定, 魔法少女☆伊莉雅) nest under a
    # named sub-series directory, and TMDB collections nest one level deeper
    # inside their sub-series.  A group needs at least two member works; a
    # lone work stays flat.  A sub-series label colliding with another work
    # child name fails closed to flat layout.
    collection_parent_by_unit: dict[str, tuple[str, int, str]] = {}
    sub_series_parent_by_unit: dict[str, str] = {}
    if container_parent is not None:
        # Directories an earlier root already created under the container are
        # family evidence for a later root's lone works (巴比伦尼亚 joining
        # 命运-冠位指定/ built by 序章/月光/所罗门).  A listing failure keeps
        # the current-root-only behaviour — the anchor is an optimization of
        # placement, never a correctness gate.
        existing_children: list[str] = []
        try:
            rows = runner.alist.list(container_parent, refresh=True)
        except Exception:
            rows = None
        if isinstance(rows, list):
            existing_children = [
                str(row.get("name") or "")
                for row in rows
                if isinstance(row, Mapping) and row.get("is_dir") is True
            ]
        sub_series_parent_by_unit = _sub_series_parents(
            container_parent, records,
            existing_children=existing_children,
        )
        collection_members: dict[int, list[WorkUnitRecord]] = {}
        collection_names: dict[int, str] = {}
        for record in records:
            identity = record.identity if isinstance(record.identity, Mapping) else {}
            if str(identity.get("media_type") or "") != "movie":
                continue
            tmdb_id = identity.get("tmdb_id")
            if not isinstance(tmdb_id, int) or isinstance(tmdb_id, bool):
                continue
            try:
                detail = runner.tmdb.get(f"/movie/{tmdb_id}")
            except Exception:
                continue
            collection = (
                detail.get("belongs_to_collection")
                if isinstance(detail, Mapping)
                else None
            ) or {}
            collection_id = collection.get("id")
            collection_name = str(collection.get("name") or "").strip()
            if not isinstance(collection_id, int) or not collection_name:
                continue
            collection_members.setdefault(collection_id, []).append(record)
            collection_names.setdefault(collection_id, collection_name)
        work_child_names = {
            posixpath.basename(_canonical_target_child(container_parent, record))
            for record in records
            if str((record.identity or {}).get("media_type") or "") == "tv"
        }
        for collection_id, members in collection_members.items():
            if len(members) < 2:
                continue
            label = _collection_directory_label(collection_names[collection_id])
            if not label or label in work_child_names:
                continue
            # A collection whose members all share one sub-series prefix
            # nests inside that sub-series directory (天之杯 inside
            # 命运之夜); a cross-series collection stays at the container.
            # A collection whose label equals the sub-series' own name (空之
            # 境界) IS that sub-series: the anchor directory itself, never a
            # same-named nested duplicate.
            member_parents = {
                sub_series_parent_by_unit.get(member.work_unit_id)
                for member in members
            }
            anchor = next(iter(member_parents)) if len(member_parents) == 1 else None
            base_dir = anchor if anchor else container_parent
            if anchor is not None and posixpath.basename(
                anchor.rstrip("/")
            ) == label:
                collection_dir = anchor
            else:
                collection_dir = posixpath.join(base_dir, label)
            for member in members:
                collection_parent_by_unit[member.work_unit_id] = (
                    collection_dir, collection_id, label,
                )

    targets: dict[str, dict[str, object]] = {}
    for record in records:
        identity = record.identity if isinstance(record.identity, Mapping) else {}
        media_type = str(identity.get("media_type") or "tv")
        tmdb_id = identity.get("tmdb_id")
        parent_path: str | None
        relation = "direct"
        parent_unit_id: str | None = None
        parent_tmdb_id: int | None = None
        uncertain = False
        expected_root: str | None = None
        if container_parent is not None:
            parent_path = container_parent
            sub_series_dir = sub_series_parent_by_unit.get(record.work_unit_id)
            if sub_series_dir is not None:
                parent_path = sub_series_dir
            collection_target = collection_parent_by_unit.get(record.work_unit_id)
            if collection_target is not None:
                parent_path = collection_target[0]
            if _is_physical_special_record(record) and len(by_tmdb) > 0:
                parent, _ambiguous = _special_parent_record(record, records)
                if parent is not None:
                    parent_unit_id = parent.work_unit_id
                    parent_identity = parent.identity or {}
                    candidate_tmdb = parent_identity.get("tmdb_id")
                    parent_tmdb_id = candidate_tmdb if isinstance(candidate_tmdb, int) else None
                    relation = "nested_special"
                    parent_path = _canonical_target_child(container_parent, parent)
                else:
                    # A special unit with no unique formal family must not be
                    # silently promoted to a direct sibling.
                    uncertain = True
                    relation = "uncertain"
            if relation == "direct" and media_type == "tv":
                relation = "direct_tv"
        elif main_tmdb is not None:
            is_main = (
                _eligible_main_tv_record(record)
                and (record.identity or {}).get("tmdb_id") == main_tmdb
            )
            if is_main:
                parent_path = shelf_root
                relation = "main_tv"
            else:
                parent_path = None  # resolved to main target at execution time
                relation = "nested_under_main"
        else:
            parent_path = shelf_root
        if expected_root is None and not uncertain:
            expected_root = (
                _canonical_target_child(parent_path, record)
                if parent_path is not None
                else None
            )
        collection_target = collection_parent_by_unit.get(record.work_unit_id)
        targets[record.work_unit_id] = {
            "parent_path": parent_path,
            "target_root": expected_root,
            "relation": relation,
            "parent_work_unit_id": parent_unit_id,
            "parent_tmdb_id": parent_tmdb_id,
            "uncertain": uncertain,
            **(
                {
                    "collection_id": collection_target[1],
                    "collection_name": collection_target[2],
                }
                if collection_target is not None
                else {}
            ),
        }
    # Preserve deterministic insertion order for callers that iterate the map.
    return {record.work_unit_id: targets[record.work_unit_id] for record in ordered if record.work_unit_id in targets}


_NON_MAIN_TV_ROLES = frozenset({
    "special_group",
    "extras_group",
    "version_group",
    "subtitle_group",
    "release_group",
    "resource_group",
    "uncertain",
})


def _record_is_single_video_scope(record: WorkUnitRecord) -> bool:
    """Whether a unit's whole source scope is exactly one video file.

    Used only as the special-pending signal in the main-TV season-split
    check: a single-video unit without season facts claims no season scope
    at all (a special awaiting its lane), so it cannot contest the seasons
    its sibling season units proved.
    """
    paths = tuple(record.source_paths or ())
    if len(paths) != 1:
        return False
    from engine.scrapeflow.media_policy import is_video_filename

    return is_video_filename(posixpath.basename(paths[0].rstrip("/")))


def _eligible_main_tv_record(record: WorkUnitRecord) -> bool:
    """Whether a persisted unit may provide main-TV hierarchy evidence."""
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    tmdb_id = identity.get("tmdb_id")
    return (
        record.identity_status == "confirmed"
        and not record.requires_content_expansion
        and str(identity.get("media_type") or "") == "tv"
        and isinstance(tmdb_id, int)
        and not isinstance(tmdb_id, bool)
        and tmdb_id > 0
        and str(record.role or "").casefold() not in _NON_MAIN_TV_ROLES
        and not _is_physical_special_record(record)
    )


def _record_proved_positive_seasons(record: WorkUnitRecord) -> set[int]:
    """Collect durable positive-season facts from an eligible regular TV."""
    if not _eligible_main_tv_record(record):
        return set()
    identity = record.identity or {}
    seasons = {
        season
        for season in record.claimed_seasons
        if (
            isinstance(season, int)
            and not isinstance(season, bool)
            and season > 0
        )
    }
    identity_season = identity.get("season")
    if (
        isinstance(identity_season, int)
        and not isinstance(identity_season, bool)
        and identity_season > 0
    ):
        seasons.add(identity_season)
    proof = SingleSeasonEpisodeProof.from_dict(record.reconciliation_evidence)
    tmdb_id = identity.get("tmdb_id")
    if (
        proof is not None
        and proof.tmdb_id == tmdb_id
        and proof.season > 0
    ):
        seasons.add(proof.season)
    return seasons


def _main_tv_record(records: Sequence[WorkUnitRecord]) -> WorkUnitRecord | None:
    """Return one regular TV record proved to own the container root.

    A confirmed special/version/opaque unit is not a main-series candidate.
    Multiple physical records for the same TMDB identity are also ambiguous:
    B/W must first coalesce or explicitly separate their ownership instead of
    letting every record write as the main TV.
    """
    candidates = [record for record in records if _eligible_main_tv_record(record)]
    if not candidates:
        return None
    by_tmdb: dict[int, list[WorkUnitRecord]] = {}
    for record in candidates:
        tmdb_id = int((record.identity or {})["tmdb_id"])
        by_tmdb.setdefault(tmdb_id, []).append(record)
    for items in by_tmdb.values():
        if len(items) <= 1:
            continue
        # Split season records are safe only when B/W gave every physical
        # record a non-empty, disjoint positive-season scope.  Empty/stale
        # siblings (including historical version backups) remain ambiguous.
        # A single-video record with no season facts claims no season at
        # all — it is a special-pending unit (High School DxD Hero 00)
        # owned by the nested-special lane, not a contested season scope.
        seen_seasons: set[int] = set()
        for item in items:
            seasons = _record_proved_positive_seasons(item)
            if not seasons:
                if _record_is_single_video_scope(item):
                    continue
                return None
            if seen_seasons.intersection(seasons):
                return None
            seen_seasons.update(seasons)

    # A second row carrying the same identity but excluded above (for example
    # VERSION_GROUP or a stale uncertain state) is an ownership collision,
    # not evidence that may be silently folded into the regular unit.  A
    # same-identity physical special (OVA/OAD/SP of this very show — same
    # TMDB id) is NOT a collision: it is the work's own special, the
    # nested-special lane owns it, and treating it as contested ownership
    # demoted the whole single-work root to a named container wrapper
    # (恶魔高校D×D nested under the intake container beside its own
    # specials).
    for tmdb_id in by_tmdb:
        same_identity = [
            record
            for record in records
            if (
                isinstance(record.identity, Mapping)
                and record.identity.get("media_type") == "tv"
                and record.identity.get("tmdb_id") == tmdb_id
            )
        ]
        colliding = [
            record
            for record in same_identity
            if not _is_physical_special_record(record)
        ]
        if (
            len(colliding) != len(by_tmdb[tmdb_id])
            or any(not _eligible_main_tv_record(item) for item in colliding)
        ):
            return None

    proved = {
        tmdb_id: {
            season
            for item in items
            for season in _record_proved_positive_seasons(item)
        }
        for tmdb_id, items in by_tmdb.items()
    }
    # For a single identity, explicit split-season records collectively prove
    # the same main TV family; return a representative for callers that need
    # one stable record id while layout uses the identity for all split parts.
    if len(by_tmdb) == 1:
        return candidates[0]
    multi_season = [
        tmdb_id for tmdb_id, seasons in proved.items() if len(seasons) > 1
    ]
    if (
        len(multi_season) == 1
        and all(
            len(proved[tmdb_id]) == 1
            for tmdb_id in proved
            if tmdb_id != multi_season[0]
        )
    ):
        return by_tmdb[multi_season[0]][0]
    return None


def _main_tv_identity(records: Sequence[WorkUnitRecord]) -> int | None:
    """Return the one structurally proved TV identity that owns the root."""
    main = _main_tv_record(records)
    if main is None:
        return None
    return int((main.identity or {})["tmdb_id"])


def _normalized_title_key(value: str) -> str:
    """One work title as a bounded comparison key for prefix grouping."""
    from engine.scrapeflow.remote_paths import provider_safe_basename

    cleaned = provider_safe_basename(
        re.sub(r"[（(]\s*\d{4}(-\d{4})?\s*[)）]", "", str(value or "").strip())
    )
    return str(cleaned or "").strip()


def _sub_series_parents(
    container_parent: str,
    records: Sequence[WorkUnitRecord],
    *,
    existing_children: Collection[str] = (),
) -> dict[str, str]:
    """Nest same-family works under sub-series directories (operator tree).

    Operator-confirmed tree (2026-08-30) for a pure franchise container:

    1. Containment: a movie whose zh-CN title contains a TV work's title as
       a bounded substring (魔法少女☆伊莉雅剧场版 contains 魔法少女☆伊莉雅,
       命运／奇异赝品 黎明低语 contains 命运／奇异赝品) belongs to that TV's
       family root — the TV's own directory, or the TV's prefix-group label
       directory when the TV itself groups with sibling TV works.
    2. Prefix groups: the remaining works whose cleaned titles share a
       separator-bounded common prefix (命运之夜, 命运-冠位指定, 空之境界)
       nest under a label directory named with the prefix; a group needs at
       least two distinct works and the prefix must end at a separator in
       every member's own original title (命运-冠位嘉年华 never folds into
       the 命运-冠位指定 stem).
    3. Library-anchored grouping: a lone work from a *later* root joins a
       prefix family that an *earlier* root already created in the library
       (巴比伦尼亚 arriving after 序章/月光/所罗门 built 命运-冠位指定/).
       The anchor must be a directory that already exists under the
       container and whose normalized name is a separator-bounded strict
       prefix of the work's normalized title.  Nothing new is ever created
       from library evidence alone — without the existing directory the
       work keeps rule 2's fail-flat behaviour.
    4. Everything else stays a direct container child.

    Collections are anchored by their members' shared family root (the
    collection pass runs after this function and reads its result).
    Returns ``{work_unit_id: family directory}``; the family-directory owner
    itself (a TV whose own dir IS the family root) is absent from the map.
    """
    identities: dict[tuple[str, int], str] = {}
    representative: dict[tuple[str, int], WorkUnitRecord] = {}
    for record in records:
        identity = record.identity if isinstance(record.identity, Mapping) else {}
        media_type = str(identity.get("media_type") or "")
        tmdb_id = identity.get("tmdb_id")
        title = str(identity.get("title") or "").strip()
        if media_type not in {"tv", "movie"} or not isinstance(tmdb_id, int):
            continue
        if not title:
            continue
        # A physical special keeps the nested_special family path — prefix
        # grouping must not steal it into a label directory.
        if _is_physical_special_record(record):
            continue
        key = (media_type, tmdb_id)
        identities.setdefault(key, title)
        representative.setdefault(key, record)
    if len(identities) < 2:
        return {}

    keys = {value: _normalized_title_key(value) for value in identities.values()}
    separators = {" ", "　", "-", "—", "–", "／", "/", "·", "：", ":", "！", "!", "？", "?", "）", "(", ")", "（", "]", "[", "「", "」"}

    # --- Rule 1: containment --------------------------------------------
    tv_keys = [key for key in identities if key[0] == "tv"]
    containment_owner: dict[tuple[str, int], tuple[str, int]] = {}
    for key in identities:
        if key[0] != "movie":
            continue
        raw = str(identities[key])
        theatrical_markers = ("剧场版", "劇場版", "电影", "電影", "Theatrical")

        def _after_ok(text: str, position: int) -> bool:
            if position >= len(text):
                return True
            if text[position] in separators:
                return True
            return any(
                text[position : position + len(marker)] == marker
                for marker in theatrical_markers
            )

        best: tuple[str, int] | None = None
        best_len = 0
        for tv_key in tv_keys:
            tv_title = str(identities[tv_key])
            if len(keys[tv_title]) < 4 or keys[tv_title] == keys[raw]:
                continue
            index = raw.find(tv_title)
            while index != -1:
                after = index + len(tv_title)
                before_ok = index == 0 or raw[index - 1] in separators
                if before_ok and _after_ok(raw, after) and len(tv_title) > best_len:
                    best = tv_key
                    best_len = len(tv_title)
                index = raw.find(tv_title, index + 1)
        if best is not None:
            containment_owner[key] = best

    # --- Rule 2: prefix groups over non-contained works -----------------
    group_members: dict[tuple[str, int], str] = {}
    uncontained = [
        key for key in identities if key not in containment_owner
    ]
    groups: dict[str, set[tuple[str, int]]] = defaultdict(set)

    def _key_bounded(key: str, prefix_len: int) -> bool:
        """The prefix ends at a separator inside this normalized key.

        命运-冠位指定 绝对魔兽战线巴比伦尼亚 has a boundary after 命运-冠位
        指定 (the next normalized character is a space); 命运-冠位嘉年华
        continues the glued word with 嘉, so the shorter stem is not a
        boundary in ITS OWN key and the pair never groups.
        """
        if prefix_len >= len(key):
            return False
        return key[prefix_len] in separators

    for index_a, key_a in enumerate(uncontained):
        title_a = identities[key_a]
        key_a_norm = keys[title_a]
        if len(key_a_norm) < 4:
            continue
        for key_b in uncontained[index_a + 1:]:
            title_b = identities[key_b]
            key_b_norm = keys[title_b]
            if len(key_b_norm) < 4 or key_a_norm == key_b_norm:
                continue
            shared = min(len(key_a_norm), len(key_b_norm))
            prefix_len = 0
            for index in range(shared):
                if key_a_norm[index] != key_b_norm[index]:
                    break
                prefix_len = index + 1
            while prefix_len > 0 and key_a_norm[prefix_len - 1] in separators:
                prefix_len -= 1
            if prefix_len < 4:
                continue
            # An anchor-exact member (its whole normalized key IS the
            # prefix, 命运之夜 the TV) joins its own group; otherwise the
            # prefix must be strictly shorter than both keys and end at a
            # separator in both.
            a_exact = prefix_len == len(key_a_norm)
            b_exact = prefix_len == len(key_b_norm)
            if a_exact and b_exact:
                continue
            if not (
                (a_exact or _key_bounded(key_a_norm, prefix_len))
                and (b_exact or _key_bounded(key_b_norm, prefix_len))
            ):
                continue
            prefix = key_a_norm[:prefix_len]
            groups[prefix].add(key_a)
            groups[prefix].add(key_b)

    # Widest-group-first assignment: a member belonging to both a broad
    # family stem (命运-冠位指定) and a narrow pair prefix (命运-冠位指定
    # -神圣圆桌领域卡美洛) joins the broad family — the narrow pairing is a
    # TMDB collection, and the collection pass nests it inside the family.
    for prefix in sorted(groups, key=len):
        if len(groups[prefix]) < 2:
            continue
        for key in groups[prefix]:
            group_members.setdefault(key, prefix)

    # --- Rule 3: library-anchored grouping --------------------------------
    # A lone later-root work joins a family directory an earlier root already
    # created in the library.  Only an existing directory anchors: the match
    # is its normalized name as a separator-bounded strict prefix of the
    # work's normalized title, the longest anchor wins, and without any
    # anchor the work keeps its flat behaviour.  The work's own canonical
    # directory name never anchors (equal names are the merge path, not a
    # family relation).
    anchored_prefixes: dict[tuple[str, int], str] = {}
    if existing_children:
        existing_by_key: list[tuple[str, str]] = []
        for name in existing_children:
            label = str(name or "").strip()
            if not label:
                continue
            existing_by_key.append((_normalized_title_key(label), label))
        for key in uncontained:
            if key in group_members:
                continue
            title_key = keys[identities[key]]
            if len(title_key) < 4:
                continue
            best: tuple[int, str] | None = None
            for existing_key, label in existing_by_key:
                if (
                    len(existing_key) < 4
                    or len(existing_key) >= len(title_key)
                    or not title_key.startswith(existing_key)
                    or not _key_bounded(title_key, len(existing_key))
                ):
                    continue
                if best is None or len(existing_key) > best[0]:
                    best = (len(existing_key), label)
            if best is not None:
                anchored_prefixes[key] = best[1]

    # --- Family roots ----------------------------------------------------
    family_dir: dict[tuple[str, int], str] = {}
    for key, prefix in group_members.items():
        family_dir[key] = posixpath.join(container_parent, prefix)
    for key, label in anchored_prefixes.items():
        family_dir[key] = posixpath.join(container_parent, label)
    # A containment-anchor TV that belongs to no prefix group keeps its own
    # directory as the family root (伊莉雅: movies nest inside the TV root).
    # An anchor TV inside a prefix group moves into the group's label
    # directory like every other member (命运之夜 2006 nests under
    # Fate/命运之夜/命运之夜/, the 来自深渊 double-nesting form).
    anchored_self: set[tuple[str, int]] = set()
    for key, tv_key in containment_owner.items():
        anchor = family_dir.get(tv_key)
        if anchor is None:
            anchor = _canonical_target_child(container_parent, representative[tv_key])
            anchored_self.add(tv_key)
        family_dir[key] = anchor
    result: dict[str, str] = {}
    for record in records:
        identity = record.identity if isinstance(record.identity, Mapping) else {}
        media_type = str(identity.get("media_type") or "")
        tmdb_id = identity.get("tmdb_id")
        key = (media_type, tmdb_id)
        if key in anchored_self:
            continue
        target = family_dir.get(key)
        if target is None:
            continue
        result[record.work_unit_id] = target
    return result


def _collection_directory_label(name: str) -> str | None:
    """Delegate to the engine's shared collection-label policy."""
    from engine.scrapeflow.media_naming import collection_directory_label

    return collection_directory_label(name)


_SEASON_DIRECTORY_RE = re.compile(r"(?:Season\s*\d{1,3}|Specials)", re.IGNORECASE)


def _existing_library_container(
    runner: SimpleEngineRunner,
    root_job: EngineJob,
) -> str | None:
    """Return this intake folder's already-established library container.

    Container continuation must be read off the library, not re-derived from
    the current root's records: an earlier root can have written one TV work
    plus its films under ``<shelf>/<cleaned intake name>/``, and this root's
    own units are then only part of the family.

    The evidence is deliberately structural and bounded to one listing:

    * the directory ``<shelf>/<cleaned intake name>`` exists,
    * it holds at least one child **directory** (a work child), and
    * none of its children is a ``Season xx`` directory — a work root owns
      its seasons directly, so seeing one means this is a work, not a
      container, and the ordinary merge path must keep handling it.

    Any listing failure returns ``None``: continuation is a placement
    improvement, never a correctness gate.
    """
    shelf_root = target_root_for_shelf(
        runner.library_root, str(root_job.target_shelf or "anime")
    )
    intake_basename = posixpath.basename(
        str(runner._job_ingress_source(root_job)).rstrip("/")  # noqa: SLF001
    )
    container_name = _clean_container_name(intake_basename)
    if not container_name:
        return None
    candidate = f"{shelf_root}/{container_name}"
    try:
        rows = runner.alist.list(candidate, refresh=True)
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    child_directories = [
        str(row.get("name") or "")
        for row in rows
        if isinstance(row, Mapping) and row.get("is_dir") is True
    ]
    if not child_directories:
        return None
    if any(_SEASON_DIRECTORY_RE.fullmatch(name.strip()) for name in child_directories):
        return None
    return candidate


def _container_plan(
    runner: SimpleEngineRunner,
    root_job: EngineJob,
    records: list[WorkUnitRecord],
) -> tuple[list[WorkUnitRecord], str | None, int | None]:
    """Decide the Fate-style container layout for a multi-unit root.

    Evidence-driven, no name guessing:

    - one distinct TV identity (e.g. 刀剑神域 + its movies): the main series
      owns the container; TV units plan at the shelf root and the main
      unit's planned target root becomes the parent of every movie unit;
    - several distinct TV identities with one proved multi-season main: the
      main TV owns the root and each proved single-season sibling nests below;
    - otherwise several TV identities or none (e.g. Fate, 空之境界): the
      container is a pure collection named after the cleaned intake folder.
      Every regular TV identity is a direct child; a formally proved
      OAD/OVA/SP unit is assigned to its unique family parent by
      ``_container_layout_targets``.

    Single-unit roots are returned unchanged with no container parent.
    Returns ``(ordered_records, container_parent, main_tmdb)``.
    """
    if len(records) <= 1:
        return list(records), None, None
    existing = _existing_library_container(runner, root_job)
    if existing is not None:
        # The library already decided this intake folder is a container: it
        # holds work child directories and no season directory of its own.
        # A later root must join that container instead of planning a sibling
        # at the shelf root — that is how `/番剧/钢之炼金术师 FA` appeared beside
        # `/番剧/钢之炼金术师` while the reviewed layout wants
        # `/番剧/钢之炼金术师/钢之炼金术师 FA`.  Every unit becomes a direct
        # child, exactly as the pure-container path below already does.
        ordered = sorted(
            records,
            key=lambda record: (
                0
                if str((record.identity or {}).get("media_type") or "") == "tv"
                and not _is_physical_special_record(record)
                else 1
                if str((record.identity or {}).get("media_type") or "") == "tv"
                else 2,
                str((record.identity or {}).get("tmdb_id") or ""),
                record.work_unit_id,
            ),
        )
        return ordered, existing, None
    main_tmdb = _main_tv_identity(records)
    if main_tmdb is not None:
        ordered = sorted(records, key=lambda record: (
            0
            if (
                _eligible_main_tv_record(record)
                and (record.identity or {}).get("tmdb_id") == main_tmdb
            )
            else 1,
            record.work_unit_id,
        ))
        return ordered, None, main_tmdb
    shelf = str(root_job.target_shelf or "anime")
    shelf_root = target_root_for_shelf(runner.library_root, shelf)
    intake_basename = posixpath.basename(
        str(runner._job_ingress_source(root_job)).rstrip("/")  # noqa: SLF001
    )
    container_name = _clean_container_name(intake_basename)
    if container_name is None:
        tv_records = [
            record for record in records
            if str((record.identity or {}).get("media_type") or "") == "tv"
        ]
        for candidate in tv_records or list(records):
            title = str((candidate.identity or {}).get("title") or "").strip()
            cleaned = (
                title
                if title
                else _clean_container_name(
                    str(candidate.display_label or "").strip()
                    or posixpath.basename(str(candidate.source_paths[0]).rstrip("/"))
                )
            )
            if cleaned:
                container_name = cleaned
                break
    container_parent = (
        f"{shelf_root}/{container_name}" if container_name else None
    )
    ordered = sorted(
        records,
        key=lambda record: (
            0
            if str((record.identity or {}).get("media_type") or "") == "tv"
            and not _is_physical_special_record(record)
            else 1
            if str((record.identity or {}).get("media_type") or "") == "tv"
            else 2,
            str((record.identity or {}).get("tmdb_id") or ""),
            record.work_unit_id,
        ),
    )
    return ordered, container_parent, None


def _container_artifact_job_id(root_task_id: str) -> str:
    """Return the one deterministic metadata carrier id for a RootJob."""
    return f"container-artifacts-{root_task_id}"


def _container_artifact_inputs(
    runner: SimpleEngineRunner,
    records: Sequence[WorkUnitRecord],
    container_parent: str,
) -> dict[str, object] | None:
    """Select a proved child identity as representative container artwork.

    Only an already executed child carrier is eligible.  This prevents a
    planned/uncertain sibling from supplying metadata and ensures the root
    marker is written only after at least one child has passed G/H.  The
    target-root containment check also protects a historical layout from
    accidentally painting a different container.
    """
    parent = _safe_remote_path(
        container_parent, field="container metadata parent", allow_root=False,
    )
    candidates: list[tuple[str, str, Mapping[str, object], WorkUnitRecord]] = []
    for record in records:
        if not record.writer_job_id:
            continue
        try:
            carrier = runner.get_job(record.writer_job_id)
        except Exception:
            continue
        if carrier.phase != "executed":
            continue
        raw_plan = carrier.plan if isinstance(carrier.plan, Mapping) else {}
        target = raw_plan.get("target_root")
        try:
            target_path = _safe_remote_path(
                target, field="已执行作品目标根", allow_root=False,
            )
        except Exception:
            continue
        if not target_path.startswith(parent.rstrip("/") + "/"):
            continue
        metadata = raw_plan.get("metadata")
        if not isinstance(metadata, Mapping):
            continue
        candidates.append((target_path, record.work_unit_id, metadata, record))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0].casefold(), item[1]))
    target_path, _unit_id, metadata, record = candidates[0]
    poster = metadata.get("poster_path")
    if not isinstance(poster, str) or not poster.strip():
        raise ContainerMetadataAttention(
            f"已执行子作品 {target_path} 没有可继承的 TMDB 海报证据"
        )
    backdrop = metadata.get("backdrop_path")
    if not isinstance(backdrop, str) or not backdrop.strip():
        backdrop = None
    tmdb_id = metadata.get("tmdb_id")
    if not (
        isinstance(tmdb_id, int)
        and not isinstance(tmdb_id, bool)
        and tmdb_id > 0
    ):
        identity = record.identity if isinstance(record.identity, Mapping) else {}
        tmdb_id = identity.get("tmdb_id")
    return {
        "poster_path": poster.strip(),
        "backdrop_path": backdrop.strip() if isinstance(backdrop, str) else None,
        "representative_tmdb_id": tmdb_id
        if isinstance(tmdb_id, int) and not isinstance(tmdb_id, bool) and tmdb_id > 0
        else None,
    }


def ensure_container_artifacts(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> EngineJob | None:
    """Ensure a pure series-container root has poster and NFO metadata.

    This is a RootJob-level artifact pass, not a second writer: the
    deterministic internal carrier is executed by ``SimpleEngineRunner``'s
    existing single writer and exact readback.  A dominant TV identity owns
    its own root and therefore does not need this directory-only marker.
    """
    records = load_work_unit_records(state_root, root_task_id)
    root_job = runner.get_job(root_task_id)
    _ordered, container_parent, main_tmdb = _container_plan(
        runner, root_job, records,
    )
    if container_parent is None or main_tmdb is not None:
        return None
    # The pure-container path and a real work's own root can name the same
    # directory when the intake basename equals that work's title (银魂): a
    # layout re-derivation after a late sibling demoted the root from the
    # dominant-TV form still keeps the already-written main unit at the
    # container path.  A work-unit carrier targeting exactly the container
    # root proves the directory already owns a real identity NFO and
    # artwork; layering the directory-only marker on top would compete with
    # that identity (and its later writes would collide with the marker).
    for record in records:
        if not record.writer_job_id:
            continue
        try:
            carrier = runner.get_job(record.writer_job_id)
        except Exception:
            continue
        raw_plan = carrier.plan if isinstance(carrier.plan, Mapping) else {}
        if raw_plan.get("target_root") == container_parent:
            return None
    container_title = posixpath.basename(container_parent.rstrip("/"))
    if not container_title:
        raise ContainerMetadataAttention("容器根目录没有可用的清洗名称")
    artifact_job_id = _container_artifact_job_id(root_task_id)
    existing: EngineJob | None = None
    try:
        existing = runner.get_job(artifact_job_id)
    except Exception:
        existing = None
    if existing is not None:
        summary = existing.summary if isinstance(existing.summary, Mapping) else {}
        if (
            summary.get("container_artifacts") is not True
            or summary.get("root_job_id") != root_task_id
            or existing.plan.get("target_root") != container_parent
        ):
            raise ContainerMetadataAttention(
                "容器元数据 carrier 的来源所有权或目标根与当前 B/W 不一致"
            )
        metadata = existing.plan.get("metadata") if isinstance(existing.plan, Mapping) else {}
        if not isinstance(metadata, Mapping):
            raise ContainerMetadataAttention("容器元数据 carrier 缺少持久化元数据")
        # The persisted artwork identity is sticky.  The representative is
        # the first proved child at carrier-creation time; re-running that
        # argmin over a sibling set that grows as late units execute would
        # "drift" on every root run and park the root behind an attention
        # with no confirmation surface.  Only the loss of the provenance
        # child reopens the choice: a rolled-back representative means the
        # borrowed artwork no longer belongs to any executed sibling.
        representative = metadata.get("representative_tmdb_id")
        if (
            isinstance(representative, int)
            and not isinstance(representative, bool)
            and not any(
                (record.identity or {}).get("tmdb_id") == representative
                and record.writer_job_id
                for record in records
            )
        ):
            # The provenance child is gone, so the choice reopens: rebind the
            # same deterministic carrier to a currently proved sibling.  A
            # rebind never overwrites library artwork — the repair replay
            # below still treats existing files as authoritative — it only
            # un-parks the root and repairs future projections.  With no
            # proved sibling left there is no safe identity to borrow, so
            # the attention still fails closed.
            fresh = _container_artifact_inputs(runner, records, container_parent)
            if fresh is None:
                raise ContainerMetadataAttention(
                    f"容器元数据代表单元已失效: tmdb/{representative}"
                )
            existing = runner.rebind_container_artifacts(
                existing.id,
                poster_path=str(fresh["poster_path"]),
                backdrop_path=(
                    str(fresh["backdrop_path"])
                    if fresh.get("backdrop_path") is not None
                    else None
                ),
                representative_tmdb_id=(
                    fresh.get("representative_tmdb_id")
                    if isinstance(fresh.get("representative_tmdb_id"), int)
                    and not isinstance(fresh.get("representative_tmdb_id"), bool)
                    else None
                ),
            )
    else:
        inputs = _container_artifact_inputs(runner, records, container_parent)
        if inputs is None:
            # No accepted child means there is no safe image identity to
            # borrow; leave the root untouched until one sibling reaches H.
            return None
        existing = runner.plan_container_artifacts(
            root_job_id=root_task_id,
            source_path=runner._job_ingress_source(root_job),  # noqa: SLF001
            target_root=container_parent,
            target_shelf=root_job.target_shelf,
            container_title=container_title,
            poster_path=str(inputs["poster_path"]),
            backdrop_path=(
                str(inputs["backdrop_path"])
                if inputs.get("backdrop_path") is not None
                else None
            ),
            representative_tmdb_id=inputs.get("representative_tmdb_id")
            if isinstance(inputs.get("representative_tmdb_id"), int)
            else None,
            job_id=artifact_job_id,
            pause_requested=pause_requested,
        )
    if existing.phase == "executed":
        # The artifact-only plan has no media rows, so this repair path only
        # rechecks/recreates root NFO/artwork and never replays a child move.
        return runner.repair_automatic_artifacts(
            existing.id, pause_requested=pause_requested,
        )
    if existing.phase in {
        "executing", "verifying", "cleaning", "retry_wait", "failed",
    }:
        existing = runner.recover_job(existing.id)
    if existing.phase in {"planned", "retry_wait", "failed"}:
        return runner.execute_job(existing.id, pause_requested=pause_requested)
    if existing.phase == "executed":
        return existing
    raise RuntimeError(
        f"容器元数据 carrier 进入不可继续状态: {existing.phase}"
    )


def _validated_formal_work_root(
    runner: SimpleEngineRunner,
    raw: object,
    *,
    field: str,
) -> str | None:
    """Return one fresh-verified formal-library work root.

    ``matched_work_root`` and an executed carrier's ``target_root`` both
    become parent directories for sibling units.  They therefore need the
    same path and existence proof before they can influence a new plan.
    """
    if not raw:
        return None
    try:
        root = _safe_remote_path(raw, field=field, allow_root=False)
    except Exception as exc:
        raise ValueError(f"{field}无效") from exc
    formal_shelves = tuple(
        target_root_for_shelf(runner.library_root, shelf)
        for shelf in ("movie", "anime", "us_tv")
    )
    if not any(root.startswith(shelf + "/") for shelf in formal_shelves):
        raise ValueError(f"{field}不在正式库货架内")
    if runner._remote_entry_kind(root) != "directory":  # noqa: SLF001 - fresh work-root proof
        raise ValueError(f"{field}已不存在或类型异常")
    return root


def _validated_matched_work_root(
    runner: SimpleEngineRunner,
    record: WorkUnitRecord,
) -> str | None:
    """Return a fresh-verified D-locked root for an existing main work.

    A main TV work may be ``duplicate_complete`` or ``existing_gap`` and
    therefore have no internal writer carrier.  Its D result is still the
    authoritative container root for a new sibling film/spinoff.  Falling
    back to the newly selected shelf in that situation would split a single
    source container across shelves.  Treat a malformed or vanished locked
    root as a real reconciliation/external-state fault instead of silently
    widening the new sibling's target.
    """
    return _validated_formal_work_root(
        runner, record.matched_work_root, field="对账锁定的作品根",
    )


def _unit_video_rows(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    *,
    strict: bool = False,
) -> list[dict[str, Any]]:
    """Return the unit's video-file rows from the persisted B snapshot."""
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None or not record.source_paths:
        if strict:
            raise GapDiscoveryAttention("缺少可验证的 B 来源快照，无法核对缺口")
        return []
    try:
        scopes = validate_source_scope(snapshot["root"], record.source_paths)
    except (KeyError, TypeError, ValueError) as exc:
        if strict:
            raise GapDiscoveryAttention("B 来源快照损坏或单元范围失配，无法核对缺口") from exc
        return []
    raw_rows = snapshot.get("rows")
    if not isinstance(raw_rows, list):
        if strict:
            raise GapDiscoveryAttention("B 来源快照文件清单损坏，无法核对缺口")
        return []
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, Mapping):
            if strict:
                raise GapDiscoveryAttention("B 来源快照含无效清单行，无法核对缺口")
            continue
        full_path = str(row.get("full_path") or "")
        if row.get("is_dir") is True:
            continue
        if not _path_in_scope(full_path, scopes):
            continue
        name = str(row.get("name") or "")
        if is_video_filename(name):
            rows.append(row)
    return rows


def _unit_has_disc_image(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> bool:
    """Check the exact persisted B scope before F can create a carrier."""
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None or not record.source_paths:
        return False
    try:
        scopes = validate_source_scope(snapshot["root"], record.source_paths)
    except ValueError:
        return False
    return any(
        row.get("is_dir") is not True
        and _path_in_scope(str(row.get("full_path") or ""), scopes)
        and is_disc_image_filename(str(row.get("name") or ""))
        for row in snapshot["rows"]
        if isinstance(row, Mapping)
    )


def _park_unit_for_disc_image(record: WorkUnitRecord) -> WorkUnitRecord:
    """Turn any stale F-ready record back into visible content attention."""
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
        attention=DISC_IMAGE_INSPECTION_REQUIRED,
        updated_at=_now(),
    )


def _fresh_merged_target_episode_tokens(
    runner: SimpleEngineRunner,
    executed_plan: Mapping[str, Any],
) -> list[str]:
    """Read the locked existing work root before registering E3 gaps.

    A merge plan commonly supplies only the *new* episode(s).  J must compare
    the official catalog with the post-write work root, not just with this
    incoming slice, otherwise every already-present sibling episode becomes
    a fabricated open gap.  The E3 plan has already proved its exact locked
    target root; this performs one fresh, bounded listing of that same root.
    """
    raw_target = executed_plan.get("target_root")
    if not isinstance(raw_target, str) or not raw_target.strip():
        raise GapLedgerPersistenceError("归并计划缺少可回读的目标作品根")
    target_root = _safe_remote_path(
        raw_target,
        field="merge gap target_root",
        allow_root=False,
    )
    if runner._remote_entry_kind(target_root) != "directory":  # noqa: SLF001
        raise GapLedgerPersistenceError("归并写后目标作品根无法精确回读")
    walk = getattr(runner.alist, "walk", None)
    try:
        if callable(walk):
            try:
                rows = walk(target_root, refresh=True)
            except TypeError:
                rows = walk(target_root)
            if not isinstance(rows, list):
                raise ValueError("AList 归并目标文件清单无效")
        else:
            # Narrow test/adapter fallback.  Production AListClient.walk()
            # excludes recognised extras before episode parsing.
            rows = walk_source_rows(runner.alist, target_root)
    except Exception as exc:
        raise GapLedgerPersistenceError("归并写后目标季集无法 fresh-list") from exc
    tokens: list[str] = []
    for row in rows:
        if row.get("is_dir") is True:
            continue
        name = str(row.get("name") or "")
        if not is_video_filename(name):
            continue
        # Pass the full path: the shared parser can then use an explicit
        # ``Season N`` directory for otherwise bare episode ordinals.
        value = str(row.get("full_path") or name)
        tokens.extend(
            f"S{season:02d}E{episode:02d}"
            for season, episode in audit_episode_tokens(value)
        )
    return tokens


def _multi_season_absolute_map_path(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> str | None:
    """Derive an explicit episode map for a multi-season absolute-number block.

    ``[01]..[47]`` style releases whose episode count exactly equals one
    contiguous suffix of the official TMDB seasons (e.g. 爱丽丝篇 = S3+S4 of
    the parent series) cannot be expressed by a single ``season`` hint.  The
    Engine's explicit episode map bypasses the smart season inference for
    exactly this shape; the map is written to the local state root and the
    request carries only its path.  Returns ``None`` when the shape does not
    match, leaving the ordinary planner path untouched.
    """
    identity = record.identity or {}
    if str(identity.get("media_type") or "") != "tv":
        return None
    rows = _unit_video_rows(state_root, root_task_id, record)
    if not rows:
        return None
    names = [str(row.get("name") or "") for row in rows]
    if any(_SE_TOKEN_RE.search(name) for name in names):
        return None
    numbers: list[int] = []
    for name in names:
        match = _ABSOLUTE_BRACKET_RE.search(name)
        if match is None:
            # Non-bracketed videos (NCED/NCOP/[18.5]-style specials) stay on
            # the planner's ordinary special path; only regular bracketed
            # episodes participate in the explicit map.
            continue
        numbers.append(int(match.group(1)))
    if not numbers:
        return None
    if sorted(numbers) != list(range(1, len(numbers) + 1)):
        return None
    total = len(numbers)
    tmdb_id = identity.get("tmdb_id")
    if isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0:
        return None
    try:
        expected = TmdbEpisodeCatalog(runner.tmdb)(
            {"tmdb_id": tmdb_id, "media_type": "tv"}
        )
    except Exception:
        return None
    if expected is None:
        return None
    seasons: dict[int, list[int]] = {}
    for season, episode_rows in expected.items():
        if isinstance(season, bool) or not isinstance(season, int) or season <= 0:
            continue
        if not isinstance(episode_rows, list):
            continue
        episodes = [
            int(row.get("episode_number"))
            for row in episode_rows
            if isinstance(row, Mapping)
            and isinstance(row.get("episode_number"), int)
            and not isinstance(row.get("episode_number"), bool)
            and int(row.get("episode_number")) > 0
        ]
        if episodes:
            seasons[season] = sorted(set(episodes))
    if not seasons:
        return None
    ordered = sorted(seasons)
    # A single season whose episode count exactly equals the source run is
    # the obvious answer: the ordinary planner with a ``season`` hint handles
    # it, and an explicit map is both unnecessary and dangerous — a later
    # pair of seasons can coincidentally sum to the same count (黑执事 S1=24
    # beside S4+S5=11+13=24) and would silently re-map S1 episodes into the
    # wrong seasons.
    if any(len(seasons[season]) == total for season in ordered):
        return None
    chosen: list[int] | None = None
    for start in ordered:
        suffix = [season for season in ordered if season >= start]
        if len(suffix) >= 2 and sum(len(seasons[s]) for s in suffix) == total:
            chosen = suffix
            break
    if chosen is None:
        return None
    mapping: dict[str, str] = {}
    absolute = 1
    for season in chosen:
        for episode in seasons[season]:
            if absolute > total:
                break
            mapping[str(absolute)] = f"S{season:02d}E{episode:02d}"
            absolute += 1
    if absolute - 1 != total:
        return None
    path = state_root / f"episode_map_{record.work_unit_id}.json"
    atomic_write_json(path, mapping, allow_nan=False)
    return str(path)


def _physical_special_episode_map_path(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    proof: SingleSeasonEpisodeProof | None,
) -> str | None:
    """Build the only safe OAD/OVA/OAV source-key map for F.

    The map uses ``SP01``…``SPN`` source keys because the Engine parser keeps
    physical ordinals in the special namespace.  The D proof has already
    established the target positive season and exact count; this function only
    carries that durable evidence into the existing planner, never bypassing
    its TMDB title/catalog validation.
    """
    if proof is None or proof.evidence_kind != _PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND:
        return None
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return None
    try:
        scoped = build_scoped_source_node(
            build_source_inventory(snapshot["rows"], snapshot["root"]),
            record.source_paths,
            boundary_key=record.boundary_key,
            display_label=record.display_label,
        )
    except (KeyError, TypeError, ValueError):
        return None
    markers, numbers, count, complete = physical_special_marker_evidence(scoped)
    if (
        not complete
        or count != proof.episode_count
        or numbers != tuple(range(1, proof.episode_count + 1))
        or not markers
    ):
        return None
    # The D proof carries the official episode tokens the release ordinals
    # were mapped onto.  For a named-arc Season 00 run those tokens are not
    # ``S00E01..S00EN`` — the arc starts wherever the parent show catalogued
    # it (``S00E08``/``S00E09``) — so the map must use the proved tokens
    # instead of guessing 1-based positions.
    proof_tokens = tuple(str(token).upper() for token in proof.episode_tokens)
    expected_local = tuple(
        f"S{proof.season:02d}E{number:02d}" for number in numbers
    )
    if proof_tokens == expected_local:
        mapping = {f"SP{number:02d}": token for number, token in zip(numbers, expected_local)}
    else:
        if len(proof_tokens) != len(numbers):
            return None
        season_prefix = f"S{proof.season:02d}E"
        for token in proof_tokens:
            if not token.startswith(season_prefix):
                return None
            episode_ordinal = token[len(season_prefix):]
            if not episode_ordinal.isdigit():
                return None
        mapping = {f"SP{number:02d}": token for number, token in zip(numbers, proof_tokens)}
    path = state_root / f"episode_map_{record.work_unit_id}.json"
    atomic_write_json(path, mapping, allow_nan=False)
    return str(path)


def _release_dash_episode_map_path(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    proof: SingleSeasonEpisodeProof | None,
) -> str | None:
    """Build the F-only ``Title - 01`` source-key map after D revalidation.

    A release title may itself contain numbers (``The 100 - 01`` or
    ``Show 2 - 01``).  The map is therefore paired with the internal planner
    gate that makes the shared release-dash parser produce source key ``01``
    before the generic title-number parser runs.  This helper never guesses:
    it reuses the exact D grammar over the B snapshot, and its caller has just
    fresh-revalidated that proof against source ownership and TMDB catalog.
    """
    if proof is None or proof.evidence_kind != _RELEASE_DASH_EPISODE_EVIDENCE_KIND:
        return None
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return None
    try:
        scoped = build_scoped_source_node(
            build_source_inventory(snapshot["rows"], snapshot["root"]),
            record.source_paths,
            boundary_key=record.boundary_key,
            display_label=record.display_label,
        )
    except (KeyError, TypeError, ValueError):
        return None
    source_ordinals = release_dash_episode_source_ordinals(scoped)
    if source_ordinals is None:
        return None
    numbers = tuple(sorted(source_ordinals.values()))
    expected = tuple(range(1, proof.episode_count + 1))
    if numbers != expected or tuple(proof.episode_tokens) != tuple(
        f"S{proof.season:02d}E{number:02d}" for number in expected
    ):
        return None
    # The keys are the parser's release ordinals, not title digits.  F sets
    # ``allow_release_dash_ordinal`` only in this exact branch, so generic
    # callers can neither create nor consume this mapping.
    mapping = {
        str(number): f"S{proof.season:02d}E{number:02d}"
        for number in numbers
    }
    if len(mapping) != proof.episode_count:
        return None
    path = state_root / f"episode_map_{record.work_unit_id}.json"
    atomic_write_json(path, mapping, allow_nan=False)
    return str(path)


def _bracketed_episode_map_path(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    proof: SingleSeasonEpisodeProof | None,
) -> str | None:
    """Build the F-only ``[01]`` source-key map after D revalidation.

    A bracketed episode run often lives beside theatrical films inside one
    ``剧场版``-labelled folder.  The smart planner's loose movie keyword must
    not hijack those proved episodes, so F plans the unit through the explicit
    episode-map path instead of the smart grouping.  The shared bracket rule
    already wins over title digits in ``extract_episode_key``, and any parser
    divergence from this strict grammar fails closed as an unmapped key.
    """
    if proof is None or proof.evidence_kind != _BRACKETED_EPISODE_EVIDENCE_KIND:
        return None
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return None
    try:
        scoped = build_scoped_source_node(
            build_source_inventory(snapshot["rows"], snapshot["root"]),
            record.source_paths,
            boundary_key=record.boundary_key,
            display_label=record.display_label,
        )
    except (KeyError, TypeError, ValueError):
        return None
    source_ordinals = bracketed_episode_source_ordinals(scoped)
    if source_ordinals is None:
        return None
    # An edition cut repeats one ordinal on its own source file, so the proved
    # run is the set of ordinals while the map still carries every file.
    numbers = tuple(sorted(set(source_ordinals.values())))
    expected = tuple(range(1, proof.episode_count + 1))
    if numbers != expected:
        return None
    proof_tokens = tuple(str(token).upper() for token in proof.episode_tokens)
    if len(proof_tokens) != len(numbers):
        return None
    # The proved token shapes are exactly three: a local run
    # ``S{season}E01…E{N}``; an overflow run whose tail lands in Season 00
    # (日在校园 1..14 = 12 regular + 2 OVA); and a named-arc Season 00
    # window that starts wherever the parent catalogued the arc
    # (命运石之门 聪明睿智的认知计算 [01]…[04] → S00E02…E05).  All three are
    # non-interleaved per-season blocks of consecutive ascending episodes
    # over the proved seasons; any other shape fails closed instead of
    # guessing a split.
    # A cumulative arc run spans the seasons D proved, not only ``proof.season``:
    # ``爱丽丝篇`` is S03E01…E24 followed by S04E01…E23.  The boundaries are the
    # proof's own output, so admitting them here adds no new inference — without
    # them the S04 block failed closed and the merge lane raised
    # "D 纯方括号集号证据无法重建 F 显式映射".
    allowed_seasons = {proof.season, 0} | {
        season for season, _count in (proof.season_boundaries or ())
    }
    block_seasons: list[int] = []
    block_episodes: list[list[int]] = []
    for token in proof_tokens:
        match = re.fullmatch(r"S0*(\d{1,3})E0*(\d{1,4})", token)
        if match is None:
            return None
        token_season = int(match.group(1))
        token_episode = int(match.group(2))
        if token_season not in allowed_seasons:
            return None
        if not block_seasons or block_seasons[-1] != token_season:
            if token_season in block_seasons:
                return None
            block_seasons.append(token_season)
            block_episodes.append([])
        block_episodes[-1].append(token_episode)
    if not block_seasons:
        return None
    if block_seasons != sorted(
        block_seasons, key=lambda season: (season != proof.season, season)
    ):
        return None
    if any(
        episodes != list(range(episodes[0], episodes[0] + len(episodes)))
        for episodes in block_episodes
    ):
        return None
    mapping = {
        str(number): token for number, token in zip(numbers, proof_tokens)
    }
    if len(mapping) != proof.episode_count:
        return None
    path = state_root / f"episode_map_{record.work_unit_id}.json"
    atomic_write_json(path, mapping, allow_nan=False)
    return str(path)


def _release_title_ordinal_episode_map_path(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    proof: SingleSeasonEpisodeProof | None,
) -> str | None:
    """Build the F-only ``Title 01`` source-key map after D revalidation."""
    if (
        proof is None
        or proof.evidence_kind != _RELEASE_TITLE_ORDINAL_EPISODE_EVIDENCE_KIND
    ):
        return None
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return None
    try:
        scoped = build_scoped_source_node(
            build_source_inventory(snapshot["rows"], snapshot["root"]),
            record.source_paths,
            boundary_key=record.boundary_key,
            display_label=record.display_label,
        )
    except (KeyError, TypeError, ValueError):
        return None
    source_ordinals = release_title_ordinal_episode_source_ordinals(scoped)
    if source_ordinals is None:
        return None
    numbers = tuple(sorted(source_ordinals.values()))
    if not numbers or numbers[0] not in (0, 1):
        return None
    # A zero-based release run (``High School DxD Hero 00`` = S04E01) keeps its
    # own source ordinals as the map keys and carries the proven +1 offset into
    # the official coordinates, so F renames from the same evidence D proved.
    offset = 1 if numbers[0] == 0 else 0
    expected = tuple(range(numbers[0], numbers[0] + proof.episode_count))
    if numbers != expected or tuple(proof.episode_tokens) != tuple(
        f"S{proof.season:02d}E{number + offset:02d}" for number in expected
    ):
        return None
    mapping = {
        str(number): f"S{proof.season:02d}E{number + offset:02d}"
        for number in numbers
    }
    if len(mapping) != proof.episode_count:
        return None
    path = state_root / f"episode_map_{record.work_unit_id}.json"
    atomic_write_json(path, mapping, allow_nan=False)
    return str(path)


def _quoted_ordinal_episode_map_path(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    proof: SingleSeasonEpisodeProof | None,
) -> str | None:
    """Build the F-only ``01「Title」`` source-key map after D revalidation.

    A Japanese disc-rip run quotes the episode title right after the leading
    ordinal.  The Engine parser already reads that ordinal natively (the
    leading quoted-ordinal rule), so F needs no parser gate — only the explicit
    episode map that pins each proved source ordinal to its official season
    coordinate, keeping a movie-keyword or title-digit detour from hijacking
    the run.
    """
    if proof is None or proof.evidence_kind != _QUOTED_ORDINAL_EPISODE_EVIDENCE_KIND:
        return None
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return None
    try:
        scoped = build_scoped_source_node(
            build_source_inventory(snapshot["rows"], snapshot["root"]),
            record.source_paths,
            boundary_key=record.boundary_key,
            display_label=record.display_label,
        )
    except (KeyError, TypeError, ValueError):
        return None
    source_ordinals = quoted_ordinal_episode_source_ordinals(scoped)
    if source_ordinals is None:
        return None
    numbers = tuple(sorted(source_ordinals.values()))
    expected = tuple(range(1, proof.episode_count + 1))
    if numbers != expected or tuple(proof.episode_tokens) != tuple(
        f"S{proof.season:02d}E{number:02d}" for number in expected
    ):
        return None
    mapping = {
        str(number): f"S{proof.season:02d}E{number:02d}"
        for number in numbers
    }
    if len(mapping) != proof.episode_count:
        return None
    path = state_root / f"episode_map_{record.work_unit_id}.json"
    atomic_write_json(path, mapping, allow_nan=False)
    return str(path)


def _revalidated_reconciliation_season(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> int | None:
    """Re-read the narrow D single-season proof before handing it to F.

    The persisted evidence is only a receipt from D, never a substitute for
    current source ownership or TMDB facts.  Any source/catalog drift stops
    before planning or a formal-library write.
    """
    raw = record.reconciliation_evidence
    if raw is None:
        return None
    stored = SingleSeasonEpisodeProof.from_dict(raw)
    if stored is None:
        raise ValueError("D 无季号季集证据记录无效；请重新执行对账")
    evidence_label = single_season_episode_evidence_label(stored.evidence_kind)
    if stored.evidence_kind == _PHYSICAL_SPECIAL_EPISODE_EVIDENCE_KIND:
        current = prove_physical_special_single_season_evidence(
            runner.alist,
            state_root,
            root_task_id,
            record,
            episode_catalog=TmdbEpisodeCatalog(runner.tmdb),
            tmdb_client=runner.tmdb,
        )
    else:
        current = prove_single_season_episode_evidence(
            runner.alist,
            state_root,
            root_task_id,
            record,
            evidence_kind=stored.evidence_kind,
            episode_catalog=TmdbEpisodeCatalog(runner.tmdb),
            tmdb_client=runner.tmdb,
        )
    if current != stored:
        raise ValueError(
            f"D {evidence_label} 季集证据已变化或无法重新核验；"
            "请保持暂停并重建边界/对账"
        )
    return current.season


def _explicit_single_scope_season(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    scopes: tuple[str, ...],
) -> int | None:
    """Return one source-directory season only when its ownership is exact.

    A planner request whose ``src_path`` is itself a season directory no longer
    has that parent segment available while parsing relative file names.  The
    bounded B/W directory marker therefore has to be carried into F explicitly.
    This helper is deliberately narrower than general season inference:

    * exactly one validated scope must equal the WorkUnit boundary;
    * the basename must contain one bounded Arabic/Chinese/decorated season;
    * any explicit ``SxxExx`` file marker must agree with that directory; and
    * an identity/declared-season claim may not contradict it.

    If any proof is missing or conflicting, return ``None`` or raise a visible
    error respectively; never fall back to an arbitrary Season 01 hint.
    """
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    if str(identity.get("media_type") or "tv") != "tv":
        return None
    if len(scopes) != 1 or not record.source_paths:
        return None
    scope = scopes[0].rstrip("/")
    boundary = str(record.boundary_key or "").rstrip("/")
    if not scope or boundary != scope:
        # A parent/root boundary may own several nested seasons.  Its basename
        # is not sufficient evidence for one request-wide season.
        return None
    season = _season_number_from_directory_name(posixpath.basename(scope))
    if season is None:
        return None

    identity_season = identity.get("season")
    if (
        identity_season is not None
        and (
            isinstance(identity_season, bool)
            or not isinstance(identity_season, int)
            or identity_season <= 0
            or identity_season != season
        )
    ):
        raise ValueError("来源目录显式季号与身份季号冲突")

    claimed = tuple(
        value
        for value in record.claimed_seasons
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
    )
    if claimed and tuple(sorted(set(claimed))) != (season,):
        raise ValueError("来源目录显式季号与声明季号冲突")

    # Reuse the persisted B/W ownership snapshot for a bounded marker check;
    # the directory itself was already fresh-proved by the caller.  Releases
    # commonly reset the *local* filename marker to S01 inside every named
    # season directory (for example ``第二季/S01E01.mkv``), so that one marker
    # is compatible with the stronger directory scope.  A different marker,
    # or mixed markers, is a source-boundary contradiction rather than a reason
    # to guess which coordinate should win.
    file_seasons: set[int] = set()
    for row in _unit_video_rows(state_root, root_task_id, record):
        name = str(row.get("name") or "")
        for match in _SEASON_EPISODE_RE.finditer(name):
            season_number = int(match.group(1))
            if season_number == 0:
                # ``S00`` is the specials bucket, not a competing main season.
                # A ``S01`` release beside its specials is one season, not a
                # season-boundary contradiction.
                continue
            file_seasons.add(season_number)
    if len(file_seasons) > 1:
        raise ValueError("来源文件显式季号与目录季号冲突")
    if file_seasons:
        file_season = next(iter(file_seasons))
        local_reset = season > 1 and file_season == 1
        if file_season != season and not local_reset:
            raise ValueError("来源文件显式季号与目录季号冲突")
    return season


def _unit_owns_tv_root_scope(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> bool:
    """Prove that one unit owns the whole TV ingress, including S00.

    The ordinary proof is an exact single-work boundary equal to the RootJob
    ingress.  A second narrow proof covers a multi-work root whose sole
    multi-season TV identity is the main series selected by the same container
    rule used by F.  Only one aggregated main record may take that branch;
    single-season siblings and split season records cannot invent specials.
    """
    try:
        job = runner.get_job(root_task_id)
        ingress = str(runner._job_ingress_source(job)).rstrip("/")  # noqa: SLF001
    except Exception:
        return False
    if not ingress:
        return False
    try:
        records = load_work_unit_records(state_root, root_task_id)
    except Exception:
        return False
    persisted = [
        item for item in records if item.work_unit_id == record.work_unit_id
    ]
    if len(persisted) != 1:
        return False
    persisted_record = persisted[0]
    # The caller may hold a stale ``replace()`` instance after another phase
    # updated the ledger.  Scope ownership is granted only to the exact
    # persisted structural/identity row, never to a same-id record with
    # mutated paths, role, claims, or confirmation state.
    for field in (
        "root_task_id",
        "boundary_key",
        "source_paths",
        "source_revision",
        "role",
        "display_label",
        "claimed_seasons",
        "requires_content_expansion",
        "identity_status",
        "identity",
        "reconciliation_evidence",
    ):
        if getattr(record, field) != getattr(persisted_record, field):
            return False
    record = persisted_record
    if str((record.identity or {}).get("media_type") or "") != "tv":
        return False

    boundary = str(record.boundary_key).rstrip("/")
    if (
        record.role == "single_work"
        and len(record.source_paths) == 1
        and boundary == str(record.source_paths[0]).rstrip("/")
        and boundary == ingress
        and _season_number_from_directory_name(posixpath.basename(boundary)) is None
    ):
        # An exact-root unit owns specials only when B/W durably proves it is
        # the sole WorkUnit in that ingress.  A sibling record means the root
        # scope overlaps another owner, so walking it for J would be unsafe.
        return len(records) == 1

    main_tmdb = _main_tv_identity(records)
    identity = record.identity if isinstance(record.identity, Mapping) else {}
    if main_tmdb is None or identity.get("tmdb_id") != main_tmdb:
        return False
    main_record = _main_tv_record(records)
    if main_record is None or main_record.work_unit_id != record.work_unit_id:
        return False
    proved_seasons = _record_proved_positive_seasons(record)
    if len(proved_seasons) <= 1:
        return False
    scoped_seasons: set[int] = set()
    for path in record.source_paths:
        scope = str(path).rstrip("/")
        if not scope.startswith(ingress + "/"):
            return False
        season = _season_number_from_directory_name(posixpath.basename(scope))
        if season is not None and season > 0:
            scoped_seasons.add(season)
            continue
        # B/W may attach a clearly-labelled SP/OVA/Extras directory to the
        # same aggregated TV unit.  It is auxiliary evidence, not a positive
        # season scope; allow it only under the explicit special marker.
        if not _SPECIAL_RELATION_MARKER_RE.search(posixpath.basename(scope)):
            return False
    # A claimed range alone is not a physical ownership proof.  Require at
    # least two distinct B/W season-directory scopes, and require each scope
    # to agree with the durable season facts used by the container rule.
    return len(scoped_seasons) > 1 and scoped_seasons.issubset(proved_seasons)


def _request_for_unit(
    runner: SimpleEngineRunner,
    record: WorkUnitRecord,
    root_task_id: str,
    state_root: Path,
    *,
    parent_override: str | None = None,
    target_scope_override: str | None = None,
) -> EngineRequest:
    root_job = runner._read(root_task_id)  # noqa: SLF001 - ledger composition
    if root_job.target_shelf is None:
        raise ValueError("根任务尚未选择目标货架，无法规划作品单元")
    shelf_root = target_root_for_shelf(runner.library_root, root_job.target_shelf)
    identity = record.identity or {}
    media_type = str(identity.get("media_type") or "tv")
    tmdb_id = identity.get("tmdb_id")
    if not isinstance(tmdb_id, int) or isinstance(tmdb_id, bool) or tmdb_id <= 0:
        raise ValueError("单元身份缺少有效 tmdb_id")
    scopes = _record_source_scopes(runner, root_job, record)
    # Detect a consumed-source continuation before any scope-shape gate: the
    # interrupted write may have emptied or removed the source directories,
    # so ``_scope_kind_map`` itself can fail on a legitimate continuation.
    continuation_manifest: tuple[Mapping[str, object], ...] | None = None
    continuation_scope_kinds: Mapping[str, str] | None = None
    try:
        scope_kinds = _scope_kind_map(runner, scopes)
    except ValueError:
        continuation_scope_kinds = _snapshot_scope_kinds(
            scopes, state_root=state_root, root_task_id=root_task_id,
        )
        if continuation_scope_kinds is None:
            raise
        consumed = _consumed_source_snapshot_rows(
            runner, state_root, root_task_id, record,
            continuation_scope_kinds,
            list(_fresh_scope_rows(runner, scopes)),
        )
        if consumed is None:
            raise
        continuation_manifest = consumed
        scope_kinds = continuation_scope_kinds
    source_path = scopes[0]
    # The legacy planners accept a directory ``src_path`` while the internal
    # manifest pins the exact file.  Keep that public planner root at the
    # file's parent for a one-file WorkUnit; the scope/manifest gates below
    # still reject every sibling object.
    if len(scopes) == 1 and scope_kinds[scopes[0]] == "file":
        source_path = posixpath.dirname(scopes[0]) or "/"
    parent_path = parent_override or shelf_root
    payload: dict[str, object] = {
        "source_path": source_path,
        "parent_path": parent_path,
        "media_type": media_type,
        "tmdb_id": tmdb_id,
    }
    season = identity.get("season")
    # A named-season window (``犬夜叉完结篇`` = the parent's Season 2 that
    # officially aired inside the boundary years) is a C-proven identity
    # fact recorded in the decision trace.  It carries the same authority as
    # an identity season: the smart planner cannot infer it from the
    # release-local ordinals alone.
    window_season = None
    decision_trace = identity.get("decision_trace")
    if isinstance(decision_trace, dict):
        window_candidate = decision_trace.get("season_window_season")
        if (
            isinstance(window_candidate, int)
            and not isinstance(window_candidate, bool)
            and window_candidate >= 0
        ):
            window_season = window_candidate
    scope_season = _explicit_single_scope_season(
        state_root, root_task_id, record, scopes,
    )
    # A consumed-source continuation must short-circuit every fresh-source
    # gate below: the D proof revalidator and the exact manifest both read
    # the (now consumed) provider source and would fail before the planner
    # could rebuild the continuation plan.
    proof = SingleSeasonEpisodeProof.from_dict(record.reconciliation_evidence)
    if continuation_manifest is None:
        try:
            proof_season = _revalidated_reconciliation_season(
                runner, state_root, root_task_id, record,
            )
        except ValueError:
            consumed = _consumed_source_snapshot_rows(
                runner, state_root, root_task_id, record,
                scope_kinds,
                list(_fresh_scope_rows(runner, scopes)),
            )
            if consumed is None:
                raise
            continuation_manifest = consumed
            proof_season = None
    else:
        proof_season = None
    if continuation_manifest is not None and proof_season is None:
        # The interrupted write's receipt was planned from the stored D
        # verdict, and the fresh revalidator cannot recompute that verdict
        # from the consumed source.  Keep the durable stored season so the
        # rebuilt plan targets the same episodes: without it the request
        # would fall to the historical implicit Season 01 default and plan
        # a different season than the one the interrupted write partly
        # wrote.  The tmdb identity guard mirrors the equality check the
        # fresh revalidator performs between record and recomputed proof.
        proof_season = (
            proof.season
            if proof is not None
            and proof.tmdb_id == tmdb_id
            and proof.season > 0
            else None
        )
    is_release_dash_proof = (
        proof is not None
        and proof.evidence_kind == _RELEASE_DASH_EPISODE_EVIDENCE_KIND
    )
    is_release_title_ordinal_proof = (
        proof is not None
        and proof.evidence_kind == _RELEASE_TITLE_ORDINAL_EPISODE_EVIDENCE_KIND
    )
    is_bracketed_proof = (
        proof is not None
        and proof.evidence_kind == _BRACKETED_EPISODE_EVIDENCE_KIND
    )
    is_quoted_ordinal_proof = (
        proof is not None
        and proof.evidence_kind == _QUOTED_ORDINAL_EPISODE_EVIDENCE_KIND
    )
    if proof_season is not None:
        if (
            isinstance(season, int)
            and not isinstance(season, bool)
            and season > 0
            and season != proof_season
        ):
            raise ValueError("D 无季号集号证据季号与身份季号冲突")
        if window_season is not None and window_season != proof_season:
            raise ValueError("D 季集证据与身份命名季窗口冲突")
        if scope_season is not None and scope_season != proof_season:
            raise ValueError("D 季集证据与来源目录显式季号冲突")
        # This is an explicit, freshly revalidated F request field—not
        # EngineRequest's historical implicit Season 01 default.
        payload["season"] = proof_season
    elif isinstance(season, int) and not isinstance(season, bool) and season > 0:
        if window_season is not None and window_season != season:
            raise ValueError("身份季号与身份命名季窗口冲突")
        if scope_season is not None and scope_season != season:
            raise ValueError("身份季号与来源目录显式季号冲突")
        payload["season"] = season
    elif window_season is not None:
        if scope_season is not None and scope_season != window_season:
            raise ValueError("来源目录显式季号与身份命名季窗口冲突")
        payload["season"] = window_season
    elif scope_season is not None:
        payload["season"] = scope_season
    elif (
        len(record.claimed_seasons) == 1
        and isinstance(record.claimed_seasons[0], int)
        and not isinstance(record.claimed_seasons[0], bool)
        and record.claimed_seasons[0] > 0
    ):
        # A one-season B/W claim is already an explicit boundary fact; carry
        # it just like a single directory marker.  Multi-season claims remain
        # source_declared_seasons only and never collapse to one season.
        payload["season"] = record.claimed_seasons[0]
    request = EngineRequest.from_mapping(payload)
    if record.claimed_seasons:
        request = replace(
            request,
            source_declared_seasons=tuple(
                season
                for season in record.claimed_seasons
                if isinstance(season, int)
                and not isinstance(season, bool)
                and season > 0
            ),
        )
    target_scope = _safe_remote_path(
        target_scope_override or request.parent_path,
        field="WorkUnit target_scope_root",
        allow_root=False,
    )
    if not (
        target_scope == request.parent_path
        or target_scope.startswith(request.parent_path + "/")
    ):
        raise ValueError("WorkUnit 目标范围不属于 Planner 父目录")
    request = replace(request, target_scope_root=target_scope)
    # A consumed-source continuation bypasses every fresh manifest gate: the
    # provider source is gone by construction, and the continuation manifest
    # (consumed B-snapshot objects) is already pinned on the request below.
    if continuation_manifest is not None:
        request = replace(
            request,
            source_files=continuation_manifest,
            source_scope_paths=scopes,
        )
        return request
    # Multi-scope WorkUnits already hand an exact fresh manifest to F.  A
    # D/F-only grammar (release-dash, title-ordinal, bracketed) needs that
    # same pin even for one scope: otherwise an object added after the proof
    # could be discovered by the planner and consume a narrowly enabled
    # parser without being proven.
    fresh_scopes: tuple[str, ...] | None = None
    manifest: tuple[Mapping[str, object], ...] | None = None
    if load_source_manifest(state_root, root_task_id) is not None:
        fresh_scopes, manifest = _fresh_exact_source_manifest(
            runner, state_root, root_task_id, record, root_job,
        )
    elif (
        len(scopes) > 1
        or any(kind == "file" for kind in scope_kinds.values())
        or is_release_dash_proof
        or is_release_title_ordinal_proof
        or is_bracketed_proof
        or is_quoted_ordinal_proof
    ):
        fresh_scopes, manifest = _fresh_scoped_source_files(
            runner,
            state_root,
            root_task_id,
            record,
            root_job,
            require_single_scope_manifest=(
                is_release_dash_proof
                or is_release_title_ordinal_proof
                or is_bracketed_proof
                or is_quoted_ordinal_proof
            ),
        )
    if fresh_scopes is not None and manifest is not None:
        source_path = request.source_path
        if len(fresh_scopes) > 1:
            source_path = str(runner._job_ingress_source(root_job)).rstrip("/")  # noqa: SLF001
        elif scope_kinds.get(fresh_scopes[0]) == "file":
            source_path = posixpath.dirname(fresh_scopes[0]) or "/"
        request = replace(
            request,
            source_path=source_path,
            source_files=manifest,
            source_scope_paths=fresh_scopes,
        )
    # A freshly revalidated release-dash proof gets both a source-key map and
    # the F-only parser gate.  This must happen as one branch: the map alone
    # cannot help when a title digit (``The 100 - 01``) would otherwise become
    # the parser's source key before map lookup.
    map_path = _release_dash_episode_map_path(
        state_root, root_task_id, record, proof,
    )
    if proof is not None and proof.evidence_kind == _RELEASE_DASH_EPISODE_EVIDENCE_KIND:
        if proof_season is None or map_path is None:
            raise ValueError(
                "D 发行组短横线集号证据无法重建 F 显式映射；"
                "请保持暂停并重建边界/对账"
            )
        request = replace(request, allow_release_dash_ordinal=True)
    if map_path is None:
        map_path = _release_title_ordinal_episode_map_path(
            state_root, root_task_id, record, proof,
        )
    if is_release_title_ordinal_proof:
        if proof_season is None or map_path is None:
            raise ValueError(
                "D 同标题裸序号集号证据无法重建 F 显式映射；"
                "请保持暂停并重建边界/对账"
            )
        request = replace(request, allow_release_title_ordinal=True)
    if map_path is None:
        map_path = _bracketed_episode_map_path(
            state_root, root_task_id, record, proof,
        )
    if is_bracketed_proof:
        if proof_season is None or map_path is None:
            raise ValueError(
                "D 纯方括号集号证据无法重建 F 显式映射；"
                "请保持暂停并重建边界/对账"
            )
    if map_path is None:
        map_path = _quoted_ordinal_episode_map_path(
            state_root, root_task_id, record, proof,
        )
    if is_quoted_ordinal_proof:
        if proof_season is None or map_path is None:
            raise ValueError(
                "D 引号集名序号集号证据无法重建 F 显式映射；"
                "请保持暂停并重建边界/对账"
            )
    if map_path is None:
        map_path = _physical_special_episode_map_path(
            state_root, root_task_id, record, proof,
        )
    if map_path is None:
        map_path = _multi_season_absolute_map_path(
            runner, state_root, root_task_id, record,
        )
    if map_path is not None:
        request = replace(request, episode_map_path=map_path)
    return request


_LEGACY_PLANNER_MISSING_SEASON_LABEL_RE = re.compile(
    r"^Season (?P<season>[0-9]{2}) (?P<season_name>\S(?:.*\S)?)$"
)


def _strict_planner_missing_seasons(
    executed_plan: Mapping[str, Any],
) -> dict[int, int]:
    """Return the exact missing-season facts carried by one executed plan.

    ``scan_report.resource_gaps`` is planner evidence, not an alternate gap
    ledger.  New plans carry a numeric ``season`` field.  For the already
    executed carriers produced before that field existed, accept only the
    exact canonical label emitted by ``_tv_season_resource_gaps`` together
    with its complete accompanying schema.  Anything looser would turn a
    human display label into a new source of episode ownership.
    """
    scan_report = executed_plan.get("scan_report")
    if scan_report is None:
        return {}
    if not isinstance(scan_report, Mapping):
        raise GapDiscoveryAttention("已执行计划的缺口报告格式无效")
    raw_gaps = scan_report.get("resource_gaps")
    if raw_gaps is None:
        return {}
    if not isinstance(raw_gaps, list):
        raise GapDiscoveryAttention("已执行计划的缺口报告格式无效")

    output: dict[int, int] = {}
    for raw_gap in raw_gaps:
        if not isinstance(raw_gap, Mapping) or raw_gap.get("kind") != "missing_season":
            continue
        label = raw_gap.get("label")
        season_name = raw_gap.get("season_name")
        expected_count = raw_gap.get("expected_episode_count")
        reason = raw_gap.get("reason")
        if (
            not isinstance(label, str)
            or not label
            or not isinstance(season_name, str)
            or not season_name
            or season_name != season_name.strip()
            or not isinstance(reason, str)
            or not reason.strip()
            or isinstance(expected_count, bool)
            or not isinstance(expected_count, int)
            or expected_count <= 0
            or raw_gap.get("files") != []
        ):
            raise GapDiscoveryAttention("已执行计划的缺季记录缺少严格坐标")

        raw_season = raw_gap.get("season")
        if raw_season is None:
            # Historical carrier compatibility is deliberately narrow: only
            # the exact old generator's fixed label is usable as a fallback.
            match = _LEGACY_PLANNER_MISSING_SEASON_LABEL_RE.fullmatch(label)
            if match is None or match.group("season_name") != season_name:
                raise GapDiscoveryAttention("已执行计划的缺季记录缺少严格坐标")
            season = int(match.group("season"))
        elif isinstance(raw_season, int) and not isinstance(raw_season, bool):
            season = raw_season
        else:
            raise GapDiscoveryAttention("已执行计划的缺季记录缺少严格坐标")
        if season <= 0 or label != f"Season {season:02d} {season_name}":
            raise GapDiscoveryAttention("已执行计划的缺季记录缺少严格坐标")

        prior = output.get(season)
        if prior is not None and prior != expected_count:
            raise GapDiscoveryAttention("已执行计划的缺季记录彼此冲突")
        output[season] = expected_count
    return output


def _strict_catalog_season_episodes(
    rows: object,
    *,
    expected_count: int,
) -> list[int]:
    """Prove the currently published prefix of one planner missing season.

    ``expected_episode_count`` in a planner report comes from season metadata,
    which may include future episodes.  ``TmdbEpisodeCatalog`` deliberately
    exposes only rows published today, so J must not turn a normal in-progress
    season into attention merely because that prefix is shorter than the
    metadata total.  We still require a non-empty, unique, gap-free ``1..N``
    prefix no longer than that total before registering any coordinate.
    """
    if not isinstance(rows, list):
        raise GapDiscoveryAttention("TMDB 无法证明已执行计划的缺季坐标")
    episodes: list[int] = []
    for row in rows:
        number = row.get("episode_number") if isinstance(row, Mapping) else None
        if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
            raise GapDiscoveryAttention("TMDB 无法证明已执行计划的缺季坐标")
        episodes.append(number)
    published_count = len(episodes)
    expected = list(range(1, published_count + 1))
    if (
        published_count <= 0
        or published_count > expected_count
        or sorted(episodes) != expected
    ):
        raise GapDiscoveryAttention("TMDB 与已执行计划的缺季坐标不一致")
    return expected


def _planner_missing_season_is_materialized(
    ledger: Sequence[Any],
    record: WorkUnitRecord,
    tmdb_id: int,
    season: int,
) -> bool:
    """Whether J has ever durably projected this planner season into gaps.

    The executed carrier's expected count is a planning-time season total,
    not a durable promise about how many episodes were published that day.
    One exact ledger row is therefore enough to prove the bridge ran.  Future
    episode publication belongs to a fresh ordinary audit, never repeated
    reopening of this historical carrier.
    """
    return any(
        gap.work_unit_id == record.work_unit_id
        and gap.kind == "missing_episode"
        and gap.media_type == "tv"
        and gap.tmdb_id == tmdb_id
        and gap.season == season
        for gap in ledger
    )


def _register_unit_episode_gaps(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    executed_plan: Mapping[str, Any],
) -> list[Any]:
    """Register precise per-episode gaps after a successful write (J step).

    Expected coordinates come from the official TMDB episode catalog, but ONLY
    for the seasons this unit actually owns: registering the whole-series
    catalog from one season unit would turn every sibling unit's files into
    phantom gaps.  Ownership is derived from the executed plan's own files
    (video tokens and bare ``Sxx`` season rows) plus the durable identity
    season.  When the plan does not enumerate videos (whole-directory moves),
    actual coverage falls back to the B snapshot rows.  Seasons whose written
    coverage cannot be proven are dropped (fail closed): the J step never
    invents a missing coordinate it cannot verify.
    """
    identity = record.identity or {}
    if str(identity.get("media_type")) != "tv":
        return []
    tmdb_id = identity.get("tmdb_id")
    if not isinstance(tmdb_id, int) or isinstance(tmdb_id, bool) or tmdb_id <= 0:
        return []
    planner_missing_seasons = _strict_planner_missing_seasons(executed_plan)

    plan_files = [
        item for item in (executed_plan.get("files") or [])
        if isinstance(item, Mapping)
    ]
    owned: set[int] = set()
    actual: list[str] = []
    has_video_row = False
    source_has_video = False
    for item in plan_files:
        name = str(item.get("final_name") or "")
        if item.get("media_kind") == "video":
            has_video_row = True
            tokens = list(audit_episode_tokens(name))
            actual.extend(f"S{season:02d}E{episode:02d}" for season, episode in tokens)
            owned.update(season for season, _ in tokens)
            continue
        match = _BARE_SEASON_RE.match(name)
        if match:
            owned.add(int(match.group(1)))
    season_hint = identity.get("season")
    if isinstance(season_hint, int) and not isinstance(season_hint, bool) and season_hint > 0:
        owned.add(season_hint)
    if not has_video_row:
        # Whole-directory move plans do not enumerate videos; the B snapshot
        # is the durable record of what this unit actually carried.
        source_rows = _unit_video_rows(
            state_root, root_task_id, record, strict=True,
        )
        source_has_video = bool(source_rows)
        for row in source_rows:
            name = str(row.get("name") or "")
            for season, episode in audit_episode_tokens(name):
                actual.append(f"S{season:02d}E{episode:02d}")
                owned.add(season)
    else:
        source_has_video = True
    if record.reconciliation_outcome == "merge_existing":
        # E3 has an existing work root whose already-present episode tokens
        # are part of the post-write truth.  Never create gaps from merely
        # the new incoming fragment.
        actual.extend(_fresh_merged_target_episode_tokens(runner, executed_plan))
    # Only register seasons whose coverage is provable: a season with zero
    # parseable files is unverified, and registering it would recreate the
    # phantom-gap failure (library-present files reported as missing).
    verified_seasons: set[int] = set()
    for token in actual:
        coordinate = parse_gap_token(token)
        if coordinate is not None:
            verified_seasons.add(coordinate[0])
    declared = {
        season for season in record.claimed_seasons
        if isinstance(season, int) and not isinstance(season, bool) and season > 0
    }
    # A verified cohort may contain an intentionally empty declared season.
    # Keep that B/W fact so J can record exact official gaps instead of
    # silently erasing the season from the result.
    owned = (owned & verified_seasons) | declared
    # A root-scoped WorkUnit owns the complete TV boundary, including the
    # provider's published Season 00.  This is deliberately narrower than
    # ``role == single_work``: the durable B/W scope must be exactly the
    # RootJob ingress, with no explicit season-directory marker.  Season
    # sub-units (and sibling units in a container) therefore retain the
    # 470537d fail-closed ownership rule and cannot invent S00 gaps.
    owns_tv_root = _unit_owns_tv_root_scope(
        runner, state_root, root_task_id, record,
    )
    if owns_tv_root and record.media_context == "tv":
        owned.add(0)
    if not owned and not planner_missing_seasons:
        if source_has_video:
            raise GapDiscoveryAttention(
                "写后视频无法证明季集坐标，无法核对缺口"
            )
        return []

    try:
        catalog = TmdbEpisodeCatalog(runner.tmdb)
        expected = catalog({"tmdb_id": tmdb_id, "media_type": "tv"})
    except Exception as exc:
        raise GapDiscoveryAttention("TMDB 季集目录查询失败，无法核对写后缺口") from exc
    if expected is None:
        raise GapDiscoveryAttention("TMDB 季集目录不可用，无法核对写后缺口")
    expected_by_season: dict[int, list[int]] = {}
    for season, rows in expected.items():
        if season not in owned:
            continue
        if not isinstance(season, int) or isinstance(season, bool) or not isinstance(rows, list):
            continue
        episodes = [
            int(row.get("episode_number"))
            for row in rows
            if isinstance(row, Mapping)
            and isinstance(row.get("episode_number"), int)
            and not isinstance(row.get("episode_number"), bool)
            and int(row.get("episode_number")) > 0
        ]
        if episodes:
            expected_by_season[season] = episodes
    # The planner has already proven that these positive seasons contain no
    # source or formal-library video.  Re-read their official catalog now and
    # materialize only its exact published SxxEyy coordinates in the normal
    # episode ledger.  Never manufacture a season-level second ledger row.
    for season, expected_count in planner_missing_seasons.items():
        if season in verified_seasons:
            raise GapDiscoveryAttention("已执行计划的缺季与已写季集冲突")
        expected_by_season[season] = _strict_catalog_season_episodes(
            expected.get(season),
            expected_count=expected_count,
        )
    if not expected_by_season:
        return []
    try:
        return discover_episode_gaps(
            state_root,
            root_task_id,
            record.work_unit_id,
            media_type="tv",
            tmdb_id=tmdb_id,
            expected_by_season=expected_by_season,
            actual_tokens=actual,
        )
    except Exception as exc:
        raise GapLedgerPersistenceError("缺口账本登记或写后回读失败") from exc


def _complete_unit_episode_gap_registration(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    executed_plan: Mapping[str, Any],
    *,
    force: bool = False,
) -> WorkUnitRecord:
    """Run J once and preserve any uncertainty without replaying G/H.

    Media has already been formally written and read back when this helper is
    called.  A catalog/evidence gap therefore becomes operator attention,
    while a durable-ledger failure remains a real technical failure.  Both are
    recorded on the WorkUnit so a later explicit retry can resume J from the
    executed carrier instead of planning or moving the media again.
    """
    # Normal pipeline retries preserve a successfully registered J result:
    # they must not make an already-completed sibling depend on a later TMDB
    # read.  The dedicated completed-carrier repair passes ``force=True``
    # only after proving a strict planner season fact is absent from ledger.
    if record.gap_status == "registered" and not force:
        return record
    try:
        _register_unit_episode_gaps(
            runner, state_root, root_task_id, record, executed_plan,
        )
    except GapDiscoveryAttention as exc:
        detail = redact_error(exc)
        return replace(
            record,
            gap_status="attention",
            gap_detail=detail,
            attention=f"写后缺口无法核对，需要确认: {detail}",
            updated_at=_now(),
        )
    except Exception as exc:
        # ``discover_episode_gaps`` has already completed its own strict
        # local readback before returning.  Any exception here is a true
        # J technical fault, not an accepted zero-gap result.
        return replace(
            record,
            gap_status="failed",
            gap_detail=redact_error(exc),
            attention=None,
            updated_at=_now(),
        )
    return replace(
        record,
        gap_status="registered",
        gap_detail=None,
        attention=None,
        updated_at=_now(),
    )


def rereview_executed_unit_gaps(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> list[WorkUnitRecord]:
    """Re-run J only for this root's already-executed internal carriers.

    This is intentionally narrower than ``execute_new_work_units``: it never
    calls the planner or writer.  It exists for an operator retry of a root
    completed before a new, generic J proof became available.
    """
    records = load_work_unit_records(state_root, root_task_id)
    ledger = load_gap_ledger(state_root, root_task_id)
    updated: list[WorkUnitRecord] = []
    changed = False

    def paused() -> bool:
        if not callable(pause_requested):
            return False
        try:
            return bool(pause_requested())
        except Exception:
            return True

    for record in records:
        if paused():
            raise EnginePauseRequested("根任务暂停已在缺口重审边界生效")
        revised = record
        if record.reconciliation_outcome == "new_work" and record.writer_job_id:
            try:
                carrier = runner.get_job(record.writer_job_id)
            except Exception:
                carrier = None
            if (
                carrier is not None
                and carrier.phase == "executed"
                and _is_owned_internal_carrier(carrier, root_task_id)
            ):
                identity = record.identity if isinstance(record.identity, Mapping) else {}
                tmdb_id = identity.get("tmdb_id")
                try:
                    planned = _strict_planner_missing_seasons(carrier.plan)
                except GapDiscoveryAttention:
                    planned = {}
                if (
                    str(identity.get("media_type") or "") == "tv"
                    and isinstance(tmdb_id, int)
                    and not isinstance(tmdb_id, bool)
                    and tmdb_id > 0
                    and any(
                        not _planner_missing_season_is_materialized(
                            ledger, record, tmdb_id, season,
                        )
                        for season in planned
                    )
                ):
                    revised = _complete_unit_episode_gap_registration(
                        runner,
                        state_root,
                        root_task_id,
                        record,
                        carrier.plan,
                        force=True,
                    )
                    # A root may have more than one strict historical
                    # carrier.  Later records must see this J write and stay
                    # untouched once their own season is materialized.
                    ledger = load_gap_ledger(state_root, root_task_id)
        changed = changed or revised.as_dict() != record.as_dict()
        updated.append(revised)
    if changed:
        save_work_unit_records(state_root, root_task_id, updated)
    return updated


def has_unmaterialized_planner_season_gaps(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
) -> bool:
    """Whether one executed carrier has a strict missing-season fact J lacks.

    The check is local-state only.  It does not query AList/TMDB, does not
    mutate a ledger, and deliberately refuses malformed historical labels.
    """
    try:
        records = load_work_unit_records(state_root, root_task_id)
        ledger = load_gap_ledger(state_root, root_task_id)
    except Exception:
        return False
    for record in records:
        identity = record.identity if isinstance(record.identity, Mapping) else {}
        tmdb_id = identity.get("tmdb_id")
        if (
            record.reconciliation_outcome != "new_work"
            or not record.writer_job_id
            or str(identity.get("media_type") or "") != "tv"
            or isinstance(tmdb_id, bool)
            or not isinstance(tmdb_id, int)
            or tmdb_id <= 0
        ):
            continue
        try:
            carrier = runner.get_job(record.writer_job_id)
        except Exception:
            continue
        if (
            carrier.phase != "executed"
            or not _is_owned_internal_carrier(carrier, root_task_id)
        ):
            continue
        try:
            planned = _strict_planner_missing_seasons(carrier.plan)
        except GapDiscoveryAttention:
            # An inexact legacy report is not enough authority to reopen a
            # completed root.  It remains a no-op until a normal replanning
            # path produces a structured fact.
            continue
        if not planned:
            continue
        for season in planned:
            if not _planner_missing_season_is_materialized(
                ledger, record, tmdb_id, season,
            ):
                return True
    return False


def execute_new_work_units(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> list[WorkAcceptanceResult]:
    """Plan + write every confirmed ``new_work`` unit of one root task.

    - F: each unit builds one EngineRequest and goes through the existing
      planner (the internal EngineJob carrier);
    - G: the single writer executes the plan under its worker lock;
    - H: the executed readback is wrapped into a WorkAcceptanceResult.
    Already-executed units are skipped on retry (``writer_job_id`` persists),
    and a failed unit keeps its ``writer_job_id`` unset so the next run
    retries it without creating a second carrier.
    """
    records = load_work_unit_records(state_root, root_task_id)
    root_job = runner.get_job(root_task_id)
    ordered, container_parent, main_tmdb = _container_plan(
        runner, root_job, records,
    )
    layout_targets = _container_layout_targets(runner, root_job, records)
    requires_main_parent = main_tmdb is not None and any(
        record.reconciliation_outcome == "new_work"
        and not (
            str((record.identity or {}).get("media_type") or "") == "tv"
            and (record.identity or {}).get("tmdb_id") == main_tmdb
        )
        for record in ordered
    )
    main_target_root: str | None = None
    if main_tmdb is not None and requires_main_parent:
        # A D-locked root wins over any carrier: it is the established work
        # root and may be on a shelf different from this root's new-work
        # authorization.  Otherwise reuse only an *executed*, fresh-verified
        # main carrier.  A planned/failed carrier is never a safe parent.
        for record in ordered:
            identity = record.identity or {}
            if not (
                str(identity.get("media_type") or "") == "tv"
                and identity.get("tmdb_id") == main_tmdb
            ):
                continue
            if record.reconciliation_outcome in {
                "duplicate_complete", "existing_gap", "merge_existing",
            }:
                main_target_root = _validated_matched_work_root(runner, record)
            if main_target_root is None and record.writer_job_id:
                try:
                    carrier = runner.get_job(record.writer_job_id)
                except Exception:
                    # A carrier lookup failure is not evidence that a sibling
                    # may use a newly selected shelf.  The main record will be
                    # recovered/failed in its ordinary turn below.
                    carrier = None
                if carrier is not None and carrier.phase == "executed":
                    # Do not catch this validation failure: silently falling
                    # back to another shelf would split the container.
                    main_target_root = _validated_formal_work_root(
                        runner,
                        carrier.plan.get("target_root"),
                        field="已执行主单元目标根",
                    )
            if main_target_root is not None:
                break
    results: list[WorkAcceptanceResult] = []
    updated: list[WorkUnitRecord] = []
    changed = False

    def paused() -> bool:
        if not callable(pause_requested):
            return False
        try:
            return bool(pause_requested())
        except Exception:
            # A root scope that cannot be checked must never authorize a
            # formal-library write.
            return True

    def persist_updates() -> None:
        """Preserve completed-unit facts without dropping untouched rows."""
        by_id = {item.work_unit_id: item for item in updated}
        save_work_unit_records(
            state_root,
            root_task_id,
            [by_id.get(record.work_unit_id, record) for record in records],
        )

    def persist_acceptance(*, retain_existing: bool) -> None:
        if not retain_existing:
            save_work_acceptance(state_root, root_task_id, results)
            return
        merged = {
            item.work_unit_id: item
            for item in load_work_acceptance(state_root, root_task_id)
        }
        merged.update({item.work_unit_id: item for item in results})
        save_work_acceptance(state_root, root_task_id, list(merged.values()))

    def accepted(record: WorkUnitRecord, carrier: EngineJob) -> WorkAcceptanceResult:
        return WorkAcceptanceResult(
            work_unit_id=record.work_unit_id,
            outcome="accepted",
            writer_job_id=carrier.id,
            phase=carrier.phase,
            target_root=str((carrier.plan.get("target_root")) or ""),
            planned_files=len(carrier.plan.get("files") or []),
            error=carrier.error,
            recorded_at=_now(),
        )

    paused_during_run = False
    # Actual executed TV roots supersede the deterministic desired title when
    # D locked an existing work root with a historical name.  Children use
    # this map only after the parent has been fresh-proved by the same carrier.
    executed_tv_roots: dict[int, str] = {}
    needed_parent_tmdbs = {
        value.get("parent_tmdb_id")
        for value in layout_targets.values()
        if isinstance(value, Mapping)
        and value.get("relation") == "nested_special"
        and isinstance(value.get("parent_tmdb_id"), int)
    }
    for candidate in ordered:
        candidate_identity = candidate.identity if isinstance(candidate.identity, Mapping) else {}
        candidate_tmdb = candidate_identity.get("tmdb_id")
        if (
            str(candidate_identity.get("media_type") or "") != "tv"
            or isinstance(candidate_tmdb, bool)
            or not isinstance(candidate_tmdb, int)
            or candidate_tmdb <= 0
            or candidate_tmdb not in needed_parent_tmdbs
            # A nested special's own root sits below its parent's work root;
            # seeding it as the family root would chain later same-identity
            # siblings under that special instead of under the parent show.
            or layout_targets.get(candidate.work_unit_id, {}).get("relation")
            == "nested_special"
        ):
            continue
        root: str | None = None
        if candidate.reconciliation_outcome in {
            "duplicate_complete", "existing_gap", "merge_existing",
        }:
            root = _validated_matched_work_root(runner, candidate)
        if root is None and candidate.writer_job_id:
            try:
                carrier = runner.get_job(candidate.writer_job_id)
            except Exception:
                carrier = None
            if carrier is not None and carrier.phase == "executed":
                root = _validated_formal_work_root(
                    runner,
                    carrier.plan.get("target_root"),
                    field="已执行 TV 父单元目标根",
                )
        if root is not None:
            executed_tv_roots[candidate_tmdb] = root
    for record in ordered:
        if paused():
            paused_during_run = True
            break
        identity = record.identity or {}
        layout = layout_targets.get(record.work_unit_id, {})
        if layout.get("uncertain") is True:
            detail = (
                "特别篇/外传无法依据 TMDB 正式标题与别名唯一确定父剧，"
                "已停在层级证据不足"
            )
            blocked = replace(
                record,
                attention=detail,
                gap_status="attention",
                gap_detail=detail,
                updated_at=_now(),
            )
            changed = changed or blocked.as_dict() != record.as_dict()
            updated.append(blocked)
            results.append(WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="skipped",
                writer_job_id=record.writer_job_id,
                phase="placement_uncertain",
                target_root=str(layout.get("target_root") or ""),
                planned_files=0,
                error=detail,
                recorded_at=_now(),
            ))
            continue
        is_main_tv = (
            main_tmdb is not None
            and str(identity.get("media_type") or "") == "tv"
            and identity.get("tmdb_id") == main_tmdb
        )
        if record.reconciliation_outcome != "new_work":
            results.append(WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="skipped",
                writer_job_id=None,
                phase=record.identity_status,
                target_root="",
                planned_files=0,
                error=None,
                recorded_at=_now(),
            ))
            updated.append(record)
            continue
        if (
            record.requires_content_expansion
            or _unit_has_disc_image(state_root, root_task_id, record)
        ):
            blocked = _park_unit_for_disc_image(record)
            changed = changed or blocked.as_dict() != record.as_dict()
            updated.append(blocked)
            results.append(WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="skipped",
                writer_job_id=None,
                phase="reconciliation_uncertain",
                target_root="",
                planned_files=0,
                error=DISC_IMAGE_INSPECTION_REQUIRED,
                recorded_at=_now(),
            ))
            continue
        if record.writer_job_id is not None:
            # An executed carrier is proof only for the identity its durable
            # plan was built under.  A receipt rollback or operator
            # reconfirmation can change the record's identity after the
            # carrier executed: that ``executed`` fact then belongs to the
            # retired write, not to this record.  Accepting it would skip
            # the corrected write and let R's intake cleanup delete the
            # restored source file as an unconsumed residual.  Retire the
            # superseded carrier so the planning path below re-plans from
            # the current source state under the confirmed identity.
            try:
                existing_carrier = runner.get_job(record.writer_job_id)
            except Exception:
                existing_carrier = None
            if (
                existing_carrier is not None
                and existing_carrier.phase == "executed"
                and not _carrier_plan_identity_matches(
                    existing_carrier.plan, identity,
                )
            ):
                _retire_superseded_unit_carrier(runner, existing_carrier.id)
                record = replace(record, writer_job_id=None)
                changed = True
        if record.writer_job_id is not None:
            # Already planned and executed; re-verify the carrier state.
            carrier = runner.get_job(record.writer_job_id)
            # A process restart converts an interrupted ``executing`` record
            # to ``retry_wait`` before it has an AList client for readback.
            # That owned internal carrier still has the only durable plan
            # matching the partial formal write, so it must use the exact
            # recovery matrix below rather than be retired/replanned from a
            # now-mutated source tree.  A failed carrier gets that same
            # treatment only when it carries the explicit generic basename
            # recovery intent: a provider may have completed its move but
            # rejected the following rename, so retiring/replanning would
            # lose the only exact ownership map for the intermediate object.
            # Ordinary retry_wait/failed carriers retain the stale-plan path.
            recoverable_carrier = carrier.phase in {
                "executing", "verifying", "cleaning",
            } or (
                carrier.phase in {"retry_wait", "failed"}
                and _is_owned_internal_carrier(carrier, root_task_id)
                and (
                    carrier.phase == "retry_wait"
                    or runner.has_provider_basename_recovery_intent(carrier)
                )
            )
            if recoverable_carrier:
                # A pause can arrive after the formal writer began.  Keep the
                # exact carrier, fresh-read it on a later unpaused pass, and
                # never create a second plan for the same WorkUnit.
                if paused():
                    updated.append(record)
                    paused_during_run = True
                    break
                carrier = runner.recover_job(carrier.id)
                if carrier.phase in {"retry_wait", "failed"}:
                    try:
                        carrier = runner.execute_job(
                            carrier.id,
                            pause_requested=pause_requested,
                        )
                    except EnginePauseRequested:
                        updated.append(record)
                        paused_during_run = True
                        break
                    except Exception as exc:
                        try:
                            carrier = runner.get_job(carrier.id)
                        except Exception:
                            pass
                        results.append(WorkAcceptanceResult(
                            work_unit_id=record.work_unit_id,
                            outcome="failed",
                            writer_job_id=carrier.id,
                            phase=carrier.phase,
                            target_root=str((carrier.plan.get("target_root")) or ""),
                            planned_files=len(carrier.plan.get("files") or []),
                            error=redact_error(exc),
                            recorded_at=_now(),
                        ))
                        updated.append(record)
                        break
                if carrier.phase == "executed":
                    completed_record = _complete_unit_episode_gap_registration(
                        runner, state_root, root_task_id, record, carrier.plan,
                    )
                    changed = changed or completed_record.as_dict() != record.as_dict()
                    record = completed_record
                    if is_main_tv and requires_main_parent and main_target_root is None:
                        main_target_root = _validated_formal_work_root(
                            runner,
                            carrier.plan.get("target_root"),
                            field="已执行主单元目标根",
                        )
                    if (
                        str(identity.get("media_type") or "") == "tv"
                        and layout.get("relation") not in {"nested_special", "nested_under_main"}
                        and isinstance(identity.get("tmdb_id"), int)
                        and not isinstance(identity.get("tmdb_id"), bool)
                    ):
                        executed_tv_roots[int(identity["tmdb_id"])] = str(
                            carrier.plan.get("target_root") or ""
                        )
                    results.append(accepted(record, carrier))
                    updated.append(record)
                    continue
                if carrier.phase.startswith("failed") or carrier.phase == "cancelled":
                    # Recovery established a durable technical conflict.  Do
                    # not disguise it as a pause or retire the sole carrier:
                    # the RootJob must surface a clear failed outcome and the
                    # persisted plan remains the evidence for a later repair.
                    results.append(WorkAcceptanceResult(
                        work_unit_id=record.work_unit_id,
                        outcome="failed",
                        writer_job_id=carrier.id,
                        phase=carrier.phase,
                        target_root=str((carrier.plan.get("target_root")) or ""),
                        planned_files=len(carrier.plan.get("files") or []),
                        error=carrier.error,
                        recorded_at=_now(),
                    ))
                    updated.append(record)
                    if is_main_tv:
                        break
                    continue
                # Preserve the active carrier for restart/readback.  It is
                # unsafe to retire or replace it until that recovery closes.
                updated.append(record)
                paused_during_run = True
                break
            if carrier.phase not in {"executed"}:
                # A terminal carrier is a stale plan from a previous failed
                # attempt.  Retire it and fall through to re-plan from the
                # current source state.
                _retire_stale_unit_carrier(runner, carrier.id)
                record = replace(record, writer_job_id=None)
                changed = True
            else:
                plan_files = len(carrier.plan.get("files") or [])
                completed_record = _complete_unit_episode_gap_registration(
                    runner, state_root, root_task_id, record, carrier.plan,
                )
                changed = changed or completed_record.as_dict() != record.as_dict()
                record = completed_record
                if is_main_tv and requires_main_parent and main_target_root is None:
                    main_target_root = _validated_formal_work_root(
                        runner,
                        carrier.plan.get("target_root"),
                        field="已执行主单元目标根",
                    )
                if (
                    str(identity.get("media_type") or "") == "tv"
                    and layout.get("relation") not in {"nested_special", "nested_under_main"}
                    and isinstance(identity.get("tmdb_id"), int)
                    and not isinstance(identity.get("tmdb_id"), bool)
                ):
                    executed_tv_roots[int(identity["tmdb_id"])] = str(
                        carrier.plan.get("target_root") or ""
                    )
                results.append(WorkAcceptanceResult(
                    work_unit_id=record.work_unit_id,
                    outcome="accepted",
                    writer_job_id=carrier.id,
                    phase=carrier.phase,
                    target_root=str((carrier.plan.get("target_root")) or ""),
                    planned_files=plan_files,
                    error=carrier.error,
                    recorded_at=_now(),
                ))
                updated.append(record)
                continue
        base_record = record
        carrier_id = _unit_job_id(record.work_unit_id)
        planned: EngineJob | None = None
        main_failed = False
        try:
            if is_main_tv and layout.get("relation") != "nested_under_main":
                # A same-identity physical special shares the main's TMDB id,
                # so the identity test above is true for it too — its layout
                # relation distinguishes it, and it must take the nested
                # path below instead of planning its own root at the shelf.
                parent_override = None
            elif layout.get("relation") == "nested_special":
                parent_tmdb = layout.get("parent_tmdb_id")
                parent_override = (
                    executed_tv_roots.get(parent_tmdb)
                    if isinstance(parent_tmdb, int)
                    else None
                ) or (
                    str(layout.get("parent_path"))
                    if layout.get("parent_path") else None
                )
                if parent_override is None:
                    raise ValueError("特别篇父剧尚未完成安全目标根证明")
            elif layout.get("relation") == "nested_under_main":
                # The main unit's freshly executed target root is the family
                # anchor: prefer the identity-keyed map (registered by every
                # executed TV unit), then the validated main root.
                parent_override = (
                    executed_tv_roots.get(main_tmdb)
                    if isinstance(main_tmdb, int)
                    else None
                ) or main_target_root
                if parent_override is None:
                    raise ValueError("主剧尚未完成安全目标根证明")
            elif layout.get("parent_path"):
                # The layout's family decision (sub-series directory,
                # collection directory, or container root) is the planner's
                # parent — a sub-series movie plans inside its family
                # directory, never flattened onto the container root.
                parent_override = str(layout["parent_path"])
            elif container_parent:
                parent_override = container_parent
            elif main_tmdb is not None:
                parent_override = main_target_root
            else:
                parent_override = None
            request = _request_for_unit(
                runner, record, root_task_id, state_root,
                parent_override=parent_override,
            )
            # plan_job refuses an existing carrier id; a stale terminal
            # carrier must be gone before the fresh plan is persisted.
            _retire_stale_unit_carrier(runner, carrier_id)
            planned = _mark_internal_carrier(
                runner,
                runner.plan_job(
                    request,
                    job_id=carrier_id,
                    pause_requested=pause_requested,
                ),
                root_task_id,
            )
            executed = runner.execute_job(
                planned.id,
                pause_requested=pause_requested,
            )
            if executed.phase != "executed":
                # EnginePauseRequested raised from inside the executor is
                # converted by the runner into an active carrier.  Record
                # that carrier instead of falsely accepting it or retrying a
                # second writer on the next pass.
                record = replace(record, writer_job_id=planned.id)
                changed = True
                updated.append(record)
                paused_during_run = True
                break
            plan_files = len(executed.plan.get("files") or [])
            record = replace(record, writer_job_id=planned.id)
            record = _complete_unit_episode_gap_registration(
                runner, state_root, root_task_id, record, executed.plan,
            )
            if is_main_tv and requires_main_parent and main_target_root is None:
                main_target_root = _validated_formal_work_root(
                    runner,
                    executed.plan.get("target_root"),
                    field="已执行主单元目标根",
                )
            if (
                str(identity.get("media_type") or "") == "tv"
                and layout.get("relation") not in {"nested_special", "nested_under_main"}
                and isinstance(identity.get("tmdb_id"), int)
                and not isinstance(identity.get("tmdb_id"), bool)
            ):
                executed_tv_roots[int(identity["tmdb_id"])] = str(
                    executed.plan.get("target_root") or ""
                )
            changed = True
            results.append(WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="accepted",
                writer_job_id=executed.id,
                phase=executed.phase,
                target_root=str((executed.plan.get("target_root")) or ""),
                planned_files=plan_files,
                error=None,
                recorded_at=_now(),
            ))
            # The derived episode map is a planning artifact; drop it once
            # the unit is accepted.
            try:
                map_path = state_root / f"episode_map_{record.work_unit_id}.json"
                if map_path.exists():
                    map_path.unlink()
            except OSError:
                pass
        except EnginePauseRequested:
            # ``plan_job`` and ``execute_job`` use this distinct signal when
            # the root-scoped predicate closes.  A plan that was already
            # persisted remains an internal carrier for the normal fresh
            # readback path; pause is never reported as a failed WorkUnit.
            if planned is not None:
                record = replace(record, writer_job_id=planned.id)
                changed = True
            else:
                record = base_record
            updated.append(record)
            paused_during_run = True
            break
        except Exception as exc:
            # Keep writer_job_id unset so the next run re-plans the unit;
            # retire the just-planned carrier so plan_job's existing-id
            # guard cannot pin a stale plan either.  The carrier's full plan
            # survives as a receipt on the acceptance record: the retry's
            # consumed-source continuation reads it instead of inferring
            # consumption from the mutated provider source.
            receipt: tuple[Mapping[str, Any], ...] | None = None
            carried_target_root = ""
            if planned is not None:
                receipt = tuple(
                    {
                        "source_path": str(item.get("source_path") or ""),
                        "target_path": posixpath.join(
                            str(item.get("target_dir") or ""),
                            str(item.get("final_name") or ""),
                        ),
                        "size": item.get("source_size"),
                    }
                    for item in (planned.plan.get("files") or [])
                    if isinstance(item, Mapping)
                ) or None
                carried_target_root = str(planned.plan.get("target_root") or "")
            if receipt is None:
                # An attempt that produced no plan receipt (a request/planner
                # failure before any file was planned) executes nothing, so
                # the previous failed attempt's receipt is still the exact
                # record of the last interrupted write.  Persisting a
                # receipt-less record in its place would destroy the
                # consumed-source continuation evidence the next retry reads,
                # and a receipt-documented source rollback would be left with
                # no sanctioned resume path at all.  Carry it forward; the
                # continuation itself re-validates every object against the
                # fresh source (present objects move, already-moved ones read
                # back).
                previous = {
                    row.work_unit_id: row
                    for row in load_work_acceptance(state_root, root_task_id)
                }.get(record.work_unit_id)
                if (
                    previous is not None
                    and previous.outcome == "failed"
                    and previous.planned_receipt
                ):
                    receipt = tuple(previous.planned_receipt)
                    carried_target_root = str(previous.target_root or "")
            try:
                _retire_stale_unit_carrier(runner, carrier_id)
            except Exception:
                pass
            record = base_record
            results.append(WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="failed",
                writer_job_id=None,
                phase="failed",
                target_root=carried_target_root,
                planned_files=len(receipt or ()),
                error=redact_error(exc),
                recorded_at=_now(),
                planned_receipt=receipt,
            ))
            # A sibling cannot safely become the first writer beneath an
            # unaccepted main TV root.  Preserve this exact failure and leave
            # every remaining unit for a later retry instead of creating a
            # detached child at the selected shelf.
            main_failed = is_main_tv
        updated.append(record)
        if main_failed:
            break
    if changed:
        persist_updates()
    persist_acceptance(retain_existing=paused_during_run)
    if paused_during_run:
        raise EnginePauseRequested("根任务暂停已在单元写入边界生效")
    return results


__all__ = [
    "ContainerMetadataAttention",
    "WorkAcceptanceResult",
    "_container_layout_targets",
    "execute_new_work_units",
    "ensure_container_artifacts",
    "has_unmaterialized_planner_season_gaps",
    "load_work_acceptance",
    "rereview_executed_unit_gaps",
    "save_work_acceptance",
]
