"""P11+P12: the authoritative RootJob pipeline (B/W/C/D -> E/F/G/H/J -> R).

Root tasks created through the S-step intake path (``create_root_job``, i.e.
tasks bound to an ``IntakeSource``) run this pipeline instead of the legacy
automatic chain.  The legacy runner remains the carrier and read-back for
imported historical records, but never plans or writes new-path roots.

Pipeline contract:

- B/W  : snapshot + boundary split (only when the unit ledger is missing, so
         retries never wipe confirmed identities or acceptance state);
- C/U  : per-unit TMDB identity; uncertain units park the root without
         blocking anything that is already confirmed;
- D    : three-shelf reconciliation per confirmed unit (with the aggregated
         known-gap coordinates from every gap ledger);
- E    : per-unit lanes for duplicate_complete / existing_gap / merge_existing
         (``unit_e_lanes``);
- F/G/H: ``execute_new_work_units`` drives the single planner/writer and
         records typed acceptance; J registers precise episode gaps;
- R    : the aggregate decides the durable root phase (``completed`` /
         ``gaps_pending`` / ``reconciliation_uncertain`` / ``failed``).
"""

from __future__ import annotations

import json
import posixpath
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Callable

from engine.scrapeflow.core import _validate_remote_source_basename
from engine.scrapeflow.disc_expansion_bridge import (
    DiscExpansionPauseRequested,
    expand_root_disc_images,
    expansion_staging_root,
)
from engine.scrapeflow.intake_source import load_intake_catalog
from engine.scrapeflow.media_policy import (
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    extension,
)
from engine.scrapeflow.remote_paths import normalize_remote_path, provider_safe_basename
from engine.scrapeflow.residual_policy import (
    UNMAPPED_VIDEO_JUNK,
    UNMAPPED_VIDEO_MIN_CONTENT_SECONDS,
    UNMAPPED_VIDEO_SUSPECT,
    classify_unmapped_video,
)
from engine.scrapeflow.root_boundaries import (
    analyze_root_boundaries,
    rebuild_root_boundary_if_unwritten,
    load_source_snapshot,
)
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.unit_identity import resolve_work_unit_identities
from engine.scrapeflow.work_unit_coalescing import (
    coalesce_confirmed_tv_season_work_units,
)
from engine.scrapeflow.work_units import (
    load_work_unit_records,
    save_work_unit_records,
)

from .library_index import reconcile_root_work_units
from .redaction import redact_error, redact_value
from .root_aggregation import aggregate_root_job
from .simple_engine_runner import (
    EngineJob,
    EnginePauseRequested,
    EngineRequestError,
    SimpleEngineRunner,
)
from .tmdb_episode_catalog import TmdbEpisodeCatalog
from .unit_e_lanes import compute_known_gap_tokens, execute_unit_e_lanes
from .unit_execution import (
    ContainerMetadataAttention,
    ensure_container_artifacts,
    execute_new_work_units,
)

# Phases the pipeline may re-enter on dispatch.  ``retry_wait`` appears
# because the scheduler's bounded retry boundary is reused for transient
# B/W/C/D failures (AList/TMDB availability); the pipeline itself is
# idempotent, so re-entry is safe.
RUNNABLE_PHASES = frozenset({"queued", "reconciliation_uncertain", "retry_wait"})

PARK_PHASE = "reconciliation_uncertain"
GAPS_PENDING_PHASE = "gaps_pending"


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _trace(message: str) -> None:
    """Bounded live observability line (mirrors the replenishment tracer)."""
    print(f"[root-pipeline] {message}", flush=True)


def _remove_intake_entry(alist: object, parent: str, name: str) -> None:
    """Remove one exact intake entry, compatibility punctuation included.

    ``AListClient.remove`` enforces the rename-safety policy — the right
    contract for destinations and moves, but an intake deletion is not a
    rename: the name came from a fresh provider listing and the provider
    accepted it at upload time (full-width ``：``/``／`` titles, dot runs).
    Three bounded tiers, each proven by the caller's fresh listing:

    1. the strict ``remove`` — names the rename policy accepts;
    2. the raw exact-name call — the policy is the only objection (e.g.
       compatibility punctuation the provider itself accepts);
    3. rename-to-safe + remove — the provider's own name guard refuses the
       existing name (Quark rejects ``..`` runs on delete), so the entry is
       first renamed to its provider-safe spelling and then removed.

    Separators, control characters, and path segments stay fail-closed
    through the source-basename contract at every tier.
    """
    remove = getattr(alist, "remove", None)
    if callable(remove):
        try:
            remove(parent, [name])
            return
        except ValueError:
            pass  # rename policy refused an existing provider name
    call = getattr(alist, "call", None)
    if not callable(call):
        raise RuntimeError(f"AList 客户端缺少删除接口，无法删除: {name!r}")
    _validate_remote_source_basename(name)
    try:
        call("remove", {"dir": normalize_remote_path(parent), "names": [name]})
        return
    except Exception as exc:
        provider_error = exc  # provider-side name guard — try tier 3
    # Tier 3: rename to the strict provider-safe spelling, then remove the
    # renamed entry.  The caller's fresh-listing proof still verifies the
    # original name is gone; a remove that no-ops leaves the renamed entry
    # visible to the next consumption pass (self-healing, never silent).
    safe_name = provider_safe_basename(name)
    if safe_name == name or safe_name in {"未命名", ".", ".."}:
        raise provider_error
    rename = getattr(alist, "rename", None)
    if not callable(rename):
        raise provider_error
    rename(posixpath.join(parent, name), safe_name)
    if callable(remove):
        remove(parent, [safe_name])
    else:
        call("remove", {"dir": normalize_remote_path(parent), "names": [safe_name]})


def is_intake_bound_root(state_root: Path, root_task_id: str) -> bool:
    """Whether an IntakeSource catalog entry binds this job as its RootJob.

    The catalog binding is the durable S-step fact (``root_task_id``), so no
    new EngineJob.summary marker is needed to tell new-path roots from
    imported legacy records.  Fully defensive: any unreadable/malformed
    catalog means "not bound" so the legacy lane stays fail-closed.
    """
    try:
        catalog = load_intake_catalog(state_root)
        return any(
            source.root_task_id == root_task_id
            for source in catalog
            if getattr(source, "root_task_id", None) is not None
        )
    except Exception:
        return False


def _persist_root(
    runner: SimpleEngineRunner,
    job: EngineJob,
    phase: str,
    *,
    error: str | None = None,
) -> EngineJob:
    """Persist one bounded root-phase transition without touching the summary."""
    cancelled = runner.consume_cancellation(job.id)
    if cancelled is not None:
        return cancelled
    updated = replace(job, phase=phase, error=error, updated_at=_now())
    atomic_write_json(
        runner._job_path(job.id),  # noqa: SLF001 - pipeline composition
        redact_value(updated.as_dict()),
        allow_nan=False,
    )
    return updated


def _refresh_lane_acceptance(
    state_root: Path,
    root_task_id: str,
    records: list,
) -> None:
    """Rewrite acceptance rows for completed E-lane units.

    E lanes record progress on the WorkUnit ledger, not the acceptance file,
    so a stale failed acceptance from an earlier run must be replaced once
    the lane finishes; otherwise R would keep counting the old failure.
    """
    from .unit_execution import (
        WorkAcceptanceResult,
        load_work_acceptance,
        save_work_acceptance,
    )
    fresh = {
        row.work_unit_id: row
        for row in load_work_acceptance(state_root, root_task_id)
    }
    for record in records:
        outcome = record.reconciliation_outcome
        if (
            outcome in {"duplicate_complete", "existing_gap", "merge_existing"}
            and record.lane_status
        ):
            fresh[record.work_unit_id] = WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="accepted",
                writer_job_id=record.writer_job_id,
                phase="executed" if outcome == "merge_existing" else "completed",
                target_root=record.matched_work_root or "",
                planned_files=0,
                error=None,
                recorded_at=_now(),
            )
    save_work_acceptance(
        state_root, root_task_id, list(fresh.values()),
    )


def _acceptance_work_unit_ids_for_coalescing(
    state_root: Path,
    root_task_id: str,
) -> frozenset[str] | None:
    """Return accepted-unit IDs only from a strictly readable H ledger.

    C-stage coalescing is allowed only before any unit's acceptance record.
    A missing or exact empty ledger is harmless; a symlink, malformed JSON,
    malformed row, or duplicate row is not evidence of no acceptance and
    therefore fails closed for the optional coalescing optimization.
    """
    path = state_root / f"work_acceptance_{root_task_id}.json"
    if not path.exists():
        return frozenset()
    if path.is_symlink() or not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(raw, list):
        return None
    output: set[str] = set()
    for row in raw:
        if not isinstance(row, Mapping):
            return None
        work_unit_id = row.get("work_unit_id")
        if not isinstance(work_unit_id, str) or not work_unit_id or work_unit_id in output:
            return None
        output.add(work_unit_id)
    return frozenset(output)


def _coalesce_confirmed_tv_season_records(
    state_root: Path,
    root_task_id: str,
    records: list,
) -> list:
    """Persist a narrow C-stage coalescing only when all proof is local.

    This helper performs no provider access and has no effect if the B/W
    snapshot or H ledger cannot be exactly verified.  It runs immediately
    after C/U and before D, so no reconciliation result, writer carrier, or
    acceptance row can be silently retired.
    """
    snapshot = load_source_snapshot(state_root, root_task_id)
    acceptance_ids = _acceptance_work_unit_ids_for_coalescing(
        state_root, root_task_id,
    )
    if snapshot is None or acceptance_ids is None:
        return records
    merged = coalesce_confirmed_tv_season_work_units(
        records,
        snapshot,
        acceptance_work_unit_ids=tuple(sorted(acceptance_ids)),
    )
    if merged != records:
        save_work_unit_records(state_root, root_task_id, merged)
    return merged


def _provably_absent(runner: SimpleEngineRunner, path: str) -> bool:
    """True only when the parent lists fine and the name is absent.

    ``source_directory_exists`` conflates "gone" with "cannot prove it
    exists" (every provider error returns False).  Terminal consumption
    must not treat an unprovable listing as an already-consumed source:
    a Quark outage during consumption would otherwise fabricate a
    success receipt.  Here a provider error is NOT absence — the caller
    proceeds to the fail-closed walk instead of the early return.
    """
    normalized = str(path).rstrip("/")
    parent = posixpath.dirname(normalized) or "/"
    name = posixpath.basename(normalized)
    if not name:
        return False
    listing = getattr(runner.alist, "list", None)
    if not callable(listing):
        return False
    try:
        try:
            rows = listing(parent, refresh=True)
        except TypeError:
            rows = listing(parent)
    except Exception:
        return False
    if not isinstance(rows, list):
        return False
    return not any(
        isinstance(item, Mapping) and item.get("name") == name
        for item in rows
    )


def _cleanup_consumed_source_root(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    source: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, object]:
    """Delete a completed root's entire intake tree, residuals included.

    Operator ruling (2026-08-27, extended 2026-09-02): the intake area is
    staging, not storage.  Once a root's media is verified in the formal
    library, the whole source tree — residual themes, MVs, backup subtitles,
    screenshots, font packs, losing versions — is deleted by default so the
    operator never cleans up manually.  A root parked in ``gaps_pending``
    is equally terminal for the source tree: the gap ledger is the durable
    record (缺口只登不补) and no lane ever re-reads the staging tree, so the
    same consumption applies.

    Unmapped-video gate (operator ruling 2026-09-05): every video still in
    the source at consumption time is by definition unwritten.  Theme-named
    credits, bonus-directory assets, and short non-episode videos are junk
    and die with the tree.  Everything else — unaired episodes, unnumbered
    SPs, web-exclusive specials, episode-grade runtimes, or anything the
    duration probe could not prove — is quarantined to
    ``/ScrapeFlow/待裁决/<root-task-id>/`` with a manifest instead of being
    deleted, because the engine can never prove a TMDB-unlisted special is
    worthless.  A quarantined video's sidecar subtitle moves with it.

    Ownership is proven by the durable catalog binding (S step), never by
    path naming.  Every file delete, quarantine move, and directory removal
    is confirmed through a fresh parent listing; the pause predicate is
    re-checked before each remote side effect.  A Quark/AList driver that
    acknowledges a delete without applying it is retried through the
    bounded parent-name remove fallback, and the whole walk simply stops
    when the provider keeps reporting the tree — the surviving residual is
    reported back to the caller instead of failing silently.
    """
    # Operator ruling 2026-09-05: the quarantine directory is named after
    # the source folder the operator created in 待刮削 (the natural 作品根
    # for browsing), not the internal root-task id.  A manifest left by a
    # *different* root under the same folder name (a consumed source that
    # was re-created later) disambiguates with a short task-id suffix so
    # review sets never mix.
    quarantine_root = posixpath.join(
        str(runner.library_root).rstrip("/"),
        "ScrapeFlow", "待裁决",
        posixpath.basename(source.rstrip("/")) or root_task_id,
    )
    _reader = getattr(runner.alist, "read_file_bytes", None)
    if callable(_reader):
        try:
            try:
                _payload = _reader(
                    posixpath.join(quarantine_root, "manifest.json"),
                    max_bytes=1024 * 1024,
                )
            except TypeError:
                _payload = _reader(
                    posixpath.join(quarantine_root, "manifest.json")
                )
            _loaded = json.loads(_payload.decode("utf-8", "replace"))
            if (
                isinstance(_loaded, dict)
                and str(_loaded.get("root_task_id") or "") not in ("", root_task_id)
            ):
                quarantine_root = f"{quarantine_root}-{root_task_id[-8:]}"
        except Exception:
            pass
    try:
        bound = any(
            entry.root_task_id == root_task_id and entry.canonical_path == source
            for entry in load_intake_catalog(state_root)
        )
    except Exception:
        bound = False
    if not bound:
        return {"source": source, "removed": [], "failures": [], "source_remaining": True, "unbound": True}
    if _provably_absent(runner, source):
        # Already consumed (e.g. archived by an E1 lane): nothing to clean.
        # Only a *proven* absence qualifies — an unprovable listing falls
        # through to the fail-closed walk instead of fabricating success.
        return {"source": source, "removed": [], "failures": [], "source_remaining": False}
    listing = getattr(runner.alist, "list", None)
    remove_empty = getattr(runner.alist, "remove_empty_dir", None)
    if not callable(listing) or not callable(remove_empty):
        return {"source": source, "removed": [], "failures": [], "source_remaining": True}

    def rows(path: str) -> list[Mapping[str, object]]:
        try:
            raw = listing(path, refresh=True)
        except TypeError:
            raw = listing(path)
        if not isinstance(raw, list) or any(
            not isinstance(item, Mapping) for item in raw
        ):
            raise RuntimeError(f"AList 源目录回读格式无效: {path}")
        return list(raw)

    removed: list[str] = []
    failures: list[str] = []

    def paused() -> bool:
        """Fail closed if the composition-root pause state is unavailable."""
        if not callable(pause_requested):
            return False
        try:
            return bool(pause_requested())
        except Exception:
            return True

    def rows_proven(path: str) -> list[Mapping[str, object]]:
        """List with bounded retries; raise only when unprovable.

        A provider that transiently fails a verification listing gets a few
        retries, but "cannot list" is never silently interpreted as "already
        gone" — the caller treats an unprovable listing as an unproven
        delete and reports it as a residual.
        """
        last: Exception | None = None
        for _attempt in range(3):
            if paused():
                raise RuntimeError(f"暂停边界，无法证明目录状态: {path}")
            try:
                return rows(path)
            except Exception as exc:  # noqa: BLE001 - retried, then raised
                last = exc
        raise RuntimeError(f"AList 目录回读失败，删除结果无法证明: {path}: {last}")

    def delete_file(parent: str, name: str) -> bool:
        """Delete one file and prove it disappeared through a fresh listing."""
        for _attempt in range(3):
            if paused():
                return False
            try:
                _remove_intake_entry(runner.alist, parent, name)
            except Exception:
                failures.append(posixpath.join(parent, name))
                return False
            try:
                after = rows_proven(parent)
            except Exception:
                # Unproven is NOT deleted: report the residual so the
                # operator re-runs consume-source when the provider recovers.
                failures.append(posixpath.join(parent, name))
                return False
            if not any(item.get("name") == name for item in after):
                return True
        # The driver keeps acknowledging the delete without applying it.
        failures.append(posixpath.join(parent, name))
        return False

    def delete_directory(directory: str) -> bool:
        """Remove one directory (empty or not) with bounded retries."""
        if paused():
            return False
        parent = posixpath.dirname(directory) or "/"
        name = posixpath.basename(directory)
        for _attempt in range(3):
            if paused():
                return False
            try:
                remove_empty(directory)
            except Exception:
                pass
            try:
                parent_rows = rows_proven(parent)
            except Exception:
                # Unproven removal is a reported residual, never success.
                failures.append(directory)
                return False
            if not any(item.get("name") == name for item in parent_rows):
                removed.append(directory)
                return True
            # Quark sometimes acknowledges remove_empty_directory without
            # deleting; the explicit parent-name remove is the bounded
            # fallback for the driver no-op.
            try:
                _remove_intake_entry(runner.alist, parent, name)
            except Exception:
                pass
            try:
                parent_rows = rows_proven(parent)
            except Exception:
                failures.append(directory)
                return False
            if not any(item.get("name") == name for item in parent_rows):
                removed.append(directory)
                return True
        failures.append(directory)
        return False

    quarantined: list[dict[str, object]] = []

    def probe_duration(path: str) -> float | None:
        """Bounded duration probe for one unmapped video (None = unproven).

        ``video_duration_probe`` on the client is the scenario-double seam;
        the production ``AListClient`` does not define it and always runs
        the real bounded ffprobe over a fresh file link.  Any probe failure
        (infrastructure or no video stream) yields ``None`` so the gate can
        fail closed.
        """
        hook = getattr(runner.alist, "video_duration_probe", None)
        if callable(hook):
            try:
                return float(hook(path))
            except Exception:
                return None
        from engine.scrapeflow.video_admission import (
            VideoAdmissionError,
            probe_remote_video_stream,
        )

        try:
            verdict = probe_remote_video_stream(runner.alist, path)
        except VideoAdmissionError:
            return None
        raw = verdict.get("duration_seconds")
        try:
            return float(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    _verified_quarantine_dirs: set[str] = set()

    def ensure_quarantine_dir(path: str) -> bool:
        """Materialize one quarantine directory level by level (cached)."""
        if path in _verified_quarantine_dirs:
            return True
        if not path.startswith("/"):
            return False
        segments = [seg for seg in path.split("/") if seg]
        current = ""
        for segment in segments:
            current = f"{current}/{segment}"
            if current in _verified_quarantine_dirs:
                continue
            try:
                probe = runner.alist.try_list(current, refresh=True)
            except Exception:
                probe = None
            if probe is None:
                try:
                    runner.alist.mkdir(current)
                except Exception as exc:  # noqa: BLE001 - reported as residual
                    failures.append(current)
                    _trace(f"待裁决目录创建失败: {current}: {exc}")
                    return False
            _verified_quarantine_dirs.add(current)
        return True

    quarantined_names: set[str] = set()

    def quarantine_file(directory: str, name: str, child: str, reason: str) -> bool:
        """Move one suspect into the quarantine root, FLAT (operator ruling
        2026-09-05: 平铺方便人工裁决，不镜像源目录结构).

        A basename that already occupies the flat root — from this batch or
        a previous partial run — falls back to a disambiguating subdirectory
        named after the file's source parent directory, so an AList move can
        never overwrite an earlier quarantine.  The manifest keeps the
        original relative path for full traceability either way.
        """
        if paused():
            return False
        relative = posixpath.relpath(child, source)
        target_dir = quarantine_root
        if name in quarantined_names:
            parent_tag = posixpath.basename(directory.rstrip("/")) or "源目录"
            target_dir = posixpath.join(quarantine_root, parent_tag)
        else:
            # Cross-run safety: a previous partial consumption may have left
            # the same basename at the flat root.  An unprovable listing also
            # routes to the disambiguating subdir (fail closed, never move
            # onto an unproven target).
            try:
                existing = rows_proven(quarantine_root)
            except Exception:
                existing = None
            if existing is not None and any(
                item.get("name") == name for item in existing
            ):
                parent_tag = posixpath.basename(directory.rstrip("/")) or "源目录"
                target_dir = posixpath.join(quarantine_root, parent_tag)
                if name in quarantined_names:
                    failures.append(child)
                    _trace(f"待裁决同名冲突无法消歧: {child}")
                    return False
        if not ensure_quarantine_dir(target_dir):
            return False
        try:
            runner.alist.move(directory, target_dir, [name])
        except Exception as exc:  # noqa: BLE001 - reported as residual
            failures.append(child)
            _trace(f"待裁决移入失败: {child}: {exc}")
            return False
        # Prove the move both ways (fresh readback on each side): gone from
        # the source parent AND present under the quarantine target.
        try:
            after = rows_proven(directory)
            target_rows = rows_proven(target_dir)
        except Exception:
            failures.append(child)
            return False
        if any(item.get("name") == name for item in after):
            failures.append(child)
            return False
        landed = next(
            (item for item in target_rows if item.get("name") == name), None,
        )
        if landed is None:
            failures.append(child)
            _trace(f"待裁决目标回读缺失: {child}")
            return False
        size = int(landed.get("size") or 0)
        quarantined_names.add(name)
        quarantined.append({
            "source_path": child,
            "relative_path": relative,
            "quarantine_path": posixpath.join(target_dir, name),
            "size": size,
            "reason": reason,
            "moved_at": _now(),
            "root_task_id": root_task_id,
        })
        return True

    def visit(directory: str) -> bool:
        """Depth-first delete of everything below ``directory``."""
        if paused():
            return False
        try:
            entries = rows_proven(directory)
        except Exception:
            # A directory that cannot be listed cannot be proven deleted;
            # abort the walk so the surviving tree stays a reported residual.
            return False
        # Unmapped-video gate (operator ruling 2026-09-05): classify every
        # still-present video, delete the provable junk, quarantine the
        # suspects together with their sidecar subtitles, and only then
        # delete the remaining non-video residuals.  Planned media was
        # already moved out by the writer, so anything still here is
        # unwritten.
        suspect_videos: list[str] = []
        junk_videos: list[str] = []
        other_files: list[str] = []
        dir_children: list[str] = []
        durations: dict[str, float | None] = {}
        for item in entries:
            name = item.get("name")
            if (
                not isinstance(name, str)
                or not name
                or name in {".", ".."}
                or "/" in name
                or "\\" in name
            ):
                raise RuntimeError(f"AList 源目录出现不安全条目: {directory}")
            child = posixpath.join(directory, name)
            if item.get("is_dir") is True:
                dir_children.append(child)
            elif extension(name) in VIDEO_EXTENSIONS:
                duration = probe_duration(child)
                durations[name] = duration
                if classify_unmapped_video(child, duration) == UNMAPPED_VIDEO_SUSPECT:
                    suspect_videos.append(name)
                else:
                    junk_videos.append(name)
            else:
                other_files.append(name)
        for name in junk_videos:
            if paused():
                return False
            if not delete_file(directory, name):
                return False
        suspect_stems = {PurePosixPath(name).stem for name in suspect_videos}
        for name in suspect_videos:
            if paused():
                return False
            child = posixpath.join(directory, name)
            duration = durations.get(name)
            if duration is not None and duration >= UNMAPPED_VIDEO_MIN_CONTENT_SECONDS:
                reason = f"未映射视频，正片级时长 {duration:.0f}s"
            elif duration is not None:
                reason = f"未映射视频，正片命名且时长 {duration:.0f}s"
            else:
                reason = "未映射视频，时长无法证明（fail-closed 保留）"
            if not quarantine_file(directory, name, child, reason):
                return False
        for name in other_files:
            if paused():
                return False
            if extension(name) in SUBTITLE_EXTENSIONS and any(
                name.startswith(stem + ".") for stem in suspect_stems
            ):
                # A sidecar subtitle (language tag included) of a
                # quarantined video moves with it.
                if not quarantine_file(
                    directory, name, posixpath.join(directory, name),
                    "疑似内容视频的配对字幕",
                ):
                    return False
                continue
            if not delete_file(directory, name):
                return False
        for child in dir_children:
            if paused():
                return False
            if not visit(child):
                return False
            if not delete_directory(child):
                return False
        return True

    def source_remaining() -> bool:
        """Fail closed: an unprovable listing counts as still remaining."""
        parent = posixpath.dirname(source) or "/"
        name = posixpath.basename(source)
        try:
            if any(item.get("name") == name for item in rows_proven(parent)):
                return True
        except Exception:
            return True
        try:
            return bool(rows_proven(source))
        except Exception:
            return True

    def write_quarantine_manifest() -> bool:
        """Persist/merge the quarantine manifest beside the moved files."""
        if not quarantined:
            return True
        manifest_path = posixpath.join(quarantine_root, "manifest.json")
        existing: dict[str, object] = {}
        reader = getattr(runner.alist, "read_file_bytes", None)
        if callable(reader):
            try:
                try:
                    payload = reader(manifest_path, max_bytes=1024 * 1024)
                except TypeError:
                    payload = reader(manifest_path)
                loaded = json.loads(payload.decode("utf-8", "replace"))
                if isinstance(loaded, dict):
                    existing = loaded
            except Exception:
                existing = {}
        rows = existing.get("entries")
        by_path = {
            str(row.get("source_path")): row
            for row in (rows if isinstance(rows, list) else [])
            if isinstance(row, dict)
        }
        for entry in quarantined:
            by_path[str(entry["source_path"])] = entry
        manifest = {
            "schema_version": 1,
            "root_task_id": root_task_id,
            "source_root": source,
            "note": (
                "未映射疑似内容视频（终态闸门 2026-09-05 裁决）：引擎无法证明"
                "它们是花絮/垃圾，也未证明到 TMDB 坐标，故不随源树删除。"
                "人工裁决后可删除本目录或告知归位坐标。"
            ),
            "entries": sorted(by_path.values(), key=lambda row: str(row.get("relative_path"))),
            "updated_at": _now(),
        }
        try:
            runner.alist.upload_bytes(
                manifest_path,
                json.dumps(manifest, ensure_ascii=False, indent=1).encode("utf-8"),
                "application/json",
                overwrite=True,
            )
        except TypeError:
            runner.alist.upload_bytes(
                manifest_path,
                json.dumps(manifest, ensure_ascii=False, indent=1).encode("utf-8"),
                "application/json",
            )
        except Exception as exc:  # noqa: BLE001 - reported as residual
            failures.append(manifest_path)
            _trace(f"待裁决 manifest 写入失败: {exc}")
            return False
        return True

    if not visit(source):
        # The walk aborted (pause, provider refusal, or an unsafe entry):
        # never fall back to a bulk recursive delete of an unproven tree.
        # Whatever was quarantined before the abort still gets its manifest:
        # those files are already out of the source tree.
        write_quarantine_manifest()
        return {
            "source": source,
            "removed": removed,
            "failures": failures[:50],
            "source_remaining": source_remaining(),
            "quarantine_root": quarantine_root,
            "quarantined": [entry["source_path"] for entry in quarantined],
            "quarantined_count": len(quarantined),
        }
    # The source root itself: delete it the same way so the intake monitor
    # marks the catalog entry missing on its next scan.  An unproven root
    # removal stays visible through source_remaining below.
    delete_directory(source)
    write_quarantine_manifest()

    return {
        "source": source,
        "removed": removed,
        "failures": failures[:50],
        "source_remaining": source_remaining(),
        "quarantine_root": quarantine_root,
        "quarantined": [entry["source_path"] for entry in quarantined],
        "quarantined_count": len(quarantined),
    }


def _cleanup_expansion_staging_root(
    runner: SimpleEngineRunner,
    state_root: Path,
    job: EngineJob,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> str | None:
    """Consume the task-owned expansion staging tree at terminal phase.

    Ownership is path-derived (``ScrapeFlow/展开/<root-task-id>``) and gated
    on the ledger's expansion provenance.  Every file still present under
    the staging root must be a declared staged payload of this root — the
    writer has already moved the verified payloads into the formal library,
    so any survivor is a redundant copy; anything undeclared keeps the tree
    alive as a reported residual instead of being blindly deleted.  Each
    delete is verified through a fresh listing and the pause predicate is
    re-checked before every remote side effect.
    """
    try:
        records = load_work_unit_records(state_root, job.id)
    except Exception:
        return None
    declared = {
        str(member.get("staged_path"))
        for record in records
        if isinstance(record.disc_expansion, Mapping)
        for member in (record.disc_expansion.get("members") or [])
        if isinstance(member, Mapping) and member.get("staged_path")
    }
    if not declared:
        return None
    staging_root = expansion_staging_root(runner.library_root, job.id)
    if _provably_absent(runner, staging_root):
        # Proven absence only: an unprovable listing falls through to the
        # walk, which reports a residual instead of fabricating success.
        return None
    listing = getattr(runner.alist, "list", None)
    remove_empty = getattr(runner.alist, "remove_empty_dir", None)
    if not callable(listing) or not callable(remove_empty):
        return "展开 staging 清理缺少 AList 删除接口"

    def paused() -> bool:
        if not callable(pause_requested):
            return False
        try:
            return bool(pause_requested())
        except Exception:
            return True

    def rows(path: str) -> list[Mapping[str, object]]:
        try:
            raw = listing(path, refresh=True)
        except TypeError:
            raw = listing(path)
        if not isinstance(raw, list):
            raise RuntimeError(f"AList staging 目录回读格式无效: {path}")
        return [item for item in raw if isinstance(item, Mapping)]

    undeclared: list[str] = []
    surviving: list[str] = []
    directories: list[str] = []

    class _PauseBoundary(Exception):
        pass

    def walk(path: str) -> None:
        for item in rows(path):
            if paused():
                raise _PauseBoundary()
            name = str(item.get("name") or "")
            full = posixpath.join(path, name)
            if item.get("is_dir"):
                directories.append(full)
                walk(full)
            elif full not in declared:
                undeclared.append(full)
            else:
                # The writer already moved the verified payload into the
                # formal library; a survivor is a redundant staged copy.
                surviving.append(full)

    try:
        walk(staging_root)
    except _PauseBoundary:
        return "展开 staging 清理在暂停边界停止，可重跑 consume-source"
    except Exception as exc:  # noqa: BLE001 - residual note, never a failure
        _trace(f"root {job.id} 展开 staging 清理异常: {redact_error(exc)}")
        return f"展开 staging 清理未完成（清理异常: {redact_error(exc)}），可重跑 consume-source"
    if undeclared:
        _trace(f"root {job.id} 展开 staging 有未声明残留 {len(undeclared)} 项")
        return (
            f"展开 staging 有 {len(undeclared)} 个未声明文件（首个: "
            f"{undeclared[0]}），需人工核对后清理"
        )
    for full in surviving:
        if paused():
            return "展开 staging 清理在暂停边界停止，可重跑 consume-source"
        parent, name = posixpath.split(full)
        try:
            _remove_intake_entry(runner.alist, parent, name)
        except Exception as exc:  # noqa: BLE001 - residual note
            _trace(f"root {job.id} 展开 staging 残件删除失败: {redact_error(exc)}")
            return f"展开 staging 残件删除失败（{redact_error(exc)}），可重跑 consume-source"
    for directory in sorted(directories, key=len, reverse=True) + [staging_root]:
        if paused():
            return "展开 staging 清理在暂停边界停止，可重跑 consume-source"
        try:
            remove_empty(directory, refresh=True)
        except TypeError:
            remove_empty(directory)
        except Exception:
            continue
    if runner.source_directory_exists(staging_root):
        return "展开 staging 目录壳仍在，可重跑 consume-source"
    _trace(f"root {job.id} 展开 staging 树已消费")
    return None


def _terminal_source_cleanup(
    runner: SimpleEngineRunner,
    state_root: Path,
    job: EngineJob,
    source: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
    receipt_out: dict[str, object] | None = None,
) -> str | None:
    """Best-effort terminal source consumption; residual becomes a job note.

    Returns an informational error string when the intake tree survived
    (partial delete, provider refusal, or pause), ``None`` when nothing
    remains.  The note never changes the phase: a completed or
    ``gaps_pending`` root stays terminal either way, and the operator can
    re-run the consumption through ``POST /api/jobs/<id>/consume-source``.

    Quarantined unmapped videos (2026-09-05 gate) are a success outcome,
    not a residual: they are recorded in the receipt (exposed through
    ``receipt_out`` for the operator endpoint) and traced, and the job's
    note stays clean.
    """
    try:
        receipt = _cleanup_consumed_source_root(
            runner,
            state_root,
            job.id,
            source,
            pause_requested=pause_requested,
        )
    except Exception as exc:  # noqa: BLE001 - residual note, never a failure
        _trace(f"root {job.id} 源清理异常: {redact_error(exc)}")
        return f"收官清源未完成（清理异常: {redact_error(exc)}），可重跑 consume-source"
    if receipt_out is not None:
        receipt_out.update(receipt)
    removed = receipt.get("removed")
    if not receipt.get("source_remaining"):
        if isinstance(removed, list) and removed:
            _trace(f"root {job.id} 源树已消费（{len(removed)} 个目录）")
        quarantined_count = receipt.get("quarantined_count") or 0
        if quarantined_count:
            _trace(
                f"root {job.id} 未映射视频闸门：{quarantined_count} 个疑似内容"
                f"已隔离至 {receipt.get('quarantine_root')}（附 manifest）"
            )
        # The disc-expansion staging tree is task-owned staging, exactly like
        # the intake tree (same operator ruling: 终态根源树一律消费).  The
        # writer already moved every verified payload into the formal
        # library; consume it in the same terminal pass so it cannot linger.
        # Without this call the staging tree survives every terminal root.
        staging_note = _cleanup_expansion_staging_root(
            runner, state_root, job, pause_requested=pause_requested,
        )
        if staging_note is not None:
            return staging_note
        return None
    failures = receipt.get("failures")
    detail = f"，失败 {len(failures)} 项" if isinstance(failures, list) and failures else ""
    _trace(f"root {job.id} 源树仍有残留{detail}")
    return f"收官清源未完成：源树仍有残留{detail}，可重跑 consume-source"


def consume_terminal_source_root(
    runner: SimpleEngineRunner,
    state_root: Path,
    job: EngineJob,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, object]:
    """Operator-triggered source consumption for one already-terminal root.

    The same engine cleanup the pipeline runs automatically, exposed as an
    explicit idempotent action for historical terminal roots (and for
    re-running a consumption that previously left residuals).  The persisted
    phase never changes; the informational residual note is written or
    cleared on the job, and the full receipt is returned to the caller.
    """
    if job.phase not in {"completed", GAPS_PENDING_PHASE}:
        raise EngineRequestError(
            f"只有终态根（completed/gaps_pending）才能清源，当前: {job.phase}"
        )
    source = runner._job_ingress_source(job)  # noqa: SLF001 - pipeline composition
    receipt: dict[str, object] = {}
    note = _terminal_source_cleanup(
        runner, state_root, job, source,
        pause_requested=pause_requested, receipt_out=receipt,
    )
    updated = _persist_root(runner, job, job.phase, error=note)
    return {
        "job_id": job.id,
        "phase": updated.phase,
        "source": source,
        "source_remaining": bool(receipt.get("source_remaining")),
        "note": note,
        "quarantine_root": receipt.get("quarantine_root"),
        "quarantined": receipt.get("quarantined") or [],
        "quarantined_count": receipt.get("quarantined_count") or 0,
    }


def run_root_pipeline(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> EngineJob:
    """Run one idempotent B/W/C/D -> E/F/G/H/J -> R pass for a root task.

    Raises on transient AList/TMDB failures so the caller's bounded retry
    boundary applies; business-level uncertainty is parked, never raised.
    """
    job = runner.get_job(root_task_id)
    if job.phase not in RUNNABLE_PHASES:
        return job

    def cancelled() -> EngineJob | None:
        """Consume a root cancellation before a stale snapshot can persist."""
        return runner.consume_cancellation(root_task_id)

    def stopped() -> bool:
        """Use the normal pause boundary for either pause or cancellation."""
        if callable(pause_requested):
            try:
                if pause_requested():
                    return True
            except Exception:
                return True
        try:
            return runner.cancellation_pending(root_task_id)
        except Exception:
            return True

    cancelled_job = cancelled()
    if cancelled_job is not None:
        return cancelled_job
    # A selected RootJob can survive an API restart.  Its freshly constructed
    # AList client has no token yet, whereas B/W immediately performs a
    # remote listing.  Authenticate through the runner's single guarded
    # client before that read boundary; otherwise a resumed persisted root
    # fails with ``尚未登录 AList`` without ever reaching C/D.
    runner._ensure_authenticated(runner.alist)  # noqa: SLF001 - shared runner guard
    cancelled_job = cancelled()
    if cancelled_job is not None:
        return cancelled_job
    source = runner._job_ingress_source(job)  # noqa: SLF001 - pipeline composition

    # Opaque ISO/UDF/SFX/masquerade sources deliberately remain in the B/W
    # intake view.  The existing archive adapter can safely extract into a
    # task-owned staging root, but a RootJob still owns its original
    # IntakeSource path: F validates every WorkUnit source scope against that
    # immutable ingress.  Replacing ``source`` here would therefore create a
    # snapshot rooted at staging while ownership/cleanup still pointed at the
    # original source.  Until a durable expansion-to-new-SourceSnapshot
    # bridge exists, invoking the adapter here would not be a safe pre-B/W
    # optimization.  B/W records opaque containers as explicit attention and
    # F refuses them before any planner/writer side effect.
    if not load_work_unit_records(state_root, root_task_id):
        analyze_root_boundaries(
            runner.alist, source, root_task_id=root_task_id, state_root=state_root,
        )
    else:
        # A persisted ledger normally wins so nothing in flight is re-keyed.
        # But a root parked in C/U or D has written nothing, and reusing its
        # ledger also pins the boundary the engine produced back then — a
        # later generic B/W fix could never reach it and a retry would repeat
        # the same verdict forever.  The rebuild is refused as soon as any
        # unit carries a write-side fact, and durable operator confirmations
        # survive only on an identical unit key/scope.
        rebuild_root_boundary_if_unwritten(
            runner.alist, source, root_task_id=root_task_id, state_root=state_root,
        )
    cancelled_job = cancelled()
    if cancelled_job is not None:
        return cancelled_job
    records = load_work_unit_records(state_root, root_task_id)
    if not records:
        # An empty source has no work units; the root is complete with
        # nothing to write.
        return _persist_root(runner, job, "completed")

    # X: read-only disc-image expansion.  B/W parks every scope that holds
    # an optical-disc image behind an inspection requirement; the bridge
    # proves the playlist→episode mapping from engine-own evidence (plus a
    # filed operator ruling that still cannot contradict the discs' own
    # durations), transfers the proven payloads into task-owned staging
    # resumably, and replaces the parked record with ordinary staged-tree
    # records whose scopes the merged B snapshot proves.  A scope that
    # cannot be proven stays parked as visible root-level attention, and
    # its siblings proceed undisturbed.
    expansion_snapshot = load_source_snapshot(state_root, root_task_id)
    if expansion_snapshot is not None:
        try:
            expand_root_disc_images(
                runner.alist,
                runner.tmdb,
                state_root,
                root_task_id,
                records,
                expansion_snapshot,
                media_root=runner.library_root,
                prefer_animation=(job.target_shelf == "anime"),
                pause_requested=stopped,
            )
        except DiscExpansionPauseRequested:
            # A paused expansion deliberately leaves the parked record and
            # the per-mapping transfer states for fresh-pass recovery.
            cancelled_job = cancelled()
            if cancelled_job is not None:
                return cancelled_job
            return job
        cancelled_job = cancelled()
        if cancelled_job is not None:
            return cancelled_job
        records = load_work_unit_records(state_root, root_task_id)

    # C/U: confirmed records (including durable operator overrides) are
    # untouched; only pending units are resolved.
    resolve_work_unit_identities(
        runner.tmdb,
        state_root,
        root_task_id,
        prefer_animation=(job.target_shelf == "anime"),
    )
    cancelled_job = cancelled()
    if cancelled_job is not None:
        return cancelled_job
    records = load_work_unit_records(state_root, root_task_id)

    # C-stage normalization: conservatively combine only same-TMDB TV
    # siblings whose explicit season directory scopes and file markers agree.
    # This happens before D creates any reconciliation decision and before F
    # can create a writer carrier, so no completed work is ever re-keyed.
    records = _coalesce_confirmed_tv_season_records(
        state_root, root_task_id, records,
    )

    # D: reconcile every independently confirmed unit even when a sibling is
    # parked in C/U.  A source container may legitimately carry a main TV
    # work plus an ambiguous special/spinoff; the latter must remain visible
    # as attention without preventing the confirmed sibling from following
    # the ordinary D→Planner→writer path.
    known_gap_tokens = compute_known_gap_tokens(state_root)
    reconcile_root_work_units(
        runner.alist,
        runner.library_root,
        state_root,
        root_task_id,
        known_gap_tokens_by_identity=known_gap_tokens,
        # D may use this read-through catalog only for explicit B/W evidence:
        # a declared empty season or a complete naked-E source whose show
        # detail proves exactly one positive season.  Any missing/malformed
        # answer remains fail-closed instead of allowing duplicate consumption.
        episode_catalog=TmdbEpisodeCatalog(runner.tmdb),
        tmdb_client=runner.tmdb,
    )
    cancelled_job = cancelled()
    if cancelled_job is not None:
        return cancelled_job
    records = load_work_unit_records(state_root, root_task_id)
    # A D/U result belongs to that one WorkUnit.  Do not short-circuit the
    # root here: E/F/G/H can still safely consume or write independently
    # reconciled siblings, and R will retain the uncertain record as visible
    # root-level attention after those siblings finish.  This mirrors C/U's
    # nonblocking behavior without treating a D-uncertain unit as E-lane work.

    # Pause/cancel boundary: everything below may move media or write the
    # formal library.
    if stopped():
        cancelled_job = cancelled()
        if cancelled_job is not None:
            return cancelled_job
        return job

    lane_records = [
        record
        for record in records
        if record.reconciliation_outcome != "new_work"
    ]
    if lane_records:
        # E1/E2/E3 per unit: archive consumption, gap registration/hold,
        # merge into the locked existing work root.
        try:
            execute_unit_e_lanes(
                runner, state_root, root_task_id, pause_requested=stopped,
            )
        except EnginePauseRequested:
            # A paused E lane deliberately leaves its durable ledger/carrier
            # for fresh-state recovery.  It is not a business failure.
            cancelled_job = cancelled()
            if cancelled_job is not None:
                return cancelled_job
            return job
        except Exception as exc:
            return _persist_root(
                runner, job, "failed",
                error=f"单元 E 通道执行失败: {redact_error(exc)}",
            )
        _refresh_lane_acceptance(
            state_root,
            root_task_id,
            load_work_unit_records(state_root, root_task_id),
        )
        cancelled_job = cancelled()
        if cancelled_job is not None:
            return cancelled_job

    new_work_records = [
        record
        for record in records
        if record.reconciliation_outcome == "new_work"
    ]
    if new_work_records:
        # F/G/H/J: single planner, single writer, typed acceptance, gap ledger.
        try:
            results = execute_new_work_units(
                runner,
                state_root,
                root_task_id,
                pause_requested=stopped,
            )
        except EnginePauseRequested:
            cancelled_job = cancelled()
            if cancelled_job is not None:
                return cancelled_job
            return job
        failed = [result for result in results if result.outcome == "failed"]
        if failed:
            return _persist_root(
                runner, job, "failed",
                error=f"{len(failed)} 个作品单元执行失败，等待重试",
            )

    # A pure series container is itself a visible library item.  Its child
    # WorkUnits own the TMDB identities, but the container root still needs a
    # poster and NFO.  The helper uses the existing single writer through a
    # deterministic internal artifact carrier; it never moves source media.
    try:
        ensure_container_artifacts(
            runner,
            state_root,
            root_task_id,
            pause_requested=stopped,
        )
    except EnginePauseRequested:
        cancelled_job = cancelled()
        if cancelled_job is not None:
            return cancelled_job
        return job
    except ContainerMetadataAttention as exc:
        return _persist_root(
            runner,
            job,
            PARK_PHASE,
            error=f"容器根元数据需要确认: {redact_error(exc)}",
        )
    except Exception as exc:
        return _persist_root(
            runner,
            job,
            "failed",
            error=f"容器根元数据写入/回读失败: {redact_error(exc)}",
        )

    cancelled_job = cancelled()
    if cancelled_job is not None:
        return cancelled_job

    # R: aggregate the ledger into the durable root phase.
    aggregate = aggregate_root_job(state_root, root_task_id)
    if aggregate.failed:
        return _persist_root(runner, job, "failed", error="存在失败的作品单元")
    # A real technical failure (including a J ledger persistence/readback
    # fault) must never be hidden behind a separate evidence attention.
    # Operators need the explicit failed result to distinguish repair work
    # from an ordinary "需要确认" pause.
    if aggregate.attention:
        return _persist_root(runner, job, PARK_PHASE)
    if aggregate.in_progress:
        return _persist_root(
            runner, job, PARK_PHASE,
            error="部分作品单元尚未完成，等待继续处理",
        )
    # H can have completed the formal media write and exact readback while J
    # has deliberately left one or more precise coordinates open.  Those
    # gaps are a normal N-step hand-off to the selected RootJob's two-tier
    # replenishment lane, not a completed root.
    if aggregate.open_gaps:
        # Operator ruling 2026-09-02: ``gaps_pending`` is equally terminal for
        # the intake tree.  The gap ledger is the durable record (缺口只登
        # 不补) and no lane ever re-reads the staging tree, so the same
        # consumption applies before the phase is persisted.
        residual_note = None
        if not stopped():
            residual_note = _terminal_source_cleanup(
                runner, state_root, job, source, pause_requested=stopped
            )
        cancelled_job = cancelled()
        if cancelled_job is not None:
            return cancelled_job
        return _persist_root(runner, job, GAPS_PENDING_PHASE, error=residual_note)
    # Post-completion housekeeping: delete the root's entire intake tree,
    # residual resources included (operator ruling 2026-08-27 — the intake
    # area is staging, not storage).  Best-effort: a provider that keeps
    # reporting the tree leaves a residual note for a consume-source re-run,
    # and a paused run skips remote deletes entirely.
    residual_note = None
    if not stopped():
        residual_note = _terminal_source_cleanup(
            runner, state_root, job, source, pause_requested=stopped
        )
    cancelled_job = cancelled()
    if cancelled_job is not None:
        return cancelled_job
    return _persist_root(runner, job, "completed", error=residual_note)


def finalize_root_gap_closure(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> EngineJob:
    """Close a pending root only after N has durably closed every J Gap.

    This re-reads only the durable ledgers and changes no media.  It is the
    one R-node transition called after a replenishment turn: a successful
    H readback alone is never sufficient to mark the root complete.
    """
    job = runner.get_job(root_task_id)
    aggregate = aggregate_root_job(state_root, root_task_id)
    if aggregate.failed:
        if job.phase == GAPS_PENDING_PHASE:
            return _persist_root(runner, job, "failed", error="存在失败的作品单元")
        return job
    if aggregate.attention or aggregate.in_progress or aggregate.open_gaps:
        if job.phase == GAPS_PENDING_PHASE and (aggregate.attention or aggregate.in_progress):
            return _persist_root(runner, job, PARK_PHASE)
        return job
    if job.phase != GAPS_PENDING_PHASE:
        return job
    # Post-completion housekeeping, identical to the direct-completion path:
    # a root whose gaps closed must not keep its intake tree forever (the
    # operator ruling applies to every completed root, regardless of which
    # path completed it).  Best-effort, never blocking the phase transition;
    # a surviving residual becomes the job's informational note instead of
    # being swallowed.
    residual_note = _terminal_source_cleanup(
        runner,
        state_root,
        job,
        runner._job_ingress_source(job),  # noqa: SLF001 - pipeline composition
        pause_requested=pause_requested,
    )
    return _persist_root(runner, job, "completed", error=residual_note)


def refresh_root_after_j_rereview(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
) -> EngineJob:
    """Run only the existing R aggregation after a J-only carrier rereview.

    No source discovery, reconciliation, planning, writer, cleanup, or
    provider action happens here.  It simply projects the one durable Gap
    ledger back onto the ordinary completed/gaps_pending/attention/failed
    root phases.
    """
    job = runner.get_job(root_task_id)
    aggregate = aggregate_root_job(state_root, root_task_id)
    if aggregate.failed:
        return _persist_root(runner, job, "failed", error="存在失败的作品单元")
    if aggregate.attention:
        return _persist_root(runner, job, PARK_PHASE)
    if aggregate.in_progress:
        return _persist_root(
            runner,
            job,
            PARK_PHASE,
            error="部分作品单元尚未完成，等待继续处理",
        )
    if aggregate.open_gaps:
        return _persist_root(runner, job, GAPS_PENDING_PHASE)
    return _persist_root(runner, job, "completed")


__all__ = [
    "PARK_PHASE",
    "GAPS_PENDING_PHASE",
    "RUNNABLE_PHASES",
    "finalize_root_gap_closure",
    "is_intake_bound_root",
    "refresh_root_after_j_rereview",
    "run_root_pipeline",
]
