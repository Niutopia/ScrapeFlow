#!/usr/bin/env python3
"""Migrate paused, offline ScrapeFlow job state to schema version 2.

The migration is deliberately local and fail-closed.  It never imports the
API, opens a socket, or touches anything outside ``--state-root``.  A dry run
is the default.  A real run additionally requires both ``--apply`` and the
operator assertion ``--confirm-api-stopped``.

Before the first mutation an atomic manifest records every ``jobs/*/job.json``
SHA, its action and its destination.  A superseded job directory is renamed
whole into the v1 archive on the same filesystem.  Live job records only lose
the explicitly enumerated v1 history fields in ``plan``.  On a retry, original
and projected SHAs (or source/archive location) are the only accepted states;
every ambiguous state is rejected.
"""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Mapping, Sequence


MIGRATION_NAME = "job-state-v2"
MANIFEST_VERSION = 1
TARGET_SCHEMA_VERSION = 2
MANIFEST_RELATIVE_PATH = Path("migrations") / MIGRATION_NAME / "manifest.json"
SCHEMA_RELATIVE_PATH = Path("state-schema.json")
ARCHIVE_RELATIVE_ROOT = Path("archive") / "job-state-v1"
JOB_ID_RE = re.compile(r"^[0-9a-f]{12}$")
MIGRATION_ID_RE = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{12}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# These keys describe the removed v1 recovery/supersession coordinator.  They
# are intentionally top-level-only: nested acquisition or subtitle evidence
# may use similar language but remains business evidence and must survive.
LEGACY_PLAN_FIELDS = (
    "recovery_create_only",
    "superseded_by_job_id",
    "superseded_from_phase",
    "superseded_at",
    "supersession_kind",
    "supersession_provenance",
    "supersedes_job_id",
)


class MigrationError(RuntimeError):
    """State failed a migration safety invariant."""


class _DuplicateJsonKey(ValueError):
    pass


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKey(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _decode_json(raw: bytes, path: Path) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
        value = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey) as exc:
        raise MigrationError(f"invalid JSON document: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise MigrationError(f"expected a JSON object: {path}")
    return value


def _read_json(path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        if path.is_symlink() or not path.is_file():
            raise MigrationError(f"expected a regular file, not a symlink: {path}")
        raw = path.read_bytes()
    except OSError as exc:
        raise MigrationError(f"cannot read {path}: {exc}") from exc
    return _decode_json(raw, path), raw


def _encode_json(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    try:
        if path.is_symlink() or not path.is_file():
            raise MigrationError(f"expected a regular file, not a symlink: {path}")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as exc:
        raise MigrationError(f"cannot hash {path}: {exc}") from exc


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, data: bytes, *, mode: int = 0o600) -> None:
    """Durably replace a file without exposing a partial JSON document."""
    if path.parent.is_symlink() or not path.parent.is_dir():
        raise MigrationError(f"atomic-write parent is missing or unsafe: {path.parent}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_write(path, _encode_json(value))


def _state_root(path: Path) -> Path:
    try:
        root = path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise MigrationError(f"state root does not exist: {path}: {exc}") from exc
    if not root.is_dir():
        raise MigrationError(f"state root is not a directory: {root}")
    return root


def _relative(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError as exc:
        raise MigrationError(f"path escapes state root: {path}") from exc


def _safe_directory_chain(root: Path, directory: Path, *, create: bool) -> bool:
    """Validate every parent component and optionally create missing ones."""
    relative = _relative(root, directory)
    current = root
    if relative == ".":
        return True
    for part in Path(relative).parts:
        parent = current
        current = current / part
        if os.path.lexists(current):
            if current.is_symlink() or not current.is_dir():
                raise MigrationError(f"unsafe state directory component: {current}")
            continue
        if not create:
            return False
        try:
            current.mkdir(mode=0o700)
            _fsync_directory(parent)
        except OSError as exc:
            raise MigrationError(f"cannot create state directory {current}: {exc}") from exc
    return True


def _path_from_relative(root: Path, value: Any, *, label: str) -> Path:
    if not isinstance(value, str) or not value or value.startswith("/"):
        raise MigrationError(f"invalid {label} in migration manifest")
    relative = Path(value)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise MigrationError(f"unsafe {label} in migration manifest: {value!r}")
    path = root.joinpath(relative)
    # Do not call resolve(): archived targets need not exist.  Lexical checks
    # above plus a trusted, already-resolved root keep this path contained.
    if _relative(root, path) != relative.as_posix():
        raise MigrationError(f"non-canonical {label} in migration manifest: {value!r}")
    return path


def _require_paused(root: Path) -> tuple[dict[str, Any], bytes]:
    path = root / "global-control.json"
    control, raw = _read_json(path)
    if control.get("version") != 1:
        raise MigrationError(f"unsupported global-control.json version: {path}")
    if control.get("paused") is not True:
        raise MigrationError(
            f"refusing migration while durable global pause is not active: {path}"
        )
    if not isinstance(control.get("updated_at"), str):
        raise MigrationError(f"invalid global-control.json updated_at: {path}")
    reason = control.get("reason")
    if reason is not None and not isinstance(reason, str):
        raise MigrationError(f"invalid global-control.json reason: {path}")
    return control, raw


def _assert_control_unchanged(root: Path, expected_sha256: str) -> None:
    _control, raw = _require_paused(root)
    actual = _sha256_bytes(raw)
    if actual != expected_sha256:
        raise MigrationError(
            "global-control.json changed after migration planning; refusing to continue"
        )


def _project_job(
    payload: Mapping[str, Any], *, path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    projected = copy.deepcopy(dict(payload))
    plan = projected.get("plan")
    if plan is None:
        return projected, {}
    if not isinstance(plan, dict):
        raise MigrationError(f"job plan must be an object or null: {path}")
    removed: dict[str, Any] = {}
    for field in LEGACY_PLAN_FIELDS:
        if field in plan:
            removed[field] = plan.pop(field)
    return projected, removed


def _schema_before(root: Path) -> dict[str, Any]:
    path = root / SCHEMA_RELATIVE_PATH
    if not path.exists():
        return {"exists": False, "sha256": None, "version": None}
    schema, raw = _read_json(path)
    version = schema.get("version")
    if type(version) is not int:
        raise MigrationError(f"state-schema.json version must be an integer: {path}")
    if version == TARGET_SCHEMA_VERSION:
        raise MigrationError(
            "state is already schema version 2 but has no matching migration manifest"
        )
    if version != 1:
        raise MigrationError(f"unsupported existing state schema version {version}: {path}")
    return {"exists": True, "sha256": _sha256_bytes(raw), "version": version}


def _new_migration_id(entries: Sequence[Mapping[str, Any]]) -> str:
    identity = "\n".join(
        f"{entry['job_id']}:{entry['original_sha256']}:{entry['action']}"
        for entry in entries
    ).encode("utf-8")
    suffix = _sha256_bytes(identity)[:12]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{suffix}"


def _scan_jobs(root: Path) -> list[dict[str, Any]]:
    jobs_root = root / "jobs"
    if jobs_root.is_symlink() or not jobs_root.is_dir():
        raise MigrationError(f"jobs directory is missing or unsafe: {jobs_root}")

    entries: list[dict[str, Any]] = []
    try:
        children = sorted(jobs_root.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise MigrationError(f"cannot list jobs directory: {jobs_root}: {exc}") from exc
    for directory in children:
        if directory.name.startswith(".") and not directory.is_dir():
            continue
        if not JOB_ID_RE.fullmatch(directory.name):
            raise MigrationError(f"unexpected entry in jobs directory: {directory}")
        if directory.is_symlink() or not directory.is_dir():
            raise MigrationError(f"job entry is not a regular directory: {directory}")
        job_path = directory / "job.json"
        payload, raw = _read_json(job_path)
        if payload.get("id") != directory.name:
            raise MigrationError(f"job id/path mismatch: {job_path}")
        phase = payload.get("phase")
        if not isinstance(phase, str) or not phase:
            raise MigrationError(f"job phase is missing or invalid: {job_path}")

        original_sha = _sha256_bytes(raw)
        projected, removed = _project_job(payload, path=job_path)
        if phase == "superseded":
            action = "archive"
            target = ""  # Filled after the migration id is known.
            projected_sha: str | None = None
        elif removed:
            action = "project"
            target = _relative(root, job_path)
            projected_sha = _sha256_bytes(_encode_json(projected))
        else:
            action = "keep"
            target = _relative(root, job_path)
            projected_sha = None
        entry: dict[str, Any] = {
            "job_id": directory.name,
            "phase": phase,
            "source": _relative(root, directory),
            "job_json": _relative(root, job_path),
            "original_sha256": original_sha,
            "action": action,
            "target": target,
            "removed_plan_fields": removed,
        }
        if projected_sha is not None:
            entry["projected_sha256"] = projected_sha
        entries.append(entry)
    return entries


def build_manifest(root: Path, *, control_sha256: str | None = None) -> dict[str, Any]:
    if control_sha256 is None:
        _control, control_raw = _require_paused(root)
        control_sha256 = _sha256_bytes(control_raw)
    entries = _scan_jobs(root)
    migration_id = _new_migration_id(entries)
    for entry in entries:
        if entry["action"] == "archive":
            target = (
                ARCHIVE_RELATIVE_ROOT / migration_id / "jobs" / str(entry["job_id"])
            )
            entry["target"] = target.as_posix()
    return {
        "manifest_version": MANIFEST_VERSION,
        "migration": MIGRATION_NAME,
        "migration_id": migration_id,
        "status": "planned",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "global_control_sha256": control_sha256,
        "state_schema_before": _schema_before(root),
        "entries": entries,
    }


def _validate_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise MigrationError(f"invalid {label} in migration manifest")
    return value


def _validate_manifest(root: Path, manifest: dict[str, Any]) -> None:
    if manifest.get("manifest_version") != MANIFEST_VERSION:
        raise MigrationError("unsupported migration manifest version")
    if manifest.get("migration") != MIGRATION_NAME:
        raise MigrationError("migration manifest has the wrong migration name")
    migration_id = manifest.get("migration_id")
    if not isinstance(migration_id, str) or not MIGRATION_ID_RE.fullmatch(migration_id):
        raise MigrationError("invalid migration id in manifest")
    if manifest.get("status") not in {"planned", "applying", "completed"}:
        raise MigrationError("invalid migration manifest status")
    if not isinstance(manifest.get("created_at"), str):
        raise MigrationError("migration manifest created_at is missing")
    _validate_sha(manifest.get("global_control_sha256"), "global control SHA")

    schema_before = manifest.get("state_schema_before")
    if not isinstance(schema_before, dict) or type(schema_before.get("exists")) is not bool:
        raise MigrationError("invalid state_schema_before in migration manifest")
    if schema_before["exists"]:
        _validate_sha(schema_before.get("sha256"), "prior state schema SHA")
        if schema_before.get("version") not in {1, TARGET_SCHEMA_VERSION}:
            raise MigrationError("invalid prior state schema version in manifest")
    elif schema_before.get("sha256") is not None or schema_before.get("version") is not None:
        raise MigrationError("invalid absent state_schema_before in manifest")

    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise MigrationError("migration manifest entries must be a list")
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise MigrationError("migration manifest entry must be an object")
        job_id = entry.get("job_id")
        if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
            raise MigrationError("invalid job id in migration manifest")
        if job_id in seen:
            raise MigrationError(f"duplicate job id in migration manifest: {job_id}")
        seen.add(job_id)
        source = _path_from_relative(root, entry.get("source"), label="job source")
        job_json = _path_from_relative(root, entry.get("job_json"), label="job JSON")
        if source != root / "jobs" / job_id or job_json != source / "job.json":
            raise MigrationError(f"non-canonical source paths for job {job_id}")
        original_sha = _validate_sha(entry.get("original_sha256"), "original job SHA")
        del original_sha
        action = entry.get("action")
        target = _path_from_relative(root, entry.get("target"), label="job target")
        if action == "archive":
            expected = root / ARCHIVE_RELATIVE_ROOT / migration_id / "jobs" / job_id
            if target != expected or entry.get("phase") != "superseded":
                raise MigrationError(f"invalid archive action for job {job_id}")
            if "projected_sha256" in entry:
                raise MigrationError(f"archive entry has a projected SHA: {job_id}")
        elif action in {"project", "keep"}:
            if target != job_json or entry.get("phase") == "superseded":
                raise MigrationError(f"invalid live action for job {job_id}")
            if action == "project":
                _validate_sha(entry.get("projected_sha256"), "projected job SHA")
            elif "projected_sha256" in entry:
                raise MigrationError(f"keep entry has a projected SHA: {job_id}")
        else:
            raise MigrationError(f"unknown migration action for job {job_id}: {action!r}")
        removed = entry.get("removed_plan_fields")
        if not isinstance(removed, dict) or any(key not in LEGACY_PLAN_FIELDS for key in removed):
            raise MigrationError(f"invalid removed plan fields for job {job_id}")
        if action == "project" and not removed:
            raise MigrationError(f"project entry has no removed fields: {job_id}")
        if action == "keep" and removed:
            raise MigrationError(f"keep entry unexpectedly removes fields: {job_id}")


def _read_manifest(root: Path) -> dict[str, Any] | None:
    path = root / MANIFEST_RELATIVE_PATH
    if not _safe_directory_chain(root, path.parent, create=False):
        return None
    if not path.exists():
        return None
    manifest, _raw = _read_json(path)
    _validate_manifest(root, manifest)
    return manifest


def _entry_state(root: Path, entry: Mapping[str, Any]) -> str:
    source = _path_from_relative(root, entry["source"], label="job source")
    job_path = _path_from_relative(root, entry["job_json"], label="job JSON")
    target = _path_from_relative(root, entry["target"], label="job target")
    action = entry["action"]
    original_sha = entry["original_sha256"]

    if action == "archive":
        source_exists = source.exists()
        target_exists = target.exists()
        if source_exists and target_exists:
            raise MigrationError(
                f"archive source and target both exist for {entry['job_id']}"
            )
        if not source_exists and not target_exists:
            raise MigrationError(
                f"archive source and target are both missing for {entry['job_id']}"
            )
        location = source if source_exists else target
        if location.is_symlink() or not location.is_dir():
            raise MigrationError(f"unsafe job directory at {location}")
        if location == target and not _safe_directory_chain(
            root, target.parent, create=False,
        ):
            raise MigrationError(f"archive parent disappeared for {entry['job_id']}")
        actual = _sha256_file(location / "job.json")
        if actual != original_sha:
            raise MigrationError(
                f"job SHA conflict for {entry['job_id']}: expected {original_sha}, got {actual}"
            )
        return "original" if source_exists else "archived"

    if not source.exists() or source.is_symlink() or not source.is_dir():
        raise MigrationError(f"live job directory is missing or unsafe: {source}")
    if target != job_path:
        raise MigrationError(f"live job target mismatch for {entry['job_id']}")
    actual = _sha256_file(job_path)
    if actual == original_sha:
        return "original"
    if action == "project" and actual == entry["projected_sha256"]:
        return "projected"
    raise MigrationError(
        f"job SHA conflict for {entry['job_id']}: expected original/projected SHA, got {actual}"
    )


def _validate_inventory(root: Path, manifest: Mapping[str, Any]) -> None:
    expected = {entry["job_id"] for entry in manifest["entries"]}
    jobs_root = root / "jobs"
    if jobs_root.is_symlink() or not jobs_root.is_dir():
        raise MigrationError(f"jobs directory is missing or unsafe: {jobs_root}")
    live: set[str] = set()
    for child in jobs_root.iterdir():
        if child.name.startswith(".") and not child.is_dir():
            continue
        if not JOB_ID_RE.fullmatch(child.name):
            raise MigrationError(f"unexpected entry in jobs directory: {child}")
        live.add(child.name)
    unknown = live - expected
    if unknown:
        raise MigrationError(
            "jobs appeared after the migration manifest was created: "
            + ", ".join(sorted(unknown))
        )

    archive_jobs = (
        root / ARCHIVE_RELATIVE_ROOT / str(manifest["migration_id"]) / "jobs"
    )
    if archive_jobs.exists():
        if archive_jobs.is_symlink() or not archive_jobs.is_dir():
            raise MigrationError(f"unsafe migration archive directory: {archive_jobs}")
        archived = {child.name for child in archive_jobs.iterdir()}
        unknown_archived = archived - expected
        if unknown_archived:
            raise MigrationError(
                "unexpected jobs exist in this migration archive: "
                + ", ".join(sorted(unknown_archived))
            )


def _validate_schema_position(root: Path, manifest: Mapping[str, Any]) -> str:
    path = root / SCHEMA_RELATIVE_PATH
    before = manifest["state_schema_before"]
    if not path.exists():
        if before["exists"]:
            raise MigrationError("state-schema.json disappeared after planning")
        return "before"
    schema, raw = _read_json(path)
    if (
        schema.get("version") == TARGET_SCHEMA_VERSION
        and schema.get("migration") == MIGRATION_NAME
        and schema.get("migration_id") == manifest["migration_id"]
    ):
        return "completed"
    actual_sha = _sha256_bytes(raw)
    if before["exists"] and actual_sha == before["sha256"]:
        return "before"
    raise MigrationError("state-schema.json conflicts with the migration manifest")


def _preflight(root: Path, manifest: Mapping[str, Any]) -> dict[str, int]:
    _validate_inventory(root, manifest)
    counts = {"original": 0, "projected": 0, "archived": 0}
    for entry in manifest["entries"]:
        counts[_entry_state(root, entry)] += 1
    _validate_schema_position(root, manifest)
    return counts


def _apply_entry(root: Path, entry: Mapping[str, Any]) -> str:
    state = _entry_state(root, entry)
    action = entry["action"]
    if action == "keep":
        return "unchanged"
    if action == "project":
        if state == "projected":
            return "already_projected"
        path = _path_from_relative(root, entry["job_json"], label="job JSON")
        payload, raw = _read_json(path)
        if _sha256_bytes(raw) != entry["original_sha256"]:
            raise MigrationError(f"job changed during projection: {entry['job_id']}")
        projected, removed = _project_job(payload, path=path)
        if removed != entry["removed_plan_fields"]:
            raise MigrationError(f"job projection differs from manifest: {entry['job_id']}")
        projected_raw = _encode_json(projected)
        if _sha256_bytes(projected_raw) != entry["projected_sha256"]:
            raise MigrationError(f"projected SHA differs from manifest: {entry['job_id']}")
        mode = path.stat().st_mode & 0o777
        _atomic_write(path, projected_raw, mode=mode)
        if _sha256_file(path) != entry["projected_sha256"]:
            raise MigrationError(f"projected job verification failed: {entry['job_id']}")
        return "projected"

    if state == "archived":
        return "already_archived"
    source = _path_from_relative(root, entry["source"], label="job source")
    target = _path_from_relative(root, entry["target"], label="job target")
    _safe_directory_chain(root, target.parent, create=True)
    if source.parent.stat().st_dev != target.parent.stat().st_dev:
        raise MigrationError(
            f"archive target is not on the source filesystem: {entry['job_id']}"
        )
    # Re-check after creating the destination parents and immediately before
    # the atomic directory rename.
    if _entry_state(root, entry) != "original":
        raise MigrationError(f"job moved during archive preparation: {entry['job_id']}")
    os.replace(source, target)
    _fsync_directory(source.parent)
    _fsync_directory(target.parent)
    if _entry_state(root, entry) != "archived":
        raise MigrationError(f"archived job verification failed: {entry['job_id']}")
    return "archived"


def _write_schema(root: Path, manifest: Mapping[str, Any]) -> None:
    position = _validate_schema_position(root, manifest)
    if position == "completed":
        return
    _atomic_json(root / SCHEMA_RELATIVE_PATH, {
        "version": TARGET_SCHEMA_VERSION,
        "migration": MIGRATION_NAME,
        "migration_id": manifest["migration_id"],
        "manifest": MANIFEST_RELATIVE_PATH.as_posix(),
        "migrated_at": datetime.now(timezone.utc).isoformat(),
    })


def _summary(manifest: Mapping[str, Any], *, mode: str) -> dict[str, Any]:
    counts = {"archive": 0, "project": 0, "keep": 0}
    phases: dict[str, int] = {}
    for entry in manifest["entries"]:
        counts[entry["action"]] += 1
        phase = str(entry["phase"])
        phases[phase] = phases.get(phase, 0) + 1
    return {
        "mode": mode,
        "migration": MIGRATION_NAME,
        "migration_id": manifest["migration_id"],
        "manifest": MANIFEST_RELATIVE_PATH.as_posix(),
        "jobs": len(manifest["entries"]),
        "actions": counts,
        "phases": dict(sorted(phases.items())),
    }


def run_migration(
    state_root: Path, *, apply: bool = False, confirm_api_stopped: bool = False,
) -> dict[str, Any]:
    """Plan or apply the migration and return a machine-readable summary."""
    root = _state_root(state_root)
    _control, control_raw = _require_paused(root)
    control_sha = _sha256_bytes(control_raw)

    manifest = _read_manifest(root)
    is_new = manifest is None
    if manifest is None:
        manifest = build_manifest(root, control_sha256=control_sha)
        _validate_manifest(root, manifest)
    elif manifest["global_control_sha256"] != control_sha:
        raise MigrationError(
            "global-control.json differs from the SHA recorded in the migration manifest"
        )
    _preflight(root, manifest)

    if not apply:
        return _summary(manifest, mode="dry-run")
    if not confirm_api_stopped:
        raise MigrationError(
            "--apply requires --confirm-api-stopped; stop the API and all "
            "ScrapeFlow containers before confirming"
        )

    _assert_control_unchanged(root, manifest["global_control_sha256"])
    manifest_path = root / MANIFEST_RELATIVE_PATH
    if is_new:
        if manifest_path.exists():
            raise MigrationError("migration manifest appeared during planning")
        manifest["status"] = "applying"
        _safe_directory_chain(root, manifest_path.parent, create=True)
        _atomic_json(manifest_path, manifest)
    elif manifest["status"] == "planned":
        manifest["status"] = "applying"
        _atomic_json(manifest_path, manifest)

    if manifest["status"] == "completed":
        if _validate_schema_position(root, manifest) != "completed":
            raise MigrationError("completed manifest has no matching v2 state schema")
        return _summary(manifest, mode="already-completed")

    # Validate every record before any job mutation.  Each entry is then
    # checked again immediately before its own atomic operation.
    _preflight(root, manifest)
    for entry in manifest["entries"]:
        _assert_control_unchanged(root, manifest["global_control_sha256"])
        _apply_entry(root, entry)

    _assert_control_unchanged(root, manifest["global_control_sha256"])
    positions = _preflight(root, manifest)
    if positions["original"] != sum(
        1 for entry in manifest["entries"] if entry["action"] == "keep"
    ):
        raise MigrationError("not every mutable job reached its v2 position")
    _write_schema(root, manifest)
    manifest["status"] = "completed"
    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    _atomic_json(manifest_path, manifest)
    return _summary(manifest, mode="applied")


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument(
        "--state-root", type=Path, required=True,
        help="directory containing global-control.json and jobs/",
    )
    value.add_argument(
        "--apply", action="store_true",
        help="perform the migration (the default is a zero-write dry run)",
    )
    value.add_argument(
        "--confirm-api-stopped", action="store_true",
        help="assert that the API and every ScrapeFlow container are stopped",
    )
    return value


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        result = run_migration(
            arguments.state_root,
            apply=arguments.apply,
            confirm_api_stopped=arguments.confirm_api_stopped,
        )
    except (MigrationError, OSError) as exc:
        print(f"job-state-v2 migration refused: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    if not arguments.apply:
        print(
            "dry run only; no files were written. Stop the API/containers, then "
            "repeat with --apply --confirm-api-stopped to migrate.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
