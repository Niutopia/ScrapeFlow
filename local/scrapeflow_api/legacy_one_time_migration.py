"""Offline retirement of the removed resident one-time owner control plane.

Planning is read-only by default.  Applying or restoring a sealed plan is an
explicit offline operation guarded by a durable pause, a byte-for-byte host
backup, per-directory compare-and-swap evidence and the operator-approved plan
SHA.  No function in this module imports or starts the Local API runtime.
"""

from __future__ import annotations

import base64
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import tempfile
from typing import Any, Mapping, Sequence


LEGACY_ONE_TIME_APPROVAL_SOURCE = "one_time_title_import"
PLAN_KIND = "legacy_one_time_owner_retirement_plan"
PLAN_SCHEMA_VERSION = 1
JOURNAL_KIND = "legacy_one_time_owner_retirement_journal"
JOURNAL_SCHEMA_VERSION = 1
ARCHIVE_RELATIVE_ROOT = Path("archive") / "legacy-one-time-owners"
JOURNAL_RELATIVE_ROOT = Path("migrations") / "legacy-one-time-owners"
RETIRED_ROOT_INDEX_RELATIVE_PATH = Path("retired-one-time-roots.json")
JOB_ID_RE = re.compile(r"^[0-9a-f]{12}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MIGRATION_ID_RE = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{12}$")
BACKUP_SNAPSHOT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{6}[+-]\d{4}$")
LEGACY_ARTIFACT_NAMES = frozenset({
    "one-time-import-origin.json",
    "one-time-import-journal.json",
    "one-time-import-preparation.json",
    "sealed-worklist.json",
    "sealed-title-scope.json",
    "sealed-title-closure.json",
    "fresh-title-closure.json",
})
REQUIRED_LEGACY_ARTIFACTS = (
    "one-time-import-origin.json",
    "one-time-import-journal.json",
    "one-time-import-preparation.json",
)
TERMINAL_PHASES = frozenset({"completed", "failed", "cancelled", "recovered"})


class LegacyOneTimeMigrationError(RuntimeError):
    """A retirement safety invariant was not proven."""


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
        value = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey) as exc:
        raise LegacyOneTimeMigrationError(
            f"invalid JSON document {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise LegacyOneTimeMigrationError(f"expected a JSON object: {path}")
    return value


def _read_json(path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        if path.is_symlink() or not path.is_file():
            raise LegacyOneTimeMigrationError(
                f"expected a regular JSON file, not a symlink: {path}"
            )
        raw = path.read_bytes()
    except OSError as exc:
        raise LegacyOneTimeMigrationError(f"cannot read {path}: {exc}") from exc
    return _decode_json(raw, path), raw


def _encode_json(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _canonical_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    try:
        if path.is_symlink() or not path.is_file():
            raise LegacyOneTimeMigrationError(
                f"expected a regular file, not a symlink: {path}"
            )
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as exc:
        raise LegacyOneTimeMigrationError(f"cannot hash {path}: {exc}") from exc


def _parse_time(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise LegacyOneTimeMigrationError(f"{label} is missing or invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise LegacyOneTimeMigrationError(f"{label} is invalid: {value!r}") from exc
    if parsed.tzinfo is None:
        raise LegacyOneTimeMigrationError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _state_root(path: Path) -> Path:
    try:
        root = path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise LegacyOneTimeMigrationError(
            f"state root does not exist: {path}: {exc}"
        ) from exc
    if not root.is_dir():
        raise LegacyOneTimeMigrationError(f"state root is not a directory: {root}")
    return root


def _relative(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError as exc:
        raise LegacyOneTimeMigrationError(f"path escapes state root: {path}") from exc


def _path_from_relative(root: Path, value: Any, *, label: str) -> Path:
    if not isinstance(value, str) or not value or value.startswith(("/", "\\")):
        raise LegacyOneTimeMigrationError(f"invalid {label} in retirement plan")
    relative = PurePosixPath(value)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise LegacyOneTimeMigrationError(f"unsafe {label}: {value!r}")
    path = root.joinpath(*relative.parts)
    if _relative(root, path) != value:
        raise LegacyOneTimeMigrationError(f"non-canonical {label}: {value!r}")
    return path


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_directory_chain(root: Path, directory: Path, *, create: bool) -> bool:
    relative = _relative(root, directory)
    current = root
    if relative == ".":
        return True
    for part in Path(relative).parts:
        parent = current
        current = current / part
        if os.path.lexists(current):
            if current.is_symlink() or not current.is_dir():
                raise LegacyOneTimeMigrationError(
                    f"unsafe state directory component: {current}"
                )
            continue
        if not create:
            return False
        try:
            current.mkdir(mode=0o700)
            _fsync_directory(parent)
        except OSError as exc:
            raise LegacyOneTimeMigrationError(
                f"cannot create state directory {current}: {exc}"
            ) from exc
    return True


def _atomic_write(path: Path, data: bytes, *, mode: int = 0o600) -> None:
    if path.parent.is_symlink() or not path.parent.is_dir():
        raise LegacyOneTimeMigrationError(
            f"atomic-write parent is missing or unsafe: {path.parent}"
        )
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
    _atomic_write(path, _encode_json(value), mode=0o600)


def _require_paused(root: Path) -> tuple[dict[str, Any], bytes]:
    path = root / "global-control.json"
    control, raw = _read_json(path)
    if control.get("version") != 1:
        raise LegacyOneTimeMigrationError(
            f"unsupported global-control.json version: {path}"
        )
    if control.get("paused") is not True:
        raise LegacyOneTimeMigrationError(
            f"durable global pause is required: {path}"
        )
    _parse_time(control.get("updated_at"), "global-control updated_at")
    reason = control.get("reason")
    if reason is not None and not isinstance(reason, str):
        raise LegacyOneTimeMigrationError("global-control reason is invalid")
    return control, raw


def _directory_snapshot(
    directory: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, bytes]]:
    if directory.is_symlink() or not directory.is_dir():
        raise LegacyOneTimeMigrationError(f"unsafe job directory: {directory}")
    rows: list[dict[str, Any]] = []
    directory_rows: list[dict[str, Any]] = [{
        "path": ".",
        "mode": stat.S_IMODE(directory.stat().st_mode),
    }]
    raw_by_path: dict[str, bytes] = {}
    try:
        for walk_root, directories, files in os.walk(directory, followlinks=False):
            directories.sort()
            files.sort()
            walk_path = Path(walk_root)
            for name in directories:
                child = walk_path / name
                if child.is_symlink() or not child.is_dir():
                    raise LegacyOneTimeMigrationError(
                        f"unsafe directory in legacy owner: {child}"
                    )
                directory_rows.append({
                    "path": child.relative_to(directory).as_posix(),
                    "mode": stat.S_IMODE(child.stat().st_mode),
                })
            for name in files:
                child = walk_path / name
                metadata = child.lstat()
                if child.is_symlink() or not stat.S_ISREG(metadata.st_mode):
                    raise LegacyOneTimeMigrationError(
                        f"unsafe file in legacy owner: {child}"
                    )
                relative = child.relative_to(directory).as_posix()
                raw = child.read_bytes()
                raw_by_path[relative] = raw
                rows.append({
                    "path": relative,
                    "bytes": len(raw),
                    "sha256": _sha256_bytes(raw),
                    "mode": stat.S_IMODE(metadata.st_mode),
                })
    except OSError as exc:
        raise LegacyOneTimeMigrationError(
            f"cannot inventory legacy owner {directory}: {exc}"
        ) from exc
    if "job.json" not in raw_by_path:
        raise LegacyOneTimeMigrationError(f"legacy owner has no job.json: {directory}")
    return (
        sorted(rows, key=lambda row: str(row["path"])),
        sorted(directory_rows, key=lambda row: str(row["path"])),
        raw_by_path,
    )


def _directory_digest(
    rows: Sequence[Mapping[str, Any]], directories: Sequence[Mapping[str, Any]],
) -> str:
    core = [
        {
            "path": row["path"], "bytes": row["bytes"],
            "sha256": row["sha256"], "mode": row["mode"],
        }
        for row in rows
    ]
    directory_core = [
        {"path": row["path"], "mode": row["mode"]} for row in directories
    ]
    return _canonical_digest({"files": core, "directories": directory_core})




def _validate_owner_artifacts(
    directory: Path, payload: Mapping[str, Any], job_id: str,
) -> None:
    if payload.get("visibility") != "internal" or payload.get("root_job_id") is not None:
        raise LegacyOneTimeMigrationError(
            f"legacy owner must be an internal root: {directory}"
        )
    for name in REQUIRED_LEGACY_ARTIFACTS:
        if not (directory / name).is_file() or (directory / name).is_symlink():
            raise LegacyOneTimeMigrationError(
                f"legacy owner is missing required artifact {name}: {directory}"
            )
    origin, _origin_raw = _read_json(directory / "one-time-import-origin.json")
    journal, _journal_raw = _read_json(directory / "one-time-import-journal.json")
    preparation, _preparation_raw = _read_json(
        directory / "one-time-import-preparation.json"
    )
    if (
        origin.get("kind") != "one_time_title_import_origin"
        or origin.get("owner_job_id") != job_id
    ):
        raise LegacyOneTimeMigrationError(f"legacy owner origin is invalid: {directory}")
    if (
        journal.get("kind") != "one_time_title_import_journal"
        or journal.get("owner_job_id") != job_id
    ):
        raise LegacyOneTimeMigrationError(f"legacy owner journal is invalid: {directory}")
    if preparation.get("kind") != "one_time_title_import_preparation":
        raise LegacyOneTimeMigrationError(
            f"legacy owner preparation is invalid: {directory}"
        )
    plan = payload.get("plan")
    if isinstance(plan, dict) and plan.get("one_time_import_origin") != origin:
        raise LegacyOneTimeMigrationError(
            f"legacy owner job/origin binding is invalid: {directory}"
        )


def _job_retirement_entry(
    root: Path, directory: Path, payload: Mapping[str, Any], raw: bytes,
    *, role: str, owner_job_id: str, lineage_depth: int,
) -> dict[str, Any]:
    job_id = directory.name
    if payload.get("id") != job_id:
        raise LegacyOneTimeMigrationError(f"job id/path mismatch: {directory}")
    phase = payload.get("phase")
    if not isinstance(phase, str) or not phase:
        raise LegacyOneTimeMigrationError(f"job phase is invalid: {directory}")
    plan = payload.get("plan")
    if plan is not None and not isinstance(plan, dict):
        raise LegacyOneTimeMigrationError(f"job plan is invalid: {directory}")
    if role == "owner":
        if payload.get("approval_source") != LEGACY_ONE_TIME_APPROVAL_SOURCE:
            raise LegacyOneTimeMigrationError(f"legacy owner marker mismatch: {directory}")
        _validate_owner_artifacts(directory, payload, job_id)
    elif (
        role != "descendant"
        or payload.get("visibility") != "internal"
        or not isinstance(payload.get("root_job_id"), str)
    ):
        raise LegacyOneTimeMigrationError(
            f"legacy descendant identity is invalid: {directory}"
        )
    files, directories, raw_by_path = _directory_snapshot(directory)
    if raw_by_path["job.json"] != raw:
        raise LegacyOneTimeMigrationError(
            f"job changed during retirement inventory: {directory}"
        )
    return {
        "job_id": job_id,
        "role": role,
        "owner_job_id": owner_job_id,
        "lineage_depth": lineage_depth,
        "original_root_job_id": payload.get("root_job_id"),
        "original_phase": phase,
        "source": _relative(root, directory),
        "original_job_sha256": _sha256_bytes(raw),
        "original_job_json_base64": base64.b64encode(raw).decode("ascii"),
        "original_job_mode": stat.S_IMODE((directory / "job.json").stat().st_mode),
        "original_directory_sha256": _directory_digest(files, directories),
        "files": files,
        "directories": directories,
        "_job_payload": copy.deepcopy(dict(payload)),
    }


def _scan_retirement_inventory(
    root: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return every marked owner plus the transitive root_job_id closure."""
    jobs_root = root / "jobs"
    if jobs_root.is_symlink() or not jobs_root.is_dir():
        raise LegacyOneTimeMigrationError(f"jobs directory is missing or unsafe: {jobs_root}")
    records: dict[str, tuple[Path, dict[str, Any], bytes]] = {}
    try:
        children = sorted(jobs_root.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise LegacyOneTimeMigrationError(f"cannot list jobs: {exc}") from exc
    for directory in children:
        if directory.name.startswith(".") and not directory.is_dir():
            continue
        if not JOB_ID_RE.fullmatch(directory.name):
            raise LegacyOneTimeMigrationError(
                f"unexpected entry in jobs directory: {directory}"
            )
        if directory.is_symlink() or not directory.is_dir():
            raise LegacyOneTimeMigrationError(f"unsafe job directory: {directory}")
        payload, raw = _read_json(directory / "job.json")
        if payload.get("id") != directory.name:
            raise LegacyOneTimeMigrationError(f"job id/path mismatch: {directory}")
        has_legacy_artifact = any((directory / name).exists() for name in LEGACY_ARTIFACT_NAMES)
        is_owner = payload.get("approval_source") == LEGACY_ONE_TIME_APPROVAL_SOURCE
        if has_legacy_artifact and not is_owner:
            raise LegacyOneTimeMigrationError(
                f"ambiguous legacy artifacts without owner marker: {directory}"
            )
        records[directory.name] = (directory, payload, raw)

    owner_ids = sorted(
        job_id for job_id, (_directory, payload, _raw) in records.items()
        if payload.get("approval_source") == LEGACY_ONE_TIME_APPROVAL_SOURCE
    )
    if not owner_ids:
        raise LegacyOneTimeMigrationError("no legacy one-time owners were found")
    assignments: dict[str, tuple[str, int]] = {
        owner_id: (owner_id, 0) for owner_id in owner_ids
    }
    changed = True
    while changed:
        changed = False
        for job_id, (_directory, payload, _raw) in sorted(records.items()):
            if job_id in assignments:
                continue
            parent_id = payload.get("root_job_id")
            if isinstance(parent_id, str) and parent_id in assignments:
                owner_id, parent_depth = assignments[parent_id]
                assignments[job_id] = (owner_id, parent_depth + 1)
                changed = True

    entries: list[dict[str, Any]] = []
    owner_rows: list[dict[str, Any]] = []
    for job_id, (owner_id, depth) in sorted(assignments.items()):
        directory, payload, raw = records[job_id]
        role = "owner" if depth == 0 else "descendant"
        entry = _job_retirement_entry(
            root, directory, payload, raw,
            role=role, owner_job_id=owner_id, lineage_depth=depth,
        )
        entries.append(entry)
        if role == "owner":
            owner_rows.append({
                "job_id": job_id,
                "phase": entry["original_phase"],
                "classification": (
                    "terminal" if entry["original_phase"] in TERMINAL_PHASES else "active"
                ),
            })
    return entries, owner_rows


def _validate_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise LegacyOneTimeMigrationError(f"invalid {label}")
    return value


def _latest_backup_manifest(manifest_path: Path, completed: datetime) -> None:
    backup_root = manifest_path.parent.parent
    try:
        siblings = sorted(backup_root.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise LegacyOneTimeMigrationError(
            f"cannot inspect backup root {backup_root}: {exc}"
        ) from exc
    for snapshot in siblings:
        if snapshot == manifest_path.parent or not BACKUP_SNAPSHOT_RE.fullmatch(snapshot.name):
            continue
        candidate = snapshot / "manifest.json"
        if not candidate.is_file() or candidate.is_symlink():
            raise LegacyOneTimeMigrationError(
                f"cannot prove latest backup; invalid snapshot manifest: {candidate}"
            )
        payload, _raw = _read_json(candidate)
        candidate_completed = _parse_time(
            payload.get("completed_at"), f"backup completed_at in {candidate}",
        )
        if candidate_completed > completed:
            raise LegacyOneTimeMigrationError(
                f"provided backup is not the latest snapshot: {candidate}"
            )


def _safe_tar_name(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise LegacyOneTimeMigrationError(f"unsafe backup archive member: {value!r}")
    return path.as_posix()


def _verify_backup_archive(
    archive_path: Path, entries: Sequence[Mapping[str, Any]],
) -> None:
    expected: dict[str, dict[str, Any]] = {}
    expected_directories: dict[str, dict[str, Any]] = {}
    for entry in entries:
        job_id = str(entry["job_id"])
        prefix = f"scrapeflow-data/jobs/{job_id}"
        for row in entry["files"]:
            name = f"{prefix}/{row['path']}"
            expected[name] = dict(row)
        for row in entry["directories"]:
            name = prefix if row["path"] == "." else f"{prefix}/{row['path']}"
            expected_directories[name] = dict(row)
    seen: dict[str, tarfile.TarInfo] = {}
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            for member in archive.getmembers():
                name = _safe_tar_name(member.name)
                if name in seen:
                    raise LegacyOneTimeMigrationError(
                        f"duplicate backup archive member: {name}"
                    )
                seen[name] = member
            target_prefixes = {
                f"scrapeflow-data/jobs/{entry['job_id']}" for entry in entries
            }
            for name, member in seen.items():
                if not any(
                    name == prefix or name.startswith(prefix + "/")
                    for prefix in target_prefixes
                ):
                    continue
                if member.issym() or member.islnk() or not (
                    member.isfile() or member.isdir()
                ):
                    raise LegacyOneTimeMigrationError(
                        f"unsafe legacy owner member in backup: {name}"
                    )
            archived_target_files = {
                name for name, member in seen.items()
                if any(
                    name.startswith(f"scrapeflow-data/jobs/{entry['job_id']}/")
                    for entry in entries
                ) and member.isfile()
            }
            if archived_target_files != set(expected):
                missing = sorted(set(expected) - archived_target_files)
                extra = sorted(archived_target_files - set(expected))
                raise LegacyOneTimeMigrationError(
                    "backup/current legacy owner file sets differ: "
                    f"missing={missing[:4]} extra={extra[:4]}"
                )
            archived_target_directories = {
                name for name, member in seen.items()
                if any(
                    name == prefix or name.startswith(prefix + "/")
                    for prefix in target_prefixes
                ) and member.isdir()
            }
            if archived_target_directories != set(expected_directories):
                missing = sorted(set(expected_directories) - archived_target_directories)
                extra = sorted(archived_target_directories - set(expected_directories))
                raise LegacyOneTimeMigrationError(
                    "backup/current legacy lineage directory sets differ: "
                    f"missing={missing[:4]} extra={extra[:4]}"
                )
            for name, row in expected_directories.items():
                archived_mode = stat.S_IMODE(seen[name].mode)
                if archived_mode != row["mode"]:
                    raise LegacyOneTimeMigrationError(
                        "backup/current legacy lineage directory modes differ: "
                        f"{name} backup={archived_mode:#o} current={row['mode']:#o}"
                    )
            for name, row in expected.items():
                member = seen[name]
                if not member.isfile() or member.issym() or member.islnk():
                    raise LegacyOneTimeMigrationError(
                        f"unsafe legacy owner member in backup: {name}"
                    )
                archived_mode = stat.S_IMODE(member.mode)
                if archived_mode != row["mode"]:
                    raise LegacyOneTimeMigrationError(
                        "backup/current legacy lineage file modes differ: "
                        f"{name} backup={archived_mode:#o} current={row['mode']:#o}"
                    )
                stream = archive.extractfile(member)
                if stream is None:
                    raise LegacyOneTimeMigrationError(
                        f"cannot read backup archive member: {name}"
                    )
                digest = hashlib.sha256()
                size = 0
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
                    size += len(chunk)
                if size != row["bytes"] or digest.hexdigest() != row["sha256"]:
                    raise LegacyOneTimeMigrationError(
                        f"backup does not match current legacy owner file: {name}"
                    )
    except (OSError, tarfile.TarError) as exc:
        raise LegacyOneTimeMigrationError(
            f"cannot verify backup archive {archive_path}: {exc}"
        ) from exc


def verify_backup(
    root: Path, backup_manifest_path: Path,
    entries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    try:
        manifest_path = backup_manifest_path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise LegacyOneTimeMigrationError(
            f"backup manifest does not exist: {backup_manifest_path}: {exc}"
        ) from exc
    manifest, manifest_raw = _read_json(manifest_path)
    if manifest.get("schema_version") != 1 or manifest.get("paused_during_backup") is not True:
        raise LegacyOneTimeMigrationError("backup manifest lacks paused snapshot evidence")
    global_control = manifest.get("global_control")
    if not isinstance(global_control, dict) or global_control.get("paused") is not True:
        raise LegacyOneTimeMigrationError("backup manifest global pause evidence is invalid")
    coordination = manifest.get("coordination")
    quiescence = coordination.get("quiescence") if isinstance(coordination, dict) else None
    if not isinstance(quiescence, dict) or quiescence.get("reasons") != []:
        raise LegacyOneTimeMigrationError("backup manifest lacks quiescence evidence")
    completed = _parse_time(manifest.get("completed_at"), "backup completed_at")
    started = _parse_time(manifest.get("started_at"), "backup started_at")
    if completed < started:
        raise LegacyOneTimeMigrationError("backup completion precedes its start")
    declared_state_root = manifest.get("state_root")
    try:
        declared = Path(str(declared_state_root)).expanduser().resolve(strict=True)
    except OSError as exc:
        raise LegacyOneTimeMigrationError(
            f"backup state_root is unavailable: {declared_state_root!r}"
        ) from exc
    if root.name != "scrapeflow-data" or declared / "scrapeflow-data" != root:
        raise LegacyOneTimeMigrationError(
            "backup manifest does not belong to this scrapeflow-data state root"
        )
    archives = manifest.get("archives")
    archive_row = archives.get("scrapeflow-data.tar.gz") if isinstance(archives, dict) else None
    if not isinstance(archive_row, dict):
        raise LegacyOneTimeMigrationError("backup manifest has no ScrapeFlow archive")
    declared_archive_sha = _validate_sha(
        archive_row.get("sha256"), "backup archive SHA-256",
    )
    archive_path = manifest_path.parent / "scrapeflow-data.tar.gz"
    if archive_path.is_symlink() or not archive_path.is_file():
        raise LegacyOneTimeMigrationError(f"backup archive is missing or unsafe: {archive_path}")
    actual_archive_sha = _sha256_file(archive_path)
    if actual_archive_sha != declared_archive_sha:
        raise LegacyOneTimeMigrationError("backup archive SHA differs from its manifest")
    declared_bytes = archive_row.get("bytes")
    if type(declared_bytes) is not int or declared_bytes != archive_path.stat().st_size:
        raise LegacyOneTimeMigrationError("backup archive byte count differs from its manifest")
    _latest_backup_manifest(manifest_path, completed)
    _verify_backup_archive(archive_path, entries)
    return {
        "manifest": str(manifest_path),
        "manifest_sha256": _sha256_bytes(manifest_raw),
        "snapshot": str(manifest_path.parent),
        "completed_at": manifest["completed_at"],
        "archive": str(archive_path),
        "archive_sha256": actual_archive_sha,
        "lineage_directories_verified": len(entries),
        "paused_during_backup": True,
        "quiescent": True,
        "latest_snapshot_verified": True,
    }


def _retirement_projection(
    entry: dict[str, Any], *, migration_id: str, generated_at: str,
) -> None:
    payload = entry.pop("_job_payload")
    plan = payload.get("plan")
    projected_plan = copy.deepcopy(plan) if isinstance(plan, dict) else {}
    projected_plan["legacy_one_time_retirement"] = {
        "kind": "legacy_one_time_owner_retirement",
        "migration_id": migration_id,
        "retired_at": generated_at,
        "original_phase": entry["original_phase"],
        "original_job_sha256": entry["original_job_sha256"],
        "role": entry["role"],
        "owner_job_id": entry["owner_job_id"],
        "lineage_depth": entry["lineage_depth"],
        "disposition": "cancelled_and_archived",
    }
    payload.update({
        "phase": "cancelled",
        "error": None,
        "digest": None,
        "plan": projected_plan,
        "progress": {
            "stage": "legacy_one_time_archived",
            "completed": 1,
            "total": 1,
            "percent": 100.0,
            "message": "历史 one-time owner 已离线取消并封存",
        },
        "updated_at": generated_at,
    })
    projected_raw = _encode_json(payload)
    entry["cancelled_job_sha256"] = _sha256_bytes(projected_raw)
    entry["cancelled_job_json_base64"] = base64.b64encode(projected_raw).decode("ascii")
    entry["archive_target"] = (
        ARCHIVE_RELATIVE_ROOT / migration_id / "jobs" / entry["job_id"]
    ).as_posix()
    entry["action"] = "cancel_and_archive"
    entry["compare_and_swap"] = {
        "from_job_sha256": entry["original_job_sha256"],
        "to_job_sha256": entry["cancelled_job_sha256"],
        "from_directory_sha256": entry["original_directory_sha256"],
    }




def build_retirement_plan(
    state_root: Path, backup_manifest: Path, *,
    expected_active_count: int, expected_terminal_count: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a sealed zero-write plan for every owner and descendant."""
    for label, count in (
        ("expected_active_count", expected_active_count),
        ("expected_terminal_count", expected_terminal_count),
    ):
        if type(count) is not int or count < 0:
            raise LegacyOneTimeMigrationError(f"{label} must be a non-negative integer")
    if expected_active_count + expected_terminal_count < 1:
        raise LegacyOneTimeMigrationError("at least one legacy owner must be expected")
    root = _state_root(state_root)
    control, control_raw = _require_paused(root)
    entries, owners = _scan_retirement_inventory(root)
    active_count = sum(row["classification"] == "active" for row in owners)
    terminal_count = sum(row["classification"] == "terminal" for row in owners)
    if active_count != expected_active_count or terminal_count != expected_terminal_count:
        raise LegacyOneTimeMigrationError(
            "legacy owner count mismatch: "
            f"expected active={expected_active_count}, terminal={expected_terminal_count}; "
            f"found active={active_count}, terminal={terminal_count}"
        )
    backup = verify_backup(root, backup_manifest, entries)
    instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    generated_at = instant.isoformat()
    identity = _canonical_digest({
        "backup_manifest_sha256": backup["manifest_sha256"],
        "expected_active_count": expected_active_count,
        "expected_terminal_count": expected_terminal_count,
        "entries": [
            {
                "job_id": entry["job_id"],
                "role": entry["role"],
                "owner_job_id": entry["owner_job_id"],
                "lineage_depth": entry["lineage_depth"],
                "job_sha256": entry["original_job_sha256"],
                "directory_sha256": entry["original_directory_sha256"],
            }
            for entry in entries
        ],
    })
    migration_id = instant.strftime("%Y%m%dT%H%M%SZ-") + identity[:12]
    for entry in entries:
        _retirement_projection(
            entry, migration_id=migration_id, generated_at=generated_at,
        )
    owner_ids = sorted(row["job_id"] for row in owners)
    descendant_ids = sorted(
        entry["job_id"] for entry in entries if entry["role"] == "descendant"
    )
    lineages = [{
        "owner_job_id": owner_id,
        "member_job_ids": sorted(
            entry["job_id"] for entry in entries
            if entry["owner_job_id"] == owner_id
        ),
    } for owner_id in owner_ids]
    descendant_set_sha256 = _canonical_digest({
        "owners": owner_ids,
        "descendants": descendant_ids,
        "lineages": lineages,
    })
    core: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "kind": PLAN_KIND,
        "migration_id": migration_id,
        "generated_at": generated_at,
        "state_root": str(root),
        "expected_active_count": expected_active_count,
        "expected_terminal_count": expected_terminal_count,
        "active_owner_count": active_count,
        "terminal_owner_count": terminal_count,
        "total_owner_count": len(owners),
        "descendant_count": len(descendant_ids),
        "total_entry_count": len(entries),
        "owner_inventory": owners,
        "owner_job_ids": owner_ids,
        "descendant_job_ids": descendant_ids,
        "lineages": lineages,
        "descendant_set_sha256": descendant_set_sha256,
        "default_mode": "read_only",
        "global_control": {
            "path": "global-control.json",
            "sha256": _sha256_bytes(control_raw),
            "paused": True,
            "updated_at": control["updated_at"],
            "reason": control.get("reason"),
        },
        "backup": backup,
        "archive_root": (
            ARCHIVE_RELATIVE_ROOT / migration_id / "jobs"
        ).as_posix(),
        "retired_root_index": RETIRED_ROOT_INDEX_RELATIVE_PATH.as_posix(),
        "apply_guards": {
            "durable_global_pause_required": True,
            "api_and_workers_stopped_confirmation_required": True,
            "latest_backup_reverification_required": True,
            "per_job_compare_and_swap_required": True,
            "descendant_set_compare_and_swap_required": True,
            "approved_plan_sha256_required": True,
            "same_filesystem_atomic_archive_required": True,
            "durable_retired_root_index_required": True,
        },
        "entries": entries,
    }
    return {**core, "plan_sha256": _canonical_digest(core)}


def _decoded_job_bytes(entry: Mapping[str, Any], field: str, sha_field: str) -> bytes:
    encoded = entry.get(field)
    if not isinstance(encoded, str):
        raise LegacyOneTimeMigrationError(f"missing {field} for {entry.get('job_id')}")
    try:
        raw = base64.b64decode(encoded.encode("ascii"), validate=True)
    except (UnicodeEncodeError, ValueError) as exc:
        raise LegacyOneTimeMigrationError(
            f"invalid {field} for {entry.get('job_id')}"
        ) from exc
    if _sha256_bytes(raw) != _validate_sha(entry.get(sha_field), sha_field):
        raise LegacyOneTimeMigrationError(
            f"{field} SHA mismatch for {entry.get('job_id')}"
        )
    return raw




def validate_retirement_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(plan))
    supplied_sha = _validate_sha(value.pop("plan_sha256", None), "plan SHA-256")
    if _canonical_digest(value) != supplied_sha:
        raise LegacyOneTimeMigrationError("retirement plan SHA-256 is invalid")
    if value.get("schema_version") != PLAN_SCHEMA_VERSION or value.get("kind") != PLAN_KIND:
        raise LegacyOneTimeMigrationError("unsupported retirement plan")
    migration_id = value.get("migration_id")
    if not isinstance(migration_id, str) or not MIGRATION_ID_RE.fullmatch(migration_id):
        raise LegacyOneTimeMigrationError("retirement plan migration_id is invalid")
    _parse_time(value.get("generated_at"), "retirement plan generated_at")
    entries = value.get("entries")
    if not isinstance(entries, list) or not entries:
        raise LegacyOneTimeMigrationError("retirement plan inventory is invalid")
    root = _state_root(Path(str(value.get("state_root") or "")))
    by_id: dict[str, dict[str, Any]] = {}
    owner_rows: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise LegacyOneTimeMigrationError("retirement plan entry is not an object")
        job_id = entry.get("job_id")
        if (
            not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id)
            or job_id in by_id
        ):
            raise LegacyOneTimeMigrationError(
                "retirement plan job id is invalid or duplicated"
            )
        by_id[job_id] = entry
        role = entry.get("role")
        owner_job_id = entry.get("owner_job_id")
        depth = entry.get("lineage_depth")
        if (
            role not in {"owner", "descendant"}
            or not isinstance(owner_job_id, str)
            or not JOB_ID_RE.fullmatch(owner_job_id)
            or type(depth) is not int or depth < 0
            or (role == "owner") != (depth == 0)
        ):
            raise LegacyOneTimeMigrationError(f"invalid lineage identity for {job_id}")
        if entry.get("action") != "cancel_and_archive":
            raise LegacyOneTimeMigrationError(f"invalid retirement action for {job_id}")
        source = _path_from_relative(root, entry.get("source"), label="job source")
        target = _path_from_relative(root, entry.get("archive_target"), label="archive target")
        if source != root / "jobs" / job_id:
            raise LegacyOneTimeMigrationError(f"non-canonical job source for {job_id}")
        expected_target = root / ARCHIVE_RELATIVE_ROOT / migration_id / "jobs" / job_id
        if target != expected_target:
            raise LegacyOneTimeMigrationError(f"non-canonical archive target for {job_id}")
        original = _decoded_job_bytes(
            entry, "original_job_json_base64", "original_job_sha256",
        )
        cancelled = _decoded_job_bytes(
            entry, "cancelled_job_json_base64", "cancelled_job_sha256",
        )
        original_payload = _decode_json(original, source / "job.json")
        cancelled_payload = _decode_json(cancelled, target / "job.json")
        if (
            original_payload.get("id") != job_id
            or original_payload.get("phase") != entry.get("original_phase")
            or cancelled_payload.get("id") != job_id
            or cancelled_payload.get("approval_source")
            != original_payload.get("approval_source")
            or cancelled_payload.get("phase") != "cancelled"
        ):
            raise LegacyOneTimeMigrationError(f"invalid job projection for {job_id}")
        if role == "owner":
            if (
                owner_job_id != job_id
                or entry.get("original_root_job_id") is not None
                or original_payload.get("approval_source")
                != LEGACY_ONE_TIME_APPROVAL_SOURCE
            ):
                raise LegacyOneTimeMigrationError(f"invalid owner projection for {job_id}")
            owner_rows.append({
                "job_id": job_id,
                "phase": entry.get("original_phase"),
                "classification": (
                    "terminal" if entry.get("original_phase") in TERMINAL_PHASES else "active"
                ),
            })
        elif (
            original_payload.get("root_job_id") != entry.get("original_root_job_id")
            or not isinstance(entry.get("original_root_job_id"), str)
        ):
            raise LegacyOneTimeMigrationError(
                f"invalid descendant projection for {job_id}"
            )
        mode = entry.get("original_job_mode")
        if type(mode) is not int or mode < 0 or mode > 0o777:
            raise LegacyOneTimeMigrationError(f"invalid job mode for {job_id}")
        files = entry.get("files")
        directories = entry.get("directories")
        if not isinstance(files, list) or not files or not isinstance(directories, list):
            raise LegacyOneTimeMigrationError(f"invalid directory inventory for {job_id}")
        file_paths: set[str] = set()
        job_row: Mapping[str, Any] | None = None
        for row in files:
            if not isinstance(row, dict):
                raise LegacyOneTimeMigrationError(f"invalid file row for {job_id}")
            relative = row.get("path")
            row_mode = row.get("mode")
            if (
                not isinstance(relative, str) or relative in file_paths
                or type(row_mode) is not int or not 0 <= row_mode <= 0o777
            ):
                raise LegacyOneTimeMigrationError(f"invalid file path/mode for {job_id}")
            _path_from_relative(source, relative, label="job file")
            file_paths.add(relative)
            if type(row.get("bytes")) is not int or row["bytes"] < 0:
                raise LegacyOneTimeMigrationError(f"invalid file size for {job_id}")
            _validate_sha(row.get("sha256"), "job file SHA-256")
            if relative == "job.json":
                job_row = row
        if job_row != {
            "path": "job.json", "bytes": len(original),
            "sha256": entry["original_job_sha256"], "mode": mode,
        }:
            raise LegacyOneTimeMigrationError(
                f"job.json evidence differs from sealed bytes: {job_id}"
            )
        directory_paths: set[str] = set()
        for row in directories:
            if not isinstance(row, dict):
                raise LegacyOneTimeMigrationError(f"invalid directory row for {job_id}")
            relative = row.get("path")
            row_mode = row.get("mode")
            if (
                not isinstance(relative, str) or relative in directory_paths
                or type(row_mode) is not int or not 0 <= row_mode <= 0o777
            ):
                raise LegacyOneTimeMigrationError(
                    f"invalid directory path/mode for {job_id}"
                )
            if relative != ".":
                _path_from_relative(source, relative, label="job directory")
            directory_paths.add(relative)
        if "." not in directory_paths:
            raise LegacyOneTimeMigrationError(f"root directory evidence is absent: {job_id}")
        if _directory_digest(files, directories) != _validate_sha(
            entry.get("original_directory_sha256"), "directory SHA-256",
        ):
            raise LegacyOneTimeMigrationError(f"directory digest mismatch for {job_id}")

    for job_id, entry in by_id.items():
        if entry["role"] == "owner":
            continue
        parent_id = entry["original_root_job_id"]
        parent = by_id.get(parent_id)
        if (
            parent is None
            or parent["owner_job_id"] != entry["owner_job_id"]
            or parent["lineage_depth"] + 1 != entry["lineage_depth"]
        ):
            raise LegacyOneTimeMigrationError(
                f"descendant parent chain is incomplete for {job_id}"
            )
    owner_ids = sorted(
        job_id for job_id, entry in by_id.items() if entry["role"] == "owner"
    )
    descendant_ids = sorted(set(by_id) - set(owner_ids))
    lineages = [{
        "owner_job_id": owner_id,
        "member_job_ids": sorted(
            job_id for job_id, entry in by_id.items()
            if entry["owner_job_id"] == owner_id
        ),
    } for owner_id in owner_ids]
    active_count = sum(row["classification"] == "active" for row in owner_rows)
    terminal_count = sum(row["classification"] == "terminal" for row in owner_rows)
    if (
        value.get("expected_active_count") != active_count
        or value.get("expected_terminal_count") != terminal_count
        or value.get("active_owner_count") != active_count
        or value.get("terminal_owner_count") != terminal_count
        or value.get("total_owner_count") != len(owner_ids)
        or value.get("descendant_count") != len(descendant_ids)
        or value.get("total_entry_count") != len(entries)
        or value.get("owner_inventory") != sorted(owner_rows, key=lambda row: row["job_id"])
        or value.get("owner_job_ids") != owner_ids
        or value.get("descendant_job_ids") != descendant_ids
        or value.get("lineages") != lineages
    ):
        raise LegacyOneTimeMigrationError("retirement plan lineage counts are inconsistent")
    descendant_set_sha256 = _canonical_digest({
        "owners": owner_ids, "descendants": descendant_ids, "lineages": lineages,
    })
    if value.get("descendant_set_sha256") != descendant_set_sha256:
        raise LegacyOneTimeMigrationError("retirement plan descendant-set SHA is invalid")
    if value.get("archive_root") != (
        ARCHIVE_RELATIVE_ROOT / migration_id / "jobs"
    ).as_posix() or value.get("retired_root_index") != RETIRED_ROOT_INDEX_RELATIVE_PATH.as_posix():
        raise LegacyOneTimeMigrationError("retirement plan archive/index path is invalid")
    return {**value, "plan_sha256": supplied_sha}


def read_retirement_plan(path: Path) -> dict[str, Any]:
    plan, _raw = _read_json(path.expanduser().resolve(strict=True))
    return validate_retirement_plan(plan)


def write_retirement_plan(path: Path, plan: Mapping[str, Any]) -> Path:
    """Seal a validated plan outside live state; never modify job state."""
    validated = validate_retirement_plan(plan)
    root = Path(validated["state_root"])
    destination = path.expanduser().resolve()
    try:
        destination.relative_to(root)
    except ValueError:
        pass
    else:
        raise LegacyOneTimeMigrationError(
            "sealed plan output must be outside the live state root"
        )
    if destination.exists():
        raise LegacyOneTimeMigrationError(f"refusing to overwrite sealed plan: {destination}")
    _atomic_write(destination, _encode_json(validated), mode=0o600)
    return destination


def _expected_file_map(
    entry: Mapping[str, Any], *, cancelled: bool,
) -> dict[str, tuple[int, str, int]]:
    expected = {
        str(row["path"]): (
            int(row["bytes"]), str(row["sha256"]), int(row["mode"]),
        )
        for row in entry["files"]
    }
    if cancelled:
        raw = _decoded_job_bytes(
            entry, "cancelled_job_json_base64", "cancelled_job_sha256",
        )
        expected["job.json"] = (
            len(raw), str(entry["cancelled_job_sha256"]),
            int(entry["original_job_mode"]),
        )
    return expected


def _matches_directory(
    directory: Path, entry: Mapping[str, Any], *, cancelled: bool,
) -> bool:
    try:
        rows, directories, _raw = _directory_snapshot(directory)
    except LegacyOneTimeMigrationError:
        return False
    actual = {
        str(row["path"]): (
            int(row["bytes"]), str(row["sha256"]), int(row["mode"]),
        )
        for row in rows
    }
    actual_directories = {
        str(row["path"]): int(row["mode"]) for row in directories
    }
    expected_directories = {
        str(row["path"]): int(row["mode"]) for row in entry["directories"]
    }
    return (
        actual == _expected_file_map(entry, cancelled=cancelled)
        and actual_directories == expected_directories
    )


def _entry_position(root: Path, entry: Mapping[str, Any]) -> str:
    source = _path_from_relative(root, entry["source"], label="job source")
    target = _path_from_relative(root, entry["archive_target"], label="archive target")
    source_exists = os.path.lexists(source)
    target_exists = os.path.lexists(target)
    if source_exists and target_exists:
        raise LegacyOneTimeMigrationError(
            f"live and archived directories both exist for {entry['job_id']}"
        )
    if not source_exists and not target_exists:
        raise LegacyOneTimeMigrationError(
            f"live and archived directories are both missing for {entry['job_id']}"
        )
    if source_exists:
        if source.is_symlink() or not source.is_dir():
            raise LegacyOneTimeMigrationError(f"unsafe live directory: {source}")
        if _matches_directory(source, entry, cancelled=False):
            return "original"
        if _matches_directory(source, entry, cancelled=True):
            return "cancelled_live"
        raise LegacyOneTimeMigrationError(
            f"live directory CAS conflict for {entry['job_id']}"
        )
    if target.is_symlink() or not target.is_dir():
        raise LegacyOneTimeMigrationError(f"unsafe archive directory: {target}")
    if _matches_directory(target, entry, cancelled=True):
        return "archived"
    raise LegacyOneTimeMigrationError(
        f"archive directory CAS conflict for {entry['job_id']}"
    )


def _preflight_positions(root: Path, plan: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(entry["job_id"]): _entry_position(root, entry)
        for entry in plan["entries"]
    }


def _assert_descendant_set_unchanged(root: Path, plan: Mapping[str, Any]) -> None:
    """Reject every new owner/descendant, including descendants of descendants."""
    jobs_root = root / "jobs"
    if jobs_root.is_symlink() or not jobs_root.is_dir():
        raise LegacyOneTimeMigrationError(f"jobs directory is missing or unsafe: {jobs_root}")
    records: dict[str, dict[str, Any]] = {}
    for directory in sorted(jobs_root.iterdir(), key=lambda path: path.name):
        if directory.name.startswith(".") and not directory.is_dir():
            continue
        if (
            not JOB_ID_RE.fullmatch(directory.name)
            or directory.is_symlink() or not directory.is_dir()
        ):
            raise LegacyOneTimeMigrationError(
                f"unexpected or unsafe entry in jobs directory: {directory}"
            )
        payload, _raw = _read_json(directory / "job.json")
        if payload.get("id") != directory.name:
            raise LegacyOneTimeMigrationError(f"job id/path mismatch: {directory}")
        records[directory.name] = payload
    expected_ids = {str(entry["job_id"]) for entry in plan["entries"]}
    expected_owner_ids = set(plan["owner_job_ids"])
    unexpected_owners = sorted(
        job_id for job_id, payload in records.items()
        if payload.get("approval_source") == LEGACY_ONE_TIME_APPROVAL_SOURCE
        and job_id not in expected_owner_ids
    )
    if unexpected_owners:
        raise LegacyOneTimeMigrationError(
            "legacy owners appeared after planning: " + ", ".join(unexpected_owners)
        )
    linked = set(expected_ids)
    unexpected_descendants: set[str] = set()
    changed = True
    while changed:
        changed = False
        for job_id, payload in records.items():
            if job_id in expected_ids or job_id in unexpected_descendants:
                continue
            parent_id = payload.get("root_job_id")
            if isinstance(parent_id, str) and parent_id in linked:
                unexpected_descendants.add(job_id)
                linked.add(job_id)
                changed = True
    if unexpected_descendants:
        raise LegacyOneTimeMigrationError(
            "legacy descendant set changed after planning: "
            + ", ".join(sorted(unexpected_descendants))
        )
    archive_jobs = root / ARCHIVE_RELATIVE_ROOT / str(plan["migration_id"]) / "jobs"
    if archive_jobs.exists():
        if archive_jobs.is_symlink() or not archive_jobs.is_dir():
            raise LegacyOneTimeMigrationError(f"unsafe retirement archive: {archive_jobs}")
        unknown_archived = sorted(
            child.name for child in archive_jobs.iterdir()
            if child.name not in expected_ids
        )
        if unknown_archived:
            raise LegacyOneTimeMigrationError(
                "unexpected jobs exist in retirement archive: "
                + ", ".join(unknown_archived)
            )


def _empty_retired_root_index() -> dict[str, Any]:
    core: dict[str, Any] = {
        "schema_version": 1,
        "kind": "retired_legacy_one_time_root_index",
        "updated_at": None,
        "roots": {},
    }
    return {**core, "index_sha256": _canonical_digest(core)}


def load_retired_root_index(state_root: Path) -> dict[str, Any]:
    """Load the durable deny-list used even after an owner directory vanishes."""
    root = _state_root(state_root)
    path = root / RETIRED_ROOT_INDEX_RELATIVE_PATH
    if not path.exists():
        return _empty_retired_root_index()
    value, _raw = _read_json(path)
    supplied_sha = _validate_sha(value.pop("index_sha256", None), "retired-root index SHA")
    if _canonical_digest(value) != supplied_sha:
        raise LegacyOneTimeMigrationError("retired-root index SHA-256 is invalid")
    if (
        value.get("schema_version") != 1
        or value.get("kind") != "retired_legacy_one_time_root_index"
        or not isinstance(value.get("roots"), dict)
    ):
        raise LegacyOneTimeMigrationError("retired-root index format is invalid")
    if value.get("updated_at") is not None:
        _parse_time(value["updated_at"], "retired-root index updated_at")
    all_members: set[str] = set()
    for owner_id, row in value["roots"].items():
        if (
            not isinstance(owner_id, str) or not JOB_ID_RE.fullmatch(owner_id)
            or not isinstance(row, dict)
            or row.get("owner_job_id") != owner_id
            or row.get("status") not in {"retiring", "retired", "restoring", "restored"}
        ):
            raise LegacyOneTimeMigrationError("retired-root index row is invalid")
        migration_id = row.get("migration_id")
        if (
            not isinstance(migration_id, str)
            or not MIGRATION_ID_RE.fullmatch(migration_id)
            or row.get("archive_root")
            != (ARCHIVE_RELATIVE_ROOT / migration_id / "jobs").as_posix()
        ):
            raise LegacyOneTimeMigrationError(
                f"retired-root archive identity is invalid: {owner_id}"
            )
        _parse_time(row.get("updated_at"), "retired-root row updated_at")
        _validate_sha(row.get("plan_sha256"), "retired-root plan SHA")
        members = row.get("member_job_ids")
        if (
            not isinstance(members, list) or owner_id not in members
            or members != sorted(set(members))
            or not all(isinstance(job_id, str) and JOB_ID_RE.fullmatch(job_id) for job_id in members)
        ):
            raise LegacyOneTimeMigrationError(
                f"retired-root member inventory is invalid: {owner_id}"
            )
        overlap = all_members.intersection(members)
        if overlap:
            raise LegacyOneTimeMigrationError(
                "retired-root member belongs to multiple roots: "
                + ", ".join(sorted(overlap))
            )
        all_members.update(members)
    return {**value, "index_sha256": supplied_sha}


def retired_legacy_lineage_ids(state_root: Path) -> tuple[set[str], set[str]]:
    index = load_retired_root_index(state_root)
    roots = set(index["roots"])
    members = {
        job_id for row in index["roots"].values()
        for job_id in row["member_job_ids"]
    }
    return roots, members


def _write_retired_root_index(
    root: Path, plan: Mapping[str, Any], *, status: str,
) -> Path:
    if status not in {"retiring", "retired", "restoring", "restored"}:
        raise LegacyOneTimeMigrationError("invalid retired-root transition status")
    current = load_retired_root_index(root)
    roots = copy.deepcopy(current["roots"])
    now = datetime.now(timezone.utc).isoformat()
    for lineage in plan["lineages"]:
        owner_id = str(lineage["owner_job_id"])
        existing = roots.get(owner_id)
        if existing is not None and (
            existing.get("plan_sha256") != plan["plan_sha256"]
            or existing.get("member_job_ids") != lineage["member_job_ids"]
        ):
            raise LegacyOneTimeMigrationError(
                f"retired-root index conflicts with approved lineage: {owner_id}"
            )
        roots[owner_id] = {
            "owner_job_id": owner_id,
            "member_job_ids": list(lineage["member_job_ids"]),
            "migration_id": plan["migration_id"],
            "plan_sha256": plan["plan_sha256"],
            "archive_root": plan["archive_root"],
            "status": status,
            "updated_at": now,
        }
    core = {
        "schema_version": 1,
        "kind": "retired_legacy_one_time_root_index",
        "updated_at": now,
        "roots": dict(sorted(roots.items())),
    }
    value = {**core, "index_sha256": _canonical_digest(core)}
    path = root / RETIRED_ROOT_INDEX_RELATIVE_PATH
    _atomic_json(path, value)
    return path


def _journal_path(root: Path, plan: Mapping[str, Any]) -> Path:
    return root / JOURNAL_RELATIVE_ROOT / f"{plan['migration_id']}.json"


def _load_or_create_journal(
    root: Path, plan: Mapping[str, Any], *, direction: str,
) -> tuple[Path, dict[str, Any]]:
    path = _journal_path(root, plan)
    _safe_directory_chain(root, path.parent, create=True)
    if path.exists():
        journal, _raw = _read_json(path)
        if (
            journal.get("schema_version") != JOURNAL_SCHEMA_VERSION
            or journal.get("kind") != JOURNAL_KIND
            or journal.get("migration_id") != plan["migration_id"]
            or journal.get("plan_sha256") != plan["plan_sha256"]
            or journal.get("sealed_plan") != plan
        ):
            raise LegacyOneTimeMigrationError(
                f"existing crash journal conflicts with the approved plan: {path}"
            )
    else:
        journal = {
            "schema_version": JOURNAL_SCHEMA_VERSION,
            "kind": JOURNAL_KIND,
            "migration_id": plan["migration_id"],
            "plan_sha256": plan["plan_sha256"],
            "status": "prepared",
            "direction": direction,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "sealed_plan": plan,
            "positions": {},
        }
        _atomic_json(path, journal)
    journal["direction"] = direction
    journal["status"] = "applying" if direction == "apply" else "restoring"
    journal["updated_at"] = datetime.now(timezone.utc).isoformat()
    journal["positions"] = _preflight_positions(root, plan)
    _atomic_json(path, journal)
    return path, journal


def _update_journal(
    path: Path, journal: dict[str, Any], root: Path, plan: Mapping[str, Any],
    *, status: str,
) -> None:
    journal["status"] = status
    journal["updated_at"] = datetime.now(timezone.utc).isoformat()
    journal["positions"] = _preflight_positions(root, plan)
    _atomic_json(path, journal)


def _replace_job_json(
    root: Path, journal_path: Path, entry: Mapping[str, Any], raw: bytes,
) -> None:
    """Atomically replace job.json without leaving crash debris in its directory."""
    job_id = str(entry["job_id"])
    source = _path_from_relative(root, entry["source"], label="job source")
    staged = journal_path.parent / f".{job_id}.job.json.staged"
    _atomic_write(staged, raw, mode=int(entry["original_job_mode"]))
    if staged.parent.stat().st_dev != source.stat().st_dev:
        raise LegacyOneTimeMigrationError(
            f"job projection staging is not on the source filesystem: {job_id}"
        )
    os.replace(staged, source / "job.json")
    _fsync_directory(source)
    _fsync_directory(staged.parent)


def _assert_approved(plan: Mapping[str, Any], approved_sha256: str) -> None:
    if not SHA256_RE.fullmatch(str(approved_sha256 or "")):
        raise LegacyOneTimeMigrationError("--approve-plan-sha256 must be a full SHA-256")
    if approved_sha256 != plan["plan_sha256"]:
        raise LegacyOneTimeMigrationError("approved SHA does not match the sealed plan")


def _verify_plan_backup(root: Path, plan: Mapping[str, Any]) -> None:
    backup = plan.get("backup")
    if not isinstance(backup, dict) or not isinstance(backup.get("manifest"), str):
        raise LegacyOneTimeMigrationError("retirement plan backup evidence is invalid")
    evidence = verify_backup(root, Path(backup["manifest"]), plan["entries"])
    for field in (
        "manifest", "manifest_sha256", "archive", "archive_sha256", "completed_at",
    ):
        if evidence.get(field) != backup.get(field):
            raise LegacyOneTimeMigrationError(
                f"backup evidence changed after planning: {field}"
            )


def apply_retirement_plan(
    plan: Mapping[str, Any], *, approved_sha256: str,
    confirm_api_stopped: bool,
) -> dict[str, Any]:
    """Idempotently cancel and atomically archive every approved legacy owner."""
    validated = validate_retirement_plan(plan)
    _assert_approved(validated, approved_sha256)
    if confirm_api_stopped is not True:
        raise LegacyOneTimeMigrationError(
            "apply requires an explicit API/workers-stopped confirmation"
        )
    root = _state_root(Path(validated["state_root"]))
    _control, control_raw = _require_paused(root)
    journal_path = _journal_path(root, validated)
    first_apply = not journal_path.exists()
    if first_apply and _sha256_bytes(control_raw) != validated["global_control"]["sha256"]:
        raise LegacyOneTimeMigrationError(
            "global-control.json changed after planning; create a new plan"
        )
    invocation_control_sha = _sha256_bytes(control_raw)
    _verify_plan_backup(root, validated)
    _preflight_positions(root, validated)
    _assert_descendant_set_unchanged(root, validated)
    journal_path, journal = _load_or_create_journal(
        root, validated, direction="apply",
    )
    index_path = _write_retired_root_index(root, validated, status="retiring")
    for entry in sorted(
        validated["entries"],
        key=lambda row: (-int(row["lineage_depth"]), str(row["job_id"])),
    ):
        _current_control, current_raw = _require_paused(root)
        if _sha256_bytes(current_raw) != invocation_control_sha:
            raise LegacyOneTimeMigrationError(
                "global-control.json changed during retirement apply"
            )
        _assert_descendant_set_unchanged(root, validated)
        position = _entry_position(root, entry)
        source = _path_from_relative(root, entry["source"], label="job source")
        target = _path_from_relative(root, entry["archive_target"], label="archive target")
        if position == "original":
            cancelled_raw = _decoded_job_bytes(
                entry, "cancelled_job_json_base64", "cancelled_job_sha256",
            )
            _replace_job_json(root, journal_path, entry, cancelled_raw)
            if _entry_position(root, entry) != "cancelled_live":
                raise LegacyOneTimeMigrationError(
                    f"cancelled projection verification failed for {entry['job_id']}"
                )
            position = "cancelled_live"
        if position == "cancelled_live":
            _safe_directory_chain(root, target.parent, create=True)
            if source.parent.stat().st_dev != target.parent.stat().st_dev:
                raise LegacyOneTimeMigrationError(
                    f"archive target is not on the source filesystem: {entry['job_id']}"
                )
            if _entry_position(root, entry) != "cancelled_live":
                raise LegacyOneTimeMigrationError(
                    f"job changed immediately before archive: {entry['job_id']}"
                )
            os.replace(source, target)
            _fsync_directory(source.parent)
            _fsync_directory(target.parent)
        if _entry_position(root, entry) != "archived":
            raise LegacyOneTimeMigrationError(
                f"archive verification failed for {entry['job_id']}"
            )
        _update_journal(
            journal_path, journal, root, validated, status="applying",
        )
    _update_journal(journal_path, journal, root, validated, status="applied")
    _write_retired_root_index(root, validated, status="retired")
    return {
        "mode": "applied",
        "migration_id": validated["migration_id"],
        "plan_sha256": validated["plan_sha256"],
        "owner_count": validated["total_owner_count"],
        "descendant_count": validated["descendant_count"],
        "archived": validated["total_entry_count"],
        "journal": str(journal_path),
        "retired_root_index": str(index_path),
    }


def restore_retirement_plan(
    plan: Mapping[str, Any], *, approved_sha256: str,
    confirm_api_stopped: bool,
) -> dict[str, Any]:
    """Idempotently restore archived directories and their exact original state."""
    validated = validate_retirement_plan(plan)
    _assert_approved(validated, approved_sha256)
    if confirm_api_stopped is not True:
        raise LegacyOneTimeMigrationError(
            "restore requires an explicit API/workers-stopped confirmation"
        )
    root = _state_root(Path(validated["state_root"]))
    _control, control_raw = _require_paused(root)
    invocation_control_sha = _sha256_bytes(control_raw)
    _verify_plan_backup(root, validated)
    _preflight_positions(root, validated)
    _assert_descendant_set_unchanged(root, validated)
    journal_path, journal = _load_or_create_journal(
        root, validated, direction="restore",
    )
    index_path = _write_retired_root_index(root, validated, status="restoring")
    for entry in sorted(
        validated["entries"],
        key=lambda row: (int(row["lineage_depth"]), str(row["job_id"])),
    ):
        _current_control, current_raw = _require_paused(root)
        if _sha256_bytes(current_raw) != invocation_control_sha:
            raise LegacyOneTimeMigrationError(
                "global-control.json changed during retirement restore"
            )
        _assert_descendant_set_unchanged(root, validated)
        position = _entry_position(root, entry)
        source = _path_from_relative(root, entry["source"], label="job source")
        target = _path_from_relative(root, entry["archive_target"], label="archive target")
        if position == "archived":
            if source.exists():
                raise LegacyOneTimeMigrationError(
                    f"live job appeared before restore: {entry['job_id']}"
                )
            if target.parent.stat().st_dev != source.parent.stat().st_dev:
                raise LegacyOneTimeMigrationError(
                    f"restore target is not on the archive filesystem: {entry['job_id']}"
                )
            os.replace(target, source)
            _fsync_directory(target.parent)
            _fsync_directory(source.parent)
            if _entry_position(root, entry) != "cancelled_live":
                raise LegacyOneTimeMigrationError(
                    f"restored directory verification failed for {entry['job_id']}"
                )
            position = "cancelled_live"
        if position == "cancelled_live":
            original_raw = _decoded_job_bytes(
                entry, "original_job_json_base64", "original_job_sha256",
            )
            _replace_job_json(root, journal_path, entry, original_raw)
        if _entry_position(root, entry) != "original":
            raise LegacyOneTimeMigrationError(
                f"original-state verification failed for {entry['job_id']}"
            )
        _update_journal(
            journal_path, journal, root, validated, status="restoring",
        )
    _update_journal(journal_path, journal, root, validated, status="restored")
    _write_retired_root_index(root, validated, status="restored")
    return {
        "mode": "restored",
        "migration_id": validated["migration_id"],
        "plan_sha256": validated["plan_sha256"],
        "owner_count": validated["total_owner_count"],
        "descendant_count": validated["descendant_count"],
        "restored": validated["total_entry_count"],
        "journal": str(journal_path),
        "retired_root_index": str(index_path),
    }
