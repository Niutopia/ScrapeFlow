#!/usr/bin/env python3
"""Create a fail-closed backup of host-mounted ScrapeFlow state.

A snapshot is only accepted while the ScrapeFlow global pause is durably
active AND no media mutation or replenishment search is in flight.  When the
system is running normally the script first asks the API to persist a pause
(coordinated through ``POST /api/control/pause`` so
the in-memory scheduler gate closes together with the durable document),
waits for in-flight work to drain, snapshots, and then restores the exact
previous pause state -- even when an exception interrupts the backup.  A
durable pause journal makes a crash between pause and restore self-healing:
the next run restores the original control document before doing anything
else.  AList's SQLite database is copied through SQLite's backup API; JSON
and job artifacts are copied only when each source file remains stable, and
the whole application tree must be byte-for-byte stable across the copy.

The script never writes to the API or to the state directories outside of
the pause it is entitled to create; ``--check`` performs a read-only gate
inspection and changes nothing.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from typing import Any


SNAPSHOT_NAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{6}[+-]\d{4}$")
CONTROL_FILE_NAME = "global-control.json"
SEARCH_LOCKS_DIR_NAME = ".replenishment-search-locks"
SLOT_LOCK_RE = re.compile(r"^slot-\d+\.lock$")
PAUSE_JOURNAL_NAME = ".pause-journal.json"
BACKUP_PAUSE_REASON_PREFIX = "host-state-backup:"

# Mirrors local/scrapeflow_api/contracts.py.  These phases mean a dangerous
# media mutation (archive extraction, media execution, recovery execution)
# is actively owned by a scheduler worker.
EXECUTION_PHASES = frozenset({
    "starting_archive_execution", "extracting_archives",
    "starting_media_execution", "executing_media",
    "starting_recovery_execution", "executing_recovery",
})
# ``replenishing`` is a durable coordinator lifecycle, not proof that work is
# currently in flight.  Under unlimited retry it remains set while the worker
# is paused at a maintenance boundary.  Actual replenishment activity is
# covered by held slot-lock probes below.  Write-capable acquisition stages are
# serialized with the global pause transition, so a successful pause has
# already drained them; treating the lifecycle phase as busy would starve every
# backup while resumable coordinators wait behind that gate.
BUSY_PHASES = EXECUTION_PHASES
ACTIVE_WRITE_LABEL = "media mutation or replenishment search in flight"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument(
        "--state-root",
        type=Path,
        default=Path.home() / "文件" / "Codex WorkSpace" / "ScrapeFlow" / "state",
        help="host directory containing alist-data and scrapeflow-data",
    )
    value.add_argument(
        "--backup-root",
        type=Path,
        default=Path.home() / "文件" / "Codex WorkSpace" / "ScrapeFlow" / "backups",
        help="destination directory for timestamped snapshots",
    )
    value.add_argument(
        "--retention",
        type=int,
        default=14,
        help="number of automated snapshots to retain",
    )
    value.add_argument(
        "--api-url",
        default=None,
        help="ScrapeFlow API base URL (default: http://127.0.0.1:$SCRAPEFLOW_PORT)",
    )
    value.add_argument(
        "--quiesce-timeout",
        type=int,
        default=600,
        help="seconds to wait for in-flight work to drain before failing closed (default: 600)",
    )
    value.add_argument(
        "--quiesce-poll",
        type=int,
        default=5,
        help="seconds between quiescence re-checks (default: 5)",
    )
    value.add_argument(
        "--check",
        action="store_true",
        help="read-only gate inspection: report API reachability, pause state and "
        "busy signals without pausing or snapshotting",
    )
    return value


def default_api_url() -> str:
    configured = os.getenv("SCRAPEFLOW_API_URL", "").strip()
    if configured:
        return configured
    port = os.getenv("SCRAPEFLOW_PORT", "3010").strip()
    if not port.isdigit():
        port = "3010"
    return f"http://127.0.0.1:{port}"


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise RuntimeError(f"expected a JSON object: {path}")
    return value


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_atomic_bytes(path: Path, data: bytes, mode: int = 0o600) -> None:
    """Durably replace ``path`` with ``data`` (tmp + fsync + rename)."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(temporary, mode)
    temporary.replace(path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def write_atomic_json(path: Path, payload: dict[str, Any], mode: int = 0o600) -> None:
    data = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    write_atomic_bytes(path, data.encode("utf-8"), mode=mode)


def _strict_paused(control: dict[str, Any], source: str) -> bool:
    paused = control.get("paused")
    if type(paused) is not bool:
        raise RuntimeError(f"{source} paused must be a strict boolean")
    return paused


def require_paused(scrapeflow_dir: Path) -> dict[str, Any]:
    control_path = scrapeflow_dir / CONTROL_FILE_NAME
    control = read_json(control_path)
    if _strict_paused(control, f"durable global control {control_path}") is not True:
        raise RuntimeError(
            f"refusing an application-state backup while global pause is not active: {control_path}"
        )
    return control


class ApiError(RuntimeError):
    """The ScrapeFlow API is unreachable or rejected the request."""


class BusyStateError(RuntimeError):
    """In-flight media mutation or replenishment search did not drain in time."""


class ApiClient:
    """Minimal JSON client for the ScrapeFlow control endpoints."""

    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/")

    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None, *,
        timeout: float = 10.0,
    ) -> dict[str, Any]:
        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(self.base + path, data=data, method=method)
        request.add_header("Content-Type", "application/json; charset=utf-8")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", "replace")[:200]
            except OSError:
                pass
            raise ApiError(
                f"API {method} {path} returned {exc.code}: {detail}"
            ) from exc
        except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError) as exc:
            raise ApiError(f"API {method} {path} failed: {exc}") from exc
        if not isinstance(payload, dict):
            raise ApiError(f"API {method} {path} returned a non-object payload")
        return payload

    def control(self) -> dict[str, Any]:
        return self._request("GET", "/api/control")

    def jobs(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/api/jobs")
        jobs = payload.get("jobs")
        if not isinstance(jobs, list) or not all(isinstance(row, dict) for row in jobs):
            raise ApiError("API /api/jobs returned an invalid payload")
        return jobs

    def pause(self, reason: str) -> dict[str, Any]:
        return self._request("POST", "/api/control/pause", {"confirm": True, "reason": reason})

    def resume(self) -> dict[str, Any]:
        return self._request("POST", "/api/control/resume", {"confirm": True})


def require_consistent_pause(
    api: ApiClient,
    control_path: Path,
    *,
    expected: bool,
    expected_sha256: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Require one strict pause bit shared by the API and durable document."""
    live = api.control()
    durable = read_json(control_path)
    live_paused = _strict_paused(live, "API global control")
    durable_paused = _strict_paused(
        durable, f"durable global control {control_path}",
    )
    if live_paused is not durable_paused or live.get("reason") != durable.get("reason"):
        raise RuntimeError(
            "API and durable global control disagree; refusing an inconsistent backup"
        )
    if live_paused is not expected:
        state = "active" if expected else "open"
        raise RuntimeError(f"global pause is not {state} in both API and durable state")
    if expected_sha256 is not None:
        verify_control_sha(control_path, expected_sha256)
    return live, durable


def job_phase_is_busy(_job_dir: Path, phase: Any) -> bool:
    if not isinstance(phase, str) or phase not in BUSY_PHASES:
        return False
    return True


def held_slot_locks(lock_dir: Path) -> list[str]:
    """Names of replenishment search slots whose flock is currently held.

    Slot files may legitimately exist without a holder; the probe must never
    create or modify them, so it opens read-only.
    """
    if not lock_dir.is_dir():
        return []
    held: list[str] = []
    for path in sorted(lock_dir.iterdir()):
        if not path.is_file() or not SLOT_LOCK_RE.fullmatch(path.name):
            continue
        try:
            handle = os.open(path, os.O_RDONLY)
        except OSError:
            # An unreadable known lock file cannot prove that its slot is free.
            held.append(path.name)
            continue
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            held.append(path.name)
        except OSError:
            held.append(path.name)
        else:
            fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            os.close(handle)
    return held


def disk_gate_reasons(state_dir: Path) -> list[str]:
    """Durable on-disk busy signals, independent of the API process."""
    reasons: list[str] = []
    jobs_dir = state_dir / "jobs"
    if jobs_dir.is_dir():
        for job_json in sorted(jobs_dir.glob("*/job.json")):
            try:
                doc = read_json(job_json)
            except (OSError, ValueError, json.JSONDecodeError):
                reasons.append(f"job_state_unreadable_disk:{job_json.parent.name}")
                continue
            phase = doc.get("phase")
            if not isinstance(phase, str) or not phase:
                reasons.append(f"job_phase_invalid_disk:{job_json.parent.name}")
                continue
            if job_phase_is_busy(job_json.parent, phase):
                reasons.append(f"job_executing_disk:{job_json.parent.name}:{phase}")
    for name in held_slot_locks(state_dir / SEARCH_LOCKS_DIR_NAME):
        reasons.append(f"replenishment_search_locked:{name}")
    return reasons


def api_gate_reasons(api: ApiClient, state_dir: Path) -> list[str]:
    """Live busy signals from the API process itself."""
    reasons: list[str] = []
    for job in api.jobs():
        job_id = job.get("id")
        phase = job.get("phase")
        if not isinstance(job_id, str) or not job_id or not isinstance(phase, str) or not phase:
            reasons.append("job_state_invalid_api")
            continue
        if job_phase_is_busy(
            state_dir / "jobs" / job_id, phase,
        ):
            reasons.append(f"job_executing:{job_id}:{phase}")
    return reasons


def evaluate_gate(api: ApiClient, state_dir: Path) -> dict[str, Any]:
    """Fail-closed gate: busy iff any live or durable signal is active."""
    reasons = sorted(set(api_gate_reasons(api, state_dir) + disk_gate_reasons(state_dir)))
    return {
        "busy": bool(reasons),
        "reasons": reasons,
        "checked_at": utc_now(),
        "label": ACTIVE_WRITE_LABEL if reasons else None,
    }


def wait_for_quiescence(
    api: ApiClient,
    state_dir: Path,
    *,
    timeout: float,
    poll: float,
    expected_control_sha256: str | None = None,
) -> dict[str, Any]:
    """Poll the gate until it clears, or fail closed on timeout.

    The persistent pause must also survive every poll: if it was lifted by an
    operator while we waited, the backup aborts instead of snapshotting under
    an open scheduler gate.
    """
    started = time.monotonic()
    last_reasons: list[str] = []

    def require_wait_pause() -> None:
        try:
            require_consistent_pause(
                api,
                state_dir / CONTROL_FILE_NAME,
                expected=True,
                expected_sha256=expected_control_sha256,
            )
        except RuntimeError as exc:
            raise RuntimeError(
                "global pause was lifted or became inconsistent while waiting for quiescence"
            ) from exc

    while True:
        require_wait_pause()
        last_reasons = evaluate_gate(api, state_dir)["reasons"]
        # The gate probes can take time.  Recheck the sole pause state even
        # when no busy reason remains, immediately before declaring quiescence.
        require_wait_pause()
        if not last_reasons:
            return {
                "waited_seconds": round(time.monotonic() - started, 3),
                "reasons": [],
            }
        if time.monotonic() - started >= timeout:
            raise BusyStateError(
                "refusing a backup while %s is in flight: %s"
                % (ACTIVE_WRITE_LABEL, ", ".join(last_reasons))
            )
        time.sleep(poll)


def copy_stable_file(source: Path, destination: Path, attempts: int = 3) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(attempts):
        before = source.stat()
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError(f"state entry is not a regular file: {source}")
        temporary = destination.with_name(destination.name + ".copying")
        try:
            shutil.copyfile(source, temporary)
            after = source.stat()
            if (
                before.st_size,
                before.st_mtime_ns,
                stat.S_IMODE(before.st_mode),
            ) == (
                after.st_size,
                after.st_mtime_ns,
                stat.S_IMODE(after.st_mode),
            ):
                os.replace(temporary, destination)
                # The staging root is private (mkdtemp mode 0700).  Preserve
                # source permissions inside it so the tar is exact mode
                # evidence, while the resulting archive itself stays 0600.
                os.chmod(destination, stat.S_IMODE(before.st_mode))
                return
        finally:
            temporary.unlink(missing_ok=True)
        if attempt + 1 < attempts:
            time.sleep(0.1)
    raise RuntimeError(f"state file changed repeatedly during backup: {source}")


def copy_stable_tree(source: Path, destination: Path) -> int:
    source_meta = source.stat()
    if not stat.S_ISDIR(source_meta.st_mode):
        raise RuntimeError(f"state directory is missing: {source}")
    destination.mkdir(parents=True, exist_ok=True)
    os.chmod(destination, stat.S_IMODE(source_meta.st_mode))
    copied = 0
    for root, directories, files in os.walk(source, followlinks=False):
        root_path = Path(root)
        for name in directories:
            entry = root_path / name
            if entry.is_symlink():
                raise RuntimeError(f"refusing symlink in application state: {entry}")
            metadata = entry.stat()
            if not stat.S_ISDIR(metadata.st_mode):
                raise RuntimeError(f"state entry is not a directory: {entry}")
            copied_directory = destination / entry.relative_to(source)
            copied_directory.mkdir(parents=True, exist_ok=True)
            os.chmod(copied_directory, stat.S_IMODE(metadata.st_mode))
        for name in files:
            entry = root_path / name
            if entry.is_symlink():
                raise RuntimeError(f"refusing symlink in application state: {entry}")
            copy_stable_file(entry, destination / entry.relative_to(source))
            copied += 1
    return copied


def tree_fingerprint(root: Path) -> list[tuple[str, str, int, int, int]]:
    """Path, kind, mode, size and mtime for every entry, including empty dirs."""
    root_meta = root.stat()
    rows: list[tuple[str, str, int, int, int]] = [(
        ".", "directory", stat.S_IMODE(root_meta.st_mode),
        root_meta.st_size, root_meta.st_mtime_ns,
    )]
    for walk_root, directories, files in os.walk(root, followlinks=False):
        directories.sort()
        files.sort()
        for name in directories:
            entry = Path(walk_root) / name
            if entry.is_symlink():
                raise RuntimeError(f"refusing symlink in application state: {entry}")
            meta = entry.stat()
            rows.append((
                str(entry.relative_to(root)), "directory",
                stat.S_IMODE(meta.st_mode), meta.st_size, meta.st_mtime_ns,
            ))
        for name in files:
            entry = Path(walk_root) / name
            if entry.is_symlink():
                raise RuntimeError(f"refusing symlink in application state: {entry}")
            meta = entry.stat()
            rows.append((
                str(entry.relative_to(root)), "file",
                stat.S_IMODE(meta.st_mode), meta.st_size, meta.st_mtime_ns,
            ))
    return rows


def backup_sqlite(source: Path, destination: Path) -> None:
    if not source.is_file():
        raise RuntimeError(f"AList database is missing: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_uri = f"file:{source.resolve()}?mode=ro"
    with sqlite3.connect(source_uri, uri=True) as source_db:
        with sqlite3.connect(destination) as backup_db:
            source_db.backup(backup_db)
            result = backup_db.execute("PRAGMA integrity_check").fetchone()
    if result != ("ok",):
        raise RuntimeError(f"AList backup integrity check failed: {result!r}")
    os.chmod(destination, 0o600)


def create_archive(source: Path, destination: Path) -> None:
    with tarfile.open(destination, "w:gz") as archive:
        archive.add(source, arcname=source.name, recursive=True)
    os.chmod(destination, 0o600)
    with tarfile.open(destination, "r:gz") as archive:
        archive.getmembers()


def prune_snapshots(backup_root: Path, retention: int, current: Path) -> list[str]:
    if retention < 1:
        raise RuntimeError("retention must be at least 1")
    snapshots = sorted(
        path
        for path in backup_root.iterdir()
        if path.is_dir() and SNAPSHOT_NAME_RE.fullmatch(path.name)
    )
    removed: list[str] = []
    for path in snapshots[:-retention]:
        if path == current:
            continue
        shutil.rmtree(path)
        removed.append(path.name)
    return removed


def verify_control_sha(control_path: Path, expected: str) -> None:
    if sha256(control_path) != expected:
        raise RuntimeError("global control document changed during backup")


def pause_journal_payload(
    *, snapshot_name: str, original_bytes: bytes, original_control: dict[str, Any], api: ApiClient,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "snapshot_name": snapshot_name,
        "stage": "pausing",
        "reason": BACKUP_PAUSE_REASON_PREFIX + snapshot_name,
        "original_control_sha256": sha256_bytes(original_bytes),
        "original_control_b64": base64.b64encode(original_bytes).decode("ascii"),
        "original_control": original_control,
        "api_url": api.base,
        "created_at": utc_now(),
    }


def restore_pause(
    api: ApiClient, control_path: Path, journal_path: Path, journal: dict[str, Any],
) -> dict[str, Any]:
    """Return the system to the pause state captured in ``journal``.

    Prefers the API so the in-memory scheduler gate closes together with the
    durable document; falls back to a byte-exact direct restore when the API
    is unreachable.  If the durable document no longer carries our pause
    reason, an operator has taken over the state and we must not clobber it.
    """
    outcome: dict[str, Any] = {"restored": False}
    try:
        live = api.control()
    except ApiError:
        live = None
    if live is not None:
        live_paused = _strict_paused(live, "API global control")
        durable = read_json(control_path)
        durable_paused = _strict_paused(
            durable, f"durable global control {control_path}",
        )
        if (
            live_paused is not durable_paused
            or live.get("reason") != durable.get("reason")
        ):
            raise RuntimeError(
                "API and durable global control disagree before pause restoration"
            )
        if live.get("reason") == journal.get("reason"):
            if live_paused is not True:
                raise RuntimeError("backup pause journal is not active before resume")
            paused_sha = journal.get("paused_document_sha256")
            if isinstance(paused_sha, str):
                verify_control_sha(control_path, paused_sha)
            after = api.resume()
            if _strict_paused(after, "API resume response") is not False:
                raise RuntimeError("API resume did not clear the global pause")
            require_consistent_pause(api, control_path, expected=False)
            outcome["method"] = "api_resume"
            outcome["resumed_at"] = utc_now()
        else:
            outcome["method"] = "operator_state_preserved"
            outcome["note"] = (
                "current pause reason differs from the backup journal; operator state kept"
            )
    else:
        # The API is unreachable, so the durable document itself decides.
        # Only restore byte-exactly when the document still carries our pause
        # (or when it never moved from the journal's record); an operator's
        # newer state must never be clobbered.
        control_exists = control_path.exists()
        try:
            file_reason = read_json(control_path).get("reason")
            file_readable = True
        except (OSError, ValueError, json.JSONDecodeError):
            file_reason = None
            file_readable = False
        if control_exists and not file_readable:
            raise RuntimeError(
                "control document exists but is unreadable; refusing to restore blindly"
            )
        if file_readable and file_reason != journal.get("reason"):
            # An operator owns the current pause; never clobber it.
            outcome["method"] = "operator_state_preserved"
            outcome["note"] = (
                "current control reason differs from the backup journal; operator state kept"
            )
            journal_path.unlink(missing_ok=True)
            outcome["restored"] = True
            return outcome
        paused_sha = journal.get("paused_document_sha256")
        if isinstance(paused_sha, str) and control_exists:
            try:
                current_sha = sha256(control_path)
            except OSError:
                current_sha = None
            if current_sha != paused_sha:
                raise RuntimeError(
                    "control document changed after the pause journal was written; "
                    "refusing to restore blindly"
                )
        original = base64.b64decode(journal.get("original_control_b64") or "")
        if not original:
            raise RuntimeError("pause journal is missing the original control document")
        write_atomic_bytes(control_path, original)
        if sha256(control_path) != journal.get("original_control_sha256"):
            raise RuntimeError("direct control restore did not match the original document")
        outcome["method"] = "direct_file"
        outcome["resumed_at"] = utc_now()
    journal_path.unlink(missing_ok=True)
    outcome["restored"] = True
    return outcome


def recover_pause_journal(
    journal_path: Path, control_path: Path, api: ApiClient,
) -> dict[str, Any]:
    """Self-heal a crashed run: restore any pause we left behind.

    A journal whose control document already matches the recorded original is
    a stale success marker and is simply cleared.  Otherwise the recorded
    reason decides whether we restore (API resume, or byte-exact direct write
    when the API is unreachable) or whether an operator owns the state now.
    """
    if not journal_path.is_file():
        return {"recovered": False}
    journal = read_json(journal_path)
    try:
        current = sha256(control_path)
    except OSError:
        current = None
    if current == journal.get("original_control_sha256"):
        journal_path.unlink(missing_ok=True)
        return {"recovered": True, "action": "stale_success_cleared", "journal": journal}
    if not isinstance(journal.get("reason"), str):
        raise RuntimeError(
            "pause journal is malformed and the control document differs from its original; "
            "refusing to guess"
        )
    outcome = restore_pause(api, control_path, journal_path, journal)
    return {"recovered": True, "action": "restored", **outcome, "journal": journal}


def check_report(api: ApiClient, state_dir: Path, control_path: Path) -> dict[str, Any]:
    """Read-only inspection used by ``--check`` (never changes state)."""
    live = api.control()
    file_control = read_json(control_path)
    pause_error = None
    try:
        live_paused = _strict_paused(live, "API global control")
        file_paused = _strict_paused(
            file_control, f"durable global control {control_path}",
        )
    except RuntimeError as exc:
        live_paused = None
        file_paused = None
        pause_error = str(exc)
    gate = evaluate_gate(api, state_dir)
    return {
        "check": True,
        "api_url": api.base,
        "control": live,
        "control_file": file_control,
        "control_consistent": (
            pause_error is None
            and live_paused is file_paused
            and live.get("reason") == file_control.get("reason")
        ),
        "control_error": pause_error,
        "busy": gate["busy"],
        "reasons": gate["reasons"],
        "would_pause": None if file_paused is None else not file_paused,
        "checked_at": gate["checked_at"],
    }


def run_locked(
    args: argparse.Namespace,
    api: ApiClient,
    state_root: Path,
    backup_root: Path,
    alist_dir: Path,
    scrapeflow_dir: Path,
    control_path: Path,
) -> dict[str, Any]:
    journal_path = backup_root / PAUSE_JOURNAL_NAME
    recovery = recover_pause_journal(journal_path, control_path, api)

    started = datetime.now().astimezone()
    snapshot_name = started.strftime("%Y-%m-%dT%H%M%S%z")
    final_dir = backup_root / snapshot_name
    if final_dir.exists():
        raise RuntimeError(f"snapshot already exists: {final_dir}")

    original_bytes = control_path.read_bytes()
    original_control = read_json(control_path)
    already_paused = _strict_paused(
        original_control, f"durable global control {control_path}",
    )
    original_sha = sha256_bytes(original_bytes)
    require_consistent_pause(
        api, control_path, expected=already_paused,
        expected_sha256=original_sha,
    )

    evidence: dict[str, Any] = {
        "mode": "already_paused" if already_paused else "paused_by_backup",
        "recovery": recovery,
    }
    paused_by_us = False
    pause_document_sha: str | None = None
    paused_document = original_control
    journal: dict[str, Any] | None = None

    try:
        if already_paused:
            require_consistent_pause(
                api, control_path, expected=True,
                expected_sha256=original_sha,
            )
        else:
            journal = pause_journal_payload(
                snapshot_name=snapshot_name,
                original_bytes=original_bytes,
                original_control=original_control,
                api=api,
            )
            # The journal must be durable before the pause is requested so a
            # crash between the two can still be restored.
            write_atomic_json(journal_path, journal)
            # Narrow the read/pause race: if an operator changed either the
            # durable document or the live gate after our initial snapshot,
            # abandon without issuing a pause that could overwrite their
            # reason.  The API remains the final authority; this is a
            # best-effort compare-before-set around an endpoint without CAS.
            try:
                require_consistent_pause(
                    api, control_path, expected=False,
                    expected_sha256=original_sha,
                )
            except RuntimeError:
                journal_path.unlink(missing_ok=True)
                journal = None
                raise RuntimeError(
                    "global control changed before the backup pause; refusing to overwrite operator state"
                )
            pause_response = api.pause(journal["reason"])
            if _strict_paused(pause_response, "API pause response") is not True:
                raise RuntimeError("API pause did not activate the global pause")
            paused_bytes = control_path.read_bytes()
            paused_document = read_json(control_path)
            pause_document_sha = sha256_bytes(paused_bytes)
            require_consistent_pause(
                api, control_path, expected=True,
                expected_sha256=pause_document_sha,
            )
            journal["stage"] = "paused"
            journal["paused_document_sha256"] = pause_document_sha
            write_atomic_json(journal_path, journal)
            paused_by_us = True

        quiescence = wait_for_quiescence(
            api,
            scrapeflow_dir,
            timeout=float(args.quiesce_timeout),
            poll=float(args.quiesce_poll),
            expected_control_sha256=(
                pause_document_sha if paused_by_us else original_sha
            ),
        )
        evidence["quiescence"] = quiescence

        expected_control_sha = pause_document_sha if paused_by_us else sha256_bytes(original_bytes)
        require_paused(scrapeflow_dir)
        verify_control_sha(control_path, expected_control_sha)
        require_consistent_pause(
            api, control_path, expected=True,
            expected_sha256=expected_control_sha,
        )
        final_gate = evaluate_gate(api, scrapeflow_dir)
        if final_gate["busy"]:
            raise BusyStateError(
                "work became active immediately before snapshotting: "
                + ", ".join(final_gate["reasons"])
            )
        fingerprint_before = tree_fingerprint(scrapeflow_dir)

        temporary_dir = Path(tempfile.mkdtemp(prefix=".backup-", dir=backup_root))
        try:
            stage_dir = temporary_dir / "stage"
            alist_stage = stage_dir / "alist-data"
            scrapeflow_stage = stage_dir / "scrapeflow-data"
            alist_stage.mkdir(parents=True)

            backup_sqlite(alist_dir / "data.db", alist_stage / "data.db")
            copy_stable_file(alist_dir / "config.json", alist_stage / "config.json")
            copied_files = copy_stable_tree(scrapeflow_dir, scrapeflow_stage)

            # The pause, the durable control document and the whole tree must
            # all be stable across the copy.
            require_paused(scrapeflow_dir)
            verify_control_sha(control_path, expected_control_sha)
            require_consistent_pause(
                api, control_path, expected=True,
                expected_sha256=expected_control_sha,
            )
            after_gate = evaluate_gate(api, scrapeflow_dir)
            if after_gate["busy"]:
                raise BusyStateError(
                    "work became active during the copy: " + ", ".join(after_gate["reasons"])
                )
            fingerprint_after = tree_fingerprint(scrapeflow_dir)
            if fingerprint_after != fingerprint_before:
                raise RuntimeError("application state changed during backup; refusing the snapshot")

            alist_archive = temporary_dir / "alist-data.tar.gz"
            scrapeflow_archive = temporary_dir / "scrapeflow-data.tar.gz"
            create_archive(alist_stage, alist_archive)
            create_archive(scrapeflow_stage, scrapeflow_archive)
            completed = datetime.now().astimezone()
            manifest: dict[str, Any] = {
                "schema_version": 1,
                "started_at": started.isoformat(),
                "completed_at": completed.isoformat(),
                "state_root": str(state_root),
                "global_control": require_paused(scrapeflow_dir),
                "scrapeflow_file_count": copied_files,
                "paused_during_backup": True,
                "coordination": {
                    "mode": evidence["mode"],
                    "paused_at": paused_document.get("updated_at") if paused_by_us else None,
                    "quiescence": quiescence,
                    "api_url": api.base,
                },
                "archives": {
                    alist_archive.name: {
                        "bytes": alist_archive.stat().st_size,
                        "sha256": sha256(alist_archive),
                    },
                    scrapeflow_archive.name: {
                        "bytes": scrapeflow_archive.stat().st_size,
                        "sha256": sha256(scrapeflow_archive),
                    },
                },
            }
            shutil.rmtree(stage_dir)
            manifest_path = temporary_dir / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            os.chmod(manifest_path, 0o600)
            require_consistent_pause(
                api, control_path, expected=True,
                expected_sha256=expected_control_sha,
            )
            os.replace(temporary_dir, final_dir)
            removed = prune_snapshots(backup_root, args.retention, final_dir)
            evidence["snapshot"] = str(final_dir)
            evidence["removed"] = removed
            evidence["manifest"] = manifest
        except Exception:
            shutil.rmtree(temporary_dir, ignore_errors=True)
            raise
    finally:
        if journal is not None:
            # A journal was written before the pause request, so a restore is
            # owed whenever control of the durable document left our hands --
            # including a pause request that failed partway through.
            try:
                restored = restore_pause(api, control_path, journal_path, journal)
            except Exception as error:  # keep the primary error; report loudly
                restored = {
                    "restored": False,
                    "error": f"{type(error).__name__}: {error}",
                }
                print(
                    "CRITICAL: the backup left the global pause active and could not "
                    f"restore it: {error}",
                    file=sys.stderr,
                )
            evidence["restore"] = restored
        elif already_paused:
            # We never touched the control document; verify we did not.
            try:
                if control_path.read_bytes() != original_bytes:
                    evidence["control_changed_by_operator"] = True
            except OSError:
                pass

    if "snapshot" in evidence:
        resumed = evidence.get("restore", {}).get("resumed_at")
        if resumed:
            manifest_path = final_dir / "manifest.json"
            manifest = read_json(manifest_path)
            manifest["coordination"]["resumed_at"] = resumed
            manifest["coordination"]["restore_method"] = evidence["restore"].get("method")
            manifest["coordination"]["restored"] = evidence["restore"].get("restored")
            write_atomic_json(manifest_path, manifest)
    return evidence


def run(argv: list[str]) -> dict[str, Any]:
    args = parser().parse_args(argv)
    os.umask(0o077)
    if args.quiesce_timeout < 0:
        raise RuntimeError("--quiesce-timeout must be >= 0")
    if args.quiesce_poll < 1:
        raise RuntimeError("--quiesce-poll must be >= 1")
    state_root = args.state_root.expanduser().resolve()
    backup_root = args.backup_root.expanduser().resolve()
    alist_dir = state_root / "alist-data"
    scrapeflow_dir = state_root / "scrapeflow-data"
    control_path = scrapeflow_dir / CONTROL_FILE_NAME
    if not control_path.is_file():
        raise RuntimeError(f"missing durable control document: {control_path}")
    api = ApiClient(args.api_url or default_api_url())

    if args.check:
        return check_report(api, scrapeflow_dir, control_path)

    # Prove the loopback control API is reachable before creating a backup
    # directory or pause journal.  This preserves the fail-closed boundary
    # that the former session-token handshake happened to provide.
    api.control()
    backup_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(backup_root, 0o700)
    lock_path = backup_root / ".backup.lock"
    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another host-state backup is already running") from error
        return run_locked(
            args, api, state_root, backup_root, alist_dir, scrapeflow_dir, control_path,
        )


def main() -> int:
    evidence = run(sys.argv[1:])
    print(json.dumps(evidence, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"host-state backup failed: {error}", file=sys.stderr)
        raise SystemExit(1)
