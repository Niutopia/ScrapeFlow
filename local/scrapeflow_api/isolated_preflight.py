"""Static preflight checks for a real isolated acceptance declaration."""

from __future__ import annotations

import hashlib
import json
import os
import posixpath
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

from .offline_backup import MANIFEST_NAME, OfflineBackupError, verify_offline_backup
from .provider_delivery import DEFAULT_DELIVERY_PARENT
from .release_checks import project_root


LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
FORMAL_SHELF_ROOTS = frozenset({
    "/quark/影视/电影",
    "/quark/影视/番剧",
    "/quark/影视/美剧",
})
REQUIRED_FALSE_FLAGS = {
    "intake_monitor": "intake monitor must stay off",
    "automatic_audit": "automatic audit must stay off before manual acceptance",
    "audit_repair": "audit repair must stay off",
    "provider_auto_repair": "provider auto repair must stay off",
    "old_backlog_restored": "old backlog must not be restored",
    "bulk_retry": "bulk retry must not run",
    "bulk_cleanup": "bulk cleanup must not run",
}
PLACEHOLDER_VALUES = frozenset({
    "isolated-storage-name",
    "external snapshot or restore note",
})
PREFLIGHT_REPORT_VERSION = 1
PREFLIGHT_REPORT_PASSED = "passed"
PREFLIGHT_REPORT_FAILED = "failed"
PREFLIGHT_REPORT_KEYS = frozenset({
    "version",
    "status",
    "checked_at",
    "declaration",
    "issues",
    "checks",
})
DECLARATION_TEXT_KEYS = (
    "api_url",
    "alist_url",
    "scrapeflow_state_dir",
    "alist_data_dir",
    "media_root",
    "storage_label",
    "offline_backup_manifest",
    "media_recovery_point",
)
DECLARATION_BOOL_KEYS = (
    "start_paused",
    "intake_monitor",
    "automatic_audit",
    "audit_repair",
    "provider_auto_repair",
    "one_task_at_a_time",
    "old_backlog_restored",
    "bulk_retry",
    "bulk_cleanup",
)


def isolated_preflight_template() -> dict[str, object]:
    """Return the JSON shape a real stage-10 declaration should fill."""
    return {
        "api_url": "http://127.0.0.1:8765",
        "alist_url": "http://127.0.0.1:5244",
        "scrapeflow_state_dir": "/absolute/isolated/scrapeflow-data",
        "alist_data_dir": "/absolute/isolated/alist-data",
        "media_root": "/quark/影视/ScrapeFlow/验收/<run-id>",
        "storage_label": "isolated-storage-name",
        "offline_backup_manifest": "/absolute/backup/scrapeflow-offline-backup.json",
        "media_recovery_point": "external snapshot or restore note",
        "provider_workers": 1,
        "start_paused": True,
        "intake_monitor": False,
        "automatic_audit": False,
        "audit_repair": False,
        "provider_auto_repair": False,
        "one_task_at_a_time": True,
        "old_backlog_restored": False,
        "bulk_retry": False,
        "bulk_cleanup": False,
    }


def _as_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _non_empty_text(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _looks_like_placeholder(value: str) -> bool:
    normalized = value.strip().casefold()
    return (
        normalized in PLACEHOLDER_VALUES
        or "<" in value
        or ">" in value
        or normalized.startswith("/absolute/")
        or "replace-with" in normalized
    )


def _loopback_url_issue(declaration: dict[str, object], key: str) -> str | None:
    value = _non_empty_text(declaration.get(key))
    if value is None:
        return f"{key} is required"
    if _looks_like_placeholder(value):
        return f"{key} must replace the template placeholder"
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return f"{key} must be an HTTP URL"
    if parsed.hostname not in LOOPBACK_HOSTS:
        return f"{key} must use a loopback host"
    return None


def _absolute_local_path(value: object) -> Path | None:
    text = _non_empty_text(value)
    if text is None:
        return None
    try:
        path = Path(text).expanduser()
        if not path.is_absolute():
            return None
        return path.resolve(strict=False)
    except (OSError, ValueError):
        return None


def _path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _path_issue(
    declaration: dict[str, object],
    key: str,
    *,
    repo_root: Path,
) -> tuple[Path | None, str | None]:
    path = _absolute_local_path(declaration.get(key))
    text = _non_empty_text(declaration.get(key))
    if text is not None and _looks_like_placeholder(text):
        return None, f"{key} must replace the template placeholder"
    if path is None:
        return None, f"{key} must be an absolute local path"
    if _path_is_within(path, repo_root):
        return path, f"{key} must be outside the repository"
    return path, None


def _directory_write_issue(path: Path, *, key: str) -> str | None:
    """Prove writability with a short-lived file rather than permission bits."""
    try:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=".scrapeflow-preflight-",
            dir=path,
        )
    except OSError as exc:
        return f"{key} must be writable: {exc}"
    probe_path = Path(raw_path)
    try:
        try:
            os.close(descriptor)
        except OSError as exc:
            return f"{key} preflight probe could not be closed: {exc}"
        try:
            probe_path.unlink()
        except OSError as exc:
            return f"{key} preflight probe could not be removed: {exc}"
    finally:
        if probe_path.exists():
            try:
                probe_path.unlink()
            except OSError:
                pass
    return None


def _isolated_directory_issue(
    declaration: dict[str, object],
    key: str,
    *,
    repo_root: Path,
) -> tuple[Path | None, str | None]:
    path, issue = _path_issue(declaration, key, repo_root=repo_root)
    if issue is not None or path is None:
        return path, issue
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        return path, f"{key} must exist and be accessible: {exc}"
    if not resolved.is_dir():
        return resolved, f"{key} must be an existing directory"
    try:
        if any(resolved.iterdir()):
            return resolved, f"{key} must be empty before isolated acceptance"
    except OSError as exc:
        return resolved, f"{key} could not be listed: {exc}"
    return resolved, _directory_write_issue(resolved, key=key)


def _captured_runtime_directory_issue(
    declaration: dict[str, object],
    key: str,
    *,
    repo_root: Path,
) -> tuple[Path | None, str | None]:
    """Revalidate durable directory facts without repeating empty probes."""
    path, issue = _path_issue(declaration, key, repo_root=repo_root)
    if issue is not None or path is None:
        return path, issue
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        return path, f"{key} must still exist and be accessible: {exc}"
    if not resolved.is_dir():
        return resolved, f"{key} must still be a directory"
    return resolved, None


def _offline_backup_manifest_issue(
    declaration: dict[str, object],
    *,
    repo_root: Path,
    runtime_dirs: tuple[Path | None, Path | None],
) -> str | None:
    key = "offline_backup_manifest"
    text = _non_empty_text(declaration.get(key))
    if text is None:
        return f"{key} is required"
    if _looks_like_placeholder(text):
        return f"{key} must replace the template placeholder"
    try:
        raw_path = Path(text).expanduser()
    except ValueError:
        return f"{key} must be an absolute local path"
    if not raw_path.is_absolute():
        return f"{key} must be an absolute local path"
    if raw_path.is_symlink():
        return f"{key} must be a regular manifest file"
    try:
        manifest_path = raw_path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        return f"{key} must exist and be accessible: {exc}"
    if not manifest_path.is_file() or manifest_path.name != MANIFEST_NAME:
        return f"{key} must point to {MANIFEST_NAME}"
    if _path_is_within(manifest_path, repo_root):
        return f"{key} must be outside the repository"
    backup_root = manifest_path.parent
    for runtime_key, runtime_dir in zip(
        ("scrapeflow_state_dir", "alist_data_dir"),
        runtime_dirs,
        strict=True,
    ):
        if runtime_dir is None:
            continue
        if _path_is_within(backup_root, runtime_dir) or _path_is_within(runtime_dir, backup_root):
            return f"{key} must be isolated from {runtime_key}"
    try:
        verify_offline_backup(backup_root)
    except OfflineBackupError as exc:
        return f"{key} is not a valid verified backup: {exc}"
    return None


def _remote_root_issue(value: object) -> str | None:
    text = _non_empty_text(value)
    if text is None:
        return "media_root is required"
    if _looks_like_placeholder(text):
        return "media_root must replace the template placeholder"
    if "\x00" in text or "\\" in text or not text.startswith("/"):
        return "media_root must be a safe absolute remote path"
    normalized = posixpath.normpath(text)
    if normalized != text.rstrip("/") or normalized == "/":
        return "media_root must be normalized and not the remote root"
    if any(part in {"", ".", ".."} for part in normalized.split("/")[1:]):
        return "media_root contains an unsafe path segment"
    if any(
        normalized == formal_root or normalized.startswith(formal_root + "/")
        for formal_root in FORMAL_SHELF_ROOTS
    ):
        return "media_root must not be a formal library shelf or its descendant"
    staging_parent = DEFAULT_DELIVERY_PARENT.rstrip("/")
    if normalized == staging_parent or normalized.startswith(staging_parent + "/"):
        return "media_root must not be the provider staging root"
    return None


def isolated_preflight_issues(
    declaration: dict[str, object],
    *,
    root: Path | None = None,
    require_empty_runtime_dirs: bool = True,
) -> list[str]:
    """Return issues that block a real isolated acceptance run.

    ``require_empty_runtime_dirs`` is disabled only while validating a
    previously captured report.  The normal live preflight always proves the
    two runtime directories are empty and writable immediately before start.
    """
    repo_root = (project_root() if root is None else Path(root)).resolve(strict=False)
    issues: list[str] = []
    for key in ("api_url", "alist_url"):
        issue = _loopback_url_issue(declaration, key)
        if issue:
            issues.append(issue)

    directory_check = (
        _isolated_directory_issue
        if require_empty_runtime_dirs
        else _captured_runtime_directory_issue
    )
    state_dir, state_issue = directory_check(
        declaration, "scrapeflow_state_dir", repo_root=repo_root,
    )
    alist_dir, alist_issue = directory_check(
        declaration, "alist_data_dir", repo_root=repo_root,
    )
    if state_issue:
        issues.append(state_issue)
    if alist_issue:
        issues.append(alist_issue)
    if state_dir is not None and alist_dir is not None:
        if state_dir == alist_dir:
            issues.append("scrapeflow_state_dir and alist_data_dir must be different")
        elif _path_is_within(state_dir, alist_dir) or _path_is_within(alist_dir, state_dir):
            issues.append("scrapeflow_state_dir and alist_data_dir must not be nested")

    media_issue = _remote_root_issue(declaration.get("media_root"))
    if media_issue:
        issues.append(media_issue)

    for key in ("storage_label", "media_recovery_point"):
        text = _non_empty_text(declaration.get(key))
        if text is None:
            issues.append(f"{key} is required")
        elif _looks_like_placeholder(text):
            issues.append(f"{key} must replace the template placeholder")

    manifest_issue = _offline_backup_manifest_issue(
        declaration,
        repo_root=repo_root,
        runtime_dirs=(state_dir, alist_dir),
    )
    if manifest_issue:
        issues.append(manifest_issue)

    if _as_int(declaration.get("provider_workers")) != 1:
        issues.append("provider_workers must be 1")
    if _as_bool(declaration.get("start_paused")) is not True:
        issues.append("start_paused must be true")
    if _as_bool(declaration.get("one_task_at_a_time")) is not True:
        issues.append("one_task_at_a_time must be true")
    for key, message in REQUIRED_FALSE_FLAGS.items():
        if _as_bool(declaration.get(key)) is not False:
            issues.append(message)
    return issues


def _captured_declaration_schema_issues(
    declaration: Mapping[str, object],
) -> list[str]:
    """Require a version-1 report to embed one canonical declaration shape."""
    expected = set(isolated_preflight_template())
    actual = set(declaration)
    issues: list[str] = []
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing:
        issues.append(f"declaration is missing keys: {', '.join(missing)}")
    if extra:
        issues.append(f"declaration has unknown keys: {', '.join(extra)}")
    for key in DECLARATION_TEXT_KEYS:
        if key in declaration and not isinstance(declaration[key], str):
            issues.append(f"declaration.{key} must be a string")
    if "provider_workers" in declaration:
        workers = declaration["provider_workers"]
        if isinstance(workers, bool) or not isinstance(workers, int):
            issues.append("declaration.provider_workers must be an integer")
    for key in DECLARATION_BOOL_KEYS:
        if key in declaration and not isinstance(declaration[key], bool):
            issues.append(f"declaration.{key} must be a boolean")
    return issues


def _deduplicate_issues(issues: list[str]) -> list[str]:
    return list(dict.fromkeys(issues))


def _sha512_file(path: Path) -> str:
    digest = hashlib.sha512()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _capture_passed_preflight_checks(
    declaration: Mapping[str, object],
) -> dict[str, object]:
    """Capture auditable facts while the two runtime directories are empty."""
    directories: dict[str, object] = {}
    for key in ("scrapeflow_state_dir", "alist_data_dir"):
        path = _absolute_local_path(declaration.get(key))
        if path is None:
            raise ValueError(f"{key} is not an absolute local path")
        resolved = path.resolve(strict=True)
        if not resolved.is_dir():
            raise ValueError(f"{key} is not a directory")
        entries = list(resolved.iterdir())
        write_issue = _directory_write_issue(resolved, key=key)
        if entries or write_issue is not None:
            raise ValueError(write_issue or f"{key} is no longer empty")
        stat = resolved.stat()
        directories[key] = {
            "resolved_path": str(resolved),
            "device": int(stat.st_dev),
            "inode": int(stat.st_ino),
            "is_directory": True,
            "entry_count": 0,
            "writable_probe": "passed",
        }

    manifest = _absolute_local_path(declaration.get("offline_backup_manifest"))
    if manifest is None:
        raise ValueError("offline_backup_manifest is not an absolute local path")
    manifest = manifest.resolve(strict=True)
    verify_offline_backup(manifest.parent)
    return {
        "runtime_directories": directories,
        "offline_backup_manifest": {
            "resolved_path": str(manifest),
            "sha512": _sha512_file(manifest),
            "verify": "passed",
        },
    }


def _captured_preflight_check_errors(
    checks: object,
    declaration: Mapping[str, object],
    *,
    require_transient_checks: bool,
) -> list[str]:
    """Validate captured facts and bind them to the same live directories."""
    if not isinstance(checks, Mapping):
        return ["checks must be a JSON object"]
    errors: list[str] = []
    expected_top = {"runtime_directories", "offline_backup_manifest"}
    if set(checks) != expected_top:
        errors.append("checks must contain runtime_directories and offline_backup_manifest")

    runtime = checks.get("runtime_directories")
    if not isinstance(runtime, Mapping):
        errors.append("checks.runtime_directories must be a JSON object")
    else:
        expected_runtime = {"scrapeflow_state_dir", "alist_data_dir"}
        if set(runtime) != expected_runtime:
            errors.append("checks.runtime_directories must contain both runtime directories")
        expected_keys = {
            "resolved_path", "device", "inode", "is_directory",
            "entry_count", "writable_probe",
        }
        for key in sorted(expected_runtime):
            snapshot = runtime.get(key)
            if not isinstance(snapshot, Mapping) or set(snapshot) != expected_keys:
                errors.append(f"checks.runtime_directories.{key} has an invalid schema")
                continue
            declared_path = _absolute_local_path(declaration.get(key))
            try:
                resolved = None if declared_path is None else declared_path.resolve(strict=True)
            except (OSError, RuntimeError):
                resolved = None
            if resolved is None or not resolved.is_dir():
                errors.append(f"checks.runtime_directories.{key} no longer resolves to a directory")
                continue
            stat = resolved.stat()
            device = snapshot.get("device")
            inode = snapshot.get("inode")
            if snapshot.get("resolved_path") != str(resolved):
                errors.append(f"checks.runtime_directories.{key}.resolved_path does not match")
            if isinstance(device, bool) or not isinstance(device, int) or device != stat.st_dev:
                errors.append(f"checks.runtime_directories.{key}.device does not match")
            if isinstance(inode, bool) or not isinstance(inode, int) or inode != stat.st_ino:
                errors.append(f"checks.runtime_directories.{key}.inode does not match")
            if snapshot.get("is_directory") is not True:
                errors.append(f"checks.runtime_directories.{key}.is_directory must be true")
            entry_count = snapshot.get("entry_count")
            if isinstance(entry_count, bool) or entry_count != 0:
                errors.append(f"checks.runtime_directories.{key}.entry_count must be zero")
            if snapshot.get("writable_probe") != "passed":
                errors.append(f"checks.runtime_directories.{key}.writable_probe must be passed")
            if require_transient_checks:
                try:
                    current_entries = sum(1 for _ in resolved.iterdir())
                except OSError:
                    current_entries = -1
                if current_entries != 0:
                    errors.append(f"checks.runtime_directories.{key} is no longer empty")
                write_issue = _directory_write_issue(resolved, key=key)
                if write_issue is not None:
                    errors.append(write_issue)

    backup = checks.get("offline_backup_manifest")
    expected_backup_keys = {"resolved_path", "sha512", "verify"}
    if not isinstance(backup, Mapping) or set(backup) != expected_backup_keys:
        errors.append("checks.offline_backup_manifest has an invalid schema")
    else:
        declared_manifest = _absolute_local_path(declaration.get("offline_backup_manifest"))
        try:
            manifest = None if declared_manifest is None else declared_manifest.resolve(strict=True)
        except (OSError, RuntimeError):
            manifest = None
        if manifest is None or not manifest.is_file():
            errors.append("checks.offline_backup_manifest no longer resolves to a file")
        else:
            if backup.get("resolved_path") != str(manifest):
                errors.append("checks.offline_backup_manifest.resolved_path does not match")
            digest = backup.get("sha512")
            try:
                actual_digest = _sha512_file(manifest)
            except OSError:
                actual_digest = ""
            if (
                not isinstance(digest, str)
                or len(digest) != 128
                or any(character not in "0123456789abcdef" for character in digest)
                or digest != actual_digest
            ):
                errors.append("checks.offline_backup_manifest.sha512 does not match")
            if backup.get("verify") != "passed":
                errors.append("checks.offline_backup_manifest.verify must be passed")
    return errors


def capture_isolated_preflight_report(
    declaration: Mapping[str, object],
    *,
    root: Path | None = None,
    checked_at: datetime | None = None,
) -> dict[str, object]:
    """Run live checks and return a self-contained stage-10 evidence report."""
    try:
        payload = json.loads(json.dumps(dict(declaration), allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"declaration must contain only JSON values: {exc}") from exc
    if not isinstance(payload, dict):  # defensive: ``dict`` above guarantees this
        raise ValueError("declaration must be a JSON object")
    issues = _deduplicate_issues([
        *_captured_declaration_schema_issues(payload),
        *isolated_preflight_issues(payload, root=root),
    ])
    checks: dict[str, object] = {}
    if not issues:
        try:
            checks = _capture_passed_preflight_checks(payload)
        except (OSError, RuntimeError, ValueError, OfflineBackupError) as exc:
            issues.append(f"captured preflight checks failed: {exc}")
    stamp = checked_at or datetime.now(UTC)
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("checked_at must include a timezone")
    return {
        "version": PREFLIGHT_REPORT_VERSION,
        "status": PREFLIGHT_REPORT_PASSED if not issues else PREFLIGHT_REPORT_FAILED,
        "checked_at": stamp.isoformat(),
        "declaration": payload,
        "issues": issues,
        "checks": checks,
    }


def validate_isolated_preflight_report(
    report: Mapping[str, object],
    *,
    expected_declaration: Mapping[str, object] | None = None,
    root: Path | None = None,
    require_passed: bool = False,
    require_transient_checks: bool = False,
) -> dict[str, object]:
    """Validate report structure, status coherence, and embedded declaration.

    A captured pass deliberately does not rerun the instantaneous empty and
    writable directory probes.  Every non-transient declaration constraint,
    including the external backup verification, is checked again.
    """
    if not isinstance(report, Mapping):
        raise ValueError("preflight report must be a JSON object")
    payload = dict(report)
    errors: list[str] = []
    actual_keys = set(payload)
    missing = sorted(PREFLIGHT_REPORT_KEYS - actual_keys)
    extra = sorted(actual_keys - PREFLIGHT_REPORT_KEYS)
    if missing:
        errors.append(f"report is missing keys: {', '.join(missing)}")
    if extra:
        errors.append(f"report has unknown keys: {', '.join(extra)}")

    version = payload.get("version")
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version != PREFLIGHT_REPORT_VERSION
    ):
        errors.append(f"version must be {PREFLIGHT_REPORT_VERSION}")

    status = payload.get("status")
    valid_status = (
        isinstance(status, str)
        and status in {PREFLIGHT_REPORT_PASSED, PREFLIGHT_REPORT_FAILED}
    )
    if not valid_status:
        errors.append("status must be passed or failed")

    checked_at = payload.get("checked_at")
    if not isinstance(checked_at, str) or not checked_at:
        errors.append("checked_at must be a non-empty ISO-8601 timestamp")
    else:
        try:
            parsed_at = datetime.fromisoformat(checked_at)
        except ValueError:
            errors.append("checked_at must be a valid ISO-8601 timestamp")
        else:
            if parsed_at.tzinfo is None or parsed_at.utcoffset() is None:
                errors.append("checked_at must include a timezone")

    declaration = payload.get("declaration")
    declaration_map: dict[str, object] | None = None
    if not isinstance(declaration, Mapping):
        errors.append("declaration must be a JSON object")
    else:
        declaration_map = dict(declaration)
        errors.extend(_captured_declaration_schema_issues(declaration_map))
        if expected_declaration is not None and declaration_map != dict(expected_declaration):
            errors.append("declaration does not match the expected declaration")

    raw_issues = payload.get("issues")
    report_issues: list[str] | None = None
    if not isinstance(raw_issues, list):
        errors.append("issues must be an array")
    elif any(not isinstance(issue, str) or not issue.strip() for issue in raw_issues):
        errors.append("issues must contain only non-empty strings")
    elif len(set(raw_issues)) != len(raw_issues):
        errors.append("issues must not contain duplicates")
    else:
        report_issues = list(raw_issues)

    checks = payload.get("checks")
    if status == PREFLIGHT_REPORT_PASSED and declaration_map is not None:
        errors.extend(_captured_preflight_check_errors(
            checks,
            declaration_map,
            require_transient_checks=require_transient_checks,
        ))
    elif status == PREFLIGHT_REPORT_FAILED and checks != {}:
        errors.append("failed reports must not contain passing checks")

    if report_issues is not None:
        derived_status = PREFLIGHT_REPORT_PASSED if not report_issues else PREFLIGHT_REPORT_FAILED
        if valid_status and status != derived_status:
            errors.append("status does not match issues")

    if status == PREFLIGHT_REPORT_PASSED and declaration_map is not None:
        stable_issues = isolated_preflight_issues(
            declaration_map,
            root=root,
            require_empty_runtime_dirs=False,
        )
        if stable_issues:
            errors.extend(
                f"captured declaration is invalid: {issue}"
                for issue in stable_issues
            )
    if require_passed and status != PREFLIGHT_REPORT_PASSED:
        errors.append("captured preflight report did not pass")
    if errors:
        raise ValueError("invalid preflight report: " + "; ".join(_deduplicate_issues(errors)))
    return payload


def load_declaration(path: Path) -> dict[str, object]:
    """Load one JSON object declaration from disk."""
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("declaration must be a JSON object")
    return payload


def load_isolated_preflight_report(
    path: Path,
    *,
    expected_declaration: Mapping[str, object] | None = None,
    root: Path | None = None,
    require_passed: bool = False,
) -> dict[str, object]:
    """Load and strictly validate one captured preflight report."""
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("preflight report must be a JSON object")
    return validate_isolated_preflight_report(
        payload,
        expected_declaration=expected_declaration,
        root=root,
        require_passed=require_passed,
    )


__all__ = [
    "PREFLIGHT_REPORT_FAILED",
    "PREFLIGHT_REPORT_PASSED",
    "PREFLIGHT_REPORT_VERSION",
    "capture_isolated_preflight_report",
    "isolated_preflight_issues",
    "isolated_preflight_template",
    "load_declaration",
    "load_isolated_preflight_report",
    "validate_isolated_preflight_report",
]
