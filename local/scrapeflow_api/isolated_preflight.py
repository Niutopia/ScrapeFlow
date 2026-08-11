"""Static preflight checks for a real isolated acceptance declaration."""

from __future__ import annotations

import json
import os
import posixpath
import tempfile
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
) -> list[str]:
    """Return issues that block a real isolated acceptance run."""
    repo_root = (project_root() if root is None else Path(root)).resolve(strict=False)
    issues: list[str] = []
    for key in ("api_url", "alist_url"):
        issue = _loopback_url_issue(declaration, key)
        if issue:
            issues.append(issue)

    state_dir, state_issue = _isolated_directory_issue(
        declaration, "scrapeflow_state_dir", repo_root=repo_root,
    )
    alist_dir, alist_issue = _isolated_directory_issue(
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


def load_declaration(path: Path) -> dict[str, object]:
    """Load one JSON object declaration from disk."""
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("declaration must be a JSON object")
    return payload


__all__ = [
    "isolated_preflight_issues",
    "isolated_preflight_template",
    "load_declaration",
]
