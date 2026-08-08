"""Fail-closed, local-only content identity witnesses.

This module contains one deliberately narrow escape hatch for a legacy library
layout that has a movie copy below a TV ``tvshow.nfo`` boundary.  The witness
is not a path/name mapping: the immutable source allow-list fixes the one
permitted path and TMDB identity, while the local state file records the exact
provider version and media evidence that must be re-proved on every audit.

No function in this module writes to AList, creates an NFO, or moves media.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import posixpath
import shutil
import subprocess
from pathlib import Path, PurePosixPath
from typing import Mapping, Sequence


CONTENT_IDENTITY_OVERRIDE_SCHEMA_VERSION = 1
CONTENT_IDENTITY_OVERRIDE_KIND = "content_identity_overrides"
CONTENT_IDENTITY_OVERRIDE_FILENAME = "content-identity-overrides.json"
CONTENT_IDENTITY_OVERRIDE_ID = "fate-strange-fake-whispers-of-dawn-child-v1"

# This is intentionally a source-level allow-list.  A hand-edited state file
# can enable or disable this exact witness, but cannot redirect it to another
# path, title, or TMDB identity.
FATE_CHILD_VIDEO_PATH = (
    "/quark/影视/番剧/Fate/命运／奇异赝品/"
    "命运／奇异赝品 黎明低语 (2023).mkv"
)
FATE_PARENT_VIDEO_PATH = "/quark/影视/番剧/Fate/命运／奇异赝品 黎明低语 (2023).mkv"
FATE_PARENT_NFO_PATH = "/quark/影视/番剧/Fate/命运／奇异赝品 黎明低语 (2023).nfo"
FATE_MOVIE_TARGET_ROOT = "/quark/影视/番剧/Fate"
FATE_MOVIE_TMDB_ID = 1145612
FATE_MOVIE_TITLE = "命运／奇异赝品 黎明低语"
FATE_MOVIE_YEAR = "2023"
FATE_IDENTITY_VERSION = "fate-strange-fake-movie-v1"

_CONTENT_PROBE_BYTES = 8 * 1024 * 1024
_CONTENT_PROBE_TIMEOUT_SECONDS = 30
_VIDEO_SUFFIXES = frozenset({
    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm", ".wmv", ".iso",
})
# The witness was recorded from the exact child object.  Keeping this compact
# and explicit makes a metadata-only collision fail closed: every selected
# container, stream, tag, chapter and prefix field must match.
FATE_CHILD_MEDIA_EVIDENCE: dict[str, object] = {
    "probe_bytes": _CONTENT_PROBE_BYTES,
    "prefix_sha256": "b15933bc32918eda91fee86e711d82a9be541057bb4d2539d43c7f31cd54cce0",
    "format": {
        "format_name": "matroska,webm",
        "duration_ms": 3354075,
        "title": "Fate strange Fake -Whispers of Dawn-",
        "encoder": "libebml v1.4.5 + libmatroska v1.7.1",
        "creation_time": "2026-01-05T14:02:29.000000Z",
    },
    "video": {
        "codec_name": "h264",
        "codec_type": "video",
        "profile": "High",
        "width": 1920,
        "height": 1080,
        "pix_fmt": "yuv420p",
        "r_frame_rate": "24000/1001",
        "title": "ADWeb",
    },
    "audio": {
        "codec_name": "aac",
        "codec_type": "audio",
        "profile": "LC",
        "channels": 2,
        "channel_layout": "stereo",
        "sample_rate": "44100",
        "language": "jpn",
        "title": "Japanese",
    },
    "subtitles": [
        {
            "codec_name": "hdmv_pgs_subtitle",
            "codec_type": "subtitle",
            "language": "chi",
            "title": "chs[NEST]",
        },
        {
            "codec_name": "hdmv_pgs_subtitle",
            "codec_type": "subtitle",
            "language": "chi",
            "title": "cht[NEST]",
        },
    ],
    "chapters": 0,
}


def content_identity_override_path(state_root: str | Path | None) -> Path | None:
    """Return the local state path, or ``None`` for an unconfigured root."""
    if state_root is None:
        return None
    return Path(state_root) / "library-audit" / CONTENT_IDENTITY_OVERRIDE_FILENAME


def _canonical_path(value: object) -> str | None:
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or "\\" in value
        or "\x00" in value
    ):
        return None
    normalized = posixpath.normpath(value)
    if normalized != value or any(part in {"", ".", ".."} for part in normalized.split("/")[1:]):
        return None
    return normalized


def _safe_version(value: object) -> str | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    if not isinstance(value, (str, int, float)):
        return None
    text = str(value)
    if not text or len(text) > 512 or any(char in text for char in "\x00\r\n"):
        return None
    return text


def _video_path(value: object) -> str | None:
    path = _canonical_path(value)
    if path is None or PurePosixPath(path).suffix.casefold() not in _VIDEO_SUFFIXES:
        return None
    return path


def _exact_json(value: object) -> object:
    """Copy JSON-shaped data while rejecting non-JSON values."""
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _expected_record() -> dict[str, object]:
    return {
        "schema_version": CONTENT_IDENTITY_OVERRIDE_SCHEMA_VERSION,
        "kind": "content_identity_override",
        "id": CONTENT_IDENTITY_OVERRIDE_ID,
        "identity_version": FATE_IDENTITY_VERSION,
        "enabled": True,
        "path": FATE_CHILD_VIDEO_PATH,
        "target_root": FATE_MOVIE_TARGET_ROOT,
        "media_type": "movie",
        "tmdb_id": FATE_MOVIE_TMDB_ID,
        "title": FATE_MOVIE_TITLE,
        "year": FATE_MOVIE_YEAR,
        "size": 3419664950,
        "version": "2026-07-26T13:27:48.068Z",
        "evidence": copy.deepcopy(FATE_CHILD_MEDIA_EVIDENCE),
        "parent_video_path": FATE_PARENT_VIDEO_PATH,
        "parent_nfo_path": FATE_PARENT_NFO_PATH,
    }


def _valid_record(raw: object) -> dict[str, object] | None:
    """Validate one state row against the immutable witness allow-list."""
    if not isinstance(raw, Mapping):
        return None
    expected = _expected_record()
    if set(raw) != set(expected):
        return None
    # All identity/coordinate fields are immutable.  ``evidence`` is checked
    # separately below to avoid accepting a hand-edited partial mapping.
    for key in (
        "schema_version", "kind", "id", "identity_version", "enabled", "path",
        "target_root", "media_type", "tmdb_id", "title", "year", "size", "version",
        "parent_video_path", "parent_nfo_path",
    ):
        if raw.get(key) != expected[key]:
            return None
    evidence = _exact_json(raw.get("evidence"))
    if evidence != expected["evidence"]:
        return None
    return copy.deepcopy(expected)


def load_content_identity_overrides(state_root: str | Path | None) -> list[dict[str, object]]:
    """Load only the exact allow-listed witness from local state.

    Any missing, malformed, duplicated, disabled, or hand-edited state is
    treated as no witness.  This is intentionally stricter than a normal
    configuration parser because a false positive would hide a TV-boundary
    identity error from the completion gate.
    """
    path = content_identity_override_path(state_root)
    if path is None:
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    if not isinstance(raw, Mapping):
        return []
    if (
        raw.get("schema_version") != CONTENT_IDENTITY_OVERRIDE_SCHEMA_VERSION
        or raw.get("kind") != CONTENT_IDENTITY_OVERRIDE_KIND
    ):
        return []
    records = raw.get("records")
    if not isinstance(records, list) or len(records) != 1:
        return []
    record = _valid_record(records[0])
    return [record] if record is not None else []


def _tag_value(stream: Mapping[str, object], key: str) -> str | None:
    tags = stream.get("tags")
    if not isinstance(tags, Mapping):
        return None
    value = tags.get(key)
    if value is None:
        value = tags.get(key.upper())
    return str(value) if value is not None else None


def _normalise_probe_payload(payload: object) -> dict[str, object] | None:
    """Reduce bounded ffprobe JSON to the witness fields."""
    if not isinstance(payload, Mapping):
        return None
    streams = payload.get("streams")
    if not isinstance(streams, list) or not all(isinstance(row, Mapping) for row in streams):
        return None
    format_row = payload.get("format")
    if not isinstance(format_row, Mapping):
        return None
    duration = format_row.get("duration")
    try:
        duration_value = float(duration)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(duration_value) or duration_value <= 0:
        return None
    video = [row for row in streams if row.get("codec_type") == "video"]
    audio = [row for row in streams if row.get("codec_type") == "audio"]
    subtitles = [row for row in streams if row.get("codec_type") == "subtitle"]
    # Attachments/data streams would make the media object materially
    # different from the reviewed witness; reject them rather than ignoring.
    if len(video) != 1 or len(audio) != 1 or len(subtitles) != 2:
        return None
    if any(row.get("codec_type") not in {"video", "audio", "subtitle"} for row in streams):
        return None

    def selected(row: Mapping[str, object], keys: Sequence[str]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key in keys:
            if key in row and row[key] is not None:
                result[key] = str(row[key]) if key in {"codec_name", "codec_type", "profile", "pix_fmt", "r_frame_rate", "language", "title"} else row[key]
            elif key in {"language", "title"}:
                tag = _tag_value(row, key)
                if tag is not None:
                    result[key] = tag
        return result

    video_row = selected(video[0], (
        "codec_name", "codec_type", "profile", "width", "height", "pix_fmt", "r_frame_rate", "title",
    ))
    audio_row = selected(audio[0], (
        "codec_name", "codec_type", "profile", "channels", "channel_layout", "sample_rate", "language", "title",
    ))
    subtitle_rows = [
        selected(row, ("codec_name", "codec_type", "language", "title"))
        for row in subtitles
    ]
    format_tags = format_row.get("tags")
    if not isinstance(format_tags, Mapping):
        format_tags = {}
    title = format_tags.get("title")
    encoder = format_tags.get("encoder")
    creation_time = format_tags.get("creation_time")
    if title is None or encoder is None or creation_time is None:
        return None
    chapters = payload.get("chapters")
    if not isinstance(chapters, list):
        return None
    return {
        "probe_bytes": _CONTENT_PROBE_BYTES,
        "format": {
            "format_name": format_row.get("format_name"),
            "duration_ms": int(round(duration_value * 1000)),
            "title": str(title),
            "encoder": str(encoder),
            "creation_time": str(creation_time),
        },
        "video": video_row,
        "audio": audio_row,
        "subtitles": subtitle_rows,
        "chapters": len(chapters),
    }


def probe_content_identity(client: object, record: Mapping[str, object]) -> tuple[bool, str]:
    """Re-probe the bounded media witness without exposing a signed URL."""
    reader = getattr(client, "read_file_prefix", None)
    if not callable(reader):
        return False, "prefix_reader_unavailable"
    evidence = record.get("evidence")
    if not isinstance(evidence, Mapping):
        return False, "evidence_missing"
    probe_bytes = evidence.get("probe_bytes")
    if probe_bytes != _CONTENT_PROBE_BYTES:
        return False, "probe_size_mismatch"
    try:
        prefix = reader(str(record["path"]), max_bytes=_CONTENT_PROBE_BYTES)
    except Exception:
        return False, "prefix_read_failed"
    if not isinstance(prefix, bytes) or len(prefix) != _CONTENT_PROBE_BYTES:
        return False, "prefix_length_mismatch"
    digest = hashlib.sha256(prefix).hexdigest()
    if digest != evidence.get("prefix_sha256"):
        return False, "prefix_digest_mismatch"
    ffprobe = shutil.which("ffprobe")
    if ffprobe is None:
        return False, "ffprobe_unavailable"
    command = [
        ffprobe, "-v", "error", "-probesize", str(_CONTENT_PROBE_BYTES),
        "-analyzeduration", str(_CONTENT_PROBE_BYTES),
        "-show_entries",
        "format=format_name,duration:format_tags=title,encoder,creation_time",
        "-show_entries",
        "stream=codec_type,codec_name,profile,width,height,pix_fmt,r_frame_rate,channels,channel_layout,sample_rate:stream_tags=language,title",
        "-show_chapters", "-of", "json", "-i", "pipe:0",
    ]
    try:
        completed = subprocess.run(
            command,
            input=prefix,
            capture_output=True,
            timeout=_CONTENT_PROBE_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False, "ffprobe_failed"
    if completed.returncode != 0 or len(completed.stdout) > 2 * 1024 * 1024:
        return False, "ffprobe_failed"
    try:
        payload = json.loads(completed.stdout)
    except (TypeError, ValueError, json.JSONDecodeError):
        return False, "ffprobe_invalid_json"
    observed = _normalise_probe_payload(payload)
    if observed is None:
        return False, "ffprobe_evidence_incomplete"
    expected = {
        key: evidence.get(key)
        for key in ("probe_bytes", "format", "video", "audio", "subtitles", "chapters")
    }
    if observed != expected:
        return False, "ffprobe_evidence_mismatch"
    return True, "matched"


def _metadata(work: Mapping[str, object]) -> Mapping[str, object]:
    value = work.get("metadata")
    return value if isinstance(value, Mapping) else work


def _identity_sources(work: Mapping[str, object]) -> set[str]:
    raw = work.get("identity_sources")
    if isinstance(raw, (list, tuple, set, frozenset)):
        return {str(value) for value in raw}
    source = work.get("identity_source")
    return {str(source)} if isinstance(source, str) else set()


def _augment_parent_movie_scope(work: Mapping[str, object]) -> dict[str, object] | None:
    metadata = _metadata(work)
    if (
        metadata.get("tmdb_id") != FATE_MOVIE_TMDB_ID
        or _canonical_path(metadata.get("target_root") or metadata.get("series_root")) != FATE_MOVIE_TARGET_ROOT
        or str(metadata.get("media_type") or metadata.get("type") or "").casefold() != "movie"
        or metadata.get("title") != FATE_MOVIE_TITLE
        or metadata.get("year") != FATE_MOVIE_YEAR
        or "library_nfo" not in _identity_sources(work)
        or _canonical_path(metadata.get("nfo_path")) != FATE_PARENT_NFO_PATH
    ):
        return None
    raw_scope = metadata.get("identity_scope")
    if not isinstance(raw_scope, Mapping):
        return None
    kind = str(raw_scope.get("kind") or "").casefold()
    if kind == "video_file":
        paths = [_video_path(raw_scope.get("video_path"))]
    elif kind == "video_files":
        raw_paths = raw_scope.get("video_paths")
        paths = [
            _video_path(value)
            for value in raw_paths
        ] if isinstance(raw_paths, (list, tuple, set, frozenset)) else []
    else:
        return None
    if (
        any(path is None for path in paths)
        or set(paths) != {FATE_PARENT_VIDEO_PATH}
    ):
        return None
    clean_paths = sorted({str(path) for path in paths if path is not None} | {FATE_CHILD_VIDEO_PATH}, key=str.casefold)
    updated = copy.deepcopy(dict(work))
    if isinstance(updated.get("metadata"), Mapping):
        nested = copy.deepcopy(dict(updated["metadata"]))
        nested["identity_scope"] = {"kind": "video_files", "video_paths": clean_paths}
        nested["content_identity_override_ids"] = [CONTENT_IDENTITY_OVERRIDE_ID]
        updated["metadata"] = nested
    else:
        updated["identity_scope"] = {"kind": "video_files", "video_paths": clean_paths}
        updated["content_identity_override_ids"] = [CONTENT_IDENTITY_OVERRIDE_ID]
    sources = sorted(_identity_sources(work) | {"content_evidence_override"})
    updated["identity_sources"] = sources
    updated["content_identity_overrides"] = [{
        "id": CONTENT_IDENTITY_OVERRIDE_ID,
        "identity_version": FATE_IDENTITY_VERSION,
        "path": FATE_CHILD_VIDEO_PATH,
        "target_root": FATE_MOVIE_TARGET_ROOT,
        "tmdb_id": FATE_MOVIE_TMDB_ID,
        "size": 3419664950,
        "version": "2026-07-26T13:27:48.068Z",
    }]
    return updated


def apply_content_identity_overrides(
    report: Mapping[str, object],
    works: Sequence[Mapping[str, object]],
    client: object | None,
    state_root: str | Path | None,
    *,
    probe: object | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Apply exact file-scope witnesses after structural inventory.

    The returned diagnostic is report metadata only.  A witness is applied
    only when the current inventory coordinate and complete media probe match;
    any mismatch leaves the original TV-boundary unknown untouched.
    """
    records = load_content_identity_overrides(state_root)
    diagnostics: dict[str, object] = {"applied": [], "rejected": []}
    if not records or not isinstance(report, Mapping) or report.get("complete") is not True:
        return [copy.deepcopy(dict(work)) for work in works if isinstance(work, Mapping)], diagnostics
    inventory = report.get("inventory")
    rows = {
        _canonical_path(row.get("path")): row
        for row in inventory
        if isinstance(row, Mapping)
        and row.get("type") == "file"
        and _canonical_path(row.get("path")) is not None
    } if isinstance(inventory, list) else {}
    updated = [copy.deepcopy(dict(work)) for work in works if isinstance(work, Mapping)]
    for record in records:
        path = str(record["path"])
        row = rows.get(path)
        reason: str | None = None
        if not isinstance(row, Mapping) or _video_path(path) is None:
            reason = "inventory_path_missing"
        elif row.get("size") != record.get("size") or row.get("version") != record.get("version"):
            reason = "inventory_identity_mismatch"
        else:
            checker = probe if callable(probe) else probe_content_identity
            try:
                result = checker(client, record)
            except Exception:
                result = (False, "probe_exception")
            if isinstance(result, tuple):
                matched = result[0] is True
                reason = None if matched else str(result[1] or "probe_rejected")
            else:
                matched = result is True
                reason = None if matched else "probe_rejected"
        if reason is not None:
            diagnostics["rejected"].append({"id": record["id"], "path": path, "reason": reason})
            continue
        match_index = next(
            (
                index for index, work in enumerate(updated)
                if _augment_parent_movie_scope(work) is not None
            ),
            None,
        )
        if match_index is None:
            diagnostics["rejected"].append({"id": record["id"], "path": path, "reason": "parent_movie_identity_missing"})
            continue
        updated[match_index] = _augment_parent_movie_scope(updated[match_index]) or updated[match_index]
        diagnostics["applied"].append({
            "id": record["id"],
            "identity_version": record["identity_version"],
            "path": path,
            "target_root": record["target_root"],
            "tmdb_id": record["tmdb_id"],
            "size": record["size"],
            "version": record["version"],
        })
    return updated, diagnostics


__all__ = [
    "CONTENT_IDENTITY_OVERRIDE_FILENAME",
    "CONTENT_IDENTITY_OVERRIDE_ID",
    "FATE_CHILD_VIDEO_PATH",
    "FATE_PARENT_VIDEO_PATH",
    "FATE_MOVIE_TMDB_ID",
    "FATE_CHILD_MEDIA_EVIDENCE",
    "apply_content_identity_overrides",
    "content_identity_override_path",
    "load_content_identity_overrides",
    "probe_content_identity",
]
