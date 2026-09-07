"""Bounded video-stream admission shared by replenishment lanes.

The local Torrent lane probes a retained file before upload.  Cloud lanes
probe the exact AList staging object after the provider task completes.  Both
paths use the same command limits and result parser so an extension or a
manifest size can never stand in for an actual video stream.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import json
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any
import urllib.parse


FFPROBE_RW_TIMEOUT_US = 15_000_000
FFPROBE_PROBE_BYTES = 32 * 1024 * 1024
FFPROBE_ANALYZE_DURATION_US = 30_000_000
FFPROBE_WALL_TIMEOUT_SECONDS = 60
FFPROBE_MAX_OUTPUT_BYTES = 1024 * 1024


class VideoAdmissionError(RuntimeError):
    """A bounded probe could not prove that one object contains video."""

    def __init__(
        self,
        reason: str,
        *,
        candidate_invalid: bool = False,
        infrastructure: bool = False,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.candidate_invalid = candidate_invalid
        self.infrastructure = infrastructure


def _safe_ffprobe_headers(headers: Mapping[str, object]) -> str:
    output: list[str] = []
    for raw_name, raw_value in headers.items():
        name = str(raw_name).strip()
        value = str(raw_value).strip()
        if (
            re.fullmatch(r"[A-Za-z0-9-]+", name) is None
            or "\r" in value
            or "\n" in value
        ):
            raise VideoAdmissionError(
                "unsafe_provider_headers", infrastructure=True,
            )
        output.append(f"{name}: {value}\r\n")
    return "".join(output)


def _ffprobe_binary(explicit: str | None) -> str:
    value = explicit or shutil.which("ffprobe")
    if not isinstance(value, str) or not value:
        raise VideoAdmissionError("ffprobe_not_installed", infrastructure=True)
    return value


def _run_video_probe(
    source: str,
    *,
    headers: str = "",
    ffprobe_path: str | None = None,
    runner: Callable[..., Any] | None = None,
) -> dict[str, object]:
    """Run one bounded ffprobe and accept only an explicit video stream."""

    command = [
        _ffprobe_binary(ffprobe_path),
        # ffprobe does not expose ffmpeg's ``-nostdin`` option on the
        # Debian/Bookworm build used by the API image (it interprets the next
        # flag as the option value and exits non-zero).  We only ever pass a
        # concrete local path or a validated HTTP URL, so ffprobe has no
        # interactive stdin to consume; omitting the unsupported flag keeps
        # the bounded probe portable across supported ffprobe builds.
        "-v", "error",
        "-rw_timeout", str(FFPROBE_RW_TIMEOUT_US),
        "-probesize", str(FFPROBE_PROBE_BYTES),
        "-analyzeduration", str(FFPROBE_ANALYZE_DURATION_US),
    ]
    if headers:
        command.extend(["-headers", headers])
    command.extend([
        "-show_entries", "stream=codec_type:format=duration",
        "-of", "json",
        "-i", source,
    ])
    run = runner or subprocess.run
    try:
        completed = run(
            command,
            check=False,
            capture_output=True,
            text=False,
            timeout=FFPROBE_WALL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        # A wall-clock timeout under load is a window fault, not a payload
        # verdict: the same object probes fine on a quieter host.  Classify
        # it with the execution errors so the lane retries instead of
        # permanently excluding the candidate.
        raise VideoAdmissionError(
            "ffprobe_timeout", infrastructure=True,
        ) from exc
    except (OSError, TypeError, ValueError) as exc:
        raise VideoAdmissionError("ffprobe_execution_error", infrastructure=True) from exc
    if getattr(completed, "returncode", None) != 0:
        raise VideoAdmissionError("ffprobe_nonzero_exit", candidate_invalid=True)
    stdout = getattr(completed, "stdout", b"")
    if not isinstance(stdout, (str, bytes, bytearray)):
        raise VideoAdmissionError("invalid_ffprobe_output", candidate_invalid=True)
    if len(stdout) > FFPROBE_MAX_OUTPUT_BYTES:
        raise VideoAdmissionError("ffprobe_output_too_large", candidate_invalid=True)
    try:
        payload = json.loads(stdout)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise VideoAdmissionError("invalid_ffprobe_output", candidate_invalid=True) from exc
    streams = payload.get("streams") if isinstance(payload, Mapping) else None
    if not isinstance(streams, list) or not all(
        isinstance(row, Mapping) for row in streams
    ):
        raise VideoAdmissionError("invalid_ffprobe_output", candidate_invalid=True)
    video_streams = sum(
        1 for row in streams if str(row.get("codec_type") or "").casefold() == "video"
    )
    if video_streams < 1:
        raise VideoAdmissionError("video_stream_missing", candidate_invalid=True)
    duration_seconds: float | None = None
    raw_duration = (
        payload.get("format", {}).get("duration")
        if isinstance(payload.get("format"), Mapping)
        else None
    )
    if raw_duration is not None:
        try:
            duration_seconds = float(raw_duration)
        except (TypeError, ValueError):
            duration_seconds = None
    return {
        "status": "satisfied",
        "video_streams": video_streams,
        "duration_seconds": duration_seconds,
    }


def probe_local_video_stream(
    path: str | Path,
    *,
    ffprobe_path: str | None = None,
    runner: Callable[..., Any] | None = None,
) -> dict[str, object]:
    """Admit one exact local file without following a symlink."""

    candidate = Path(path)
    if candidate.is_symlink() or not candidate.is_file():
        raise VideoAdmissionError("local_video_not_regular", candidate_invalid=True)
    return _run_video_probe(
        str(candidate), ffprobe_path=ffprobe_path, runner=runner,
    )


def probe_remote_video_stream(
    client: object,
    path: str,
    *,
    ffprobe_path: str | None = None,
    runner: Callable[..., Any] | None = None,
) -> dict[str, object]:
    """Admit one exact task-staging object through a fresh AList file link."""

    link = getattr(client, "file_link", None)
    if not callable(link):
        raise VideoAdmissionError("alist_file_link_unavailable", infrastructure=True)
    try:
        try:
            raw_url, raw_headers = link(path, refresh=True)
        except TypeError:
            raw_url, raw_headers = link(path)
    except Exception as exc:
        raise VideoAdmissionError("alist_file_link_error", infrastructure=True) from exc
    if not isinstance(raw_url, str) or not isinstance(raw_headers, Mapping):
        raise VideoAdmissionError("invalid_alist_file_link", infrastructure=True)
    try:
        parsed = urllib.parse.urlsplit(raw_url)
        port = parsed.port
    except ValueError as exc:
        raise VideoAdmissionError("invalid_alist_file_link", infrastructure=True) from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or port is not None and not (1 <= port <= 65535)
        or any(char in raw_url for char in ("\r", "\n", "\x00"))
    ):
        raise VideoAdmissionError("invalid_alist_file_link", infrastructure=True)
    return _run_video_probe(
        raw_url,
        headers=_safe_ffprobe_headers(raw_headers),
        ffprobe_path=ffprobe_path,
        runner=runner,
    )


__all__ = [
    "FFPROBE_ANALYZE_DURATION_US",
    "FFPROBE_MAX_OUTPUT_BYTES",
    "FFPROBE_PROBE_BYTES",
    "FFPROBE_RW_TIMEOUT_US",
    "FFPROBE_WALL_TIMEOUT_SECONDS",
    "VideoAdmissionError",
    "probe_local_video_stream",
    "probe_remote_video_stream",
]
