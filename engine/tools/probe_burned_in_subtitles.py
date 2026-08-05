#!/usr/bin/env python3
"""Background-only, resumable burned-in Simplified Chinese subtitle probe."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import struct
import subprocess
import tempfile
from typing import Any, Mapping

from engine.scraper import AListClient
from engine.scrapeflow.burned_in_subtitle_ocr import (
    FRAMES_PER_WINDOW, OCR_POLICY_VERSION, classify_burned_in_ocr_windows,
    plan_window_offsets,
)
from engine.scrapeflow.clients.http import redact_sensitive_text
from engine.tools.refine_subtitle_audit import (
    _safe_ffprobe_headers, formal_library_category,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    temporary.replace(path)


def _load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "policy_version": OCR_POLICY_VERSION, "video_results": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or payload.get("policy_version") != OCR_POLICY_VERSION
        or not isinstance(payload.get("video_results"), dict)
    ):
        raise ValueError("硬字幕 OCR 缓存格式或策略版本无效")
    return payload


def _paused(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload.get("version") != 1 or payload.get("paused") is True
    except (OSError, ValueError, json.JSONDecodeError):
        return True


def _media_identity(client: AListClient, video_path: str) -> dict[str, Any]:
    parent = str(PurePosixPath(video_path).parent)
    name = PurePosixPath(video_path).name
    for row in client.list(parent, refresh=True):
        if str(row.get("name") or "") == name and not row.get("is_dir"):
            return {
                "size": row.get("size"),
                "modified": row.get("modified"),
                "hash_info": row.get("hash_info"),
            }
    return {"status": "identity_not_found"}


def _ffmpeg_executable() -> str | None:
    executable = shutil.which("ffmpeg")
    if executable is not None:
        return executable
    try:
        import imageio_ffmpeg
    except ImportError:
        return None
    candidate = imageio_ffmpeg.get_ffmpeg_exe()
    return candidate if candidate and Path(candidate).is_file() else None


def _duration_from_ffmpeg_stderr(stderr: str) -> float | None:
    match = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", stderr)
    if match is None:
        return None
    hours, minutes, seconds = match.groups()
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def _png_dimensions(path: Path) -> tuple[int, int]:
    header = path.read_bytes()[:24]
    if len(header) != 24 or header[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError("invalid_png_header")
    width, height = struct.unpack(">II", header[16:24])
    if width <= 0 or height <= 0:
        raise ValueError("invalid_png_dimensions")
    return width, height


def _probe_duration(
    raw_url: str, headers: Mapping[str, Any], *, timeout: int,
) -> tuple[float | None, dict[str, Any] | None]:
    ffprobe = shutil.which("ffprobe")
    safe_headers = _safe_ffprobe_headers(headers)
    if safe_headers is None:
        return None, {"status": "pending", "reason": "unsafe_provider_headers"}
    if ffprobe is None:
        ffmpeg = _ffmpeg_executable()
        if ffmpeg is None:
            return None, {"status": "pending", "reason": "ffmpeg_not_installed"}
        command = [ffmpeg, "-hide_banner", "-rw_timeout", "15000000"]
        if safe_headers:
            command.extend(["-headers", safe_headers])
        command.extend(["-i", raw_url, "-t", "0", "-f", "null", "-"])
        try:
            completed = subprocess.run(
                command, capture_output=True, text=True, check=False, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return None, {"status": "pending", "reason": "ffmpeg_duration_timeout"}
        duration = _duration_from_ffmpeg_stderr(completed.stderr)
        if duration is None:
            return None, {
                "status": "pending", "reason": "duration_unavailable",
                "stderr": redact_sensitive_text(
                    completed.stderr[-2000:],
                    secrets=[raw_url, *(str(value) for value in headers.values())],
                ),
            }
        return duration, None
    command = [ffprobe, "-v", "error", "-rw_timeout", "15000000"]
    if safe_headers:
        command.extend(["-headers", safe_headers])
    command.extend(["-show_entries", "format=duration", "-of", "json", raw_url])
    try:
        completed = subprocess.run(
            command, capture_output=True, text=True, check=False, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return None, {"status": "pending", "reason": "ffprobe_timeout"}
    if completed.returncode != 0:
        return None, {
            "status": "pending", "reason": "ffprobe_nonzero_exit",
            "stderr": redact_sensitive_text(
                completed.stderr[-2000:],
                secrets=[raw_url, *(str(value) for value in headers.values())],
            ),
        }
    try:
        duration = float(json.loads(completed.stdout)["format"]["duration"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None, {"status": "pending", "reason": "duration_unavailable"}
    return duration, None


def _extract_window(
    raw_url: str, headers: Mapping[str, Any], offset: float, directory: Path,
    *, timeout: int,
) -> tuple[list[Path], dict[str, Any] | None]:
    ffmpeg = _ffmpeg_executable()
    if ffmpeg is None:
        return [], {"status": "pending", "reason": "ffmpeg_not_installed"}
    safe_headers = _safe_ffprobe_headers(headers)
    if safe_headers is None:
        return [], {"status": "pending", "reason": "unsafe_provider_headers"}
    pattern = directory / "frame-%02d.png"
    command = [ffmpeg, "-v", "error", "-rw_timeout", "15000000"]
    if safe_headers:
        command.extend(["-headers", safe_headers])
    command.extend([
        "-ss", str(offset), "-i", raw_url, "-t", "8", "-an", "-sn",
        "-vf", "fps=1,scale='min(1920,iw)':-2", "-frames:v",
        str(FRAMES_PER_WINDOW), str(pattern),
    ])
    try:
        completed = subprocess.run(
            command, capture_output=True, check=False, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return [], {"status": "pending", "reason": "frame_extraction_timeout"}
    if completed.returncode != 0:
        return [], {
            "status": "pending", "reason": "frame_extraction_nonzero_exit",
            "stderr": redact_sensitive_text(
                completed.stderr[-2000:].decode("utf-8", errors="replace"),
                secrets=[raw_url, *(str(value) for value in headers.values())],
            ),
        }
    frames = sorted(directory.glob("frame-*.png"))
    if len(frames) < FRAMES_PER_WINDOW:
        return frames, {"status": "pending", "reason": "insufficient_extracted_frames"}
    return frames[:FRAMES_PER_WINDOW], None


def _rapidocr_lines(engine: Any, frame: Path) -> tuple[int, int, list[dict[str, Any]]]:
    result = engine(str(frame))
    image = None if result is None else result.img
    if image is None:
        width, height = _png_dimensions(frame)
    else:
        height, width = int(image.shape[0]), int(image.shape[1])
    if result is None:
        return width, height, []
    boxes = [] if result.boxes is None else result.boxes
    texts = [] if result.txts is None else result.txts
    scores = [] if result.scores is None else result.scores
    lines = []
    for box, text, score in zip(boxes, texts, scores):
        xs = [float(point[0]) for point in box]
        ys = [float(point[1]) for point in box]
        left, top = min(xs), min(ys)
        lines.append({
            "text": str(text)[:120],
            "confidence": round(float(score) * 100, 2),
            "left": round(left), "top": round(top),
            "width": round(max(xs) - left), "height": round(max(ys) - top),
        })
    return width, height, lines[:64]


def _probe_video(
    client: AListClient, engine: Any, video_path: str, *, timeout: int,
    evidence_root: Path | None = None,
) -> dict[str, Any]:
    identity = _media_identity(client, video_path)
    try:
        raw_url, headers = client.file_link(video_path, refresh=True)
    except Exception as exc:
        return {
            "status": "pending", "reason": type(exc).__name__,
            "media_identity": identity,
        }
    duration, duration_error = _probe_duration(raw_url, headers, timeout=timeout)
    if duration_error is not None:
        return {**duration_error, "media_identity": identity}
    assert duration is not None
    offsets, plan_error = plan_window_offsets(duration)
    if plan_error is not None:
        return {
            "status": "pending", "reason": plan_error,
            "duration_seconds": duration, "planned_offsets": offsets,
            "media_identity": identity,
        }
    windows = []
    evidence_directory = None
    if evidence_root is not None:
        evidence_directory = evidence_root / hashlib.sha256(video_path.encode("utf-8")).hexdigest()[:16]
        evidence_directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="scrapeflow-ocr-") as temporary:
        root = Path(temporary)
        for window_index, offset in enumerate(offsets):
            directory = root / f"window-{window_index}"
            directory.mkdir()
            frames, error = _extract_window(
                raw_url, headers, offset, directory, timeout=timeout,
            )
            frame_rows = []
            for frame_index, frame in enumerate(frames):
                retained_path = None
                if evidence_directory is not None:
                    retained_path = evidence_directory / f"window-{window_index}-frame-{frame_index}.png"
                    shutil.copy2(frame, retained_path)
                try:
                    width, height, lines = _rapidocr_lines(engine, frame)
                    frame_rows.append({
                        "status": "ocr_success", "width": width, "height": height,
                        "frame_sha256": hashlib.sha256(frame.read_bytes()).hexdigest(),
                        "evidence_path": str(retained_path) if retained_path else None,
                        "ocr_lines": lines,
                    })
                except Exception as exc:
                    frame_rows.append({"status": "ocr_failed", "reason": type(exc).__name__})
            if error is not None:
                frame_rows.append(error)
            windows.append({"offset_seconds": offset, "frames": frame_rows})
    decision = classify_burned_in_ocr_windows(windows)
    return {
        **decision,
        "duration_seconds": duration,
        "media_identity": identity,
        "probed_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--pause-file", type=Path, required=True)
    parser.add_argument("--max-videos", type=int, default=25)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--title-contains")
    parser.add_argument("--retry-pending", action="store_true")
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    if not 1 <= args.max_videos <= 500 or not 10 <= args.timeout <= 120:
        raise ValueError("OCR 批次或超时参数超出安全范围")
    try:
        from rapidocr import RapidOCR
    except ImportError as exc:
        raise SystemExit("rapidocr/onnxruntime 未安装在隔离 OCR 环境中") from exc

    source_bytes = args.input.read_bytes()
    source = json.loads(source_bytes)
    candidates = {
        str(row.get("video_path") or "")
        for bucket in ("confirmed_missing_chinese", "pending_review_or_probe")
        for row in source.get(bucket, []) if isinstance(row, Mapping)
        and formal_library_category(str(row.get("video_path") or "")) is not None
    }
    if args.title_contains:
        candidates = {path for path in candidates if args.title_contains in path}
    cache = _load_cache(args.cache)
    if args.retry_pending:
        for path, result in list(cache["video_results"].items()):
            if isinstance(result, Mapping) and result.get("status") == "pending":
                del cache["video_results"][path]
    client = AListClient(
        os.environ.get("ALIST_URL", "http://127.0.0.1:5244"),
        os.environ.get("ALIST_USERNAME", ""), os.environ.get("ALIST_PASSWORD", ""),
        allow_insecure_http=True,
    )
    client.login()
    engine = RapidOCR()
    processed = 0
    for video_path in sorted(candidates):
        if _paused(args.pause_file):
            break
        if video_path in cache["video_results"]:
            continue
        cache["video_results"][video_path] = _probe_video(
            client, engine, video_path, timeout=args.timeout,
            evidence_root=args.evidence_dir,
        )
        processed += 1
        _write_json(args.cache, cache)
        if processed >= args.max_videos:
            break

    statuses = Counter(
        str(result.get("status"))
        for path, result in cache["video_results"].items()
        if path in candidates and isinstance(result, Mapping)
    )
    payload = {
        "schema_version": 1,
        "audited_at": datetime.now(timezone.utc).isoformat(),
        "source_audit": str(args.input),
        "source_audit_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "policy": {
            "required_language": "zh-CN", "ocr_policy_version": OCR_POLICY_VERSION,
            "ocr_backend": "rapidocr_onnxruntime", "remote_mutations": False,
            "ui_involvement": "none", "cache_checkpoint": "per_video",
        },
        "summary": {
            "eligible_videos": len(candidates), "cached_videos": sum(statuses.values()),
            "remaining_videos": len(candidates) - sum(statuses.values()),
            "status_counts": dict(statuses), "processed_this_run": processed,
            "paused": _paused(args.pause_file),
        },
        "video_results": {
            path: cache["video_results"][path]
            for path in sorted(candidates) if path in cache["video_results"]
        },
    }
    _write_json(args.output, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
