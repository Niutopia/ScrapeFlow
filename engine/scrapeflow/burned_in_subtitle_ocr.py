"""Pure evidence classification for burned-in Simplified Chinese subtitles.

Frame extraction and OCR are intentionally kept outside this module.  The
classifier accepts bounded Tesseract TSV evidence so it can be tested without
network or media access and replayed deterministically by unattended cycles.
"""

from __future__ import annotations

from difflib import SequenceMatcher
import re
from typing import Any, Mapping

from engine.tools.refine_subtitle_audit import CHINESE_MARKERS, TRADITIONAL_MARKERS


OCR_POLICY_VERSION = 4
MIN_WORD_CONFIDENCE = 70.0
MIN_HAN_PER_LINE = 4
MIN_SUCCESSFUL_WINDOWS_FOR_NEGATIVE = 6
MIN_SUCCESSFUL_FRAMES_FOR_NEGATIVE = 36
FRAMES_PER_WINDOW = 6
MIN_TEXT_WINDOWS_FOR_NEGATIVE = 3
MIN_TEXT_FRAMES_FOR_NEGATIVE = 3
WINDOW_FRACTIONS = (0.20, 0.32, 0.44, 0.56, 0.68, 0.80)
MIN_WINDOW_OFFSET_SECONDS = 240.0
MIN_WINDOW_SEPARATION_SECONDS = 120.0
TAIL_EXCLUSION_SECONDS = 120.0


def apply_ocr_resolution(
    refined: Mapping[str, Any], evidence: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Resolve only current paths with high-confidence current-policy OCR.

    Negative, pending, stale, malformed, and out-of-scope evidence is ignored.
    This keeps OCR fail-closed: it can prove that Simplified Chinese is already
    burned into a video, but can never manufacture a missing-subtitle verdict.
    """
    source = dict(refined)
    raw_evidence = evidence if isinstance(evidence, Mapping) else {}
    resolved = [
        dict(row) for row in source.get("resolved_with_chinese", [])
        if isinstance(row, Mapping)
    ]
    output: dict[str, Any] = dict(source)
    for bucket in ("confirmed_missing_chinese", "pending_review_or_probe"):
        retained: list[dict[str, Any]] = []
        raw_rows = source.get(bucket, [])
        if not isinstance(raw_rows, list):
            raw_rows = []
        for raw_row in raw_rows:
            if not isinstance(raw_row, Mapping):
                continue
            row = dict(raw_row)
            video_path = str(row.get("video_path") or "")
            witness = raw_evidence.get(video_path)
            current_positive = (
                isinstance(witness, Mapping)
                and witness.get("status") == "burned_in_chinese_confirmed"
                and witness.get("policy_version") == OCR_POLICY_VERSION
            )
            if not current_positive:
                retained.append(row)
                continue
            resolved.append({
                **row,
                "resolution": "burned_in_simplified_chinese_ocr_confirmed",
                "burned_in_ocr_evidence": dict(witness),
            })
        output[bucket] = retained
    output["resolved_with_chinese"] = resolved
    return output


def _normalized_text(value: str) -> str:
    return "".join(re.findall(r"[\u3400-\u9fffA-Za-z0-9]", value)).casefold()


def plan_window_offsets(duration_seconds: float) -> tuple[list[float], str | None]:
    """Plan six separated body windows while excluding common OP/ED regions."""
    if duration_seconds <= MIN_WINDOW_OFFSET_SECONDS + TAIL_EXCLUSION_SECONDS:
        return [], "video_too_short"
    latest = duration_seconds - TAIL_EXCLUSION_SECONDS
    offsets = []
    for fraction in WINDOW_FRACTIONS:
        offset = max(MIN_WINDOW_OFFSET_SECONDS, duration_seconds * fraction)
        offset = min(offset, latest)
        if offsets and offset - offsets[-1] < MIN_WINDOW_SEPARATION_SECONDS:
            continue
        offsets.append(round(offset, 3))
    if len(offsets) < MIN_SUCCESSFUL_WINDOWS_FOR_NEGATIVE:
        return offsets, "insufficient_independent_windows"
    return offsets, None


def classify_ocr_line_records(
    lines: list[dict[str, Any]], *, frame_width: int, frame_height: int,
) -> list[dict[str, Any]]:
    """Return high-confidence Chinese-looking OCR lines in the subtitle band."""
    if frame_width <= 0 or frame_height <= 0:
        return []
    candidates = []
    for line in lines:
        try:
            text = str(line.get("text") or "").strip()
            confidence = float(line.get("confidence") or 0)
            left = int(line.get("left") or 0)
            top = int(line.get("top") or 0)
            width = int(line.get("width") or 0)
            height = int(line.get("height") or 0)
        except (TypeError, ValueError):
            continue
        if confidence < MIN_WORD_CONFIDENCE or not text:
            continue
        normalized = _normalized_text(text)
        han = re.findall(r"[\u3400-\u9fff]", normalized)
        markers = sum(character in CHINESE_MARKERS for character in han)
        traditional_markers = sum(character in TRADITIONAL_MARKERS for character in han)
        vertical_center = (top + height / 2) / frame_height
        horizontal_center = (left + width / 2) / frame_width
        width_ratio = width / frame_width
        if (
            len(han) < MIN_HAN_PER_LINE
            or markers < 1
            or markers < traditional_markers
            or not 0.56 <= vertical_center <= 0.94
            or not 0.12 <= horizontal_center <= 0.88
            or not 0.05 <= width_ratio <= 0.92
        ):
            continue
        candidates.append({
            "text": text[:120],
            "normalized": normalized[:120],
            "han_characters": len(han),
            "simplified_markers": markers,
            "traditional_markers": traditional_markers,
            "mean_confidence": round(confidence, 2),
            "vertical_center": round(vertical_center, 4),
            "horizontal_center": round(horizontal_center, 4),
            "width_ratio": round(width_ratio, 4),
        })
    return candidates


def _line_candidates(
    tsv: str, *, frame_width: int, frame_height: int,
) -> list[dict[str, Any]]:
    """Adapt Tesseract TSV to the engine-neutral OCR line classifier."""
    if frame_width <= 0 or frame_height <= 0:
        return []
    lines: dict[tuple[int, int, int], list[dict[str, Any]]] = {}
    rows = tsv.splitlines()
    if not rows:
        return []
    columns = rows[0].split("\t")
    required = {"block_num", "par_num", "line_num", "left", "top", "width", "height", "conf", "text"}
    if not required.issubset(columns):
        return []
    positions = {name: columns.index(name) for name in required}
    for raw in rows[1:]:
        fields = raw.split("\t")
        if len(fields) < len(columns):
            continue
        try:
            confidence = float(fields[positions["conf"]])
            left = int(fields[positions["left"]])
            top = int(fields[positions["top"]])
            width = int(fields[positions["width"]])
            height = int(fields[positions["height"]])
            key = tuple(int(fields[positions[name]]) for name in ("block_num", "par_num", "line_num"))
        except (TypeError, ValueError):
            continue
        text = fields[positions["text"]].strip()
        if confidence < MIN_WORD_CONFIDENCE or not text:
            continue
        lines.setdefault(key, []).append({
            "text": text, "confidence": confidence,
            "left": left, "top": top, "width": width, "height": height,
        })

    records = []
    for words in lines.values():
        text = "".join(word["text"] for word in words)
        left = min(word["left"] for word in words)
        top = min(word["top"] for word in words)
        right = max(word["left"] + word["width"] for word in words)
        bottom = max(word["top"] + word["height"] for word in words)
        mean_confidence = sum(word["confidence"] for word in words) / len(words)
        records.append({
            "text": text,
            "confidence": mean_confidence,
            "left": left,
            "top": top,
            "width": right - left,
            "height": bottom - top,
        })
    return classify_ocr_line_records(
        records, frame_width=frame_width, frame_height=frame_height,
    )


def _similar(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    first = str(left.get("normalized") or "")
    second = str(right.get("normalized") or "")
    if min(len(first), len(second)) < MIN_HAN_PER_LINE:
        return False
    shared_han = set(re.findall(r"[\u3400-\u9fff]", first)) & set(
        re.findall(r"[\u3400-\u9fff]", second)
    )
    return len(shared_han) >= 3 and SequenceMatcher(None, first, second).ratio() >= 0.72


def classify_burned_in_ocr_windows(windows: list[dict[str, Any]]) -> dict[str, Any]:
    """Classify bounded OCR windows without treating one frame as proof.

    A positive requires subtitle-like Chinese text persisting across adjacent
    frames in at least two separated windows.  A fully successful negative is
    bounded evidence, not a claim that OCR can prove mathematical absence.
    """
    evidence_windows = []
    successful_frames = 0
    persistent_windows = 0
    persistent_signatures: list[str] = []
    text_frames = 0
    text_windows = 0
    bottom_band_han_frames = 0
    bottom_band_han_windows = 0
    for raw_window in windows:
        frames = raw_window.get("frames") if isinstance(raw_window, Mapping) else None
        if not isinstance(frames, list):
            continue
        frame_evidence = []
        previous_candidates: list[dict[str, Any]] = []
        persistent_pairs = 0
        window_signatures: list[str] = []
        window_has_text = False
        window_has_bottom_band_han = False
        for frame in frames:
            if not isinstance(frame, Mapping) or frame.get("status") != "ocr_success":
                frame_evidence.append({"status": str(frame.get("status") if isinstance(frame, Mapping) else "invalid")})
                previous_candidates = []
                continue
            successful_frames += 1
            if isinstance(frame.get("ocr_lines"), list):
                raw_lines = [
                    dict(line) for line in frame["ocr_lines"]
                    if isinstance(line, Mapping) and str(line.get("text") or "").strip()
                ]
                candidates = classify_ocr_line_records(
                    raw_lines,
                    frame_width=int(frame.get("width") or 0),
                    frame_height=int(frame.get("height") or 0),
                )
                han_lines = 0
                frame_height = int(frame.get("height") or 0)
                for line in raw_lines:
                    try:
                        confidence = float(line.get("confidence") or 0)
                        top = float(line.get("top") or 0)
                        height = float(line.get("height") or 0)
                    except (TypeError, ValueError):
                        continue
                    han = re.findall(r"[\u3400-\u9fff]", str(line.get("text") or ""))
                    vertical_center = (top + height / 2) / frame_height if frame_height > 0 else 0
                    if confidence >= 50 and len(han) >= 3 and 0.56 <= vertical_center <= 0.94:
                        han_lines += 1
                raw_line_count = len(raw_lines)
            else:
                tsv_text = str(frame.get("tsv") or "")
                candidates = _line_candidates(
                    tsv_text,
                    frame_width=int(frame.get("width") or 0),
                    frame_height=int(frame.get("height") or 0),
                )
                raw_line_count = sum(
                    1 for row in tsv_text.splitlines()[1:]
                    if row.rsplit("\t", 1)[-1].strip()
                )
                han_lines = len(candidates)
            if raw_line_count:
                text_frames += 1
                window_has_text = True
            if han_lines:
                bottom_band_han_frames += 1
                window_has_bottom_band_han = True
            if previous_candidates:
                for before in previous_candidates:
                    for after in candidates:
                        if _similar(before, after):
                            persistent_pairs += 1
                            signature = min(
                                str(before.get("normalized") or ""),
                                str(after.get("normalized") or ""),
                                key=len,
                            )
                            if signature:
                                window_signatures.append(signature)
                            break
                    else:
                        continue
                    break
            previous_candidates = candidates
            frame_evidence.append({
                "status": "ocr_success",
                "frame_sha256": frame.get("frame_sha256"),
                "evidence_path": frame.get("evidence_path"),
                "ocr_line_count": raw_line_count,
                "bottom_band_han_line_count": han_lines,
                "candidates": candidates,
            })
        persistent = persistent_pairs >= 1
        if persistent:
            persistent_windows += 1
            persistent_signatures.extend(window_signatures)
        if window_has_text:
            text_windows += 1
        if window_has_bottom_band_han:
            bottom_band_han_windows += 1
        evidence_windows.append({
            "offset_seconds": raw_window.get("offset_seconds"),
            "persistent_chinese_candidate": persistent,
            "persistent_pairs": persistent_pairs,
            "persistent_signatures": sorted(set(window_signatures))[:8],
            "frames": frame_evidence,
        })

    successful_windows = sum(
        any(frame.get("status") == "ocr_success" for frame in window["frames"])
        for window in evidence_windows
    )
    base = {
        "policy_version": OCR_POLICY_VERSION,
        "successful_windows": successful_windows,
        "successful_frames": successful_frames,
        "persistent_chinese_windows": persistent_windows,
        "ocr_text_windows": text_windows,
        "ocr_text_frames": text_frames,
        "bottom_band_han_windows": bottom_band_han_windows,
        "bottom_band_han_frames": bottom_band_han_frames,
        "windows": evidence_windows,
    }
    distinct_signatures: list[str] = []
    for signature in persistent_signatures:
        if not any(SequenceMatcher(None, signature, existing).ratio() >= 0.72 for existing in distinct_signatures):
            distinct_signatures.append(signature)
    base["distinct_persistent_texts"] = len(distinct_signatures)
    if persistent_windows >= 2 and len(distinct_signatures) >= 2:
        return {
            **base,
            "status": "burned_in_chinese_confirmed",
            "confidence": "high",
            "reason": "persistent_bottom_band_chinese_in_multiple_windows",
        }
    if persistent_windows >= 2:
        return {
            **base,
            "status": "pending",
            "confidence": "insufficient",
            "reason": "static_overlay_only",
        }
    if persistent_windows == 1:
        return {
            **base,
            "status": "pending",
            "confidence": "insufficient",
            "reason": "single_window_chinese_candidate",
        }
    if bottom_band_han_windows:
        return {
            **base,
            "status": "pending",
            "confidence": "insufficient",
            "reason": "bottom_band_han_detected_but_zh_cn_not_confirmed",
        }
    if (
        successful_windows >= MIN_SUCCESSFUL_WINDOWS_FOR_NEGATIVE
        and successful_frames >= MIN_SUCCESSFUL_FRAMES_FOR_NEGATIVE
        and text_windows >= MIN_TEXT_WINDOWS_FOR_NEGATIVE
        and text_frames >= MIN_TEXT_FRAMES_FOR_NEGATIVE
    ):
        return {
            **base,
            "status": "no_burned_in_chinese_evidence",
            "confidence": "bounded_negative",
            "reason": "adequate_multi_window_sample_without_persistent_chinese",
        }
    if (
        successful_windows >= MIN_SUCCESSFUL_WINDOWS_FOR_NEGATIVE
        and successful_frames >= MIN_SUCCESSFUL_FRAMES_FOR_NEGATIVE
    ):
        return {
            **base,
            "status": "pending",
            "confidence": "insufficient",
            "reason": "ocr_control_text_not_detected",
        }
    return {
        **base,
        "status": "pending",
        "confidence": "insufficient",
        "reason": "insufficient_successful_ocr_windows",
    }
