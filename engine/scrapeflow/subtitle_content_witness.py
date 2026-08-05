"""Fail-closed content witnesses for ambiguous external subtitles.

Paths, provider order and filenames are deliberately absent from the decision.
An ambiguous candidate is selected only when two separated samples from the
target video's embedded text stream match that candidate's dialogue at the
same timeline positions and its final cue closes against the video duration.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from typing import Any, Mapping, Sequence

from engine.tools.refine_subtitle_audit import classify_subtitle_content


MIN_SAMPLE_WINDOWS = 2
MIN_WINDOW_SEPARATION_SECONDS = 120.0
MIN_UNIQUE_LINES_PER_WINDOW = 8
MIN_WINDOW_COVERAGE = 0.90
MAX_FIRST_CUE_SECONDS = 120.0
MAX_LAST_CUE_EARLY_SECONDS = 120.0
MAX_LAST_CUE_LATE_SECONDS = 10.0
WINDOW_SLACK_SECONDS = 3.0


def _seconds(value: str) -> float | None:
    match = re.fullmatch(r"\s*(\d+):(\d{2}):(\d{2})[.,](\d+)\s*", value)
    if not match:
        return None
    hours, minutes, seconds, fraction = match.groups()
    return (
        int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        + int(fraction) / (10 ** len(fraction))
    )


def _line_digest(value: str) -> str | None:
    value = re.sub(r"\{[^}]*\}|\\[Nnh]", " ", value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = unicodedata.normalize("NFKC", value).casefold()
    normalized = "".join(
        re.findall(r"[0-9a-z\u3400-\u9fff\u3040-\u30ff]+", value)
    )
    if len(normalized) < 3:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _decode(payload: bytes) -> tuple[str, str] | None:
    encodings = (
        ("utf-16", "utf-8-sig", "gb18030", "big5")
        if payload.startswith((b"\xff\xfe", b"\xfe\xff"))
        else ("utf-8-sig", "gb18030", "big5", "utf-16")
    )
    for encoding in encodings:
        try:
            value = payload.decode(encoding)
        except UnicodeDecodeError:
            continue
        if "\x00" not in value[:4096]:
            return value, encoding
    return None


def _ass_records(text: str) -> list[dict[str, Any]]:
    records = []
    for line in text.splitlines():
        if not line.casefold().startswith("dialogue:"):
            continue
        parts = line.split(",", 9)
        if len(parts) != 10:
            continue
        start = _seconds(parts[1])
        end = _seconds(parts[2])
        digest = _line_digest(parts[9])
        if start is None or end is None or end < start or digest is None:
            continue
        records.append({
            "start_seconds": start,
            "end_seconds": end,
            "line_sha256": digest,
        })
    return records


def _srt_records(text: str) -> list[dict[str, Any]]:
    records = []
    blocks = re.split(r"\r?\n\s*\r?\n", text.strip())
    for block in blocks:
        lines = block.splitlines()
        timing_index = next(
            (index for index, line in enumerate(lines) if "-->" in line), None,
        )
        if timing_index is None:
            continue
        timing = lines[timing_index].split("-->", 1)
        start = _seconds(timing[0])
        end = _seconds(timing[1].split()[0])
        digest = _line_digest(" ".join(lines[timing_index + 1:]))
        if start is None or end is None or end < start or digest is None:
            continue
        records.append({
            "start_seconds": start,
            "end_seconds": end,
            "line_sha256": digest,
        })
    return records


def build_text_witness(payload: bytes, extension: str) -> dict[str, Any]:
    """Build content-only evidence; no subtitle text is retained in the result."""
    decoded = _decode(payload)
    if decoded is None:
        return {"status": "invalid", "reason": "unknown_encoding", "records": []}
    text, encoding = decoded
    suffix = extension.casefold()
    if suffix in {".ass", ".ssa"}:
        records = _ass_records(text)
    elif suffix == ".srt":
        records = _srt_records(text)
    else:
        return {"status": "invalid", "reason": "unsupported_extension", "records": []}
    if not records:
        return {"status": "invalid", "reason": "no_timed_dialogue", "records": []}
    language = classify_subtitle_content(payload[:256 * 1024], suffix)
    unique_lines = {str(row["line_sha256"]) for row in records}
    return {
        "status": "ok",
        "encoding": encoding,
        "language_evidence": language,
        "first_dialogue_seconds": min(float(row["start_seconds"]) for row in records),
        "last_dialogue_seconds": max(float(row["end_seconds"]) for row in records),
        "dialogue_records": len(records),
        "unique_line_count": len(unique_lines),
        "records": records,
    }


def _canonical_digest(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _finite_positive(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _finite_nonnegative(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _timeline_closes(witness: Mapping[str, Any], duration: float) -> tuple[bool, float | None]:
    first = _finite_nonnegative(witness.get("first_dialogue_seconds"))
    last = _finite_nonnegative(witness.get("last_dialogue_seconds"))
    if first is None or last is None or first > MAX_FIRST_CUE_SECONDS:
        return False, None
    delta = duration - last
    return (
        -MAX_LAST_CUE_LATE_SECONDS <= delta <= MAX_LAST_CUE_EARLY_SECONDS,
        delta,
    )


def _window_lines(
    witness: Mapping[str, Any], *, offset: float, duration: float,
) -> set[str]:
    start = offset - WINDOW_SLACK_SECONDS
    end = offset + duration + WINDOW_SLACK_SECONDS
    return {
        str(row.get("line_sha256"))
        for row in witness.get("records", [])
        if isinstance(row, Mapping)
        and isinstance(row.get("line_sha256"), str)
        and _finite_nonnegative(row.get("end_seconds")) is not None
        and _finite_nonnegative(row.get("start_seconds")) is not None
        and float(row["end_seconds"]) >= start
        and float(row["start_seconds"]) <= end
    }


def _unresolved(reason: str, **evidence: Any) -> dict[str, Any]:
    return {
        "status": "unresolved",
        "selected_candidate_id": None,
        "reason": reason,
        **evidence,
    }


def resolve_ambiguous_by_embedded_witness(
    request: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    video_probe: Mapping[str, Any],
    embedded_samples: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Select one candidate only from unique timeline-bound content evidence.

    Candidate rows require ``candidate_id``, raw ``payload`` bytes and an
    ``extension``. Sample rows require raw ``payload`` bytes, ``extension``,
    ``offset_seconds`` and ``duration_seconds``. The caller is responsible for
    acquiring these read-only inputs and for binding them to the current video.
    """
    duration = _finite_positive(video_probe.get("duration_seconds"))
    if duration is None:
        return _unresolved("missing_video_duration")
    if len(candidates) < 2:
        return _unresolved("ambiguity_requires_multiple_candidates")

    candidate_rows = []
    seen_ids: set[str] = set()
    for raw in candidates:
        candidate_id = str(raw.get("candidate_id") or "")
        payload = raw.get("payload")
        extension = str(raw.get("extension") or "")
        if not candidate_id or candidate_id in seen_ids or not isinstance(payload, bytes):
            return _unresolved("invalid_candidate_input")
        seen_ids.add(candidate_id)
        witness = build_text_witness(payload, extension)
        closed, end_delta = _timeline_closes(witness, duration)
        candidate_rows.append({
            "candidate_id": candidate_id,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "witness": witness,
            "language_verified": (
                witness.get("language_evidence", {}).get("status") == "chinese"
            ),
            "timeline_closed": closed,
            "video_minus_last_cue_seconds": end_delta,
        })

    samples = []
    for raw in embedded_samples:
        offset = _finite_nonnegative(raw.get("offset_seconds"))
        window_duration = _finite_positive(raw.get("duration_seconds"))
        payload = raw.get("payload")
        extension = str(raw.get("extension") or "")
        if (
            offset is None or window_duration is None or not isinstance(payload, bytes)
            or offset + window_duration > duration + 1.0
        ):
            return _unresolved("invalid_embedded_sample")
        witness = build_text_witness(payload, extension)
        hashes = {
            str(row.get("line_sha256")) for row in witness.get("records", [])
            if isinstance(row, Mapping) and isinstance(row.get("line_sha256"), str)
        }
        if witness.get("status") != "ok" or len(hashes) < MIN_UNIQUE_LINES_PER_WINDOW:
            return _unresolved("embedded_sample_has_insufficient_dialogue")
        samples.append({
            "offset_seconds": offset,
            "duration_seconds": window_duration,
            "line_sha256": hashes,
            "payload_bytes": len(payload),
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "line_set_sha256": _canonical_digest(sorted(hashes)),
        })
    samples.sort(key=lambda row: float(row["offset_seconds"]))
    if len(samples) < MIN_SAMPLE_WINDOWS:
        return _unresolved("insufficient_sample_windows")
    if any(
        float(right["offset_seconds"]) - float(left["offset_seconds"])
        < MIN_WINDOW_SEPARATION_SECONDS
        for left, right in zip(samples, samples[1:])
    ):
        return _unresolved("sample_windows_not_independent")

    proof_rows = []
    winners = []
    for candidate in candidate_rows:
        witness = candidate["witness"]
        window_proofs = []
        for sample in samples:
            expected = sample["line_sha256"]
            actual = _window_lines(
                witness,
                offset=float(sample["offset_seconds"]),
                duration=float(sample["duration_seconds"]),
            )
            exact = len(expected & actual)
            coverage = exact / len(expected)
            window_proofs.append({
                "offset_seconds": sample["offset_seconds"],
                "sample_unique_lines": len(expected),
                "candidate_window_unique_lines": len(actual),
                "exact_line_matches": exact,
                "coverage": round(coverage, 6),
            })
        qualifies = bool(
            candidate["language_verified"]
            and candidate["timeline_closed"]
            and all(
                row["exact_line_matches"] >= MIN_UNIQUE_LINES_PER_WINDOW
                and row["coverage"] >= MIN_WINDOW_COVERAGE
                for row in window_proofs
            )
        )
        proof = {
            "candidate_id": candidate["candidate_id"],
            "payload_sha256": candidate["payload_sha256"],
            "language_verified": candidate["language_verified"],
            "timeline_closed": candidate["timeline_closed"],
            "first_dialogue_seconds": witness.get("first_dialogue_seconds"),
            "last_dialogue_seconds": witness.get("last_dialogue_seconds"),
            "video_minus_last_cue_seconds": candidate["video_minus_last_cue_seconds"],
            "windows": window_proofs,
            "qualifies": qualifies,
        }
        proof_rows.append(proof)
        if qualifies:
            winners.append(proof)

    evidence = {
        "schema_version": 1,
        "kind": "subtitle_ambiguity_content_witness",
        "request_id": request.get("request_id"),
        "video_path": request.get("video_path"),
        "video_duration_seconds": duration,
        "embedded_stream_index": video_probe.get("stream_index"),
        "candidate_proofs": sorted(proof_rows, key=lambda row: str(row["candidate_id"])),
        "embedded_sample_proofs": [{
            key: sample[key] for key in (
                "offset_seconds", "duration_seconds", "payload_bytes",
                "payload_sha256", "line_set_sha256",
            )
        } | {"unique_line_count": len(sample["line_sha256"])} for sample in samples],
        "policy": {
            "minimum_windows": MIN_SAMPLE_WINDOWS,
            "minimum_window_separation_seconds": MIN_WINDOW_SEPARATION_SECONDS,
            "minimum_unique_lines_per_window": MIN_UNIQUE_LINES_PER_WINDOW,
            "minimum_window_coverage": MIN_WINDOW_COVERAGE,
            "maximum_last_cue_early_seconds": MAX_LAST_CUE_EARLY_SECONDS,
            "maximum_last_cue_late_seconds": MAX_LAST_CUE_LATE_SECONDS,
        },
    }
    if len(winners) != 1:
        return _unresolved(
            "no_unique_content_timeline_winner" if not winners else "multiple_content_timeline_winners",
            **evidence,
        )
    selected = winners[0]
    result = {
        "status": "selected",
        "selected_candidate_id": selected["candidate_id"],
        "reason": "unique_embedded_multisample_and_timeline_witness",
        **evidence,
    }
    proof_core = {
        key: result[key] for key in (
            "schema_version", "kind", "request_id", "video_path",
            "video_duration_seconds", "embedded_stream_index", "candidate_proofs",
            "embedded_sample_proofs", "policy", "selected_candidate_id", "reason",
        )
    }
    result["proof_sha256"] = _canonical_digest(proof_core)
    return result
