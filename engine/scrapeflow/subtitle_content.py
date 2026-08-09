"""Bounded subtitle-content decoding and lightweight language classification.

The library audit must not infer a configured subtitle language from a file
suffix alone.  This module deliberately has no AList/Engine dependencies: it
accepts a bounded byte prefix, decodes a small set of common encodings, parses
only the body of ASS/SRT/VTT cues, and returns a fail-closed verdict.

It is *not* an OCR or translation system.  Ambiguous Han-only text, mixed
scripts, malformed containers, and truncated prefixes remain ``unknown``.
"""

from __future__ import annotations

import re
from typing import Mapping


DEFAULT_MAX_PREFIX_BYTES = 512 * 1024
MAX_PREFIX_BYTES = 8 * 1024 * 1024

# Characters whose simplified/traditional forms are sufficiently distinctive
# for a conservative signal.  The sets intentionally stay small: a character
# absent from these sets is not evidence for either Chinese lane.
_SIMPLIFIED_MARKERS = frozenset(
    "体国台万与为后画龙门风云广东听说这个们来发见进还开关长问间应实书车东专业现报电网号语说让过边儿无产阶级习题测试测"
)
_TRADITIONAL_MARKERS = frozenset(
    "體國臺萬與為後畫龍門風雲廣東聽說這個們來發見進還開關長問間應實書車東專業現報電網號語說讓過邊兒無產階級習題測試測"
)

_ASS_DIALOGUE_RE = re.compile(r"^\s*dialogue\s*:\s*(.*)$", re.IGNORECASE)
_ASS_SECTION_RE = re.compile(r"^\s*\[[^]]+\]\s*$")
_SRT_VTT_TIMING_RE = re.compile(
    r"(?:\d{1,3}:)?\d{2}:\d{2}[,.]\d{3}\s*-->\s*"
    r"(?:\d{1,3}:)?\d{2}:\d{2}[,.]\d{3}"
)
_VTT_SHORT_TIMING_RE = re.compile(
    r"\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{2}:\d{2}[,.]\d{3}"
)
_ASS_TAG_RE = re.compile(r"\{[^}]*\}")
_HTML_TAG_RE = re.compile(r"<[^>]{1,128}>")
_ASS_OVERRIDE_RE = re.compile(r"\\(?:N|n|h)")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def normalize_subtitle_language(value: object) -> str | None:
    """Return one of ``simplified_chinese``, ``traditional_chinese``,
    ``japanese`` or ``None`` for an unsupported/ambiguous target.

    ``zh`` is intentionally mapped to *simplified* Chinese.  Traditional
    aliases must be requested explicitly; otherwise a traditional sidecar
    could incorrectly satisfy the usual simplified-Chinese lane.
    """

    text = str(value or "").strip().casefold().replace("_", "-")
    compact = re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", text)
    if compact in {
        "zh", "zho", "chi", "chs", "cmn", "zhcn", "zhhans", "simplified",
        "simplifiedchinese", "简中", "简体", "简体中文", "中文简体", "简",
    } or any(marker in text for marker in ("simplified chinese", "简体中文", "简体", "简中")):
        return "simplified_chinese"
    if compact in {
        "cht", "zhtw", "zhhant", "traditional", "traditionalchinese",
        "繁中", "繁體", "繁體中文", "中文繁體", "繁",
    } or any(marker in text for marker in ("traditional chinese", "繁體中文", "繁体", "繁中")):
        return "traditional_chinese"
    if compact in {"ja", "jpn", "jp", "japanese", "日文", "日语", "日語", "日本語"}:
        return "japanese"
    return None


def _bounded_bytes(value: object, max_bytes: int) -> bytes | None:
    if isinstance(value, str):
        return value.encode("utf-8")[:max_bytes]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)[:max_bytes]
    return None


def _decode_candidates(raw: bytes) -> list[str]:
    """Decode a bounded prefix using only explicitly supported encodings."""
    if not raw:
        return []
    candidates: list[str] = []
    # BOMs are authoritative and avoid a permissive fallback swallowing a
    # malformed UTF-16 payload as GB18030.
    if raw.startswith(b"\xef\xbb\xbf"):
        encodings = ("utf-8-sig",)
    elif raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        encodings = ("utf-16",)
    else:
        # UTF-16 without a BOM has a high NUL ratio in one byte lane.  Guess
        # the endian direction only when that signal is clear; otherwise the
        # strict single-byte candidates below are safer.
        even_nuls = sum(raw[index] == 0 for index in range(0, len(raw), 2))
        odd_nuls = sum(raw[index] == 0 for index in range(1, len(raw), 2))
        sample_pairs = max(1, len(raw) // 2)
        if odd_nuls >= max(2, sample_pairs // 4):
            encodings = ("utf-16-le", "utf-8", "gb18030", "big5")
        elif even_nuls >= max(2, sample_pairs // 4):
            encodings = ("utf-16-be", "utf-8", "gb18030", "big5")
        else:
            encodings = ("utf-8", "gb18030", "big5")
    for encoding in encodings:
        try:
            text = raw.decode(encoding, errors="strict")
        except (UnicodeDecodeError, LookupError):
            continue
        if text not in candidates:
            candidates.append(text)
        # A valid UTF-8 stream is unambiguous.  Do not reinterpret it as a
        # legacy code page merely because a few bytes happen to form Han
        # characters there (GB18030 can decode almost every byte sequence).
        if encoding == "utf-8" or encoding == "utf-8-sig":
            return [text]
    return candidates


def _strip_markup(value: str) -> str:
    text = _ASS_TAG_RE.sub(" ", value)
    text = _HTML_TAG_RE.sub(" ", text)
    text = _ASS_OVERRIDE_RE.sub(" ", text)
    text = text.replace("\u200b", "").replace("\ufeff", "")
    return _CONTROL_RE.sub(" ", text)


def _extract_ass_body(text: str) -> list[str]:
    rows: list[str] = []
    for line in text.splitlines():
        match = _ASS_DIALOGUE_RE.match(line)
        if not match:
            continue
        fields = match.group(1).split(",", 9)
        if len(fields) != 10:
            continue
        rows.append(fields[9])
    return rows


def _extract_timed_body(text: str) -> list[str]:
    """Extract SRT/VTT cue bodies, rejecting a plain prose file."""
    lines = text.splitlines()
    rows: list[str] = []
    saw_timing = False
    in_cue = False
    skip_block = False
    for raw_line in lines:
        line = raw_line.strip("\ufeff\r\n")
        if not line.strip():
            in_cue = False
            skip_block = False
            continue
        upper = line.upper()
        if upper in {"WEBVTT", "WEBVTT FILE"} or upper.startswith(("NOTE", "STYLE", "REGION")):
            skip_block = True
            continue
        if _SRT_VTT_TIMING_RE.search(line) or _VTT_SHORT_TIMING_RE.search(line):
            saw_timing = True
            in_cue = True
            skip_block = False
            continue
        # Numeric SRT sequence labels and cue identifiers are not body text.
        if not saw_timing and line.isdigit():
            continue
        if in_cue and not skip_block:
            rows.append(line)
    return rows if saw_timing else []


def extract_subtitle_body(text: str) -> tuple[str, str] | None:
    """Return ``(format, body)`` for a recognized ASS/SRT/VTT prefix."""
    ass_rows = _extract_ass_body(text)
    if ass_rows:
        return "ass", "\n".join(ass_rows)
    timed_rows = _extract_timed_body(text)
    if timed_rows:
        format_name = "vtt" if re.search(r"^\s*WEBVTT", text, re.IGNORECASE | re.MULTILINE) else "srt"
        return format_name, "\n".join(timed_rows)
    return None


def _script_counts(body: str) -> tuple[int, int, int]:
    cleaned = _strip_markup(body)
    japanese = sum(1 for char in cleaned if "\u3040" <= char <= "\u30ff")
    simplified = sum(1 for char in cleaned if char in _SIMPLIFIED_MARKERS)
    traditional = sum(1 for char in cleaned if char in _TRADITIONAL_MARKERS)
    return japanese, simplified, traditional


def _classify_script(body: str) -> str:
    cleaned = _strip_markup(body)
    if not cleaned.strip():
        return "unknown"
    japanese, simplified, traditional = _script_counts(cleaned)
    # Kana is a strong Japanese signal, but mixed Japanese + Chinese script is
    # intentionally unknown rather than being used to satisfy either lane.
    if japanese and (simplified or traditional):
        return "unknown"
    if japanese >= 2:
        return "japanese"
    if simplified and traditional:
        return "unknown"
    # One distinctive Han character is too weak to distinguish a short
    # bilingual/garbled prefix.  Requiring two signals keeps UTF-8 ``你好``
    # and a wrong legacy decode fail closed while normal cues classify.
    if simplified >= 2:
        return "simplified_chinese"
    if traditional >= 2:
        return "traditional_chinese"
    # Han-only text such as “你好” is shared by both Chinese variants.
    return "unknown"


def classify_subtitle_content(
    value: object,
    required_language: object = "zh",
    *,
    max_bytes: int = DEFAULT_MAX_PREFIX_BYTES,
) -> dict[str, object]:
    """Classify a bounded subtitle prefix and compare it to a target lane.

    The returned ``status`` is mutually exclusive: ``satisfied`` only means
    the parsed body proves the requested language; ``missing`` means a known,
    different language; all decode/parse/ambiguous cases are ``unknown``.
    """
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
        max_bytes = DEFAULT_MAX_PREFIX_BYTES
    max_bytes = max(1024, min(MAX_PREFIX_BYTES, max_bytes))
    target = normalize_subtitle_language(required_language)
    if target is None:
        return {"status": "unknown", "classification": "unknown", "language_lane": "unknown", "reason": "unsupported_required_language"}
    raw = _bounded_bytes(value, max_bytes)
    if raw is None or not raw:
        return {"status": "unknown", "classification": "unknown", "language_lane": "unknown", "reason": "subtitle_content_unavailable"}
    unknown_result: dict[str, object] | None = None
    known_candidates: list[tuple[int, dict[str, object]]] = []
    for text in _decode_candidates(raw):
        parsed = extract_subtitle_body(text)
        if parsed is None:
            continue
        format_name, body = parsed
        classification = _classify_script(body)
        if classification == "unknown":
            unknown_result = {
                "status": "unknown", "classification": classification,
                "language_lane": "unknown",
                "format": format_name, "reason": "subtitle_language_ambiguous",
            }
            continue
        japanese, simplified, traditional = _script_counts(body)
        strength = max(japanese * 3, simplified, traditional)
        known_candidates.append((strength, {
            "status": "satisfied" if classification == target else "missing",
            "classification": classification,
            # Public audit language is intentionally only this three-way
            # distinction; the legacy classification is retained for
            # compatibility with existing callers/tests.
            "language_lane": (
                "zh-Hans" if classification == "simplified_chinese"
                else "non-zh-Hans"
            ),
            "format": format_name,
            "reason": "subtitle_language_match" if classification == target else "subtitle_language_mismatch",
        }))
    if known_candidates:
        # Prefer the strongest language signal when a legacy byte sequence is
        # valid under more than one code page (notably Big5 vs GB18030).
        return max(known_candidates, key=lambda item: item[0])[1]
    if unknown_result is not None:
        return unknown_result
    return {
        "status": "unknown", "classification": "unknown",
        "language_lane": "unknown", "reason": "subtitle_decode_or_format_unknown",
    }


__all__ = [
    "DEFAULT_MAX_PREFIX_BYTES",
    "MAX_PREFIX_BYTES",
    "classify_subtitle_content",
    "extract_subtitle_body",
    "normalize_subtitle_language",
]
