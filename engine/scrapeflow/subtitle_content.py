"""Bounded subtitle-content decoding and lightweight language classification.

The library audit must not infer a configured subtitle language from a file
suffix alone.  This module deliberately has no AList/Engine dependencies: it
accepts a bounded byte prefix, decodes a small set of common encodings, parses
only the body of ASS/SRT/VTT cues, and returns a fail-closed verdict.

It is *not* an OCR or translation system.  Ambiguous Han-only text, mixed
scripts, malformed containers, and truncated prefixes remain ``unknown``.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Mapping

from pathlib import PurePosixPath


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
_ENGLISH_WORD_RE = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")
_ENGLISH_SIGNAL_WORDS = frozenset({
    "about", "after", "again", "and", "are", "because", "been", "before",
    "being", "between", "but", "cannot", "could", "did", "does", "doing",
    "don't", "for", "from", "have", "here", "how", "into", "just", "know",
    "like", "more", "much", "must", "not", "now", "only", "our", "out",
    "please", "really", "should", "some", "than", "that", "the", "their",
    "them", "then", "there", "these", "they", "this", "those", "through",
    "under", "very", "was", "were", "what", "when", "where", "which", "who",
    "will", "with", "would", "you", "your", "you're", "you've", "we",
})


# Keep the merge lane bounded independently of the audit prefix reader.  The
# provider applies its own download cap before calling this module; this
# second cap prevents an accidental direct caller from making a large output
# object in memory.
MAX_MERGED_SUBTITLE_BYTES = MAX_PREFIX_BYTES

# DBD-Raws and a few other providers export UTF-8 subtitle sidecars through a
# text endpoint, appending a language marker and a second ``.txt`` suffix (for
# example ``Show.S02E13.sc.srt.txt``).  The same export lane also carries ASS
# documents (``Show[01].sc.ass.txt``).  These are not generic text files: they
# may enter the normal subtitle planner only after the complete, bounded
# document has been validated.  Keep the cap independent from the larger
# archive/download limits so a malformed text object cannot become an
# unbounded planner read.
EXPORTED_SRT_MAX_BYTES = MAX_MERGED_SUBTITLE_BYTES
EXPORTED_SRT_SUFFIX_RE = re.compile(
    r"\.(?P<marker>sc|tc)\.(?P<format>srt|ass|ssa)\.txt$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ExportedSrtNormalization:
    """Content proof and canonical virtual name for an exported SRT sidecar."""

    source_name: str
    normalized_name: str
    language: str
    marker: str
    size: int
    format: str = "srt"

_TIMING_LINE_RE = re.compile(
    r"^\s*(?P<start>(?:(?:\d{1,3}:)?\d{2}:\d{2}[,.]\d{3}|"
    r"\d{1,2}:\d{2}[,.]\d{3}))\s*-->\s*"
    r"(?P<end>(?:(?:\d{1,3}:)?\d{2}:\d{2}[,.]\d{3}|"
    r"\d{1,2}:\d{2}[,.]\d{3}))(?P<settings>.*)$"
)
_ASS_TIMING_RE = re.compile(
    r"^\s*(?P<start>\d{1,3}:\d{2}:\d{2}(?:[.,]\d{1,3}))\s*$"
)


@dataclass(frozen=True, slots=True)
class SubtitleCue:
    """A parsed, format-neutral subtitle cue.

    ``start_ms`` and ``end_ms`` are the only fields used for bilingual
    pairing.  A pair is accepted only when both values match exactly and the
    cue order is identical; no offset or nearest-neighbour matching is
    attempted.
    """

    start_ms: int
    end_ms: int
    text: str
    settings: str = ""


@dataclass(frozen=True, slots=True)
class SubtitleDocument:
    """A bounded subtitle document suitable for strict cue merging."""

    format: str
    cues: tuple[SubtitleCue, ...]
    text: str


def normalize_subtitle_language(value: object) -> str | None:
    """Return one supported, precisely verifiable subtitle language.

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
    if compact in {"en", "eng", "enus", "engus", "english", "英文", "英语", "英語"}:
        return "english"
    if compact in {"ko", "kor", "kokr", "korean", "韩语", "韓語"} or any(
        marker in text for marker in ("korean", "韩语", "韓語", "한국어")
    ):
        return "korean"
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


def _parse_timestamp(value: str, *, ass: bool = False) -> int | None:
    """Parse a subtitle timestamp into integer milliseconds.

    SRT/VTT use either ``HH:MM:SS.mmm`` or (VTT only) ``MM:SS.mmm``;
    ASS/SSA uses ``H:MM:SS.cc``.  Floating point is deliberately avoided so
    that a comma/dot spelling change cannot create a false timing mismatch.
    """
    token = str(value).strip().replace(",", ".")
    parts = token.split(":")
    if len(parts) == 3:
        hour_text, minute_text, second_text = parts
        try:
            hours = int(hour_text)
            minutes = int(minute_text)
        except ValueError:
            return None
        if hours < 0 or not 0 <= minutes < 60:
            return None
    elif len(parts) == 2 and not ass:
        hour_text = "0"
        minute_text, second_text = parts
        try:
            hours = 0
            minutes = int(minute_text)
        except ValueError:
            return None
        if not 0 <= minutes < 60:
            return None
    else:
        return None
    if "." not in second_text:
        return None
    seconds_text, fraction_text = second_text.split(".", 1)
    if not seconds_text.isdigit() or not fraction_text.isdigit():
        return None
    if not 0 <= int(seconds_text) < 60:
        return None
    if not 1 <= len(fraction_text) <= 3:
        return None
    milliseconds = int(fraction_text.ljust(3, "0"))
    return (hours * 3600 + minutes * 60 + int(seconds_text)) * 1000 + milliseconds


def _validate_cue_sequence(cues: list[SubtitleCue]) -> bool:
    """Reject malformed, duplicate, or out-of-order cue coordinates."""
    previous: tuple[int, int] | None = None
    seen: set[tuple[int, int]] = set()
    for cue in cues:
        if cue.start_ms < 0 or cue.end_ms <= cue.start_ms:
            return False
        if not cue.text.strip() or "\x00" in cue.text:
            return False
        key = (cue.start_ms, cue.end_ms)
        if key in seen or (previous is not None and key < previous):
            return False
        seen.add(key)
        previous = key
    return bool(cues)


def _normalised_lines(text: str) -> list[str]:
    return text.replace("\r\n", "\n").replace("\r", "\n").split("\n")


def _parse_srt_document(text: str) -> SubtitleDocument | None:
    lines = _normalised_lines(text)
    # A blank line is the only legal cue separator.  This intentionally
    # rejects a malformed/partial download instead of silently pairing cues
    # by ordinal position after dropping data.
    stripped = "\n".join(lines).strip("\n")
    if not stripped:
        return None
    blocks = [block for block in re.split(r"\n{2,}", stripped) if block.strip()]
    cues: list[SubtitleCue] = []
    for block in blocks:
        block_lines = block.split("\n")
        timing_index = 0
        if block_lines and _TIMING_LINE_RE.fullmatch(block_lines[0].strip()) is None:
            # SRT identifiers are optional but may occupy exactly one line.
            if len(block_lines) < 2:
                return None
            timing_index = 1
        if timing_index >= len(block_lines):
            return None
        match = _TIMING_LINE_RE.fullmatch(block_lines[timing_index].strip())
        if match is None or match.group("settings").strip():
            return None
        start = _parse_timestamp(match.group("start"))
        end = _parse_timestamp(match.group("end"))
        body = "\n".join(block_lines[timing_index + 1 :])
        if start is None or end is None or not body.strip():
            return None
        cues.append(SubtitleCue(start, end, body))
    if not _validate_cue_sequence(cues):
        return None
    return SubtitleDocument("srt", tuple(cues), stripped)


def _parse_vtt_document(text: str) -> SubtitleDocument | None:
    lines = _normalised_lines(text)
    first = next((line.strip() for line in lines if line.strip()), "")
    if not first.casefold().startswith("webvtt"):
        return None
    stripped = "\n".join(lines).strip("\n")
    blocks = [block for block in re.split(r"\n{2,}", stripped) if block.strip()]
    cues: list[SubtitleCue] = []
    for block in blocks:
        block_lines = block.split("\n")
        first_line = block_lines[0].strip().casefold()
        if first_line.startswith("webvtt"):
            continue
        if first_line.startswith(("style", "region")):
            # These blocks can alter rendering.  The strict merger does not
            # synthesize or reconcile CSS/region declarations, so accepting
            # them and then omitting them would silently change the subtitle.
            return None
        if first_line.startswith("note"):
            # NOTE is non-rendering commentary and cannot affect cue pairing.
            continue
        timing_index: int | None = None
        timing_match: re.Match[str] | None = None
        for index, line in enumerate(block_lines[:2]):
            candidate = _TIMING_LINE_RE.fullmatch(line.strip())
            if candidate is not None:
                timing_index, timing_match = index, candidate
                break
        if timing_index is None or timing_match is None:
            return None
        start = _parse_timestamp(timing_match.group("start"))
        end = _parse_timestamp(timing_match.group("end"))
        body = "\n".join(block_lines[timing_index + 1 :])
        if start is None or end is None or not body.strip():
            return None
        cues.append(SubtitleCue(
            start,
            end,
            body,
            timing_match.group("settings").strip(),
        ))
    if not _validate_cue_sequence(cues):
        return None
    return SubtitleDocument("vtt", tuple(cues), stripped)


def _ass_format_and_dialogues(
    text: str,
) -> tuple[list[str], int, int, list[tuple[int, list[str], str]]] | None:
    """Return ASS lines, text-field index, field count, and dialogue rows."""
    lines = _normalised_lines(text)
    in_events = False
    format_fields: list[str] | None = None
    for line in lines:
        marker = line.strip().casefold()
        if marker.startswith("[") and marker.endswith("]"):
            in_events = marker == "[events]"
            continue
        if in_events and marker.startswith("format:"):
            fields = [item.strip().casefold() for item in line.split(":", 1)[1].split(",")]
            if fields:
                format_fields = fields
            break
    if format_fields is None:
        # The canonical ASS/SSA event order is safe only when no custom
        # Format line exists; keep this fallback intentionally narrow.
        format_fields = [
            "layer", "start", "end", "style", "name", "marginl",
            "marginr", "marginv", "effect", "text",
        ]
    if any(field not in format_fields for field in ("start", "end", "text")):
        return None
    start_index = format_fields.index("start")
    end_index = format_fields.index("end")
    text_index = format_fields.index("text")
    # ASS text must be the final field: otherwise comma-containing dialogue
    # cannot be separated without guessing.
    if text_index != len(format_fields) - 1:
        return None
    dialogues: list[tuple[int, list[str], str]] = []
    in_events = False
    prefix_re = re.compile(r"^(?P<prefix>\s*Dialogue\s*:\s*)(?P<data>.*)$", re.IGNORECASE)
    for line_index, line in enumerate(lines):
        marker = line.strip().casefold()
        if marker.startswith("[") and marker.endswith("]"):
            in_events = marker == "[events]"
            continue
        if not in_events:
            continue
        match = prefix_re.match(line)
        if match is None:
            continue
        fields = match.group("data").split(",", len(format_fields) - 1)
        if len(fields) != len(format_fields):
            return None
        start = _parse_timestamp(fields[start_index], ass=True)
        end = _parse_timestamp(fields[end_index], ass=True)
        body = fields[text_index]
        if start is None or end is None or not body.strip():
            return None
        dialogues.append((line_index, fields, match.group("prefix")))
    if not dialogues:
        return None
    cues = [
        SubtitleCue(
            _parse_timestamp(fields[start_index], ass=True) or 0,
            _parse_timestamp(fields[end_index], ass=True) or 0,
            fields[text_index],
        )
        for _line_index, fields, _prefix in dialogues
    ]
    if not _validate_cue_sequence(cues):
        return None
    return lines, text_index, len(format_fields), dialogues


def _parse_ass_document(text: str) -> SubtitleDocument | None:
    parsed = _ass_format_and_dialogues(text)
    if parsed is None:
        return None
    lines, _text_index, _field_count, dialogues = parsed
    cues: list[SubtitleCue] = []
    # Reparse the timestamps here to keep the public document independent of
    # the mutable ASS field arrays used by the renderer.
    # ``_ass_format_and_dialogues`` already validated every row; locating the
    # indices from its format is repeated narrowly and deterministically.
    in_events = False
    fields_def: list[str] | None = None
    for line in lines:
        marker = line.strip().casefold()
        if marker.startswith("[") and marker.endswith("]"):
            in_events = marker == "[events]"
            continue
        if in_events and marker.startswith("format:"):
            fields_def = [item.strip().casefold() for item in line.split(":", 1)[1].split(",")]
            break
    if fields_def is None:
        fields_def = [
            "layer", "start", "end", "style", "name", "marginl",
            "marginr", "marginv", "effect", "text",
        ]
    start_index = fields_def.index("start")
    end_index = fields_def.index("end")
    text_index = fields_def.index("text")
    for _line_index, fields, _prefix in dialogues:
        start = _parse_timestamp(fields[start_index], ass=True)
        end = _parse_timestamp(fields[end_index], ass=True)
        if start is None or end is None:
            return None
        cues.append(SubtitleCue(start, end, fields[text_index]))
    return SubtitleDocument("ass", tuple(cues), "\n".join(lines).strip("\n"))


def parse_subtitle_document(
    value: object,
    *,
    max_bytes: int = MAX_MERGED_SUBTITLE_BYTES,
) -> SubtitleDocument | None:
    """Parse a complete bounded SRT, VTT, or ASS/SSA text document.

    The function intentionally returns ``None`` for every malformed or
    over-limit input.  Callers must not recover by pairing a partial prefix or
    by ordinally aligning a document whose cue structure is unknown.
    """
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
        max_bytes = MAX_MERGED_SUBTITLE_BYTES
    max_bytes = max(1024, min(MAX_MERGED_SUBTITLE_BYTES, max_bytes))
    if isinstance(value, str):
        raw = value.encode("utf-8")
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
    else:
        return None
    if not raw or len(raw) > max_bytes:
        return None
    for text in _decode_candidates(raw):
        first = next((line.strip() for line in _normalised_lines(text) if line.strip()), "")
        if first.casefold().startswith("webvtt"):
            document = _parse_vtt_document(text)
        elif re.search(r"^\s*\[events\]\s*$", text, re.IGNORECASE | re.MULTILINE):
            document = _parse_ass_document(text)
        else:
            document = _parse_srt_document(text)
        if document is not None:
            return document
    return None


def validate_exported_srt_sidecar(
    name: object,
    value: object,
    *,
    declared_size: int | None = None,
    max_bytes: int = EXPORTED_SRT_MAX_BYTES,
) -> ExportedSrtNormalization | None:
    """Validate and canonically name a ``.sc/.tc.srt.txt``/``.ass.txt`` sidecar.

    The suffix is only a language *claim*.  A caller must provide the bounded
    bytes read from the exact source object; a missing payload, a non-UTF-8
    stream, a malformed document of the declared kind, or a size mismatch
    fails closed.  The returned name is a planner-only virtual basename.  The
    caller must retain the original ``full_path`` so the normal writer moves
    the exact source object and never creates a second provider-specific copy.

    ``sc`` maps to the project's canonical ``zh-CN`` lane and ``tc`` maps to
    ``zh-TW``.  Only a final, case-insensitive ``.srt.txt``/``.ass.txt``
    suffix is accepted; an arbitrary ``.txt`` file or an embedded marker is
    never promoted.
    """
    if not isinstance(name, str) or not name or "/" in name or "\\" in name:
        return None
    match = EXPORTED_SRT_SUFFIX_RE.search(name)
    if match is None:
        return None
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
        max_bytes = EXPORTED_SRT_MAX_BYTES
    max_bytes = max(1024, min(EXPORTED_SRT_MAX_BYTES, max_bytes))
    if isinstance(declared_size, bool):
        return None
    if declared_size is not None:
        if not isinstance(declared_size, int) or declared_size <= 0 or declared_size > max_bytes:
            return None
    if isinstance(value, str):
        raw = value.encode("utf-8")
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
    else:
        return None
    if not raw or len(raw) > max_bytes:
        return None
    # A remote range read must not silently validate a truncated document.
    # When the provider supplied a size, require an exact bounded read.  The
    # no-size case is still bounded by ``max_bytes`` and is useful for local
    # staging adapters whose listings do not expose byte counts.
    if declared_size is not None and len(raw) != declared_size:
        return None
    try:
        raw.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError:
        return None
    document = parse_subtitle_document(raw, max_bytes=max_bytes)
    if document is None:
        return None
    # The declared export format must match the parsed document: a
    # ``.sc.ass.txt`` carrying SRT bytes (or the reverse) is not promoted.
    declared_format = match.group("format").casefold()
    if declared_format == "srt":
        if document.format != "srt":
            return None
        extension = "srt"
    else:
        if document.format != "ass":
            return None
        extension = "ass"
    marker = match.group("marker").casefold()
    language = "zh-CN" if marker == "sc" else "zh-TW"
    # Remove exactly the provider export suffix, preserving the release stem
    # (including its episode token) and avoiding ``Path.stem``'s lossy handling
    # of a double extension.
    stem = name[: match.start()]
    if not stem or stem in {".", ".."}:
        return None
    normalized_name = f"{stem}.{language}.{extension}"
    # Ensure the resulting basename remains a single safe path component.  A
    # Unicode control or separator in the source name must be rejected by the
    # normal source validator before this helper is called; this check keeps
    # the standalone helper fail-closed as well.
    if PurePosixPath(normalized_name).name != normalized_name or "\x00" in normalized_name:
        return None
    return ExportedSrtNormalization(
        source_name=name,
        normalized_name=normalized_name,
        language=language,
        marker=marker,
        size=len(raw),
        format=extension,
    )


def _format_srt_timestamp(milliseconds: int) -> str:
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1_000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{millis:03d}"


def _format_vtt_timestamp(milliseconds: int) -> str:
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1_000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}"


def _format_ass_timestamp(milliseconds: int) -> str:
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1_000)
    centiseconds = millis // 10
    return f"{hours}:{minutes:02d}:{seconds:02d}.{centiseconds:02d}"


def _merge_text(primary: str, original: str, format_name: str) -> str:
    separator = r"\N" if format_name == "ass" else "\n"
    return f"{primary}{separator}{original}"


def _render_merged_document(
    primary: SubtitleDocument,
    original: SubtitleDocument,
) -> bytes | None:
    if primary.format != original.format or len(primary.cues) != len(original.cues):
        return None
    for left, right in zip(primary.cues, original.cues):
        if (
            left.start_ms != right.start_ms
            or left.end_ms != right.end_ms
            or not left.text.strip()
            or not right.text.strip()
        ):
            return None
    if primary.format == "srt":
        blocks: list[str] = []
        for index, (left, right) in enumerate(zip(primary.cues, original.cues), 1):
            blocks.append(
                "\n".join((
                    str(index),
                    f"{_format_srt_timestamp(left.start_ms)} --> "
                    f"{_format_srt_timestamp(left.end_ms)}",
                    _merge_text(left.text, right.text, "srt"),
                ))
            )
        output = "\n\n".join(blocks) + "\n"
    elif primary.format == "vtt":
        blocks = ["WEBVTT"]
        for left, right in zip(primary.cues, original.cues):
            settings = f" {left.settings}" if left.settings else ""
            blocks.append(
                "\n".join((
                    f"{_format_vtt_timestamp(left.start_ms)} --> "
                    f"{_format_vtt_timestamp(left.end_ms)}{settings}",
                    _merge_text(left.text, right.text, "vtt"),
                ))
            )
        output = "\n\n".join(blocks) + "\n"
    elif primary.format == "ass":
        parsed = _ass_format_and_dialogues(primary.text)
        if parsed is None:
            return None
        lines, text_index, field_count, dialogues = parsed
        if len(dialogues) != len(primary.cues):
            return None
        for index, (line_index, fields, prefix) in enumerate(dialogues):
            left, right = primary.cues[index], original.cues[index]
            if len(fields) != field_count:
                return None
            fields[text_index] = _merge_text(left.text, right.text, "ass")
            lines[line_index] = prefix + ",".join(fields)
        output = "\n".join(lines) + "\n"
    else:
        return None
    encoded = output.encode("utf-8")
    return encoded if len(encoded) <= MAX_MERGED_SUBTITLE_BYTES else None


def _cue_has_embedded_line_break(cue: SubtitleCue, format_name: str) -> bool:
    """Return whether one source cue would hide the bilingual boundary.

    The merged formats use their ordinary rendered line separator to put
    Chinese before the original language.  If either source cue already
    contains that separator, a fresh read cannot prove which visual line is
    the language boundary after a restart.  Rejecting those candidates is
    intentionally conservative: it preserves the one-file contract without
    inventing a private marker that media players would display.
    """
    if format_name == "ass":
        return r"\N" in cue.text or r"\n" in cue.text
    return "\n" in cue.text or "\r" in cue.text


def _bilingual_failure(reason: str, *, format_name: str | None = None) -> dict[str, object]:
    result: dict[str, object] = {
        "status": "unknown",
        "classification": "unknown",
        "language_lane": "unknown",
        "reason": reason,
    }
    if format_name is not None:
        result["format"] = format_name
    return result


def _language_signal_present(body: str, language: str) -> bool:
    """Return a conservative aggregate signal for one pure language lane."""
    japanese, korean, simplified, traditional, english = _script_counts(body)
    if language == "simplified_chinese":
        return simplified >= 2 and traditional == 0
    if language == "traditional_chinese":
        return traditional >= 2 and simplified == 0
    if language == "japanese":
        return japanese >= 2
    if language == "korean":
        return korean >= 2
    if language == "english":
        return english > 0
    return False


def classify_bilingual_subtitle_content(
    value: object,
    original_language: object,
    *,
    max_bytes: int = DEFAULT_MAX_PREFIX_BYTES,
) -> dict[str, object]:
    """Validate one merged subtitle as ``中文\n原文`` for every cue.

    Every cue has exactly two nonempty rendered lines: Chinese first and the
    TMDB-confirmed original second.  Requiring exactly two lines is stricter
    than accepting arbitrary multiline source cues, but lets a fresh
    post-write reader prove the ordering instead of trusting stale delivery
    metadata.
    """
    target = normalize_subtitle_language(original_language)
    if target in {None, "simplified_chinese", "traditional_chinese"}:
        return _bilingual_failure("unsupported_original_language")
    document = parse_subtitle_document(value, max_bytes=max_bytes)
    if document is None:
        return _bilingual_failure("subtitle_decode_or_format_unknown")
    separator = r"\N" if document.format == "ass" else "\n"
    split_cues = [cue.text.split(separator) for cue in document.cues]
    if any(
        len(parts) != 2
        or not parts[0].strip()
        or not parts[1].strip()
        for parts in split_cues
    ):
        return _bilingual_failure(
            "bilingual_cue_separator_missing", format_name=document.format,
        )
    # Aggregate script counts are not enough: one valid Chinese/Japanese cue
    # followed by an English/English cue would otherwise look like a valid
    # bilingual file.  The user-visible contract is per cue, so every left
    # half must independently prove one *consistent* Chinese lane and every
    # right half must independently prove the TMDB-confirmed original
    # language.  Simplified and traditional Chinese are both Chinese here;
    # mixing them per cue is deliberately not guessed into a single track.
    chinese_lanes = [_classify_script(parts[0]) for parts in split_cues]
    if (
        not chinese_lanes
        or any(lane not in {"simplified_chinese", "traditional_chinese"}
               for lane in chinese_lanes)
        or len(set(chinese_lanes)) != 1
        or any(_classify_script(parts[1]) != target for parts in split_cues)
    ):
        return _bilingual_failure(
            "bilingual_cue_language_order_not_proven",
            format_name=document.format,
        )
    chinese_lane = chinese_lanes[0]
    classification = f"bilingual_{chinese_lane}_{target}"
    return {
        "status": "satisfied",
        "classification": classification,
        "language_lane": (
            "zh-Hans+bilingual"
            if chinese_lane == "simplified_chinese"
            else "zh-Hant+bilingual"
        ),
        "chinese_language": chinese_lane,
        "original_language": target,
        "format": document.format,
        "cue_count": len(document.cues),
        "reason": "bilingual_language_match",
    }


def _managed_subtitle_failure(
    reason: str,
    *,
    format_name: str | None = None,
) -> dict[str, object]:
    """Return a uniform fail-closed verdict for one managed sidecar.

    This is intentionally separate from :func:`classify_subtitle_content`.
    A managed external subtitle is a writer input, not merely audit evidence:
    it must be one complete UTF-8 SRT object with an exact size before it is
    eligible for the global one-track selector.
    """
    result: dict[str, object] = {
        "status": "unknown",
        "classification": "unknown",
        "selection": "unverified",
        "preference": 99,
        "reason": reason,
    }
    if format_name is not None:
        result["format"] = format_name
    return result


def validate_managed_subtitle_content(
    value: object,
    original_language: object = None,
    *,
    declared_size: int | None = None,
    max_bytes: int = EXPORTED_SRT_MAX_BYTES,
) -> dict[str, object]:
    """Prove one candidate for the managed external-subtitle slot.

    The selector is deliberately content-first.  It never treats a filename
    marker such as ``.sc``/``.tc`` as language evidence, and it never joins
    two independent Chinese tracks into a fictional bilingual track.  A
    satisfied result has exactly one of these stable preferences:

    ``0`` same-file Chinese + TMDB-confirmed original-language bilingual;
    ``1`` Simplified Chinese; ``2`` Traditional Chinese.

    Only complete, exact-size UTF-8 SRT documents are eligible.  The strict
    all-cue language check means a valid Chinese prefix followed by unrelated
    content cannot pass a persisted-plan revalidation.
    """
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
        max_bytes = EXPORTED_SRT_MAX_BYTES
    max_bytes = max(1024, min(EXPORTED_SRT_MAX_BYTES, max_bytes))
    if isinstance(declared_size, bool):
        return _managed_subtitle_failure("subtitle_size_unproven")
    if declared_size is not None and (
        not isinstance(declared_size, int)
        or declared_size <= 0
        or declared_size > max_bytes
    ):
        return _managed_subtitle_failure("subtitle_size_unproven")
    if isinstance(value, str):
        raw = value.encode("utf-8")
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
    else:
        return _managed_subtitle_failure("subtitle_content_unavailable")
    if not raw or len(raw) > max_bytes:
        return _managed_subtitle_failure("subtitle_size_unproven")
    if declared_size is not None and len(raw) != declared_size:
        return _managed_subtitle_failure("subtitle_full_read_unproven")
    try:
        raw.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError:
        return _managed_subtitle_failure("subtitle_utf8_required")
    document = parse_subtitle_document(raw, max_bytes=max_bytes)
    if document is None or document.format != "srt":
        return _managed_subtitle_failure("subtitle_complete_srt_required")

    normalized_original = normalize_subtitle_language(original_language)
    if normalized_original in {"japanese", "english", "korean"}:
        bilingual = classify_bilingual_subtitle_content(
            raw,
            normalized_original,
            max_bytes=max_bytes,
        )
        if str(bilingual.get("status") or "").casefold() == "satisfied":
            return {
                "status": "satisfied",
                "classification": str(bilingual["classification"]),
                "selection": "bilingual",
                "preference": 0,
                "chinese_language": bilingual.get("chinese_language"),
                "original_language": normalized_original,
                "format": "srt",
                "cue_count": len(document.cues),
                "size": len(raw),
                "reason": "same_file_bilingual_verified",
            }

    simplified = classify_subtitle_content(
        raw,
        "zh",
        max_bytes=max_bytes,
        require_each_cue=True,
    )
    if str(simplified.get("status") or "").casefold() == "satisfied":
        return {
            "status": "satisfied",
            "classification": "simplified_chinese",
            "selection": "simplified_chinese",
            "preference": 1,
            "format": "srt",
            "cue_count": len(document.cues),
            "size": len(raw),
            "reason": "simplified_chinese_verified",
        }

    traditional = classify_subtitle_content(
        raw,
        "zh-Hant",
        max_bytes=max_bytes,
        require_each_cue=True,
    )
    if str(traditional.get("status") or "").casefold() == "satisfied":
        return {
            "status": "satisfied",
            "classification": "traditional_chinese",
            "selection": "traditional_chinese",
            "preference": 2,
            "format": "srt",
            "cue_count": len(document.cues),
            "size": len(raw),
            "reason": "traditional_chinese_verified",
        }
    return _managed_subtitle_failure("subtitle_chinese_language_not_proven", format_name="srt")


def merge_bilingual_subtitle(
    chinese_raw: object,
    original_raw: object,
    original_language: object,
    *,
    max_bytes: int = MAX_MERGED_SUBTITLE_BYTES,
) -> dict[str, object]:
    """Strictly merge two same-episode subtitle documents.

    Both inputs must independently prove their language, use the same text
    subtitle format, and contain the exact same ordered ``(start, end)`` cue
    sequence.  A mismatch returns an ``unknown`` result and no output bytes;
    callers must leave the Gap open rather than guessing an alignment.
    """
    target = normalize_subtitle_language(original_language)
    if target in {None, "simplified_chinese", "traditional_chinese"}:
        return _bilingual_failure("unsupported_original_language")
    primary = parse_subtitle_document(chinese_raw, max_bytes=max_bytes)
    original = parse_subtitle_document(original_raw, max_bytes=max_bytes)
    if primary is None or original is None:
        return _bilingual_failure("subtitle_decode_or_format_unknown")
    if primary.format != original.format:
        return _bilingual_failure("subtitle_format_mismatch")
    if len(primary.cues) != len(original.cues):
        return _bilingual_failure("subtitle_cue_count_mismatch", format_name=primary.format)
    if any(
        left.start_ms != right.start_ms or left.end_ms != right.end_ms
        for left, right in zip(primary.cues, original.cues)
    ):
        return _bilingual_failure("subtitle_timing_mismatch", format_name=primary.format)
    if any(
        _cue_has_embedded_line_break(cue, primary.format)
        for cue in (*primary.cues, *original.cues)
    ):
        return _bilingual_failure(
            "subtitle_multiline_cue_unsupported", format_name=primary.format,
        )
    chinese_verdict = classify_subtitle_content(chinese_raw, "zh", max_bytes=max_bytes)
    if str(chinese_verdict.get("status") or "").casefold() != "satisfied":
        chinese_verdict = classify_subtitle_content(
            chinese_raw, "zh-Hant", max_bytes=max_bytes,
        )
    if str(chinese_verdict.get("status") or "").casefold() != "satisfied":
        return _bilingual_failure(
            "chinese_language_not_proven", format_name=primary.format,
        )
    original_verdict = classify_subtitle_content(
        original_raw, target, max_bytes=max_bytes,
    )
    if str(original_verdict.get("status") or "").casefold() != "satisfied":
        return _bilingual_failure(
            "original_language_not_proven", format_name=primary.format,
        )
    merged = _render_merged_document(primary, original)
    if merged is None:
        return _bilingual_failure("subtitle_merge_render_failed", format_name=primary.format)
    proof = classify_bilingual_subtitle_content(
        merged, target, max_bytes=max_bytes,
    )
    if str(proof.get("status") or "").casefold() != "satisfied":
        return _bilingual_failure(
            "merged_bilingual_content_not_proven", format_name=primary.format,
        )
    return {
        "status": "satisfied",
        "classification": proof["classification"],
        "language_lane": proof["language_lane"],
        "chinese_language": proof.get("chinese_language"),
        "original_language": target,
        "format": primary.format,
        "cue_count": len(primary.cues),
        "content": merged,
        "size": len(merged),
        "reason": "bilingual_merge_succeeded",
    }


def _is_hangul(char: str) -> bool:
    return (
        "\u1100" <= char <= "\u11ff"
        or "\u3130" <= char <= "\u318f"
        or "\ua960" <= char <= "\ua97f"
        or "\uac00" <= char <= "\ud7a3"
        or "\ud7b0" <= char <= "\ud7ff"
    )


def _english_signal_strength(body: str) -> int:
    markers = [
        word.casefold()
        for word in _ENGLISH_WORD_RE.findall(body)
        if word.casefold() in _ENGLISH_SIGNAL_WORDS
    ]
    if len(markers) < 4 or len(set(markers)) < 3:
        return 0
    return len(markers)


def _script_counts(body: str) -> tuple[int, int, int, int, int]:
    cleaned = _strip_markup(body)
    japanese = sum(1 for char in cleaned if "\u3040" <= char <= "\u30ff")
    korean = sum(1 for char in cleaned if _is_hangul(char))
    simplified = sum(1 for char in cleaned if char in _SIMPLIFIED_MARKERS)
    traditional = sum(1 for char in cleaned if char in _TRADITIONAL_MARKERS)
    english = _english_signal_strength(cleaned)
    return japanese, korean, simplified, traditional, english


def _classify_script(body: str) -> str:
    cleaned = _strip_markup(body)
    if not cleaned.strip():
        return "unknown"
    japanese, korean, simplified, traditional, english = _script_counts(cleaned)
    # Kana is a strong Japanese signal, but mixed Japanese + Chinese script is
    # intentionally unknown rather than being used to satisfy either lane.
    if japanese and (simplified or traditional):
        return "unknown"
    has_japanese = japanese >= 2
    has_korean = korean >= 2
    has_english = english > 0
    has_simplified = simplified >= 2
    has_traditional = traditional >= 2
    if has_simplified and has_traditional:
        return "unknown"
    if has_korean and (has_simplified or has_traditional or japanese or english):
        return "unknown"
    if has_english and (has_simplified or has_traditional or japanese or korean):
        return "unknown"
    if sum((has_japanese, has_korean, has_simplified, has_traditional, has_english)) > 1:
        return "unknown"
    if has_japanese:
        return "japanese"
    if has_korean:
        return "korean"
    if has_english:
        return "english"
    # One distinctive Han character is too weak to distinguish a short
    # bilingual/garbled prefix.  Requiring two signals keeps UTF-8 ``你好``
    # and a wrong legacy decode fail closed while normal cues classify.
    if has_simplified:
        return "simplified_chinese"
    if has_traditional:
        return "traditional_chinese"
    # Han-only text such as “你好” is shared by both Chinese variants.
    return "unknown"


def classify_subtitle_content(
    value: object,
    required_language: object = "zh",
    *,
    max_bytes: int = DEFAULT_MAX_PREFIX_BYTES,
    allow_bilingual: bool = False,
    original_language: object = None,
    require_each_cue: bool = False,
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
    if allow_bilingual and target == "simplified_chinese":
        bilingual = classify_bilingual_subtitle_content(value, original_language)
        if bilingual.get("status") == "satisfied":
            return bilingual
    if target is None:
        return {"status": "unknown", "classification": "unknown", "language_lane": "unknown", "reason": "unsupported_required_language"}
    if require_each_cue:
        # Formal writer proof is stronger than the ordinary library-audit
        # prefix heuristic: it has the exact object size and a full read, so
        # every cue must independently prove the requested language.  This
        # prevents a huge valid Chinese prefix plus an unrelated language tail
        # from becoming a satisfied sidecar merely by aggregate script count.
        document = parse_subtitle_document(value, max_bytes=max_bytes)
        if document is None:
            return {
                "status": "unknown", "classification": "unknown",
                "language_lane": "unknown",
                "reason": "subtitle_decode_or_format_unknown",
            }
        if any(_classify_script(cue.text) != target for cue in document.cues):
            return {
                "status": "unknown", "classification": "unknown",
                "language_lane": "unknown", "format": document.format,
                "reason": "subtitle_cue_language_not_proven",
            }
        return {
            "status": "satisfied", "classification": target,
            "language_lane": (
                "zh-Hans" if target == "simplified_chinese" else "non-zh-Hans"
            ),
            "format": document.format,
            "reason": "subtitle_language_match",
        }
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
        japanese, korean, simplified, traditional, english = _script_counts(body)
        strength = max(japanese * 3, korean * 3, simplified, traditional, english)
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
    "EXPORTED_SRT_MAX_BYTES",
    "EXPORTED_SRT_SUFFIX_RE",
    "MAX_PREFIX_BYTES",
    "MAX_MERGED_SUBTITLE_BYTES",
    "ExportedSrtNormalization",
    "SubtitleCue",
    "SubtitleDocument",
    "classify_subtitle_content",
    "classify_bilingual_subtitle_content",
    "extract_subtitle_body",
    "merge_bilingual_subtitle",
    "normalize_subtitle_language",
    "parse_subtitle_document",
    "validate_exported_srt_sidecar",
    "validate_managed_subtitle_content",
]
