"""Small, explicit work-level subtitle policy overrides.

The audit never infers a hard-subtitle decision from a filename or a probe
failure.  A user may opt one work out of an *otherwise confirmed* missing
subtitle gap by placing a bounded record in the local state file, or by
including the same explicit policy fields in an Engine/NFO work projection.
Unknown evidence remains unknown even when an override exists.
"""

from __future__ import annotations

import json
import posixpath
import re
from pathlib import Path
from typing import Mapping, Sequence


SUBTITLE_POLICY_SCHEMA_VERSION = 1
SUBTITLE_POLICY_KIND = "subtitle_policy_overrides"
SUBTITLE_POLICY_FILENAME = "subtitle-policy-overrides.json"
SUBTITLE_POLICY_MODES = frozenset({"hard_subtitle", "no_subtitle"})


def subtitle_policy_override_path(state_root: str | Path | None) -> Path | None:
    if state_root is None:
        return None
    return Path(state_root) / "library-audit" / SUBTITLE_POLICY_FILENAME


def _canonical_path(value: object) -> str | None:
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or "\\" in value
        or "\x00" in value
    ):
        return None
    normalized = posixpath.normpath(value)
    if normalized != value or any(
        part in {"", ".", ".."} for part in normalized.split("/")[1:]
    ):
        return None
    return normalized


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdecimal() and int(value) > 0:
        return int(value)
    return None


def _compact_text(value: object, limit: int = 240) -> str | None:
    if not isinstance(value, str):
        return None
    text = re.sub(r"\s+", " ", value).strip()
    if not text or len(text) > limit or any(ord(char) < 32 for char in text):
        return None
    return text


def _language_keys(value: object) -> set[str]:
    """Map only the small configured language lanes used by the audit."""
    text = str(value or "").strip().casefold().replace("_", "-")
    compact = re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", text)
    keys: set[str] = set()
    if compact in {
        "zh", "zho", "chi", "chs", "cht", "cmn", "zhcn", "zhtw",
        "zhhans", "zhhant", "简中", "繁中", "简体", "繁体", "中文",
    } or any(marker in text for marker in ("chinese", "中文", "简中", "繁中", "简体", "繁体")):
        keys.add("zh")
    if compact in {"en", "eng", "english"} or any(
        marker in text for marker in ("english", "英文", "英语", "英語")
    ):
        keys.add("en")
    if compact in {"ja", "jpn", "jp", "japanese"} or any(
        marker in text for marker in ("japanese", "日文", "日语", "日語")
    ):
        keys.add("ja")
    return keys


def _normalise_languages(value: object) -> tuple[str, ...] | None:
    if value is None:
        return ()
    values: list[object]
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = list(value)
    else:
        return None
    result: set[str] = set()
    for raw in values:
        keys = _language_keys(raw)
        if not keys:
            return None
        result.update(keys)
    return tuple(sorted(result))


def normalize_subtitle_policy_override(value: object) -> dict[str, object] | None:
    """Normalize one explicit policy value, or return ``None``.

    Accepted ergonomic forms are intentionally narrow: ``"hard_subtitle"``
    / ``"no_subtitle"``, a mapping with ``mode`` and optional language(s), or
    a boolean ``True`` under a caller's ``hard_subtitle``/``no_subtitle`` key.
    The boolean aliases are handled by :func:`subtitle_policy_for_work`.
    """
    if isinstance(value, str):
        mode = value.strip().casefold().replace("-", "_").replace(" ", "_")
        languages: tuple[str, ...] = ()
        reason = None
    elif isinstance(value, Mapping):
        if value.get("enabled") is False:
            return None
        mode = str(value.get("mode") or value.get("kind") or "").strip().casefold()
        mode = mode.replace("-", "_").replace(" ", "_")
        languages = _normalise_languages(
            value.get("languages")
            if "languages" in value
            else value.get("language")
            if "language" in value
            else value.get("subtitle_language")
        )
        if languages is None:
            return None
        reason = _compact_text(value.get("reason"))
    else:
        return None
    aliases = {
        "hard": "hard_subtitle",
        "hardsub": "hard_subtitle",
        "hard_sub": "hard_subtitle",
        "burned_in": "hard_subtitle",
        "burned_subtitle": "hard_subtitle",
        "embedded_hard": "hard_subtitle",
        "none": "no_subtitle",
        "not_required": "no_subtitle",
        "subtitle_not_required": "no_subtitle",
        "no_subtitle_needed": "no_subtitle",
    }
    mode = aliases.get(mode, mode)
    if mode not in SUBTITLE_POLICY_MODES:
        return None
    result: dict[str, object] = {"mode": mode}
    if languages:
        result["languages"] = list(languages)
    if reason is not None:
        result["reason"] = reason
    return result


def subtitle_policy_for_work(
    work: Mapping[str, object],
    required_language: str | Sequence[str] | None,
    overrides: Sequence[Mapping[str, object]] = (),
) -> dict[str, object] | None:
    """Resolve an explicit policy for one work and configured language."""
    metadata = work.get("metadata") if isinstance(work.get("metadata"), Mapping) else work
    candidates: list[object] = []
    if isinstance(metadata, Mapping):
        for key in ("subtitle_override", "subtitle_policy", "subtitle_policy_override"):
            if key in metadata:
                candidates.append(metadata.get(key))
        if metadata.get("hard_subtitle") is True or metadata.get("has_hard_subtitle") is True:
            candidates.append("hard_subtitle")
        if metadata.get("no_subtitle") is True or metadata.get("subtitle_not_required") is True:
            candidates.append("no_subtitle")
    target = set()
    if isinstance(required_language, str):
        target.update(_language_keys(required_language))
    elif isinstance(required_language, (list, tuple, set, frozenset)):
        for raw in required_language:
            target.update(_language_keys(raw))

    work_target = _canonical_path(metadata.get("target_root") or metadata.get("series_root")) if isinstance(metadata, Mapping) else None
    work_tmdb = _positive_int(metadata.get("tmdb_id")) if isinstance(metadata, Mapping) else None
    for raw in overrides:
        if not isinstance(raw, Mapping):
            continue
        override_target = _canonical_path(raw.get("target_root"))
        override_tmdb = _positive_int(raw.get("tmdb_id"))
        if work_target is None or override_target != work_target or override_tmdb != work_tmdb:
            continue
        candidates.append(raw)

    for candidate in candidates:
        normalized = normalize_subtitle_policy_override(candidate)
        if normalized is None:
            continue
        if isinstance(candidate, Mapping):
            record_id = _compact_text(candidate.get("id"), 128)
            if record_id is not None:
                normalized["id"] = record_id
        languages = set(str(value) for value in normalized.get("languages", []))
        if languages and not (target & languages):
            continue
        return normalized
    return None


def _valid_record(raw: object) -> dict[str, object] | None:
    if not isinstance(raw, Mapping):
        return None
    target = _canonical_path(raw.get("target_root"))
    tmdb_id = _positive_int(raw.get("tmdb_id"))
    policy = normalize_subtitle_policy_override(raw)
    record_id = _compact_text(raw.get("id"), 128)
    if target is None or tmdb_id is None or policy is None or record_id is None:
        return None
    # Preserve only the bounded fields consumed by the audit; arbitrary JSON
    # from a hand-edited state file never reaches a semantic work projection.
    result: dict[str, object] = {
        "id": record_id,
        "target_root": target,
        "tmdb_id": tmdb_id,
        "mode": policy["mode"],
    }
    if "languages" in policy:
        result["languages"] = list(policy["languages"])
    if "reason" in policy:
        result["reason"] = policy["reason"]
    return result


def load_subtitle_policy_overrides(state_root: str | Path | None) -> list[dict[str, object]]:
    """Load strict local work-level overrides; malformed state is ignored."""
    path = subtitle_policy_override_path(state_root)
    if path is None:
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    if not isinstance(raw, Mapping) or raw.get("schema_version") != SUBTITLE_POLICY_SCHEMA_VERSION or raw.get("kind") != SUBTITLE_POLICY_KIND:
        return []
    rows = raw.get("records")
    if not isinstance(rows, list) or len(rows) > 256:
        return []
    output: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        valid = _valid_record(row)
        if valid is None or str(valid["id"]) in seen:
            return []
        seen.add(str(valid["id"]))
        output.append(valid)
    return output


__all__ = [
    "SUBTITLE_POLICY_FILENAME",
    "SUBTITLE_POLICY_KIND",
    "SUBTITLE_POLICY_SCHEMA_VERSION",
    "load_subtitle_policy_overrides",
    "normalize_subtitle_policy_override",
    "subtitle_policy_for_work",
    "subtitle_policy_override_path",
]
