"""Small, local metadata readers used by the RootJob library index.

This is deliberately not a library-wide audit.  The D step asks for one NFO
only while it is indexing the three formal shelves for the selected RootJob.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET


_NFO_MAX_BYTES = 1024 * 1024


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdecimal() and int(value) > 0:
        return int(value)
    return None


def _xml_tag_name(element: ET.Element) -> str:
    return str(element.tag).rsplit("}", 1)[-1].casefold()


def _compact_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = re.sub(r"\s+", " ", value).strip()
    if not text or any(ord(char) < 32 for char in text):
        return None
    return text[:240]


def _child_text(root: ET.Element, *names: str) -> str | None:
    wanted = {name.casefold() for name in names}
    for element in root:
        if _xml_tag_name(element) in wanted:
            text = _compact_text(element.text)
            if text is not None:
                return text
    return None


def read_nfo_identity(client: object | None, path: str) -> dict[str, object] | None:
    """Read one bounded NFO sidecar without treating it as trusted input."""
    reader = getattr(client, "read_file_bytes", None) if client is not None else None
    if not callable(reader):
        return None
    try:
        payload = reader(path, max_bytes=_NFO_MAX_BYTES)
    except Exception:
        return None
    if not isinstance(payload, (bytes, bytearray)) or len(payload) > _NFO_MAX_BYTES:
        return None
    raw = bytes(payload)
    lowered = raw.lower()
    # NFO is a remote artifact.  Reject entity declarations before parsing so
    # XML cannot pull in untrusted external content or expand entities.
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        return None
    try:
        root = ET.fromstring(raw)
    except (ET.ParseError, ValueError, UnicodeError):
        return None
    media_type = {"movie": "movie", "tvshow": "tv"}.get(_xml_tag_name(root))
    if media_type is None:
        return None
    tmdb_ids: set[int] = set()
    for element in root.iter():
        tag = _xml_tag_name(element)
        is_tmdb_unique = (
            tag == "uniqueid"
            and str(element.attrib.get("type", "")).casefold() == "tmdb"
        )
        if tag != "tmdbid" and not is_tmdb_unique:
            continue
        tmdb_id = _positive_int((element.text or "").strip())
        if tmdb_id is not None:
            tmdb_ids.add(tmdb_id)
    if len(tmdb_ids) != 1:
        return None
    year_text = _child_text(root, "year", "premiered", "releasedate")
    year_match = re.search(r"(?:18|19|20)\d{2}", year_text or "")
    return {
        "tmdb_id": next(iter(tmdb_ids)),
        "media_type": media_type,
        "title": _child_text(root, "title", "name"),
        "original_title": _child_text(root, "originaltitle", "original_title"),
        "year": year_match.group(0) if year_match else None,
    }


__all__ = ["read_nfo_identity"]
