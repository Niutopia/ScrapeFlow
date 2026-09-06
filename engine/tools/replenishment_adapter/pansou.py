"""Read-only PanSou discovery for the first replenishment tier.

PanSou is only an index.  Its official ``POST /api/search`` response carries
Quark share URLs, not a file manifest.  A row becomes runnable only after an
injected read-only Quark inspector has recursively returned the exact
``file_id/path/size`` set and the existing replenishment matcher can bind one
file to every advertised gap.  A disabled, unconfigured, capped, malformed,
or unreachable source therefore stays explicitly incomplete; it can never
manufacture a ``search_complete_no_candidates`` proof.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import json
import hashlib
import os
from pathlib import Path
import re
import time
from typing import Any, Protocol
import urllib.error
import urllib.parse
import urllib.request

import engine.tools._replenishment_local_adapter_impl as _impl
from engine.scrapeflow.media_policy import SUBTITLE_EXTENSIONS, VIDEO_EXTENSIONS
from engine.scrapeflow.provider_capabilities import (
    ACQUISITION_QUARK_FAST_SAVE,
    PROVIDER_QUARK_SHARE,
)
from engine.scrapeflow.quark_fast_save_bridge import (
    QuarkBridgeError,
    QuarkFastSaveBridge,
    QuarkShareExpiredError,
    UrlLibQuarkTransport,
    delegated_quark_session,
)


PANSOU_SOURCE = "pansou"
_FALSE_VALUES = frozenset({"", "0", "false", "no", "off"})
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_SAFE_SHARE_ID = re.compile(r"[A-Za-z0-9_-]{6,128}")
_SAFE_SHARE_PASSCODE = re.compile(r"[A-Za-z0-9_-]{1,128}")
_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
_MAX_SHARE_ENTRIES = 4096
# ``_compact_dynamic_search_terms`` is itself bounded, but a lower operator
# query cap must never be reinterpreted as proof that the source was fully
# searched.  This deliberately high inspection ceiling lets us account for
# omitted deterministic terms without turning a malformed/unbounded request
# into unbounded network I/O.
_MAX_QUERY_PROOF_TERMS = 256
_SAFE_CURSOR_FINGERPRINT = re.compile(r"^[a-f0-9]{64}$")
# Quark shares are published as show/season-level packs: their titles almost
# never carry per-episode tokens, and exact episode coverage is proven later
# by read-only manifest inspection.  Episode-specific search terms therefore
# add no precision on this lane while burying the broad pack titles behind
# many windows of guaranteed-empty queries.
_SHARE_PACK_EPISODE_MARKER = re.compile(
    r"(?i)S\d{1,3}E\d{1,4}|\bE\d{2,4}\b|\b\d{1,2}x\d{1,3}\b"
)
_SHARE_PACK_SEASON_MARKER = re.compile(
    r"(?i)\bS\d{1,3}\b|\bseason\s+\d{1,2}\b|第.+季"
)


def _share_pack_term_order(terms: Sequence[str]) -> list[str]:
    """Reorder deterministic terms for the Quark share-pack lane.

    Pack-level (season, then bare show) terms come first so the first search
    windows reach the resources this lane can actually consume; per-episode
    terms keep their deterministic order at the tail for exhaustion proof.
    """
    season_level: list[str] = []
    broad: list[str] = []
    episode_level: list[str] = []
    for term in terms:
        if _SHARE_PACK_EPISODE_MARKER.search(term):
            episode_level.append(term)
        elif _SHARE_PACK_SEASON_MARKER.search(term):
            season_level.append(term)
        else:
            broad.append(term)
    return [*season_level, *broad, *episode_level]


class PanSouDiscoveryError(RuntimeError):
    """PanSou/configuration failure which cannot prove source exhaustion."""


class PanSouTransport(Protocol):
    def __call__(
        self,
        endpoint: str,
        payload: Mapping[str, Any],
        token: str,
        timeout: float,
    ) -> Mapping[str, Any]: ...


class ShareInspector(Protocol):
    def __call__(
        self,
        pwd_id: str,
        passcode: str,
    ) -> Sequence[Mapping[str, Any]]: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        del req, fp, code, msg, headers, newurl
        return None


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, parsed))


def _bounded_float(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        parsed = float(raw)
    except ValueError:
        return default
    if parsed != parsed:  # NaN
        return default
    return max(minimum, min(maximum, parsed))


def _pansou_endpoint(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise PanSouDiscoveryError("PanSou URL is not configured")
    parsed = urllib.parse.urlsplit(raw)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise PanSouDiscoveryError("PanSou URL is invalid")
    path = parsed.path.rstrip("/")
    if path in {"", "/"}:
        path = "/api/search"
    elif path != "/api/search":
        raise PanSouDiscoveryError("PanSou URL must be an origin or /api/search")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _http_post_json(
    endpoint: str,
    payload: Mapping[str, Any],
    token: str,
    timeout: float,
) -> Mapping[str, Any]:
    """Call the documented PanSou search endpoint without following redirects."""
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "ScrapeFlow/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(endpoint, data=data, method="POST", headers=headers)
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        with opener.open(request, timeout=timeout) as response:
            raw = response.read(_MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise PanSouDiscoveryError(f"PanSou HTTP status {exc.code}") from exc
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        raise PanSouDiscoveryError(
            f"PanSou request failed: {type(exc).__name__}"
        ) from exc
    if len(raw) > _MAX_RESPONSE_BYTES:
        raise PanSouDiscoveryError("PanSou response exceeds size limit")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PanSouDiscoveryError("PanSou response is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise PanSouDiscoveryError("PanSou response is not an object")
    return value


def _official_response(value: Mapping[str, Any]) -> Mapping[str, Any]:
    """Unwrap PanSou's official ``code/message/data`` response envelope."""
    if "code" in value:
        code = value.get("code")
        if code != 0:
            raise PanSouDiscoveryError(f"PanSou API returned code {code!r}")
        data = value.get("data")
        if not isinstance(data, Mapping):
            raise PanSouDiscoveryError("PanSou success response lacks data object")
        return data
    # The upstream README also illustrates the SearchResponse object directly.
    # Accept that documented shape, but no arbitrary nested alternatives.
    return value


def _safe_text(value: object, *, maximum: int = 1024) -> str:
    if not isinstance(value, str):
        return ""
    text = " ".join(value.split()).strip()
    if not text or len(text) > maximum or any(ord(char) < 32 for char in text):
        return ""
    return text


def _quark_share(value: object) -> tuple[str, str, str] | None:
    """Return a canonical Quark share plus a narrowly accepted URL passcode.

    PanSou usually carries a share password in its separate ``password``
    field, but real Quark links can also carry exactly one ``?pwd=`` query
    parameter.  Accept only that documented form; arbitrary query strings or
    redirect-style URLs are not discovery evidence.
    """
    if not isinstance(value, str) or len(value) > 2048:
        return None
    parsed = urllib.parse.urlsplit(value.strip())
    if (
        parsed.scheme != "https"
        or parsed.hostname != "pan.quark.cn"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.fragment
    ):
        return None
    match = re.fullmatch(r"/s/([A-Za-z0-9_-]{6,128})/?", parsed.path)
    if match is None:
        return None
    pwd_id = match.group(1)
    try:
        query = urllib.parse.parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
        ) if parsed.query else []
    except ValueError:
        return None
    if query and (
        len(query) != 1
        or query[0][0] != "pwd"
        or _SAFE_SHARE_PASSCODE.fullmatch(query[0][1]) is None
    ):
        return None
    return pwd_id, f"https://pan.quark.cn/s/{pwd_id}", (
        query[0][1] if query else ""
    )


def _share_passcode(value: object, *, url_passcode: str = "") -> str:
    """Choose a safe PanSou password field, then a safe URL fallback."""
    if isinstance(value, str):
        candidate = value.strip()
        if candidate and _SAFE_SHARE_PASSCODE.fullmatch(candidate):
            return candidate
    return url_passcode


def _normalized_share_link(raw: Mapping[str, str]) -> tuple[str, dict[str, str]] | None:
    """Canonicalize one index row before it can enter the inspection queue."""
    parsed = _quark_share(raw.get("url"))
    if parsed is None:
        return None
    pwd_id, share_url, url_passcode = parsed
    normalized = dict(raw)
    normalized["url"] = share_url
    normalized["passcode"] = _share_passcode(
        raw.get("passcode"),
        url_passcode=url_passcode,
    )
    return f"{PROVIDER_QUARK_SHARE}:{pwd_id}", normalized


def _reviewed_resource_miss_locators(request: Mapping[str, Any]) -> set[str]:
    """Return bounded, canonical misses proven for this exact gap request.

    A prior successful read-only inspection can establish that a specific
    immutable share neither contains a usable manifest nor covers the current
    request.  That is distinct from a materializer failure: it is safe to
    avoid re-inspecting the same share *only* for the request that persisted
    the proof.  The root coordinator supplies this field from its scoped
    durable cache; arbitrary locators, URLs and passcodes are rejected here.
    """
    raw = request.get("reviewed_resource_miss_locators")
    if not isinstance(raw, list):
        return set()
    prefix = f"{PROVIDER_QUARK_SHARE}:"
    output: set[str] = set()
    for value in raw[:_MAX_SHARE_ENTRIES]:
        if not isinstance(value, str) or not value.startswith(prefix):
            continue
        share_id = value[len(prefix):]
        if _SAFE_SHARE_ID.fullmatch(share_id) is not None:
            output.add(value)
    return output


def _preferred_share_link(
    current: Mapping[str, str] | None,
    incoming: Mapping[str, str],
) -> dict[str, str]:
    """Keep the richer duplicate index row for a single canonical share."""
    if current is None:
        return dict(incoming)

    def rank(value: Mapping[str, str]) -> tuple[int, int, int, int]:
        return (
            1 if _safe_text(value.get("release_name")) else 0,
            1 if _safe_text(value.get("passcode"), maximum=128) else 0,
            len(_safe_text(value.get("release_name"))),
            len(_safe_text(value.get("updated_at"), maximum=128)),
        )

    return dict(incoming if rank(incoming) > rank(current) else current)


def _response_links(value: Mapping[str, Any]) -> tuple[list[dict[str, str]], int]:
    """Return normalized Quark links and response rows not proven inspected."""
    total = value.get("total")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise PanSouDiscoveryError("PanSou response total is invalid")
    result_rows = value.get("results")
    # PanSou legitimately omits both result containers for a zero-hit query.
    # Treat only that exact shape as an empty, fully inspected result; a
    # missing results array with a positive total remains a protocol failure.
    if result_rows is None and total == 0:
        result_rows = []
    merged = value.get("merged_by_type")
    if merged is None:
        # Upstream omits the merged map entirely when the requested cloud
        # type matched nothing.  That is an empty result, not a protocol
        # violation, and rejecting it turned an ordinary zero-Quark query
        # into an infrastructure failure that no exhaustion proof survives.
        merged = {}
    if not isinstance(result_rows, list) or not isinstance(merged, Mapping):
        raise PanSouDiscoveryError(
            "PanSou res=all response lacks results/merged_by_type"
        )
    unchecked = max(0, total - len(result_rows))
    links: list[dict[str, str]] = []
    for result in result_rows:
        if not isinstance(result, Mapping):
            raise PanSouDiscoveryError("PanSou result row is invalid")
        raw_links = result.get("links")
        if raw_links is None:
            # A matched post that carries no link of the requested cloud type
            # is ordinary filtered output.  It contributes no candidate and
            # nothing left unchecked, so it must not fail the whole query.
            raw_links = []
        if not isinstance(raw_links, list):
            raise PanSouDiscoveryError("PanSou result links are invalid")
        result_title = _safe_text(result.get("title"))
        for raw in raw_links:
            if not isinstance(raw, Mapping):
                raise PanSouDiscoveryError("PanSou link row is invalid")
            if str(raw.get("type") or "").strip().casefold() != "quark":
                continue
            links.append({
                "url": _safe_text(raw.get("url"), maximum=2048),
                "passcode": _safe_text(raw.get("password"), maximum=128),
                "release_name": (
                    _safe_text(raw.get("work_title")) or result_title
                ),
                "updated_at": _safe_text(raw.get("datetime"), maximum=128),
                "source": _safe_text(result.get("channel"), maximum=256),
            })
    quark_rows = merged.get("quark")
    if quark_rows is not None and not isinstance(quark_rows, list):
        raise PanSouDiscoveryError("PanSou merged Quark rows are invalid")
    for raw in quark_rows or []:
        if not isinstance(raw, Mapping):
            raise PanSouDiscoveryError("PanSou merged Quark row is invalid")
        links.append({
            "url": _safe_text(raw.get("url"), maximum=2048),
            "passcode": _safe_text(raw.get("password"), maximum=128),
            "release_name": _safe_text(raw.get("note")),
            "updated_at": _safe_text(raw.get("datetime"), maximum=128),
            "source": _safe_text(raw.get("source"), maximum=256),
        })
    return links, unchecked


def _manifest(rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], dict[int, str]]:
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise QuarkShareExpiredError("PanSou share manifest is not a file list")
    if len(rows) > _MAX_SHARE_ENTRIES:
        raise QuarkShareExpiredError("PanSou share manifest exceeds entry limit")
    files: dict[int, dict[str, Any]] = {}
    file_ids: dict[int, str] = {}
    seen_ids: dict[str, tuple[str, int]] = {}
    seen_paths: dict[str, tuple[str, int]] = {}
    for index, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise QuarkShareExpiredError("PanSou share manifest row is invalid")
        file_id = raw.get("file_id")
        path = raw.get("path")
        size = raw.get("size")
        try:
            encoded_path = path.encode("utf-8") if isinstance(path, str) else b""
        except UnicodeEncodeError as exc:
            raise QuarkShareExpiredError(
                "PanSou share manifest path is not valid UTF-8"
            ) from exc
        if (
            not isinstance(file_id, str)
            or not file_id
            or len(file_id) > 512
            or any(char in {"/", "\\"} or ord(char) < 32 for char in file_id)
            or not isinstance(path, str)
            or not path
            or len(encoded_path) > 4096
            or path.startswith("/")
            or "\\" in path
            or any(ord(char) < 32 for char in path)
            or len(path.split("/")) > QuarkFastSaveBridge.MAX_SHARE_DEPTH
            or any(part in {"", ".", ".."} for part in path.split("/"))
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size <= 0
        ):
            raise QuarkShareExpiredError("PanSou share manifest metadata is unsafe")
        identity = (path, size)
        if file_id in seen_ids and seen_ids[file_id] != identity:
            raise QuarkShareExpiredError("PanSou share file id metadata conflicts")
        path_identity = (file_id, size)
        if path in seen_paths and seen_paths[path] != path_identity:
            raise QuarkShareExpiredError("PanSou share path metadata conflicts")
        if file_id in seen_ids:
            continue
        seen_ids[file_id] = identity
        seen_paths[path] = path_identity
        files[index] = {"path": path, "size": size}
        file_ids[index] = file_id
    return {"files": files}, file_ids


def _candidate_from_share(
    request: Mapping[str, Any],
    raw: Mapping[str, str],
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    parsed = _quark_share(raw.get("url"))
    release_name = _safe_text(raw.get("release_name"))
    if parsed is None or not release_name:
        return None
    if _impl._release_year_conflict(request, release_name):  # noqa: SLF001
        # Same-title different-series share packs (``无耻之徒 2024`` the movie
        # against the 2011 series) map cleanly onto episode coordinates; the
        # shared premiere-year guard keeps them out before inspection.
        return None
    pwd_id, share_url, url_passcode = parsed
    manifest, file_ids = _manifest(rows)
    gaps = [gap for gap in request.get("gaps") or [] if isinstance(gap, Mapping)]
    has_subtitle = any(gap.get("kind") == "missing_subtitle" for gap in gaps)
    has_media = any(gap.get("kind") != "missing_subtitle" for gap in gaps)
    allowed = (
        SUBTITLE_EXTENSIONS
        if has_subtitle and not has_media
        else VIDEO_EXTENSIONS | SUBTITLE_EXTENSIONS
        if has_subtitle
        else VIDEO_EXTENSIONS
    )
    gap_map, coverage = _impl._gap_file_map(  # noqa: SLF001 - shared exact matcher
        request,
        release_name,
        manifest,
        allowed_payload_extensions=allowed,
    )
    # The fast-save materializer currently requires one exact share file per
    # selected gap.  Do not serialize a row which it cannot execute.
    if not gap_map or any(len(indices) != 1 for indices in gap_map.values()):
        return None
    selected_indices = sorted({index for values in gap_map.values() for index in values})
    files = manifest["files"]
    file_id_by_gap = {
        gap_id: [file_ids[indices[0]]]
        for gap_id, indices in gap_map.items()
    }
    selected_ids = {file_ids[index] for index in selected_indices}
    quality = " ".join(
        [release_name, *(str(files[index]["path"]) for index in selected_indices)]
    ).casefold()
    resolution = (
        "2160p" if "2160" in quality or "4k" in quality
        else "1080p" if "1080" in quality
        else "720p" if "720" in quality
        else "unknown"
    )
    candidate: dict[str, Any] = {
        "provider": PROVIDER_QUARK_SHARE,
        "locator": f"{PROVIDER_QUARK_SHARE}:{pwd_id}",
        "release_name": release_name,
        "resolution": resolution,
        "availability": "metadata_verified",
        "files": [str(files[index]["path"]) for index in selected_indices],
        "file_coverage": sorted(coverage),
        "acquisition": {
            "kind": ACQUISITION_QUARK_FAST_SAVE,
            "pwd_id": pwd_id,
            "share_id": pwd_id,
            "share_url": share_url,
            "passcode": _share_passcode(
                raw.get("passcode"), url_passcode=url_passcode,
            ),
            "file_id_by_gap": file_id_by_gap,
            "file_path_by_id": {
                file_ids[index]: str(files[index]["path"])
                for index in selected_indices
                if file_ids[index] in selected_ids
            },
            "file_size_by_id": {
                file_ids[index]: int(files[index]["size"])
                for index in selected_indices
                if file_ids[index] in selected_ids
            },
            "save_strategy": "server_side_copy",
            "requires_share_revalidation": True,
        },
        "discovery_source": PANSOU_SOURCE,
    }
    if raw.get("updated_at"):
        candidate["updated_at"] = raw["updated_at"]
    if raw.get("source"):
        candidate["source_label"] = raw["source"]
    return candidate


def quark_share_inspector(
    alist: Any,
    destination: str,
    *,
    bridge: QuarkFastSaveBridge | None = None,
    timeout: float = 12.0,
) -> ShareInspector:
    """Build a read-only inspector from the delegated AList Quark session."""
    actual_bridge = bridge or QuarkFastSaveBridge(
        UrlLibQuarkTransport(timeout=max(1.0, min(30.0, float(timeout))))
    )

    def inspect(pwd_id: str, passcode: str) -> Sequence[Mapping[str, Any]]:
        # A RootJob worker can be constructed after an API restart, before a
        # normal source read has established its in-memory AList session.  The
        # share inspector is still read-only, but it needs that session to
        # delegate the same Quark mount credentials used by the eventual
        # materializer.  Authenticate lazily here rather than treating an
        # uninitialised local client as a broken PanSou/Quark provider.
        if not getattr(alist, "token", None):
            login = getattr(alist, "login", None)
            if callable(login):
                login()
        session = delegated_quark_session(alist, destination)
        return actual_bridge.inspect_share(
            session,
            pwd_id=pwd_id,
            passcode=passcode,
        )

    return inspect


class PanSouDiscovery:
    """Bounded official-API search plus exact read-only share inspection."""

    def __init__(
        self,
        *,
        enabled: bool,
        url: str,
        token: str = "",
        inspector: ShareInspector | None = None,
        transport: PanSouTransport | None = None,
        timeout: float = 12.0,
        max_queries: int = 4,
        max_links: int = 64,
        config_issue: str = "",
    ) -> None:
        self.enabled = bool(enabled)
        self.url = url
        self.token = token
        self.inspector = inspector
        self.transport = transport or _http_post_json
        self.timeout = max(1.0, min(60.0, float(timeout)))
        self.max_queries = max(1, min(12, int(max_queries)))
        self.max_links = max(1, min(256, int(max_links)))
        self.config_issue = config_issue

    @classmethod
    def from_env(
        cls,
        *,
        inspector: ShareInspector | None = None,
        transport: PanSouTransport | None = None,
    ) -> "PanSouDiscovery":
        raw_enabled = os.getenv("SCRAPEFLOW_PANSOU_ENABLED", "0").strip().casefold()
        config_issue = ""
        if raw_enabled in _TRUE_VALUES:
            enabled = True
        elif raw_enabled in _FALSE_VALUES:
            enabled = False
        else:
            enabled = False
            config_issue = "invalid SCRAPEFLOW_PANSOU_ENABLED"
        return cls(
            enabled=enabled,
            url=os.getenv("SCRAPEFLOW_PANSOU_URL", "").strip(),
            token=os.getenv("SCRAPEFLOW_PANSOU_TOKEN", "").strip(),
            inspector=inspector,
            transport=transport,
            # The budget covers this round's queries plus read-only share
            # inspections.  A broad pack term can return hundreds of links,
            # most of them expired shares that each cost one fast 404 round
            # trip; a 60s ceiling left such windows grinding for hours at
            # ~13 links per round, so the clamp allows a longer read budget.
            timeout=_bounded_float("SCRAPEFLOW_PANSOU_TIMEOUT", 12.0, 1.0, 600.0),
            max_queries=_bounded_int("SCRAPEFLOW_PANSOU_MAX_QUERIES", 12, 1, 32),
            max_links=_bounded_int("SCRAPEFLOW_PANSOU_MAX_LINKS", 64, 1, 256),
            config_issue=config_issue,
        )

    @staticmethod
    def _incomplete(reason: str, *, configured: bool) -> dict[str, Any]:
        return {
            "version": 1,
            "candidates": [],
            "completed_sources": [],
            "search_complete": False,
            "search_complete_no_candidates": False,
            "unchecked_secondary_candidates": 1,
            "failure_scope": "infrastructure",
            "warnings": [reason],
            "source_telemetry": {
                "PanSou": {
                    "required": True,
                    "configured": configured,
                    "query_attempts": 0,
                    "query_responses": 0,
                    "query_terms_discovered": 0,
                    "query_terms_unchecked": 1,
                    "source_exhausted": False,
                    "infrastructure_failures": 1,
                    "preexcluded_candidate_count": 0,
                    "invalid_share_count": 0,
                    "inspected_share_count": 0,
                    "status": "incomplete",
                    "reason": reason,
                },
            },
            "active_search_lane": "quark_share",
        }

    def run(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            reason = self.config_issue or "PanSou discovery is disabled"
            return self._incomplete(reason, configured=False)
        try:
            endpoint = _pansou_endpoint(self.url)
        except PanSouDiscoveryError as exc:
            return self._incomplete(str(exc), configured=False)
        if self.inspector is None:
            return self._incomplete(
                "PanSou has no read-only Quark share inspector",
                configured=True,
            )
        try:
            all_terms = _share_pack_term_order(_impl._compact_dynamic_search_terms(  # noqa: SLF001
                request,
                maximum=_MAX_QUERY_PROOF_TERMS,
            ))
        except Exception:
            return self._incomplete(
                "PanSou request term construction failed",
                configured=True,
            )
        if not all_terms:
            return self._incomplete("PanSou request has no safe search term", configured=True)
        # Searches are deliberately windowed across explicit retries.  The
        # cursor is request-scoped and contains only a deterministic-term
        # fingerprint plus a bounded offset; malformed/stale cursors fail
        # closed by restarting at offset zero.
        fingerprint = hashlib.sha256(
            json.dumps(all_terms, ensure_ascii=True, separators=(",", ":")).encode()
        ).hexdigest()
        cursor = request.get("pansou_query_cursor")
        offset = 0
        if isinstance(cursor, Mapping):
            raw_fp = cursor.get("fingerprint")
            raw_offset = cursor.get("offset")
            if (
                isinstance(raw_fp, str)
                and _SAFE_CURSOR_FINGERPRINT.fullmatch(raw_fp)
                and raw_fp == fingerprint
                and isinstance(raw_offset, int)
                and not isinstance(raw_offset, bool)
                and 0 <= raw_offset <= len(all_terms)
            ):
                offset = raw_offset
        end_offset = min(len(all_terms), offset + self.max_queries)
        terms = all_terms[offset:end_offset]
        # Hitting the inspection ceiling is itself an omitted-term proof.  It
        # is intentionally represented by one opaque unit rather than a made
        # up count of terms we did not enumerate.
        unchecked_query_terms = max(0, len(all_terms) - end_offset) + (
            1 if len(all_terms) == _MAX_QUERY_PROOF_TERMS and end_offset == len(all_terms) else 0
        )

        excluded_rows = request.get("excluded_candidates")
        excluded = {
            str(row.get("locator") or "")
            for row in excluded_rows if isinstance(row, Mapping)
        } if isinstance(excluded_rows, list) else set()
        reviewed_misses = _reviewed_resource_miss_locators(request)
        attempts = 0
        responses = 0
        unchecked = unchecked_query_terms
        raw_by_locator: dict[str, dict[str, str]] = {}
        warnings: list[str] = []
        infrastructure_failures = 0
        deadline = time.monotonic() + self.timeout
        invalid_share_count = 0
        for index, term in enumerate(terms):
            if time.monotonic() >= deadline:
                unchecked += len(terms) - index
                break
            attempts += 1
            payload = {
                "kw": term,
                "res": "all",
                "src": "all",
                "refresh": False,
                "cloud_types": ["quark"],
            }
            try:
                response = _official_response(self.transport(
                    endpoint,
                    payload,
                    self.token,
                    max(1.0, deadline - time.monotonic()),
                ))
                links, response_unchecked = _response_links(response)
            except Exception as exc:
                infrastructure_failures += 1
                warnings.append(f"PanSou search unavailable: {type(exc).__name__}")
                continue
            responses += 1
            unchecked += response_unchecked
            for raw in links:
                normalized = _normalized_share_link(raw)
                if normalized is None:
                    invalid_share_count += 1
                    continue
                locator, normalized_raw = normalized
                raw_by_locator[locator] = _preferred_share_link(
                    raw_by_locator.get(locator), normalized_raw,
                )

        # Filter durable evidence before applying the per-run inspection cap.
        # Otherwise the first page can be filled entirely by already-known
        # shares forever, leaving later raw rows permanently unreachable.
        # Materializer exclusions remain deliberately unchecked: they do not
        # prove that a previously runnable share is now absent.  In contrast,
        # ``reviewed_misses`` are scoped to this exact request and come only
        # from a prior successful read-only manifest inspection.
        preexcluded = 0
        previously_reviewed = 0
        raw_links: list[tuple[str, dict[str, str]]] = []
        for locator, raw in raw_by_locator.items():
            if locator in excluded:
                preexcluded += 1
                continue
            if locator in reviewed_misses:
                previously_reviewed += 1
                continue
            raw_links.append((locator, raw))
        if len(raw_links) > self.max_links:
            unchecked += len(raw_links) - self.max_links
            raw_links = raw_links[:self.max_links]

        candidates: list[dict[str, Any]] = []
        resource_failures: list[str] = []
        reviewed_resource_misses: list[str] = []
        inspected = 0
        for index, (locator, raw) in enumerate(raw_links):
            parsed = _quark_share(raw.get("url"))
            if parsed is None:
                resource_failures.append(locator)
                inspected += 1
                continue
            pwd_id, _share_url, url_passcode = parsed
            if time.monotonic() >= deadline:
                unchecked += len(raw_links) - index
                break
            try:
                manifest_rows = self.inspector(
                    pwd_id,
                    _share_passcode(
                        raw.get("passcode"), url_passcode=url_passcode,
                    ),
                )
                candidate = _candidate_from_share(request, raw, manifest_rows)
            except QuarkShareExpiredError:
                resource_failures.append(locator)
                reviewed_resource_misses.append(locator)
                inspected += 1
                continue
            except (QuarkBridgeError, OSError, TimeoutError) as exc:
                infrastructure_failures += 1
                warnings.append(
                    f"PanSou share inspection unavailable: {type(exc).__name__}"
                )
                # This row and every later row remain unchecked.
                unchecked += len(raw_links) - inspected
                break
            except Exception as exc:
                infrastructure_failures += 1
                warnings.append(
                    f"PanSou share inspection invalid: {type(exc).__name__}"
                )
                unchecked += len(raw_links) - inspected
                break
            inspected += 1
            if candidate is None:
                resource_failures.append(locator)
                reviewed_resource_misses.append(locator)
                continue
            candidates.append(candidate)

        # A materializer-failed locator is not a fresh zero-result search.
        # The policy's 30-distinct-resource rule must remain the only way for
        # those prior valid candidates to advance this cloud tier.  Carry the
        # observation through the generic unchecked-proof field because the
        # policy deliberately accepts only that stable contract here.
        if preexcluded:
            unchecked += preexcluded
            warnings.append(
                "PanSou observed previously failed candidate(s); "
                "zero-candidate proof is unavailable"
            )

        source_exhausted = bool(
            attempts == len(terms)
            and responses == attempts
            and infrastructure_failures == 0
            and unchecked == 0
        )
        no_candidates = source_exhausted and not candidates
        # A cursor may advance only after this whole window's deterministic
        # terms and every returned share have been accounted for.  Transport,
        # inspection, response-size, or link-cap failures leave the offset in
        # place so a retry cannot silently skip evidence.
        window_complete = infrastructure_failures == 0 and unchecked == unchecked_query_terms
        cursor_offset = end_offset if window_complete else offset
        telemetry = {
            "required": True,
            "configured": True,
            "query_attempts": attempts,
            "query_responses": responses,
            "query_terms_discovered": len(all_terms),
            "query_terms_offset": offset,
            "query_cursor": {
                "fingerprint": fingerprint,
                "offset": cursor_offset,
                "exhausted": cursor_offset >= len(all_terms),
            },
            "query_terms_unchecked": unchecked_query_terms,
            "source_exhausted": source_exhausted,
            "infrastructure_failures": infrastructure_failures,
            "resource_failed_locators": sorted(set(resource_failures)),
            "reviewed_resource_miss_locators": sorted(
                set(reviewed_resource_misses),
            ),
            "previously_reviewed_miss_count": previously_reviewed,
            "unchecked_secondary_candidates": unchecked,
            "preexcluded_candidate_count": preexcluded,
            "invalid_share_count": invalid_share_count,
            "inspected_share_count": inspected,
            "status": "complete" if source_exhausted else "incomplete",
        }
        return {
            "version": 1,
            "candidates": candidates,
            "completed_sources": [PANSOU_SOURCE] if source_exhausted else [],
            "search_complete": source_exhausted,
            "search_complete_no_candidates": no_candidates,
            "unchecked_secondary_candidates": unchecked,
            **({"failure_scope": "infrastructure"} if infrastructure_failures else {}),
            "warnings": warnings,
            "source_telemetry": {"PanSou": telemetry},
            "excluded_candidate_count": len(excluded),
            "active_search_lane": "quark_share",
        }


__all__ = [
    "PANSOU_SOURCE",
    "PanSouDiscovery",
    "PanSouDiscoveryError",
    "quark_share_inspector",
]
