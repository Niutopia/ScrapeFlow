"""P14: drive the new-path Gap ledger through the exact two-tier chain.

The new architecture records precise gaps in ``gap_ledger_<root_task_id>.json``
(see ``engine.scrapeflow.gap_ledger``).  This module is the orchestrator that
closes those gaps with the current replenishment machinery:

    quark_share  -> QuarkFastSaveMaterializer
    magnet       -> LocalTorrentMaterializer

``alist_offline`` is retired.  If a single old RootJob still has that tier in
its local state, it is shown as task attention and is not executed.  It does
not block service startup, other RootJobs, or normal Quark/Torrent work.

It directly uses thin, injectable per-tier materializers and the bridge/search
boundaries, while this module owns the durable tier state.

``missing_subtitle`` gaps run through a small, independent subtitle channel
before the two video tiers.  It only retrieves direct, exact sidecars and
installs **one** formal subtitle file per gap: a verified Simplified-Chinese
track, optionally merged with the TMDB-proven original language into that same
file.  It never creates a second original-language sidecar and never falls
back to a video, season pack, or archive download.

Tier progression (contract rule 4) is delegated to the pure
``replenishment_tiers.apply_tier_outcome`` policy:

* ``candidate`` failures accumulate (per locator);
* the tier advances only on a complete no-candidate proof across every open
  request for the tier;
* ``infrastructure`` -> ``waiting == "retry_wait"`` (same tier, never downgrade);
* ``in_doubt`` -> ``waiting == "waiting_reconcile"`` (same tier, and the in-flight
  gap ids are remembered in the durable state so they are never re-submitted).

Parked current-lane ``in_doubt`` coordinates are NOT re-submitted.

The durable state lives at ``state_root / replenishment_<root_task_id>.json``
and is written with ``engine.scrapeflow.serialization.atomic_write_json``.  It
merges the pure tier-policy fields (``tier``, ``candidate_failures_by_provider``,
``exhaustion_proof_by_provider``, ``last_error_scope``) with the orchestration
fields documented below.

Reused vs. wrapped materializers
--------------------------------

The two current per-tier materializer classes are reused **as-is** through the
injectable ``materializer_factory`` (defaulting to the real classes).  They
already enforce the provider/lane contracts and write only into the
``staging_root`` they are given, so no wrapper was required.  The one small
adaptation lives in ``_classify_error``: the materializers signal *why* an
attempt failed through exception attributes (``failure_scope``,
``exclude_candidate``, ``external_task_id``/``task_id``) rather than a typed
return value, so that surface is read here and mapped onto the pure policy
scopes.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import posixpath
import re
import shutil
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from engine.scrapeflow.gap_ledger import (
    Gap,
    close_gap,
    load_gap_ledger,
    record_attempt,
)
from engine.scrapeflow.media_policy import VIDEO_EXTENSIONS
from engine.scrapeflow.replenishment_matching import audit_episode_tokens
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.subtitle_content import (
    MAX_MERGED_SUBTITLE_BYTES,
    classify_bilingual_subtitle_content,
    classify_subtitle_content,
    normalize_subtitle_language,
)
from engine.scrapeflow.target_shelf import target_root_for_shelf
from engine.scrapeflow.work_units import load_work_unit_records
from engine.tools.replenishment_adapter import SubtitleMaterializer

from .replenishment_bridge import (
    _nonempty_strings,
    gap_ledger_requests,
    gap_ledger_selection,
)
from .replenishment_tiers import (
    FAILURE_CANDIDATE,
    FAILURE_IN_DOUBT,
    FAILURE_INFRASTRUCTURE,
    STRICT_TIER_ORDER,
    TIER_LOCAL_MAGNET,
    TIER_QUARK_SHARE,
    apply_tier_outcome,
    initial_tier_state,
    required_sources_for_tier,
)
from .simple_engine_runner import EngineRequest
from .redaction import redact_error

_STATE_FILE_PREFIX = "replenishment_"
_STATE_SUFFIX = ".json"
_IN_FLIGHT_KEY = "in_flight_gap_ids"
_VIDEO_INTENTS_KEY = "video_intents"
_SUBTITLE_INTENTS_KEY = "subtitle_intents"
_SEARCH_RESOURCE_MISSES_KEY = "search_resource_misses_by_request"
_PANSOU_QUERY_CURSORS_KEY = "pansou_query_cursors_by_request"
_SEARCH_QUERY_CURSORS_KEY = "search_query_cursors_by_request"
_REVIEWED_TORRENT_MISSES_KEY = "reviewed_torrent_misses_by_request"
_MAX_ATTEMPT_LOG = 200
_STAGING_NAMESPACE = "/ScrapeFlow/补源"
_SUBTITLE_TIER = "subtitle"
_SUBTITLE_PROVIDER = "subtitle"
_SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES = frozenset({
    "japanese", "english", "korean",
})
_SUBTITLE_TEXT_EXTENSIONS = frozenset({".ass", ".ssa", ".srt", ".vtt"})
_SUBTITLE_INTENT_PHASES = frozenset({
    "prepared", "submitting", "staged", "installing", "waiting_reconcile",
})
_VIDEO_INTENT_PHASES = frozenset({
    "prepared", "submitting", "staged", "installing", "waiting_reconcile",
})
# ``load_root_replenishment_state`` intentionally keeps this marker in memory
# only.  Persisting over an unreadable state file would destroy the only
# forensic evidence of an external provider attempt.  A caller seeing the
# marker must not search, submit, download, or write until an operator has
# reconciled the file.
_STATE_RECOVERY_BLOCKED_KEY = "_recovery_blocked"
_STATE_RECOVERY_REASON_KEY = "_recovery_reason"
_STATE_ATTENTION_KEY = "_attention"
_STATE_ATTENTION_REASON_KEY = "_attention_reason"
_VIDEO_SECRET_KEY_PARTS = frozenset({
    "password", "passcode", "token", "cookie", "authorization", "secret", "pwd",
})
_LOCATOR_SECRET_QUERY = re.compile(
    # Locators are provider-controlled opaque strings, not necessarily URLs.
    # Match ordinary query strings *and* URL fragments because some share/CDN
    # providers put a passcode or token after ``#``.  Durable state may retain
    # a redacted locator for diagnostics, but never a credential value.
    r"([?&#][^=&?#]*(?:password|passcode|token|cookie|authorization|secret|pwd)[^=&?#]*=)[^&#]*",
    re.IGNORECASE,
)
_SAFE_DURABLE_TASK_ID = re.compile(r"^[A-Za-z0-9._:-]{1,256}$")
_SAFE_SEARCH_REQUEST_KEY = re.compile(r"^[a-f0-9]{64}$")
_SAFE_QUARK_SHARE_LOCATOR = re.compile(
    r"^quark_share:[A-Za-z0-9_-]{6,128}$",
)
_MAX_SEARCH_RESOURCE_MISS_SCOPES = 64
_MAX_SEARCH_RESOURCE_MISSES_PER_SCOPE = 512
_MAX_PANSOU_QUERY_CURSORS = 64
_MAX_SEARCH_QUERY_CURSOR_SCOPES = 64
_MAX_REVIEWED_TORRENT_MISS_SCOPES = 64
_MAX_REVIEWED_TORRENT_MISSES_PER_SCOPE = 512
_SAFE_TORRENT_LOCATOR = re.compile(
    r"^torrent:(?:[0-9a-f]{40}|[a-z2-7]{32})$", re.IGNORECASE,
)
_VIDEO_MEDIA_SUBROOT = "__scrapeflow_media__"
_MAX_VIDEO_STAGING_NODES = 512
_MAX_VIDEO_STAGING_DEPTH = 16

_KNOWN_FAILURE_SCOPES = frozenset({
    FAILURE_CANDIDATE, FAILURE_INFRASTRUCTURE, FAILURE_IN_DOUBT,
})


def _trace(message: str) -> None:
    """Bounded live observability line (mirrors the legacy runner's tracer)."""
    print(f"[root-replenishment] {message}", flush=True)

# The provider name for each current tier equals the tier name, which is also
# what current materializers validate.
_TIER_PROVIDER = {
    TIER_QUARK_SHARE: TIER_QUARK_SHARE,
    TIER_LOCAL_MAGNET: TIER_LOCAL_MAGNET,
}


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _state_path(state_root: Path, root_task_id: str) -> Path:
    return state_root / f"{_STATE_FILE_PREFIX}{root_task_id}{_STATE_SUFFIX}"


def _fresh_state() -> dict[str, Any]:
    state: dict[str, Any] = initial_tier_state()
    state.update({
        "updated_at": _now(),
        "last_attempt_at": None,
        "waiting": None,
        "attempt_log": [],
        _IN_FLIGHT_KEY: {},
        # One immutable row per video provider attempt.  Unlike the former
        # token->task-id projection, this contains enough request/selection
        # evidence to resume the same staged child without another acquire.
        _VIDEO_INTENTS_KEY: {},
        # Exact read-only share misses are scoped to one identity and its
        # current open gap coordinates.  They let a bounded PanSou pass make
        # forward progress without turning an uninspected share into a
        # candidate failure or a cross-work exclusion.
        _SEARCH_RESOURCE_MISSES_KEY: {},
        # Bounded PanSou deterministic-term cursors.  Values are only a
        # fingerprint and offset, scoped to one exact open-gap request.
        _PANSOU_QUERY_CURSORS_KEY: {},
        # Generic read-only provider cursors.  Values are source-keyed and
        # contain only a validated request fingerprint plus bounded integer
        # coordinates (for example AnimeTosho term/page).
        _SEARCH_QUERY_CURSORS_KEY: {},
        # Infohashes of torrent manifests that were fetched, parsed and
        # proven non-covering for this exact request.  URLs are never stored.
        _REVIEWED_TORRENT_MISSES_KEY: {},
        # One entry exists only while a subtitle acquisition/write requires
        # recovery.  It is intentionally separate from the video-tier
        # in-flight map: subtitle providers have no video-tier reconcile API,
        # so an uncertain subtitle must be held rather than resubmitted.
        _SUBTITLE_INTENTS_KEY: {},
    })
    return state


def _bounded_path(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    path = value.strip()
    if not path or len(path) > 2048 or "\x00" in path:
        return None
    return path


def _redact_sensitive_text(value: str) -> tuple[str, bool]:
    redacted = _LOCATOR_SECRET_QUERY.sub(r"\1<redacted>", value)
    return redacted, redacted == value and "<redacted>" not in value


def _safe_durable_error(value: object, *, fallback: str = "补源操作失败") -> str:
    """Return a bounded error projection safe for JSON state and ledgers.

    Provider exceptions can echo a share URL, a task URL, or a passcode.  The
    root state and Gap ledger are durable forensic records, so they must never
    receive a raw ``str(exc)`` even when the caller only intends a diagnostic.
    ``redact_error`` covers configured/structured secrets; the locator pass
    also covers query and fragment credentials in opaque provider text.
    """
    text, _safe = _redact_sensitive_text(redact_error(value))
    return (text or fallback)[:200]


def _durable_locator(value: object) -> tuple[str | None, bool]:
    locator = _bounded_path(value)
    return (None, False) if locator is None else _redact_sensitive_text(locator)


def _durable_mapping(value: object) -> tuple[dict[str, Any] | None, bool]:
    """Copy JSON evidence, redacting credentials and marking it unrecoverable."""
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
        copied = json.loads(encoded)
    except (TypeError, ValueError):
        return None, False
    if not isinstance(copied, dict) or len(encoded.encode("utf-8")) > 262_144:
        return None, False
    recovery_safe = True

    def redact(row: object) -> object:
        nonlocal recovery_safe
        if isinstance(row, dict):
            output = {}
            for key, item in row.items():
                normalized = "".join(char for char in key.casefold() if char.isalnum())
                if any(part in normalized for part in _VIDEO_SECRET_KEY_PARTS):
                    recovery_safe = False
                    continue
                output[key] = redact(item)
            return output
        if isinstance(row, list):
            return [redact(item) for item in row]
        if isinstance(row, str):
            text, safe = _redact_sensitive_text(row)
            recovery_safe &= safe
            return text
        return row

    return redact(copied), recovery_safe


def _normalize_video_intents(value: object) -> dict[str, dict[str, Any]] | None:
    """Return canonical recovery rows, or ``None`` for an unsafe state."""
    if value is None:
        return {}
    if not isinstance(value, Mapping) or len(value) > 32:
        return None
    intents: dict[str, dict[str, Any]] = {}
    for key, raw in value.items():
        attempt_id = _safe_task_id(key)
        if attempt_id is None or not isinstance(raw, Mapping):
            return None
        tier = str(raw.get("tier") or "").strip().casefold()
        provider = str(raw.get("provider") or "").strip().casefold()
        selected = raw.get("selected_gap_ids")
        ledger_ids = raw.get("ledger_gap_ids")
        request, request_safe = _durable_mapping(raw.get("request"))
        selection, selection_safe = _durable_mapping(raw.get("selection"))
        locator, locator_safe = _durable_locator(raw.get("locator"))
        valid_ids = lambda rows: (
            isinstance(rows, list) and bool(rows)
            and all(isinstance(item, str) and item and len(item) <= 512 for item in rows)
            and len(rows) == len(set(rows))
        )
        if (
            raw.get("attempt_id") != attempt_id
            or str(raw.get("phase") or "").casefold() not in _VIDEO_INTENT_PHASES
            or tier not in STRICT_TIER_ORDER
            or provider != _TIER_PROVIDER.get(tier)
            or locator is None
            or _bounded_path(raw.get("staging_root")) is None
            or _bounded_path(raw.get("workspace")) is None
            or _safe_task_id(raw.get("child_job_id")) is None
            or not valid_ids(selected) or not valid_ids(ledger_ids)
            or request is None or selection is None
            or selection.get("provider") != provider
            or _durable_locator(selection.get("locator"))[0] != locator
            or selection.get("selected_gap_ids") != selected
            or not isinstance(raw.get("recovery_safe"), bool)
        ):
            return None
        entry = {
            "phase": str(raw["phase"]).casefold(), "attempt_id": attempt_id,
            "tier": tier, "provider": provider, "locator": locator,
            "selected_gap_ids": list(selected), "ledger_gap_ids": list(ledger_ids),
            "staging_root": _bounded_path(raw["staging_root"]),
            "workspace": _bounded_path(raw["workspace"]),
            "child_job_id": _safe_task_id(raw["child_job_id"]),
            "request": request, "selection": selection,
            "recovery_safe": bool(
                raw["recovery_safe"] and request_safe and selection_safe and locator_safe
            ),
        }
        task_id = raw.get("external_task_id")
        if task_id is not None:
            task_id = _safe_task_id(task_id)
            if task_id is None:
                return None
            entry["external_task_id"] = task_id
        delivery = raw.get("delivery")
        if delivery is not None:
            delivery, _ = _durable_mapping(delivery)
            if delivery is None:
                return None
            entry["delivery"] = delivery
        intents[attempt_id] = entry
    return intents


def _state_recovery_blocked(reason: str) -> dict[str, Any]:
    state = _fresh_state()
    state.update({
        _STATE_RECOVERY_BLOCKED_KEY: True,
        _STATE_RECOVERY_REASON_KEY: reason[:200],
        "waiting": "waiting_reconcile",
    })
    return state


def _state_attention(reason: str) -> dict[str, Any]:
    """Describe one retired local task without changing its old state file.

    The old AList lane cannot be resumed safely, but it must not become a
    service-wide check or a migration workflow.  Returning an in-memory
    attention state leaves the original file untouched and lets all other
    selected RootJobs run normally.
    """
    state = _fresh_state()
    state.update({
        _STATE_ATTENTION_KEY: True,
        _STATE_ATTENTION_REASON_KEY: reason[:200],
        "waiting": "attention",
    })
    return state


def _normalize_subtitle_intents(value: object) -> dict[str, dict[str, Any]]:
    """Retain only minimal, non-secret recovery evidence for subtitle work.

    Durable intent is written before the provider is called.  A malformed
    state must therefore become a conservative recovery barrier rather than a
    license to call a provider again with an unknown prior attempt.
    """
    if not isinstance(value, Mapping):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for raw_gap_id, raw in value.items():
        if (
            not isinstance(raw_gap_id, str)
            or not raw_gap_id
            or len(raw_gap_id) > 512
        ):
            continue
        if not isinstance(raw, Mapping):
            normalized[raw_gap_id] = {"phase": "waiting_reconcile"}
            continue
        phase = str(raw.get("phase") or "").strip().casefold()
        # An unrecognised persisted attempt may be a partially-written record
        # from an older process.  Keep it as a barrier; silently dropping it
        # would permit a duplicate subtitle provider submission.
        if phase not in _SUBTITLE_INTENT_PHASES:
            normalized[raw_gap_id] = {"phase": "waiting_reconcile"}
            continue
        entry: dict[str, Any] = {"phase": phase}
        for key in ("attempt_id", "staging_root", "source", "target", "video_path"):
            parsed = _bounded_path(raw.get(key))
            if parsed is not None:
                entry[key] = parsed
        size = raw.get("size")
        if (
            isinstance(size, int)
            and not isinstance(size, bool)
            and 0 < size <= MAX_MERGED_SUBTITLE_BYTES
        ):
            entry["size"] = size
        language = normalize_subtitle_language(raw.get("subtitle_language"))
        if language == "simplified_chinese":
            entry["subtitle_language"] = language
        original_language = normalize_subtitle_language(raw.get("original_language"))
        if original_language in _SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES:
            entry["original_language"] = original_language
        if raw.get("bilingual") is True:
            entry["bilingual"] = True
        updated_at = raw.get("updated_at")
        if isinstance(updated_at, str) and len(updated_at) <= 64:
            entry["updated_at"] = updated_at
        normalized[raw_gap_id] = entry
    return normalized


def _normalize_search_resource_misses(value: object) -> dict[str, list[str]]:
    """Keep only bounded, request-scoped, non-secret read-only miss facts.

    This cache is never recovery evidence for a provider submit.  Malformed
    entries are therefore discarded rather than becoming a barrier: the safe
    fallback is simply to inspect that share again.
    """
    if not isinstance(value, Mapping):
        return {}
    output: dict[str, list[str]] = {}
    for raw_key, raw_locators in list(value.items())[:_MAX_SEARCH_RESOURCE_MISS_SCOPES]:
        if (
            not isinstance(raw_key, str)
            or _SAFE_SEARCH_REQUEST_KEY.fullmatch(raw_key) is None
            or not isinstance(raw_locators, list)
        ):
            continue
        locators = sorted({
            locator
            for locator in raw_locators[:_MAX_SEARCH_RESOURCE_MISSES_PER_SCOPE]
            if isinstance(locator, str)
            and _SAFE_QUARK_SHARE_LOCATOR.fullmatch(locator) is not None
        })
        if locators:
            output[raw_key] = locators
    return output


def _search_resource_miss_request_key(
    tier: str,
    request: Mapping[str, Any],
) -> str | None:
    """Fingerprint one search identity plus its exact currently-open gaps."""
    if tier != TIER_QUARK_SHARE:
        return None
    media = request.get("media")
    if not isinstance(media, Mapping):
        return None
    media_type = str(media.get("media_type") or "").strip().casefold()
    tmdb_id = media.get("tmdb_id")
    if (
        media_type not in {"movie", "tv"}
        or isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        return None
    raw_gaps = request.get("gaps")
    gap_ids = sorted({
        str(row.get("id") or "").strip()
        for row in (raw_gaps if isinstance(raw_gaps, list) else [])
        if isinstance(row, Mapping)
        and isinstance(row.get("id"), str)
        and row.get("id").strip()
        and len(row.get("id").strip()) <= 512
    })
    if not gap_ids:
        return None
    payload = json.dumps(
        [tier, media_type, tmdb_id, gap_ids],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _known_search_resource_misses(
    state: Mapping[str, Any],
    request_key: str | None,
) -> list[str]:
    if request_key is None:
        return []
    cached = state.get(_SEARCH_RESOURCE_MISSES_KEY)
    if not isinstance(cached, Mapping):
        return []
    values = cached.get(request_key)
    if not isinstance(values, list):
        return []
    return sorted({
        locator
        for locator in values[:_MAX_SEARCH_RESOURCE_MISSES_PER_SCOPE]
        if isinstance(locator, str)
        and _SAFE_QUARK_SHARE_LOCATOR.fullmatch(locator) is not None
    })


def _remember_search_resource_misses(
    state: dict[str, Any],
    request_key: str | None,
    locators: object,
) -> None:
    if request_key is None or not isinstance(locators, list):
        return
    existing = _normalize_search_resource_misses(
        state.get(_SEARCH_RESOURCE_MISSES_KEY),
    )
    accepted = {
        locator
        for locator in locators[:_MAX_SEARCH_RESOURCE_MISSES_PER_SCOPE]
        if isinstance(locator, str)
        and _SAFE_QUARK_SHARE_LOCATOR.fullmatch(locator) is not None
    }
    if not accepted:
        state[_SEARCH_RESOURCE_MISSES_KEY] = existing
        return
    merged = sorted({*existing.get(request_key, []), *accepted})[
        :_MAX_SEARCH_RESOURCE_MISSES_PER_SCOPE
    ]
    if request_key not in existing and len(existing) >= _MAX_SEARCH_RESOURCE_MISS_SCOPES:
        # Insertion order is durable JSON order.  Dropping the oldest cache
        # entry only causes a future read-only reinspection; it cannot hide a
        # provider submission or widen any writer scope.
        oldest = next(iter(existing), None)
        if oldest is not None:
            existing.pop(oldest, None)
    existing[request_key] = merged
    state[_SEARCH_RESOURCE_MISSES_KEY] = existing


def _normalize_pansou_query_cursors(value: object) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    output: dict[str, dict[str, Any]] = {}
    for raw_key, raw_cursor in list(value.items())[:_MAX_PANSOU_QUERY_CURSORS]:
        if (
            not isinstance(raw_key, str)
            or _SAFE_SEARCH_REQUEST_KEY.fullmatch(raw_key) is None
            or not isinstance(raw_cursor, Mapping)
        ):
            continue
        fingerprint = raw_cursor.get("fingerprint")
        offset = raw_cursor.get("offset")
        if (
            not isinstance(fingerprint, str)
            or not re.fullmatch(r"[a-f0-9]{64}", fingerprint)
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
            or offset > 256
        ):
            continue
        output[raw_key] = {"fingerprint": fingerprint, "offset": offset}
    return output


def _known_pansou_query_cursor(
    state: Mapping[str, Any], request_key: str | None,
) -> dict[str, Any] | None:
    if request_key is None:
        return None
    value = _normalize_pansou_query_cursors(state.get(_PANSOU_QUERY_CURSORS_KEY))
    cursor = value.get(request_key)
    return dict(cursor) if isinstance(cursor, Mapping) else None


def _remember_pansou_query_cursor(
    state: dict[str, Any], request_key: str | None, cursor: object,
) -> None:
    if request_key is None or not isinstance(cursor, Mapping):
        return
    value = _normalize_pansou_query_cursors(state.get(_PANSOU_QUERY_CURSORS_KEY))
    fingerprint = cursor.get("fingerprint")
    offset = cursor.get("offset")
    if (
        not isinstance(fingerprint, str)
        or not re.fullmatch(r"[a-f0-9]{64}", fingerprint)
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
        or offset > 256
    ):
        state[_PANSOU_QUERY_CURSORS_KEY] = value
        return
    if request_key not in value and len(value) >= _MAX_PANSOU_QUERY_CURSORS:
        value.pop(next(iter(value)), None)
    value[request_key] = {"fingerprint": fingerprint, "offset": offset}
    state[_PANSOU_QUERY_CURSORS_KEY] = value


def _search_query_request_key(
    tier: str,
    request: Mapping[str, Any],
) -> str | None:
    """Fingerprint one exact identity/gap request for generic search facts."""
    if tier not in STRICT_TIER_ORDER:
        return None
    media = request.get("media")
    if not isinstance(media, Mapping):
        return None
    media_type = str(media.get("media_type") or "").strip().casefold()
    tmdb_id = media.get("tmdb_id")
    if (
        media_type not in {"movie", "tv"}
        or isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        return None
    raw_gaps = request.get("gaps")
    gap_rows = []
    for row in raw_gaps if isinstance(raw_gaps, list) else []:
        if not isinstance(row, Mapping):
            continue
        gap_id = row.get("id")
        if not isinstance(gap_id, str) or not gap_id.strip() or len(gap_id) > 512:
            continue
        gap_rows.append({
            "id": gap_id.strip(),
            "kind": str(row.get("kind") or ""),
            "season": row.get("season"),
            "episodes": sorted({
                int(value) for value in (row.get("episodes") or [])
                if type(value) is int and value > 0
            }),
        })
    if not gap_rows:
        return None
    payload = [
        "search_cursor", tier, media_type, tmdb_id,
        media.get("title"), media.get("original_title"),
        [value for value in (media.get("aliases") or []) if isinstance(value, str)][:40],
        sorted(gap_rows, key=lambda row: row["id"]),
    ]
    return hashlib.sha256(json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _normalize_search_query_cursors(
    value: object,
) -> dict[str, dict[str, dict[str, Any]]]:
    if not isinstance(value, Mapping):
        return {}
    output: dict[str, dict[str, dict[str, Any]]] = {}
    for raw_key, raw_sources in list(value.items())[:_MAX_SEARCH_QUERY_CURSOR_SCOPES]:
        if (
            not isinstance(raw_key, str)
            or _SAFE_SEARCH_REQUEST_KEY.fullmatch(raw_key) is None
            or not isinstance(raw_sources, Mapping)
        ):
            continue
        sources: dict[str, dict[str, Any]] = {}
        for raw_source, raw_cursor in list(raw_sources.items())[:32]:
            source = re.sub(r"[^a-z0-9_]+", "", str(raw_source).casefold())
            if not source or len(source) > 32 or not isinstance(raw_cursor, Mapping):
                continue
            fingerprint = raw_cursor.get("fingerprint")
            exhausted = raw_cursor.get("exhausted")
            if (
                not isinstance(fingerprint, str)
                or re.fullmatch(r"[a-f0-9]{64}", fingerprint) is None
                or type(exhausted) is not bool
            ):
                continue
            cursor: dict[str, Any] = {
                "fingerprint": fingerprint,
                "exhausted": exhausted,
            }
            offset = raw_cursor.get("offset")
            if (
                type(offset) is int and 0 <= offset <= 256
            ):
                cursor["offset"] = offset
            else:
                term_index = raw_cursor.get("term_index")
                page = raw_cursor.get("page")
                if not (
                    type(term_index) is int and 0 <= term_index <= 256
                    and type(page) is int and 1 <= page <= 256
                ):
                    continue
                cursor["term_index"] = term_index
                cursor["page"] = page
            sources[source] = cursor
        if sources:
            output[raw_key] = sources
    return output


def _known_search_query_cursors(
    state: Mapping[str, Any], request_key: str | None,
) -> dict[str, dict[str, Any]]:
    if request_key is None:
        return {}
    sources = _normalize_search_query_cursors(
        state.get(_SEARCH_QUERY_CURSORS_KEY),
    ).get(request_key, {})
    return {
        source: dict(cursor) for source, cursor in sources.items()
        if isinstance(cursor, Mapping)
    }


def _remember_search_query_cursors(
    state: dict[str, Any], request_key: str | None, telemetry: object,
) -> None:
    if request_key is None or not isinstance(telemetry, Mapping):
        return
    value = _normalize_search_query_cursors(
        state.get(_SEARCH_QUERY_CURSORS_KEY),
    )
    sources = dict(value.get(request_key, {}))
    for raw_source, raw_facts in telemetry.items():
        source = re.sub(r"[^a-z0-9_]+", "", str(raw_source).casefold())
        if not source or len(source) > 32 or not isinstance(raw_facts, Mapping):
            continue
        raw_cursor = raw_facts.get("query_cursor")
        if not isinstance(raw_cursor, Mapping):
            continue
        # Reuse the same strict shape validator as the durable normalizer by
        # feeding one synthetic request-scoped row through it.
        normalized = _normalize_search_query_cursors({
            request_key: {source: raw_cursor},
        }).get(request_key, {}).get(source)
        if normalized is not None:
            sources[source] = normalized
    if sources:
        if request_key not in value and len(value) >= _MAX_SEARCH_QUERY_CURSOR_SCOPES:
            value.pop(next(iter(value)), None)
        value[request_key] = sources
    state[_SEARCH_QUERY_CURSORS_KEY] = value


def _normalize_reviewed_torrent_misses(
    value: object,
) -> dict[str, list[str]]:
    if not isinstance(value, Mapping):
        return {}
    output: dict[str, list[str]] = {}
    for raw_key, raw_locators in list(value.items())[:_MAX_REVIEWED_TORRENT_MISS_SCOPES]:
        if (
            not isinstance(raw_key, str)
            or _SAFE_SEARCH_REQUEST_KEY.fullmatch(raw_key) is None
            or not isinstance(raw_locators, list)
        ):
            continue
        locators = sorted({
            locator.casefold()
            for locator in raw_locators[:_MAX_REVIEWED_TORRENT_MISSES_PER_SCOPE]
            if isinstance(locator, str)
            and _SAFE_TORRENT_LOCATOR.fullmatch(locator) is not None
        })
        if locators:
            output[raw_key] = locators
    return output


def _known_reviewed_torrent_misses(
    state: Mapping[str, Any], request_key: str | None,
) -> list[str]:
    if request_key is None:
        return []
    return _normalize_reviewed_torrent_misses(
        state.get(_REVIEWED_TORRENT_MISSES_KEY),
    ).get(request_key, [])


def _remember_reviewed_torrent_misses(
    state: dict[str, Any], request_key: str | None, locators: object,
) -> None:
    if request_key is None or not isinstance(locators, list):
        return
    value = _normalize_reviewed_torrent_misses(
        state.get(_REVIEWED_TORRENT_MISSES_KEY),
    )
    accepted = {
        locator.casefold()
        for locator in locators[:_MAX_REVIEWED_TORRENT_MISSES_PER_SCOPE]
        if isinstance(locator, str)
        and _SAFE_TORRENT_LOCATOR.fullmatch(locator) is not None
    }
    if request_key not in value and len(value) >= _MAX_REVIEWED_TORRENT_MISS_SCOPES:
        # Only evict the oldest scope when this round actually contributed
        # evidence: an empty accept set must not throw away persisted
        # "reviewed, no coverage" rows for nothing (the quark tier passes an
        # empty locator set on rounds it does not search torrents).
        if accepted:
            value.pop(next(iter(value)), None)
    if accepted:
        value[request_key] = sorted({
            *value.get(request_key, []), *accepted,
        })[:_MAX_REVIEWED_TORRENT_MISSES_PER_SCOPE]
    state[_REVIEWED_TORRENT_MISSES_KEY] = value


def _normalize_state(raw: Mapping[str, Any]) -> dict[str, Any]:
    state = dict(raw)
    tier = state.get("tier")
    if tier not in STRICT_TIER_ORDER:
        # An existing but malformed state is not equivalent to an absent
        # state.  It might have been torn down just after a provider submit.
        return _state_recovery_blocked("补源状态 tier 无效或缺失")
    state.setdefault("candidate_failures_by_provider", {})
    state.setdefault("exhaustion_proof_by_provider", {})
    state.setdefault("last_error_scope", None)
    state.setdefault("updated_at", _now())
    state.setdefault("last_attempt_at", None)
    state.setdefault("waiting", None)
    state.setdefault("attempt_log", [])
    state.setdefault(_SUBTITLE_INTENTS_KEY, {})
    legacy_in_flight = state.get(_IN_FLIGHT_KEY, {})
    if not isinstance(legacy_in_flight, Mapping):
        return _state_recovery_blocked("视频 in_flight 状态结构无效")
    normalized_legacy_in_flight: dict[str, str | None] = {}
    for key, value in legacy_in_flight.items():
        if (
            not isinstance(key, str)
            or not key
            or len(key) > 512
            or (
                value is not None
                and value != ""
                and _safe_task_id(value) is None
            )
        ):
            return _state_recovery_blocked("视频 in_flight 状态字段无效")
        normalized_legacy_in_flight[key] = (
            _safe_task_id(value) if value else None
        )
    video_intents = _normalize_video_intents(state.get(_VIDEO_INTENTS_KEY))
    if video_intents is None:
        return _state_recovery_blocked("视频补源 attempt 状态无效")
    expected_in_flight = {
        token: _safe_task_id(intent.get("external_task_id"))
        for intent in video_intents.values()
        for token in intent.get("selected_gap_ids") or []
        if isinstance(token, str) and token
    }
    # The former projection alone has no request/selection/staging evidence.
    # Treat any such historical in-flight token as ambiguous instead of
    # letting the new code start a fresh provider attempt around it.
    if (
        not video_intents
        and normalized_legacy_in_flight
    ) or any(
        token not in expected_in_flight
        for token in normalized_legacy_in_flight
    ):
        return _state_recovery_blocked("存在无完整 attempt 证据的视频 in_flight 记录")
    state[_VIDEO_INTENTS_KEY] = video_intents
    state[_IN_FLIGHT_KEY] = expected_in_flight
    log = state.get("attempt_log")
    if not isinstance(log, list):
        state["attempt_log"] = []
    else:
        state["attempt_log"] = [
            dict(item) for item in log if isinstance(item, Mapping)
        ][- _MAX_ATTEMPT_LOG:]
    state[_SUBTITLE_INTENTS_KEY] = _normalize_subtitle_intents(
        state.get(_SUBTITLE_INTENTS_KEY),
    )
    state[_SEARCH_RESOURCE_MISSES_KEY] = _normalize_search_resource_misses(
        state.get(_SEARCH_RESOURCE_MISSES_KEY),
    )
    state[_PANSOU_QUERY_CURSORS_KEY] = _normalize_pansou_query_cursors(
        state.get(_PANSOU_QUERY_CURSORS_KEY),
    )
    state[_SEARCH_QUERY_CURSORS_KEY] = _normalize_search_query_cursors(
        state.get(_SEARCH_QUERY_CURSORS_KEY),
    )
    state[_REVIEWED_TORRENT_MISSES_KEY] = _normalize_reviewed_torrent_misses(
        state.get(_REVIEWED_TORRENT_MISSES_KEY),
    )
    return state


_PUBLIC_ATTEMPT_LIMIT = 50
_PUBLIC_TEXT_CHARS = 200
_PUBLIC_SAMPLE_LIMIT = 3
_PUBLIC_SOURCE_LIMIT = 8


def public_replenishment_tier_state(
    state: Mapping[str, Any],
) -> dict[str, Any]:
    """Bounded web projection of one root's replenishment tier state.

    The dashboard must never couple to the durable state file's raw shape:
    the lane is headed for a refactor, and this projection is the stable
    contract between the two — the same role ``public_work_unit_row`` plays
    for work units.  Every field is whitelisted and bounded; unknown keys
    are dropped, so internal refactors can change the durable shape freely
    as long as this projection keeps resolving.
    """
    attempts = state.get("attempt_log")
    attempt_rows: list[dict[str, Any]] = []
    if isinstance(attempts, list):
        for raw in attempts[-_PUBLIC_ATTEMPT_LIMIT:]:
            if not isinstance(raw, Mapping):
                continue
            attempt_rows.append({
                "gap_id": str(raw.get("gap_id") or ""),
                "tier": str(raw.get("tier") or ""),
                "outcome": str(raw.get("outcome") or ""),
                "error": str(raw.get("error") or "")[:_PUBLIC_TEXT_CHARS],
                "recorded_at": str(raw.get("recorded_at") or ""),
            })
    failures: dict[str, dict[str, Any]] = {}
    raw_failures = state.get("candidate_failures_by_provider")
    if isinstance(raw_failures, Mapping):
        for provider, entries in raw_failures.items():
            if not isinstance(entries, (list, tuple)):
                continue
            failures[str(provider)] = {
                "count": len(entries),
                "samples": [
                    str(item)[:_PUBLIC_TEXT_CHARS]
                    for item in entries[:_PUBLIC_SAMPLE_LIMIT]
                ],
            }
    exhaustion: dict[str, dict[str, Any]] = {}
    raw_exhaustion = state.get("exhaustion_proof_by_provider")
    if isinstance(raw_exhaustion, Mapping):
        for provider, proof in raw_exhaustion.items():
            if not isinstance(proof, Mapping):
                continue
            exhaustion[str(provider)] = {
                "type": str(proof.get("type") or ""),
                "completed_sources": [
                    str(item) for item in (proof.get("completed_sources") or [])
                    if isinstance(item, (str, int))
                ][:_PUBLIC_SOURCE_LIMIT],
            }
    return {
        "tier": str(state.get("tier") or ""),
        "waiting": str(state.get("waiting") or ""),
        "last_error_scope": str(state.get("last_error_scope") or ""),
        "updated_at": str(state.get("updated_at") or ""),
        "last_attempt_at": str(state.get("last_attempt_at") or ""),
        "attempt_count": len(attempts) if isinstance(attempts, list) else 0,
        "attempt_log": attempt_rows,
        "candidate_failures_by_provider": failures,
        "exhaustion_proof_by_provider": exhaustion,
    }


def load_root_replenishment_state(
    state_root: Path, root_task_id: str,
) -> dict[str, Any]:
    """Load the durable tier/orchestration state for one root task.

    Only a *missing* file yields a fresh ``quark_share`` state.  An unreadable
    or malformed existing file is a recovery barrier: it may have been torn
    down during a provider submission, so treating it as a new task would
    authorize a duplicate download/share-save.
    """
    state_root = Path(state_root)
    path = _state_path(state_root, root_task_id)
    if not path.exists():
        return _fresh_state()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError):
        return _state_recovery_blocked("补源状态文件不可读")
    if not isinstance(raw, Mapping):
        return _state_recovery_blocked("补源状态文件不是对象")
    raw_tier = str(raw.get("tier") or "").strip().casefold()
    if raw_tier in {"alist_offline", "legacy_alist_offline_blocked"}:
        return _state_attention("此 RootJob 保留了已移除的 AList 离线下载记录")
    return _normalize_state(raw)



def _shrink_state_to_caps(state: Mapping[str, Any]) -> dict[str, Any] | None:
    """Trim the documented miss-caches until the state fits the mapping cap.

    A healthy state may legitimately exceed the 256 KiB durable-mapping cap:
    the miss-caches are bounded at 64 scopes x 512 locators of up to ~128
    characters each.  Rather than failing the save (which wedged the lane
    with a ``submitting`` intent and orphaned provider tasks), drop the
    caches entirely — they are advisory "already reviewed, no hit" evidence,
    never correctness state — and retry the projection.  Returns ``None``
    when even a cache-free state cannot be represented.
    """
    try:
        trimmed = dict(state)
        trimmed.pop(_REVIEWED_TORRENT_MISSES_KEY, None)
        trimmed.pop(_SEARCH_RESOURCE_MISSES_KEY, None)
        trimmed.pop(_PANSOU_QUERY_CURSORS_KEY, None)
        trimmed.pop(_SEARCH_QUERY_CURSORS_KEY, None)
        return trimmed
    except Exception:
        return None


def save_root_replenishment_state(
    state_root: Path, root_task_id: str, state: Mapping[str, Any],
) -> None:
    """Persist the durable tier/orchestration state atomically."""
    # Every state writer goes through this last JSON projection.  Normal paths
    # already build non-secret rows, but this prevents an exception/debug
    # payload from bypassing the durable redaction boundary in a future call
    # site.  A value that cannot be represented safely is a local failure,
    # never a reason to write a partial replacement state.
    projected, _safe = _durable_mapping(state)
    if projected is None:
        # The miss-caches alone are bounded at 64 scopes x 512 locators of
        # up to ~128 chars each (several MB), so a healthy state can exceed
        # the historical 256 KiB mapping cap.  Shrink to the documented
        # per-scope caps before failing: dropping the OLDEST evidence rows
        # is strictly better than wedging the replenishment lane forever.
        trimmed = _shrink_state_to_caps(state)
        if trimmed is None:
            raise ValueError("补源状态无法安全持久化")
        projected, _safe = _durable_mapping(trimmed)
        if projected is None:
            raise ValueError("补源状态无法安全持久化")
    atomic_write_json(
        _state_path(Path(state_root), root_task_id),
        projected,
        allow_nan=False,
    )


def _default_materializer_factory(
    tier: str,
    *,
    archive_preprocessor: object | None = None,
) -> Any:
    """Map one tier to its real per-tier materializer class instance."""
    if tier == TIER_QUARK_SHARE:
        from .provider_materializers import QuarkFastSaveMaterializer
        return QuarkFastSaveMaterializer()
    if tier == TIER_LOCAL_MAGNET:
        from .provider_materializers import LocalTorrentMaterializer
        return LocalTorrentMaterializer(
            archive_preprocessor=archive_preprocessor,
        )
    raise ValueError(f"补源 tier 无效: {tier!r}")


def _exception_chain(error: BaseException):
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _safe_task_id(value: object) -> str | None:
    if isinstance(value, str) and _SAFE_DURABLE_TASK_ID.fullmatch(value):
        return value
    return None


def _classify_error(error: Exception) -> tuple[str, str | None]:
    """Map a materializer/search exception onto a pure failure scope.

    The materializers signal their own scope through exception attributes
    (``failure_scope`` / ``exclude_candidate``) and an in-doubt submit through
    ``external_task_id``/``task_id``.  Anything unclassified is conservative
    ``infrastructure`` (same tier, ``retry_wait``) so a surprising failure can
    never silently advance or downgrade the tier.
    """
    for item in _exception_chain(error):
        scope = getattr(item, "failure_scope", None)
        if isinstance(scope, str):
            normalized = scope.strip().casefold()
            if normalized in _KNOWN_FAILURE_SCOPES:
                if normalized == FAILURE_IN_DOUBT:
                    return normalized, _safe_task_id(
                        getattr(item, "external_task_id", None)
                        or getattr(item, "task_id", None)
                    )
                return normalized, None
        if getattr(item, "exclude_candidate", False) is True:
            return FAILURE_CANDIDATE, None
        task_id = _safe_task_id(
            getattr(item, "external_task_id", None)
            or getattr(item, "task_id", None)
        )
        if task_id is not None:
            return FAILURE_IN_DOUBT, task_id
    return FAILURE_INFRASTRUCTURE, None


def _is_pause_error(error: BaseException) -> bool:
    """Recognize the lower provider boundary's resumable stop signal."""
    return any(
        getattr(item, "pause_requested", False) is True
        for item in _exception_chain(error)
    )


def _call_materializer_with_pause(
    method: Callable[..., object],
    *args: object,
    pause_requested: Callable[[], bool] | None,
    **kwargs: object,
) -> object:
    """Never silently call a scoped provider boundary without its fence."""
    if pause_requested is None:
        return method(*args, **kwargs)
    try:
        parameters = inspect.signature(method).parameters.values()
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "补源 materializer 无法证明支持 pause_requested，已安全停止",
        ) from exc
    accepts_pause = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        or parameter.name == "pause_requested"
        for parameter in parameters
    )
    if not accepts_pause:
        raise RuntimeError(
            "补源 materializer 不支持 pause_requested，拒绝在 RootJob 试运行范围执行",
        )
    return method(*args, pause_requested=pause_requested, **kwargs)


def _episode_token(season: int, episodes: object) -> str | None:
    ordered = sorted({
        int(item) for item in episodes
        if isinstance(item, int) and not isinstance(item, bool) and item > 0
    })
    if not ordered:
        return None
    if len(ordered) == 1:
        return f"S{season:02d}E{ordered[0]:02d}"
    return f"S{season:02d}E{ordered[0]:02d}-E{ordered[-1]:02d}"


def _bridge_token(gap: Gap) -> str | None:
    """Render the bridge token for one ledger gap (mirrors the bridge)."""
    if gap.kind == "missing_episode":
        if isinstance(gap.season, int) and not isinstance(gap.season, bool):
            return _episode_token(gap.season, gap.episodes)
        return None
    if gap.kind == "missing_season":
        if isinstance(gap.season, int) and not isinstance(gap.season, bool):
            return f"S{gap.season:02d}"
        return None
    if gap.kind in {"missing_media", "missing_subtitle"}:
        return gap.gap_id
    return None


def _open_gaps_by_token(
    state_root: Path,
    root_task_id: str,
    media_type: str,
    tmdb_id: int,
) -> tuple[dict[str, list[Gap]], dict[str, list[Gap]]]:
    """Index open ledger gaps by bridge token and by work unit.

    Returns ``(by_token, by_unit)``.  ``by_token`` maps the bridge's gap token
    (``SxxEyy``, ``Sxx``, or the media/subtitle ``gap_id``) back to the open
    ``Gap`` rows, which carry the authoritative ledger ``gap_id``.
    """
    by_token: dict[str, list[Gap]] = {}
    by_unit: dict[str, list[Gap]] = {}
    for gap in load_gap_ledger(state_root, root_task_id):
        if gap.status != "open":
            continue
        if (
            str(gap.media_type).strip().casefold() != media_type.casefold()
            or gap.tmdb_id != tmdb_id
        ):
            continue
        token = _bridge_token(gap)
        if token is not None:
            by_token.setdefault(token, []).append(gap)
        by_unit.setdefault(gap.work_unit_id, []).append(gap)
    return by_token, by_unit


def _required_episode_coordinates(
    gap: Gap, by_unit: Mapping[str, list[Gap]],
) -> set[tuple[int, int]]:
    if gap.kind == "missing_episode":
        if isinstance(gap.season, int) and not isinstance(gap.season, bool):
            return {
                (gap.season, int(ep)) for ep in gap.episodes
                if isinstance(ep, int) and not isinstance(ep, bool) and ep > 0
            }
        return set()
    if gap.kind == "missing_season":
        if not isinstance(gap.season, int) or isinstance(gap.season, bool):
            return set()
        coords = {
            (gap.season, int(ep)) for ep in gap.episodes
            if isinstance(ep, int) and not isinstance(ep, bool) and ep > 0
        }
        for other in by_unit.get(gap.work_unit_id, ()):
            if (
                other.kind == "missing_episode"
                and other.status == "open"
                and other.season == gap.season
            ):
                coords |= {
                    (other.season, int(ep)) for ep in other.episodes
                    if isinstance(ep, int) and not isinstance(ep, bool) and ep > 0
                }
        return coords
    return set()


def _path_under(path: str, root: str) -> bool:
    if not path or not root:
        return False
    normalized = posixpath.normpath(path)
    root_norm = root.rstrip("/")
    return normalized == root_norm or normalized.startswith(root_norm + "/")


def _prove_gap_coverage(
    gap: Gap,
    by_unit: Mapping[str, list[Gap]],
    executed_plan: Mapping[str, Any],
) -> bool:
    """Prove one gap is covered by the executed child plan's files.

    The executed plan is the single durable proof: episode/season coverage
    comes from ``files[].final_name`` via ``audit_episode_tokens``; media needs
    one video executed under the plan target; subtitle needs a subtitle file
    executed next to a video (same target directory).
    """
    files = executed_plan.get("files") or []
    target_root = str(executed_plan.get("target_root") or "")
    rows = [item for item in files if isinstance(item, Mapping)]

    if gap.kind == "missing_media":
        return any(
            item.get("media_kind") == "video"
            and _path_under(str(item.get("target_dir") or ""), target_root)
            for item in rows
        )

    if gap.kind == "missing_subtitle":
        video_dirs = {
            str(item.get("target_dir") or "")
            for item in rows
            if item.get("media_kind") == "video"
        }
        return any(
            item.get("media_kind") == "subtitle"
            and str(item.get("target_dir") or "") in video_dirs
            for item in rows
        )

    required = _required_episode_coordinates(gap, by_unit)
    if not required:
        return False
    actual: set[tuple[int, int]] = set()
    for item in rows:
        if item.get("media_kind") != "video":
            continue
        actual |= audit_episode_tokens(str(item.get("final_name") or ""))
    return required <= actual


def _attempt_status(scope: str) -> str:
    if scope == FAILURE_IN_DOUBT:
        return "in_doubt"
    if scope == FAILURE_INFRASTRUCTURE:
        return "infrastructure"
    return "candidate_failed"


def _work_parent(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    request: Mapping[str, Any],
) -> str:
    """Derive the child plan's ``parent_path`` from the work unit's library root.

    The planner writes back onto the already-audited work root when given its
    parent, exactly like the merge lane.  Best-effort: an unknown root falls
    back to the confirmed shelf root (the planner then derives the title dir).
    """
    media = request.get("media") or {}
    media_type = str(media.get("media_type") or "").strip().casefold()
    tmdb_id = media.get("tmdb_id")
    for record in load_work_unit_records(state_root, root_task_id):
        identity = record.identity or {}
        if (
            str(identity.get("media_type") or "").strip().casefold() != media_type
            or identity.get("tmdb_id") != tmdb_id
        ):
            continue
        work_root = record.matched_work_root
        if not work_root and record.writer_job_id:
            try:
                job = runner.get_job(record.writer_job_id)
                plan = job.plan if isinstance(job.plan, Mapping) else {}
                work_root = str(plan.get("target_root") or "") or None
            except Exception:
                work_root = None
        if work_root:
            return posixpath.dirname(str(work_root).rstrip("/"))
    try:
        root_job = runner.get_job(root_task_id)
        if root_job.target_shelf is not None:
            return target_root_for_shelf(runner.library_root, root_job.target_shelf)
    except Exception:
        pass
    return str(runner.library_root).rstrip("/") or "/"


def _request_season(request: Mapping[str, Any]) -> int | None:
    for row in request.get("gaps") or []:
        if not isinstance(row, Mapping):
            continue
        if row.get("kind") in {"missing_episode", "missing_season"}:
            season = row.get("season")
            if isinstance(season, int) and not isinstance(season, bool) and season >= 0:
                return season
    return None


def _child_request(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    request: Mapping[str, Any],
    intent: Mapping[str, Any],
    delivery: Mapping[str, Any],
) -> EngineRequest:
    media = request.get("media") or {}
    media_type = str(media.get("media_type") or "movie")
    tmdb_id = media.get("tmdb_id")
    if (
        isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        raise ValueError("补源请求缺少有效 tmdb_id")
    staging_root = _video_child_source_root(intent, delivery)
    # The provider delivery has already been fresh-listed and isolated to
    # ``staging_root``.  Carry that exact manifest into the internal Engine
    # request so the normal Planner sees only the selected gap members; it
    # must not recursively walk the broader attempt staging directory or
    # accidentally ingest a subtitle/bonus sibling.  These fields are
    # internal-only and are accepted by ``from_persisted_mapping`` during
    # execute/recovery, never from an HTTP request.
    delivered_files = delivery.get("files")
    if not isinstance(delivered_files, list) or not delivered_files:
        raise ValueError("视频补源 delivery 缺少媒体文件清单")
    source_files: list[dict[str, Any]] = []
    for row in delivered_files:
        if not isinstance(row, Mapping):
            raise ValueError("视频补源 delivery 文件清单无效")
        path = _bounded_path(row.get("path"))
        size = row.get("size")
        if (
            path is None
            or posixpath.dirname(path) != staging_root
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size <= 0
        ):
            raise ValueError("视频补源 delivery 文件未通过隔离清单核验")
        source_files.append({
            "full_path": path,
            "name": posixpath.basename(path),
            "size": size,
            "is_dir": False,
        })
    payload: dict[str, Any] = {
        "source_path": staging_root,
        "parent_path": _work_parent(runner, state_root, root_task_id, request),
        "media_type": media_type,
        "tmdb_id": tmdb_id,
    }
    season = _request_season(request)
    if season is not None:
        payload["season"] = season
    base = EngineRequest.from_mapping(payload)
    return replace(
        base,
        source_files=tuple(source_files),
        source_scope_paths=(staging_root,),
    )


def _strip_video_companion_subtitle_members(
    selection: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a video-only RootJob selection without mutating durable state.

    Older Torrent candidate builders could attach a subtitle index as a
    ``companion_subtitle_index_by_media_gap`` entry.  The local adapter turns
    that map directly into aria2 ``--select-file`` arguments.  RootJob owns
    the only automatic subtitle transaction now (including the strict merged
    bilingual proof), so a video materializer must never receive that map.
    Keep the original selection immutable for recovery and copy
    just the two shallow layers that carry it.
    """
    output = dict(selection)
    acquisition = selection.get("acquisition")
    if not isinstance(acquisition, Mapping):
        return output
    cleaned = dict(acquisition)
    cleaned.pop("companion_subtitle_index_by_media_gap", None)
    output["acquisition"] = cleaned
    return output


def _append_attempt_log(
    state: dict[str, Any],
    entry: Mapping[str, Any],
) -> None:
    log = state.setdefault("attempt_log", [])
    if not isinstance(log, list):
        log = []
        state["attempt_log"] = log
    log.append(dict(entry))
    if len(log) > _MAX_ATTEMPT_LOG:
        del log[:- _MAX_ATTEMPT_LOG]


def _root_target_shelf(runner: Any, root_task_id: str) -> str | None:
    """Return the root's declared shelf without trusting an arbitrary value."""
    try:
        shelf = runner.get_job(root_task_id).target_shelf
    except Exception:
        return None
    return shelf if shelf in {"movie", "anime", "us_tv"} else None


def _search_evidence_completion(
    tier: str,
    evidence: Mapping[str, Any] | None,
    *,
    shelf: str | None,
) -> tuple[bool, list[str], list[str]]:
    """Validate one raw-search proof for this tier and return its sources.

    ``gap_ledger_selection`` carries this evidence from the original search
    response.  Do not turn an empty selector result into a proof here: every
    identity request must independently show all required sources for the
    active tier and zero unchecked candidates.  Telemetry outside that
    required-source set is diagnostic only and cannot make this tier fail or
    succeed.
    """
    if not isinstance(evidence, Mapping):
        return False, [], []
    if evidence.get("scope") != FAILURE_CANDIDATE:
        return False, [], []
    if evidence.get("search_complete_no_candidates") is not True:
        return False, [], []
    unchecked = evidence.get("unchecked_secondary_candidates")
    if (
        not isinstance(unchecked, int)
        or isinstance(unchecked, bool)
        or unchecked != 0
    ):
        return False, [], []
    required = required_sources_for_tier(tier, shelf)

    if tier == TIER_LOCAL_MAGNET:
        # Magnet proof is configuration-scoped, not a static promise that all
        # seven possible anime indexes are enabled.  The adapter emits every
        # canonical source row (including disabled rows); requiring the full
        # row set here prevents a hand-written/partial telemetry object from
        # hiding an unexamined configured source.
        telemetry = evidence.get("source_telemetry")
        if not isinstance(telemetry, Mapping) or not required:
            return False, [], []
        configuration_rows: list[bool] = []
        for source in required:
            raw = telemetry.get(source)
            if not isinstance(raw, Mapping):
                return False, [], []
            configured = raw.get("configured")
            if configured is not None and not isinstance(configured, bool):
                return False, [], []
            if isinstance(configured, bool):
                configuration_rows.append(configured)

        # The current adapter supplies ``configured`` for every canonical
        # source.  A small all-or-nothing compatibility branch accepts an old
        # adapter only when it reports *every* canonical source exhausted and
        # healthy; a partial or mixed legacy shape cannot weaken the proof.
        if not configuration_rows:
            for source in required:
                raw = telemetry[source]
                failures = raw.get("infrastructure_failures")
                if (
                    raw.get("source_exhausted") is not True
                    or not isinstance(failures, int)
                    or isinstance(failures, bool)
                    or failures != 0
                ):
                    return False, [], []
            completed = sorted(required)
            return True, completed, completed
        if len(configuration_rows) != len(required):
            return False, [], []

        configured: set[str] = set()
        for source in required:
            raw = telemetry.get(source)
            if raw.get("configured") is not True:
                continue
            configured.add(source)
            failures = raw.get("infrastructure_failures")
            attempts = raw.get("query_attempts")
            responses = raw.get("query_responses")
            if (
                raw.get("source_exhausted") is not True
                or not isinstance(failures, int)
                or isinstance(failures, bool)
                or failures != 0
                or not isinstance(attempts, int)
                or isinstance(attempts, bool)
                or attempts <= 0
                or not isinstance(responses, int)
                or isinstance(responses, bool)
                or responses <= 0
            ):
                return False, [], []
        if not configured:
            return False, [], []
        completed = sorted(configured)
        return True, completed, completed

    completed = {
        str(value).strip().casefold()
        for value in (evidence.get("completed_sources") or [])
        if isinstance(value, str) and value.strip()
    } & set(required)
    telemetry = evidence.get("source_telemetry")
    if isinstance(telemetry, Mapping):
        for source in required:
            raw = telemetry.get(source)
            if not isinstance(raw, Mapping):
                continue
            failures = raw.get("infrastructure_failures")
            if (
                isinstance(failures, int)
                and not isinstance(failures, bool)
                and failures > 0
            ):
                return False, [], []
            if raw.get("source_exhausted") is True:
                completed.add(source)
    if not required.issubset(completed):
        return False, [], []
    return True, sorted(completed), []


def _search_evidence_reviewed_resource_misses(
    tier: str,
    evidence: Mapping[str, Any] | None,
) -> list[str]:
    """Return only canonical, current-tier read-only share miss facts."""
    if tier != TIER_QUARK_SHARE or not isinstance(evidence, Mapping):
        return []
    telemetry = evidence.get("source_telemetry")
    if not isinstance(telemetry, Mapping):
        return []
    rows = telemetry.get("pansou")
    if not isinstance(rows, Mapping):
        return []
    values = rows.get("reviewed_resource_miss_locators")
    if not isinstance(values, list):
        return []
    return sorted({
        locator
        for locator in values[:_MAX_SEARCH_RESOURCE_MISSES_PER_SCOPE]
        if isinstance(locator, str)
        and _SAFE_QUARK_SHARE_LOCATOR.fullmatch(locator) is not None
    })


def _search_evidence_reviewed_torrent_misses(
    evidence: Mapping[str, Any] | None,
) -> list[str]:
    """Extract only validated non-covering torrent infohash receipts."""
    if not isinstance(evidence, Mapping):
        return []
    telemetry = evidence.get("source_telemetry")
    if not isinstance(telemetry, Mapping):
        return []
    output: set[str] = set()
    for raw in telemetry.values():
        if not isinstance(raw, Mapping):
            continue
        values = raw.get("reviewed_torrent_miss_locators")
        if not isinstance(values, list):
            continue
        output.update({
            locator.casefold()
            for locator in values[:_MAX_REVIEWED_TORRENT_MISSES_PER_SCOPE]
            if isinstance(locator, str)
            and _SAFE_TORRENT_LOCATOR.fullmatch(locator) is not None
        })
    return sorted(output)[:_MAX_REVIEWED_TORRENT_MISSES_PER_SCOPE]


def _search_evidence_is_clean_incomplete(
    tier: str,
    evidence: Mapping[str, Any] | None,
    *,
    shelf: str | None,
) -> bool:
    """Recognize a bounded-but-healthy search without calling it infra.

    Unknown/malformed telemetry remains a same-tier infrastructure retry.  We
    only use the more specific candidate-discovery status when every source
    required for the active tier explicitly says it is configured, healthy and
    incomplete (for example because a share-inspection cap/deadline was hit).
    """
    if not isinstance(evidence, Mapping) or evidence.get("scope") != FAILURE_CANDIDATE:
        return False
    telemetry = evidence.get("source_telemetry")
    if not isinstance(telemetry, Mapping):
        return False
    required = required_sources_for_tier(tier, shelf)
    if not required:
        return False
    for source in required:
        raw = telemetry.get(source)
        if (
            not isinstance(raw, Mapping)
            or raw.get("configured") is not True
            or raw.get("status") != "incomplete"
            or not isinstance(raw.get("infrastructure_failures"), int)
            or isinstance(raw.get("infrastructure_failures"), bool)
            or raw.get("infrastructure_failures") != 0
        ):
            return False
    return True


def _incomplete_search_reason(evidence: Mapping[str, Any] | None) -> str:
    """Describe an incomplete search without persisting provider text.

    Search responses may contain opaque share URLs or provider-provided error
    strings.  The bridge retains only closed telemetry fields, so use those
    facts to make a same-tier retry visible in both the state and per-gap
    attempt ledger without leaking credentials.
    """
    if not isinstance(evidence, Mapping):
        return "补源搜索未提供可验证的完成证明；保持当前层等待重试"
    telemetry = evidence.get("source_telemetry")
    if isinstance(telemetry, Mapping):
        disabled = sorted(
            str(source)
            for source, raw in telemetry.items()
            if isinstance(source, str)
            and isinstance(raw, Mapping)
            and raw.get("configured") is False
        )
        if disabled:
            return f"{', '.join(disabled[:3])} 发现器未配置或已禁用；保持当前层等待重试"
        unavailable = sorted(
            str(source)
            for source, raw in telemetry.items()
            if isinstance(source, str)
            and isinstance(raw, Mapping)
            and isinstance(raw.get("infrastructure_failures"), int)
            and not isinstance(raw.get("infrastructure_failures"), bool)
            and raw.get("infrastructure_failures", 0) > 0
        )
        if unavailable:
            return f"{', '.join(unavailable[:3])} 搜索基础设施不可用；保持当前层等待重试"
        bounded = sorted(
            str(source)
            for source, raw in telemetry.items()
            if isinstance(source, str)
            and isinstance(raw, Mapping)
            and raw.get("configured") is True
            and raw.get("status") == "incomplete"
            and raw.get("infrastructure_failures") == 0
        )
        if bounded:
            return f"{', '.join(bounded[:3])} 只读分享检查尚未穷尽；保持当前层等待重试"
    return "补源搜索未提供可验证的完成证明；保持当前层等待重试"


def _noop_result(state: dict[str, Any], tier: str) -> dict[str, Any]:
    return {
        "tier": tier,
        "tier_before": tier,
        "requests_built": 0,
        "attempts": [],
        "gaps_closed": [],
        "state": state,
        "waiting": None,
    }


def _enrich_media_titles(runner: Any, request: dict[str, Any]) -> None:
    """Enrich bridge aliases and a trustworthy original language from TMDB.

    An operator-confirmed unit identity may carry only ``media_type +
    tmdb_id`` (no title), which would leave the bridge with empty aliases and
    the selector would reject every candidate.  The TMDB client is the
    canonical title source.  The subtitle path also consumes
    ``original_language`` but only when this exact read-only TMDB detail
    response proves it; request/identity/provider values are deliberately
    discarded first so stale metadata cannot manufacture a bilingual track.
    """
    media = request.get("media")
    if not isinstance(media, dict):
        return
    media.pop("original_language", None)
    media.pop("original_language_verified_by_tmdb", None)
    media_type = str(media.get("media_type") or "").strip().casefold()
    tmdb_id = media.get("tmdb_id")
    if (
        media_type not in {"movie", "tv"}
        or isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        return
    tmdb = getattr(runner, "tmdb", None)
    getter = getattr(tmdb, "get", None)
    if not callable(getter):
        return
    try:
        details = getter(f"/{media_type}/{tmdb_id}")
    except Exception:
        return
    if not isinstance(details, Mapping):
        return
    title = (
        str(details.get("name") or "").strip()
        if media_type == "tv"
        else str(details.get("title") or "").strip()
    )
    original_title = str(
        details.get("original_name") or details.get("original_title") or ""
    ).strip()
    if title:
        media["title"] = title
        if original_title:
            media["original_title"] = original_title
        media["aliases"] = _nonempty_strings([
            title,
            original_title,
            *(media.get("aliases") if isinstance(media.get("aliases"), list) else []),
        ])

    # A Chinese-original title cannot form the requested 简中 + 原语 pair;
    # unknown / unsupported values are not preserved as a weak hint.  The
    # boolean remains bound to this request's concrete TMDB id and is checked
    # again immediately before any merged file can be installed.
    original_language = normalize_subtitle_language(
        details.get("original_language"),
    )
    if original_language in _SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES:
        media["original_language"] = original_language
        media["original_language_verified_by_tmdb"] = True


def _subtitle_target_marker(
    language: object,
    *,
    bilingual: bool,
    original_language: object = None,
) -> str | None:
    """Build the one formal marker for a Chinese subtitle gap.

    The bilingual marker is deliberately part of the *same filename*.  It is
    durable state for recovery code that this one Chinese-named
    sidecar was merged against a particular TMDB-proven original language; it
    never denotes another file to write.
    """
    normalized_language = normalize_subtitle_language(language)
    if normalized_language not in {"simplified_chinese", "traditional_chinese"}:
        return None
    marker = "zh-CN" if normalized_language == "simplified_chinese" else "zh-TW"
    if not bilingual:
        return marker
    codes = {"japanese": "ja", "english": "en", "korean": "ko"}
    code = codes.get(normalize_subtitle_language(original_language))
    if code is None:
        return None
    return f"{marker}-bilingual-{code}"


def _subtitle_target_path(
    gap: Mapping[str, Any],
    source_path: str,
    *,
    bilingual: bool,
    original_language: object = None,
    subtitle_language: object = None,
) -> str | None:
    """Derive a single non-overlapping formal target for one subtitle gap."""
    video_path = gap.get("path")
    if not isinstance(video_path, str) or not video_path.startswith("/"):
        return None
    suffix = posixpath.splitext(source_path)[1].casefold()
    if suffix not in _SUBTITLE_TEXT_EXTENSIONS:
        return None
    marker = _subtitle_target_marker(
        subtitle_language if subtitle_language is not None
        else gap.get("subtitle_language"),
        bilingual=bilingual,
        original_language=original_language,
    )
    if marker is None:
        return None
    return f"{posixpath.splitext(video_path)[0]}.{marker}{suffix}"


def _verified_original_language(request: Mapping[str, Any]) -> str | None:
    """Return only a value tied to a fresh TMDB detail response."""
    media = request.get("media")
    if not isinstance(media, Mapping):
        return None
    tmdb_id = media.get("tmdb_id")
    if (
        media.get("original_language_verified_by_tmdb") is not True
        or isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        return None
    language = normalize_subtitle_language(media.get("original_language"))
    return language if language in _SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES else None


def _task_subtitle_staging_root(
    runner: Any, root_task_id: str, attempt_id: str,
) -> str:
    return (
        f"{str(runner.library_root).rstrip('/')}"
        f"{_STAGING_NAMESPACE}/{root_task_id}/{attempt_id}"
    )


def _is_task_subtitle_staging_path(
    value: object,
    staging_root: str,
) -> bool:
    if not isinstance(value, str) or not value.startswith("/"):
        return False
    normalized = posixpath.normpath(value)
    root = posixpath.normpath(staging_root)
    return normalized.startswith(root.rstrip("/") + "/")


def _fresh_file_info(alist: Any, path: str) -> Mapping[str, Any] | None:
    """Read one exact remote object, with a refreshed-parent fallback."""
    exact = getattr(alist, "exact_file_info", None)
    if callable(exact):
        try:
            row = exact(path)
        except Exception:
            row = None
        if isinstance(row, Mapping) and row.get("is_dir") is not True:
            return row
    listing = getattr(alist, "list", None)
    if not callable(listing):
        return None
    parent = posixpath.dirname(path) or "/"
    name = posixpath.basename(path)
    try:
        try:
            rows = listing(parent, refresh=True)
        except TypeError:
            rows = listing(parent)
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    matches = [
        row for row in rows
        if isinstance(row, Mapping)
        and row.get("name") == name
        and row.get("is_dir") is not True
    ]
    return matches[0] if len(matches) == 1 else None


def _file_size(row: Mapping[str, Any] | None) -> int | None:
    if not isinstance(row, Mapping):
        return None
    value = row.get("size")
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _read_subtitle_bytes(alist: Any, path: str, *, max_bytes: int) -> bytes | None:
    """Bound a content read used for pre/post writer proof."""
    reader = getattr(alist, "read_file_prefix", None)
    if not callable(reader):
        reader = getattr(alist, "read_file_bytes", None)
    if not callable(reader):
        return None
    try:
        try:
            raw = reader(path, max_bytes=max_bytes)
        except TypeError:
            raw = reader(path, max_bytes)
    except Exception:
        return None
    if not isinstance(raw, (bytes, bytearray)):
        return None
    return bytes(raw)


def _validate_root_subtitle_content(
    runner: Any,
    source_path: str,
    *,
    expected_size: int,
    bilingual: bool,
    original_language: str | None,
    subtitle_language: str = "zh",
) -> dict[str, Any]:
    """Validate the full, freshly observed object that will be written.

    A prefix alone is insufficient: a provider could append another
    track or payload after the parsed bytes.  The exact remote size must equal
    the persisted delivery size and the bounded read must contain all of it.
    """
    if (
        isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or not (0 < expected_size <= MAX_MERGED_SUBTITLE_BYTES)
        or _file_size(_fresh_file_info(runner.alist, source_path)) != expected_size
    ):
        return {"status": "unknown", "reason": "subtitle_size_unproven"}
    raw = _read_subtitle_bytes(
        runner.alist, source_path, max_bytes=expected_size,
    )
    if raw is None or len(raw) != expected_size:
        return {"status": "unknown", "reason": "subtitle_full_read_unproven"}
    target_language = normalize_subtitle_language(subtitle_language)
    if target_language not in {"simplified_chinese", "traditional_chinese"}:
        return {"status": "unknown", "reason": "unsupported_subtitle_language"}
    if bilingual:
        if original_language not in _SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES:
            return {"status": "unknown", "reason": "unsupported_original_language"}
        verdict = dict(classify_bilingual_subtitle_content(
            raw, original_language, max_bytes=expected_size,
        ))
        if (
            _subtitle_content_is_satisfied(verdict)
            and verdict.get("chinese_language") != target_language
        ):
            return {"status": "unknown", "reason": "bilingual_chinese_lane_mismatch"}
        return verdict
    # ``raw`` above is a complete, exact-size read.  Preserve that result in
    # the classifier instead of silently falling back to a bounded prefix
    # check (which could otherwise accept a valid beginning followed by
    # unrelated bytes).
    return dict(classify_subtitle_content(
        raw, target_language, max_bytes=expected_size, require_each_cue=True,
    ))


def _subtitle_content_is_satisfied(result: Mapping[str, Any] | object) -> bool:
    return (
        isinstance(result, Mapping)
        and str(result.get("status") or "").strip().casefold() == "satisfied"
    )


def _call_subtitle_installer(
    installer: Callable[..., object],
    source_path: str,
    target_path: str,
    *,
    expected_size: int,
    video_path: str,
    subtitle_language: str,
    validator: Callable[..., object],
    pause_requested: Callable[[], bool] | None,
) -> object:
    """Invoke the only writer without silently dropping its pause fence."""
    try:
        parameters = inspect.signature(installer).parameters.values()
    except (TypeError, ValueError) as exc:
        raise RuntimeError("字幕 writer 无法证明支持受控调用") from exc
    names = {parameter.name for parameter in parameters}
    accepts_kwargs = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters
    )
    if pause_requested is not None and not (accepts_kwargs or "pause_requested" in names):
        raise RuntimeError("字幕 writer 不支持 pause_requested，已安全停止")
    # The direct pre/post validation below is necessary but is not a reason to
    # bypass the writer's own validator for a merged file.
    if not (accepts_kwargs or "subtitle_validator" in names):
        raise RuntimeError("字幕 writer 不支持内容验证回调，拒绝写入")
    kwargs: dict[str, object] = {
        "expected_size": expected_size,
        "video_path": video_path,
        # Preserve the writer's historical public language alias for SC;
        # ``normalize_subtitle_language`` makes the TC alias equally strict.
        "subtitle_language": (
            "zh" if subtitle_language == "simplified_chinese" else "zh-Hant"
        ),
        "subtitle_validator": validator,
    }
    if pause_requested is not None:
        kwargs["pause_requested"] = pause_requested
    return installer(source_path, target_path, **kwargs)


def _empty_subtitle_result() -> dict[str, Any]:
    return {
        "subtitle_requests_built": 0,
        "subtitle_attempts": [],
        "subtitle_gaps_closed": [],
        "subtitle_waiting": None,
        "paused": False,
    }


def _subtitle_request_rows(request: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Return only formal Simplified-Chinese subtitle gaps for one request."""
    rows: dict[str, dict[str, Any]] = {}
    for raw in request.get("gaps") or []:
        if not isinstance(raw, Mapping) or raw.get("kind") != "missing_subtitle":
            continue
        gap_id = raw.get("id")
        if (
            not isinstance(gap_id, str)
            or not gap_id
            or normalize_subtitle_language(raw.get("subtitle_language"))
            != "simplified_chinese"
            or not isinstance(raw.get("path"), str)
            or not str(raw.get("path")).startswith("/")
        ):
            continue
        rows[gap_id] = dict(raw)
    return rows


def _extract_subtitle_delivery(
    delivery: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    staging_root: str,
) -> dict[str, dict[str, Any]]:
    """Validate provider delivery metadata without trusting a second track.

    The provider must emit one row bound to one subtitle gap.  A merged row
    carries an explicit ``bilingual`` flag and the exact TMDB-verified original
    language; Chinese-only is the safe fallback.  ``original_optional`` or a
    second row for the same gap is rejected rather than quietly becoming a
    second formal sidecar.
    """
    rows = delivery.get("files")
    if not isinstance(rows, list):
        raise ValueError("字幕 provider delivery 缺少 files 列表")
    gaps = _subtitle_request_rows(request)
    output: dict[str, dict[str, Any]] = {}
    global_bilingual = delivery.get("bilingual") is True
    global_original = normalize_subtitle_language(delivery.get("original_language"))
    verified_original = _verified_original_language(request)
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise ValueError("字幕 provider files 项无效")
        if raw.get("kind") not in {None, "subtitle"}:
            raise ValueError("字幕 provider 返回了非字幕成员")
        role = str(raw.get("subtitle_role") or "").strip().casefold()
        if role in {"original", "original_optional", "secondary"}:
            raise ValueError("字幕 provider 试图交付独立原语轨")
        gap_ids = raw.get("gap_ids")
        if not isinstance(gap_ids, list) or len(gap_ids) != 1:
            raise ValueError("字幕 provider 未将文件精确绑定唯一 gap")
        gap_id = gap_ids[0]
        if not isinstance(gap_id, str) or gap_id not in gaps or gap_id in output:
            raise ValueError("字幕 provider 交付了未知或重复 gap")
        source = raw.get("path")
        size = raw.get("size")
        if (
            not _is_task_subtitle_staging_path(source, staging_root)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size <= 0
            or size > MAX_MERGED_SUBTITLE_BYTES
        ):
            raise ValueError("字幕 provider 交付未绑定有效任务 staging 文件")
        suffix = posixpath.splitext(str(source))[1].casefold()
        if suffix not in _SUBTITLE_TEXT_EXTENSIONS:
            raise ValueError("双语字幕仅接受可解析文本格式")
        bilingual = raw.get("bilingual") is True or (
            "bilingual" not in raw and global_bilingual
        )
        selected_language = normalize_subtitle_language(
            raw.get("subtitle_language")
            if raw.get("subtitle_language") is not None
            else gaps[gap_id].get("subtitle_language"),
        )
        if selected_language not in {"simplified_chinese", "traditional_chinese"}:
            raise ValueError("字幕 provider 未交付可验证的中文语言轨")
        original_language = normalize_subtitle_language(
            raw.get("original_language")
            if raw.get("original_language") is not None
            else global_original,
        )
        if bilingual:
            if (
                verified_original is None
                or original_language != verified_original
            ):
                raise ValueError("双语字幕未绑定 TMDB 已验证原语")
            expected_marker = _subtitle_target_marker(
                selected_language,
                bilingual=True,
                original_language=original_language,
            )
            marker = raw.get("subtitle_marker")
            if marker is None:
                marker = delivery.get("subtitle_marker")
            if not isinstance(marker, str) or marker != expected_marker:
                raise ValueError("双语字幕缺少可信持久化语言标识")
        else:
            original_language = None
        target = _subtitle_target_path(
            gaps[gap_id], str(source), bilingual=bilingual,
            original_language=original_language,
            subtitle_language=selected_language,
        )
        if target is None:
            raise ValueError("字幕正式目标无法证明为单一中文文件")
        output[gap_id] = {
            "source": str(source),
            "size": size,
            "target": target,
            "video_path": str(gaps[gap_id]["path"]),
            "subtitle_language": selected_language,
            "bilingual": bilingual,
            **({"original_language": original_language} if original_language else {}),
        }
    return output


def _subtitle_intents(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    intents = state.get(_SUBTITLE_INTENTS_KEY)
    if not isinstance(intents, dict):
        intents = {}
        state[_SUBTITLE_INTENTS_KEY] = intents
    return intents


def _write_subtitle_intent(
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    gap_id: str,
    value: Mapping[str, Any],
) -> None:
    intents = _subtitle_intents(state)
    intents[gap_id] = dict(value)
    state["updated_at"] = _now()
    save_root_replenishment_state(state_root, root_task_id, state)


def _drop_subtitle_intent(
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    gap_id: str,
) -> None:
    _subtitle_intents(state).pop(gap_id, None)
    state["updated_at"] = _now()
    save_root_replenishment_state(state_root, root_task_id, state)


def _record_subtitle_attempt(
    state_root: Path,
    root_task_id: str,
    gap: Gap,
    *,
    attempt_id: str,
    status: str,
    staged_paths: list[str] | None = None,
    error: str | None = None,
) -> None:
    record_attempt(
        state_root,
        root_task_id,
        gap.gap_id,
        attempt_id=attempt_id,
        provider=_SUBTITLE_PROVIDER,
        tier=_SUBTITLE_TIER,
        locator=None,
        status=status,
        staged_paths=staged_paths or [],
        error=(_safe_durable_error(error) if error else None),
    )


def _cleanup_empty_subtitle_staging(
    runner: Any,
    staging_root: str,
    *,
    pause_requested: Callable[[], bool] | None,
) -> bool:
    """Remove at most one freshly-proven-empty task attempt directory."""
    if pause_requested is not None and pause_requested():
        return False
    listing = getattr(runner.alist, "list", None)
    remove_empty = getattr(runner.alist, "remove_empty_dir", None)
    if not callable(listing) or not callable(remove_empty):
        return False
    try:
        try:
            rows = listing(staging_root, refresh=True)
        except TypeError:
            rows = listing(staging_root)
    except Exception:
        return False
    if not isinstance(rows, list) or rows:
        return False
    if pause_requested is not None and pause_requested():
        return False
    try:
        remove_empty(staging_root)
    except Exception:
        return False
    parent = posixpath.dirname(staging_root) or "/"
    name = posixpath.basename(staging_root)
    try:
        try:
            parent_rows = listing(parent, refresh=True)
        except TypeError:
            parent_rows = listing(parent)
    except Exception:
        return False
    return not any(
        isinstance(row, Mapping)
        and row.get("name") == name
        and row.get("is_dir") is True
        for row in (parent_rows if isinstance(parent_rows, list) else [])
    )


def _subtitle_staging_is_fresh_empty(runner: Any, staging_root: str) -> bool:
    """Prove a task attempt has no staged side effect before re-arming it.

    An empty/invalid provider response is not a no-op proof.  We clear an
    intent only after a fresh listing of its exact task root (or its parent
    when the root no longer exists) shows that no object or directory remains.
    Any read error stays fail-closed.
    """
    listing = getattr(runner.alist, "list", None)
    if not callable(listing):
        return False
    try:
        try:
            rows = listing(staging_root, refresh=True)
        except TypeError:
            rows = listing(staging_root)
    except Exception:
        parent = posixpath.dirname(staging_root) or "/"
        name = posixpath.basename(staging_root)
        try:
            try:
                parent_rows = listing(parent, refresh=True)
            except TypeError:
                parent_rows = listing(parent)
        except Exception:
            return False
        return (
            isinstance(parent_rows, list)
            and not any(
                isinstance(row, Mapping) and row.get("name") == name
                for row in parent_rows
            )
        )
    return isinstance(rows, list) and not rows


def _finish_subtitle_intent(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    *,
    gap: Gap,
    gap_row: Mapping[str, Any],
    request: Mapping[str, Any],
    pause_requested: Callable[[], bool] | None,
) -> dict[str, Any]:
    """Install/recover one persisted subtitle delivery without re-submitting it.

    A ``submitting`` intent with no immutable staged-file evidence is never
    replayed: its provider response may have been lost.  Once a source/target
    pair has been persisted, recovery uses fresh exact observations to either
    prove the formal target and close the gap, or safely continue that one
    writer operation.  Any ambiguous state remains ``waiting_reconcile``.
    """
    intent = _subtitle_intents(state).get(gap.gap_id)
    if not isinstance(intent, Mapping):
        return {"outcome": "none"}
    attempt_id = str(intent.get("attempt_id") or uuid.uuid4().hex)
    source = _bounded_path(intent.get("source"))
    target = _bounded_path(intent.get("target"))
    video_path = _bounded_path(intent.get("video_path"))
    staging_root = _bounded_path(intent.get("staging_root"))
    size = intent.get("size")
    bilingual = intent.get("bilingual") is True
    original_language = normalize_subtitle_language(intent.get("original_language"))
    subtitle_language = normalize_subtitle_language(
        intent.get("subtitle_language")
        if intent.get("subtitle_language") is not None
        else gap_row.get("subtitle_language"),
    )
    phase = str(intent.get("phase") or "").casefold()
    expected_staging_root = (
        _task_subtitle_staging_root(runner, root_task_id, attempt_id)
        if _safe_task_id(attempt_id) is not None
        else None
    )
    if (
        phase in {"submitting", "waiting_reconcile"}
        and (not source or not target or not video_path or not staging_root or not size)
    ):
        return {"outcome": "waiting_reconcile"}
    if (
        not source
        or not target
        or not video_path
        or not staging_root
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size <= 0
        or size > MAX_MERGED_SUBTITLE_BYTES
        or subtitle_language not in {"simplified_chinese", "traditional_chinese"}
        or expected_staging_root is None
        or posixpath.normpath(staging_root) != posixpath.normpath(expected_staging_root)
        or not _is_task_subtitle_staging_path(source, staging_root)
        or _subtitle_target_path(
            gap_row, source, bilingual=bilingual,
            original_language=original_language,
            subtitle_language=subtitle_language,
        ) != target
        or (bilingual and original_language != _verified_original_language(request))
    ):
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id,
            {"phase": "waiting_reconcile", "attempt_id": attempt_id},
        )
        return {"outcome": "waiting_reconcile"}

    def prove_target() -> bool:
        info = _fresh_file_info(runner.alist, target)
        if _file_size(info) != size:
            return False
        verdict = _validate_root_subtitle_content(
            runner,
            target,
            expected_size=size,
            bilingual=bilingual,
            original_language=original_language if bilingual else None,
            subtitle_language=subtitle_language,
        )
        return _subtitle_content_is_satisfied(verdict)

    # This first branch handles a lost writer response and makes restart
    # idempotent without ever creating a second sidecar.
    if prove_target():
        try:
            close_gap(state_root, root_task_id, gap.gap_id)
        except KeyError:
            return {"outcome": "none"}
        _drop_subtitle_intent(state_root, root_task_id, state, gap.gap_id)
        return {
            "outcome": "closed",
            "gap_id": gap.gap_id,
            "attempt_id": attempt_id,
            "target": target,
            "bilingual": bilingual,
        }

    if pause_requested is not None and pause_requested():
        return {"outcome": "paused"}

    source_info = _fresh_file_info(runner.alist, source)
    if _file_size(source_info) != size:
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id,
            {"phase": "waiting_reconcile", "attempt_id": attempt_id},
        )
        return {"outcome": "waiting_reconcile"}

    verdict = _validate_root_subtitle_content(
        runner,
        source,
        expected_size=size,
        bilingual=bilingual,
        original_language=original_language if bilingual else None,
        subtitle_language=subtitle_language,
    )
    if not _subtitle_content_is_satisfied(verdict):
        # The provider has already staged a real remote object.  Even when
        # its bytes fail our strict content proof, treating that as a simple
        # candidate miss would drop the durable intent and permit a second
        # submission beside unexamined task-owned bytes.  Hold the attempt
        # for reconciliation instead; only a separately proven cleanup may
        # make this coordinate retryable.
        pending = dict(intent)
        pending["phase"] = "waiting_reconcile"
        pending["updated_at"] = _now()
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id, pending,
        )
        _record_subtitle_attempt(
            state_root, root_task_id, gap,
            attempt_id=attempt_id,
            status="in_doubt",
            staged_paths=[source],
            error=_safe_durable_error(
                verdict.get("reason") or "subtitle_content_unproven",
            ),
        )
        return {"outcome": "waiting_reconcile", "gap_id": gap.gap_id}

    installer = getattr(runner, "install_subtitle_sidecar", None)
    if not callable(installer):
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id,
            {"phase": "waiting_reconcile", "attempt_id": attempt_id},
        )
        return {"outcome": "waiting_reconcile"}

    def validator(candidate_path: str, _required_language: str = "zh") -> Mapping[str, Any]:
        return _validate_root_subtitle_content(
            runner,
            candidate_path,
            expected_size=size,
            bilingual=bilingual,
            original_language=original_language if bilingual else None,
            subtitle_language=subtitle_language,
        )

    persisted = dict(intent)
    persisted["phase"] = "installing"
    persisted["updated_at"] = _now()
    _write_subtitle_intent(
        state_root, root_task_id, state, gap.gap_id, persisted,
    )
    try:
        result = _call_subtitle_installer(
            installer,
            source,
            target,
            expected_size=size,
            video_path=video_path,
            subtitle_language=subtitle_language,
            validator=validator,
            pause_requested=pause_requested,
        )
    except Exception as exc:
        if _is_pause_error(exc):
            return {"outcome": "paused"}
        # A lost move response is distinguishable only by fresh target/source
        # observations, never by replaying the provider.  Keep the delivery
        # intent intact so the next round can make that proof.
        if prove_target():
            try:
                close_gap(state_root, root_task_id, gap.gap_id)
            except KeyError:
                return {"outcome": "none"}
            _drop_subtitle_intent(state_root, root_task_id, state, gap.gap_id)
            return {
                "outcome": "closed", "gap_id": gap.gap_id,
                "attempt_id": attempt_id, "target": target,
                "bilingual": bilingual,
            }
        pending = dict(persisted)
        pending["phase"] = "waiting_reconcile"
        pending["updated_at"] = _now()
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id, pending,
        )
        return {
            "outcome": "waiting_reconcile", "gap_id": gap.gap_id,
            "error": _safe_durable_error(exc),
        }

    if (
        not isinstance(result, Mapping)
        or _file_size(result) not in {None, size}
        or not prove_target()
    ):
        pending = dict(persisted)
        pending["phase"] = "waiting_reconcile"
        pending["updated_at"] = _now()
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id, pending,
        )
        return {"outcome": "waiting_reconcile", "gap_id": gap.gap_id}
    try:
        close_gap(state_root, root_task_id, gap.gap_id)
    except KeyError:
        return {"outcome": "none"}
    _drop_subtitle_intent(state_root, root_task_id, state, gap.gap_id)
    return {
        "outcome": "closed", "gap_id": gap.gap_id,
        "attempt_id": attempt_id, "target": target,
        "bilingual": bilingual,
    }


def _recover_root_subtitle_intents(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    subtitle_requests: list[dict[str, Any]],
    *,
    pause_requested: Callable[[], bool] | None,
) -> dict[str, Any]:
    """Reconcile persisted subtitle writes without calling a provider again."""
    result = _empty_subtitle_result()
    by_gap: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for request in subtitle_requests:
        for gap_id, row in _subtitle_request_rows(request).items():
            by_gap[gap_id] = (request, row)
    gap_records = {
        gap.gap_id: gap
        for gap in load_gap_ledger(state_root, root_task_id)
        if gap.status == "open" and gap.kind == "missing_subtitle"
    }
    for gap_id in list(_subtitle_intents(state)):
        if pause_requested is not None and pause_requested():
            result["paused"] = True
            break
        pair = by_gap.get(gap_id)
        gap = gap_records.get(gap_id)
        if gap is None:
            # A fresh re-audit may already have proven this target.  The gap is
            # no longer open, so retaining a recovery token would only block
            # unrelated subtitle work; do not delete any remote staging tree.
            _drop_subtitle_intent(state_root, root_task_id, state, gap_id)
            continue
        if pair is None:
            # Missing bridge/title evidence does not prove the old provider
            # call never happened.  Keep the barrier rather than discarding it
            # and allowing a future bridge rebuild to submit again.
            result["subtitle_waiting"] = "waiting_reconcile"
            continue
        request, gap_row = pair
        intent = _subtitle_intents(state).get(gap_id)
        if (
            isinstance(intent, Mapping)
            and str(intent.get("phase") or "").casefold() == "prepared"
        ):
            # ``prepared`` is persisted before local workspace setup only.
            # The provider submission state is written later, immediately
            # before its call, so this state cannot denote a remote effect and
            # can safely be re-armed after a restart or a pre-call pause.
            _drop_subtitle_intent(state_root, root_task_id, state, gap_id)
            continue
        completed = _finish_subtitle_intent(
            runner, state_root, root_task_id, state,
            gap=gap, gap_row=gap_row, request=request,
            pause_requested=pause_requested,
        )
        outcome = completed.get("outcome")
        if outcome == "closed":
            result["subtitle_gaps_closed"].append(gap_id)
            result["subtitle_attempts"].append({
                "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                "outcome": "closed", "recovered": True,
            })
        elif outcome == "paused":
            result["paused"] = True
            break
        elif outcome == FAILURE_CANDIDATE:
            result["subtitle_attempts"].append({
                "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                "outcome": FAILURE_CANDIDATE, "recovered": True,
            })
        elif outcome == "waiting_reconcile":
            result["subtitle_waiting"] = "waiting_reconcile"
    if _subtitle_intents(state):
        result["subtitle_waiting"] = "waiting_reconcile"
    return result


def _run_root_subtitle_channel(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    requests: list[dict[str, Any]],
    *,
    subtitle_materializer_factory: Callable[[], Any] | Any | None,
    pause_requested: Callable[[], bool] | None,
) -> dict[str, Any]:
    """Drive independent subtitle acquisition before any video-tier effect.

    The provider is called at most once per new root request in a round.  A
    durable ``prepared`` intent covers local setup; it becomes ``submitting``
    immediately before the provider call.  If that call loses its response,
    later invocations only inspect the task-owned staging/target state; they
    never call the provider again for that gap.
    """
    result = _empty_subtitle_result()
    subtitle_requests: list[dict[str, Any]] = []
    for raw_request in requests:
        rows = _subtitle_request_rows(raw_request)
        if not rows:
            continue
        request = dict(raw_request)
        media = raw_request.get("media")
        request["media"] = dict(media) if isinstance(media, Mapping) else {}
        request["gaps"] = list(rows.values())
        subtitle_requests.append(request)
    result["subtitle_requests_built"] = len(subtitle_requests)
    if not subtitle_requests and not _subtitle_intents(state):
        return result

    recovery = _recover_root_subtitle_intents(
        runner, state_root, root_task_id, state, subtitle_requests,
        pause_requested=pause_requested,
    )
    result["subtitle_attempts"].extend(recovery["subtitle_attempts"])
    result["subtitle_gaps_closed"].extend(recovery["subtitle_gaps_closed"])
    result["subtitle_waiting"] = recovery["subtitle_waiting"]
    if recovery["paused"]:
        result["paused"] = True
        return result

    gap_records = {
        gap.gap_id: gap
        for gap in load_gap_ledger(state_root, root_task_id)
        if gap.status == "open" and gap.kind == "missing_subtitle"
    }
    candidate_failed = False
    active_staging_roots: set[str] = set()

    for request in subtitle_requests:
        if pause_requested is not None and pause_requested():
            result["paused"] = True
            break
        rows = _subtitle_request_rows(request)
        pending = {
            gap_id: row for gap_id, row in rows.items()
            if gap_id in gap_records and gap_id not in _subtitle_intents(state)
        }
        if not pending:
            continue

        attempt_id = f"subtitle-{uuid.uuid4().hex}"
        staging_root = _task_subtitle_staging_root(
            runner, root_task_id, attempt_id,
        )
        workspace = state_root / "subtitle_replenishment_workspace" / root_task_id / attempt_id
        if pause_requested is not None and pause_requested():
            result["paused"] = True
            break

        # Persist every gap in this provider call before allocating its local
        # workspace or invoking any discovery/download/upload boundary.
        intents = _subtitle_intents(state)
        for gap_id in pending:
            intents[gap_id] = {
                # This reserves the local preparation work only.  It must not
                # be mistaken for a provider submission after a pause or
                # crash before the final pre-call fence.
                "phase": "prepared",
                "attempt_id": attempt_id,
                "staging_root": staging_root,
                "subtitle_language": "simplified_chinese",
                "updated_at": _now(),
            }
        state["last_attempt_at"] = _now()
        state["updated_at"] = _now()
        save_root_replenishment_state(state_root, root_task_id, state)

        if pause_requested is not None and pause_requested():
            # No external provider boundary has been crossed.  Do not leave a
            # misleading in-flight token solely because the pause landed
            # between local intent persistence and workspace preparation.
            for gap_id in pending:
                intents.pop(gap_id, None)
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["paused"] = True
            break
        try:
            workspace.mkdir(parents=True, exist_ok=True)
        except OSError:
            # The adapter uses the workspace only for bounded temporary bytes;
            # a local failure before the provider call has no remote state and
            # can be classified safely as infrastructure, not in-doubt.
            for gap_id in pending:
                intents.pop(gap_id, None)
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="infrastructure",
                    error="字幕本地 workspace 无法创建",
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_INFRASTRUCTURE,
                })
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["subtitle_waiting"] = "retry_wait"
            continue

        if subtitle_materializer_factory is None:
            materializer = SubtitleMaterializer()
        elif callable(subtitle_materializer_factory):
            materializer = subtitle_materializer_factory()
        else:
            materializer = subtitle_materializer_factory
        acquire = getattr(materializer, "acquire_subtitles", None)
        if not callable(acquire):
            # No call occurred; the safe recovery intent may be cleared and
            # the visible failure remains a retryable infrastructure defect.
            for gap_id in pending:
                intents.pop(gap_id, None)
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="infrastructure",
                    error="字幕 materializer 不支持 acquire_subtitles",
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_INFRASTRUCTURE,
                })
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["subtitle_waiting"] = "retry_wait"
            continue

        if pause_requested is not None and pause_requested():
            # ``acquire_subtitles`` is the first provider effect.  Clearing a
            # prepared intent here is safe and makes a user pause resumable
            # without requiring a remote reconciliation round.
            for gap_id in pending:
                intents.pop(gap_id, None)
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["paused"] = True
            break

        # From this exact point onward a crash can race the provider call, so
        # make the durable token conservative before emitting the attempt.
        for gap_id in pending:
            intents[gap_id]["phase"] = "submitting"
            intents[gap_id]["updated_at"] = _now()
        state["updated_at"] = _now()
        save_root_replenishment_state(state_root, root_task_id, state)
        for gap_id in pending:
            _record_subtitle_attempt(
                state_root, root_task_id, gap_records[gap_id],
                attempt_id=attempt_id, status="submitted",
            )

        try:
            delivery = _call_materializer_with_pause(
                acquire,
                request,
                list(pending.values()),
                staging_root=staging_root,
                workspace=workspace,
                alist=runner.alist,
                pause_requested=pause_requested,
            )
        except Exception as exc:
            if _is_pause_error(exc):
                # The adapter might have stopped just before or just after an
                # upload.  Do not clear/replay the persisted intent.
                for gap_id in pending:
                    intents[gap_id]["phase"] = "waiting_reconcile"
                    intents[gap_id]["updated_at"] = _now()
                state["updated_at"] = _now()
                save_root_replenishment_state(state_root, root_task_id, state)
                result["paused"] = True
                result["subtitle_waiting"] = "waiting_reconcile"
                break
            scope, _task_id = _classify_error(exc)
            if (
                scope == FAILURE_CANDIDATE
                and _subtitle_staging_is_fresh_empty(runner, staging_root)
            ):
                # A candidate-labelled exception becomes retryable only after
                # the exact task root freshly proves it contains no staged
                # object.  The exception class alone cannot prove an upload
                # response was not lost.
                for gap_id in pending:
                    intents.pop(gap_id, None)
                    _record_subtitle_attempt(
                        state_root, root_task_id, gap_records[gap_id],
                        attempt_id=attempt_id, status="candidate_failed",
                        error=_safe_durable_error(exc),
                    )
                    result["subtitle_attempts"].append({
                        "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                        "outcome": FAILURE_CANDIDATE,
                    })
                candidate_failed = True
                state["updated_at"] = _now()
                save_root_replenishment_state(state_root, root_task_id, state)
                continue
            # A transport/upload *or candidate-labelled* exception can arrive
            # after the remote staging write committed. Preserve the intent as
            # in-doubt; no second provider call for these gap ids is allowed.
            for gap_id in pending:
                intents[gap_id]["phase"] = "waiting_reconcile"
                intents[gap_id]["updated_at"] = _now()
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="in_doubt",
                    error=_safe_durable_error(exc),
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_IN_DOUBT,
                })
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["subtitle_waiting"] = "waiting_reconcile"
            continue

        if not isinstance(delivery, Mapping):
            delivered: dict[str, dict[str, Any]] = {}
            delivery_error = "字幕 provider 返回无效 delivery"
        else:
            try:
                delivered = _extract_subtitle_delivery(
                    delivery, request, staging_root=staging_root,
                )
                delivery_error = None
            except ValueError as exc:
                delivered = {}
                delivery_error = _safe_durable_error(exc)

        valid: dict[str, dict[str, Any]] = {}
        for gap_id, entry in delivered.items():
            info = _fresh_file_info(runner.alist, entry["source"])
            if _file_size(info) == entry["size"]:
                valid[gap_id] = entry
            else:
                delivery_error = "字幕 provider staging 回读不可证明"
        staging_proven_empty = _subtitle_staging_is_fresh_empty(
            runner, staging_root,
        )
        for gap_id in pending:
            if gap_id in valid:
                entry = valid[gap_id]
                intents[gap_id] = {
                    "phase": "staged",
                    "attempt_id": attempt_id,
                    "staging_root": staging_root,
                    **entry,
                    "updated_at": _now(),
                }
                active_staging_roots.add(staging_root)
                continue
            if staging_proven_empty:
                intents.pop(gap_id, None)
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="candidate_failed",
                    error=delivery_error or "字幕 provider 未交付精确单集文件",
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_CANDIDATE,
                })
                candidate_failed = True
            else:
                # A malformed/partial response can still have staged bytes
                # that are absent from its file list.  Keep its original
                # durable intent and make the ambiguity explicit; a future
                # round is not permitted to submit this gap again.
                intents[gap_id]["phase"] = "waiting_reconcile"
                intents[gap_id]["updated_at"] = _now()
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="in_doubt",
                    error=delivery_error or "字幕 provider 交付不完整",
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_IN_DOUBT,
                })
                result["subtitle_waiting"] = "waiting_reconcile"
        state["updated_at"] = _now()
        save_root_replenishment_state(state_root, root_task_id, state)

        for gap_id, entry in valid.items():
            if pause_requested is not None and pause_requested():
                result["paused"] = True
                break
            # The provider may bind a delivery file to a gap outside this
            # round's pending set (already closed, or still owned by a
            # recovery intent).  That row is not this round's to finish:
            # skip it with a visible note instead of KeyErroring the whole
            # subtitle lane.
            gap_row = pending.get(gap_id)
            if gap_row is None:
                result.setdefault("subtitle_out_of_round", []).append(gap_id)
                continue
            completed = _finish_subtitle_intent(
                runner, state_root, root_task_id, state,
                gap=gap_records[gap_id], gap_row=gap_row,
                request=request, pause_requested=pause_requested,
            )
            outcome = completed.get("outcome")
            if outcome == "closed":
                result["subtitle_gaps_closed"].append(gap_id)
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": "closed", "bilingual": bool(entry.get("bilingual")),
                })
            elif outcome == "paused":
                result["paused"] = True
                break
            elif outcome == FAILURE_CANDIDATE:
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_CANDIDATE,
                })
                candidate_failed = True
            elif outcome == "waiting_reconcile":
                result["subtitle_waiting"] = "waiting_reconcile"
        if result["paused"]:
            break

    # Cleanup never walks a non-empty tree and is skipped while any intent
    # still references that attempt.  This leaves uncertain staging untouched
    # for an operator/recovery read rather than deleting evidence.
    still_active = {
        str(entry.get("staging_root"))
        for entry in _subtitle_intents(state).values()
        if isinstance(entry, Mapping) and entry.get("staging_root")
    }
    for staging_root in sorted(active_staging_roots - still_active):
        _cleanup_empty_subtitle_staging(
            runner, staging_root, pause_requested=pause_requested,
        )

    if _subtitle_intents(state):
        result["subtitle_waiting"] = "waiting_reconcile"
    elif result["subtitle_waiting"] is None and candidate_failed:
        result["subtitle_waiting"] = "retry_wait"
    return result


def _exclude_locator(state: dict[str, Any], tier: str, locator: str) -> None:
    """Add one locator to the durable per-tier candidate failure memory."""
    if not locator:
        return
    failures = state.get("candidate_failures_by_provider")
    if not isinstance(failures, dict):
        failures = {}
        state["candidate_failures_by_provider"] = failures
    rows = failures.get(tier)
    if not isinstance(rows, list):
        rows = []
        failures[tier] = rows
    if locator not in rows:
        rows.append(locator)


def _video_paths(runner: Any, state_root: Path, root_task_id: str, attempt_id: str) -> tuple[str, Path, str]:
    staging = f"{str(runner.library_root).rstrip('/')}{_STAGING_NAMESPACE}/{root_task_id}/{attempt_id}"
    workspace = Path(state_root) / "replenishment_workspace" / root_task_id / attempt_id
    return staging, workspace, f"replenishment-{attempt_id}"


_ATTEMPT_DIR_NAME_RE = re.compile(r"^(?:subtitle-)?[0-9a-f]{32}$")


def _remove_attempt_workspace_tree(workspace_value: object) -> None:
    """Remove one finished attempt's local workspace, boundary-checked.

    The orchestrator owns the workspace once its intent ends: the adapter
    only frees group payload directories mid-attempt.  A symlink or a
    non-directory at the exact path is left for the operator — never follow.
    """
    workspace = _bounded_path(workspace_value)
    if workspace is None:
        return
    path = Path(workspace)
    try:
        if path.is_symlink() or not path.is_dir():
            return
        shutil.rmtree(path)
    except OSError:
        pass


def _sweep_orphan_attempt_workspaces(
    state_root: Path, root_task_id: str, state: Mapping[str, Any],
) -> None:
    """Reclaim attempt workspaces no durable intent references.

    Superseded and abandoned attempts accumulate local bytes forever
    otherwise: the adapter frees only group payload directories, and neither
    the lane's terminal paths nor the job cleanup endpoint historically
    touched these trees (tens of GB of unreferenced corpses were measured on
    one root).  Only exact attempt-id-shaped directory names directly under
    the root's own workspace are ever removed; anything else is left for the
    operator.
    """
    state_root = Path(state_root)
    video_attempts = set(_video_intents(state))
    subtitle_attempts = {
        str(intent.get("attempt_id") or "")
        for intent in (state.get(_SUBTITLE_INTENTS_KEY) or {}).values()
        if isinstance(intent, Mapping)
    }
    for root, live_names in (
        (
            state_root / "replenishment_workspace" / root_task_id,
            video_attempts,
        ),
        (
            state_root / "subtitle_replenishment_workspace" / root_task_id,
            subtitle_attempts,
        ),
    ):
        try:
            entries = list(root.iterdir())
        except OSError:
            continue
        swept_everything = True
        for entry in entries:
            name = entry.name
            if (
                name in live_names
                or _ATTEMPT_DIR_NAME_RE.fullmatch(name) is None
                or entry.is_symlink()
            ):
                swept_everything = False
                continue
            try:
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    swept_everything = False
            except OSError:
                swept_everything = False
        if swept_everything:
            # An empty root shell is task-owned residue too.
            try:
                root.rmdir()
            except OSError:
                pass


def _video_intents(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    intents = state.get(_VIDEO_INTENTS_KEY)
    if not isinstance(intents, dict):
        intents = {}
        state[_VIDEO_INTENTS_KEY] = intents
    return intents


def _sync_video_in_flight(state: dict[str, Any]) -> None:
    state[_IN_FLIGHT_KEY] = {
        token: _safe_task_id(intent.get("external_task_id"))
        for intent in _video_intents(state).values()
        if isinstance(intent, Mapping)
        for token in intent.get("selected_gap_ids", [])
        if isinstance(token, str) and token
    }


def _write_video_intent(state_root: Path, root_task_id: str, state: dict[str, Any], attempt_id: str, value: Mapping[str, Any]) -> None:
    _video_intents(state)[attempt_id] = dict(value)
    _sync_video_in_flight(state)
    state["updated_at"] = _now()
    save_root_replenishment_state(state_root, root_task_id, state)


def _drop_video_intent(state_root: Path, root_task_id: str, state: dict[str, Any], attempt_id: str) -> None:
    _video_intents(state).pop(attempt_id, None)
    _sync_video_in_flight(state)
    state["updated_at"] = _now()
    save_root_replenishment_state(state_root, root_task_id, state)


def _new_video_intent(runner: Any, state_root: Path, root_task_id: str, *, attempt_id: str, tier: str, request: Mapping[str, Any], selection: Mapping[str, Any], covered_gaps: list[Gap]) -> dict[str, Any]:
    request_copy, request_safe = _durable_mapping(request)
    selection_copy, selection_safe = _durable_mapping(selection)
    provider = str(selection.get("provider") or "").strip().casefold()
    locator, locator_safe = _durable_locator(selection.get("locator"))
    selected = selection.get("selected_gap_ids")
    if (
        request_copy is None or selection_copy is None or _safe_task_id(attempt_id) is None
        or tier not in STRICT_TIER_ORDER or provider != _TIER_PROVIDER.get(tier)
        or locator is None or not isinstance(selected, list) or not selected
        or any(not isinstance(item, str) or not item for item in selected)
        or len(selected) != len(set(selected))
        or not covered_gaps
    ):
        raise ValueError("补源 selection 无法形成完整可恢复 attempt")
    selection_copy.update({"provider": provider, "locator": locator, "selected_gap_ids": list(selected)})
    staging_root, workspace, child_job_id = _video_paths(runner, state_root, root_task_id, attempt_id)
    return {
        "phase": "prepared", "attempt_id": attempt_id, "tier": tier,
        "provider": provider, "locator": locator, "selected_gap_ids": list(selected),
        "ledger_gap_ids": [gap.gap_id for gap in covered_gaps],
        "staging_root": staging_root, "workspace": str(workspace),
        "child_job_id": child_job_id, "request": request_copy, "selection": selection_copy,
        "recovery_safe": request_safe and selection_safe and locator_safe,
    }


def _valid_video_intent(runner: Any, state_root: Path, root_task_id: str, value: object) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    attempt_id = _safe_task_id(value.get("attempt_id"))
    parsed = _normalize_video_intents({attempt_id: value}) if attempt_id else None
    if not parsed:
        return None
    intent = parsed[attempt_id]
    staging_root, workspace, child_job_id = _video_paths(runner, state_root, root_task_id, attempt_id)
    if (intent["staging_root"], intent["workspace"], intent["child_job_id"]) != (staging_root, str(workspace), child_job_id):
        return None
    return intent


def _video_delivery(value: object, intent: Mapping[str, Any]) -> dict[str, Any] | None:
    delivery, _ = _durable_mapping(value)
    if delivery is None or (
        delivery.get("lane", intent["tier"]) != intent["tier"]
        or delivery.get("attempt_id", intent["attempt_id"]) != intent["attempt_id"]
        or delivery.get("staging_root") != intent["staging_root"]
    ):
        return None
    files = delivery.get("files", [])
    selected = set(intent["selected_gap_ids"])
    if not isinstance(files, list) or not files:
        return None
    normalized: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    covered: dict[str, int] = {gap_id: 0 for gap_id in selected}
    for row in files:
        if not isinstance(row, Mapping):
            return None
        path, size, kind, gap_ids = _bounded_path(row.get("path")), row.get("size"), row.get("kind"), row.get("gap_ids")
        if (
            path is None or not _path_under(path, str(intent["staging_root"]))
            or isinstance(size, bool) or not isinstance(size, int) or size <= 0
            # The video tiers are not permitted to smuggle a companion
            # sidecar through the staging root.  RootJob's independent
            # subtitle transaction is the sole automatic subtitle writer,
            # including its exact bilingual validation.
            or kind != "video"
            # ``kind`` is provider-controlled metadata.  Require an actual
            # normal video suffix too, otherwise a subtitle/archive can call
            # itself ``video`` and reach the child planner.
            or posixpath.splitext(path)[1].casefold() not in VIDEO_EXTENSIONS
            or path in seen_paths
            or not isinstance(gap_ids, list)
            # One selected media coordinate means one ordinary file.  A
            # season pack, a multi-episode file, or a duplicate mapping is
            # not precise replenishment and must remain out of the writer.
            or len(gap_ids) != 1
            or not isinstance(gap_ids[0], str)
            or gap_ids[0] not in selected
        ):
            return None
        gap_id = gap_ids[0]
        seen_paths.add(path)
        covered[gap_id] = covered.get(gap_id, 0) + 1
        normalized.append({"path": path, "size": size, "kind": kind, "gap_ids": [gap_id]})
    if set(covered) != selected or any(count != 1 for count in covered.values()):
        return None
    output = {"lane": intent["tier"], "attempt_id": intent["attempt_id"], "staging_root": intent["staging_root"], "files": normalized}
    task_id = delivery.get("external_task_id")
    if task_id is not None:
        task_id = _safe_task_id(task_id)
        if task_id is None:
            return None
        output["external_task_id"] = task_id
    return output


class _VideoDeliveryPaused(RuntimeError):
    """Pause reached a staging isolation boundary before a remote move."""

    pause_requested = True


def _video_pause_checkpoint(
    pause_requested: Callable[[], bool] | None,
) -> None:
    if pause_requested is None:
        return
    try:
        paused = bool(pause_requested())
    except Exception as exc:
        raise _VideoDeliveryPaused("补源暂停状态不可确认") from exc
    if paused:
        raise _VideoDeliveryPaused("补源已暂停")


def _safe_video_inventory_name(value: object) -> str | None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
        or "\x00" in value
    ):
        return None
    return value


def _fresh_video_staging_inventory(
    runner: Any,
    staging_root: str,
) -> dict[str, int] | None:
    """Freshly enumerate a bounded task staging subtree.

    Delivery metadata is an assertion from a provider, not proof that no
    other file was staged.  The child planner has a directory-only request
    surface, so prove the whole source tree contains precisely the selected
    members before it ever sees that directory.  An unreadable or unusually
    large tree is deliberately unknown rather than treated as empty.
    """
    listing = getattr(runner.alist, "list", None)
    if not callable(listing):
        return None
    root = posixpath.normpath(staging_root)
    if not root.startswith("/") or root == "/":
        return None
    queue: list[tuple[str, int]] = [(root, 0)]
    visited_dirs: set[str] = set()
    files: dict[str, int] = {}
    nodes = 0
    while queue:
        directory, depth = queue.pop(0)
        if directory in visited_dirs or depth > _MAX_VIDEO_STAGING_DEPTH:
            return None
        visited_dirs.add(directory)
        try:
            try:
                rows = listing(directory, refresh=True)
            except TypeError:
                rows = listing(directory)
        except Exception:
            return None
        if not isinstance(rows, list):
            return None
        names: set[str] = set()
        for row in rows:
            if not isinstance(row, Mapping):
                return None
            name = _safe_video_inventory_name(row.get("name"))
            if name is None or name in names:
                return None
            names.add(name)
            nodes += 1
            if nodes > _MAX_VIDEO_STAGING_NODES:
                return None
            path = posixpath.join(directory, name)
            if not _path_under(path, root) or path == root:
                return None
            if row.get("is_dir") is True:
                queue.append((path, depth + 1))
                continue
            size = row.get("size")
            if (
                isinstance(size, bool)
                or not isinstance(size, int)
                or size <= 0
                or path in files
            ):
                return None
            files[path] = size
    return files


def _video_media_root(intent: Mapping[str, Any]) -> str:
    staging = _bounded_path(intent.get("staging_root"))
    if staging is None or not staging.startswith("/"):
        raise ValueError("视频补源 staging_root 无效")
    return f"{staging.rstrip('/')}/{_VIDEO_MEDIA_SUBROOT}"


def _video_child_source_root(
    intent: Mapping[str, Any],
    delivery: Mapping[str, Any],
) -> str:
    """Return only the isolated directory containing accepted video files."""
    media_root = _video_media_root(intent)
    files = delivery.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("视频补源 delivery 缺少已隔离文件")
    for row in files:
        if not isinstance(row, Mapping):
            raise ValueError("视频补源 delivery 文件无效")
        path = _bounded_path(row.get("path"))
        if path is None or posixpath.dirname(path) != media_root:
            raise ValueError("视频补源文件未隔离到受控媒体目录")
    return media_root


def _prepare_video_delivery_for_child(
    runner: Any,
    intent: Mapping[str, Any],
    delivery: Mapping[str, Any],
    *,
    pause_requested: Callable[[], bool] | None,
) -> dict[str, Any]:
    """Prove and isolate exactly the delivered videos before child planning.

    Moves occur only inside the current attempt's staging root.  The operation
    is idempotent across a crash: each source is accepted only if its original
    location or its deterministic isolated destination has the exact declared
    size, never both and never an unrelated sibling.  A caller persists the
    original delivery before entering this function, so any uncertainty stays
    recoverable as ``waiting_reconcile`` without a provider replay.
    """
    checked = _video_delivery(delivery, intent)
    if checked is None:
        raise ValueError("视频补源 delivery 合同无效")
    media_root = _video_media_root(intent)
    rows = checked["files"]
    assert isinstance(rows, list)  # proven by _video_delivery
    destinations: dict[str, tuple[dict[str, Any], str]] = {}
    for row in rows:
        assert isinstance(row, dict)
        source = str(row["path"])
        name = posixpath.basename(source)
        destination = f"{media_root}/{name}"
        if name in destinations:
            raise ValueError("视频补源隔离文件名冲突")
        destinations[name] = (row, destination)

    # Before the first staging move, every visible file must be one of the
    # declared originals or deterministic destinations left by an interrupted
    # earlier isolation.  This prevents a rogue provider sidecar/extra video
    # from entering the directory-only child planner.
    inventory = _fresh_video_staging_inventory(runner, str(intent["staging_root"]))
    if inventory is None:
        raise ValueError("视频补源 staging 清单无法证明")
    allowed_paths = {
        path
        for row, destination in destinations.values()
        for path in (str(row["path"]), destination)
    }
    if set(inventory) - allowed_paths:
        raise ValueError("视频补源 staging 含有未声明文件")
    for row, destination in destinations.values():
        source = str(row["path"])
        size = int(row["size"])
        source_size = inventory.get(source)
        destination_size = inventory.get(destination)
        if source == destination:
            if source_size != size:
                raise ValueError("视频补源隔离文件大小无法证明")
        elif (source_size is None) == (destination_size is None):
            # Neither path means data is absent; both paths means a partial
            # copy/duplicate that must not be handed to the child.
            raise ValueError("视频补源隔离状态不唯一")
        elif source_size not in {None, size} or destination_size not in {None, size}:
            raise ValueError("视频补源隔离文件大小不匹配")

    mkdir = getattr(runner.alist, "ensure_directory", None)
    if not callable(mkdir):
        mkdir = getattr(runner.alist, "mkdir", None)
    move = getattr(runner.alist, "move", None)
    if not callable(mkdir) or not callable(move):
        raise ValueError("AList 客户端缺少视频 staging 隔离能力")
    for row, destination in destinations.values():
        source = str(row["path"])
        if source == destination or inventory.get(destination) == row["size"]:
            continue
        _video_pause_checkpoint(pause_requested)
        mkdir(media_root)
        _video_pause_checkpoint(pause_requested)
        move(posixpath.dirname(source), media_root, [posixpath.basename(source)])

    isolated = _fresh_video_staging_inventory(runner, media_root)
    expected = {
        destination: int(row["size"])
        for row, destination in destinations.values()
    }
    if isolated != expected:
        raise ValueError("视频补源隔离 staging 回读不精确")
    output = dict(checked)
    output["files"] = [
        {
            "path": destination,
            "size": row["size"],
            "kind": "video",
            "gap_ids": list(row["gap_ids"]),
        }
        for row, destination in destinations.values()
    ]
    return output


def _video_gaps(state_root: Path, root_task_id: str, intent: Mapping[str, Any]) -> list[Gap]:
    open_gaps = {gap.gap_id: gap for gap in load_gap_ledger(state_root, root_task_id) if gap.status == "open" and gap.kind != "missing_subtitle"}
    return [open_gaps[gap_id] for gap_id in intent["ledger_gap_ids"] if gap_id in open_gaps]


def _wait_video_intent(state_root: Path, root_task_id: str, state: dict[str, Any], intent: Mapping[str, Any], *, task_id: str | None = None) -> dict[str, Any]:
    pending = dict(intent)
    pending["phase"] = "waiting_reconcile"
    if task_id is not None:
        pending["external_task_id"] = task_id
    _write_video_intent(state_root, root_task_id, state, str(pending["attempt_id"]), pending)
    return pending


def _resume_video_intent(runner: Any, state_root: Path, root_task_id: str, state: dict[str, Any], intent: Mapping[str, Any], *, pause_requested: Callable[[], bool] | None) -> dict[str, Any]:
    intent = _valid_video_intent(runner, state_root, root_task_id, intent)
    delivery = _video_delivery(intent.get("delivery"), intent) if intent else None
    if intent is None or delivery is None:
        if intent is not None:
            _wait_video_intent(state_root, root_task_id, state, intent)
        return {"outcome": "waiting_reconcile", "attempts": [], "gaps_closed": []}
    gaps = _video_gaps(state_root, root_task_id, intent)
    if not gaps:
        _drop_video_intent(state_root, root_task_id, state, str(intent["attempt_id"]))
        _remove_attempt_workspace_tree(intent.get("workspace"))
        return {"outcome": "closed", "attempts": [], "gaps_closed": []}
    if pause_requested is not None and pause_requested():
        return {"outcome": "paused", "attempts": [], "gaps_closed": []}
    pending = dict(intent)
    try:
        delivery = _prepare_video_delivery_for_child(
            runner,
            pending,
            delivery,
            pause_requested=pause_requested,
        )
    except Exception as exc:
        if _is_pause_error(exc):
            return {"outcome": "paused", "attempts": [], "gaps_closed": []}
        _wait_video_intent(state_root, root_task_id, state, pending)
        return {"outcome": "waiting_reconcile", "attempts": [], "gaps_closed": []}
    # Persist the deterministic isolated paths before the child is planned.
    # A crash during the staging moves leaves the prior (source-path) delivery
    # intact; the next round re-proves/moves only the missing members.
    pending.update({"phase": "staged", "delivery": delivery})
    _write_video_intent(state_root, root_task_id, state, str(pending["attempt_id"]), pending)
    pending["phase"] = "installing"
    _write_video_intent(state_root, root_task_id, state, str(pending["attempt_id"]), pending)
    try:
        try:
            child = runner.get_job(str(pending["child_job_id"]))
        except Exception:
            child = None
        if child is None:
            child = runner.plan_job(_child_request(runner, state_root, root_task_id, pending["request"], pending, delivery), job_id=pending["child_job_id"], internal_child_of=root_task_id, pause_requested=pause_requested)
        if getattr(child, "phase", None) == "executed":
            executed = child
        elif getattr(child, "phase", None) in {"planned", "retry_wait"}:
            executed = runner.execute_job(str(pending["child_job_id"]), pause_requested=pause_requested)
        else:
            return {"outcome": "waiting_reconcile", "attempts": [], "gaps_closed": []}
    except Exception as exc:
        if _is_pause_error(exc):
            return {"outcome": "paused", "attempts": [], "gaps_closed": []}
        _wait_video_intent(state_root, root_task_id, state, pending)
        return {"outcome": "waiting_reconcile", "attempts": [], "gaps_closed": []}
    if getattr(executed, "phase", None) != "executed":
        _wait_video_intent(state_root, root_task_id, state, pending)
        return {"outcome": "waiting_reconcile", "attempts": [], "gaps_closed": []}
    media = pending["request"].get("media") or {}
    _, by_unit = _open_gaps_by_token(state_root, root_task_id, str(media.get("media_type") or ""), media.get("tmdb_id"))
    plan = executed.plan if isinstance(getattr(executed, "plan", None), Mapping) else {}
    attempts, closed, uncovered = [], [], []
    for gap in gaps:
        if _prove_gap_coverage(gap, by_unit, plan):
            try:
                close_gap(state_root, root_task_id, gap.gap_id)
            except KeyError:
                continue
            closed.append(gap.gap_id)
            attempts.append({"gap_id": gap.gap_id, "tier": pending["tier"], "outcome": "closed", "candidate_key": pending["locator"]})
        else:
            uncovered.append(gap)
            record_attempt(state_root, root_task_id, gap.gap_id, attempt_id=pending["attempt_id"], provider=pending["provider"], tier=pending["tier"], locator=pending["locator"], status="candidate_failed", error="补源执行后未证明缺口被覆盖")
            attempts.append({"gap_id": gap.gap_id, "tier": pending["tier"], "outcome": FAILURE_CANDIDATE, "candidate_key": pending["locator"]})
    if uncovered:
        _wait_video_intent(state_root, root_task_id, state, pending)
        return {"outcome": "waiting_reconcile", "attempts": attempts, "gaps_closed": closed}
    _drop_video_intent(state_root, root_task_id, state, str(pending["attempt_id"]))
    _remove_attempt_workspace_tree(pending.get("workspace"))
    return {"outcome": "closed", "attempts": attempts, "gaps_closed": closed}


def _recover_video_intents(runner: Any, state_root: Path, root_task_id: str, state: dict[str, Any], *, materializer_factory: Callable[[str], Any], pause_requested: Callable[[], bool] | None) -> dict[str, Any]:
    result = {"attempts": [], "gaps_closed": [], "waiting": None, "paused": False}
    for attempt_id in list(_video_intents(state)):
        if pause_requested is not None and pause_requested():
            result["paused"] = True
            return result
        intent = _valid_video_intent(runner, state_root, root_task_id, _video_intents(state).get(attempt_id))
        if intent is None:
            result["waiting"] = "waiting_reconcile"
            return result
        if not isinstance(intent.get("delivery"), Mapping):
            task_id = _safe_task_id(intent.get("external_task_id"))
            if (
                intent["recovery_safe"] is not True
                or intent["phase"] not in {"submitting", "waiting_reconcile"}
            ):
                _wait_video_intent(state_root, root_task_id, state, intent)
                result["waiting"] = "waiting_reconcile"
                return result
            if task_id is not None:
                try:
                    reconcile = getattr(materializer_factory(intent["tier"]), "reconcile_existing_task")
                    delivery = _call_materializer_with_pause(reconcile, intent["request"], [intent["selection"]], staging_root=intent["staging_root"], workspace=Path(intent["workspace"]), alist=runner.alist, external_task_id=task_id, pause_requested=pause_requested)
                except Exception as exc:
                    if _is_pause_error(exc):
                        result["paused"] = True
                        return result
                    _wait_video_intent(state_root, root_task_id, state, intent, task_id=task_id)
                    result["waiting"] = "waiting_reconcile"
                    return result
            elif intent["tier"] == TIER_LOCAL_MAGNET:
                # The local Torrent lane has no external task to query: a
                # crash mid-acquire leaves only local bytes plus possibly a
                # few committed staging uploads.  Its acquisition is
                # idempotent — the per-member pipeline skips remote-committed
                # and locally-complete members by exact size — so resuming
                # the same durable attempt is the reconciliation itself.
                materializer = materializer_factory(intent["tier"])
                submitting = dict(intent)
                submitting["phase"] = "submitting"
                _write_video_intent(state_root, root_task_id, state, attempt_id, submitting)
                try:
                    delivery = _call_materializer_with_pause(
                        materializer.acquire, intent["request"], [intent["selection"]],
                        staging_root=intent["staging_root"],
                        workspace=Path(intent["workspace"]),
                        alist=runner.alist,
                        pause_requested=pause_requested,
                    )
                except Exception as exc:
                    if _is_pause_error(exc):
                        _wait_video_intent(state_root, root_task_id, state, submitting)
                        result["paused"] = True
                        return result
                    scope, _external = _classify_error(exc)
                    if scope == FAILURE_CANDIDATE:
                        # A clean resource failure (dead swarm, rejected
                        # payload) drops the durable attempt so a later
                        # round may search for a different candidate.
                        _drop_video_intent(state_root, root_task_id, state, attempt_id)
                        _remove_attempt_workspace_tree(intent.get("workspace"))
                        for gap_id in intent["ledger_gap_ids"]:
                            record_attempt(
                                state_root, root_task_id, gap_id,
                                attempt_id=attempt_id,
                                provider=intent["provider"], tier=intent["tier"],
                                locator=intent["locator"], status="candidate_failed",
                                error=_safe_durable_error(exc),
                            )
                            result["attempts"].append({
                                "gap_id": gap_id, "tier": intent["tier"],
                                "outcome": FAILURE_CANDIDATE,
                                "candidate_key": intent["locator"],
                            })
                        _exclude_locator(state, intent["tier"], intent["locator"])
                        continue
                    _wait_video_intent(state_root, root_task_id, state, submitting)
                    result["waiting"] = "waiting_reconcile"
                    return result
            else:
                _wait_video_intent(state_root, root_task_id, state, intent)
                result["waiting"] = "waiting_reconcile"
                return result
            delivery = _video_delivery(delivery, intent)
            if delivery is None or (
                # Only the reconcile path owes task-id continuity: it claims
                # to describe the very external task the intent recorded.  A
                # re-run local acquire returns fresh truth instead.
                task_id is not None
                and delivery.get("external_task_id") not in {None, task_id}
            ):
                _wait_video_intent(state_root, root_task_id, state, intent, task_id=task_id)
                result["waiting"] = "waiting_reconcile"
                return result
            intent = dict(intent)
            intent.update({"phase": "staged", "delivery": delivery, "external_task_id": task_id})
            _write_video_intent(state_root, root_task_id, state, attempt_id, intent)
        continued = _resume_video_intent(runner, state_root, root_task_id, state, intent, pause_requested=pause_requested)
        result["attempts"].extend(continued["attempts"])
        result["gaps_closed"].extend(continued["gaps_closed"])
        if continued["outcome"] == "paused":
            result["paused"] = True
            return result
        if continued["outcome"] == "waiting_reconcile":
            result["waiting"] = "waiting_reconcile"
            return result
    return result


def run_root_replenishment(
    runner,
    state_root: Path,
    root_task_id: str,
    *,
    search_runner=None,
    materializer_factory=None,
    subtitle_materializer_factory: Callable[[], Any] | Any | None = None,
    pause_requested=None,
) -> dict[str, Any]:
    """Run one replenishment round for the root task's open gaps.

    One invocation processes every open gap for the *current* tier and returns
    a JSON-serializable summary.  Progressing to the next tier happens only
    after a complete no-candidate proof across all open requests; retry/reconcile
    waits pin the tier and never re-submit an in-flight gap.
    """
    state_root = Path(state_root)

    def pause() -> bool:
        """Read the root fence defensively; unknown control means stopped."""
        if pause_requested is None:
            return False
        try:
            return bool(pause_requested())
        except Exception:
            return True

    materializer_pause = pause if pause_requested is not None else None
    if materializer_factory is None:
        archive_preprocessor = getattr(runner, "archive_preprocessor", None)

        def factory(tier: str):
            return _default_materializer_factory(
                tier,
                archive_preprocessor=archive_preprocessor,
            )
    else:
        factory = materializer_factory

    state = load_root_replenishment_state(state_root, root_task_id)
    if state.get(_STATE_ATTENTION_KEY) is True:
        tier = str(state.get("tier") or TIER_QUARK_SHARE)
        return {
            "tier": tier,
            "tier_before": tier,
            "requests_built": 0,
            "attempts": [],
            "gaps_closed": [],
            "state": state,
            "waiting": "attention",
            "attention": state.get(_STATE_ATTENTION_REASON_KEY),
            "subtitle_requests_built": 0,
            "subtitle_attempts": [],
            "subtitle_gaps_closed": [],
            "subtitle_waiting": "attention",
            "paused": False,
        }
    if state.get(_STATE_RECOVERY_BLOCKED_KEY) is True:
        # Do not overwrite the malformed file with a fresh-looking state.
        # Its unreadable bytes may be the only evidence of a provider submit.
        tier = str(state.get("tier") or TIER_QUARK_SHARE)
        return {
            "tier": tier,
            "tier_before": tier,
            "requests_built": 0,
            "attempts": [],
            "gaps_closed": [],
            "state": state,
            "waiting": "waiting_reconcile",
            "subtitle_requests_built": 0,
            "subtitle_attempts": [],
            "subtitle_gaps_closed": [],
            "subtitle_waiting": "waiting_reconcile",
            "paused": False,
        }
    tier = str(state.get("tier") or TIER_QUARK_SHARE)

    # A durable provider boundary is resolved before any new read, search,
    # subtitle lane, or materializer can run.  Recovery never calls acquire.
    video_recovery = _recover_video_intents(runner, state_root, root_task_id, state, materializer_factory=factory, pause_requested=materializer_pause)
    recovered_attempts = list(video_recovery["attempts"])
    recovered_gaps_closed = list(video_recovery["gaps_closed"])
    if video_recovery["paused"] or video_recovery["waiting"]:
        state.update({"updated_at": _now(), "waiting": "waiting_reconcile"})
        _sync_video_in_flight(state)
        _sweep_orphan_attempt_workspaces(state_root, root_task_id, state)
        save_root_replenishment_state(state_root, root_task_id, state)
        return {
            "tier": tier, "tier_before": tier, "requests_built": 0,
            "attempts": recovered_attempts, "gaps_closed": recovered_gaps_closed,
            "state": state, "waiting": "waiting_reconcile",
            "subtitle_requests_built": 0, "subtitle_attempts": [],
            "subtitle_gaps_closed": [], "subtitle_waiting": None,
            "paused": bool(video_recovery["paused"]),
        }

    # Reaudit phantom gaps before building a fresh candidate request.
    try:
        from .gap_reaudit import reaudit_open_gaps
        reaudit = reaudit_open_gaps(runner, state_root, root_task_id)
        if reaudit.get("closed"):
            _trace(f"reaudit root={root_task_id} closed={len(reaudit['closed'])} kept={len(reaudit.get('kept_open') or [])}")
    except Exception:
        pass
    requests = gap_ledger_requests(state_root, root_task_id)
    _trace(f"start root={root_task_id} tier={tier} requests={len(requests)}")
    for request in requests:
        _enrich_media_titles(runner, request)

    # Subtitle-only delivery is intentionally completed before any video tier
    # can reach a search/materializer/write boundary.  A pause requested here
    # therefore cannot be followed by an unrelated video acquisition.
    subtitle_result = _run_root_subtitle_channel(
        runner,
        state_root,
        root_task_id,
        state,
        requests,
        subtitle_materializer_factory=subtitle_materializer_factory,
        pause_requested=materializer_pause,
    )

    def attach_subtitle_result(result: dict[str, Any]) -> dict[str, Any]:
        result.update({
            "subtitle_requests_built": subtitle_result["subtitle_requests_built"],
            "subtitle_attempts": subtitle_result["subtitle_attempts"],
            "subtitle_gaps_closed": subtitle_result["subtitle_gaps_closed"],
            "subtitle_waiting": subtitle_result["subtitle_waiting"],
            "paused": subtitle_result["paused"],
        })
        return result

    def merged_waiting(video_waiting: str | None) -> str | None:
        subtitle_waiting = subtitle_result.get("subtitle_waiting")
        if "waiting_reconcile" in {video_waiting, subtitle_waiting}:
            return "waiting_reconcile"
        if "retry_wait" in {video_waiting, subtitle_waiting}:
            return "retry_wait"
        return video_waiting

    if subtitle_result["paused"]:
        state["updated_at"] = _now()
        state["waiting"] = merged_waiting(None)
        save_root_replenishment_state(state_root, root_task_id, state)
        return attach_subtitle_result({
            "tier": tier,
            "tier_before": tier,
            "requests_built": 0,
            "attempts": [],
            "gaps_closed": [],
            "state": state,
            "waiting": state["waiting"],
        })

    if not requests:
        state["updated_at"] = _now()
        state["waiting"] = merged_waiting(None)
        _sync_video_in_flight(state)
        _sweep_orphan_attempt_workspaces(state_root, root_task_id, state)
        save_root_replenishment_state(state_root, root_task_id, state)
        noop = _noop_result(state, tier)
        noop["attempts"] = recovered_attempts
        noop["gaps_closed"] = recovered_gaps_closed
        noop["waiting"] = state["waiting"]
        return attach_subtitle_result(noop)

    # Any durable video intent returned above after reconciliation; only
    # subtitle rows remain to be removed from this video-only loop.
    filtered: list[dict[str, Any]] = []
    for request in requests:
        rows = [row for row in request.get("gaps", []) if isinstance(row, Mapping) and row.get("kind") != "missing_subtitle"]
        if rows:
            filtered.append({**request, "gaps": rows})
    requests = filtered
    reconcile_attempts: list[dict[str, Any]] = list(recovered_attempts)
    reconcile_closed: list[str] = list(recovered_gaps_closed)
    reconcile_waiting: str | None = None

    if not requests:
        waiting = merged_waiting(None)
        for entry in reconcile_attempts:
            _append_attempt_log(state, {"gap_id": entry["gap_id"], "tier": entry["tier"], "outcome": entry["outcome"], "candidate_key": entry.get("candidate_key"), "recorded_at": _now()})
        state.update({"updated_at": _now(), "waiting": waiting})
        _sync_video_in_flight(state)
        _sweep_orphan_attempt_workspaces(state_root, root_task_id, state)
        save_root_replenishment_state(state_root, root_task_id, state)
        return attach_subtitle_result({"tier": tier, "tier_before": tier, "requests_built": 0, "attempts": reconcile_attempts, "gaps_closed": reconcile_closed, "state": state, "waiting": waiting})

    requests_built = len(requests)
    attempts: list[dict[str, Any]] = list(reconcile_attempts)
    gaps_closed: list[str] = list(reconcile_closed)
    waiting: str | None = None

    hit_in_doubt = False
    hit_infrastructure = False
    hit_clean_incomplete = False
    failed_locators: set[str] = set()
    no_candidate_proofs: list[Mapping[str, Any]] = []
    searched_requests = 0
    paused_during_round = False
    shelf = _root_target_shelf(runner, root_task_id)

    for request in requests:
        if pause():
            paused_during_round = True
            break
        media = request.get("media") or {}
        media_type = str(media.get("media_type") or "").strip().casefold()
        tmdb_id = media.get("tmdb_id")

        by_token, by_unit = _open_gaps_by_token(
            state_root, root_task_id, media_type, tmdb_id,
        )

        request["tier"] = tier
        resource_miss_key = _search_resource_miss_request_key(tier, request)
        search_query_key = _search_query_request_key(tier, request)
        known_resource_misses = _known_search_resource_misses(
            state, resource_miss_key,
        )
        pansou_cursor = _known_pansou_query_cursor(state, resource_miss_key)
        if pansou_cursor is not None:
            request["pansou_query_cursor"] = pansou_cursor
        search_cursors = _known_search_query_cursors(state, search_query_key)
        if search_cursors:
            request["search_cursors"] = search_cursors
        reviewed_torrent_misses = _known_reviewed_torrent_misses(
            state, search_query_key,
        )
        if reviewed_torrent_misses:
            request["reviewed_torrent_miss_locators"] = reviewed_torrent_misses
        if known_resource_misses:
            request["reviewed_resource_miss_locators"] = known_resource_misses
        # Feed the durable per-tier candidate failures back as excluded
        # locators so a Quark-rejected candidate is never re-submitted.
        excluded = [
            {"locator": locator}
            for locator in (
                (state.get("candidate_failures_by_provider") or {}).get(tier) or []
            )
            if isinstance(locator, str) and locator
        ]
        if excluded:
            request["excluded_candidates"] = excluded
        _trace(f"select root={root_task_id} tier={tier} tmdb={tmdb_id} gaps={len(request.get('gaps') or [])}")
        try:
            bundle = gap_ledger_selection(
                state_root, root_task_id, request, search_runner=search_runner,
            )
        except Exception as exc:  # search boundary failure
            scope, task_id = _classify_error(exc)
            # A search exception has not inspected or submitted a concrete
            # locator.  Even if a lower adapter labels it ``candidate``, it
            # cannot establish no-candidate exhaustion, so retry this same
            # tier instead of leaving an unprovable silent stop.
            if scope != FAILURE_INFRASTRUCTURE:
                scope = FAILURE_INFRASTRUCTURE
                task_id = None
            for row in request.get("gaps") or []:
                token = str(row.get("id") or "")
                for gap in by_token.get(token, ()):
                    record_attempt(
                        state_root, root_task_id, gap.gap_id,
                        attempt_id=uuid.uuid4().hex,
                        provider=_TIER_PROVIDER.get(tier, tier),
                        tier=tier,
                        locator=None,
                        status=_attempt_status(scope),
                        external_task_id=task_id,
                        error=_safe_durable_error(exc),
                    )
                    attempts.append({
                        "gap_id": gap.gap_id,
                        "tier": tier,
                        "outcome": scope,
                    })
            hit_infrastructure = True
            continue

        if not isinstance(bundle, Mapping):
            bundle = {}
        searched_requests += 1
        # Advance the bounded PanSou window for every successful read-only
        # search, including rounds that returned a candidate.  Otherwise a
        # partially satisfied request would restart at the same deterministic
        # terms forever and could never prove later-window exhaustion.
        search_evidence = bundle.get("search_evidence")
        search_evidence = search_evidence if isinstance(search_evidence, Mapping) else None
        if search_evidence is not None:
            telemetry = search_evidence.get("source_telemetry")
            pansou = (
                (telemetry.get("pansou") or telemetry.get("PanSou"))
                if isinstance(telemetry, Mapping)
                else None
            )
            if isinstance(pansou, Mapping):
                _remember_pansou_query_cursor(
                    state, resource_miss_key, pansou.get("query_cursor"),
                )
            _remember_search_query_cursors(
                state,
                search_query_key,
                search_evidence.get("source_telemetry"),
            )
            _remember_reviewed_torrent_misses(
                state,
                search_query_key,
                _search_evidence_reviewed_torrent_misses(search_evidence),
            )
        if pause():
            # A completed read-only search is not permission to certify the
            # request or advance the tier after the operator pauses.
            paused_during_round = True
            break
        selections = bundle.get("selections")
        selections = selections if isinstance(selections, list) else []
        _trace(f"selected root={root_task_id} tier={tier} tmdb={tmdb_id} selections={len(selections)}")

        if not selections:
            evidence = bundle.get("search_evidence")
            evidence = evidence if isinstance(evidence, Mapping) else None
            _remember_search_resource_misses(
                state,
                resource_miss_key,
                _search_evidence_reviewed_resource_misses(tier, evidence),
            )
            proof_complete, completed_sources, configured_sources = _search_evidence_completion(
                tier,
                evidence,
                shelf=shelf,
            )
            if proof_complete:
                no_candidate_proofs.append({
                    "completed_sources": completed_sources,
                    "configured_sources": configured_sources,
                })
            else:
                # Search returned no selectable candidate but did not prove
                # the active tier exhausted.  This is never a candidate
                # exclusion: keep the tier and re-arm the same lane.
                clean_incomplete = _search_evidence_is_clean_incomplete(
                    tier,
                    evidence,
                    shelf=shelf,
                )
                if clean_incomplete:
                    hit_clean_incomplete = True
                    failure_scope = FAILURE_CANDIDATE
                else:
                    hit_infrastructure = True
                    failure_scope = FAILURE_INFRASTRUCTURE
                failure_detail = _incomplete_search_reason(evidence)
                _trace(
                    f"no exhaustion proof root={root_task_id} tier={tier} "
                    f"tmdb={tmdb_id}",
                )
            for row in request.get("gaps") or []:
                token = str(row.get("id") or "")
                for gap in by_token.get(token, ()):
                    if not proof_complete:
                        record_attempt(
                            state_root,
                            root_task_id,
                            gap.gap_id,
                            attempt_id=uuid.uuid4().hex,
                            provider=_TIER_PROVIDER.get(tier, tier),
                            tier=tier,
                            locator=None,
                            status=_attempt_status(failure_scope),
                            error=failure_detail,
                        )
                    attempts.append({
                        "gap_id": gap.gap_id,
                        "tier": tier,
                        "outcome": (
                            FAILURE_CANDIDATE
                            if proof_complete else failure_scope
                        ),
                        **({"error": failure_detail} if not proof_complete else {}),
                    })
            continue

        materializer = factory(tier)

        for selection in selections:
            if not isinstance(selection, Mapping):
                continue
            # Video tiers may not piggy-back an old torrent companion
            # subtitle.  RootJob's independent subtitle channel is the only
            # route allowed to stage and formally install the merged file.
            selection = _strip_video_companion_subtitle_members(selection)
            if pause():
                paused_during_round = True
                break
            selected_tokens = [
                str(gid) for gid in (selection.get("selected_gap_ids") or [])
                if isinstance(gid, str) and gid
            ]
            covered_gaps: list[Gap] = []
            for token in selected_tokens:
                covered_gaps.extend(by_token.get(token, ()))
            covered_gaps = list({
                gap.gap_id: gap for gap in covered_gaps
            }.values())
            if not covered_gaps:
                continue

            attempt_id = uuid.uuid4().hex
            try:
                intent = _new_video_intent(runner, state_root, root_task_id, attempt_id=attempt_id, tier=tier, request=request, selection=selection, covered_gaps=covered_gaps)
            except ValueError:
                hit_infrastructure = True
                attempts.extend({"gap_id": gap.gap_id, "tier": tier, "outcome": FAILURE_INFRASTRUCTURE} for gap in covered_gaps)
                continue
            locator, staging_root = str(intent["locator"]), str(intent["staging_root"])
            workspace = Path(str(intent["workspace"]))
            _write_video_intent(state_root, root_task_id, state, attempt_id, intent)
            if pause():
                _drop_video_intent(state_root, root_task_id, state, attempt_id)
                paused_during_round = True
                break
            try:
                workspace.mkdir(parents=True, exist_ok=True)
            except OSError:
                _drop_video_intent(state_root, root_task_id, state, attempt_id)
                for gap in covered_gaps:
                    record_attempt(state_root, root_task_id, gap.gap_id, attempt_id=attempt_id, provider=intent["provider"], tier=tier, locator=locator, status="infrastructure", error="补源本地 workspace 无法创建")
                    attempts.append({"gap_id": gap.gap_id, "tier": tier, "outcome": FAILURE_INFRASTRUCTURE})
                hit_infrastructure = True
                continue

            submitting = dict(intent)
            submitting["phase"] = "submitting"
            _write_video_intent(state_root, root_task_id, state, attempt_id, submitting)
            state["last_attempt_at"] = _now()
            for gap in covered_gaps:
                record_attempt(state_root, root_task_id, gap.gap_id, attempt_id=attempt_id, provider=intent["provider"], tier=tier, locator=locator, status="submitted")
            try:
                delivery = _call_materializer_with_pause(materializer.acquire, request, [selection], staging_root=staging_root, workspace=workspace, alist=runner.alist, pause_requested=materializer_pause)
            except Exception as exc:
                if _is_pause_error(exc):
                    _wait_video_intent(state_root, root_task_id, state, submitting)
                    paused_during_round = True
                    break
                scope, task_id = _classify_error(exc)
                if scope == FAILURE_CANDIDATE:
                    _drop_video_intent(state_root, root_task_id, state, attempt_id)
                    _remove_attempt_workspace_tree(intent.get("workspace"))
                    # Keep the provider's bounded, redacted diagnostic in the
                    # attempt ledger.  A generic label such as "provider
                    # rejected" is not enough to distinguish a bad torrent
                    # manifest, payload/ffprobe rejection, or an explicitly
                    # refused candidate during recovery.  The helper strips
                    # credentials/URLs' query secrets and caps the durable
                    # value, so this adds evidence without widening the
                    # persisted secret surface.
                    candidate_error = _safe_durable_error(
                        exc,
                        fallback="补源候选被 provider 明确拒绝",
                    )
                    for gap in covered_gaps:
                        record_attempt(
                            state_root,
                            root_task_id,
                            gap.gap_id,
                            attempt_id=attempt_id,
                            provider=intent["provider"],
                            tier=tier,
                            locator=locator,
                            status="candidate_failed",
                            error=candidate_error,
                        )
                        attempts.append({"gap_id": gap.gap_id, "tier": tier, "outcome": FAILURE_CANDIDATE, "candidate_key": locator})
                    failed_locators.add(locator)
                    continue
                _wait_video_intent(state_root, root_task_id, state, submitting, task_id=task_id)
                for gap in covered_gaps:
                    record_attempt(state_root, root_task_id, gap.gap_id, attempt_id=attempt_id, provider=intent["provider"], tier=tier, locator=locator, status="in_doubt", external_task_id=task_id, error="补源 provider 响应未证明；等待对账")
                    attempts.append({"gap_id": gap.gap_id, "tier": tier, "outcome": FAILURE_IN_DOUBT, "candidate_key": locator})
                hit_in_doubt = True
                break

            delivery = _video_delivery(delivery, submitting)
            if delivery is None:
                _wait_video_intent(state_root, root_task_id, state, submitting)
                for gap in covered_gaps:
                    record_attempt(state_root, root_task_id, gap.gap_id, attempt_id=attempt_id, provider=intent["provider"], tier=tier, locator=locator, status="in_doubt", error="补源 delivery 无法证明属于当前 attempt")
                    attempts.append({"gap_id": gap.gap_id, "tier": tier, "outcome": FAILURE_IN_DOUBT, "candidate_key": locator})
                hit_in_doubt = True
                break
            staged = dict(submitting)
            staged.update({"phase": "staged", "delivery": delivery})
            task_id = _safe_task_id(delivery.get("external_task_id"))
            if task_id is not None:
                staged["external_task_id"] = task_id
            _write_video_intent(state_root, root_task_id, state, attempt_id, staged)
            continued = _resume_video_intent(runner, state_root, root_task_id, state, staged, pause_requested=materializer_pause)
            attempts.extend(continued["attempts"])
            gaps_closed.extend(continued["gaps_closed"])
            if continued["outcome"] == "paused":
                paused_during_round = True
                break
            if continued["outcome"] == "waiting_reconcile":
                hit_in_doubt = True
                break

        if hit_in_doubt:
            break
        if pause():
            paused_during_round = True
            break

    # Tier progression / waiting (contract rule 4).
    # Candidate-locator memory is recorded FIRST so a mixed run (one
    # infrastructure failure plus several rejected candidates) never loses
    # the rejected locators.  A pause, incomplete search proof, or in-doubt
    # task can land after a materializer reports a candidate failure: retain
    # that evidence, but defer the policy update so the 30-locator exhaustion
    # threshold cannot promote a tier while the round is not fully clean.
    if pause():
        paused_during_round = True
    defer_candidate_transition = (
        paused_during_round
        or hit_infrastructure
        or hit_clean_incomplete
        or hit_in_doubt
        or reconcile_waiting in {"retry_wait", "waiting_reconcile"}
    )
    if defer_candidate_transition:
        for locator in sorted(failed_locators):
            _exclude_locator(state, tier, locator)
    else:
        for locator in sorted(failed_locators):
            state = apply_tier_outcome(state, {
                "scope": FAILURE_CANDIDATE,
                "locator": locator,
            })
    if (
        hit_in_doubt
        or _video_intents(state)
        or reconcile_waiting == "waiting_reconcile"
    ):
        waiting = "waiting_reconcile"
        if hit_in_doubt or _video_intents(state):
            state = apply_tier_outcome(state, {
                "scope": FAILURE_IN_DOUBT,
            })
    elif hit_infrastructure or reconcile_waiting == "retry_wait":
        waiting = "retry_wait"
        state = apply_tier_outcome(state, {"scope": FAILURE_INFRASTRUCTURE})
    elif hit_clean_incomplete:
        # A bounded, healthy discovery pass is neither a provider outage nor
        # an exhaustion proof.  Keep the same tier without invoking the pure
        # candidate transition (which could otherwise consume unrelated
        # historical locator failures while another share page is unchecked).
        waiting = "retry_wait"
        state["last_error_scope"] = FAILURE_CANDIDATE
        state["status"] = "candidate_failed"
    elif (
        requests_built > 0
        and not paused_during_round
        and not pause()
        and searched_requests == requests_built
        and len(no_candidate_proofs) == requests_built
    ):
        # No selector result is allowed to manufacture this outcome.  Every
        # request independently supplied a complete raw-search proof above;
        # only then can their already-observed source evidence be aggregated
        # into the policy transition.
        completed_sources = sorted({
            str(source).strip().casefold()
            for proof in no_candidate_proofs
            for source in (proof.get("completed_sources") or [])
            if isinstance(source, str) and source.strip()
        })
        configured_sources = sorted({
            str(source).strip().casefold()
            for proof in no_candidate_proofs
            for source in (proof.get("configured_sources") or [])
            if isinstance(source, str) and source.strip()
        })
        # The preceding check closes the normal loop boundary; this one is
        # deliberately adjacent to the durable state transition so a pause
        # that arrives while the aggregate proof is being assembled cannot
        # promote the next tier.
        if pause():
            paused_during_round = True
        else:
            state = apply_tier_outcome(state, {
                "scope": FAILURE_CANDIDATE,
                "search_complete_no_candidates": True,
                "completed_sources": completed_sources,
                "configured_sources": configured_sources,
                "unchecked_secondary_candidates": 0,
                **({"shelf": shelf} if shelf is not None else {}),
            })

    waiting = merged_waiting(waiting)
    state["updated_at"] = _now()
    state["waiting"] = waiting
    _sync_video_in_flight(state)
    _sweep_orphan_attempt_workspaces(state_root, root_task_id, state)
    _trace(f"end root={root_task_id} tier={state.get('tier')} waiting={waiting} closed={len(gaps_closed)}")
    for entry in attempts:
        _append_attempt_log(state, {
            "gap_id": entry["gap_id"],
            "tier": entry["tier"],
            "outcome": entry["outcome"],
            "candidate_key": entry.get("candidate_key"),
            "error": entry.get("error"),
            "recorded_at": _now(),
        })
    save_root_replenishment_state(state_root, root_task_id, state)

    return attach_subtitle_result({
        "tier": str(state.get("tier") or tier),
        "tier_before": tier,
        "requests_built": requests_built,
        "attempts": attempts,
        "gaps_closed": gaps_closed,
        "state": state,
        "waiting": waiting,
    })


__all__ = [
    "load_root_replenishment_state",
    "run_root_replenishment",
    "save_root_replenishment_state",
]
