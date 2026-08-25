"""P14: translate the new-path Gap ledger into runtime replenishment requests.

The new architecture (``IntakeSource -> RootJob -> WorkUnit``) registers gaps in
``gap_ledger_<root_task_id>.json`` (see ``engine.scrapeflow.gap_ledger``), while
the automatic replenishment runtime and its adapters already accept a legacy
request shape built by ``local.scrapeflow_api.replenishment`` from a scan
report.  This module is the bridge between the two: it turns the durable Gap
ledger into the runtime request shape and wires the existing search + selection
boundaries without touching either legacy path.

Request field contract (one request per ``(media_type, tmdb_id)`` identity):

    {
        "tier": "magnet",                      # legacy selector shape only;
                                                 # root runtime overwrites this
                                                 # with its durable current tier
        "media": {
            "tmdb_id": <int>,                  # positive TMDB id
            "media_type": "movie" | "tv",      # gap ledger media_type
            "title": <str>,                    # work unit identity.title
            "original_title": <str>,           # identity field or C/TMDB formal trace ("" if absent)
            "aliases": [<str>, ...],           # persisted C/TMDB title evidence
        },
        "gaps": [
            {
                "id": <str>,                   # gap_id for media/subtitle;
                                               # S{season:02d}E{ep:02d}(-E{ep:02d}) for episodes;
                                               # S{season:02d} for a whole season
                "kind": "missing_episode" | "missing_season"
                        | "missing_media" | "missing_subtitle",
                "season": <int> | None,
                "episodes": [<int>, ...],      # episode ordinals; [] for non-episode kinds
                "title": <str>,                # media title for media/subtitle; "" otherwise
                # subtitle lane only:
                "path": <str>,                 # target video path (subtitle_path)
                "subtitle_language": <str>,
            },
            ...
        ],
        "excluded_candidates": [],             # always empty here; the runtime owns
                                               # durable exclusion memory
        # "rules" is deliberately NOT set.  The runtime selection boundary does
        # not need the legacy optional-discovery/quality-ladder rule block.
    }

``rules`` is omitted so that the request is a data projection of the Gap ledger
rather than a re-import of legacy plan policy.  The selection boundary
(``select_replenishment_candidates``) is written to accept this minimal shape:
``media.tmdb_id`` for identity, ``media.aliases`` (falling back to
``media.title``) for the title gate, and ``gaps[].id``/``kind``/``season`` for
coverage.  ``media.aliases`` must never be an empty list, because the selector
treats an explicit empty list as "no alias evidence" and rejects every
candidate; this module therefore always seeds it with ``title`` and reuses
durable C/TMDB title evidence when it is present.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
import re
from pathlib import Path
from typing import Any

from engine.scrapeflow.gap_ledger import Gap, load_gap_ledger
from engine.scrapeflow.work_units import load_work_unit_records
from engine.tools.replenishment_adapter.search import search as _default_search

from .replenishment import select_replenishment_candidates
from .replenishment_tiers import TIER_LOCAL_MAGNET


_CANONICAL_QUARK_SHARE_LOCATOR_RE = re.compile(
    r"\Aquark_share:[A-Za-z0-9_-]{6,128}\Z",
)
_CANONICAL_TORRENT_LOCATOR_RE = re.compile(
    r"\Atorrent:(?:[0-9a-f]{40}|[a-z2-7]{32})\Z",
    re.IGNORECASE,
)
_MAX_REVIEWED_RESOURCE_MISSES = 512
_MAX_REVIEWED_TORRENT_MISSES = 512
_MAX_MEDIA_ALIASES = 40
_SAFE_SOURCE_FAILURE_CODES = frozenset({
    "connection_refused",
    "connection_reset",
    "dns_failure",
    "network_error",
    "os_error",
    "runtime_error",
    "source_error",
    "timeout",
    "tls_failure",
    "unknown_error",
    "value_error",
    "xml_parse_error",
})
_HTTP_SOURCE_FAILURE_CODE_RE = re.compile(
    r"\Ahttp_(?:1\d\d|2\d\d|3\d\d|4\d\d|5\d\d)\Z",
)


def _nonempty_strings(values: Sequence[Any], *, limit: int = 40) -> list[str]:
    """Deduplicate trimmed non-empty strings, preserving first-seen order."""
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        output.append(text)
        if len(output) >= limit:
            break
    return output


def _identity_original_title(identity: Mapping[str, Any]) -> str:
    """Return C's durable TMDB original title without a fresh TMDB read.

    Newer identity rows may persist ``original_title`` directly.  Older rows
    deliberately retain the resolver's formal TMDB evidence in
    ``decision_trace.official_titles`` instead.  The resolver builds that
    list from TMDB's localized title/name followed by its original
    title/name, preserving that order.  Recover the second formal title when
    available (or the sole formal title when TMDB exposed only one), rather
    than falling back to a directory label or a provider/web result.

    This makes the generic-search request fingerprint stable when a later
    best-effort TMDB detail enrichment is temporarily unavailable.
    """
    explicit = identity.get("original_title")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    trace = identity.get("decision_trace")
    if not isinstance(trace, Mapping):
        return ""
    raw_titles = trace.get("official_titles")
    if not isinstance(raw_titles, (list, tuple)):
        return ""
    titles = _nonempty_strings(raw_titles, limit=2)
    if not titles:
        return ""
    return titles[1] if len(titles) > 1 else titles[0]


def _identity_media_aliases(
    identity: Mapping[str, Any],
    *,
    title: str,
    original_title: str,
) -> list[str]:
    """Project only persisted C/TMDB title evidence into provider aliases.

    Replenishment must search the exact identity already accepted by C, not a
    new directory-name guess or a web-search result. ``official_titles`` and
    ``aliases_checked`` are the title evidence saved by the TMDB resolver in
    ``decision_trace``; they commonly include the original-script and
    international release names absent from the display title. Ignore all
    other trace fields and malformed values, then preserve stable order with a
    hard limit so one identity cannot broaden provider queries indefinitely.
    """
    trace = identity.get("decision_trace")
    trace_titles: list[Any] = []
    if isinstance(trace, Mapping):
        for key in ("official_titles", "aliases_checked"):
            raw = trace.get(key)
            if isinstance(raw, (list, tuple)):
                trace_titles.extend(
                    value for value in raw if isinstance(value, str)
                )
    raw_aliases = identity.get("aliases")
    identity_aliases = (
        [value for value in raw_aliases if isinstance(value, str)]
        if isinstance(raw_aliases, (list, tuple))
        else []
    )
    return _nonempty_strings([
        title,
        original_title,
        *identity_aliases,
        *trace_titles,
    ], limit=_MAX_MEDIA_ALIASES)


def _source_name(value: object) -> str:
    """Normalize a search-source label to the tier-policy spelling."""
    if not isinstance(value, str):
        return ""
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def _nonnegative_int(value: object, *, fallback: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return fallback


def _source_failure_types(value: object) -> dict[str, int]:
    """Project only closed, non-sensitive source-health failure codes."""
    if not isinstance(value, Mapping):
        return {}
    output: dict[str, int] = {}
    for raw_code, raw_count in value.items():
        if not isinstance(raw_code, str) or type(raw_count) is not int:
            continue
        count = max(0, min(raw_count, 100_000))
        if count <= 0:
            continue
        code = raw_code.strip().casefold()
        if (
            code not in _SAFE_SOURCE_FAILURE_CODES
            and _HTTP_SOURCE_FAILURE_CODE_RE.fullmatch(code) is None
        ):
            code = "source_error"
        output[code] = min(100_000, output.get(code, 0) + count)
    return dict(sorted(output.items()))


def _validated_query_cursor(value: object) -> dict[str, Any] | None:
    """Keep a provider-neutral bounded continuation receipt.

    PanSou uses ``offset`` while AnimeTosho uses ``term_index``/``page``.
    Only the exact request fingerprint and small integer coordinates cross
    this boundary; no provider URL, query text, or diagnostics are retained.
    """
    if not isinstance(value, Mapping):
        return None
    fingerprint = value.get("fingerprint")
    exhausted = value.get("exhausted")
    if (
        not isinstance(fingerprint, str)
        or re.fullmatch(r"[a-f0-9]{64}", fingerprint) is None
        or not isinstance(exhausted, bool)
    ):
        return None
    output: dict[str, Any] = {
        "fingerprint": fingerprint,
        "exhausted": exhausted,
    }
    offset = value.get("offset")
    if (
        isinstance(offset, int)
        and not isinstance(offset, bool)
        and 0 <= offset <= 256
    ):
        output["offset"] = offset
        return output
    term_index = value.get("term_index")
    page = value.get("page")
    if (
        isinstance(term_index, int)
        and not isinstance(term_index, bool)
        and 0 <= term_index <= 256
        and isinstance(page, int)
        and not isinstance(page, bool)
        and 1 <= page <= 256
    ):
        output["term_index"] = term_index
        output["page"] = page
        return output
    return None


def _reviewed_resource_miss_locators(value: object) -> list[str]:
    """Project only non-secret canonical Quark locators from telemetry.

    These values are evidence that a particular share was successfully
    inspected and was unusable for the current request.  URLs, passcodes and
    arbitrary provider diagnostics never cross this bridge.
    """
    if not isinstance(value, list):
        return []
    return sorted({
        item
        for item in value[:_MAX_REVIEWED_RESOURCE_MISSES]
        if isinstance(item, str)
        and _CANONICAL_QUARK_SHARE_LOCATOR_RE.fullmatch(item) is not None
    })


def _reviewed_torrent_miss_locators(value: object) -> list[str]:
    """Project only infohash locators for validated non-covering manifests.

    A torrent URL is intentionally not durable evidence: it can be mutable,
    secret-bearing, or point at a different metainfo object later.  The
    adapter emits this locator only after fetching and validating the torrent
    manifest and proving that it covers none of the current exact gaps.
    """
    if not isinstance(value, list):
        return []
    return sorted({
        item.casefold()
        for item in value[:_MAX_REVIEWED_TORRENT_MISSES]
        if isinstance(item, str)
        and _CANONICAL_TORRENT_LOCATOR_RE.fullmatch(item) is not None
    })


def _search_completion_evidence(
    result: Mapping[str, Any],
    selection: Mapping[str, Any],
) -> dict[str, Any]:
    """Project raw search completion facts alongside a selection bundle.

    A blank selection is not itself evidence that the current lane is
    exhausted: it can also be caused by a disabled source, a timeout, or a
    pause between requests.  Keep the search result's completion/telemetry
    facts separate from selection diagnostics so the root orchestrator can
    advance only when the *original* search actually proved no candidates.
    """
    # Preserve explicit completion claims and compact raw telemetry separately.
    # The root knows the active tier's shelf-scoped required-source set; it is
    # the only layer allowed to decide which telemetry rows count toward a
    # transition.  In particular, an unrelated source's failure must not
    # poison a complete proof for the current tier.
    completed = {
        name
        for value in (result.get("completed_sources") or [])
        if (name := _source_name(value))
    } if isinstance(result.get("completed_sources"), list) else set()
    infrastructure_failure = (
        str(result.get("failure_scope") or "").strip().casefold()
        not in {"", "candidate"}
    )
    telemetry = result.get("source_telemetry")
    source_telemetry: dict[str, dict[str, Any]] = {}
    if isinstance(telemetry, Mapping):
        for source, raw in telemetry.items():
            name = _source_name(source)
            if not name or not isinstance(raw, Mapping):
                continue
            failures = _nonnegative_int(
                raw.get("infrastructure_failures"), fallback=0,
            )
            failure_types = _source_failure_types(
                raw.get("infrastructure_failure_types"),
            )
            failures = max(failures, sum(failure_types.values()))
            facts = source_telemetry.setdefault(name, {
                "source_exhausted": False,
                "infrastructure_failures": 0,
            })
            facts["source_exhausted"] = (
                facts["source_exhausted"] is True
                or raw.get("source_exhausted") is True
            )
            facts["infrastructure_failures"] = max(
                int(facts["infrastructure_failures"]), failures,
            )
            if failure_types:
                existing_types = facts.get("infrastructure_failure_types")
                merged_types = (
                    dict(existing_types)
                    if isinstance(existing_types, Mapping)
                    else {}
                )
                for code, count in failure_types.items():
                    merged_types[code] = min(
                        100_000,
                        int(merged_types.get(code, 0)) + count,
                    )
                facts["infrastructure_failure_types"] = dict(sorted(merged_types.items()))
            # These are closed, provider-neutral booleans/enums rather than
            # upstream warning text.  The root orchestrator can therefore
            # explain a retry_wait without persisting share URLs, passcodes,
            # or arbitrary provider diagnostics.
            if raw.get("configured") is False:
                facts["configured"] = False
            elif raw.get("configured") is True and "configured" not in facts:
                facts["configured"] = True
            # A source is considered "run" only when its bounded query and
            # response counters are present and positive.  Carry these closed
            # counters across the bridge so the root can reject a fabricated
            # exhausted flag from a source that never actually made a query.
            for counter in ("query_attempts", "query_responses"):
                value = _nonnegative_int(raw.get(counter), fallback=-1)
                if value >= 0:
                    facts[counter] = max(int(facts.get(counter, 0)), value)
            status = raw.get("status")
            if status in {"complete", "incomplete"}:
                facts["status"] = status
            reviewed_misses = _reviewed_resource_miss_locators(
                raw.get("reviewed_resource_miss_locators"),
            )
            if reviewed_misses:
                existing = facts.get("reviewed_resource_miss_locators")
                facts["reviewed_resource_miss_locators"] = sorted({
                    *(
                        existing
                        if isinstance(existing, list)
                        else []
                    ),
                    *reviewed_misses,
                })[:_MAX_REVIEWED_RESOURCE_MISSES]
            reviewed_torrent_misses = _reviewed_torrent_miss_locators(
                raw.get("reviewed_torrent_miss_locators"),
            )
            if reviewed_torrent_misses:
                existing = facts.get("reviewed_torrent_miss_locators")
                facts["reviewed_torrent_miss_locators"] = sorted({
                    *(
                        existing
                        if isinstance(existing, list)
                        else []
                    ),
                    *reviewed_torrent_misses,
                })[:_MAX_REVIEWED_TORRENT_MISSES]
            # Carry only the bounded deterministic-query cursor.  The raw
            # PanSou response may contain provider URLs or diagnostics; the
            # cursor is the sole state needed to resume the next read-only
            # window and is validated before crossing this bridge.
            raw_cursor = _validated_query_cursor(raw.get("query_cursor"))
            if raw_cursor is not None:
                facts["query_cursor"] = raw_cursor

    selector_unchecked = _nonnegative_int(
        selection.get("unchecked_current_tier_candidate_count"), fallback=1,
    )
    raw_unchecked = _nonnegative_int(
        result.get("unchecked_secondary_candidates"), fallback=selector_unchecked,
    )
    unchecked = max(selector_unchecked, raw_unchecked)
    eligible = _nonnegative_int(
        selection.get("eligible_current_tier_candidate_count"), fallback=1,
    )
    raw_completion = (
        result.get("search_complete_no_candidates") is True
        or result.get("search_complete") is True
    )
    return {
        "scope": "infrastructure" if infrastructure_failure else "candidate",
        "search_complete_no_candidates": bool(
            raw_completion
            and eligible == 0
            and unchecked == 0
            and not infrastructure_failure
        ),
        "completed_sources": sorted(completed),
        "unchecked_secondary_candidates": unchecked,
        "source_telemetry": source_telemetry,
    }


def _episode_gap_id(season: int, episodes: Sequence[int]) -> str | None:
    """Render one contiguous episode coordinate as the canonical SxxEyy form."""
    ordered = sorted({int(item) for item in episodes if isinstance(item, int) and item > 0})
    if not ordered:
        return None
    if len(ordered) == 1:
        return f"S{season:02d}E{ordered[0]:02d}"
    return f"S{season:02d}E{ordered[0]:02d}-E{ordered[-1]:02d}"


def _work_unit_identities(
    state_root: Path, root_task_id: str,
) -> tuple[dict[str, Mapping[str, Any]], dict[tuple[str, int], Mapping[str, Any]]]:
    """Index confirmed work-unit identities by unit id and by TMDB identity."""
    identity_by_unit: dict[str, Mapping[str, Any]] = {}
    identity_by_key: dict[tuple[str, int], Mapping[str, Any]] = {}
    for record in load_work_unit_records(state_root, root_task_id):
        identity = record.identity
        if not isinstance(identity, Mapping):
            continue
        identity_by_unit[record.work_unit_id] = identity
        media_type = str(identity.get("media_type") or "").strip().casefold()
        tmdb_id = identity.get("tmdb_id")
        if (
            media_type in {"movie", "tv"}
            and isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and tmdb_id > 0
        ):
            identity_by_key.setdefault((media_type, tmdb_id), identity)
    return identity_by_unit, identity_by_key


def _gap_row(gap: Gap, *, title: str) -> dict[str, Any] | None:
    """Project one ledger Gap into the runtime gap row; ``None`` if unusable."""
    kind = gap.kind
    if kind == "missing_episode":
        if not isinstance(gap.season, int) or isinstance(gap.season, bool) or gap.season < 0:
            return None
        episodes = tuple(
            sorted({
                int(item) for item in gap.episodes
                if isinstance(item, int) and not isinstance(item, bool) and item > 0
            })
        )
        gap_id = _episode_gap_id(gap.season, episodes)
        if gap_id is None:
            return None
        return {
            "id": gap_id,
            "kind": kind,
            "season": gap.season,
            "episodes": list(episodes),
            "title": "",
        }
    if kind == "missing_season":
        if not isinstance(gap.season, int) or isinstance(gap.season, bool) or gap.season < 0:
            return None
        return {
            "id": f"S{gap.season:02d}",
            "kind": kind,
            "season": gap.season,
            "episodes": [],
            "title": "",
        }
    if kind == "missing_media":
        return {
            "id": gap.gap_id,
            "kind": kind,
            "season": None,
            "episodes": [],
            "title": title,
        }
    if kind == "missing_subtitle":
        row: dict[str, Any] = {
            "id": gap.gap_id,
            "kind": kind,
            "season": None,
            "episodes": [],
            "title": title,
        }
        if gap.subtitle_path:
            row["path"] = gap.subtitle_path
        if gap.subtitle_language:
            row["subtitle_language"] = gap.subtitle_language
        return row
    return None


def gap_ledger_requests(
    state_root: str | Path,
    root_task_id: str,
) -> list[dict[str, Any]]:
    """Build one runtime request per open-gap ``(media_type, tmdb_id)`` identity.

    Open gaps are read from ``gap_ledger_<root_task_id>.json``; title evidence
    is read from each confirmed work unit's ``identity`` (including its
    persisted C/TMDB ``decision_trace`` title aliases) in
    ``work_units_<root_task_id>.json``. Closed gaps and gaps whose kind cannot
    be projected are skipped. The result is deterministic: identities sort by
    ``(media_type, tmdb_id)`` and gaps by ``(work_unit_id, kind, gap_id)``.
    """
    state_root = Path(state_root)
    identities_by_unit, identities_by_key = _work_unit_identities(
        state_root, root_task_id,
    )

    grouped: dict[tuple[str, int], list[Gap]] = defaultdict(list)
    for gap in load_gap_ledger(state_root, root_task_id):
        if gap.status != "open":
            continue
        key = (str(gap.media_type).strip().casefold(), gap.tmdb_id)
        grouped[key].append(gap)

    requests: list[dict[str, Any]] = []
    for (media_type, tmdb_id) in sorted(grouped):
        identity = identities_by_key.get((media_type, tmdb_id))
        if identity is None:
            # The identity projection may lack a canonical (media_type, tmdb_id)
            # row (for example an operator override).  Fall back to the gap's
            # own work unit identity before giving up on a title.
            for gap in grouped[(media_type, tmdb_id)]:
                candidate = identities_by_unit.get(gap.work_unit_id)
                if isinstance(candidate, Mapping):
                    identity = candidate
                    break
        identity = identity if isinstance(identity, Mapping) else {}

        title = str(identity.get("title") or "").strip()
        original_title = _identity_original_title(identity)
        aliases = _identity_media_aliases(
            identity,
            title=title,
            original_title=original_title,
        )

        gap_rows: list[dict[str, Any]] = []
        for gap in sorted(
            grouped[(media_type, tmdb_id)],
            key=lambda item: (item.work_unit_id, item.kind, item.gap_id),
        ):
            row = _gap_row(gap, title=title)
            if row is None:
                continue
            gap_rows.append(row)
        if not gap_rows:
            continue

        requests.append({
            "tier": TIER_LOCAL_MAGNET,
            "media": {
                "tmdb_id": tmdb_id,
                "media_type": media_type,
                "title": title,
                "original_title": original_title,
                "aliases": aliases,
            },
            "gaps": gap_rows,
            "excluded_candidates": [],
        })
    return requests


def gap_ledger_selection(
    state_root: str | Path,
    root_task_id: str,
    request: Mapping[str, Any],
    *,
    search_runner: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run search + selection for one bridged request and return the bundle.

    ``search_runner`` is injectable for tests (the ``search`` module also
    exposes ``ReplenishmentSearchService(runner=...)`` for the same purpose).
    When omitted, the real ``engine.tools.replenishment_adapter.search.search``
    boundary runs.

    ``state_root``/``root_task_id`` are accepted for symmetry with
    ``gap_ledger_requests`` and are reserved for the integration phase, where
    the caller will record ``AcquisitionAttempt`` rows and close gaps after the
    staging re-audit proves a coordinate present:

        # integration-phase hook (NOT wired here):
        #   engine.scrapeflow.gap_ledger.record_attempt(
        #       state_root, root_task_id, gap_id, ...)
        #   engine.scrapeflow.gap_ledger.close_gap(
        #       state_root, root_task_id, gap_id)

    The returned bundle maps ``selected_gap_ids`` back to ledger ``gap_id`` via
    the ``SxxEyy`` token (episodes) or the whole ``gap_id`` (media/subtitle);
    the integrator resolves those back through the gap ledger to close gaps.
    It also includes ``search_evidence`` derived from the *raw* result's
    completion and telemetry fields.  Callers must not infer exhaustion from
    an empty ``selections`` list alone.
    """
    del state_root, root_task_id  # reserved for the integration-phase hooks above

    runner = search_runner if search_runner is not None else _default_search
    result = runner(request)
    if not isinstance(result, Mapping):
        raise TypeError("补源搜索结果必须是对象")
    raw_candidates = result.get("candidates")
    candidates = raw_candidates if isinstance(raw_candidates, list) else []
    selection = select_replenishment_candidates(request, candidates)
    selection["search_evidence"] = _search_completion_evidence(result, selection)
    return selection


__all__ = [
    "gap_ledger_requests",
    "gap_ledger_selection",
]
