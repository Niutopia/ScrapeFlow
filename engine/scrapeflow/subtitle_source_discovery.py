"""Read-only provider discovery for unmatched subtitle members.

Discovery records complete source manifests only.  It never saves a Quark
share, starts an offline task, downloads a torrent payload, or selects video
members.  Video rows are retained solely as identity witnesses for the later
subtitle-only planner.
"""

from __future__ import annotations

import base64
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET

from engine.scrapeflow.subtitle_member_acquisition import (
    _title_key,
    bind_source_manifest,
    validate_source_manifest,
)


FetchBytes = Callable[..., bytes]
DownloadTorrent = Callable[..., Mapping[str, Any]]


class _TorrentAnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[dict[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a" or self._href is not None:
            return
        href = dict(attrs).get("href")
        if isinstance(href, str):
            self._href = href
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() == "a" and self._href is not None:
            self.anchors.append({
                "href": self._href, "text": " ".join(self._text).strip(),
            })
            self._href = None
            self._text = []


def _infohash_aliases(value: Any) -> set[str]:
    raw = str(value or "").strip().casefold()
    if re.fullmatch(r"[0-9a-f]{40}", raw):
        return {raw, base64.b32encode(bytes.fromhex(raw)).decode().rstrip("=").casefold()}
    if re.fullmatch(r"[a-z2-7]{32}", raw):
        try:
            return {raw, base64.b32decode(raw.upper()).hex()}
        except ValueError:
            return {raw}
    return {raw} if raw else set()


def _tokyotosho_rows(page: bytes, base_url: str) -> list[tuple[str, str, str]]:
    parser = _TorrentAnchorParser()
    parser.feed(page.decode("utf-8", "replace"))
    rows: list[tuple[str, str, str]] = []
    pending_infohash = ""
    for anchor in parser.anchors:
        href = str(anchor.get("href") or "").strip()
        if href.casefold().startswith("magnet:"):
            match = re.search(
                r"(?i)(?:urn:)?btih:([0-9a-f]{40}|[a-z2-7]{32})\b", href,
            )
            pending_infohash = match.group(1).casefold() if match else ""
            continue
        torrent_url = urllib.parse.urljoin(base_url + "/", href)
        parsed = urllib.parse.urlsplit(torrent_url)
        if (
            parsed.scheme == "https" and parsed.hostname
            and parsed.path.casefold().endswith(".torrent")
        ):
            rows.append((
                str(anchor.get("text") or "").strip(),
                torrent_url, pending_infohash,
            ))
            pending_infohash = ""
    return rows


def _provider_request(batch: Mapping[str, Any]) -> dict[str, Any]:
    title = str(batch.get("title") or "").strip()
    raw_aliases = batch.get("aliases")
    if raw_aliases is None:
        raw_aliases = []
    if not isinstance(raw_aliases, list) or not all(
        isinstance(value, str) and value.strip() for value in raw_aliases
    ):
        raise ValueError("subtitle discovery batch has invalid title aliases")
    search_titles = list(dict.fromkeys([
        title, *(value.strip() for value in raw_aliases),
    ]))[:2]
    terms = batch.get("query_terms")
    if not title or not isinstance(terms, list) or not all(
        isinstance(value, str) and value.strip() for value in terms
    ):
        raise ValueError("subtitle discovery batch has invalid search terms")
    raw_terms = list(dict.fromkeys(value.strip() for value in terms))
    source_identity_terms = []
    generic_title_keys = {_title_key(value) for value in search_titles}
    by_season: dict[int, set[int]] = {}
    for value in raw_terms:
        matches = [
            (int(match.group(1)), int(match.group(2)))
            for match in re.finditer(r"(?i)\bS(\d{1,2})E(\d{1,4})\b", value)
        ]
        if not matches:
            continue
        prefix = re.split(r"(?i)\bS\d{1,2}E\d{1,4}\b", value, maxsplit=1)[0].strip()
        if prefix and _title_key(prefix) not in generic_title_keys:
            source_identity_terms.append(value)
        for season, episode in matches:
            by_season.setdefault(season, set()).add(episode)
    compact_terms = list(search_titles)
    for season, episodes in sorted(by_season.items()):
        compact_terms.extend(
            f"{search_title} S{season:02d}" for search_title in search_titles
        )
        identity_title = search_titles[-1]
        if season == 0:
            compact_terms.extend((
                f"{identity_title} OVA", f"{identity_title} Special",
            ))
        ordered = sorted(episodes)
        if len(ordered) <= 3:
            compact_terms.extend(
                f"{identity_title} S{season:02d}E{episode:02d}"
                for episode in ordered
            )
        else:
            compact_terms.append(
                f"{identity_title} S{season:02d}E{ordered[0]:02d}-E{ordered[-1]:02d}"
            )
    if not compact_terms:
        compact_terms = raw_terms[:8]
    compact_terms = list(dict.fromkeys([*source_identity_terms[:4], *compact_terms]))[:12]
    return {
        "media": {"title": title, "aliases": search_titles[1:]},
        "search_queries": compact_terms,
        "original_query_count": len(raw_terms),
        "query_groups": [],
    }


def discover_quark_manifests(
    batch: Mapping[str, Any], *,
    search_links: Callable[[Mapping[str, Any]], tuple[Sequence[Mapping[str, Any]], Mapping[str, Any]]],
    inspect_share: Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]],
    existing_locators: Iterable[str] = (),
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Search and recursively inspect Quark shares without saving anything."""
    request_ids = batch.get("request_ids")
    if not isinstance(request_ids, list) or not all(isinstance(value, str) for value in request_ids):
        raise ValueError("subtitle discovery batch has invalid request ids")
    links, raw_telemetry = search_links(_provider_request(batch))
    excluded = set(existing_locators)
    manifests: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    resource_failed_locators: list[str] = []
    infrastructure_failures = 0
    inspected = 0
    for row in links:
        share_id = str(row.get("share_id") or "")
        locator = f"quark_share:{share_id}"
        if not share_id or locator in excluded:
            continue
        try:
            files = [dict(value) for value in inspect_share(row)]
            manifest = bind_source_manifest(
                provider="quark_share",
                locator=locator,
                release_name=str(row.get("release_name") or batch.get("title") or share_id),
                search_request_ids=request_ids,
                files=files,
                acquisition={
                    "share_id": share_id,
                    "share_url": str(row.get("share_url") or f"https://pan.quark.cn/s/{share_id}"),
                    "passcode": str(row.get("passcode") or ""),
                },
            )
            manifests.append(validate_source_manifest(manifest))
            inspected += 1
        except Exception as exc:  # provider rows are isolated evidence
            scope = str(getattr(
                exc, "failure_scope",
                "candidate" if isinstance(exc, ValueError) else "infrastructure",
            ))
            failures.append({
                "locator": locator, "error": type(exc).__name__, "scope": scope,
            })
            if scope == "candidate":
                resource_failed_locators.append(locator)
            else:
                infrastructure_failures += 1
    telemetry = {
        **dict(raw_telemetry),
        "provider": "quark_share",
        "inspected": inspected,
        "manifest_count": len(manifests),
        "failures": failures,
        "resource_failed_locators": sorted(set(resource_failed_locators)),
        "infrastructure_failures": infrastructure_failures,
        "read_only": True,
        "save_operations": 0,
        "ui_operations": 0,
    }
    candidate_failures = len(set(resource_failed_locators))
    available_discovered = int(
        raw_telemetry.get("available_discovered") or len(links)
    )
    telemetry["search_complete"] = bool(
        raw_telemetry.get("search_complete")
        and infrastructure_failures == 0
        and inspected + candidate_failures >= available_discovered
    )
    return manifests, telemetry


def _rss_infohash(item: ET.Element) -> str:
    for child in item:
        if child.tag.rsplit("}", 1)[-1].casefold() == "infohash":
            return str(child.text or "").strip().casefold()
    return ""


def _mikan_rows(page: bytes) -> list[tuple[str, str, str]]:
    """Parse only Mikan's public HTTPS Torrent enclosures."""
    root = ET.fromstring(page)
    rows: list[tuple[str, str, str]] = []
    for item in root.findall("./channel/item"):
        title = str(item.findtext("title") or "").strip()
        enclosure = item.find("enclosure")
        torrent_url = (
            str(enclosure.attrib.get("url") or "").strip()
            if enclosure is not None else ""
        )
        parsed = urllib.parse.urlsplit(torrent_url)
        if not (
            title
            and parsed.scheme == "https"
            and parsed.hostname == "mikanani.me"
            and parsed.username is None
            and parsed.password is None
            and parsed.port is None
            and parsed.path.startswith("/Download/")
            and parsed.path.casefold().endswith(".torrent")
            and not parsed.query
            and not parsed.fragment
        ):
            continue
        rows.append((title, torrent_url, ""))
    return rows


def _animetosho_torrent_url(row: Mapping[str, Any]) -> str:
    """Prefer AnimeTosho's immutable storage copy over an upstream link.

    Older feed rows can retain a Nyaa download URL even though AnimeTosho has
    already archived the metainfo.  Nyaa is an optional source here and must
    not become required infrastructure merely because such a row was returned
    by the required AnimeTosho feed.
    """
    supplied = str(row.get("torrent_url") or "").strip()
    parsed = urllib.parse.urlsplit(supplied)
    if (
        parsed.scheme == "https" and parsed.hostname == "storage.animetosho.org"
        and parsed.path.startswith("/torrent/")
    ):
        return supplied
    infohash = str(row.get("info_hash") or "").strip().casefold()
    torrent_name = str(row.get("torrent_name") or "").strip()
    if re.fullmatch(r"[0-9a-f]{40}", infohash) and torrent_name:
        filename = torrent_name
        # Single-file torrent names commonly include the media extension,
        # while AnimeTosho's archived metainfo filename does not.
        filename = re.sub(
            r"(?i)\.(?:mkv|mp4|avi|wmv|mov|m4v|ts|m2ts)$", "", filename,
        )
        if not filename.casefold().endswith(".torrent"):
            filename += ".torrent"
        return (
            f"https://storage.animetosho.org/torrent/{infohash}/"
            + urllib.parse.quote(filename, safe="")
        )
    return supplied


def _permanent_metainfo_http_failure(exc: BaseException) -> bool:
    """Recognise only HTTP statuses that permanently reject one exact URL."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, urllib.error.HTTPError):
            return current.code in {404, 410, 451}
        current = current.__cause__ or current.__context__
    return False


def _torrent_manifest(
    *, batch: Mapping[str, Any], release_name: str, torrent_url: str,
    raw: Mapping[str, Any], request_ids: Sequence[str],
) -> dict[str, Any]:
    files = raw.get("files")
    if not isinstance(files, Mapping):
        raise ValueError("torrent metainfo lacks complete file map")
    rows = [
        {
            "torrent_index": int(index),
            "path": str(value.get("path") or ""),
            "size": value.get("size"),
        }
        for index, value in sorted(files.items(), key=lambda item: int(item[0]))
        if isinstance(value, Mapping)
    ]
    infohash = str(raw.get("infohash") or "").casefold()
    manifest = bind_source_manifest(
        provider="torrent", locator=f"torrent:{torrent_url}",
        release_name=release_name or str(batch.get("title") or infohash),
        search_request_ids=request_ids, files=rows,
        acquisition={"infohash": infohash, "torrent_url": torrent_url},
    )
    return validate_source_manifest(manifest)


def discover_torrent_manifests(
    batch: Mapping[str, Any], *, fetch_bytes: FetchBytes,
    download_torrent: DownloadTorrent,
    existing_locators: Iterable[str] = (), max_candidates: int = 32,
    deadline: float | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read feeds and metainfo only; torrent payload bytes are never fetched."""
    request = _provider_request(batch)
    terms = request["search_queries"]
    request_ids = batch.get("request_ids")
    if not isinstance(request_ids, list) or not all(isinstance(value, str) for value in request_ids):
        raise ValueError("subtitle discovery batch has invalid request ids")
    deadline = time.monotonic() + 180 if deadline is None else deadline
    excluded = set(existing_locators)
    discovered: dict[str, tuple[str, str, str, bool]] = {}
    attempts = responses = 0
    hit_cap = False
    failures: list[dict[str, str]] = []
    resource_failed_locators: list[str] = []
    infrastructure_failures = 0
    required_infrastructure_failures = 0
    source_telemetry: dict[str, dict[str, Any]] = {}

    tokyotosho_enabled = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH", "0",
    ).strip().casefold() in {"1", "true", "yes", "on"}
    tokyotosho_url = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_URL",
        "https://tokyo-tosho.net",
    ).strip().rstrip("/")
    parsed_tokyo = urllib.parse.urlsplit(tokyotosho_url)
    if tokyotosho_enabled and (
        parsed_tokyo.scheme != "https"
        or parsed_tokyo.hostname not in {
            "tokyo-tosho.net", "tokyotosho.info", "www.tokyotosho.info",
            "tokyotosho.se", "www.tokyotosho.se",
        }
        or parsed_tokyo.username is not None or parsed_tokyo.password is not None
        or parsed_tokyo.port is not None or parsed_tokyo.path not in {"", "/"}
        or parsed_tokyo.query or parsed_tokyo.fragment
    ):
        raise ValueError("subtitle TokyoTosho origin is invalid")

    providers = [
        *((("tokyotosho", True),) if tokyotosho_enabled else ()),
        ("animetosho", True),
        # Mikan expands discovery but stays optional: an outage must not keep
        # every subtitle batch retryable.
        *((
            ("mikan", False),
        ) if os.getenv(
            "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH", "0",
        ).strip().casefold() in {"1", "true", "yes", "on"} else ()),
        # Nyaa is retained for additional manifests but its TLS outage must
        # not keep every subtitle batch retryable forever.
        ("nyaa", False),
    ]
    for provider, required in providers:
        provider_attempts = provider_responses = 0
        provider_hit_cap = False
        query_telemetry: list[dict[str, Any]] = []
        for term_index, term in enumerate(terms):
            if time.monotonic() >= deadline:
                break
            attempts += 1
            provider_attempts += 1
            try:
                if provider == "mikan":
                    url = (
                        "https://mikanani.me/RSS/Search?searchstr="
                        + urllib.parse.quote(term)
                    )
                    rows = _mikan_rows(fetch_bytes(
                        url, max_bytes=4 * 1024 * 1024,
                        timeout=12, attempts=1,
                    ))
                elif provider == "nyaa":
                    url = "https://nyaa.si/?page=rss&c=1_2&f=0&q=" + urllib.parse.quote(term)
                    root = ET.fromstring(fetch_bytes(
                        url, max_bytes=4 * 1024 * 1024, timeout=12, attempts=1,
                    ))
                    rows = [
                        (str(item.findtext("title") or "").strip(),
                         str(item.findtext("link") or "").strip(), _rss_infohash(item))
                        for item in root.findall("./channel/item")
                    ]
                elif provider == "animetosho":
                    url = "https://feed.animetosho.org/json?q=" + urllib.parse.quote(term)
                    payload = json.loads(fetch_bytes(
                        url, max_bytes=8 * 1024 * 1024, timeout=12, attempts=1,
                    ))
                    if not isinstance(payload, list):
                        raise ValueError("AnimeTosho response is not a list")
                    rows = [
                        (str(row.get("title") or "").strip(),
                         _animetosho_torrent_url(row),
                         str(row.get("info_hash") or "").strip().casefold())
                        for row in payload if isinstance(row, Mapping)
                    ]
                else:
                    url = tokyotosho_url + "/search.php?" + urllib.parse.urlencode({
                        "terms": term,
                        "searchName": "true",
                        "searchComment": "true",
                    })
                    rows = _tokyotosho_rows(fetch_bytes(
                        url, max_bytes=8 * 1024 * 1024, timeout=12, attempts=1,
                    ), tokyotosho_url)
                responses += 1
                provider_responses += 1
            except Exception as exc:
                failures.append({"provider": provider, "query": term, "error": type(exc).__name__})
                infrastructure_failures += 1
                if required:
                    required_infrastructure_failures += 1
                query_telemetry.append({
                    "query": term, "response": False,
                    "eligible_candidates": 0, "selected_candidates": 0,
                    "truncated": False,
                })
                continue
            # Reserve a fair share of the remaining global metainfo budget for
            # every remaining query.  In particular, a broad Mikan title feed
            # must not consume all 32 slots before S00 / exact-episode queries
            # are even attempted.  Duplicate and already-excluded rows do not
            # consume this per-query share.
            remaining_queries = len(terms) - term_index
            remaining_slots = max(0, max_candidates - len(discovered))
            query_budget = (
                (remaining_slots + remaining_queries - 1) // remaining_queries
                if remaining_queries else 0
            )
            query_eligible = query_selected = 0
            query_truncated = False
            for release_name, torrent_url, feed_hash in rows:
                if not release_name or not torrent_url.startswith("https://"):
                    continue
                locator = f"torrent:{torrent_url}"
                aliases = _infohash_aliases(feed_hash)
                if locator in excluded or any(
                    f"torrent_infohash:{alias}" in excluded for alias in aliases
                ):
                    continue
                if torrent_url in discovered:
                    continue
                query_eligible += 1
                if query_selected >= query_budget or len(discovered) >= max_candidates:
                    query_truncated = True
                    provider_hit_cap = True
                    if required:
                        hit_cap = True
                    continue
                discovered[torrent_url] = (
                    release_name, feed_hash, provider, required,
                )
                query_selected += 1
            query_telemetry.append({
                "query": term, "response": True,
                "eligible_candidates": query_eligible,
                "selected_candidates": query_selected,
                "truncated": query_truncated,
            })
        source_telemetry[provider] = {
            "required": required,
            "query_attempts": provider_attempts,
            "query_responses": provider_responses,
            "hit_cap": provider_hit_cap,
            "queries": query_telemetry,
            "search_complete": bool(
                terms and provider_attempts == len(terms)
                and provider_responses == provider_attempts
                and not provider_hit_cap
            ),
        }
        if time.monotonic() >= deadline:
            break

    manifests: list[dict[str, Any]] = []
    processed = 0
    metainfo_attempted = 0
    for torrent_url, (release_name, _feed_hash, source_provider, required) in discovered.items():
        if time.monotonic() >= deadline:
            break
        parsed_torrent = urllib.parse.urlsplit(torrent_url)
        # Required indexes can point at metainfo hosted by an optional upstream
        # site.  A healthy AnimeTosho/TokyoTosho query remains required, but an
        # old Nyaa (or other external tracker) URL returned by either index must
        # not promote that upstream host into required infrastructure.  Only
        # metainfo hosted by the index's own approved origin remains strict.
        optional_upstream = bool(
            source_provider == "animetosho"
            and parsed_torrent.hostname != "storage.animetosho.org"
        ) or bool(
            source_provider == "tokyotosho"
            and parsed_torrent.hostname not in {
                "tokyo-tosho.net", "tokyotosho.info", "www.tokyotosho.info",
                "tokyotosho.se", "www.tokyotosho.se",
            }
        )
        metainfo_required = bool(
            required and not optional_upstream
        )
        metainfo_attempted += 1
        try:
            with tempfile.TemporaryDirectory(prefix="scrapeflow-subtitle-discovery-") as directory:
                raw = download_torrent(
                    torrent_url, Path(directory) / "source.torrent",
                    timeout=12, attempts=1,
                )
            manifests.append(_torrent_manifest(
                batch=batch, release_name=release_name, torrent_url=torrent_url,
                raw=raw, request_ids=request_ids,
            ))
            processed += 1
        except Exception as exc:
            locator = f"torrent:{torrent_url}"
            scope = str(getattr(exc, "failure_scope", (
                "candidate" if (
                    isinstance(exc, ValueError)
                    or _permanent_metainfo_http_failure(exc)
                ) else "infrastructure"
            )))
            failures.append({
                "provider": source_provider, "stage": "torrent_metainfo", "locator": locator,
                "error": type(exc).__name__, "scope": scope,
            })
            if scope == "candidate":
                resource_failed_locators.append(locator)
            else:
                infrastructure_failures += 1
                if metainfo_required:
                    required_infrastructure_failures += 1
    required_sources = [name for name, required in providers if required]
    optional_sources_capped = sorted(
        name for name, required in providers
        if not required and source_telemetry.get(name, {}).get("hit_cap") is True
    )
    complete = bool(
        terms and required_sources and not hit_cap
        and all(
            source_telemetry.get(name, {}).get("search_complete") is True
            for name in required_sources
        )
        and metainfo_attempted == len(discovered)
        and required_infrastructure_failures == 0
    )
    return manifests, {
        "provider": "torrent",
        "query_attempts": attempts,
        "query_responses": responses,
        "original_query_count": int(request.get("original_query_count") or len(terms)),
        "compacted_query_count": len(terms),
        "discovered": len(discovered),
        "metainfo_processed": processed,
        "metainfo_attempted": metainfo_attempted,
        "manifest_count": len(manifests),
        "hit_cap": hit_cap,
        "failures": failures,
        "resource_failed_locators": sorted(set(resource_failed_locators)),
        "infrastructure_failures": infrastructure_failures,
        "required_infrastructure_failures": required_infrastructure_failures,
        "sources": source_telemetry,
        "required_sources": required_sources,
        "optional_sources_capped": optional_sources_capped,
        "search_complete": complete,
        "metainfo_only": True,
        "payload_downloads": 0,
        "video_members_selected": 0,
    }
