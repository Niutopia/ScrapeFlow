"""Shared replenishment adapter infrastructure (extracted 2026-09-07).

Foundation imported by the impl facade and the extracted searcher/terms
modules alike: the error taxonomy, the bounded HTTP fetch, the DHT metadata
machinery, the torrent manifest decoder, and the pause checkpoints.  Import
direction is strictly downward (common ← everything), so no cycle can form.
"""

from __future__ import annotations

from html.parser import HTMLParser
import hashlib
import os
from pathlib import Path
import re
import shutil
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any, Callable, Iterable, Mapping, Sequence

class ReplenishmentDeliveryError(RuntimeError):
    """The verified payload is reusable; only cloud delivery failed."""

    failure_scope = "delivery"
    reusable_candidate = True
    exclude_candidate = False

    def __init__(self, message: str, *, stage: str = "delivery") -> None:
        super().__init__(message)
        self.failure_stage = stage


class ReplenishmentCandidateError(RuntimeError):
    """The selected release itself failed identity, manifest, or acquisition checks."""

    failure_scope = "candidate"
    reusable_candidate = False
    exclude_candidate = True

    def __init__(
        self, message: str, *, stage: str = "candidate_acquire",
        candidate: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_stage = stage
        self.candidate = {
            key: candidate.get(key)
            for key in ("provider", "release_name", "locator", "infohash")
            if candidate is not None and candidate.get(key) is not None
        }


class ReplenishmentInfrastructureError(RuntimeError):
    """Local capacity, dependencies, or orchestration failed without blaming a release."""

    failure_scope = "infrastructure"
    reusable_candidate = False
    exclude_candidate = False

    def __init__(self, message: str, *, stage: str = "infrastructure") -> None:
        super().__init__(message)
        self.failure_stage = stage


class ReplenishmentPauseRequested(RuntimeError):
    """A caller withdrew its RootJob scope before a local provider effect."""

    pause_requested = True


def _pause_checkpoint(pause_requested: Callable[[], bool] | None) -> None:
    """Fail closed immediately before a provider-owned external operation."""
    if pause_requested is None:
        return
    try:
        paused = bool(pause_requested())
    except Exception as exc:
        if getattr(exc, "pause_requested", False) is True:
            raise
        raise ReplenishmentPauseRequested(
            "补源暂停状态不可确认，已在外部操作前停止",
        ) from exc
    if paused:
        raise ReplenishmentPauseRequested(
            "补源已暂停或不属于当前 RootJob",
        )


from engine.scrapeflow.replenishment_matching import expanded_episode_ids as _expanded_episode_ids
from engine.scrapeflow.media_policy import (
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
)


# Compatibility names are intentionally kept local because this adapter's
# public helpers accept an ``allowed_payload_extensions`` default.
MAX_TORRENT_BYTES = 8 * 1024 * 1024


_SAFE_INFRA_FAILURE_CODES = frozenset({
    "connection_refused",
    "connection_reset",
    "dht_window_cold",
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
_HTTP_FAILURE_CODE_RE = re.compile(r"\Ahttp_(?:1\d\d|2\d\d|3\d\d|4\d\d|5\d\d)\Z")


def _safe_infrastructure_failure_types(
    values: Mapping[object, object] | None,
) -> dict[str, int]:
    """Keep only bounded, provider-neutral source-health failure codes.

    Search adapters may inspect arbitrary upstream exceptions.  Only the
    closed codes below cross the adapter boundary; exception messages, URLs,
    credentials, and class names supplied by an untrusted implementation do
    not become durable telemetry.
    """
    output: dict[str, int] = {}
    if not isinstance(values, Mapping):
        return output
    for raw_code, raw_count in values.items():
        if not isinstance(raw_code, str) or type(raw_count) is not int:
            continue
        count = max(0, min(raw_count, 100_000))
        if count <= 0:
            continue
        code = raw_code.strip().casefold()
        if code not in _SAFE_INFRA_FAILURE_CODES and not _HTTP_FAILURE_CODE_RE.fullmatch(code):
            code = "source_error"
        output[code] = min(100_000, output.get(code, 0) + count)
    return dict(sorted(output.items()))


def _bencode(value: Any) -> bytes:
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii") + b"e"
    if isinstance(value, bytes):
        return str(len(value)).encode("ascii") + b":" + value
    if isinstance(value, list):
        return b"l" + b"".join(_bencode(item) for item in value) + b"e"
    if isinstance(value, dict):
        return b"d" + b"".join(_bencode(key) + _bencode(item) for key, item in value.items()) + b"e"
    raise TypeError(type(value))


def _direct_download_env(base: Mapping[str, str]) -> dict[str, str]:
    """Clone an environment with every HTTP proxy removed.

    Search indexes may need the proxy; tracker announces and BT peer
    traffic are direct connections and must never inherit it (aria2 reads
    the ``http_proxy`` environment family for its HTTP tracker requests).
    """
    cleaned = dict(base)
    for key in ("http_proxy", "https_proxy", "all_proxy", "no_proxy",
                "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
        cleaned.pop(key, None)
    return cleaned


def _bounded_seconds(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError(f"{name} 需要是整数秒")
    value = int(raw)
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} 需要在 {minimum}–{maximum} 秒之间")
    return value


class _AnchorParser(HTMLParser):
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
            self.anchors.append({"href": self._href, "text": " ".join(self._text).strip()})
            self._href = None
            self._text = []


class _DynamicSearchResult(list[dict[str, Any]]):
    """Candidates plus enough telemetry to distinguish empty from unreachable."""

    def __init__(
        self, values: list[dict[str, Any]], *, query_attempts: int, query_responses: int,
        source_exhausted: bool = False,
        resource_failed_locators: list[str] | None = None,
        infrastructure_failures: int = 0,
        infrastructure_failure_types: Mapping[str, int] | None = None,
        preexcluded_count: int = 0,
        query_cursor: Mapping[str, Any] | None = None,
        reviewed_torrent_miss_locators: Iterable[str] | None = None,
    ) -> None:
        super().__init__(values)
        self.query_attempts = query_attempts
        self.query_responses = query_responses
        self.source_exhausted = bool(source_exhausted)
        self.resource_failed_locators = list(resource_failed_locators or [])
        self.infrastructure_failure_types = _safe_infrastructure_failure_types(
            infrastructure_failure_types,
        )
        # Keep the scalar counter and its breakdown consistent even when a
        # provider forgot to increment the scalar for one failure branch.
        self.infrastructure_failures = max(
            0,
            int(infrastructure_failures),
            sum(self.infrastructure_failure_types.values()),
        )
        self.preexcluded_count = max(0, int(preexcluded_count))
        # A cursor is a read-only continuation receipt.  Provider adapters
        # decide its shape; the bridge/root boundary validates and bounds it
        # before durable persistence.  Keep a shallow copy so a caller cannot
        # mutate the result after it has been emitted as telemetry.
        self.query_cursor = dict(query_cursor) if isinstance(query_cursor, Mapping) else None
        self.reviewed_torrent_miss_locators = [
            str(value) for value in (reviewed_torrent_miss_locators or [])
            if isinstance(value, str) and value
        ]


def _network_failure_code(exc: BaseException) -> str:
    """Return a stable, non-sensitive code for source-health telemetry."""
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in chain:
        chain.append(current)
        reason = current.reason if isinstance(current, urllib.error.URLError) else None
        current = reason if isinstance(reason, BaseException) else (
            current.__cause__ or current.__context__
        )
    for error in chain:
        if isinstance(error, urllib.error.HTTPError):
            code = error.code
            if type(code) is int and 100 <= code <= 599:
                return f"http_{code}"
            return "network_error"
        if isinstance(error, ET.ParseError):
            return "xml_parse_error"
        if isinstance(error, ConnectionRefusedError):
            return "connection_refused"
        if isinstance(error, ConnectionResetError):
            return "connection_reset"
        if isinstance(error, socket.gaierror):
            return "dns_failure"
        if isinstance(error, (TimeoutError, socket.timeout)):
            return "timeout"
        if isinstance(error, ssl.SSLError):
            return "tls_failure"
        if isinstance(error, urllib.error.URLError):
            return "network_error"
        if isinstance(error, ValueError):
            return "value_error"
        if isinstance(error, RuntimeError):
            return "runtime_error"
        if isinstance(error, OSError):
            return "os_error"
    return "source_error"

def _fetch_bytes(
    url: str, *, max_bytes: int, timeout: int = 60, attempts: int = 4,
    opener: Any | None = None, user_agent: str = "ScrapeFlow/1.0",
) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    last_error: Exception | None = None
    # When no explicit per-source proxy opener is supplied, stay direct:
    # urllib's default opener would inherit an ambient host proxy.
    direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for attempt in range(attempts):
        try:
            open_request = opener.open if opener is not None else direct_opener.open
            with open_request(request, timeout=timeout) as response:
                data = response.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise ValueError("HTTP 响应超过大小上限")
            return data
        except (OSError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(min(2 ** attempt, 4))
    raise RuntimeError(f"HTTP 读取失败: {type(last_error).__name__}") from last_error


def _acg_http_opener() -> Any | None:
    """Use an explicit per-source proxy without changing AList traffic."""
    value = os.getenv("SCRAPEFLOW_REPLENISHMENT_ACG_PROXY", "").strip()
    if not value:
        return None
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname or parsed.port is None
        or parsed.username is not None or parsed.password is not None
        or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
    ):
        raise ValueError("SCRAPEFLOW_REPLENISHMENT_ACG_PROXY must be a credential-free HTTP proxy URL")
    return urllib.request.build_opener(urllib.request.ProxyHandler({
        "http": value, "https": value,
    }))


def _magnet_metadatas_batch(
    magnet_uris: Sequence[str],
    scratch: Path,
    *,
    timeout: int = 200,
    pause_requested: Callable[[], bool] | None = None,
) -> tuple[dict[str, dict[str, Any]], int]:
    """Resolve several magnets' metadata in one bounded aria2 DHT pass.

    The returned mapping is keyed by each resolved torrent's true infohash.
    Metadata comes from the swarm itself, so it is anchored to the magnet's
    identity and cannot be swapped by a hostile download endpoint.  The
    second return value counts magnets the window could not resolve: a cold
    DHT window is a *window* fact, and the callers report it as
    infrastructure telemetry instead of silently dropping the rows.
    """
    if not magnet_uris:
        return {}, 0
    scratch.mkdir(parents=True, exist_ok=True)
    command = [
        "aria2c", "--seed-time=0", "--file-allocation=none",
        "--enable-dht=true", "--enable-peer-exchange=true", "--bt-enable-lpd=true",
        "--bt-metadata-only=true", "--bt-save-metadata=true",
        f"--bt-stop-timeout={max(60, min(timeout, 600))}",
        "--console-log-level=notice", "--summary-interval=0",
        f"--dir={scratch}",
        *magnet_uris,
    ]
    batch_failed = False
    try:
        _pause_checkpoint(pause_requested)
        subprocess.run(
            command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=max(60, timeout + 90),
            env=_direct_download_env(os.environ),
        )
    except (subprocess.TimeoutExpired, OSError):
        # A cold DHT window resolved nothing or the runner died: the
        # unresolved count below carries that fact into telemetry.
        batch_failed = True
    manifests: dict[str, dict[str, Any]] = {}
    for path in scratch.iterdir():
        if not path.is_file() or path.suffix != ".torrent":
            continue
        try:
            data = path.read_bytes()
            if len(data) > MAX_TORRENT_BYTES:
                continue
            manifest = _torrent_manifest(data)
        except (OSError, ValueError):
            continue
        manifests[manifest["infohash"]] = manifest
    resolved_hashes = {
        str(manifest.get("infohash") or "").lower() for manifest in manifests.values()
    }
    unresolved = 0
    for uri in magnet_uris:
        match = _MAGNET_URI_PATTERN.search(uri)
        if match is not None and match.group(1).lower() not in resolved_hashes:
            unresolved += 1
    if batch_failed and not manifests:
        unresolved = max(unresolved, len(magnet_uris))
    return manifests, unresolved


class MagnetMetadataUnavailable(RuntimeError):
    """The swarm did not serve its metadata within the bounded DHT window.

    This is a window verdict (cold DHT bootstrap, seeders offline), never a
    resource verdict: the same magnet may resolve minutes later.
    """


class _BDecoder:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def parse(self, offset: int = 0) -> tuple[Any, int]:
        data = self.data
        marker = data[offset:offset + 1]
        if marker == b"i":
            end = data.index(b"e", offset)
            return int(data[offset + 1:end]), end + 1
        if marker == b"l":
            values: list[Any] = []
            offset += 1
            while data[offset:offset + 1] != b"e":
                value, offset = self.parse(offset)
                values.append(value)
            return values, offset + 1
        if marker == b"d":
            values: dict[bytes, Any] = {}
            offset += 1
            while data[offset:offset + 1] != b"e":
                key, offset = self.parse(offset)
                value, offset = self.parse(offset)
                if not isinstance(key, bytes):
                    raise ValueError("torrent 字典键格式无效")
                values[key] = value
            return values, offset + 1
        separator = data.index(b":", offset)
        size = int(data[offset:separator])
        start = separator + 1
        return data[start:start + size], start + size


def _torrent_manifest(data: bytes) -> dict[str, Any]:
    meta, offset = _BDecoder(data).parse()
    if offset != len(data) or not isinstance(meta, dict) or not isinstance(meta.get(b"info"), dict):
        raise ValueError("torrent 元数据格式无效")
    info = meta[b"info"]
    root = info.get(b"name")
    if not isinstance(root, bytes):
        raise ValueError("torrent 缺少名称")
    files: dict[int, dict[str, Any]] = {}
    if isinstance(info.get(b"files"), list):
        for index, row in enumerate(info[b"files"], start=1):
            if not isinstance(row, dict) or not isinstance(row.get(b"path"), list):
                raise ValueError("torrent 文件清单格式无效")
            parts = row[b"path"]
            if not all(isinstance(part, bytes) for part in parts):
                raise ValueError("torrent 文件路径格式无效")
            files[index] = {
                "path": "/".join(part.decode("utf-8", "replace") for part in parts),
                "size": row.get(b"length"),
            }
    else:
        files[1] = {"path": root.decode("utf-8", "replace"), "size": info.get(b"length")}
    return {
        "root": root.decode("utf-8", "replace"),
        "infohash": hashlib.sha1(_bencode(info)).hexdigest(),
        "files": files,
    }


def _nyaa_torrent_mirror_url(url: str) -> str | None:
    """Return the tightly-bounded HTTPS mirror URL for a canonical Nyaa link.

    TokyoTosho's authorized release rows use Nyaa's ``/view/<id>/torrent``
    endpoint, but that origin can be unavailable from a deployment even when
    TokyoTosho itself is reachable.  This helper intentionally accepts no
    arbitrary host, query, credential, port, or path: the caller may only
    retry the same numeric torrent ID through the fixed HTTPS mirror.
    """
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"nyaa.si", "www.nyaa.si"}
        or parsed.username is not None or parsed.password is not None
        or parsed.port is not None or parsed.query or parsed.fragment
    ):
        return None
    match = re.fullmatch(
        r"/(?:view/(?P<view>[1-9]\d*)/torrent|download/(?P<download>[1-9]\d*)\.torrent)",
        parsed.path,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    torrent_id = match.group("view") or match.group("download")
    return f"https://nyaa.land/view/{torrent_id}/torrent"


def _repair_nyaa_land_torrent_comment(data: bytes, mirror_url: str) -> bytes:
    """Repair only Nyaa Land's known top-level comment-length rewrite.

    The mirror changes ``nyaa.si`` to ``nyaa.land`` in its top-level comment
    but leaves the original bencode byte length.  Its ``info`` dictionary is
    unchanged, yet standard decoders rightly reject the malformed wrapper.
    Restrict the repair to one exact comment before ``info`` so no hashed
    payload byte can be altered; callers still verify the resulting infohash
    against the BTIH advertised by TokyoTosho.
    """
    parsed = urllib.parse.urlsplit(mirror_url)
    match = re.fullmatch(r"/view/([1-9]\d*)/torrent", parsed.path)
    if (
        parsed.scheme != "https" or parsed.hostname != "nyaa.land"
        or parsed.username is not None or parsed.password is not None
        or parsed.port is not None or parsed.query or parsed.fragment
        or match is None
    ):
        return data
    torrent_id = match.group(1).encode("ascii")
    original_comment = b"https://nyaa.si/view/" + torrent_id
    mirrored_comment = b"https://nyaa.land/view/" + torrent_id
    malformed = (
        b"7:comment" + str(len(original_comment)).encode("ascii")
        + b":" + mirrored_comment
    )
    corrected = (
        b"7:comment" + str(len(mirrored_comment)).encode("ascii")
        + b":" + mirrored_comment
    )
    comment_offset = data.find(malformed)
    info_offset = data.find(b"4:info")
    if (
        comment_offset < 0 or data.count(malformed) != 1
        or info_offset < 0 or comment_offset >= info_offset
    ):
        return data
    return data.replace(malformed, corrected, 1)


_MAGNET_URI_PATTERN = re.compile(
    r"magnet:\?xt=urn:btih:([0-9a-fA-F]{40}|[A-Za-z2-7]{32})\b[^\s]*",
)
# Public trackers bootstrap DHT announces for magnet-only metadata fetches.
# Peer traffic stays direct; these are announce endpoints only.
# Public trackers bootstrap DHT announces for magnet-only metadata fetches.
# Peer traffic stays direct; these are announce endpoints only.
_MAGNET_BOOTSTRAP_TRACKERS = (
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
)


def _magnet_metadata(
    magnet_url: str, destination: Path, *,
    timeout: int = 240,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Resolve a magnet URI's torrent metadata via a bounded aria2 DHT pass.

    ``--bt-metadata-only`` stops the transfer as soon as the metadata is
    fetched, so no payload bytes are downloaded and the session never seeds.
    The saved ``.torrent`` is written to ``destination`` so the normal
    manifest verification and the later data download share one file.
    """
    match = _MAGNET_URI_PATTERN.fullmatch(magnet_url.strip())
    if match is None:
        raise ValueError("magnet 地址缺少有效 btih")
    if len(magnet_url) > 8192:
        raise ValueError("magnet 地址过长")
    full_url = magnet_url.strip()
    if "&tr=" not in full_url:
        full_url = full_url + "&tr=" + "&tr=".join(_MAGNET_BOOTSTRAP_TRACKERS)
    workspace = destination.parent
    workspace.mkdir(parents=True, exist_ok=True)
    # aria2 names the metadata file after the infohash; run in a scratch dir
    # so we can find the file regardless of its name, then move it into place.
    scratch = workspace / f".{destination.name}.magnet"
    if scratch.exists():
        shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True, exist_ok=True)
    command = [
        "aria2c", "--seed-time=0", "--file-allocation=none",
        "--enable-dht=true", "--enable-peer-exchange=true", "--bt-enable-lpd=true",
        "--bt-metadata-only=true", "--bt-save-metadata=true",
        f"--bt-stop-timeout={max(60, min(timeout, 300))}",
        "--console-log-level=notice", "--summary-interval=0",
        f"--dir={scratch}",
        full_url,
    ]
    try:
        _pause_checkpoint(pause_requested)
        completed = subprocess.run(
            command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=max(60, timeout + 60),
            env=_direct_download_env(os.environ),
        )
    except subprocess.TimeoutExpired as exc:
        shutil.rmtree(scratch, ignore_errors=True)
        # A hung aria2 killed past its own bt-stop window is the same DHT
        # cold-window fault as a non-zero exit: the resource itself is
        # unproven, so the caller must retry later instead of excluding it.
        # (A bare TimeoutError is an OSError and would fall into the
        # candidate branch of _preflight's except chain.)
        raise MagnetMetadataUnavailable(
            "magnet 元数据解析超时: DHT 窗口内未取回元数据",
        ) from exc
    except ReplenishmentPauseRequested:
        shutil.rmtree(scratch, ignore_errors=True)
        raise
    saved = None
    if completed.returncode == 0:
        for row in sorted(scratch.iterdir()):
            if row.is_file() and row.suffix == ".torrent":
                saved = row
                break
    if saved is None:
        tail = " ".join(completed.stdout.splitlines()[-8:])[:1200]
        shutil.rmtree(scratch, ignore_errors=True)
        raise MagnetMetadataUnavailable(f"magnet 元数据解析失败: {tail}")
    try:
        data = saved.read_bytes()
        if len(data) > MAX_TORRENT_BYTES:
            raise ValueError("torrent 元数据超过大小上限")
        manifest = _torrent_manifest(data)
    finally:
        # The scratch must not outlive this function on ANY exit — a leaked
        # parse failure dir would only be reclaimed by the next same-named
        # attempt, if one ever comes.
        shutil.rmtree(scratch, ignore_errors=True)
    _pause_checkpoint(pause_requested)
    destination.write_bytes(data)
    return manifest


def _download_torrent(
    url: str, destination: Path, *, timeout: int = 60, attempts: int = 4,
    opener: Any | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    if url.startswith("magnet:"):
        # A magnet carries no downloadable .torrent document; its metadata
        # comes from the swarm itself via a bounded DHT resolution pass.
        return _magnet_metadata(
            url, destination,
            timeout=_bounded_seconds(
                "SCRAPEFLOW_REPLENISHMENT_MAGNET_METADATA_TIMEOUT", 240, 60, 600,
            ),
            pause_requested=pause_requested,
        )
    if not url.startswith("https://"):
        raise ValueError("torrent 地址需要使用 HTTPS")
    try:
        _pause_checkpoint(pause_requested)
        data = _fetch_bytes(
            url, max_bytes=MAX_TORRENT_BYTES, timeout=timeout,
            attempts=attempts, opener=opener,
        )
    except ReplenishmentPauseRequested:
        raise
    except (OSError, RuntimeError, TimeoutError):
        mirror_url = _nyaa_torrent_mirror_url(url)
        if mirror_url is None:
            raise
        _pause_checkpoint(pause_requested)
        data = _fetch_bytes(
            mirror_url, max_bytes=MAX_TORRENT_BYTES, timeout=timeout,
            attempts=attempts, opener=opener,
        )
        data = _repair_nyaa_land_torrent_comment(data, mirror_url)
    manifest = _torrent_manifest(data)
    _pause_checkpoint(pause_requested)
    destination.write_bytes(data)
    return manifest


# A provider manifest is untrusted evidence.  These members can carry an
# episode-looking token while being an opening/ending, preview, sample,
# bonus, scan or other supplemental payload.  They must never be selected as
# the one primary video for an audited ``missing_episode`` gap.  Keep the
# expression delimiter-aware so ordinary words such as ``Extraordinary`` do
# not accidentally become a rejection, while still failing closed for the
# common directory and release-name spellings.
_SUPPLEMENTAL_VIDEO_PATH_RE = re.compile(
    r"(?i)(?:^|[/\\\s._\-\[\](){}])"
    r"(?:bonus(?:es)?|extra(?:s)?|sample(?:s)?|scan(?:s)?|"
    r"menu|preview(?:s)?|trailer(?:s)?|teaser(?:s)?|featurette(?:s)?|"
    r"behind[ ._\-]*the[ ._\-]*scenes|"
    r"ncop|nced|pv|cm|creditless|op|ed)"
    r"(?=$|[/\\\s._\-\[\](){}])"
)
def _is_supplemental_video_path(path: str) -> bool:
    """Return whether a manifest path is recognizably non-primary media."""
    normalized = str(path or "").replace("\\", "/")
    return bool(_SUPPLEMENTAL_VIDEO_PATH_RE.search(normalized))
def _is_ordinary_primary_video_path(path: str) -> bool:
    """Check path shape which remains meaningful after selection serialization."""
    return (
        Path(path).suffix.casefold() in VIDEO_EXTENSIONS
        and not _is_supplemental_video_path(path)
        and len(_expanded_episode_ids(path)) <= 1
    )
def _base32_infohash(hex_hash: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", hex_hash):
        return ""
    import base64
    return base64.b32encode(bytes.fromhex(hex_hash)).decode("ascii").rstrip("=").casefold()
