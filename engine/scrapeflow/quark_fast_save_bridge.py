"""Minimal Quark share-save bridge with injectable HTTP transport.

Credentials are delegated from the matching AList Quark storage and remain in
memory.  Dry-run planning never reads AList storage additions or calls Quark.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import json
import os
import posixpath
import re
import time
from typing import Any, Protocol
import urllib.error
import urllib.parse
import urllib.request


QUARK_SHARE_API = "https://drive-pc.quark.cn/1/clouddrive"
QUARK_DRIVE_API = "https://drive.quark.cn/1/clouddrive"
QUARK_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) quark-cloud-drive/2.5.20 Chrome/100.0.4896.160 "
    "Electron/18.3.5.4 Safari/537.36 Channel/pckk_other_ch"
)

# Quark returns these both in successful HTTP envelopes and in HTTP error
# bodies.  They all mean this exact share is no longer usable; none indicates
# an account/CDP/network outage.  Keep the classification in one place so a
# cancelled share cannot poison the entire first-tier discovery lane.
QUARK_SHARE_INVALID_CODES = frozenset({
    41004, 41006,
    41010,  # 分享文件涉及违规内容
    41011,  # 分享地址已失效
    41012,  # 分享者主动取消
    41017,
    41019,  # 分享地址已过期
    41031,  # 分享者封禁，链接查看受限
})


class QuarkBridgeError(RuntimeError):
    failure_scope = "infrastructure"
    failure_stage = "quark_fast_save_submit"
    reusable_candidate = False
    exclude_candidate = False


class QuarkShareExpiredError(QuarkBridgeError):
    failure_scope = "candidate"
    failure_stage = "quark_share_candidate"
    exclude_candidate = True


class QuarkShareInDoubtError(QuarkBridgeError):
    """Fast-save may have committed; only exact arrival or task polling is safe."""

    failure_scope = "delivery"
    failure_stage = "quark_fast_save_submit_in_doubt"
    reusable_candidate = True
    exclude_candidate = False


class QuarkMagnetCandidateError(QuarkBridgeError):
    failure_scope = "candidate"
    failure_stage = "quark_magnet_candidate"
    exclude_candidate = True


class QuarkMagnetDeliveryError(QuarkBridgeError):
    failure_scope = "delivery"
    failure_stage = "quark_magnet_progress"
    reusable_candidate = True
    exclude_candidate = False


class QuarkMagnetInfrastructureError(QuarkBridgeError):
    failure_stage = "quark_magnet_submit"


class QuarkMagnetParseInfrastructureError(QuarkMagnetInfrastructureError):
    failure_stage = "quark_magnet_parse"


class QuarkMagnetWsgUnavailableError(QuarkMagnetInfrastructureError):
    failure_stage = "quark_magnet_wsg_not_configured"


class QuarkMagnetInDoubtError(QuarkMagnetInfrastructureError):
    """Submit may have committed; never repeat or fall back until reconciled."""

    failure_scope = "delivery"
    failure_stage = "quark_magnet_submit_in_doubt"
    reusable_candidate = True


@dataclass(frozen=True)
class QuarkSession:
    mount_path: str
    root_id: str
    cookie: str


class JsonTransport(Protocol):
    def request(
        self, method: str, endpoint: str, *, params: Mapping[str, Any],
        body: Mapping[str, Any] | None, cookie: str,
    ) -> Mapping[str, Any]: ...


class UrlLibQuarkTransport:
    """Fixed-origin JSON transport which never exposes the cookie in errors."""

    # Quark offline endpoints reject plain JSON with code=31007/decrypt_fail.
    # This transport intentionally does not pretend to implement the native
    # client WSG envelope.
    supports_wsg = False

    def __init__(self, *, timeout: float = 60.0) -> None:
        self.timeout = timeout

    def request(
        self, method: str, endpoint: str, *, params: Mapping[str, Any],
        body: Mapping[str, Any] | None, cookie: str,
    ) -> Mapping[str, Any]:
        if not any(endpoint.startswith(base + "/") for base in (QUARK_SHARE_API, QUARK_DRIVE_API)):
            raise QuarkBridgeError("Quark endpoint escaped the fixed API origin")
        query = urllib.parse.urlencode({key: str(value) for key, value in params.items()})
        url = endpoint + ("?" + query if query else "")
        payload = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        request = urllib.request.Request(url, data=payload, method=method, headers={
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Cookie": cookie,
            "Origin": "https://pan.quark.cn",
            "Referer": "https://pan.quark.cn/",
            "User-Agent": QUARK_UA,
        })
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                value = json.load(response)
        except urllib.error.HTTPError as exc:
            # Keep only bounded response diagnostics.  Request URLs, headers,
            # Cookie, parse token and magnet payload never enter the exception.
            raw = exc.read(16_384)
            code: Any = None
            message = ""
            try:
                error_value = json.loads(raw.decode("utf-8", "replace"))
                if isinstance(error_value, Mapping):
                    code = error_value.get("code")
                    message = str(error_value.get("message") or error_value.get("msg") or "")
            except (UnicodeError, json.JSONDecodeError):
                pass
            safe_message = re.sub(
                r"(?i)(cookie|token|magnet|authorization)\s*[:=]\s*\S+",
                r"\1=<redacted>", message,
            )[:240]
            error = (
                QuarkShareExpiredError
                if code in QUARK_SHARE_INVALID_CODES else QuarkBridgeError
            )
            raise error(
                f"Quark HTTP error: status={exc.code}, code={code!r}, "
                f"message={safe_message!r}"
            ) from exc
        except Exception as exc:
            raise QuarkBridgeError(f"Quark request failed: {type(exc).__name__}") from exc
        if not isinstance(value, Mapping):
            raise QuarkBridgeError("Quark returned a non-object response")
        return value


class QuarkNativeHelperTransport:
    """Authenticated client for the host-side Quark native runtime helper.

    The helper owns the WSG/native boundary.  The API container still delegates
    the Quark login Cookie from AList, but never needs Chromium automation or a
    Codex/browser process.  Requests are restricted again by the helper, and a
    stable idempotency key lets it replay a committed response after either side
    restarts without repeating a submit mutation.
    """

    supports_wsg = True

    def __init__(
        self, base_url: str, token: str, *, timeout: float = 90.0,
        opener: Callable[..., Any] = urllib.request.urlopen,
        passive_only: bool = False,
    ) -> None:
        parsed = urllib.parse.urlsplit(base_url.rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise QuarkBridgeError("Quark native helper URL is invalid")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise QuarkBridgeError("Quark native helper URL must not contain credentials")
        if parsed.scheme == "http" and parsed.hostname not in {
            "127.0.0.1", "localhost", "::1", "host.docker.internal",
        }:
            raise QuarkBridgeError("plain HTTP Quark helper must be loopback/Docker-host only")
        if not token or len(token) < 24:
            raise QuarkBridgeError("Quark native helper token is missing or too short")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.opener = opener
        self.passive_only = bool(passive_only)

    @staticmethod
    def _request_id(
        method: str, endpoint: str, params: Mapping[str, Any],
        body: Mapping[str, Any] | None,
    ) -> str:
        # The Cookie is deliberately excluded: login refreshes must be able to
        # recover the same submitted operation from the helper journal.
        canonical_body = body
        if endpoint.endswith("/offline/download/submit") and isinstance(body, Mapping):
            # parse tokens are ephemeral.  The durable identity of a submit is
            # its destination/title/exact indices, so a retry that had to parse
            # again must still hit the same helper journal row.
            canonical_body = {key: value for key, value in body.items() if key != "token"}
        canonical = json.dumps({
            "method": method, "endpoint": endpoint,
            "params": dict(params), "body": canonical_body,
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def request(
        self, method: str, endpoint: str, *, params: Mapping[str, Any],
        body: Mapping[str, Any] | None, cookie: str,
    ) -> Mapping[str, Any]:
        if not any(endpoint.startswith(base + "/") for base in (QUARK_SHARE_API, QUARK_DRIVE_API)):
            raise QuarkBridgeError("Quark endpoint escaped the fixed API origin")
        payload = json.dumps({
            "version": 1, "method": method, "endpoint": endpoint,
            "params": dict(params), "body": body, "cookie": cookie,
            "request_id": self._request_id(method, endpoint, params, body),
            "passive_only": self.passive_only,
        }, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + "/v1/quark/request", data=payload, method="POST",
            headers={
                "Accept": "application/json", "Content-Type": "application/json",
                "Authorization": "Bearer " + self.token,
            },
        )
        try:
            with self.opener(request, timeout=self.timeout) as response:
                value = json.load(response)
        except urllib.error.HTTPError as exc:
            raw = exc.read(16_384)
            message = ""
            in_doubt = False
            try:
                error_value = json.loads(raw.decode("utf-8", "replace"))
                if isinstance(error_value, Mapping):
                    message = str(error_value.get("error") or error_value.get("message") or "")
                    in_doubt = error_value.get("in_doubt") is True
            except (UnicodeError, json.JSONDecodeError):
                pass
            if exc.code == 409 and in_doubt:
                raise QuarkMagnetInDoubtError(
                    "Quark native submit is in doubt; destination reconciliation is required"
                ) from exc
            raise QuarkMagnetInfrastructureError(
                f"Quark native helper HTTP error: status={exc.code}, message={message[:240]!r}"
            ) from exc
        except Exception as exc:
            raise QuarkMagnetInfrastructureError(
                f"Quark native helper unavailable: {type(exc).__name__}"
            ) from exc
        if not isinstance(value, Mapping) or value.get("status") != "ok":
            raise QuarkMagnetInfrastructureError("Quark native helper returned an invalid response")
        result = value.get("result")
        if not isinstance(result, Mapping):
            raise QuarkMagnetInfrastructureError("Quark native helper result is not an object")
        return result


def normalize_quark_fast_save_selection(
    selection: Mapping[str, Any],
) -> dict[str, Any]:
    """Turn catalog file-id/path/size maps into an exact save manifest."""
    output = dict(selection)
    acquisition = selection.get("acquisition")
    if (
        not isinstance(acquisition, Mapping)
        or acquisition.get("kind") not in {"quark_fast_save", "quark_sfx_archive"}
    ):
        raise QuarkBridgeError("candidate lacks quark_fast_save acquisition")
    gap_map = acquisition.get("file_id_by_gap")
    path_map = acquisition.get("file_path_by_id")
    size_map = acquisition.get("file_size_by_id")
    selected = selection.get("selected_gap_ids")
    if (
        not isinstance(gap_map, Mapping) or not isinstance(path_map, Mapping)
        or not isinstance(size_map, Mapping) or not isinstance(selected, list) or not selected
    ):
        raise QuarkBridgeError("quark share candidate lacks exact file manifest")
    by_id: dict[str, dict[str, Any]] = {}
    file_name_by_gap: dict[str, str] = {}
    for gap in selected:
        if not isinstance(gap, str) or not gap:
            raise QuarkBridgeError("selected Quark gap id is invalid")
        raw_ids = gap_map.get(gap)
        ids = [raw_ids] if isinstance(raw_ids, str) else raw_ids
        if not isinstance(ids, list) or len(ids) != 1 or not isinstance(ids[0], str) or not ids[0]:
            raise QuarkBridgeError("each selected gap must map to exactly one Quark file id")
        file_id = ids[0]
        raw_path = path_map.get(file_id)
        raw_size = size_map.get(file_id)
        if not isinstance(raw_path, str) or not raw_path or type(raw_size) is not int or raw_size <= 0:
            raise QuarkBridgeError("Quark file path/size manifest is incomplete")
        name = posixpath.basename(raw_path.replace("\\", "/"))
        if not name or name in {".", ".."}:
            raise QuarkBridgeError("Quark file path has no safe basename")
        file_name_by_gap[gap] = name
        row = by_id.setdefault(file_id, {
            "file_id": file_id, "name": name, "size": raw_size, "gap_ids": [],
        })
        if row["name"] != name or row["size"] != raw_size:
            raise QuarkBridgeError("Quark file id has conflicting manifest metadata")
        row["gap_ids"].append(gap)
    names = [str(row["name"]) for row in by_id.values()]
    if len(names) != len(set(names)):
        raise QuarkBridgeError("Quark selected files collide at destination basename")
    normalized_acquisition = dict(acquisition)
    normalized_acquisition["file_name_by_gap"] = file_name_by_gap
    normalized_acquisition["expected_files"] = list(by_id.values())
    output["acquisition"] = normalized_acquisition
    return output


def delegated_quark_session(alist_client: Any, destination: str) -> QuarkSession:
    """Select the longest matching enabled AList Quark mount."""
    matches: list[tuple[int, QuarkSession]] = []
    for row in alist_client.admin_storages():
        mount = str(row.get("mount_path") or "").rstrip("/") or "/"
        if (
            row.get("driver") != "Quark" or row.get("disabled") is True
            or not (destination == mount or destination.startswith(mount.rstrip("/") + "/"))
        ):
            continue
        try:
            addition = json.loads(str(row.get("addition") or "{}"))
        except json.JSONDecodeError as exc:
            raise QuarkBridgeError("AList Quark storage addition is invalid") from exc
        cookie = addition.get("cookie")
        root_id = addition.get("root_id", "0")
        if not isinstance(cookie, str) or not cookie.strip():
            raise QuarkBridgeError("AList Quark storage has no delegated login state")
        if not isinstance(root_id, str) or not root_id:
            raise QuarkBridgeError("AList Quark storage root_id is invalid")
        matches.append((len(mount), QuarkSession(mount, root_id, cookie)))
    if not matches:
        raise QuarkBridgeError("no enabled AList Quark storage covers destination")
    return max(matches, key=lambda item: item[0])[1]


class QuarkFastSaveBridge:
    # A reviewed share path is resolved inside this bounded tree walk.  The
    # limits keep a malformed/changed share from turning one save into an
    # unbounded crawl.
    MAX_SHARE_DEPTH = 12
    MAX_SHARE_DIRECTORIES = 256
    MAX_SHARE_ENTRIES = 4096

    def __init__(self, transport: JsonTransport, *, sleep=time.sleep) -> None:
        self.transport = transport
        self.sleep = sleep

    @staticmethod
    def dry_run(selection: Mapping[str, Any], destination: str) -> dict[str, Any]:
        selection = normalize_quark_fast_save_selection(selection)
        acquisition = selection["acquisition"]
        pwd_id = acquisition.get("pwd_id") or acquisition.get("share_id")
        names = acquisition.get("file_name_by_gap")
        selected = selection.get("selected_gap_ids")
        if (
            not isinstance(pwd_id, str) or not pwd_id
            or not isinstance(names, Mapping)
            or not isinstance(selected, list) or not selected
            or any(not isinstance(names.get(gap), str) or not names.get(gap) for gap in selected)
        ):
            raise QuarkBridgeError("quark_fast_save acquisition is incomplete")
        return {
            "status": "dry_run", "provider": "quark_share",
            "destination": destination, "pwd_id": pwd_id,
            "selected_gap_ids": list(selected),
            "file_names": [str(names[gap]) for gap in selected],
            "expected_files": list(acquisition["expected_files"]),
            "endpoints": ["sharepage/token", "sharepage/detail", "sharepage/save", "task"],
        }

    def _call(
        self, session: QuarkSession, method: str, path: str,
        *, params: Mapping[str, Any] | None = None,
        body: Mapping[str, Any] | None = None,
        api: str = QUARK_SHARE_API,
    ) -> Mapping[str, Any]:
        value = self.transport.request(
            method, api + path,
            params={"pr": "ucpro", "fr": "pc", **dict(params or {})},
            body=body, cookie=session.cookie,
        )
        if value.get("code") not in {0, None} or value.get("status") not in {200, None}:
            code = value.get("code")
            message = str(value.get("message") or "Quark operation failed")
            error = (
                QuarkShareExpiredError
                if code in QUARK_SHARE_INVALID_CODES else QuarkBridgeError
            )
            raise error(f"Quark operation failed: code={code}, message={message[:160]}")
        return value

    def _resolve_destination(self, session: QuarkSession, destination: str) -> str:
        relative = destination[len(session.mount_path):].strip("/")
        parent = session.root_id
        for component in (part for part in relative.split("/") if part):
            value = self._call(session, "GET", "/file/sort", api=QUARK_DRIVE_API, params={
                "pdir_fid": parent, "_page": 1, "_size": 100,
                "_fetch_total": 1, "fetch_all_file": 1,
            })
            rows = (value.get("data") or {}).get("list")
            matches = [
                row for row in rows if isinstance(row, Mapping)
                and row.get("file_name") == component and not row.get("file")
            ] if isinstance(rows, list) else []
            if len(matches) != 1 or not isinstance(matches[0].get("fid"), str):
                raise QuarkBridgeError("destination directory is not uniquely resolvable")
            parent = str(matches[0]["fid"])
        return parent

    def _resolve_reviewed_share_files(
        self, session: QuarkSession, *, pwd_id: str, stoken: str,
        acquisition: Mapping[str, Any], selected_gap_ids: list[str],
    ) -> list[tuple[str, str, Mapping[str, Any]]]:
        """Resolve reviewed paths without accepting an equal basename elsewhere."""
        cache: dict[str, list[Mapping[str, Any]]] = {}
        counters = {"directories": 0, "entries": 0}

        def list_dir(parent: str) -> list[Mapping[str, Any]]:
            if parent in cache:
                return cache[parent]
            counters["directories"] += 1
            if counters["directories"] > self.MAX_SHARE_DIRECTORIES:
                raise QuarkShareExpiredError("reviewed share exceeds directory traversal limit")
            output: list[Mapping[str, Any]] = []
            for page in range(1, 42):
                value = self._call(session, "GET", "/share/sharepage/detail", params={
                    "pwd_id": pwd_id, "stoken": stoken, "pdir_fid": parent,
                    "force": 0, "_page": page, "_size": 100, "_fetch_total": 1,
                })
                rows = (value.get("data") or {}).get("list")
                if not isinstance(rows, list):
                    raise QuarkShareExpiredError("Quark share detail lacks file list")
                valid = [row for row in rows if isinstance(row, Mapping)]
                output.extend(valid)
                counters["entries"] += len(valid)
                if counters["entries"] > self.MAX_SHARE_ENTRIES:
                    raise QuarkShareExpiredError("reviewed share exceeds entry traversal limit")
                if len(rows) < 100:
                    break
            else:
                raise QuarkShareExpiredError("reviewed share exceeds pagination limit")
            cache[parent] = output
            return output

        resolved: list[tuple[str, str, Mapping[str, Any]]] = []
        for gap_id in selected_gap_ids:
            declared_ids = acquisition["file_id_by_gap"][gap_id]
            declared_id = declared_ids if isinstance(declared_ids, str) else declared_ids[0]
            raw_path = acquisition["file_path_by_id"].get(declared_id)
            if not isinstance(raw_path, str):
                raise QuarkShareExpiredError("reviewed share path is missing")
            parts = raw_path.replace("\\", "/").split("/")
            if (
                not parts or len(parts) > self.MAX_SHARE_DEPTH
                or any(part in {"", ".", ".."} for part in parts)
            ):
                raise QuarkShareExpiredError("reviewed share path is unsafe or too deep")
            parent = "0"
            leaf: Mapping[str, Any] | None = None
            for index, component in enumerate(parts):
                matches = [
                    row for row in list_dir(parent)
                    if row.get("file_name") == component
                ]
                if len(matches) != 1:
                    raise QuarkShareExpiredError("reviewed share path is not uniquely present")
                row = matches[0]
                fid = row.get("fid")
                if not isinstance(fid, str) or not fid:
                    raise QuarkShareExpiredError("reviewed share path has invalid file id")
                if index < len(parts) - 1:
                    if row.get("file") is not False:
                        raise QuarkShareExpiredError("reviewed share path crosses a non-directory")
                    parent = fid
                else:
                    if row.get("file") is False:
                        raise QuarkShareExpiredError("reviewed share leaf is a directory")
                    leaf = row
            if leaf is None:
                raise QuarkShareExpiredError("reviewed share leaf is missing")
            resolved.append((gap_id, str(declared_id), leaf))
        return resolved

    def inspect_share(
        self, session: QuarkSession, *, pwd_id: str, passcode: str = "",
    ) -> list[dict[str, Any]]:
        """Return a bounded, read-only recursive file manifest for one share."""
        token = self._call(session, "POST", "/share/sharepage/token", body={
            "pwd_id": pwd_id, "passcode": passcode,
        })
        stoken = (token.get("data") or {}).get("stoken")
        if not isinstance(stoken, str) or not stoken:
            raise QuarkShareExpiredError("Quark share did not return stoken")
        queue: list[tuple[str, tuple[str, ...], int]] = [("0", (), 0)]
        visited: set[str] = set()
        entry_count = 0
        output: list[dict[str, Any]] = []
        while queue:
            parent, prefix, depth = queue.pop(0)
            if parent in visited:
                continue
            visited.add(parent)
            if len(visited) > self.MAX_SHARE_DIRECTORIES:
                raise QuarkShareExpiredError("share exceeds directory traversal limit")
            for page in range(1, 42):
                value = self._call(session, "GET", "/share/sharepage/detail", params={
                    "pwd_id": pwd_id, "stoken": stoken, "pdir_fid": parent,
                    "force": 0, "_page": page, "_size": 100, "_fetch_total": 1,
                })
                rows = (value.get("data") or {}).get("list")
                if not isinstance(rows, list):
                    raise QuarkShareExpiredError("Quark share detail lacks file list")
                entry_count += len(rows)
                if entry_count > self.MAX_SHARE_ENTRIES:
                    raise QuarkShareExpiredError("share exceeds entry traversal limit")
                for row in rows:
                    if not isinstance(row, Mapping):
                        continue
                    name, fid = row.get("file_name"), row.get("fid")
                    if (
                        not isinstance(name, str) or not name
                        or name in {".", ".."} or "/" in name or "\\" in name
                        or not isinstance(fid, str) or not fid
                    ):
                        raise QuarkShareExpiredError("share contains unsafe file metadata")
                    path = "/".join((*prefix, name))
                    if row.get("file") is False:
                        if depth >= self.MAX_SHARE_DEPTH:
                            raise QuarkShareExpiredError("share exceeds directory depth limit")
                        queue.append((fid, (*prefix, name), depth + 1))
                        continue
                    size = row.get("size")
                    if type(size) is not int or size < 0:
                        raise QuarkShareExpiredError("share file has invalid size")
                    output.append({"file_id": fid, "path": path, "size": size})
                if len(rows) < 100:
                    break
            else:
                raise QuarkShareExpiredError("share exceeds pagination limit")
        return output

    def execute(
        self, selection: Mapping[str, Any], destination: str, session: QuarkSession,
        *, resume_task_id: str | None = None,
        on_prepared: Callable[[], None] | None = None,
        on_submitted: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        selection = normalize_quark_fast_save_selection(selection)
        plan = self.dry_run(selection, destination)
        acquisition = selection["acquisition"]
        expected = [
            {
                "name": str(row["name"]), "size": int(row["size"]),
                "gap_ids": list(row["gap_ids"]),
            }
            for row in acquisition["expected_files"]
        ]
        task_id = resume_task_id
        if task_id is None:
            pwd_id = str(acquisition.get("pwd_id") or acquisition.get("share_id"))
            token = self._call(session, "POST", "/share/sharepage/token", body={
                "pwd_id": pwd_id, "passcode": str(acquisition.get("passcode") or ""),
            })
            stoken = (token.get("data") or {}).get("stoken")
            if not isinstance(stoken, str) or not stoken:
                raise QuarkShareExpiredError("Quark share did not return stoken")
            expected = []
            fids: list[str] = []
            fid_tokens: list[str] = []
            expected_by_id = {
                str(row["file_id"]): row for row in acquisition["expected_files"]
            }
            actual_by_id: dict[str, Mapping[str, Any]] = {}
            reviewed = self._resolve_reviewed_share_files(
                session, pwd_id=pwd_id, stoken=stoken, acquisition=acquisition,
                selected_gap_ids=plan["selected_gap_ids"],
            )
            for (gap_id, declared_id, row), name in zip(reviewed, plan["file_names"]):
                fid, fid_token, size = row.get("fid"), row.get("share_fid_token"), row.get("size")
                if not isinstance(fid, str) or not isinstance(fid_token, str) or type(size) is not int or size <= 0:
                    raise QuarkShareExpiredError("selected share file metadata is incomplete")
                declared = expected_by_id.get(str(declared_id))
                if (
                    fid != declared_id or row.get("file_name") != name
                    or not isinstance(declared, Mapping) or size != declared.get("size")
                ):
                    raise QuarkShareExpiredError("share detail differs from candidate file manifest")
                if fid not in actual_by_id:
                    fids.append(fid); fid_tokens.append(fid_token); actual_by_id[fid] = row
                    expected.append({
                        "name": name, "size": size,
                        "gap_ids": list(declared["gap_ids"]),
                    })
            target_fid = self._resolve_destination(session, destination)
            if on_prepared is not None:
                on_prepared()
            saved = self._call(session, "POST", "/share/sharepage/save", api=QUARK_DRIVE_API, body={
                "fid_list": fids, "fid_token_list": fid_tokens,
                "to_pdir_fid": target_fid, "pwd_id": pwd_id, "stoken": stoken,
                "pdir_fid": "0", "scene": "link",
            })
            task_id = (saved.get("data") or {}).get("task_id")
            if not isinstance(task_id, str) or not task_id:
                raise QuarkBridgeError("Quark save did not return task_id")
            if on_submitted is not None:
                on_submitted(task_id)
        elif not isinstance(task_id, str) or not task_id:
            raise QuarkBridgeError("fast-save checkpoint task_id is invalid")
        for retry_index in range(60):
            task = self._call(session, "GET", "/task", params={
                "task_id": task_id, "retry_index": retry_index,
            })
            task_data = task.get("data") if isinstance(task.get("data"), Mapping) else {}
            if task_data.get("status") == 2:
                return {
                    "status": "submitted", "destination": destination,
                    "expected_files": expected, "task_id": task_id,
                    # Persist enough terminal evidence for the delivery layer
                    # to distinguish a briefly invisible successful save from
                    # a stale, completed task whose destination was later
                    # emptied.  These fields are non-secret Quark task
                    # metadata and do not weaken submit idempotency.
                    "task_status": 2,
                    "task_created_at": task_data.get("created_at"),
                    "task_finished_at": task_data.get("finished_at"),
                }
            self.sleep(0.5)
        raise QuarkBridgeError("Quark save task did not finish before timeout")


class QuarkMagnetOfflineBridge(QuarkFastSaveBridge):
    """Submit a reviewed magnet to Quark's cloud task queue.

    This fixture-oriented bridge deliberately shares the fixed-origin transport,
    delegated AList Cookie, destination-FID resolver, and task polling contract
    with fast-save.  It never starts a local BitTorrent client.
    """

    @staticmethod
    def _plan(selection: Mapping[str, Any], destination: str) -> dict[str, Any]:
        acquisition = selection.get("acquisition")
        selected = selection.get("selected_gap_ids")
        if (
            not isinstance(acquisition, Mapping)
            or acquisition.get("kind") != "quark_magnet_offline"
            or not isinstance(selected, list) or not selected
        ):
            raise QuarkBridgeError("candidate lacks quark_magnet_offline acquisition")
        magnet = acquisition.get("magnet_url")
        if not isinstance(magnet, str) or not magnet.startswith("magnet:?"):
            raise QuarkMagnetCandidateError("offline candidate lacks a magnet URL")
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(magnet).query)
        xt = query.get("xt", [""])[0]
        match = re.fullmatch(r"urn:btih:([0-9a-fA-F]{40}|[A-Z2-7a-z2-7]{32})", xt)
        if match is None:
            raise QuarkMagnetCandidateError("offline candidate has an invalid BTIH")
        rows = acquisition.get("expected_files")
        if not isinstance(rows, list) or not rows:
            raise QuarkMagnetCandidateError("offline candidate lacks exact expected files")
        selected_set = {
            str(gap) for gap in selected
            if isinstance(gap, str) and gap
        }
        if len(selected_set) != len(selected):
            raise QuarkMagnetCandidateError("offline selected gaps are invalid")
        expected: list[dict[str, Any]] = []
        covered: set[str] = set()
        for row in rows:
            if not isinstance(row, Mapping):
                raise QuarkMagnetCandidateError("offline expected file is invalid")
            path, size, gaps = row.get("path"), row.get("size"), row.get("gap_ids")
            if (
                not isinstance(path, str) or not path or path.startswith("/")
                or any(part in {"", ".", ".."} for part in path.replace("\\", "/").split("/"))
                or type(size) is not int or size <= 0
                or not isinstance(gaps, list) or not gaps
            ):
                raise QuarkMagnetCandidateError("offline expected file manifest is invalid")
            selected_gaps = [
                str(gap) for gap in gaps
                if isinstance(gap, str) and gap in selected_set
            ]
            # Catalog candidates may describe a complete season or release,
            # while the current unattended retry only asks for the gaps that
            # are still missing.  The cloud mutation must receive that exact
            # subset: submitting unrelated files wastes quota, but rejecting
            # the otherwise valid release causes an endless retry loop.
            if not selected_gaps:
                continue
            covered.update(selected_gaps)
            item = {"path": path.replace("\\", "/"), "size": size,
                    "gap_ids": selected_gaps}
            torrent_index = row.get("torrent_index")
            if torrent_index is not None:
                if type(torrent_index) is not int or torrent_index <= 0:
                    raise QuarkMagnetCandidateError("offline torrent index is invalid")
                item["torrent_index"] = torrent_index
            expected.append(item)
        if covered != selected_set:
            raise QuarkMagnetCandidateError("offline expected files do not cover selected gaps")
        return {
            "status": "dry_run", "provider": "quark_magnet",
            "destination": destination, "magnet_url": magnet,
            "infohash": match.group(1).lower(), "selected_gap_ids": list(selected),
            "expected_files": expected,
            "endpoints": [
                "file/sort", "offline/download/parse",
                "offline/download/submit", "offline/save_to/progress",
            ],
        }

    @classmethod
    def dry_run(cls, selection: Mapping[str, Any], destination: str) -> dict[str, Any]:
        return cls._plan(selection, destination)

    def _call(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        try:
            return super()._call(*args, **kwargs)
        except (QuarkMagnetCandidateError, QuarkMagnetDeliveryError,
                QuarkMagnetInfrastructureError):
            raise
        except QuarkBridgeError as exc:
            raise QuarkMagnetInfrastructureError(str(exc)) from exc

    def execute(
        self, selection: Mapping[str, Any], destination: str, session: QuarkSession,
        *, resume_task_id: str | None = None,
        on_submitted: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        plan = self._plan(selection, destination)
        task_id = resume_task_id
        if task_id is None:
            if getattr(self.transport, "supports_wsg", False) is not True:
                raise QuarkMagnetWsgUnavailableError(
                    "Quark offline requires a WSG-capable transport; plain JSON is disabled"
                )
            target_fid = self._resolve_destination(session, destination)
            try:
                parsed = self._call(
                    session, "POST", "/offline/download/parse", params={"api_ver": 2},
                    body={
                        # Quark's native client omits nullable/desktop-only
                        # fields here.  Sending them as explicit JSON nulls is
                        # not equivalent: the service decrypts the WSG body but
                        # then returns an internal error instead of a manifest.
                        "url": plan["magnet_url"], "parse_mode": 0,
                        "cookie": "", "entry": "download",
                        "req_info": {"method": "", "body": "", "is_multipart": False},
                        "conflict_mode": 4, "auto_download": False,
                        "support_v2_play": True,
                    },
                )
            except QuarkMagnetInfrastructureError as exc:
                raise QuarkMagnetParseInfrastructureError(str(exc)) from exc
            parse_data = parsed.get("data") if isinstance(parsed.get("data"), Mapping) else {}
            token = parse_data.get("token")
            files = parse_data.get("files") or parse_data.get("list")
            if not isinstance(token, str) or not token or not isinstance(files, list):
                # The torrent manifest was already verified independently.
                # An empty/malformed cloud parse response proves only that the
                # Quark lane could not inspect it now; it does not prove the
                # selected release is bad.  Retry/cool down this lane without
                # poisoning the candidate locator or infoHash.
                raise QuarkMagnetParseInfrastructureError(
                    "Quark offline parse lacks token/file listing"
                )
            selected_files: list[Any] = []
            for expected in plan["expected_files"]:
                matches = []
                for row in files:
                    if not isinstance(row, Mapping):
                        continue
                    raw_path = row.get("path") or row.get("file_path") or row.get("file_name")
                    if (
                        isinstance(raw_path, str)
                        and raw_path.replace("\\", "/") == expected["path"]
                        and row.get("size") == expected["size"]
                    ):
                        matches.append(row)
                if len(matches) != 1:
                    raise QuarkMagnetCandidateError(
                        "Quark offline parse differs from exact candidate manifest"
                    )
                index = matches[0].get(
                    "index", matches[0].get("file_index", matches[0].get("file_no")),
                )
                if isinstance(index, bool) or not isinstance(index, (int, str)):
                    raise QuarkMagnetCandidateError("Quark offline parse file lacks selection index")
                # Quark's parse index counts directory nodes, while a torrent
                # metainfo index counts files only.  They are different index
                # spaces.  The reviewed torrent index remains in the durable
                # manifest; the submit index must be the value resolved from
                # the unique full-path + exact-size Quark parse row.
                try:
                    int(index)
                except (TypeError, ValueError) as exc:
                    raise QuarkMagnetCandidateError(
                        "Quark offline parse file index is invalid"
                    ) from exc
                selected_files.append(index)
            submitted = self._call(
                session, "POST", "/offline/download/submit", body={
                    "token": token, "entry": "download", "parse_mode": 0,
                    "pdir_fid": target_fid, "auto_select": False,
                    "selected_files": selected_files,
                    "title": selection.get("release_name"),
                },
            )
            submit_data = submitted.get("data") if isinstance(submitted.get("data"), Mapping) else {}
            task_id = submit_data.get("task_id")
            if not isinstance(task_id, str) or not task_id:
                raise QuarkMagnetInfrastructureError(
                    "Quark offline submit did not return task_id"
                )
            if on_submitted is not None:
                on_submitted(task_id)
        elif not isinstance(task_id, str) or not task_id:
            raise QuarkMagnetInfrastructureError("offline checkpoint task_id is invalid")
        try:
            progress_polls = int(os.getenv(
                "SCRAPEFLOW_QUARK_OFFLINE_PROGRESS_POLLS", "21600",
            ))
        except ValueError:
            progress_polls = 21600
        progress_polls = max(60, min(86400, progress_polls))
        for retry_index in range(progress_polls):
            progress = self._call(
                session, "POST", "/offline/save_to/progress",
                params={"api_ver": 3}, body={
                    "task_ids": [task_id], "query_times": retry_index,
                    "req_scene": "poll_wait", "support_v2_play": True,
                },
            )
            data = progress.get("data")
            tasks = (
                data.get("list") if isinstance(data, Mapping) and isinstance(data.get("list"), list)
                else data if isinstance(data, list) else [data] if isinstance(data, Mapping) else []
            )
            task = next(
                (row for row in tasks if isinstance(row, Mapping)
                 and row.get("task_id", task_id) == task_id),
                None,
            )
            status = task.get("status") if isinstance(task, Mapping) else None
            if status in {2, "finished", "success"}:
                return {
                    "status": "submitted", "destination": destination,
                    "expected_files": plan["expected_files"], "task_id": task_id,
                    "infohash": plan["infohash"],
                }
            if status in {3, 4, "failed", "error"}:
                raise QuarkMagnetCandidateError("Quark offline task rejected the magnet payload")
            self.sleep(1.0)
        raise QuarkMagnetDeliveryError("Quark offline task did not finish before timeout")
