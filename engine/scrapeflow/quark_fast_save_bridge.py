"""Minimal Quark share fast-save bridge with injectable transport.

The bridge is deliberately narrow: it validates a reviewed share file manifest,
delegates the matching AList Quark session in memory, saves only the selected
file IDs to the task attempt staging directory, and polls the returned Quark
task.  It does not know formal library paths.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import json
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
QUARK_SHARE_INVALID_CODES = frozenset({
    41004, 41006, 41010, 41011, 41012, 41017, 41019, 41031,
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
    """A save may have reached Quark but its durable task id was not known."""

    failure_scope = "in_doubt"
    failure_stage = "quark_fast_save_submit_in_doubt"
    reusable_candidate = True


@dataclass(frozen=True)
class QuarkSession:
    mount_path: str
    root_id: str
    cookie: str


class JsonTransport(Protocol):
    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any],
        body: Mapping[str, Any] | None,
        cookie: str,
    ) -> Mapping[str, Any]: ...


class _RejectRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject redirects before urllib can replay the delegated Cookie."""

    def redirect_request(self, _req, _fp, _code, _msg, _headers, _newurl):
        return None


def _build_quark_opener() -> urllib.request.OpenerDirector:
    """Build a transport opener without consulting ambient proxy settings."""
    # A delegated AList Cookie is valid only at the reviewed Quark API
    # boundary.  Passing an explicit empty mapping is important: ProxyHandler
    # otherwise reads HTTP(S)_PROXY/NO_PROXY from the process environment.
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _RejectRedirectHandler(),
    )


# Keep this opener process-local, proxy-free, redirect-free, and without a
# cookie jar so ambient host configuration cannot widen that boundary.
_QUARK_OPENER = _build_quark_opener()


def _open_quark_request(request: Any, timeout: float) -> Any:
    return _QUARK_OPENER.open(request, timeout=timeout)


def _approved_quark_endpoint(endpoint: str) -> bool:
    """Require an unadorned URL below one of the two fixed Quark API bases."""
    try:
        parsed = urllib.parse.urlsplit(endpoint)
        port = parsed.port
    except (TypeError, ValueError):
        return False
    if (
        parsed.scheme != "https"
        or parsed.username
        or parsed.password
        or port is not None
        or parsed.query
        or parsed.fragment
    ):
        return False
    for base in (QUARK_SHARE_API, QUARK_DRIVE_API):
        allowed = urllib.parse.urlsplit(base)
        if (
            parsed.hostname == allowed.hostname
            and parsed.netloc == allowed.netloc
            and parsed.path.startswith(allowed.path + "/")
        ):
            return True
    return False


class UrlLibQuarkTransport:
    """Fixed-origin JSON transport which never exposes the cookie in errors."""

    def __init__(
        self,
        *,
        timeout: float = 60.0,
        opener: Callable[..., Any] = _open_quark_request,
    ) -> None:
        self.timeout = timeout
        self.opener = opener

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any],
        body: Mapping[str, Any] | None,
        cookie: str,
    ) -> Mapping[str, Any]:
        if not _approved_quark_endpoint(endpoint):
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
            with self.opener(request, timeout=self.timeout) as response:
                geturl = getattr(response, "geturl", None)
                final_url = geturl() if callable(geturl) else None
                if final_url != url:
                    raise QuarkBridgeError(
                        "Quark response URL escaped the fixed API endpoint"
                    )
                value = json.load(response)
        except QuarkBridgeError:
            raise
        except urllib.error.HTTPError as exc:
            if 300 <= exc.code < 400:
                raise QuarkBridgeError("Quark redirect refused") from exc
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
                r"(?i)(cookie|token|authorization)\s*[:=]\s*\S+",
                r"\1=<redacted>",
                message,
            )[:240]
            error = QuarkShareExpiredError if code in QUARK_SHARE_INVALID_CODES else QuarkBridgeError
            raise error(
                f"Quark HTTP error: status={exc.code}, code={code!r}, "
                f"message={safe_message!r}"
            ) from exc
        except Exception as exc:
            raise QuarkBridgeError(f"Quark request failed: {type(exc).__name__}") from exc
        if not isinstance(value, Mapping):
            raise QuarkBridgeError("Quark returned a non-object response")
        return value


def normalize_quark_fast_save_selection(selection: Mapping[str, Any]) -> dict[str, Any]:
    """Turn a reviewed share file-id/path/size map into an exact save manifest."""
    if str(selection.get("provider") or "").strip().casefold() != "quark_share":
        raise QuarkBridgeError("candidate is not quark_share")
    output = dict(selection)
    acquisition = selection.get("acquisition")
    if not isinstance(acquisition, Mapping) or acquisition.get("kind") != "quark_fast_save":
        raise QuarkBridgeError("candidate lacks quark_fast_save acquisition")
    gap_map = acquisition.get("file_id_by_gap")
    path_map = acquisition.get("file_path_by_id")
    size_map = acquisition.get("file_size_by_id")
    selected = selection.get("selected_gap_ids")
    if (
        not isinstance(gap_map, Mapping)
        or not isinstance(path_map, Mapping)
        or not isinstance(size_map, Mapping)
        or not isinstance(selected, list)
        or not selected
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
        if not name or name in {".", ".."} or "/" in name or "\\" in name:
            raise QuarkBridgeError("Quark file path has no safe basename")
        file_name_by_gap[gap] = name
        row = by_id.setdefault(file_id, {
            "file_id": file_id,
            "name": name,
            "size": raw_size,
            "gap_ids": [],
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
    admin_storages = getattr(alist_client, "admin_storages", None)
    if not callable(admin_storages):
        raise QuarkBridgeError("AList client cannot delegate Quark session")
    matches: list[tuple[int, QuarkSession]] = []
    for row in admin_storages():
        if not isinstance(row, Mapping):
            continue
        mount = str(row.get("mount_path") or "").rstrip("/") or "/"
        if (
            row.get("driver") != "Quark"
            or row.get("disabled") is True
            or not (destination == mount or destination.startswith(mount.rstrip("/") + "/"))
        ):
            continue
        try:
            addition = json.loads(str(row.get("addition") or "{}"))
        except json.JSONDecodeError as exc:
            raise QuarkBridgeError("AList Quark storage addition is invalid") from exc
        if not isinstance(addition, Mapping):
            raise QuarkBridgeError("AList Quark storage addition is invalid")
        cookie = addition.get("cookie")
        if not isinstance(cookie, str) or not cookie.strip():
            raise QuarkBridgeError("AList Quark storage has no delegated login state")
        # AList v3.62 names this field ``root_folder_id``.  Older storage
        # rows used ``root_id``.  A missing or malformed modern field must
        # never silently fall back to account root ("0"), because that could
        # turn a dedicated acceptance mount into a writer for the whole Quark
        # account.  When both schema variants appear, require them to agree.
        if "root_folder_id" in addition:
            root_id = addition["root_folder_id"]
            legacy_root_id = addition.get("root_id")
            if (
                not isinstance(root_id, str)
                or not root_id
                or root_id != root_id.strip()
                or (
                    "root_id" in addition
                    and (
                        not isinstance(legacy_root_id, str)
                        or not legacy_root_id
                        or legacy_root_id != root_id
                    )
                )
            ):
                raise QuarkBridgeError("AList Quark storage root folder is invalid")
        else:
            root_id = addition.get("root_id")
            if (
                not isinstance(root_id, str)
                or not root_id
                or root_id != root_id.strip()
            ):
                raise QuarkBridgeError("AList Quark storage root folder is invalid")
        matches.append((len(mount), QuarkSession(mount, root_id, cookie)))
    if not matches:
        raise QuarkBridgeError("no enabled AList Quark storage covers destination")
    return max(matches, key=lambda item: item[0])[1]


class QuarkFastSaveBridge:
    MAX_SHARE_DEPTH = 12
    MAX_SHARE_DIRECTORIES = 256
    MAX_SHARE_ENTRIES = 4096

    def __init__(self, transport: JsonTransport, *, sleep: Callable[[float], None] = time.sleep) -> None:
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
            not isinstance(pwd_id, str)
            or not pwd_id
            or not isinstance(names, Mapping)
            or not isinstance(selected, list)
            or not selected
            or any(not isinstance(names.get(gap), str) or not names.get(gap) for gap in selected)
        ):
            raise QuarkBridgeError("quark_fast_save acquisition is incomplete")
        return {
            "status": "dry_run",
            "provider": "quark_share",
            "destination": destination,
            "pwd_id": pwd_id,
            "selected_gap_ids": list(selected),
            "file_names": [str(names[gap]) for gap in selected],
            "expected_files": list(acquisition["expected_files"]),
            "endpoints": ["sharepage/token", "sharepage/detail", "sharepage/save", "task"],
        }

    def _call(
        self,
        session: QuarkSession,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Mapping[str, Any] | None = None,
        api: str = QUARK_SHARE_API,
    ) -> Mapping[str, Any]:
        value = self.transport.request(
            method,
            api + path,
            params={"pr": "ucpro", "fr": "pc", **dict(params or {})},
            body=body,
            cookie=session.cookie,
        )
        if value.get("code") not in {0, None} or value.get("status") not in {200, None}:
            code = value.get("code")
            error = QuarkShareExpiredError if code in QUARK_SHARE_INVALID_CODES else QuarkBridgeError
            # Quark business-error text is server-controlled and may echo a
            # cookie, token, or signed locator.  Keep it out of the exception
            # entirely; callers persist/log the exception at several state
            # boundaries and cannot safely assume the text is redacted.
            raise error(f"Quark operation failed: code={code}")
        return value

    def _resolve_destination(self, session: QuarkSession, destination: str) -> str:
        relative = destination[len(session.mount_path):].strip("/")
        parent = session.root_id
        for component in (part for part in relative.split("/") if part):
            value = self._call(session, "GET", "/file/sort", api=QUARK_DRIVE_API, params={
                "pdir_fid": parent,
                "_page": 1,
                "_size": 100,
                "_fetch_total": 1,
                "fetch_all_file": 1,
            })
            rows = (value.get("data") or {}).get("list")
            matches = [
                row for row in rows
                if isinstance(row, Mapping)
                and row.get("file_name") == component
                and row.get("file") is False
            ] if isinstance(rows, list) else []
            if len(matches) != 1 or not isinstance(matches[0].get("fid"), str):
                raise QuarkBridgeError("destination directory is not uniquely resolvable")
            parent = str(matches[0]["fid"])
        return parent

    def _resolve_reviewed_share_files(
        self,
        session: QuarkSession,
        *,
        pwd_id: str,
        stoken: str,
        acquisition: Mapping[str, Any],
        selected_gap_ids: list[str],
    ) -> list[tuple[str, str, Mapping[str, Any]]]:
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
                    "pwd_id": pwd_id,
                    "stoken": stoken,
                    "pdir_fid": parent,
                    "force": 0,
                    "_page": page,
                    "_size": 100,
                    "_fetch_total": 1,
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
                not parts
                or len(parts) > self.MAX_SHARE_DEPTH
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
        self,
        session: QuarkSession,
        *,
        pwd_id: str,
        passcode: str = "",
    ) -> list[dict[str, Any]]:
        """Return a bounded, read-only recursive file manifest for one share."""
        token = self._call(session, "POST", "/share/sharepage/token", body={
            "pwd_id": pwd_id,
            "passcode": passcode,
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
                    "pwd_id": pwd_id,
                    "stoken": stoken,
                    "pdir_fid": parent,
                    "force": 0,
                    "_page": page,
                    "_size": 100,
                    "_fetch_total": 1,
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
                        not isinstance(name, str)
                        or not name
                        or name in {".", ".."}
                        or "/" in name
                        or "\\" in name
                        or not isinstance(fid, str)
                        or not fid
                    ):
                        raise QuarkShareExpiredError("share contains unsafe file metadata")
                    path = "/".join((*prefix, name))
                    if row.get("file") is False:
                        if depth >= self.MAX_SHARE_DEPTH:
                            raise QuarkShareExpiredError("share exceeds directory depth limit")
                        queue.append((fid, (*prefix, name), depth + 1))
                        continue
                    size = row.get("size")
                    if type(size) is not int or size <= 0:
                        raise QuarkShareExpiredError("share file has invalid size")
                    output.append({"file_id": fid, "path": path, "size": size})
                if len(rows) < 100:
                    break
            else:
                raise QuarkShareExpiredError("share exceeds pagination limit")
        return output

    @staticmethod
    def _safe_task_id(value: object) -> str | None:
        if (
            isinstance(value, str)
            and value
            and len(value) <= 256
            and not any(char in value for char in ("/", "\\", "\x00", "\n", "\r"))
        ):
            return value
        return None

    def _wait_for_task(
        self,
        session: QuarkSession,
        *,
        task_id: str,
        destination: str,
        expected_files: list[Mapping[str, Any]],
    ) -> dict[str, Any]:
        for retry_index in range(60):
            task = self._call(session, "GET", "/task", params={
                "task_id": task_id,
                "retry_index": retry_index,
            })
            task_data = task.get("data") if isinstance(task.get("data"), Mapping) else {}
            if task_data.get("status") == 2:
                return {
                    "status": "submitted",
                    "destination": destination,
                    "expected_files": [dict(row) for row in expected_files],
                    "task_id": task_id,
                }
            self.sleep(0.5)
        raise QuarkBridgeError("Quark save task did not finish before timeout")

    def execute(
        self,
        selection: Mapping[str, Any],
        destination: str,
        session: QuarkSession,
        *,
        task_id: str | None = None,
        on_task_id: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        selection = normalize_quark_fast_save_selection(selection)
        plan = self.dry_run(selection, destination)
        existing_task_id = self._safe_task_id(task_id)
        if task_id is not None and existing_task_id is None:
            raise QuarkBridgeError("Quark save task_id is invalid")
        if existing_task_id is not None:
            return self._wait_for_task(
                session,
                task_id=existing_task_id,
                destination=destination,
                expected_files=plan["expected_files"],
            )
        acquisition = selection["acquisition"]
        pwd_id = str(acquisition.get("pwd_id") or acquisition.get("share_id"))
        token = self._call(session, "POST", "/share/sharepage/token", body={
            "pwd_id": pwd_id,
            "passcode": str(acquisition.get("passcode") or ""),
        })
        stoken = (token.get("data") or {}).get("stoken")
        if not isinstance(stoken, str) or not stoken:
            raise QuarkShareExpiredError("Quark share did not return stoken")
        expected = []
        fids: list[str] = []
        fid_tokens: list[str] = []
        expected_by_id = {
            str(row["file_id"]): row
            for row in acquisition["expected_files"]
        }
        actual_by_id: dict[str, Mapping[str, Any]] = {}
        reviewed = self._resolve_reviewed_share_files(
            session,
            pwd_id=pwd_id,
            stoken=stoken,
            acquisition=acquisition,
            selected_gap_ids=plan["selected_gap_ids"],
        )
        for (gap_id, declared_id, row), name in zip(reviewed, plan["file_names"]):
            fid = row.get("fid")
            fid_token = row.get("share_fid_token")
            size = row.get("size")
            if (
                not isinstance(fid, str)
                or not isinstance(fid_token, str)
                or type(size) is not int
                or size <= 0
            ):
                raise QuarkShareExpiredError("selected share file metadata is incomplete")
            declared = expected_by_id.get(str(declared_id))
            if (
                fid != declared_id
                or row.get("file_name") != name
                or not isinstance(declared, Mapping)
                or size != declared.get("size")
            ):
                raise QuarkShareExpiredError("share detail differs from candidate file manifest")
            if fid not in actual_by_id:
                fids.append(fid)
                fid_tokens.append(fid_token)
                actual_by_id[fid] = row
                expected.append({
                    "name": name,
                    "size": size,
                    "gap_ids": list(declared["gap_ids"]),
                })
        target_fid = self._resolve_destination(session, destination)
        try:
            saved = self._call(session, "POST", "/share/sharepage/save", api=QUARK_DRIVE_API, body={
                "fid_list": fids,
                "fid_token_list": fid_tokens,
                "to_pdir_fid": target_fid,
                "pwd_id": pwd_id,
                "stoken": stoken,
                "pdir_fid": "0",
                "scene": "link",
            })
        except QuarkShareExpiredError:
            raise
        except QuarkBridgeError as exc:
            raise QuarkShareInDoubtError(
                "Quark save response is unknown; reconcile before retrying"
            ) from exc
        saved_task_id = self._safe_task_id((saved.get("data") or {}).get("task_id"))
        if saved_task_id is None:
            raise QuarkShareInDoubtError(
                "Quark save did not return task_id; reconcile before retrying"
            )
        if on_task_id is not None:
            try:
                on_task_id(saved_task_id)
            except Exception as exc:
                raise QuarkShareInDoubtError(
                    "Quark save task was submitted but local attempt state was not saved"
                ) from exc
        return self._wait_for_task(
            session,
            task_id=saved_task_id,
            destination=destination,
            expected_files=expected,
        )


__all__ = [
    "QUARK_DRIVE_API",
    "QUARK_SHARE_API",
    "QuarkBridgeError",
    "QuarkFastSaveBridge",
    "QuarkSession",
    "QuarkShareInDoubtError",
    "QuarkShareExpiredError",
    "UrlLibQuarkTransport",
    "delegated_quark_session",
    "normalize_quark_fast_save_selection",
]
