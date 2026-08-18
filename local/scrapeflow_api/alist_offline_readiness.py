"""Read-only preflight for ScrapeFlow's AList/aria2 offline lane.

The normal materializer deliberately does not expose a harmless "submit a
sample" operation: an AList offline task is a real download and a failed
sample can itself consume disk.  This module instead verifies the complete
configuration/reachability chain with read-only APIs before a bounded pilot is
allowed to resume.

It proves that AList registered the ``aria2`` tool, accepts the configured
credentials, exposes its task manager, can reach the configured aria2 RPC
server, and has a readable enabled storage route for the derived staging
namespace.  It intentionally does *not* claim that a real file transfer has
completed; proving that would require submitting a task and is outside this
preflight's safety boundary.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
import json
import os
import posixpath
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

from engine.scrapeflow.provider_capabilities import ALIST_OFFLINE_TOOL_NAME

from .provider_staging import (
    ProviderStagingPathError,
    replenishment_staging_root_for_media_root,
)
from .redaction import redact_error


ARIA2_RPC_EXPECTED_PORT = 6800
ARIA2_RPC_EXPECTED_DIR = "/opt/alist/data/temp/aria2"
DEFAULT_ARIA2_HOSTS = frozenset({"offline-aria2"})
# AList v3's admin storage row does not consistently expose a ``NoUpload``
# field.  This is the one reviewed driver used by this product's mounted
# `/quark` storage.  Every other driver remains fail-closed until it gets an
# equally explicit read-only capability proof.
_UPLOAD_CAPABLE_STORAGE_DRIVERS = frozenset({"Quark"})


class AlistOfflineReadinessError(RuntimeError):
    """The no-write AList offline preflight could not verify a dependency."""


Aria2RpcCall = Callable[[str, str, str], Mapping[str, object]]


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _safe_text(
    value: object,
    *,
    limit: int = 160,
    secrets: tuple[str, ...] = (),
) -> str:
    """Return a bounded diagnostic that never reflects credentials verbatim."""
    text = redact_error(value).replace("\r", " ").replace("\n", " ").strip()
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text[:limit] or "unknown"


def _result(status: str, *, detail: Mapping[str, object] | None = None) -> dict[str, object]:
    payload: dict[str, object] = {"status": status, "verified": status == "ready"}
    if detail:
        payload.update(dict(detail))
    return payload


def _configured_aria2_hosts(environ: Mapping[str, object] | None = None) -> frozenset[str]:
    """Return the shared Compose DNS name allowed for direct RPC checks.

    The probe is made by the API container, whereas AList itself initiates
    offline-download RPC from its own container.  ``localhost`` or loopback
    would therefore validate the wrong network namespace and produce a false
    readiness result.  The reviewed Compose topology gives both containers
    the ``offline-aria2`` DNS name; a different value is deliberately not an
    operator escape hatch for this preflight.
    """
    source = os.environ if environ is None else environ
    raw = source.get("SCRAPEFLOW_ALIST_OFFLINE_ARIA2_HOST", "offline-aria2")
    if not isinstance(raw, str):
        raise AlistOfflineReadinessError(
            "SCRAPEFLOW_ALIST_OFFLINE_ARIA2_HOST 必须是 aria2 服务主机名"
        )
    host = raw.strip().casefold()
    if host != "offline-aria2":
        raise AlistOfflineReadinessError(
            "SCRAPEFLOW_ALIST_OFFLINE_ARIA2_HOST 必须是 Compose 服务 offline-aria2"
        )
    return DEFAULT_ARIA2_HOSTS


def validate_aria2_rpc_url(
    value: object,
    *,
    environ: Mapping[str, object] | None = None,
) -> str:
    """Validate the AList-configured RPC target without exposing its secret."""
    if not isinstance(value, str) or not value.strip():
        raise AlistOfflineReadinessError("AList 未配置 aria2 RPC 地址")
    url = value.strip()
    try:
        parsed = urlsplit(url)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        raise AlistOfflineReadinessError("AList aria2 RPC 地址无效") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or port != ARIA2_RPC_EXPECTED_PORT
        or parsed.hostname.casefold() not in _configured_aria2_hosts(environ)
        or parsed.path.rstrip("/") != "/jsonrpc"
    ):
        raise AlistOfflineReadinessError(
            "AList aria2 RPC 必须指向共享 Compose 服务 offline-aria2:6800/jsonrpc（不能使用 localhost）"
        )
    return url


def _json_rpc_call(url: str, secret: str, method: str) -> Mapping[str, object]:
    """Call one aria2 read-only RPC method without inheriting host proxies."""
    if method not in {"aria2.getVersion", "aria2.getGlobalOption"}:
        raise AlistOfflineReadinessError("不允许的 aria2 预检方法")
    params: list[str] = [f"token:{secret}"] if secret else []
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "scrapeflow-readiness",
            "method": method,
            "params": params,
        },
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    request = Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(request, timeout=4.0) as response:
            raw = response.read(64 * 1024)
    except HTTPError as exc:
        raw = exc.read(64 * 1024)
        raise AlistOfflineReadinessError(
            f"aria2 RPC returned HTTP {int(exc.code)}"
        ) from exc
    except URLError as exc:
        raise AlistOfflineReadinessError("aria2 RPC 不可达") from exc
    except OSError as exc:
        raise AlistOfflineReadinessError("aria2 RPC 连接失败") from exc
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AlistOfflineReadinessError("aria2 RPC 未返回 JSON") from exc
    if not isinstance(decoded, Mapping):
        raise AlistOfflineReadinessError("aria2 RPC 响应格式无效")
    if decoded.get("error") is not None:
        raise AlistOfflineReadinessError("aria2 RPC 拒绝只读探针")
    result = decoded.get("result")
    if not isinstance(result, Mapping):
        raise AlistOfflineReadinessError("aria2 RPC 未返回结果")
    return dict(result)


def _ensure_authenticated(alist: object) -> None:
    """Perform only AList authentication when the client exposes it."""
    login = getattr(alist, "login", None)
    if callable(login) and not getattr(alist, "token", None):
        login()


def _call_list(alist: object, path: str) -> list[Mapping[str, object]]:
    listing = getattr(alist, "list", None)
    if not callable(listing):
        raise AlistOfflineReadinessError("AList 客户端缺少只读目录列表")
    try:
        rows = listing(path, refresh=True)
    except TypeError:
        rows = listing(path)
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise AlistOfflineReadinessError("AList 目录列表响应无效")
    return list(rows)


def _storage_for_path(
    storages: object,
    path: str,
    *,
    path_label: str,
) -> Mapping[str, object]:
    if not isinstance(storages, list):
        raise AlistOfflineReadinessError("AList storage 列表响应无效")
    candidates: list[tuple[int, Mapping[str, object]]] = []
    for row in storages:
        if not isinstance(row, Mapping):
            raise AlistOfflineReadinessError("AList storage 项无效")
        mount = row.get("mount_path")
        if not isinstance(mount, str) or not mount.startswith("/"):
            continue
        normalized = posixpath.normpath(mount)
        if normalized != mount or "\\" in mount or "\x00" in mount:
            continue
        prefix = normalized.rstrip("/") or "/"
        if path == prefix or (prefix != "/" and path.startswith(prefix + "/")) or prefix == "/":
            candidates.append((len(prefix), row))
    if not candidates:
        raise AlistOfflineReadinessError(f"{path_label} 没有对应的 AList storage")
    _length, storage = max(candidates, key=lambda item: item[0])
    if storage.get("disabled") is True:
        raise AlistOfflineReadinessError(f"{path_label} 对应的 AList storage 已禁用")
    driver = storage.get("driver")
    if not isinstance(driver, str) or not driver.strip():
        raise AlistOfflineReadinessError(f"{path_label} 对应的 AList storage 未声明 driver")
    return storage


def _offline_sibling_for_staging_root(staging_root: str) -> str:
    """Mirror the materializer's exact AList ``AddURL`` destination.

    ``AlistOfflineAutomaticMaterializer._offline_sibling`` places its AddURL
    destination next to the staging root, named ``<staging>__offline__``.  Do
    not validate the broader media root here: a nested mount can change the
    upload semantics specifically for this destination.
    """
    parent = posixpath.dirname(staging_root.rstrip("/"))
    name = posixpath.basename(staging_root.rstrip("/"))
    if not parent or not name:
        raise AlistOfflineReadinessError("AList 离线 staging sibling 无效")
    return f"{parent}/{name}__offline__"


def _verified_upload_capability(storage: Mapping[str, object]) -> dict[str, object]:
    """Prove the selected destination storage can accept AList transfer.

    AList v3.62's storage row contains ``disabled``/``status`` but, on this
    deployed schema, omits ``NoUpload``.  An explicit upload-disable flag is
    always authoritative when present.  If it is absent, only the reviewed
    Quark driver with an active ``work`` status is accepted; unknown drivers
    must not turn a read-only list response into a green transfer route.
    """
    if storage.get("disabled") is not False:
        raise AlistOfflineReadinessError("AList 离线 destination storage 未明确启用")
    status = storage.get("status")
    if not isinstance(status, str) or status.casefold() != "work":
        raise AlistOfflineReadinessError("AList 离线 destination storage 未处于 work 状态")
    no_upload_key: str | None = None
    no_upload: bool | None = None
    for key in ("NoUpload", "no_upload"):
        value = storage.get(key)
        if value is not None:
            if type(value) is not bool:
                raise AlistOfflineReadinessError("AList storage NoUpload 能力字段无效")
            no_upload_key = key
            no_upload = value
            break
    if no_upload is True:
        raise AlistOfflineReadinessError("AList 离线 destination storage 禁止上传")
    driver = storage.get("driver")
    if not isinstance(driver, str) or driver not in _UPLOAD_CAPABLE_STORAGE_DRIVERS:
        raise AlistOfflineReadinessError(
            "AList 离线 destination storage 未提供可验证的上传能力"
        )
    return {
        "storage_status": status,
        "upload_capability": "reviewed_driver",
        "no_upload_field": no_upload_key,
        "no_upload": no_upload,
    }


def _method_configured(alist: object, name: str) -> bool:
    return callable(getattr(alist, name, None))


def alist_offline_readiness(
    alist: object | None,
    *,
    media_root: str,
    environ: Mapping[str, object] | None = None,
    rpc_call: Aria2RpcCall = _json_rpc_call,
) -> dict[str, object]:
    """Return a redacted, no-write readiness report for the offline lane.

    It never calls AList's add/mkdir/move/delete/cancel operations and never
    persists a record.  Authentication may refresh a bearer token in memory,
    which is necessary to prove the configured account has task-manager
    access; no media or task state is changed.
    """
    checks: dict[str, dict[str, object]] = {}
    issues: list[str] = []
    configured = alist is not None
    try:
        staging_root = replenishment_staging_root_for_media_root(media_root)
    except ProviderStagingPathError as exc:
        staging_root = None
        configured = False
        issues.append("媒体根不能派生安全的 AList 离线 staging")
        checks["transfer"] = _result("not_ready", detail={"reason": _safe_text(exc)})

    required_methods = (
        "offline_download_tools",
        "offline_download_aria2_settings",
        "admin_storages",
        "offline_download_undone",
        "offline_download_done",
        "list",
    )
    if alist is None:
        checks["client"] = _result("not_ready", detail={"reason": "AList 客户端未配置"})
        issues.append("AList 客户端未配置")
    else:
        missing = [name for name in required_methods if not _method_configured(alist, name)]
        if missing:
            configured = False
            checks["client"] = _result(
                "not_ready", detail={"missing_methods": sorted(missing)},
            )
            issues.append("AList 客户端缺少离线预检所需只读接口")
        else:
            checks["client"] = _result("ready")

    tools: list[str] | None = None
    if alist is not None and _method_configured(alist, "offline_download_tools"):
        try:
            raw_tools = getattr(alist, "offline_download_tools")()
            if not isinstance(raw_tools, list) or any(not isinstance(item, str) for item in raw_tools):
                raise AlistOfflineReadinessError("AList 离线工具列表响应无效")
            tools = [item.strip() for item in raw_tools if item.strip()]
            if ALIST_OFFLINE_TOOL_NAME not in tools:
                raise AlistOfflineReadinessError("AList 未注册 aria2 离线工具")
            checks["tool"] = _result("ready", detail={"name": ALIST_OFFLINE_TOOL_NAME})
        except Exception as exc:
            checks["tool"] = _result("not_ready", detail={"reason": _safe_text(exc)})
            issues.append("AList aria2 离线工具不可用")
    else:
        checks["tool"] = _result("not_ready", detail={"reason": "客户端未提供工具列表"})
        issues.append("无法读取 AList 离线工具列表")

    storages: list[Mapping[str, object]] | None = None
    authenticated = False
    if alist is not None and _method_configured(alist, "admin_storages"):
        try:
            _ensure_authenticated(alist)
            raw_storages = getattr(alist, "admin_storages")()
            if not isinstance(raw_storages, list):
                raise AlistOfflineReadinessError("AList storage 列表响应无效")
            storages = [row for row in raw_storages if isinstance(row, Mapping)]
            if len(storages) != len(raw_storages):
                raise AlistOfflineReadinessError("AList storage 列表包含无效项")
            authenticated = True
            checks["authentication"] = _result("ready")
        except Exception as exc:
            checks["authentication"] = _result("not_ready", detail={"reason": _safe_text(exc)})
            issues.append("AList 管理认证或 storage 读取失败")
    else:
        checks["authentication"] = _result("not_ready", detail={"reason": "客户端未提供 storage 列表"})
        issues.append("无法验证 AList 管理认证")

    if authenticated and alist is not None:
        try:
            undone = getattr(alist, "offline_download_undone")()
            done = getattr(alist, "offline_download_done")()
            if not isinstance(undone, list) or not isinstance(done, list):
                raise AlistOfflineReadinessError("AList 离线任务列表响应无效")
            checks["task_manager"] = _result(
                "ready", detail={"undone_count": len(undone), "done_count": len(done)},
            )
        except Exception as exc:
            checks["task_manager"] = _result("not_ready", detail={"reason": _safe_text(exc)})
            issues.append("AList 离线任务管理器不可读")
    else:
        checks["task_manager"] = _result("not_ready", detail={"reason": "认证未通过"})

    aria2_settings: Mapping[str, object] | None = None
    if authenticated and alist is not None and _method_configured(alist, "offline_download_aria2_settings"):
        try:
            raw_settings = getattr(alist, "offline_download_aria2_settings")()
            if not isinstance(raw_settings, Mapping):
                raise AlistOfflineReadinessError("AList aria2 设置响应无效")
            uri = validate_aria2_rpc_url(raw_settings.get("aria2_uri"), environ=environ)
            secret = raw_settings.get("aria2_secret")
            if not isinstance(secret, str) or not secret.strip():
                raise AlistOfflineReadinessError("AList aria2 secret 必须为非空值")
            aria2_settings = {"aria2_uri": uri, "aria2_secret": secret}
        except Exception as exc:
            checks["aria2"] = _result("not_ready", detail={"reason": _safe_text(exc)})
            issues.append("AList aria2 配置不可用")
    elif "aria2" not in checks:
        checks["aria2"] = _result("not_ready", detail={"reason": "认证未通过或客户端不支持设置读取"})

    aria2_options: Mapping[str, object] | None = None
    if aria2_settings is not None:
        secret = ""
        try:
            uri = str(aria2_settings["aria2_uri"])
            secret = str(aria2_settings["aria2_secret"])
            version = rpc_call(uri, secret, "aria2.getVersion")
            version_text = version.get("version") if isinstance(version, Mapping) else None
            if not isinstance(version_text, str) or not version_text.strip():
                raise AlistOfflineReadinessError("aria2 未返回版本")
            options = rpc_call(uri, secret, "aria2.getGlobalOption")
            if not isinstance(options, Mapping):
                raise AlistOfflineReadinessError("aria2 未返回全局选项")
            aria2_options = dict(options)
            configured_dir = aria2_options.get("dir")
            if configured_dir != ARIA2_RPC_EXPECTED_DIR:
                raise AlistOfflineReadinessError("aria2 临时目录未与 AList 转存路径对齐")
            checks["aria2"] = _result(
                "ready",
                detail={
                    "version": _safe_text(version_text, limit=64),
                    "temp_dir": ARIA2_RPC_EXPECTED_DIR,
                },
            )
        except Exception as exc:
            checks["aria2"] = _result(
                "not_ready",
                detail={"reason": _safe_text(exc, secrets=(secret,))},
            )
            issues.append("aria2 RPC 或临时目录未就绪")

    if staging_root is not None and storages is not None and alist is not None:
        try:
            # The AList AddURL destination is the materializer's offline
            # sibling, not the broad media root and not even the final
            # staging root.  Choose its longest matching mount so a nested
            # disabled/read-only storage can never be hidden by a healthy
            # parent mount.  List the parent only: that proves the no-write
            # preflight can read the destination namespace without creating a
            # task-owned sibling just to test it.
            offline_sibling = _offline_sibling_for_staging_root(staging_root)
            storage = _storage_for_path(
                storages,
                offline_sibling,
                path_label="AList 离线 AddURL destination",
            )
            capability = _verified_upload_capability(storage)
            staging_parent = posixpath.dirname(offline_sibling)
            parent_rows = _call_list(alist, staging_parent)
            sibling_name = posixpath.basename(offline_sibling)
            sibling_rows = [
                row for row in parent_rows
                if row.get("name") == sibling_name
            ]
            sibling_exists = bool(sibling_rows)
            sibling_readable = False
            if sibling_rows:
                if len(sibling_rows) != 1 or sibling_rows[0].get("is_dir") is not True:
                    raise AlistOfflineReadinessError(
                        "AList 离线 AddURL destination sibling 不是唯一目录"
                    )
                _call_list(alist, offline_sibling)
                sibling_readable = True
            checks["transfer"] = _result(
                "ready",
                detail={
                    "media_root": media_root,
                    "staging_root": staging_root,
                    "offline_sibling": offline_sibling,
                    "staging_parent": staging_parent,
                    "staging_parent_readable": True,
                    "offline_sibling_exists": sibling_exists,
                    "offline_sibling_readable": sibling_readable,
                    "storage_mount": storage.get("mount_path"),
                    "storage_driver": str(storage.get("driver")),
                    **capability,
                    # This means topology/readability has been verified.  The
                    # explicit limitation below records that no bytes were
                    # transferred by this no-write check.
                    "route_verified": True,
                    "end_to_end_transfer_proven": False,
                },
            )
        except Exception as exc:
            checks["transfer"] = _result("not_ready", detail={"reason": _safe_text(exc)})
            issues.append("AList 转存 staging storage 或其父目录不可读")
    elif "transfer" not in checks:
        checks["transfer"] = _result("not_ready", detail={"reason": "storage 认证未通过"})

    verified = configured and not issues and all(
        row.get("verified") is True for row in checks.values()
    )
    return {
        "status": "ready" if verified else "not_ready" if configured else "unverified",
        "verified": verified,
        "configured": configured,
        "read_only": True,
        "checked_at": _now(),
        "checks": checks,
        "issues": issues,
        "limitations": [
            "预检不会创建 AList 目录、提交离线任务、下载文件、转存或删除任务。",
            "通过表示工具、认证、共享 Compose aria2 DNS/RPC、临时目录与实际 staging storage 路由已验证；不等同于已执行真实文件下载或转存。",
            "RPC 探针从 API 容器发出；它借由 AList 配置的同一 Compose DNS 证明共同拓扑，不能替代一次真实 AList 提交/转存验收。",
        ],
    }


def unverified_alist_offline_readiness(reason: str = "尚未执行只读预检") -> dict[str, object]:
    """Return the startup-safe cached health shape without any network I/O."""
    return {
        "status": "unverified",
        "verified": False,
        "configured": False,
        "read_only": True,
        "checked_at": None,
        "checks": {},
        "issues": [reason],
        "limitations": [
            "尚未执行只读预检；该状态不能作为恢复自动运行的依据。",
        ],
    }


__all__ = [
    "ALIST_OFFLINE_TOOL_NAME",
    "ARIA2_RPC_EXPECTED_DIR",
    "ARIA2_RPC_EXPECTED_PORT",
    "AlistOfflineReadinessError",
    "alist_offline_readiness",
    "unverified_alist_offline_readiness",
    "validate_aria2_rpc_url",
]
