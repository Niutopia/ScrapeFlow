"""Automatic Torrent materialization in a task-owned staging directory."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable

import engine.tools._replenishment_local_adapter_impl as _impl


def _require_executable_torrent_bundle(selection_wrapper: Mapping[str, Any]) -> None:
    """Reject historical/forged provider rows before the local adapter runs."""
    bundle = selection_wrapper.get("selection")
    selections = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(selections, list) or not selections:
        raise ValueError("补源选择缺少 selections")
    for row in selections:
        if not isinstance(row, Mapping):
            raise ValueError("补源选择项格式无效")
        acquisition = row.get("acquisition")
        if (
            str(row.get("provider") or "").strip().casefold() != "magnet"
            or not isinstance(acquisition, Mapping)
            or str(acquisition.get("kind") or "").strip().casefold() != "torrent"
        ):
            raise ValueError("本地 materializer 只接受 magnet/torrent")


class LocalTorrentMaterializer:
    """Injectable boundary for candidate checks and task-scoped acquisition."""

    def preflight(
        self,
        selection_wrapper: Mapping[str, Any],
        workspace: Path,
        *,
        resume_workspace: Path | None = None,
    ) -> dict[str, Any]:
        _require_executable_torrent_bundle(selection_wrapper)
        return dict(_impl._preflight_dispatch(
            selection_wrapper, workspace, resume_workspace=resume_workspace,
        ))

    def acquire(
        self,
        selection_wrapper: Mapping[str, Any],
        workspace: Path,
        *,
        automatic: bool = False,
        client: Any | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        # There is one materialization path; the argument is kept until all
        # callers use the final service signature.
        _require_executable_torrent_bundle(selection_wrapper)
        del automatic
        delivery = _impl._acquire_dispatch(
            selection_wrapper,
            workspace,
            automatic=True,
            client=client,
            pause_requested=pause_requested,
        )
        if not isinstance(delivery, Mapping):
            raise TypeError("补源 materializer 返回结果必须是对象")
        output = dict(delivery)
        output.setdefault("delivery_kind", "torrent_delivery")
        return output

    def inspect_remote(
        self,
        client: Any,
        remote_root: str,
        uploaded: list[dict[str, Any]],
    ) -> None:
        _impl._verify_remote_uploads(client, remote_root, uploaded)


__all__ = ["LocalTorrentMaterializer"]
