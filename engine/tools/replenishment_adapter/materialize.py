"""Automatic Torrent materialization in a task-owned staging directory."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import engine.tools._replenishment_local_adapter_impl as _impl


class LocalTorrentMaterializer:
    """Injectable boundary for preflight and isolated acquisition."""

    def preflight(
        self,
        selection_wrapper: Mapping[str, Any],
        workspace: Path,
        *,
        resume_workspace: Path | None = None,
    ) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
        # There is one materialization path; the argument is kept until all
        # callers use the final service signature.
        del automatic
        delivery = _impl._acquire_dispatch(
            selection_wrapper,
            workspace,
            automatic=True,
            client=client,
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


_DEFAULT = LocalTorrentMaterializer()


def preflight(
    selection_wrapper: Mapping[str, Any],
    workspace: Path,
    *,
    resume_workspace: Path | None = None,
) -> dict[str, Any]:
    return _DEFAULT.preflight(
        selection_wrapper, workspace, resume_workspace=resume_workspace,
    )


def acquire(
    selection_wrapper: Mapping[str, Any], workspace: Path,
) -> dict[str, Any]:
    return _DEFAULT.acquire(selection_wrapper, workspace, automatic=True)


__all__ = ["LocalTorrentMaterializer", "acquire", "preflight"]
