"""Automatic, task-owned provider acquisition and replenishment."""

from __future__ import annotations

import json
import posixpath
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.replenishment_matching import (
    expanded_episode_ids as _expanded_episode_ids,
    normalized_text as _normalized_text,
    season_markers as _season_markers,
)

from .replenishment import (
    build_replenishment_requests,
    enrich_replenishment_plan_aliases,
    select_replenishment_candidates,
)
from .simple_engine_runner import EngineJob, SimpleEngineRunner


_VIDEO_EXTENSIONS = frozenset({
    ".3gp", ".asf", ".avi", ".flv", ".m2ts", ".m4v", ".mkv", ".mov",
    ".mp4", ".mpeg", ".mpg", ".mts", ".rm", ".rmvb", ".ts", ".webm", ".wmv",
})
_SUBTITLE_EXTENSIONS = frozenset({
    ".ass", ".idx", ".srt", ".ssa", ".sub", ".sup", ".vtt",
})
_GAP_SLUG = re.compile(r"[^a-zA-Z0-9._-]+")
_EPISODE_TOKEN = re.compile(r"(?<![A-Z0-9])S0*(\d{1,3})[ ._-]*E0*(\d{1,4})(?!\d)", re.I)
_SEASON_TOKEN = re.compile(r"(?<![A-Z0-9])S0*(\d{1,3})(?!\d)", re.I)
_INTERRUPTED_GAP_PHASES = frozenset({
    "provider_searching", "acquiring", "staging_verifying",
    "subtitle_installing", "child_planning", "child_executing",
    "final_verifying", "cleaning", "child_failed",
})
_DURABLE_CANDIDATE_EXCLUSION_LIMIT = 24
_DURABLE_CANDIDATE_LOCATOR_LIMIT = 4096
_DURABLE_CANDIDATE_PROVIDER_LIMIT = 64
_DURABLE_CANDIDATE_RELEASE_NAME_LIMIT = 512
_BTIH_TOKEN = re.compile(r"(?i)\bbtih:([0-9a-f]{40}|[a-z2-7]{32})\b")
_INFOHASH_TOKEN = re.compile(r"(?i)^(?:[0-9a-f]{40}|[a-z2-7]{32})$")

# A provider candidate may optionally deliver a subtitle next to a *new*
# media member.  This is deliberately a separate acquisition contract from
# ``missing_subtitle``: the latter points at an already-existing formal video,
# while this map points at one selected media-gap coordinate and one manifest
# subtitle index.  Keeping the field names narrow makes it impossible for a
# bare subtitle member to silently become a sidecar claim.
_COMPANION_INDEX_MAP_KEY = "companion_subtitle_index_by_media_gap"
_COMPANION_GAP_KEYS = (
    "companion_for_gap_ids",
    "companion_for_gap_id",
    "paired_gap_id",
)
_COMPANION_LANGUAGE_MARKER_RE = re.compile(
    r"(?i)(?<![a-z0-9])(?:zh|zho|chi|chs|cht|中文|简中|簡中|简体|繁体|繁體|"
    r"chinese)(?![a-z0-9])"
)
_COMPANION_LANGUAGE_VALUE_RE = re.compile(
    r"(?i)^(?:zh|zho|chi|chs|cht|中文|简中|簡中|简体|繁体|繁體|chinese)$"
)


class AutomaticReplenishmentError(RuntimeError):
    """An automatic replenishment attempt could not be completed."""


class AutomaticReplenishmentCancelled(AutomaticReplenishmentError):
    """A cooperative control boundary stopped an in-flight provider run."""


class AutomaticProviderSearch(Protocol):
    def run(self, request: Mapping[str, object]) -> Mapping[str, object]: ...


class AutomaticMaterializer(Protocol):
    def acquire(
        self,
        request: Mapping[str, object],
        selections: Sequence[Mapping[str, object]],
        *,
        staging_root: str,
        workspace: Path,
        alist: object,
    ) -> Mapping[str, object]: ...


class LocalTorrentAutomaticMaterializer:
    """Use the bundled Torrent downloader with a task-owned staging root."""

    def __init__(self, delegate: object | None = None) -> None:
        if delegate is None:
            from engine.tools.replenishment_adapter.materialize import LocalTorrentMaterializer
            delegate = LocalTorrentMaterializer()
        self.delegate = delegate

    def acquire(
        self,
        request: Mapping[str, object],
        selections: Sequence[Mapping[str, object]],
        *,
        staging_root: str,
        workspace: Path,
        alist: object,
    ) -> Mapping[str, object]:
        method = getattr(self.delegate, "acquire", None)
        if not callable(method):
            raise AutomaticReplenishmentError("Torrent materializer 不支持 acquire")
        wrapper = {
            "request": dict(request),
            "selection": {"selections": [dict(row) for row in selections]},
            "automatic_staging_root": staging_root,
            # The coordinator, rather than an HTTP caller, derives this
            # parent from its configured library root.  Passing it explicitly
            # keeps a non-default `/quark/...` mount usable without reviving
            # a user-selectable staging path.
            "automatic_staging_parent": posixpath.dirname(posixpath.dirname(staging_root)),
        }
        result = method(wrapper, workspace, automatic=True, client=alist)
        if not isinstance(result, Mapping):
            raise AutomaticReplenishmentError("Torrent materializer 返回无效")
        return dict(result)


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _safe_path(value: object, *, label: str, allow_root: bool = False) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "\x00" in value or "\\" in value:
        raise AutomaticReplenishmentError(f"{label} 不是安全的绝对路径")
    normalized = posixpath.normpath(value)
    if normalized != value or (not allow_root and normalized == "/"):
        raise AutomaticReplenishmentError(f"{label} 不是规范化路径")
    if any(part in {"", ".", ".."} for part in normalized.split("/")[1:]):
        raise AutomaticReplenishmentError(f"{label} 含有不安全路径段")
    return normalized


def _safe_name(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or value in {".", ".."}:
        raise AutomaticReplenishmentError(f"{label} 文件名无效")
    if "/" in value or "\\" in value or "\x00" in value:
        raise AutomaticReplenishmentError(f"{label} 文件名不安全")
    return value


def _gap_file_name(gap_id: str) -> str:
    slug = _GAP_SLUG.sub("-", gap_id).strip(".-")[:96]
    # Gap state is durable task state, not an event log.  A stable name lets a
    # retry update the same record instead of creating one file every 30
    # seconds and eventually making the state directory look like a second
    # provider queue.
    return f"{slug or 'gap'}.json"


def reconcile_interrupted_gap_states(
    state_root: str | Path,
    job_id: str,
    *,
    error: str,
) -> int:
    """Turn only orphaned in-flight local gap records into ``retry_wait``.

    This is deliberately local-state-only.  A forced API recreation has no
    live Python future to own a persisted ``acquiring`` record; reporting it
    as still active would be misleading, while touching AList to clean it up
    would violate a global pause.
    """
    safe_job = _GAP_SLUG.sub("-", job_id).strip(".-")[:96] or "job"
    directory = Path(state_root).resolve() / "gaps" / safe_job
    try:
        paths = sorted(path for path in directory.glob("*.json") if path.is_file())
    except OSError:
        return 0
    updated = 0
    for path in paths:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(raw, Mapping) or str(raw.get("phase") or "") not in _INTERRUPTED_GAP_PHASES:
            continue
        state = dict(raw)
        state.update({"phase": "retry_wait", "updated_at": _now(), "error": error})
        try:
            atomic_write_json(path, state, allow_nan=False)
        except OSError:
            continue
        updated += 1
    return updated


@dataclass(frozen=True, slots=True)
class StagingFile:
    path: str
    size: int
    kind: str

    def as_dict(self) -> dict[str, object]:
        return {"path": self.path, "size": self.size, "kind": self.kind}


class AutomaticReplenishmentRuntime:
    """Run one provider attempt end-to-end for Engine resource gaps."""

    def __init__(
        self,
        state_root: str | Path,
        *,
        engine_runner: SimpleEngineRunner,
        alist: object,
        search: AutomaticProviderSearch,
        materializer: AutomaticMaterializer,
        staging_root: str = "/quark/影视/ScrapeFlow/补源",
        max_candidate_rounds: int = 3,
        progress: Callable[[EngineJob, str, Mapping[str, object]], None] | None = None,
        cancel_requested: Callable[[EngineJob], bool] | None = None,
    ) -> None:
        self.state_root = Path(state_root).resolve()
        self.engine_runner = engine_runner
        self.alist = alist
        self.search = search
        self.materializer = materializer
        self.staging_root = _safe_path(staging_root, label="staging_root")
        self.max_candidate_rounds = max(1, min(12, int(max_candidate_rounds)))
        self.progress = progress
        # This intentionally remains a cooperative boundary.  It cannot
        # safely interrupt an already-running downloader, but it prevents a
        # stopped pilot from starting another provider round or formal write.
        self.cancel_requested = cancel_requested
        self.gaps_root = self.state_root / "gaps"
        self.workspace_root = self.state_root / "staging"
        for root in (self.gaps_root, self.workspace_root):
            root.mkdir(mode=0o700, parents=True, exist_ok=True)

    def _progress(self, job: EngineJob, phase: str, **details: object) -> None:
        """Publish best-effort progress for the root job."""
        callback = self.progress
        if callback is None:
            return
        try:
            callback(job, phase, dict(details))
        except Exception:
            return

    def _raise_if_cancelled(
        self,
        job: EngineJob,
        *,
        round_number: int,
        boundary: str,
    ) -> None:
        """Stop before a new external/provider write boundary when requested.

        The application supplies the predicate from its persisted pause
        control and current pilot allowlist.  A predicate failure is also
        fail-closed: continuing from a malformed/unknown control state could
        otherwise widen a deliberately bounded provider run.
        """
        predicate = self.cancel_requested
        if predicate is None:
            return
        try:
            requested = bool(predicate(job))
        except Exception as exc:
            message = "自动补源取消条件不可用，已停止当前尝试"
            self._progress(
                job, "retry_wait", round=round_number, error=message,
                cancellation_boundary=boundary,
            )
            raise AutomaticReplenishmentCancelled(message) from exc
        if requested:
            message = "自动补源已暂停或不在当前试点范围"
            self._progress(
                job, "retry_wait", round=round_number, error=message,
                cancellation_boundary=boundary,
            )
            raise AutomaticReplenishmentCancelled(message)

    def _plan_internal_child(
        self,
        request: Mapping[str, object],
        *,
        root_job_id: str,
    ) -> EngineJob:
        """Persist a provider attempt as an internal child of the root job."""
        planner = getattr(self.engine_runner, "plan_job", None)
        if not callable(planner):
            raise AutomaticReplenishmentError("Engine runner 不支持 child plan")
        child = planner(request, internal_child_of=root_job_id)
        if not isinstance(child, EngineJob):
            raise AutomaticReplenishmentError("Engine child plan 返回无效")
        return child

    @staticmethod
    def _audit_child_target_matches(root: EngineJob, child: EngineJob) -> bool:
        """Fail closed if an audit-owned child would write another work root."""
        root_summary = root.summary if isinstance(root.summary, Mapping) else {}
        if root_summary.get("audit_owned") is not True:
            return True
        root_plan = root.plan if isinstance(root.plan, Mapping) else {}
        root_metadata = root_plan.get("metadata") if isinstance(root_plan.get("metadata"), Mapping) else {}
        expected = (
            root_metadata.get("series_root")
            or root_metadata.get("target_root")
            or root_plan.get("target_root")
            or root_summary.get("target_root")
        )
        child_plan = child.plan if isinstance(child.plan, Mapping) else {}
        child_metadata = child_plan.get("metadata") if isinstance(child_plan.get("metadata"), Mapping) else {}
        actual = (
            child_metadata.get("series_root")
            or child_metadata.get("target_root")
            or child_plan.get("target_root")
        )
        return isinstance(expected, str) and isinstance(actual, str) and expected == actual

    def _write_gap(self, state: Mapping[str, object], existing_path: Path | None = None) -> Path:
        path = existing_path
        if path is None:
            gap_id = str(state.get("id") or "gap")
            job_id = str(state.get("job_id") or "job")
            path = self.gaps_root / _GAP_SLUG.sub("-", job_id).strip(".-") / _gap_file_name(gap_id)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        atomic_write_json(path, dict(state), allow_nan=False)
        return path

    def _gap_path(self, *, job_id: str, gap_id: str) -> Path:
        """Return the one durable state file for a job/gap pair."""
        safe_job = _GAP_SLUG.sub("-", job_id).strip(".-")[:96] or "job"
        return self.gaps_root / safe_job / _gap_file_name(gap_id)

    @staticmethod
    def _request_gaps(request: Mapping[str, object]) -> list[dict[str, object]]:
        rows = request.get("gaps")
        if not isinstance(rows, list):
            return []
        return [dict(row) for row in rows if isinstance(row, Mapping)]

    @classmethod
    def _delivered_subtitle_gap_ids(
        cls,
        acquisition: Mapping[str, object],
        request: Mapping[str, object],
    ) -> set[str]:
        """Return only subtitle gaps with one explicitly delivered member.

        This is intentionally derived from the materializer's durable
        ``files`` map rather than from a provider candidate.  A manifest can
        claim a subtitle while delivery can still omit it.
        """
        rows = acquisition.get("files")
        if not isinstance(rows, list):
            raise AutomaticReplenishmentError("字幕获取结果缺少 files 映射")
        kinds = {
            str(row.get("id")): str(row.get("kind") or "")
            for row in cls._request_gaps(request)
            if isinstance(row.get("id"), str) and row.get("id")
        }
        delivered: set[str] = set()
        for raw in rows:
            if not isinstance(raw, Mapping):
                raise AutomaticReplenishmentError("字幕获取 files 项无效")
            # A mixed media candidate may carry a new-video companion.  It is
            # intentionally not an audited ``missing_subtitle`` member and
            # therefore has no ordinary gap_ids binding.
            if cls._companion_row_gap_ids(raw):
                continue
            gap_ids = raw.get("gap_ids")
            if not isinstance(gap_ids, list) or len(gap_ids) != 1:
                raise AutomaticReplenishmentError("字幕获取结果未绑定唯一 gap")
            gap_id = gap_ids[0]
            if not isinstance(gap_id, str) or not gap_id or gap_id not in kinds:
                raise AutomaticReplenishmentError("字幕获取结果绑定了未知 gap")
            if kinds[gap_id] == "missing_subtitle":
                delivered.add(gap_id)
        return delivered

    @staticmethod
    def _media_child_staging_root(
        acquisition: Mapping[str, object],
        staging_root: str,
        staging_files: Sequence[StagingFile],
    ) -> str:
        """Return the isolated media subroot for a mixed provider attempt.

        Subtitle companions for existing formal videos must never be passed to
        the Engine child planner.  A local materializer that delivers both
        kinds therefore declares ``media_staging_root``; all videos must be
        inside it and no subtitle may be inside it.  Older/simple test
        materializers remain usable for video-only attempts, but a mixed flat
        staging layout is rejected rather than guessed.
        """
        root = _safe_path(staging_root, label="staging path")
        videos = [item.path for item in staging_files if item.kind == "video"]
        subtitles = [item.path for item in staging_files if item.kind == "subtitle"]
        if not videos:
            raise AutomaticReplenishmentError("媒体 child staging 没有视频文件")
        declared = acquisition.get("media_staging_root")
        if not subtitles and declared is None:
            return root
        if not isinstance(declared, str):
            raise AutomaticReplenishmentError("混合补源缺少隔离的媒体 staging 根")
        media_root = _safe_path(declared, label="media staging path")
        if media_root == root or not media_root.startswith(root + "/"):
            raise AutomaticReplenishmentError("媒体 staging 根超出当前补源任务")
        prefix = media_root + "/"
        if any(not path.startswith(prefix) for path in videos):
            raise AutomaticReplenishmentError("媒体 staging 根未覆盖所有视频文件")
        if any(path.startswith(prefix) for path in subtitles):
            raise AutomaticReplenishmentError("字幕不得进入媒体 child staging 根")
        return media_root

    @staticmethod
    def _subtitle_staging_root(
        acquisition: Mapping[str, object],
        staging_root: str,
        staging_files: Sequence[StagingFile],
    ) -> str:
        """Return the exact task-owned root containing subtitle payloads.

        A mixed candidate is delivered as two sibling subroots.  The subtitle
        installer must consume the declared ``subtitles`` root, never the
        parent that also contains media.  For a pure subtitle attempt we keep
        accepting the historical flat root (there is no media child planner),
        while still validating any explicit root supplied by a materializer.
        """
        root = _safe_path(staging_root, label="staging path")
        subtitles = [item.path for item in staging_files if item.kind == "subtitle"]
        videos = [item.path for item in staging_files if item.kind == "video"]
        if not subtitles:
            raise AutomaticReplenishmentError("字幕 staging 没有字幕文件")

        declared = acquisition.get("subtitle_staging_root")
        if declared is None:
            # A pure subtitle materializer may legitimately use the attempt
            # root directly.  A mixed attempt without an explicit isolated
            # root is unsafe and is rejected rather than guessed.
            if videos:
                raise AutomaticReplenishmentError("混合补源缺少隔离的字幕 staging 根")
            return root
        if not isinstance(declared, str):
            raise AutomaticReplenishmentError("字幕 staging 根无效")
        subtitle_root = _safe_path(declared, label="subtitle staging path")
        if subtitle_root == root:
            if videos:
                raise AutomaticReplenishmentError("混合补源字幕 staging 根未隔离")
            return root
        if not subtitle_root.startswith(root + "/"):
            raise AutomaticReplenishmentError("字幕 staging 根超出当前补源任务")
        prefix = subtitle_root + "/"
        if any(not path.startswith(prefix) for path in subtitles):
            raise AutomaticReplenishmentError("字幕 staging 根未覆盖所有字幕文件")
        if any(path.startswith(prefix) for path in videos):
            raise AutomaticReplenishmentError("视频不得进入字幕 staging 根")
        return subtitle_root

    @staticmethod
    def _companion_manifest_index(row: Mapping[str, object]) -> int | None:
        """Read the optional manifest index carried through materialization.

        The local Torrent materializer keeps this small piece of provenance on
        each delivered row.  Older test/materializer doubles may omit it, so
        callers can still use the explicit ``companion_for_gap_ids`` marker;
        an unmarked subtitle is never guessed from directory order.
        """
        for key in ("manifest_index", "file_index", "index"):
            value = row.get(key)
            if type(value) is int and value > 0:
                return value
        return None

    @classmethod
    def _companion_row_gap_ids(cls, row: Mapping[str, object]) -> set[str]:
        """Return explicitly declared media-gap coordinates for one row."""
        result: set[str] = set()
        for key in _COMPANION_GAP_KEYS:
            value = row.get(key)
            if isinstance(value, str) and value:
                result.add(value)
            elif isinstance(value, list):
                result.update(
                    item for item in value
                    if isinstance(item, str) and item
                )
        return result

    @staticmethod
    def _companion_manifest_path(
        acquisition: Mapping[str, object], index: int,
    ) -> str | None:
        path_map = acquisition.get("file_path_by_index")
        if not isinstance(path_map, Mapping):
            return None
        value = path_map.get(str(index), path_map.get(index))
        if not isinstance(value, str) or not value or value.startswith("/"):
            return None
        # A provider path is a manifest identity witness, not a path to be
        # opened locally.  Reject traversal and platform separators before it
        # participates in basename/episode pairing.
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or any(
            part in {"", ".", ".."} for part in normalized.split("/")
        ):
            return None
        return normalized

    @staticmethod
    def _companion_manifest_size(
        acquisition: Mapping[str, object], index: int,
    ) -> int | None:
        size_map = acquisition.get("file_size_by_index")
        if not isinstance(size_map, Mapping):
            return None
        value = size_map.get(str(index), size_map.get(index))
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            return None
        return value

    @staticmethod
    def _companion_core(path: str) -> str:
        """Normalize a provider stem after removing only a language suffix."""
        stem = Path(path).stem
        # Keep this suffix list intentionally finite.  Removing arbitrary
        # release/quality tokens would make a cross-work or cross-edition
        # subtitle look like an exact companion.
        stem = re.sub(
            r"(?i)(?:[ ._\-\[\]()]+(?:zh|zho|chi|chs|cht|中文|简中|簡中|"
            r"简体|繁体|繁體|chinese))+$",
            "",
            stem,
        )
        return _normalized_text(stem)

    @staticmethod
    def _companion_required_language(value: object) -> bool:
        text = str(value or "").strip().casefold()
        return any(
            token in text
            for token in ("zh", "中文", "简", "繁", "chinese", "chs", "cht")
        )

    @classmethod
    def _companion_path_has_language(
        cls, path: str, requested: object, declared: object = None,
    ) -> bool:
        """Require an explicit CHS/Chinese marker in the provider member."""
        if not cls._companion_required_language(requested):
            # The companion lane is deliberately only a Chinese lane.  Other
            # languages remain eligible for the ordinary subtitle audit path.
            return False
        if declared is not None and not cls._companion_required_language(declared):
            return False
        return _COMPANION_LANGUAGE_MARKER_RE.search(path) is not None

    @staticmethod
    def _companion_gap_episode_ids(gap: Mapping[str, object]) -> set[str]:
        values: set[str] = set()
        for key in ("label", "title", "id"):
            value = gap.get(key)
            if isinstance(value, str):
                values.update(_expanded_episode_ids(value))
        season = gap.get("season")
        episodes = gap.get("episodes")
        if type(season) is int and isinstance(episodes, list):
            values.update(
                f"S{season:02d}E{episode:02d}"
                for episode in episodes
                if type(episode) is int and episode > 0
            )
        return values

    @classmethod
    def _companion_pair_is_exact(
        cls,
        *,
        request: Mapping[str, object],
        gap: Mapping[str, object],
        video_path: str,
        subtitle_path: str,
        subtitle_language: object,
        declared_language: object = None,
    ) -> bool:
        """Prove one manifest subtitle belongs to one selected new video.

        This gate intentionally does not use the broader subtitle selector's
        bare-ordinal fallback.  A companion has to carry the same single
        episode coordinate as the selected video, the same season evidence,
        an explicit Chinese marker, and a work identity (or an exactly equal
        stem after the language suffix is removed).
        """
        if not cls._companion_path_has_language(
            subtitle_path, subtitle_language, declared_language,
        ):
            return False
        video_ids = _expanded_episode_ids(video_path)
        subtitle_ids = _expanded_episode_ids(subtitle_path)
        if video_ids or subtitle_ids:
            # Ranges/packs and bare ordinal sidecars are never companions.
            if len(video_ids) != 1 or subtitle_ids != video_ids:
                return False
            expected_ids = cls._companion_gap_episode_ids(gap)
            if expected_ids and video_ids != expected_ids:
                return False
        else:
            # Movie/opaque media still require an exact same-stem pairing.  A
            # language-only ``E05.chs.ass`` cannot be laundered as a movie
            # companion.
            video_core = cls._companion_core(video_path)
            subtitle_core = cls._companion_core(subtitle_path)
            if not video_core or video_core != subtitle_core:
                return False

        video_seasons = _season_markers(video_path)
        subtitle_seasons = _season_markers(subtitle_path)
        if video_seasons and subtitle_seasons and video_seasons != subtitle_seasons:
            return False
        expected_season = gap.get("season")
        if type(expected_season) is int and expected_season > 0:
            if any(season != expected_season for season in video_seasons | subtitle_seasons):
                return False

        video_core = cls._companion_core(video_path)
        subtitle_core = cls._companion_core(subtitle_path)
        if video_core == subtitle_core:
            return True

        # Different release tags are acceptable only when the same explicit
        # work alias appears in both manifest members.  This keeps a subtitle
        # from another franchise (even with the same SxxEyy) out of the lane.
        media = request.get("media")
        if not isinstance(media, Mapping):
            return False
        raw_aliases = media.get("aliases")
        aliases = raw_aliases if isinstance(raw_aliases, list) else [media.get("title")]
        video_key = _normalized_text(video_path)
        subtitle_key = _normalized_text(subtitle_path)
        for alias in aliases:
            alias_key = _normalized_text(alias)
            han_count = sum("\u3400" <= char <= "\u9fff" for char in alias_key)
            if (
                alias_key and (len(alias_key) >= 4 or han_count >= 2)
                and alias_key in video_key and alias_key in subtitle_key
            ):
                return True
        return False

    @classmethod
    def _validated_companion_subtitles(
        cls,
        *,
        request: Mapping[str, object],
        acquisition: Mapping[str, object],
        staging_files: Sequence[StagingFile],
        selected_gap_ids: set[str],
    ) -> list[dict[str, object]]:
        """Return only defensible media/subtitle companion pairs.

        Invalid or ambiguous companion evidence is ignored rather than made a
        media failure: the newly selected video can still complete normally,
        and the next library audit will create the ordinary subtitle-only gap.
        """
        raw_map = acquisition.get(_COMPANION_INDEX_MAP_KEY)
        if not isinstance(raw_map, Mapping):
            return []
        media_gaps = {
            str(gap.get("id")): dict(gap)
            for gap in cls._request_gaps(request)
            if str(gap.get("kind") or "") != "missing_subtitle"
            and isinstance(gap.get("id"), str) and gap.get("id")
            and str(gap.get("id")) in selected_gap_ids
        }
        if not media_gaps:
            return []
        media_map = acquisition.get("file_index_by_gap")
        rows = acquisition.get("files")
        if not isinstance(media_map, Mapping) or not isinstance(rows, list):
            return []
        staging_by_path = {
            item.path: item for item in staging_files
        }
        output: list[dict[str, object]] = []
        used_subtitle_indices: set[int] = set()
        for raw_gap_id, raw_indices in raw_map.items():
            gap_id = str(raw_gap_id)
            gap = media_gaps.get(gap_id)
            if gap is None or not isinstance(raw_indices, list) or len(raw_indices) != 1:
                continue
            subtitle_index = raw_indices[0]
            if type(subtitle_index) is not int or subtitle_index <= 0:
                continue
            raw_video_indices = media_map.get(gap_id)
            if not isinstance(raw_video_indices, list) or len(raw_video_indices) != 1:
                continue
            video_index = raw_video_indices[0]
            if type(video_index) is not int or video_index <= 0 or video_index == subtitle_index:
                continue
            if subtitle_index in used_subtitle_indices:
                continue
            subtitle_manifest_path = cls._companion_manifest_path(acquisition, subtitle_index)
            video_manifest_path = cls._companion_manifest_path(acquisition, video_index)
            subtitle_size = cls._companion_manifest_size(acquisition, subtitle_index)
            video_size = cls._companion_manifest_size(acquisition, video_index)
            if (
                subtitle_manifest_path is None or video_manifest_path is None
                or subtitle_size is None or video_size is None
                or Path(subtitle_manifest_path).suffix.casefold() not in _SUBTITLE_EXTENSIONS
                or Path(video_manifest_path).suffix.casefold() not in _VIDEO_EXTENSIONS
            ):
                continue
            subtitle_rows: list[Mapping[str, object]] = []
            video_rows: list[Mapping[str, object]] = []
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                kind = str(row.get("kind") or "").casefold()
                index = cls._companion_manifest_index(row)
                companion_ids = cls._companion_row_gap_ids(row)
                gap_ids = row.get("gap_ids")
                ordinary_ids = {
                    value for value in gap_ids if isinstance(value, str) and value
                } if isinstance(gap_ids, list) else set()
                if kind == "subtitle" and (
                    gap_id in companion_ids or index == subtitle_index
                ):
                    subtitle_rows.append(row)
                if kind == "video" and (
                    gap_id in ordinary_ids or index == video_index
                ):
                    video_rows.append(row)
            if len(subtitle_rows) != 1 or len(video_rows) != 1:
                continue
            subtitle_row = subtitle_rows[0]
            video_row = video_rows[0]
            subtitle_source = subtitle_row.get("path")
            video_source = video_row.get("path")
            subtitle_row_size = subtitle_row.get("size")
            video_row_size = video_row.get("size")
            if (
                not isinstance(subtitle_source, str)
                or not isinstance(video_source, str)
                or not subtitle_source.startswith("/")
                or not video_source.startswith("/")
                or type(subtitle_row_size) is not int
                or type(video_row_size) is not int
                or subtitle_row_size != subtitle_size
                or video_row_size != video_size
                or subtitle_source not in staging_by_path
                or video_source not in staging_by_path
                or staging_by_path[subtitle_source].kind != "subtitle"
                or staging_by_path[video_source].kind != "video"
            ):
                continue
            declared_language = subtitle_row.get("subtitle_language") or subtitle_row.get("language")
            if not cls._companion_pair_is_exact(
                request=request,
                gap=gap,
                video_path=video_manifest_path,
                subtitle_path=subtitle_manifest_path,
                subtitle_language=gap.get("subtitle_language") or "zh",
                declared_language=declared_language,
            ):
                continue
            output.append({
                "gap_id": gap_id,
                "video_index": video_index,
                "subtitle_index": subtitle_index,
                "video_source": video_source,
                "subtitle_source": subtitle_source,
                "video_size": video_size,
                "subtitle_size": subtitle_size,
                "video_manifest_path": video_manifest_path,
                "subtitle_manifest_path": subtitle_manifest_path,
                "subtitle_language": gap.get("subtitle_language") or "zh",
            })
            used_subtitle_indices.add(subtitle_index)
        return output

    @staticmethod
    def _new_child_video_targets(
        child: EngineJob,
    ) -> list[dict[str, object]]:
        """Return only video targets that this child actually moved now."""
        execution = child.execution if isinstance(child.execution, Mapping) else {}
        rows = execution.get("files")
        if not isinstance(rows, list):
            return []
        output: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, Mapping) or str(row.get("status") or "").casefold() != "moved":
                continue
            target = row.get("target") or row.get("path")
            source = row.get("source")
            if (
                not isinstance(target, str) or not target.startswith("/")
                or Path(target).suffix.casefold() not in _VIDEO_EXTENSIONS
            ):
                continue
            output.append({
                "target": target,
                **({"source": source} if isinstance(source, str) else {}),
                "size": row.get("size"),
            })
        return output

    @classmethod
    def _companion_target_for_child(
        cls,
        child: EngineJob,
        spec: Mapping[str, object],
    ) -> str | None:
        """Bind one companion to a newly moved child target, never a stale one."""
        moved = cls._new_child_video_targets(child)
        if not moved:
            return None
        video_source = spec.get("video_source")
        video_ids = _expanded_episode_ids(str(spec.get("video_manifest_path") or ""))
        candidates: list[str] = []
        plan = child.plan if isinstance(child.plan, Mapping) else {}
        plan_files = plan.get("files") if isinstance(plan.get("files"), list) else []
        for row in moved:
            target = row.get("target")
            source = row.get("source")
            if not isinstance(target, str):
                continue
            if isinstance(video_source, str) and isinstance(source, str) and source == video_source:
                candidates.append(target)
                continue
            for planned in plan_files:
                if not isinstance(planned, Mapping):
                    continue
                if str(planned.get("media_kind") or "") != "video":
                    continue
                planned_source = planned.get("source_path")
                planned_target = posixpath.join(
                    str(planned.get("target_dir") or ""),
                    str(planned.get("final_name") or ""),
                )
                if (
                    isinstance(video_source, str)
                    and planned_source == video_source
                    and planned_target == target
                ):
                    candidates.append(target)
                    break
            if target not in candidates and video_ids:
                target_ids = _expanded_episode_ids(target)
                if target_ids == video_ids:
                    candidates.append(target)
        unique = sorted(set(candidates))
        return unique[0] if len(unique) == 1 else None

    def _install_companion_subtitles(
        self,
        *,
        job: EngineJob,
        request: Mapping[str, object],
        specs: Sequence[Mapping[str, object]],
        subtitle_root: str,
        round_number: int,
        child: EngineJob,
    ) -> list[dict[str, object]]:
        """Install only companions whose media child reports a fresh move."""
        installer = getattr(self.engine_runner, "install_subtitle_sidecar", None)
        if not callable(installer):
            return []
        installed: list[dict[str, object]] = []
        for spec in specs:
            source = spec.get("subtitle_source")
            size = spec.get("subtitle_size")
            if (
                not isinstance(source, str)
                or not source.startswith(subtitle_root.rstrip("/") + "/")
                or type(size) is not int or size <= 0
            ):
                continue
            video_target = self._companion_target_for_child(child, spec)
            if video_target is None:
                # The child may have been idempotently recovered or may have
                # written a different episode.  Do not attach a subtitle in
                # either case; the next audit will use the safe sidecar lane.
                continue
            suffix = Path(source).suffix.casefold()
            if suffix not in _SUBTITLE_EXTENSIONS:
                continue
            language = spec.get("subtitle_language") or "zh"
            target = (
                f"{posixpath.splitext(video_target)[0]}."
                f"{self._subtitle_marker(language)}{suffix}"
            )
            self._raise_if_cancelled(
                job, round_number=round_number, boundary="subtitle_write",
            )
            self._progress(
                job, "subtitle_installing", gap_id=str(spec.get("gap_id") or ""),
                target=target, companion=True,
            )
            result = installer(
                source, target, expected_size=size, video_path=video_target,
            )
            if not isinstance(result, Mapping) or int(result.get("size") or 0) != size:
                raise AutomaticReplenishmentError("伴随字幕正式库回读大小不匹配")
            installed.append({
                "gap_id": str(spec.get("gap_id") or ""),
                "source": source,
                "target": target,
                "size": size,
                "companion": True,
                "video_target": video_target,
            })
        return installed

    def _gap_state(
        self,
        gap: Mapping[str, object],
        *,
        job_id: str,
        prior_state: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        gap_id = gap.get("id")
        if not isinstance(gap_id, str) or not gap_id:
            raise AutomaticReplenishmentError("Engine gap 缺少 id")
        state: dict[str, object] = {
            "id": gap_id,
            "job_id": job_id,
            "gap": dict(gap),
            "phase": "provider_searching",
            "attempts": 0,
            "created_at": _now(),
            "updated_at": _now(),
            "error": None,
        }
        # Candidate failures are local, task-owned evidence.  Carry only the
        # bounded, normalized identity list forward when a fresh audit
        # projection recreates this gap state; never copy arbitrary persisted
        # JSON into a provider request.
        prior_exclusions = (
            prior_state.get("excluded_candidates")
            if isinstance(prior_state, Mapping) else None
        )
        exclusions = self._merge_excluded_candidates(prior_exclusions)
        if exclusions:
            state["excluded_candidates"] = exclusions
        return state

    def _fresh_list(self, path: str) -> list[Mapping[str, object]]:
        listing = getattr(self.alist, "list", None)
        if not callable(listing):
            raise AutomaticReplenishmentError("AList 客户端缺少 list")
        login = getattr(self.alist, "login", None)
        if callable(login) and not getattr(self.alist, "token", None):
            login()
        try:
            rows = listing(path, refresh=True)
        except TypeError:
            # Small test doubles and older AList wrappers expose ``list(path)``
            # only.  Losing the refresh keyword must not turn a usable client
            # into a permanent provider failure.
            rows = listing(path)
        if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
            raise AutomaticReplenishmentError(f"AList 目录响应无效: {path}")
        return list(rows)

    def verify_staging(self, staging_root: str) -> list[StagingFile]:
        """Recursively inspect staging for usable media or subtitles.

        The caller decides whether the request is media or subtitle-only. This
        keeps the shared staging verifier strict about ownership, paths, types
        and sizes without forcing a subtitle-only download through a media
        child plan.
        """
        root = _safe_path(staging_root, label="staging path")
        if root == self.staging_root or not root.startswith(self.staging_root + "/"):
            raise AutomaticReplenishmentError("拒绝核对任务 staging 根以外的目录")
        files: list[StagingFile] = []
        seen: set[str] = set()

        def visit(directory: str) -> None:
            for row in self._fresh_list(directory):
                name = _safe_name(row.get("name"), label=f"{directory} entry")
                full = posixpath.join(directory, name)
                if full in seen:
                    raise AutomaticReplenishmentError(f"staging 出现重复条目: {full}")
                seen.add(full)
                if row.get("is_dir") is True:
                    visit(full)
                    continue
                suffix = Path(name).suffix.casefold()
                if suffix in _VIDEO_EXTENSIONS:
                    kind = "video"
                elif suffix in _SUBTITLE_EXTENSIONS:
                    kind = "subtitle"
                else:
                    raise AutomaticReplenishmentError(f"staging 包含不支持文件: {full}")
                raw_size = row.get("size")
                if isinstance(raw_size, bool) or not isinstance(raw_size, int) or raw_size <= 0:
                    raise AutomaticReplenishmentError(f"staging 文件大小无效: {full}")
                files.append(StagingFile(path=full, size=raw_size, kind=kind))

        visit(root)
        if not files:
            raise AutomaticReplenishmentError("staging 没有可回投文件")
        return files

    @staticmethod
    def _subtitle_marker(value: object) -> str:
        text = str(value or "zh").casefold()
        if any(token in text for token in ("zh", "中文", "简中", "簡中", "chinese")):
            return "zh"
        if any(token in text for token in ("en", "英文", "英语", "english")):
            return "en"
        if any(token in text for token in ("ja", "日文", "日语", "japanese")):
            return "ja"
        marker = re.sub(r"[^a-z0-9-]+", "-", text).strip("-")
        return marker[:16] or "sub"

    def _install_subtitle_members(
        self,
        *,
        job: EngineJob,
        request: Mapping[str, object],
        acquisition: Mapping[str, object],
        staging_root: str,
        round_number: int,
        required_gap_ids: set[str] | None = None,
    ) -> list[dict[str, object]]:
        """Pair explicitly delivered subtitle members with audited videos.

        A mixed media request may contain a partial subtitle selection.  The
        caller passes the subset actually present in ``acquisition.files``;
        no candidate metadata alone can mark a subtitle gap resolved.  Pure
        subtitle requests leave this unset and therefore require every
        requested subtitle gap to have one exact member.
        """
        rows = acquisition.get("files")
        if not isinstance(rows, list):
            raise AutomaticReplenishmentError("字幕获取结果缺少 files 映射")
        gaps = {
            str(row.get("id")): dict(row)
            for row in self._request_gaps(request)
            if row.get("kind") == "missing_subtitle"
            and isinstance(row.get("id"), str) and row.get("id")
        }
        requested_ids = {
            str(row.get("id"))
            for row in self._request_gaps(request)
            if isinstance(row.get("id"), str) and row.get("id")
        }
        delivered_ids: set[str] = set()
        for raw in rows:
            if not isinstance(raw, Mapping):
                raise AutomaticReplenishmentError("字幕获取 files 项无效")
            if self._companion_row_gap_ids(raw):
                continue
            gap_ids = raw.get("gap_ids")
            if not isinstance(gap_ids, list) or len(gap_ids) != 1:
                raise AutomaticReplenishmentError("字幕获取结果未绑定唯一 gap")
            gap_id = gap_ids[0]
            if not isinstance(gap_id, str) or not gap_id or gap_id not in requested_ids:
                raise AutomaticReplenishmentError("字幕获取结果绑定了未知 gap")
            if gap_id in gaps:
                delivered_ids.add(gap_id)
        required = set(gaps) if required_gap_ids is None else set(required_gap_ids)
        if not required <= delivered_ids or not required <= set(gaps):
            raise AutomaticReplenishmentError("字幕获取结果未覆盖所需 gap")
        installer = getattr(self.engine_runner, "install_subtitle_sidecar", None)
        if not callable(installer):
            raise AutomaticReplenishmentError("Engine runner 不支持字幕侧挂写入")
        installed: list[dict[str, object]] = []
        installed_ids: set[str] = set()
        for raw in rows:
            if self._companion_row_gap_ids(raw):
                continue
            source, size, gap_ids = raw.get("path"), raw.get("size"), raw.get("gap_ids")
            gap_id = str(gap_ids[0])
            if gap_id not in required:
                # Video members (and subtitle members selected for another
                # pending gap) stay in the media/staging transaction.
                continue
            if gap_id in installed_ids:
                raise AutomaticReplenishmentError("同一字幕 gap 被重复安装")
            if (
                not isinstance(source, str)
                or not source.startswith(staging_root.rstrip("/") + "/")
                or isinstance(size, bool) or not isinstance(size, int) or size <= 0
            ):
                raise AutomaticReplenishmentError("字幕获取结果未绑定有效 staging 文件")
            gap = gaps.get(gap_id)
            video_path = gap.get("path") if isinstance(gap, Mapping) else None
            if not isinstance(video_path, str) or not video_path.startswith("/"):
                raise AutomaticReplenishmentError("字幕 gap 缺少正式视频路径")
            suffix = Path(source).suffix.casefold()
            if suffix not in _SUBTITLE_EXTENSIONS:
                raise AutomaticReplenishmentError("字幕获取结果包含不支持的文件格式")
            self._raise_if_cancelled(
                job, round_number=round_number, boundary="subtitle_write",
            )
            target = f"{posixpath.splitext(video_path)[0]}.{self._subtitle_marker(gap.get('subtitle_language'))}{suffix}"
            self._progress(job, "subtitle_installing", gap_id=str(gap_id), target=target)
            result = installer(source, target, expected_size=size, video_path=video_path)
            if not isinstance(result, Mapping) or int(result.get("size") or 0) != size:
                raise AutomaticReplenishmentError("字幕正式库回读大小不匹配")
            installed_ids.add(gap_id)
            installed.append({"gap_id": str(gap_id), "source": source, "target": target, "size": size})
        if installed_ids != required:
            raise AutomaticReplenishmentError("字幕补源未覆盖所有 gap")
        return installed

    def _remove_staging(self, staging_root: str) -> None:
        """Delete only one verified task-owned staging tree and read it back."""
        root = _safe_path(staging_root, label="staging cleanup path")
        if root == self.staging_root or not root.startswith(self.staging_root + "/"):
            raise AutomaticReplenishmentError("拒绝清理任务 staging 根以外的目录")
        remove = getattr(self.alist, "remove", None)
        remove_empty = getattr(self.alist, "remove_empty_dir", None)
        if not callable(remove):
            raise AutomaticReplenishmentError("AList 客户端缺少 remove")

        def fresh_cleanup_list(path: str) -> list[Mapping[str, object]]:
            """Treat an already-removed task path as a successful cleanup.

            The child engine may remove the attempt (or one of its parents)
            before the coordinator's final readback.  AList reports that
            state as ``object not found``; it is not a network or permission
            failure and must not turn an otherwise failed download into a
            misleading ``staging cleanup failed`` outcome.
            """
            try:
                return self._fresh_list(path)
            except Exception as exc:
                message = str(exc).casefold()
                if "object not found" in message:
                    return []
                raise

        # The child Engine may already have removed the now-empty source root.
        # That is a successful cleanup outcome, not a reason to leave the
        # parent task in retry forever.
        parent = posixpath.dirname(root) or "/"
        name = posixpath.basename(root)
        parent_rows = fresh_cleanup_list(parent)
        if not any(row.get("name") == name and row.get("is_dir") is True for row in parent_rows):
            return

        def remove_verified_empty(parent: str, name: str) -> bool:
            """Remove one exact empty task directory, with a stale-list fallback.

            Some AList storage backends acknowledge ``remove_empty_directory``
            before a forced parent listing stops showing the directory.  The
            normal endpoint remains the first choice.  The generic remove
            fallback is deliberately available only after two fresh checks
            prove that the exact, task-owned directory is still an empty
            directory; it must never turn a stale cleanup observation into a
            recursive delete of a non-empty or shared parent.
            """
            directory = posixpath.join(parent, name)
            rows = fresh_cleanup_list(parent)
            matching = [row for row in rows if row.get("name") == name]
            if not matching:
                return True
            if len(matching) != 1 or matching[0].get("is_dir") is not True:
                return False
            if fresh_cleanup_list(directory):
                return False

            if callable(remove_empty):
                removed = remove_empty(directory)
                # An explicit non-empty result is authoritative.  Do not
                # fall back to a potentially recursive generic remove.
                if removed is False:
                    return False
            else:
                remove(parent, [name])

            rows = fresh_cleanup_list(parent)
            matching = [row for row in rows if row.get("name") == name]
            if not matching:
                return True
            if not callable(remove_empty):
                return False

            # ``remove_empty_dir`` reported success but this fresh parent
            # readback still sees the exact directory.  Re-check that it is
            # empty before using ``remove(parent, [name])`` as a narrowly
            # scoped fallback for this backend inconsistency.
            if (
                len(matching) != 1
                or matching[0].get("is_dir") is not True
                or fresh_cleanup_list(directory)
            ):
                return False
            remove(parent, [name])
            rows = fresh_cleanup_list(parent)
            return not any(row.get("name") == name for row in rows)

        def visit(directory: str) -> None:
            rows = fresh_cleanup_list(directory)
            files: list[str] = []
            directories: list[str] = []
            for row in rows:
                name = _safe_name(row.get("name"), label=f"{directory} cleanup entry")
                if row.get("is_dir") is True:
                    directories.append(name)
                else:
                    files.append(name)
            if files:
                remove(directory, files)
            for name in directories:
                child = posixpath.join(directory, name)
                visit(child)
                remove_verified_empty(directory, name)

        visit(root)
        current = root
        while current != self.staging_root:
            current_parent = posixpath.dirname(current) or "/"
            current_name = posixpath.basename(current)
            if not remove_verified_empty(current_parent, current_name):
                # A shared parent may contain another attempt.  Stop rather
                # than deleting something this task did not create.
                if current == root:
                    raise AutomaticReplenishmentError(f"staging 清理后仍可见: {root}")
                break
            current = current_parent

    @staticmethod
    def _normalized_excluded_selection(
        selection: Mapping[str, object],
    ) -> dict[str, object] | None:
        """Return one bounded, provider-neutral candidate identity.

        Gap JSON survives API recreation, so it must never be replayed as
        arbitrary provider input.  The selector only needs a provider plus a
        locator and/or a torrent infohash; keep that small evidence set and
        discard malformed or unbounded historic rows.
        """
        raw_provider = selection.get("provider")
        if not isinstance(raw_provider, str):
            return None
        provider = raw_provider.strip().casefold()
        if not provider or len(provider) > _DURABLE_CANDIDATE_PROVIDER_LIMIT:
            return None

        raw_locator = selection.get("locator")
        locator = raw_locator.strip() if isinstance(raw_locator, str) else ""
        if len(locator) > _DURABLE_CANDIDATE_LOCATOR_LIMIT:
            return None

        raw_infohash = selection.get("infohash")
        infohash = (
            raw_infohash.strip().casefold()
            if isinstance(raw_infohash, str) else ""
        )
        if not _INFOHASH_TOKEN.fullmatch(infohash):
            infohash = ""
        if not infohash and locator:
            match = _BTIH_TOKEN.search(locator)
            if match:
                infohash = match.group(1).casefold()
        if not locator and not infohash:
            return None

        normalized: dict[str, object] = {"provider": provider}
        if locator:
            normalized["locator"] = locator
        if infohash:
            normalized["infohash"] = infohash
        raw_release_name = selection.get("release_name")
        if isinstance(raw_release_name, str):
            release_name = raw_release_name.strip()
            if release_name and len(release_name) <= _DURABLE_CANDIDATE_RELEASE_NAME_LIMIT:
                normalized["release_name"] = release_name
        return normalized

    @classmethod
    def _excluded_selection_key(cls, selection: Mapping[str, object]) -> tuple[str, ...]:
        provider = str(selection.get("provider") or "").casefold()
        infohash = str(selection.get("infohash") or "").casefold()
        if infohash:
            return ("infohash", provider, infohash)
        return ("locator", provider, str(selection.get("locator") or ""))

    @classmethod
    def _merge_excluded_candidates(
        cls,
        *groups: object,
    ) -> list[dict[str, object]]:
        """Normalize, deduplicate and cap persisted candidate identities."""
        merged: list[dict[str, object]] = []
        seen: set[tuple[str, ...]] = set()
        for rows in groups:
            if not isinstance(rows, (list, tuple)):
                continue
            for raw in rows:
                if not isinstance(raw, Mapping):
                    continue
                normalized = cls._normalized_excluded_selection(raw)
                if normalized is None:
                    continue
                key = cls._excluded_selection_key(normalized)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(normalized)
        # Preserve the newest records when a legacy or damaged state file
        # carries more history than this runtime is willing to replay.
        return merged[-_DURABLE_CANDIDATE_EXCLUSION_LIMIT:]

    @classmethod
    def _same_excluded_candidate(
        cls,
        left: Mapping[str, object],
        right: Mapping[str, object],
    ) -> bool:
        if str(left.get("provider") or "").casefold() != str(
            right.get("provider") or ""
        ).casefold():
            return False
        left_hash = str(left.get("infohash") or "").casefold()
        right_hash = str(right.get("infohash") or "").casefold()
        if left_hash and right_hash and left_hash == right_hash:
            return True
        left_locator = str(left.get("locator") or "")
        right_locator = str(right.get("locator") or "")
        return bool(left_locator and right_locator and left_locator == right_locator)

    @classmethod
    def _failure_candidate_exclusions(
        cls,
        error: Exception,
        selections: Sequence[Mapping[str, object]],
    ) -> list[dict[str, object]]:
        """Return only identities that the materializer blamed on a release.

        Network, AList, capacity and control failures must remain retryable;
        treating them as a bad release would hide a usable source.  The local
        Torrent materializer explicitly marks candidate-local failures with
        ``exclude_candidate=True`` and normally includes the exact selection.
        """
        if getattr(error, "exclude_candidate", False) is not True:
            return []
        normalized_selections = cls._merge_excluded_candidates(list(selections))
        if not normalized_selections:
            return []
        raw_candidate = getattr(error, "candidate", None)
        if isinstance(raw_candidate, Mapping):
            normalized_failure = cls._normalized_excluded_selection(raw_candidate)
            if normalized_failure is not None:
                matching = [
                    selection for selection in normalized_selections
                    if cls._same_excluded_candidate(selection, normalized_failure)
                ]
                if matching:
                    return matching
                # The error did identify a release, but not one in this
                # selected bundle.  Do not trust it as durable state.
                return []
        # Older/materializer-agnostic candidate errors may not carry a row.
        # One selected release is still unambiguous; a multi-selection bundle
        # is not, so leave it retryable rather than poisoning every source.
        return normalized_selections if len(normalized_selections) == 1 else []

    def _load_excluded_candidates(
        self,
        gap_state_paths: Mapping[str, Path],
    ) -> list[dict[str, object]]:
        persisted: list[object] = []
        for path in dict.fromkeys(gap_state_paths.values()):
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(state, Mapping):
                persisted.append(state.get("excluded_candidates"))
        return self._merge_excluded_candidates(*persisted)

    def _persist_excluded_candidates(
        self,
        gap_state_paths: Mapping[str, Path],
        candidates: Sequence[Mapping[str, object]],
    ) -> None:
        """Save explicit candidate failures into each active task-gap record.

        A materializer acquisition is shared by the request's selected gaps:
        a torrent that cannot deliver one selected member cannot deliver the
        same selected release for its sibling gap either.  Persisting the
        narrow provider/locator/hash identity on the active local records
        therefore prevents a process restart from spending another full idle
        timeout on that exact release, while still permitting every other
        identity to be searched and selected.
        """
        additions = self._merge_excluded_candidates(list(candidates))
        if not additions:
            return
        for path in dict.fromkeys(gap_state_paths.values()):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(raw, Mapping):
                continue
            state = dict(raw)
            merged = self._merge_excluded_candidates(
                state.get("excluded_candidates"), additions,
            )
            if not merged:
                continue
            state["excluded_candidates"] = merged
            state["updated_at"] = _now()
            self._write_gap(state, path)

    @classmethod
    def _excluded_selection(
        cls,
        selection: Mapping[str, object],
    ) -> dict[str, object]:
        """Return a selector-compatible identity, or an empty safe row."""
        return cls._normalized_excluded_selection(selection) or {}

    @staticmethod
    def _child_resolved_gap_ids(
        child: EngineJob,
        gaps: Sequence[Mapping[str, object]],
    ) -> set[str]:
        """Return gaps proven by the child execution's actual video targets.

        Provider manifests are only candidate evidence.  A child can complete
        successfully after receiving a partial pack, so a provider-selected
        gap must not become durable ``resolved`` state until the child reports
        a corresponding, executed video target.
        """
        execution = child.execution if isinstance(child.execution, Mapping) else {}
        rows = execution.get("files")
        if not isinstance(rows, list):
            raise AutomaticReplenishmentError("补源 child 缺少已执行文件回读")
        video_targets: list[str] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise AutomaticReplenishmentError("补源 child 文件回读格式无效")
            target = row.get("target") or row.get("path")
            if not isinstance(target, str) or not target.startswith("/"):
                continue
            if Path(target).suffix.casefold() in _VIDEO_EXTENSIONS:
                video_targets.append(target)
        if not video_targets:
            raise AutomaticReplenishmentError("补源 child 没有已执行视频")

        episode_ids: set[str] = set()
        seasons: set[int] = set()
        for target in video_targets:
            for match in _EPISODE_TOKEN.finditer(target):
                season, episode = int(match.group(1)), int(match.group(2))
                if season > 999 or episode <= 0:
                    continue
                episode_ids.add(f"S{season:02d}E{episode:02d}")
                seasons.add(season)
            for match in _SEASON_TOKEN.finditer(target):
                seasons.add(int(match.group(1)))

        resolved: set[str] = set()
        for raw_gap in gaps:
            gap_id = raw_gap.get("id")
            if not isinstance(gap_id, str) or not gap_id:
                continue
            kind = str(raw_gap.get("kind") or "")
            if kind == "missing_media":
                resolved.add(gap_id)
                continue
            if kind == "missing_episode":
                season = raw_gap.get("season")
                episodes = raw_gap.get("episodes")
                if isinstance(season, int) and isinstance(episodes, list) and episodes:
                    expected = {
                        f"S{season:02d}E{int(episode):02d}"
                        for episode in episodes
                        if isinstance(episode, int) and episode > 0
                    }
                    if expected and expected.issubset(episode_ids):
                        resolved.add(gap_id)
                elif gap_id in episode_ids:
                    resolved.add(gap_id)
                continue
            if kind == "missing_season":
                season = raw_gap.get("season")
                if isinstance(season, int) and season in seasons:
                    expected_count = raw_gap.get("expected_episode_count")
                    if (
                        isinstance(expected_count, int)
                        and expected_count > 0
                        and sum(1 for item in episode_ids if item.startswith(f"S{season:02d}E")) < expected_count
                    ):
                        continue
                    resolved.add(gap_id)
        return resolved

    def _run_request(
        self,
        *,
        job: EngineJob,
        request: Mapping[str, object],
        gap_state_paths: Mapping[str, Path],
    ) -> dict[str, object]:
        # These records are loaded from task-owned gap JSON, so a recreated
        # API does not reselect a release that already failed at the candidate
        # acquisition boundary.  The loader normalizes and caps the list.
        excluded = self._load_excluded_candidates(gap_state_paths)
        request_body = dict(request)
        request_gaps = self._request_gaps(request_body)
        resolved_total: set[str] = set()
        for round_number in range(1, self.max_candidate_rounds + 1):
            # A failed downloader may return after an operator has paused the
            # pilot.  Check *before* the next provider search so a single
            # in-flight attempt cannot fan out into another candidate round.
            self._raise_if_cancelled(
                job, round_number=round_number, boundary="candidate_round",
            )
            self._progress(job, "provider_searching", round=round_number)
            request_body["excluded_candidates"] = list(excluded)
            request_body["gaps"] = [dict(gap) for gap in request_gaps]
            for gap in request_gaps:
                gap_id = str(gap.get("id") or "")
                state_path = gap_state_paths.get(gap_id)
                if state_path is None:
                    continue
                state = json.loads(state_path.read_text(encoding="utf-8"))
                state.update({"phase": "provider_searching", "attempts": round_number, "updated_at": _now(), "error": None})
                self._write_gap(state, state_path)
            result = self.search.run(request_body)
            candidates = result.get("candidates") if isinstance(result, Mapping) else None
            if not isinstance(candidates, list):
                raise AutomaticReplenishmentError("provider 搜索没有返回 candidates 数组")
            selection_bundle = select_replenishment_candidates(request_body, candidates)
            selections = selection_bundle.get("selections")
            if not isinstance(selections, list) or not selections:
                raise AutomaticReplenishmentError("provider 没有找到可用候选")
            # Selection/search is read-only. Do not turn it into a staging
            # write once a live control change has stopped this root.
            self._raise_if_cancelled(
                job, round_number=round_number, boundary="materialization",
            )
            attempt_id = f"attempt-{uuid.uuid4().hex}"
            staging = f"{self.staging_root}/{job.id}/{attempt_id}"
            workspace = self.workspace_root / job.id / attempt_id
            for gap in request_gaps:
                gap_id = str(gap.get("id") or "")
                state_path = gap_state_paths.get(gap_id)
                if state_path is None:
                    continue
                state = json.loads(state_path.read_text(encoding="utf-8"))
                state.update({"phase": "acquiring", "attempts": round_number, "updated_at": _now(), "staging_root": staging})
                self._write_gap(state, state_path)
            staging_files: list[StagingFile] = []
            child: EngineJob | None = None
            completed_child: EngineJob | None = None
            installed_subtitles: list[dict[str, object]] = []
            installed_companions: list[dict[str, object]] = []
            companion_specs: list[dict[str, object]] = []
            subtitle_lane = bool(request_gaps) and all(
                str(gap.get("kind") or "") == "missing_subtitle"
                for gap in request_gaps
            )
            subtitle_gap_ids = {
                str(gap.get("id"))
                for gap in request_gaps
                if str(gap.get("kind") or "") == "missing_subtitle"
                and isinstance(gap.get("id"), str) and gap.get("id")
            }
            attempt_error: Exception | None = None
            candidate_exclusions: list[dict[str, object]] = []
            try:
                self._progress(job, "acquiring", round=round_number, staging_root=staging)
                # Keep this immediately adjacent to the materializer: a pause
                # can arrive while local gap state is being updated above.
                self._raise_if_cancelled(
                    job, round_number=round_number, boundary="materialization",
                )
                acquisition = self.materializer.acquire(
                    request_body,
                    [dict(row) for row in selections if isinstance(row, Mapping)],
                    staging_root=staging,
                    workspace=workspace,
                    alist=self.alist,
                )
                # A downloader can finish just as the operator pauses. Stop
                # before even verifying/claiming its result; no child or
                # subtitle write may follow that boundary.
                self._raise_if_cancelled(
                    job, round_number=round_number, boundary="post_materialization",
                )
                if acquisition.get("status") != "ready":
                    raise AutomaticReplenishmentError("provider 获取没有返回 ready")
                self._progress(job, "staging_verifying", round=round_number, staging_root=staging)
                staging_files = self.verify_staging(staging)
                # Validate both lane roots before any Engine child or formal
                # subtitle write.  A malformed mixed delivery must fail
                # closed while all bytes are still in task-owned staging.
                subtitle_root: str | None = None
                if any(item.kind == "subtitle" for item in staging_files):
                    subtitle_root = self._subtitle_staging_root(
                        acquisition, staging, staging_files,
                    )
                if not subtitle_lane:
                    selected_media_gap_ids = {
                        str(gap_id)
                        for selection in selections
                        if isinstance(selection, Mapping)
                        for gap_id in (
                            selection.get("selected_gap_ids")
                            if isinstance(selection.get("selected_gap_ids"), list)
                            else []
                        )
                        if isinstance(gap_id, str) and gap_id
                    }
                    companion_specs = self._validated_companion_subtitles(
                        request=request_body,
                        acquisition=acquisition,
                        staging_files=staging_files,
                        selected_gap_ids=selected_media_gap_ids,
                    )
                if subtitle_lane:
                    if any(item.kind != "subtitle" for item in staging_files):
                        raise AutomaticReplenishmentError("字幕补源 staging 不得混入视频文件")
                    if subtitle_root is None:
                        raise AutomaticReplenishmentError("字幕补源 staging 缺少字幕根")
                    self._raise_if_cancelled(
                        job, round_number=round_number, boundary="subtitle_write",
                    )
                    installed_subtitles = self._install_subtitle_members(
                        job=job,
                        request=request_body,
                        acquisition=acquisition,
                        staging_root=subtitle_root,
                        round_number=round_number,
                        required_gap_ids=subtitle_gap_ids,
                    )
                    self._progress(
                        job, "final_verifying", round=round_number,
                        subtitle_sidecars=len(installed_subtitles),
                    )
                else:
                    has_video = any(item.kind == "video" for item in staging_files)
                    if has_video:
                        # Planning is durable local state; check before creating
                        # it as well as immediately before the formal child write.
                        self._raise_if_cancelled(
                            job, round_number=round_number, boundary="child_plan",
                        )
                        child_request = dict(job.request)
                        child_request["source_path"] = self._media_child_staging_root(
                            acquisition, staging, staging_files,
                        )
                        child = self._plan_internal_child(child_request, root_job_id=job.id)
                        if not self._audit_child_target_matches(job, child):
                            raise AutomaticReplenishmentError(
                                "审计补源 child 目标与已审计正式目录不一致，拒绝写入"
                            )
                        self._progress(
                            job, "child_planning", round=round_number,
                            staging_root=staging, child_job_id=child.id,
                            child_phase=child.phase,
                        )
                        self._progress(
                            job, "child_executing", round=round_number,
                            child_job_id=child.id, child_phase="executing",
                        )
                        self._raise_if_cancelled(
                            job, round_number=round_number, boundary="child_write",
                        )
                        completed_child = self.engine_runner.execute_automatic(child.id)
                        if completed_child.phase != "executed":
                            raise AutomaticReplenishmentError("补源 child 未完成")

                    # This lane is distinct from an audited missing_subtitle
                    # repair: it has no pre-existing formal video.  A
                    # companion becomes eligible only after the internal child
                    # reports that it *moved* its corresponding new video.
                    # Invalid/missing companion evidence is intentionally not
                    # an error for the media acquisition; audit will discover
                    # the missing sidecar later.
                    if companion_specs and completed_child is not None:
                        if subtitle_root is None:
                            raise AutomaticReplenishmentError("伴随字幕 staging 缺少字幕根")
                        installed_companions = self._install_companion_subtitles(
                            job=job,
                            request=request_body,
                            specs=companion_specs,
                            subtitle_root=subtitle_root,
                            round_number=round_number,
                            child=completed_child,
                        )

                    # A media candidate may carry a subtitle member for an
                    # already-audited video.  Only an explicit subtitle gap
                    # can claim that member, and the child video (if any) is
                    # committed first so the sidecar writer can verify its
                    # exact formal target.
                    if subtitle_gap_ids:
                        delivered_subtitle_ids = self._delivered_subtitle_gap_ids(
                            acquisition, request_body,
                        )
                        if delivered_subtitle_ids:
                            if subtitle_root is None:
                                raise AutomaticReplenishmentError("字幕补源 staging 缺少字幕根")
                            self._raise_if_cancelled(
                                job, round_number=round_number, boundary="subtitle_write",
                            )
                            installed_subtitles = self._install_subtitle_members(
                                job=job,
                                request=request_body,
                                acquisition=acquisition,
                                staging_root=subtitle_root,
                                round_number=round_number,
                                required_gap_ids=delivered_subtitle_ids,
                            )
                    if not has_video and not installed_subtitles:
                        raise AutomaticReplenishmentError(
                            "视频补源 staging 没有可回投的视频或已配对字幕"
                        )
                    self._progress(
                        job, "final_verifying", round=round_number,
                        **({"child_job_id": completed_child.id, "child_phase": completed_child.phase}
                           if completed_child is not None else {}),
                        **({"subtitle_sidecars": len(installed_subtitles) + len(installed_companions)}
                           if installed_subtitles or installed_companions else {}),
                    )
            except Exception as exc:
                attempt_error = exc
                # Capture the materializer's explicit candidate attribution
                # before cleanup can wrap the error as an infrastructure
                # failure.  A generic exception is intentionally not enough
                # evidence to poison a provider identity across restarts.
                candidate_exclusions = self._failure_candidate_exclusions(
                    exc,
                    [row for row in selections if isinstance(row, Mapping)],
                )
                if child is not None:
                    if isinstance(exc, AutomaticReplenishmentCancelled):
                        # A plan may have been persisted in the narrow window
                        # between the pre-plan check and the pre-execute
                        # check.  Cancel that local child record without
                        # touching AList, rather than leaving a misleading
                        # planned implementation task behind.
                        cancel = getattr(self.engine_runner, "cancel_job", None)
                        if callable(cancel):
                            try:
                                cancel(child.id, reason="provider paused before child write")
                            except TypeError:
                                try:
                                    cancel(child.id)
                                except Exception:
                                    pass
                            except Exception:
                                pass
                    else:
                        self._progress(
                            job, "child_failed", round=round_number,
                            child_job_id=child.id, child_phase="failed",
                            error=str(exc) or type(exc).__name__,
                        )
            finally:
                # A pause is a no-new-write boundary.  In particular, do not
                # remove a task staging tree after cancellation: deletion is
                # itself a remote write, and the retained task-owned tree is
                # safe for later verification/recovery.
                if not isinstance(attempt_error, AutomaticReplenishmentCancelled):
                    try:
                        self._progress(job, "cleaning", round=round_number, staging_root=staging)
                        self._remove_staging(staging)
                    except Exception as cleanup_exc:
                        if attempt_error is None:
                            attempt_error = cleanup_exc
                        else:
                            attempt_error = AutomaticReplenishmentError(
                                f"补源尝试失败且 staging 清理失败: {attempt_error}; {cleanup_exc}"
                            )
            if attempt_error is not None:
                # Keep the last bounded candidate failure visible while a
                # later round is running. Without this projection a fresh
                # search masks whether the failure was manifest, payload,
                # delivery, cleanup, or child planning.
                self._progress(
                    job,
                    "retry_wait",
                    round=round_number,
                    error=str(attempt_error) or type(attempt_error).__name__,
                    attempt_failure_stage=getattr(
                        attempt_error, "failure_stage", None,
                    ),
                )
                # Do not exclude the candidate or continue into a second
                # round after a pause/allowlist stop. ``run_for_job`` records
                # the durable per-gap retry_wait state for this exception.
                if isinstance(attempt_error, AutomaticReplenishmentCancelled):
                    raise attempt_error
                if candidate_exclusions:
                    self._persist_excluded_candidates(
                        gap_state_paths, candidate_exclusions,
                    )
                excluded = self._merge_excluded_candidates(
                    excluded,
                    [
                        self._excluded_selection(row)
                        for row in selections if isinstance(row, Mapping)
                    ],
                )
                if round_number >= self.max_candidate_rounds:
                    self._progress(job, "retry_wait", round=round_number, error=str(attempt_error))
                    raise AutomaticReplenishmentError(
                        f"补源已尝试 {round_number} 轮仍失败: {attempt_error}"
                    ) from attempt_error
                continue
            if subtitle_lane:
                resolved_now = {
                    str(row.get("gap_id"))
                    for row in installed_subtitles
                    if isinstance(row, Mapping) and isinstance(row.get("gap_id"), str)
                }
            else:
                resolved_now = set()
                if completed_child is not None:
                    resolved_now.update(
                        self._child_resolved_gap_ids(completed_child, request_gaps)
                    )
                resolved_now.update(
                    str(row.get("gap_id"))
                    for row in installed_subtitles
                    if isinstance(row, Mapping) and isinstance(row.get("gap_id"), str)
                )
            resolved_now &= {
                str(gap.get("id")) for gap in request_gaps
                if isinstance(gap.get("id"), str) and gap.get("id")
            }
            if not resolved_now:
                raise AutomaticReplenishmentError("补源 child 实际文件未覆盖当前 gap")
            resolved_total.update(resolved_now)
            resolved = sorted(resolved_now)
            for gap_id in resolved:
                state_path = gap_state_paths.get(gap_id)
                if state_path is None:
                    continue
                state = json.loads(state_path.read_text(encoding="utf-8"))
                state.update({
                    "phase": "resolved",
                    "updated_at": _now(),
                    "error": None,
                    "staging_files": [item.as_dict() for item in staging_files],
                })
                if completed_child is not None:
                    state["child_job_id"] = completed_child.id
                self._write_gap(state, state_path)
            pending = [
                gap for gap in request_gaps
                if str(gap.get("id") or "") not in resolved_now
            ]
            if pending:
                excluded = self._merge_excluded_candidates(
                    excluded,
                    [
                        self._excluded_selection(row)
                        for row in selections if isinstance(row, Mapping)
                    ],
                )
                if round_number >= self.max_candidate_rounds:
                    self._progress(
                        job, "retry_wait", round=round_number,
                        error="补源 child 只覆盖了部分 gap",
                    )
                    raise AutomaticReplenishmentError("补源 child 只覆盖了部分 gap")
                request_gaps = pending
                continue
            return {
                "request": request_body,
                "resolved_gap_ids": sorted(resolved_total),
                "staging_files": [item.as_dict() for item in staging_files],
                **({"child_job_id": completed_child.id} if completed_child is not None else {}),
                **({"companion_subtitles": installed_companions}
                   if installed_companions else {}),
            }
        raise AutomaticReplenishmentError("补源没有可执行候选")

    def run_for_job(self, job: EngineJob) -> dict[str, object]:
        """Automatically resolve all engine-discovered, provider-compatible gaps."""
        if not isinstance(job.plan, Mapping):
            raise AutomaticReplenishmentError("根任务缺少可用 Engine 计划")
        self._progress(job, "gap_discovering")
        # NFO-bootstrapped audit roots often have only a local-language title.
        # Enrich the in-memory request with aliases from the authoritative
        # TMDB record before querying providers; this does not alter the root
        # job or weaken candidate identity checks.
        request_plan = enrich_replenishment_plan_aliases(
            job.plan, getattr(self.engine_runner, "tmdb", None),
        )
        request_bundle = build_replenishment_requests(
            request_plan, job_id=job.id, round_number=1,
        )
        requests = request_bundle.get("requests") if isinstance(request_bundle, Mapping) else None
        if not isinstance(requests, list):
            raise AutomaticReplenishmentError("Engine gap 请求格式无效")
        outcomes: list[dict[str, object]] = []
        already_resolved: list[str] = []
        for request in requests:
            if not isinstance(request, Mapping) or not self._request_gaps(request):
                continue
            active_gaps: list[dict[str, object]] = []
            states: dict[str, Path] = {}
            for gap in self._request_gaps(request):
                gap_id = gap.get("id")
                if not isinstance(gap_id, str) or not gap_id:
                    continue
                path = self._gap_path(job_id=job.id, gap_id=gap_id)
                state: Mapping[str, object] | None = None
                if path.exists():
                    try:
                        state = json.loads(path.read_text(encoding="utf-8"))
                    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                        state = None
                    if (
                        isinstance(state, Mapping)
                        and state.get("phase") == "resolved"
                        and not (
                            isinstance(state.get("gap"), Mapping)
                            and state["gap"].get("source") == "automatic_library_audit"
                        )
                    ):
                        already_resolved.append(gap_id)
                        continue
                states[gap_id] = self._write_gap(
                    self._gap_state(
                        gap,
                        job_id=job.id,
                        prior_state=state if isinstance(state, Mapping) else None,
                    ),
                    path,
                )
                active_gaps.append(gap)
            if not active_gaps:
                continue
            active_request = dict(request)
            active_request["gaps"] = active_gaps
            try:
                outcomes.append(self._run_request(
                    job=job,
                    request=active_request,
                    gap_state_paths=states,
                ))
            except AutomaticReplenishmentCancelled as exc:
                for gap_id, path in states.items():
                    state = json.loads(path.read_text(encoding="utf-8"))
                    if state.get("phase") != "resolved":
                        state.update({
                            "phase": "retry_wait",
                            "updated_at": _now(),
                            "error": str(exc) or type(exc).__name__,
                        })
                        self._write_gap(state, path)
                outcomes.append({
                    "request": active_request,
                    "resolved_gap_ids": [],
                    "error": str(exc) or type(exc).__name__,
                    "cancelled": True,
                })
                # Do not create state or invoke a provider for another request
                # group once the operator has paused this root.
                return {
                    "job_id": job.id,
                    "outcomes": outcomes,
                    "already_resolved_gap_ids": sorted(set(already_resolved)),
                    "unresolved_gaps": list(request_bundle.get("unresolved_gaps") or []),
                    "cancelled": True,
                }
            except Exception as exc:
                for gap_id, path in states.items():
                    state = json.loads(path.read_text(encoding="utf-8"))
                    if state.get("phase") != "resolved":
                        state.update({"phase": "retry_wait", "updated_at": _now(), "error": str(exc) or type(exc).__name__})
                        self._write_gap(state, path)
                outcomes.append({
                    "request": active_request,
                    "resolved_gap_ids": [],
                    "error": str(exc) or type(exc).__name__,
                })
        return {
            "job_id": job.id,
            "outcomes": outcomes,
            "already_resolved_gap_ids": sorted(set(already_resolved)),
            "unresolved_gaps": list(request_bundle.get("unresolved_gaps") or []),
        }


__all__ = [
    "AutomaticMaterializer",
    "AutomaticProviderSearch",
    "AutomaticReplenishmentCancelled",
    "AutomaticReplenishmentError",
    "AutomaticReplenishmentRuntime",
    "LocalTorrentAutomaticMaterializer",
    "StagingFile",
    "reconcile_interrupted_gap_states",
]
