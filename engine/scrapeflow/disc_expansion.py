"""Read-only disc-image expansion into ordinary task-staged media files.

Constitution (mirrors the operator's safe-expansion ruling):

- The image is never executed or mounted.
- Only verified single-clip primary playlists become media files; the
  selection itself is fail-closed (:meth:`DiscInventory.episode_playlists`).
- The playlist→episode coordinate is proven from engine-own evidence only:
  disc order plus playlist order must admit exactly one order-preserving,
  duration-tolerant, injective assignment against the published TMDB
  episode runtimes that covers every roster episode (extra playlists above
  the selection threshold are skipped as bonus features). Zero or several
  valid assignments park the scope.
- A parked scope whose disc declares no readable ordering at all may be
  resolved by an operator ruling filed as durable data; the ruling is
  validated against the same roster and duration tolerance so the operator
  lane can never launder a mapping the disc itself contradicts.
- Every remux preserves the embedded streams and lands in task-owned
  disposable staging — never in the formal library — and the staged files
  then flow through the normal planner and single writer like any ordinary
  source object.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import posixpath
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from typing import Callable, Mapping, Sequence

from engine.scrapeflow.disc_image import (
    DiscImageError,
    DiscInventory,
    InnerFile,
    iter_inner_file_bytes,
    retrying_alist_range_reader,
)

_EXPLICIT_DISC_ORDINAL_RE = re.compile(
    r"disc[\s._-]*(\d{1,3})(?!\d)",
    re.IGNORECASE,
)
_BARE_DISC_ORDINAL_RE = re.compile(
    r"(?:^|[.\s_\-])d(\d{1,3})(?!\d)",
    re.IGNORECASE,
)


def disc_ordinal_from_image_name(name: str) -> int | None:
    """Return the disc ordinal encoded in an image filename, if any.

    ``Show.S01-DISC2.iso`` and ``Show.S09.D01.iso`` both declare their disc
    position.  An explicit ``DISC`` token wins over a bare ``D01`` token;
    the bare form is only accepted directly after a separator so an audio
    tag such as ``DTS-HD5.1`` can never pose as a disc ordinal.  The ordinal
    is only ordering evidence for the mapping proof; an image without one is
    still expandable, but a multi-image scope that cannot order its discs
    fails closed in the derivation.
    """
    stem = posixpath.splitext(posixpath.basename(str(name)))[0]
    for pattern in (_EXPLICIT_DISC_ORDINAL_RE, _BARE_DISC_ORDINAL_RE):
        best: int | None = None
        for match in pattern.finditer(stem):
            value = int(match.group(1))
            if value > 0:
                best = value
        if best is not None:
            return best
    return None


@dataclass(frozen=True)
class SeasonEpisodeRoster:
    """Published season coordinates used as mapping evidence.

    ``episodes`` carries ``(episode_number, runtime_minutes)`` pairs in
    ascending episode order; minutes is TMDB's native published unit, so the
    field name pins the unit at the boundary instead of asking every caller
    to remember a conversion.  A season whose published roster is incomplete
    (missing episode numbers or runtimes) cannot prove an assignment, so the
    derivation fails closed instead of guessing a partial match.
    """

    season: int
    episodes: tuple[tuple[int, int | None], ...]

    def __post_init__(self) -> None:
        if isinstance(self.season, bool) or self.season < 0:
            raise ValueError("season 必须是非负整数")
        seen: set[int] = set()
        for number, runtime in self.episodes:
            if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
                raise ValueError("episode_number 必须是正整数")
            if number in seen:
                raise ValueError("episode_number 重复")
            seen.add(number)
            if runtime is not None and (
                isinstance(runtime, bool)
                or not isinstance(runtime, int)
                or runtime <= 0
            ):
                raise ValueError("runtime_minutes 必须是正整数或 None")

    @property
    def runtime_seconds(self) -> tuple[tuple[int, int], ...]:
        """Published runtimes as seconds, holes excluded."""
        return tuple(
            (number, minutes * 60)
            for number, minutes in self.episodes
            if minutes is not None
        )


@dataclass(frozen=True)
class PlaylistCandidate:
    """One selected playlist with its clip and stable image fingerprint."""

    image_path: str
    image_size: int
    image_version: str
    playlist_inner_path: str
    clip_inner_path: str
    clip_size: int
    duration_seconds: float
    disc_ordinal: int | None
    playlist_ordinal: int


@dataclass(frozen=True)
class EpisodeMapping:
    """One proven playlist→episode assignment and its staging target."""

    candidate: PlaylistCandidate
    season: int
    episode: int
    target_path: str


@dataclass(frozen=True)
class ScopeExpansionPlan:
    """The proven expansion of one source scope, or its park reason."""

    scope_path: str
    season: int | None
    mappings: tuple[EpisodeMapping, ...] = ()
    attention: str | None = None
    basis: str = "duration-dp"
    skipped_playlists: tuple[str, ...] = ()

    @property
    def proven(self) -> bool:
        return self.attention is None


def collect_playlist_candidates(
    image_path: str,
    inventory: DiscInventory,
    *,
    image_size: int,
    image_version: str,
) -> list[PlaylistCandidate]:
    """Project one inventory's selected playlists into mapping candidates."""
    if inventory.structure != "bdmv":
        raise DiscImageError(
            f"镜像结构尚不支持正片展开: {image_path} ({inventory.structure})"
        )
    by_inner_path = {
        item.inner_path.casefold(): item for item in inventory.inner_files
    }
    candidates: list[PlaylistCandidate] = []
    for playlist in inventory.episode_playlists():
        play_item = playlist.play_items[0]
        clip_path = f"/BDMV/STREAM/{play_item.clip_id}.M2TS".casefold()
        clip = by_inner_path.get(clip_path)
        if clip is None:
            raise DiscImageError(
                f"playlist 引用的 clip 不在镜像清单内: {playlist.inner_path}"
            )
        stem = posixpath.splitext(posixpath.basename(playlist.inner_path))[0]
        try:
            playlist_ordinal = int(stem)
        except ValueError as exc:
            raise DiscImageError(
                f"playlist 文件名不是数字序号: {playlist.inner_path}"
            ) from exc
        candidates.append(
            PlaylistCandidate(
                image_path=image_path,
                image_size=image_size,
                image_version=str(image_version),
                playlist_inner_path=playlist.inner_path,
                clip_inner_path=clip.inner_path,
                clip_size=clip.size,
                duration_seconds=playlist.duration_seconds,
                disc_ordinal=disc_ordinal_from_image_name(image_path),
                playlist_ordinal=playlist_ordinal,
            )
        )
    return candidates


def _sort_key(candidate: PlaylistCandidate) -> tuple[int, int, str]:
    ordinal = candidate.disc_ordinal
    return (
        ordinal if ordinal is not None else 10**9,
        candidate.playlist_ordinal,
        candidate.playlist_inner_path.casefold(),
    )


def _unique_order_preserving_assignment(
    playlist_durations: Sequence[float],
    roster: Sequence[tuple[int, int | None]],
    *,
    tolerance_seconds: float,
) -> list[int | None] | None:
    """Return the single order-preserving assignment, or ``None``.

    ``playlist_durations[i]`` (seconds) may map to some episode ``roster[j]``
    whose published runtime in minutes is within ``tolerance_seconds`` once
    converted to seconds.  Every roster episode must be covered by exactly one
    playlist (injective, strictly order-preserving); extra playlists — bonus
    features that survived the duration selection — are skipped and reported
    as ``None`` entries.  The dynamic programme counts valid assignments
    capped at two: exactly one proves the coordinates, zero or several do
    not.
    """
    n = len(playlist_durations)
    if any(runtime is None for _number, runtime in roster):
        # A season with unpublished runtimes cannot discriminate any
        # assignment; treating the holes as wildcards would fabricate
        # uniqueness, so the whole season fails closed.
        return None
    episodes = [
        (number, minutes * 60) for number, minutes in roster
    ]
    m = len(episodes)
    if m == 0 or n < m:
        # Every episode needs its own playlist; too few candidates can never
        # cover the roster.
        return None
    # match[i][j]: playlist i may take episode j within tolerance.
    match = [
        [
            abs(duration - episodes[j][1]) <= tolerance_seconds
            for j in range(m)
        ]
        for duration in playlist_durations
    ]
    # count[i][j]: number (capped at 2) of ways to cover every episode in
    # episodes[j:] with a strictly increasing subsequence of playlists[i:].
    count = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(n + 1):
        count[i][m] = 1
    for i in range(n - 1, -1, -1):
        for j in range(m - 1, -1, -1):
            total = count[i + 1][j]
            if match[i][j]:
                total += count[i + 1][j + 1]
            count[i][j] = min(total, 2)
    if count[0][0] == 0:
        return None
    if count[0][0] > 1:
        return None
    assignment: list[int | None] = []
    i = 0
    j = 0
    while j < m:
        if i >= n:
            return None
        skip_live = count[i + 1][j] > 0
        match_live = match[i][j] and count[i + 1][j + 1] > 0
        if skip_live and match_live:
            # Contradicts the unique count above; kept as a defensive guard.
            return None
        if match_live:
            assignment.append(episodes[j][0])
            i += 1
            j += 1
        elif skip_live:
            assignment.append(None)
            i += 1
        else:
            return None
    assignment.extend([None] * (n - len(assignment)))
    return assignment


def derive_scope_expansion(
    *,
    scope_path: str,
    season: int | None,
    roster: SeasonEpisodeRoster | None,
    candidates: Sequence[PlaylistCandidate],
    staging_root: str,
    work_name: str,
    tolerance_seconds: float = 120.0,
) -> ScopeExpansionPlan:
    """Prove one scope's playlist→episode mapping or return its park reason.

    ``season`` comes from the scope's own directory evidence and must agree
    with the roster.  ``candidates`` span every image of the scope and are
    ordered by (disc ordinal, playlist ordinal); the proof requires exactly
    one order-preserving duration-tolerant assignment that covers every
    roster episode — extra playlists (bonus features that survived the
    duration selection) are skipped.
    """
    if not candidates:
        return ScopeExpansionPlan(
            scope_path=scope_path,
            season=season,
            attention="镜像内没有可证明的正片 playlist（单 clip 主 playlist 未通过只读甄别）",
        )
    if season is None:
        return ScopeExpansionPlan(
            scope_path=scope_path,
            season=None,
            attention="来源范围缺少明确的季证据，无法证明 playlist→集坐标",
        )
    if roster is None or roster.season != season or not roster.episodes:
        return ScopeExpansionPlan(
            scope_path=scope_path,
            season=season,
            attention=f"TMDB 第 {season} 季缺少已发布的集清单，无法证明集坐标",
        )
    if len({c.image_path for c in candidates}) > 1 and any(
        c.disc_ordinal is None for c in candidates
    ):
        return ScopeExpansionPlan(
            scope_path=scope_path,
            season=season,
            attention="多碟来源存在无碟序证据的镜像，无法证明跨碟集顺序",
        )
    ordered = sorted(candidates, key=_sort_key)
    image_count = len({c.image_path for c in ordered})
    if image_count > 1 and len(
        {c.disc_ordinal for c in ordered if c.disc_ordinal is not None}
    ) != image_count:
        return ScopeExpansionPlan(
            scope_path=scope_path,
            season=season,
            attention="多碟来源的碟序证据缺失或重复，无法证明跨碟集顺序",
        )
    assignment = _unique_order_preserving_assignment(
        [c.duration_seconds for c in ordered],
        roster.episodes,
        tolerance_seconds=tolerance_seconds,
    )
    if assignment is None:
        return ScopeExpansionPlan(
            scope_path=scope_path,
            season=season,
            attention=(
                "playlist 时长与 TMDB 集时长不存在唯一的保序映射"
                f"（容差 ±{tolerance_seconds:.0f}s），拒绝猜测集坐标"
            ),
        )
    mappings: list[EpisodeMapping] = []
    skipped: list[str] = []
    scope_name = posixpath.basename(scope_path.rstrip("/")) or "expanded"
    for candidate, episode in zip(ordered, assignment):
        if episode is None:
            skipped.append(candidate.playlist_inner_path)
            continue
        target = posixpath.join(
            staging_root,
            scope_name,
            f"Season {season:02d}",
            f"{work_name} - S{season:02d}E{episode:02d}.mkv",
        )
        mappings.append(
            EpisodeMapping(
                candidate=candidate,
                season=season,
                episode=episode,
                target_path=target,
            )
        )
    return ScopeExpansionPlan(
        scope_path=scope_path,
        season=season,
        mappings=tuple(mappings),
        skipped_playlists=tuple(skipped),
    )


@dataclass(frozen=True)
class ScopeMappingRuling:
    """An operator-filed playlist→episode ruling for one scope.

    Some repacked discs declare their episode order in no readable metadata
    (playlist names, clip numbering, file entries, navigation objects all
    fail).  When the duration proof parks such a scope, the operator may
    confirm one assignment as durable data.  The engine never invents this:
    the ruling is validated against the same roster and duration tolerance
    as the proof, and its provenance travels with the plan.
    """

    scope_path: str
    season: int
    assignments: tuple[tuple[str, str, int], ...]
    operator: str
    note: str
    filed_at: str

    def as_dict(self) -> dict[str, object]:
        return {
            "scope_path": self.scope_path,
            "season": self.season,
            "assignments": [
                {
                    "image_path": image_path,
                    "playlist_inner_path": path,
                    "episode": episode,
                }
                for image_path, path, episode in self.assignments
            ],
            "operator": self.operator,
            "note": self.note,
            "filed_at": self.filed_at,
        }

    @classmethod
    def from_mapping(cls, data: Mapping[str, object]) -> "ScopeMappingRuling":
        if not isinstance(data, Mapping):
            raise ValueError("裁决必须是一个 JSON 对象")
        raw_assignments = data.get("assignments")
        if not isinstance(raw_assignments, Sequence) or isinstance(
            raw_assignments, (str, bytes)
        ):
            raise ValueError("裁决缺少 assignments 序列")
        assignments: list[tuple[str, str, int]] = []
        for item in raw_assignments:
            if not isinstance(item, Mapping):
                raise ValueError("裁决条目必须是对象")
            image_path = item.get("image_path")
            path = item.get("playlist_inner_path")
            episode = item.get("episode")
            if not isinstance(image_path, str) or not image_path:
                raise ValueError("裁决条目缺少 image_path")
            if not isinstance(path, str) or not path:
                raise ValueError("裁决条目缺少 playlist_inner_path")
            if isinstance(episode, bool) or not isinstance(episode, int):
                raise ValueError(f"裁决条目 {path} 的集号不是整数")
            assignments.append((image_path, path, episode))
        season = data.get("season")
        scope_path = data.get("scope_path")
        operator = data.get("operator")
        note = data.get("note")
        filed_at = data.get("filed_at")
        if not isinstance(scope_path, str) or not scope_path:
            raise ValueError("裁决缺少 scope_path")
        if isinstance(season, bool) or not isinstance(season, int):
            raise ValueError("裁决缺少整数季号")
        if not isinstance(operator, str) or not operator:
            raise ValueError("裁决缺少 operator 出处")
        if not isinstance(note, str) or not note:
            raise ValueError("裁决缺少 note 依据说明")
        if not isinstance(filed_at, str) or not filed_at:
            raise ValueError("裁决缺少 filed_at 时间戳")
        return cls(
            scope_path=scope_path,
            season=season,
            assignments=tuple(assignments),
            operator=operator,
            note=note,
            filed_at=filed_at,
        )


def apply_scope_mapping_ruling(
    *,
    scope_path: str,
    season: int | None,
    roster: SeasonEpisodeRoster | None,
    candidates: Sequence[PlaylistCandidate],
    ruling: ScopeMappingRuling,
    staging_root: str,
    work_name: str,
    tolerance_seconds: float = 120.0,
) -> ScopeExpansionPlan:
    """Turn one operator ruling into mappings, or explain why it is invalid.

    The ruling must address exactly this scope's candidates, cover the roster
    bijectively, and remain within the same duration tolerance the automatic
    proof uses — a ruling that contradicts the disc's own durations is
    rejected so the operator lane can never launder a wrong mapping.
    """
    if not candidates:
        raise ValueError("镜像内没有可展开的正片 playlist，裁决无事可裁")
    if season is None or roster is None or roster.season != season:
        raise ValueError("裁决与来源范围或 TMDB 集清单不一致")
    if ruling.scope_path != scope_path or ruling.season != season:
        raise ValueError("裁决指向了别的来源范围或季")
    by_playlist: dict[tuple[str, str], PlaylistCandidate] = {}
    for candidate in candidates:
        key = (
            candidate.image_path.casefold(),
            candidate.playlist_inner_path.casefold(),
        )
        if key in by_playlist:
            raise ValueError("候选 playlist 重复，无法应用裁决")
        by_playlist[key] = candidate
    ruled_keys = [
        (image_path.casefold(), path.casefold())
        for image_path, path, _episode in ruling.assignments
    ]
    if len(set(ruled_keys)) != len(ruled_keys):
        raise ValueError("裁决对同一 playlist 给出了多个集号")
    if set(ruled_keys) != set(by_playlist):
        missing = sorted(set(by_playlist) - set(ruled_keys))
        extra = sorted(set(ruled_keys) - set(by_playlist))
        raise ValueError(
            "裁决的 playlist 集合与镜像候选不一致"
            f"（缺 {missing[:3]} 多 {extra[:3]}）"
        )
    roster_episodes = {number for number, _runtime in roster.episodes}
    ruled_episodes = [episode for _image, _path, episode in ruling.assignments]
    if len(set(ruled_episodes)) != len(ruled_episodes):
        raise ValueError("裁决给多个 playlist 指定了同一集")
    if set(ruled_episodes) != roster_episodes:
        raise ValueError("裁决的集号集合与 TMDB 集清单不一致")
    runtime_by_episode = {
        number: runtime for number, runtime in roster.episodes
    }
    for image_path, path, episode in ruling.assignments:
        candidate = by_playlist[(image_path.casefold(), path.casefold())]
        runtime_minutes = runtime_by_episode.get(episode)
        if runtime_minutes is None:
            raise ValueError(f"裁决指定的集 E{episode:02d} 不在 TMDB 集清单内")
        delta = candidate.duration_seconds - runtime_minutes * 60
        if abs(delta) > tolerance_seconds:
            raise ValueError(
                f"裁决 {path}→E{episode:02d} 与时长矛盾"
                f"（偏差 {delta:+.0f}s 超出 ±{tolerance_seconds:.0f}s）"
            )
    mappings: list[EpisodeMapping] = []
    scope_name = posixpath.basename(scope_path.rstrip("/")) or "expanded"
    for image_path, path, episode in sorted(
        ruling.assignments, key=lambda item: item[2]
    ):
        candidate = by_playlist[(image_path.casefold(), path.casefold())]
        target = posixpath.join(
            staging_root,
            scope_name,
            f"Season {season:02d}",
            f"{work_name} - S{season:02d}E{episode:02d}.mkv",
        )
        mappings.append(
            EpisodeMapping(
                candidate=candidate,
                season=season,
                episode=episode,
                target_path=target,
            )
        )
    return ScopeExpansionPlan(
        scope_path=scope_path,
        season=season,
        mappings=tuple(mappings),
        basis="operator-ruling",
    )


# ---------------------------------------------------------------------------
# Remux and transfer
# ---------------------------------------------------------------------------


class DiscExpansionError(Exception):
    """A staged expansion transfer could not be completed."""


@dataclass(frozen=True)
class RemuxEvidence:
    """Local verification evidence for one remuxed playlist."""

    output_path: str
    output_bytes: int
    duration_seconds: float
    video_streams: int
    audio_streams: int
    subtitle_streams: int
    md5: str
    sha1: str


def remux_inner_file_to_matroska(
    read_range: Callable[[int, int], bytes],
    inner_file: InnerFile,
    *,
    image_size: int,
    output_path: str,
    chunk_bytes: int = 16 * 1024 * 1024,
    ffmpeg_argv: Sequence[str] = ("ffmpeg",),
    duration_tolerance_seconds: float = 5.0,
    expected_duration_seconds: float | None = None,
    probe_timeout_seconds: float = 300.0,
) -> RemuxEvidence:
    """Remux one inner transport stream into a local Matroska buffer.

    The bytes stream from the remote image through ``ffmpeg -c copy`` into
    ``output_path``; nothing is mounted or executed.  The result is probed
    locally before it may be uploaded: it must carry at least one video
    stream and a duration consistent with the playlist it came from.  The
    evidence hashes cover the produced file — the object the executor
    uploads and declares to the provider — never the input stream, because
    the provider's commit callback verifies the declared hashes against the
    uploaded object itself.
    """
    if os.path.exists(output_path):
        raise DiscExpansionError(f"本地 remux 缓冲已存在，拒绝覆盖: {output_path}")
    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    argv = [
        *ffmpeg_argv,
        "-nostdin",
        "-loglevel", "error",
        "-f", "mpegts",
        "-probesize", "100M",
        "-analyzeduration", "100M",
        "-i", "pipe:0",
        "-map", "0",
        "-c", "copy",
        "-f", "matroska",
        "-y",
        output_path,
    ]
    fed = 0
    process = subprocess.Popen(
        argv,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    assert process.stdin is not None
    try:
        try:
            for chunk in iter_inner_file_bytes(
                read_range,
                inner_file,
                image_size=image_size,
                chunk_bytes=chunk_bytes,
            ):
                fed += len(chunk)
                process.stdin.write(chunk)
            process.stdin.close()
        except BrokenPipeError:
            process.stdin.close()
        except BaseException:
            process.kill()
            process.wait(timeout=30)
            raise
        stderr = process.stderr.read() if process.stderr is not None else b""
        returncode = process.wait()
        if returncode != 0:
            raise DiscExpansionError(
                "ffmpeg remux 失败: "
                + stderr.decode("utf-8", "replace").strip()[-2000:]
            )
    finally:
        if process.poll() is None:  # pragma: no cover - defensive
            process.kill()
    if fed != inner_file.size:
        raise DiscExpansionError(
            f"喂给 ffmpeg 的字节数与 clip 大小不符: {fed} != {inner_file.size}"
        )
    digest_md5 = hashlib.md5(usedforsecurity=False)
    digest_sha1 = hashlib.sha1(usedforsecurity=False)
    output_bytes = 0
    with open(output_path, "rb") as handle:
        while True:
            block = handle.read(chunk_bytes)
            if not block:
                break
            digest_md5.update(block)
            digest_sha1.update(block)
            output_bytes += len(block)
    if output_bytes <= 0:
        raise DiscExpansionError("本地 remux 输出为空，拒绝上传")
    return _probe_local_matroska(
        output_path,
        md5=digest_md5.hexdigest(),
        sha1=digest_sha1.hexdigest(),
        expected_duration_seconds=expected_duration_seconds,
        duration_tolerance_seconds=duration_tolerance_seconds,
        probe_timeout_seconds=probe_timeout_seconds,
        ffmpeg_argv=ffmpeg_argv,
    )


def _video_packet_span_seconds(
    output_path: str,
    *,
    probe_timeout_seconds: float,
    ffmpeg_argv: Sequence[str],
) -> float:
    """First→last video packet timestamp span; 0.0 when unavailable.

    ``ffprobe`` 5.1 does not report a per-stream ``duration`` for Matroska,
    so the video stream's runtime is derived from its packet timestamps.
    """
    probe_argv = [
        ffmpeg_argv[0].replace("ffmpeg", "ffprobe"),
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "packet=pts_time",
        "-of", "csv=p=0",
        output_path,
    ]
    try:
        completed = subprocess.run(
            probe_argv,
            capture_output=True,
            timeout=probe_timeout_seconds,
        )
    except FileNotFoundError:
        return 0.0
    if completed.returncode != 0:
        return 0.0
    stamps: list[float] = []
    for line in completed.stdout.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            stamps.append(float(line))
        except ValueError:
            continue
    if len(stamps) < 2:
        return 0.0
    span = stamps[-1] - stamps[0]
    return span if span > 0 else 0.0


def _probe_local_matroska(
    output_path: str,
    *,
    md5: str,
    sha1: str,
    expected_duration_seconds: float | None,
    duration_tolerance_seconds: float,
    probe_timeout_seconds: float,
    ffmpeg_argv: Sequence[str],
) -> RemuxEvidence:
    """ffprobe one local remux and prove its shape before upload."""
    probe_argv = [
        ffmpeg_argv[0].replace("ffmpeg", "ffprobe"),
        "-v", "error",
        "-show_entries",
        "format=duration:stream=index,codec_type,duration",
        "-of", "json",
        output_path,
    ]
    try:
        completed = subprocess.run(
            probe_argv,
            capture_output=True,
            timeout=probe_timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise DiscExpansionError("ffprobe 不可用，无法核验 remux 结果") from exc
    if completed.returncode != 0:
        raise DiscExpansionError(
            "ffprobe 核验失败: "
            + completed.stderr.decode("utf-8", "replace").strip()[-2000:]
        )
    try:
        payload = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DiscExpansionError("ffprobe 输出无法解析") from exc
    streams = payload.get("streams") or []
    video = sum(1 for s in streams if s.get("codec_type") == "video")
    audio = sum(1 for s in streams if s.get("codec_type") == "audio")
    subtitle = sum(1 for s in streams if s.get("codec_type") == "subtitle")
    if video < 1:
        raise DiscExpansionError("remux 结果不含视频轨")
    # The container duration covers every stream; a trailing subtitle track
    # can extend it past the playlist's clip span (observed on a DIY disc
    # whose last subtitle lagged the video by ~7s).  The episode's runtime
    # is the video stream's duration, so gate on that.  ffprobe 5.1 does
    # not emit a per-stream "duration" for Matroska, so the video span is
    # read from the first/last packet timestamps when the field is absent;
    # only when neither is available does the container duration stand in.
    duration = 0.0
    for stream in streams:
        if stream.get("codec_type") == "video":
            raw = stream.get("duration")
            try:
                candidate = float(raw) if raw is not None else 0.0
            except (TypeError, ValueError):
                candidate = 0.0
            duration = max(duration, candidate)
    if duration <= 0:
        duration = _video_packet_span_seconds(
            output_path, probe_timeout_seconds=probe_timeout_seconds,
            ffmpeg_argv=ffmpeg_argv,
        )
    if duration <= 0:
        duration = float(payload.get("format", {}).get("duration") or 0.0)
    if expected_duration_seconds is not None and (
        duration <= 0
        or abs(duration - expected_duration_seconds) > duration_tolerance_seconds
    ):
        raise DiscExpansionError(
            f"remux 时长与 playlist 不符: {duration:.3f}s vs "
            f"{expected_duration_seconds:.3f}s"
        )
    return RemuxEvidence(
        output_path=output_path,
        output_bytes=os.path.getsize(output_path),
        duration_seconds=duration,
        video_streams=video,
        audio_streams=audio,
        subtitle_streams=subtitle,
        md5=md5,
        sha1=sha1,
    )


@dataclass
class ExpansionTransferState:
    """Durable per-mapping transfer record (JSON on the task's state area)."""

    image_path: str
    playlist_inner_path: str
    season: int
    episode: int
    target_path: str
    status: str  # "pending" | "uploading" | "uploaded" | "completed" | "failed"
    inner_size: int = 0
    duration_seconds: float = 0.0
    output_bytes: int = 0
    md5: str = ""
    sha1: str = ""
    updated_at: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "image_path": self.image_path,
            "playlist_inner_path": self.playlist_inner_path,
            "season": self.season,
            "episode": self.episode,
            "target_path": self.target_path,
            "status": self.status,
            "inner_size": self.inner_size,
            "duration_seconds": self.duration_seconds,
            "output_bytes": self.output_bytes,
            "md5": self.md5,
            "sha1": self.sha1,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_mapping(cls, mapping: EpisodeMapping) -> "ExpansionTransferState":
        candidate = mapping.candidate
        return cls(
            image_path=candidate.image_path,
            playlist_inner_path=candidate.playlist_inner_path,
            season=mapping.season,
            episode=mapping.episode,
            target_path=mapping.target_path,
            status="pending",
            inner_size=candidate.clip_size,
            duration_seconds=candidate.duration_seconds,
        )


def _state_key(mapping: EpisodeMapping) -> str:
    candidate = mapping.candidate
    stem = posixpath.splitext(posixpath.basename(candidate.playlist_inner_path))[0]
    image_stem = posixpath.splitext(posixpath.basename(candidate.image_path))[0]
    return f"{image_stem}__{stem}__S{mapping.season:02d}E{mapping.episode:02d}.json"


class DiscExpansionExecutor:
    """Transfer proven mappings into task-owned remote staging, resumably.

    Each mapping keeps a small JSON state record beside the task's other
    durable state, advancing ``pending → uploading → uploaded → completed``:

    - the deterministic remux evidence (size and hashes of the produced
      file) is persisted *before* the upload starts, so a crash mid-upload
      still leaves the evidence on disk;
    - the state flips to ``uploaded`` the moment the provider commits;
    - only a successful readback marks ``completed``.

    A target that already exists at precheck is reconciled against that
    evidence by the size gate: our own committed upload from an attempt
    whose state was never saved is adopted (re-remuxing once if the state
    was lost, since the remux is deterministic), while a foreign or
    divergent object stays a hard refusal.  A completed record plus an
    exact-size remote object short-circuits the transfer, so an interrupted
    expansion resumes without re-reading gigabytes.  A partial local buffer
    is always discarded: an ffmpeg output cannot be resumed safely.
    """

    def __init__(
        self,
        alist_client: object,
        *,
        state_dir: str,
        local_buffer_dir: str,
        chunk_bytes: int = 16 * 1024 * 1024,
        min_free_buffer_bytes: int = 20 * 1024 * 1024 * 1024,
        readback_attempts: int = 30,
        readback_interval_seconds: float = 60.0,
        now: Callable[[], str] | None = None,
        sleep: Callable[[float], None] | None = None,
        reader_opener: Callable[..., object] | None = None,
        statter: Callable[[str], Mapping[str, object] | None] | None = None,
        uploader: Callable[..., object] | None = None,
        remux: Callable[..., RemuxEvidence] | None = None,
    ) -> None:
        self.alist = alist_client
        self.state_dir = state_dir
        self.local_buffer_dir = local_buffer_dir
        self.chunk_bytes = chunk_bytes
        self.min_free_buffer_bytes = min_free_buffer_bytes
        # Provider listings lag behind a committed upload by minutes at
        # multi-GB sizes (empirically ~4 min on quark_uc for one file, and
        # ~12 min when back-to-back multi-GB uploads land in the same
        # directory; the exact-path stat itself has been observed needing
        # ~19 minutes, right at the edge of a 20-round window), so the
        # post-upload readback tolerates a bounded window before declaring
        # failure.
        self.readback_attempts = max(1, readback_attempts)
        self.readback_interval_seconds = max(0.0, readback_interval_seconds)
        self._now = now or (lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        self._sleep = sleep or time.sleep
        self._reader_opener = reader_opener or retrying_alist_range_reader
        self._statter = statter
        self._uploader = uploader
        self._remux = remux or remux_inner_file_to_matroska

    # -- state -----------------------------------------------------------

    def state_path(self, mapping: EpisodeMapping) -> str:
        return os.path.join(self.state_dir, _state_key(mapping))

    def load_state(self, mapping: EpisodeMapping) -> ExpansionTransferState | None:
        path = self.state_path(mapping)
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            return None
        if not isinstance(payload, Mapping):
            raise DiscExpansionError(f"展开状态记录损坏: {path}")
        state = ExpansionTransferState(
            image_path=str(payload.get("image_path") or ""),
            playlist_inner_path=str(payload.get("playlist_inner_path") or ""),
            season=int(payload.get("season") or 0),
            episode=int(payload.get("episode") or 0),
            target_path=str(payload.get("target_path") or ""),
            status=str(payload.get("status") or "pending"),
            inner_size=int(payload.get("inner_size") or 0),
            duration_seconds=float(payload.get("duration_seconds") or 0.0),
            output_bytes=int(payload.get("output_bytes") or 0),
            md5=str(payload.get("md5") or ""),
            sha1=str(payload.get("sha1") or ""),
            updated_at=str(payload.get("updated_at") or ""),
        )
        if (
            state.image_path != mapping.candidate.image_path
            or state.playlist_inner_path != mapping.candidate.playlist_inner_path
            or state.season != mapping.season
            or state.episode != mapping.episode
            or state.target_path != mapping.target_path
        ):
            raise DiscExpansionError(f"展开状态记录与当前映射不一致: {path}")
        return state

    def _save_state(self, state: ExpansionTransferState) -> None:
        os.makedirs(self.state_dir, exist_ok=True)
        path = os.path.join(self.state_dir, _state_key_from_state(state))
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(state.as_dict(), handle, ensure_ascii=False, indent=1)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)

    # -- transfer --------------------------------------------------------

    def _stat_remote(self, path: str) -> Mapping[str, object] | None:
        if self._statter is not None:
            return self._statter(path)
        statter = getattr(self.alist, "exact_file_info", None)
        if not callable(statter):
            raise DiscExpansionError("AList 客户端缺少 exact_file_info 安全接口")
        return statter(path)

    def _listing_second_opinion(
        self, path: str
    ) -> Mapping[str, object] | None:
        """Arbitrate the exact-path stat against the refreshed listing.

        During the post-upload visibility window the provider's exact-path
        stat can lag behind its own refreshed listing (observed on quark_uc:
        the listing showed the fully committed object while the stat stayed
        blind past the whole readback window).  Both speak for the same
        remote state, so the listing may confirm — never invent — a hit,
        and only on an exact-name, non-directory match.
        """
        lister = getattr(self.alist, "list", None)
        if not callable(lister):
            return None
        parent = posixpath.dirname(path)
        name = posixpath.basename(path)
        try:
            entries = lister(parent, refresh=True)
        except Exception:  # noqa: BLE001 - a second opinion stays optional
            return None
        for entry in entries or []:
            if (
                isinstance(entry, Mapping)
                and not entry.get("is_dir")
                and str(entry.get("name") or "") == name
            ):
                return entry
        return None

    def _stat_remote_with_second_opinion(
        self, path: str
    ) -> Mapping[str, object] | None:
        """One exact stat plus one refreshed-listing check, without waiting.

        For an object that should long be visible — a completed mapping's
        target, or a precheck hit from an earlier attempt — a transiently
        blind exact stat is arbitrated by a single refreshed listing.  The
        bounded visibility window is reserved for just-committed uploads.
        """
        remote = self._stat_remote(path)
        if remote is None:
            remote = self._listing_second_opinion(path)
        return remote

    def _stat_remote_with_lag(self, path: str) -> Mapping[str, object] | None:
        """Stat with a bounded wait for provider listing lag.

        The precheck and completed-state paths stay immediate: those files
        were either visible long ago or must not exist at all.  Only a just
        committed upload can sit in the provider's visibility window, and
        each round consults the refreshed listing as a second opinion
        because the exact-path stat can be the slower of the two indexes.
        """
        remote = self._stat_remote(path)
        if remote is None:
            remote = self._listing_second_opinion(path)
        attempts = self.readback_attempts
        while (
            remote is None
            and attempts > 1
        ):
            attempts -= 1
            self._sleep(self.readback_interval_seconds)
            remote = self._stat_remote(path)
            if remote is None:
                remote = self._listing_second_opinion(path)
        return remote

    def _upload_stream(self, target_path: str, chunks, *, size: int, md5: str, sha1: str):
        if self._uploader is not None:
            return self._uploader(
                target_path, chunks, size=size, md5=md5, sha1=sha1,
                content_type="video/x-matroska",
            )
        uploader = getattr(self.alist, "upload_stream", None)
        if not callable(uploader):
            raise DiscExpansionError("AList 客户端缺少零落盘上传安全接口")
        return uploader(
            target_path, chunks, size=size, md5=md5, sha1=sha1,
            content_type="video/x-matroska",
        )

    def _local_buffer_path(self, mapping: EpisodeMapping) -> str:
        stem = posixpath.splitext(
            posixpath.basename(mapping.candidate.image_path)
        )[0]
        playlist_stem = posixpath.splitext(
            posixpath.basename(mapping.candidate.playlist_inner_path)
        )[0]
        name = f"{stem}__{playlist_stem}__S{mapping.season:02d}E{mapping.episode:02d}.mkv"
        return os.path.join(self.local_buffer_dir, name)

    def _require_free_buffer_space(self, expected_bytes: int) -> None:
        usage = shutil.disk_usage(self.local_buffer_dir)
        if usage.free - expected_bytes < self.min_free_buffer_bytes:
            raise DiscExpansionError(
                "本地 remux 缓冲空间不足: "
                f"free={usage.free}, need={expected_bytes + self.min_free_buffer_bytes}"
            )

    def execute_mapping(
        self,
        mapping: EpisodeMapping,
        *,
        inner_file: InnerFile,
    ) -> ExpansionTransferState:
        """Transfer one mapping; idempotent for completed mappings."""
        state = self.load_state(mapping)
        if state is not None and state.status == "completed":
            remote = self._stat_remote_with_second_opinion(
                mapping.target_path
            )
            if (
                isinstance(remote, Mapping)
                and remote.get("size") == state.output_bytes
            ):
                return state
            raise DiscExpansionError(
                f"已完成的展开目标在远端缺失或大小不符: {mapping.target_path}"
            )
        if state is None:
            state = ExpansionTransferState.from_mapping(mapping)
        candidate = mapping.candidate
        buffer_path = self._local_buffer_path(mapping)
        if os.path.exists(buffer_path):
            os.remove(buffer_path)
        self._require_free_buffer_space(candidate.clip_size)
        try:
            evidence: RemuxEvidence | None = None
            remote_precheck = self._stat_remote_with_second_opinion(
                mapping.target_path
            )
            if remote_precheck is not None:
                # The target already exists: either our own committed upload
                # from an attempt whose state was never saved, or a foreign
                # object.  A saved state (uploading/uploaded) carries the
                # remux evidence; without one, the deterministic remux
                # recomputes it.  The size gate decides — a match adopts our
                # object, a mismatch is a hard refusal that never overwrites.
                if state.output_bytes <= 0:
                    with self._reader_opener(
                        self.alist,
                        image_path=candidate.image_path,
                        image_size=candidate.image_size,
                    ) as read_range:
                        evidence = self._remux(
                            read_range,
                            inner_file,
                            image_size=candidate.image_size,
                            output_path=buffer_path,
                            chunk_bytes=self.chunk_bytes,
                            expected_duration_seconds=candidate.duration_seconds,
                        )
                    expected_bytes = evidence.output_bytes
                else:
                    expected_bytes = state.output_bytes
                if remote_precheck.get("size") != expected_bytes:
                    raise DiscExpansionError(
                        "展开 staging 目标已存在且大小与确定性 remux 证据不符，"
                        f"拒绝覆盖: {mapping.target_path}"
                        f"（远端 {remote_precheck.get('size')}"
                        f" != 期望 {expected_bytes}）"
                    )
                if evidence is not None:
                    state.inner_size = candidate.clip_size
                    state.duration_seconds = evidence.duration_seconds
                    state.output_bytes = evidence.output_bytes
                    state.md5 = evidence.md5
                    state.sha1 = evidence.sha1
                state.status = "uploaded"
                state.updated_at = self._now()
                self._save_state(state)
            elif state.status != "uploaded":
                with self._reader_opener(
                    self.alist,
                    image_path=candidate.image_path,
                    image_size=candidate.image_size,
                ) as read_range:
                    evidence = self._remux(
                        read_range,
                        inner_file,
                        image_size=candidate.image_size,
                        output_path=buffer_path,
                        chunk_bytes=self.chunk_bytes,
                        expected_duration_seconds=candidate.duration_seconds,
                    )
                # Persist the evidence before the first byte leaves: a crash
                # mid-upload leaves the deterministic proof on disk, so the
                # next pass can reconcile a committed target by size alone.
                state.inner_size = candidate.clip_size
                state.duration_seconds = evidence.duration_seconds
                state.output_bytes = evidence.output_bytes
                state.md5 = evidence.md5
                state.sha1 = evidence.sha1
                state.status = "uploading"
                state.updated_at = self._now()
                self._save_state(state)

                # A part-level transport blip inside the provider proxy can
                # reject an otherwise complete upload (observed on quark_uc: a
                # broken pipe on one part was retried by the proxy with an
                # already-drained reader, so the part arrived empty and the
                # provider answered 400 EntityTooSmall), and a transport break
                # after commit can fail the request while the object lands
                # anyway.  The remux buffer is still intact here, so reconcile
                # the target and retry once when nothing committed.
                def chunks():
                    with open(buffer_path, "rb") as handle:
                        while True:
                            block = handle.read(self.chunk_bytes)
                            if not block:
                                break
                            yield block

                uploaded = False
                last_error: Exception | None = None
                for _attempt in range(2):
                    try:
                        self._upload_stream(
                            mapping.target_path,
                            chunks(),
                            size=evidence.output_bytes,
                            md5=evidence.md5,
                            sha1=evidence.sha1,
                        )
                        uploaded = True
                        break
                    except Exception as exc:  # noqa: BLE001 - reconciled below
                        last_error = exc
                        reconciled = self._stat_remote_with_lag(
                            mapping.target_path
                        )
                        if (
                            isinstance(reconciled, Mapping)
                            and reconciled.get("size") == evidence.output_bytes
                        ):
                            uploaded = True
                            break
                if last_error is not None and not uploaded:
                    raise last_error
                state.status = "uploaded"
                state.updated_at = self._now()
                self._save_state(state)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.remove(buffer_path)
        remote = self._stat_remote_with_lag(mapping.target_path)
        if (
            not isinstance(remote, Mapping)
            or remote.get("size") != state.output_bytes
        ):
            raise DiscExpansionError(
                f"展开上传后回读失败: {mapping.target_path}"
            )
        state.status = "completed"
        state.updated_at = self._now()
        self._save_state(state)
        return state


def _state_key_from_state(state: ExpansionTransferState) -> str:
    stem = posixpath.splitext(posixpath.basename(state.playlist_inner_path))[0]
    image_stem = posixpath.splitext(posixpath.basename(state.image_path))[0]
    return f"{image_stem}__{stem}__S{state.season:02d}E{state.episode:02d}.json"


__all__ = [
    "DiscExpansionError",
    "DiscExpansionExecutor",
    "EpisodeMapping",
    "ExpansionTransferState",
    "PlaylistCandidate",
    "RemuxEvidence",
    "ScopeExpansionPlan",
    "SeasonEpisodeRoster",
    "collect_playlist_candidates",
    "derive_scope_expansion",
    "disc_ordinal_from_image_name",
    "remux_inner_file_to_matroska",
]
