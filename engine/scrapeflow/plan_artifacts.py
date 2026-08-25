"""Pure planned-artwork and NFO sidecar derivation.

This module owns the deterministic, in-memory part of the Engine's artwork
and sidecar pipeline.  It does not open an AList/TMDB client, inspect a remote
directory, or perform a remote operation. The ``engine.scraper`` runtime
supplies path and policy helpers dynamically so callers that override it
continue to observe those overrides.

The functions return the runtime's tuples and UTF-8 XML bytes. Uploading these
values remains an explicit local transaction owned by the runtime; deriving
them here is read-only.
"""

from __future__ import annotations

import html
import re
import unicodedata
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Mapping

from .errors import PlanError
from .media_naming import is_planned_bonus
from .models import Plan
from .remote_paths import join_remote, normalize_remote_path, split_remote


_RUNTIME: ModuleType | None = None


def _fallback_collision_key(value: str) -> str:
    """Standalone equivalent of the facade's conservative collision key."""
    normalized = unicodedata.normalize("NFC", str(value)).casefold()
    if "/" in normalized:
        return "/".join(part.rstrip(" .") for part in normalized.split("/"))
    return normalized.rstrip(" .")


def _fallback_path_is_within(path: str, root: str) -> bool:
    path_key = _fallback_collision_key(normalize_remote_path(path).rstrip("/") or "/")
    root_key = _fallback_collision_key(normalize_remote_path(root).rstrip("/") or "/")
    return path_key == root_key or path_key.startswith(root_key.rstrip("/") + "/")


def _runtime_value(name: str, default: Any) -> Any:
    runtime = _RUNTIME
    return getattr(runtime, name, default) if runtime is not None else default


def _plan_error(message: str) -> Exception:
    error_type = _runtime_value("PlanError", PlanError)
    return error_type(message)


def _planned_artwork_impl(
    plan: Plan,
    *,
    join_remote_fn: Callable[[str, str], str],
    collision_key_fn: Callable[[str], str],
    normalize_remote_path_fn: Callable[[str], str],
    is_planned_bonus_fn: Callable[[str], bool],
) -> list[tuple[str, str, str]]:
    """Return ``(target, TMDB image path, purpose)`` requests.

    This is intentionally a pure projection.  It only reads the already
    accepted plan metadata/files and de-duplicates target paths by the Engine
    collision policy.
    """
    requests: list[tuple[str, str, str]] = []
    # A directory-only series container has no TMDB identity of its own, but
    # the media-library contract still requires the container folder to be a
    # visible item with its own artwork.  The root artifact carrier supplies
    # these already-approved image paths; they are deliberately kept
    # separate from the child work identities so a container is never
    # mistaken for another TMDB work.
    metadata = plan.metadata if isinstance(plan.metadata, Mapping) else {}
    container_root = metadata.get("container_root")
    if isinstance(container_root, str) and container_root:
        container_poster = metadata.get("container_poster_path")
        container_backdrop = metadata.get("container_backdrop_path")
        if isinstance(container_poster, str) and container_poster:
            requests.extend(
                [
                    (join_remote_fn(container_root, "folder.jpg"), container_poster, "container-folder"),
                    (join_remote_fn(container_root, "poster.jpg"), container_poster, "container-poster"),
                ]
            )
        if isinstance(container_backdrop, str) and container_backdrop:
            requests.append(
                (join_remote_fn(container_root, "fanart.jpg"), container_backdrop, "container-fanart")
            )
    primary_root = (
        str(plan.metadata.get("series_root"))
        if plan.mode == "mixed" and plan.metadata.get("series_root")
        else plan.target_root
    )
    poster_path = plan.metadata.get("poster_path")
    backdrop_path = plan.metadata.get("backdrop_path")
    if isinstance(poster_path, str) and poster_path:
        requests.append((join_remote_fn(primary_root, "folder.jpg"), poster_path, "folder"))
        if plan.mode in {"tv", "mixed"}:
            requests.append((join_remote_fn(primary_root, "poster.jpg"), poster_path, "series-poster"))
        elif plan.mode == "movie":
            for item in plan.files:
                if item.media_kind != "video" or is_planned_bonus_fn(item.final_name):
                    continue
                requests.append(
                    (
                        join_remote_fn(item.target_dir, f"{Path(item.final_name).stem}.jpg"),
                        poster_path,
                        "movie-poster",
                    )
                )
    if isinstance(backdrop_path, str) and backdrop_path:
        requests.append((join_remote_fn(primary_root, "fanart.jpg"), backdrop_path, "fanart"))
    season_posters = plan.metadata.get("season_posters")
    if plan.mode in {"tv", "mixed"} and isinstance(season_posters, Mapping):
        for season_number, image_path in season_posters.items():
            if isinstance(image_path, str) and image_path:
                requests.append(
                    (
                        join_remote_fn(primary_root, f"season {int(season_number)}-poster.jpg"),
                        image_path,
                        "season-poster",
                    )
                )
    member_posters = plan.metadata.get("member_posters")
    if plan.mode in {"collection", "mixed", "batch"} and isinstance(member_posters, Mapping):
        planned_videos = {
            collision_key_fn(join_remote_fn(item.target_dir, item.final_name)): item
            for item in plan.files
            if item.media_kind == "video" and not is_planned_bonus_fn(item.final_name)
        }
        for target, image_path in member_posters.items():
            if not isinstance(target, str) or not isinstance(image_path, str) or not image_path:
                continue
            normalized_target = normalize_remote_path_fn(target)
            flat_video = planned_videos.get(collision_key_fn(normalized_target))
            if flat_video is not None:
                requests.append(
                    (
                        join_remote_fn(
                            flat_video.target_dir,
                            f"{Path(flat_video.final_name).stem}.jpg",
                        ),
                        image_path,
                        "member-movie-poster",
                    )
                )
                continue
            requests.append((join_remote_fn(normalized_target, "folder.jpg"), image_path, "member-folder"))
            for item in plan.files:
                if item.target_dir == normalized_target and item.media_kind == "video":
                    requests.append(
                        (
                            join_remote_fn(normalized_target, f"{Path(item.final_name).stem}.jpg"),
                            image_path,
                            "member-movie-poster",
                        )
                    )
    member_tv = plan.metadata.get("member_tv")
    if plan.mode == "batch" and isinstance(member_tv, Mapping):
        # A franchise root is rendered as a shelf item by Infuse.  Batch plans
        # have no single TMDB identity, so use the shallowest deterministic TV
        # member as representative artwork; member artwork remains authoritative
        # for every actual show/movie.
        representative_tv: Mapping[str, Any] | None = None
        for series_root, identity in sorted(
            member_tv.items(),
            key=lambda pair: (
                normalize_remote_path_fn(str(pair[0])).count("/"),
                collision_key_fn(str(pair[0])),
            ),
        ):
            if not isinstance(series_root, str) or not isinstance(identity, Mapping):
                continue
            if isinstance(identity.get("poster_path"), str) and identity.get("poster_path"):
                representative_tv = identity
                break
        if representative_tv is not None:
            representative_poster = str(representative_tv["poster_path"])
            requests.extend(
                [
                    (join_remote_fn(plan.target_root, "folder.jpg"), representative_poster, "batch-folder"),
                    (join_remote_fn(plan.target_root, "poster.jpg"), representative_poster, "batch-poster"),
                ]
            )
            representative_backdrop = representative_tv.get("backdrop_path")
            if isinstance(representative_backdrop, str) and representative_backdrop:
                requests.append(
                    (join_remote_fn(plan.target_root, "fanart.jpg"), representative_backdrop, "batch-fanart")
                )
        for series_root, identity in member_tv.items():
            if not isinstance(series_root, str) or not isinstance(identity, Mapping):
                continue
            member_poster = identity.get("poster_path")
            member_backdrop = identity.get("backdrop_path")
            if isinstance(member_poster, str) and member_poster:
                requests.extend(
                    [
                        (join_remote_fn(series_root, "folder.jpg"), member_poster, "folder"),
                        (join_remote_fn(series_root, "poster.jpg"), member_poster, "series-poster"),
                    ]
                )
            if isinstance(member_backdrop, str) and member_backdrop:
                requests.append(
                    (join_remote_fn(series_root, "fanart.jpg"), member_backdrop, "fanart")
                )
            raw_seasons = identity.get("season_posters")
            if isinstance(raw_seasons, Mapping):
                for season_number, image_path in raw_seasons.items():
                    if isinstance(image_path, str) and image_path:
                        requests.append(
                            (
                                join_remote_fn(series_root, f"season {int(season_number)}-poster.jpg"),
                                image_path,
                                "season-poster",
                            )
                        )
    deduplicated: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for request in requests:
        key = collision_key_fn(request[0])
        if key not in seen:
            seen.add(key)
            deduplicated.append(request)
    return deduplicated


def _planned_movie_nfos_impl(
    plan: Plan,
    *,
    join_remote_fn: Callable[[str, str], str],
    normalize_remote_path_fn: Callable[[str], str],
    is_planned_bonus_fn: Callable[[str], bool],
) -> list[tuple[str, bytes]]:
    if plan.mode not in {"movie", "collection", "mixed", "batch"}:
        return []
    member_movies = plan.metadata.get("member_movies")
    member_movies = member_movies if isinstance(member_movies, Mapping) else {}
    output: list[tuple[str, bytes]] = []
    for item in plan.files:
        if item.media_kind != "video" or is_planned_bonus_fn(item.final_name):
            continue
        stem = Path(item.final_name).stem
        target_path = join_remote_fn(item.target_dir, item.final_name)
        identity = (
            plan.metadata
            if plan.mode == "movie"
            else (
                member_movies.get(normalize_remote_path_fn(target_path))
                or member_movies.get(normalize_remote_path_fn(item.target_dir))
            )
        )
        if isinstance(identity, Mapping):
            raw_tmdb_id = identity.get("tmdb_id")
            raw_title = identity.get("title")
            raw_year = identity.get("year")
            if (
                isinstance(raw_tmdb_id, int)
                and not isinstance(raw_tmdb_id, bool)
                and raw_tmdb_id > 0
                and isinstance(raw_title, str)
                and raw_title
                and isinstance(raw_year, str)
            ):
                tmdb_id = str(raw_tmdb_id)
                title = raw_title
                year = raw_year
            else:
                identity = None
        if not isinstance(identity, Mapping):
            # Current plans carry an explicit identity for every media file.
            # A filename marker is not independent evidence and must never
            # manufacture metadata for a plan that was not identity-resolved.
            continue
        payload = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            "<movie>\n"
            f"  <title>{html.escape(title)}</title>\n"
            f"  <year>{html.escape(year)}</year>\n"
            f"  <tmdbid>{tmdb_id}</tmdbid>\n"
            f'  <uniqueid type="tmdb" default="true">{tmdb_id}</uniqueid>\n'
            "</movie>\n"
        ).encode("utf-8")
        output.append((join_remote_fn(item.target_dir, f"{stem}.nfo"), payload))
    return output


def _planned_tv_nfos_impl(
    plan: Plan,
    *,
    join_remote_fn: Callable[[str, str], str],
) -> list[tuple[str, bytes]]:
    metadata = plan.metadata if isinstance(plan.metadata, Mapping) else {}
    container_root = metadata.get("container_root")
    container_title = metadata.get("container_title")
    container_kind = metadata.get("container_nfo_kind", "tvshow")
    if (
        isinstance(container_root, str)
        and container_root
        and isinstance(container_title, str)
        and container_title
        and container_kind == "tvshow"
    ):
        # Do not attach a TMDB id to a directory-only container.  It is a
        # grouping item, not a second identity; the child work NFOs remain
        # authoritative for identity and season metadata.
        payload = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            "<!-- ScrapeFlow container metadata; no TMDB identity -->\n"
            "<tvshow>\n"
            f"  <title>{html.escape(container_title)}</title>\n"
            "  <type>collection</type>\n"
            "</tvshow>\n"
        ).encode("utf-8")
        if plan.mode == "container":
            return [(join_remote_fn(container_root, "tvshow.nfo"), payload)]
        # A future mixed/batch carrier may carry both this root marker and
        # ordinary child NFOs.  Keep the root first so deterministic writers
        # preserve it if a target spelling collides.
        output = [(join_remote_fn(container_root, "tvshow.nfo"), payload)]
    else:
        output = []
    if plan.mode == "batch":
        members = plan.metadata.get("member_tv")
        if not isinstance(members, Mapping):
            return output
        for series_root, identity in members.items():
            if not isinstance(series_root, str) or not isinstance(identity, Mapping):
                continue
            tmdb_id = identity.get("tmdb_id")
            title = identity.get("title")
            year = identity.get("year")
            if (
                isinstance(tmdb_id, bool)
                or not isinstance(tmdb_id, int)
                or tmdb_id <= 0
                or not isinstance(title, str)
                or not title
                or not isinstance(year, str)
            ):
                continue
            payload = (
                '<?xml version="1.0" encoding="UTF-8"?>\n'
                "<tvshow>\n"
                f"  <title>{html.escape(title)}</title>\n"
                f"  <year>{html.escape(year)}</year>\n"
                f"  <tmdbid>{tmdb_id}</tmdbid>\n"
                f'  <uniqueid type="tmdb" default="true">{tmdb_id}</uniqueid>\n'
                "</tvshow>\n"
            ).encode("utf-8")
            output.append((join_remote_fn(series_root, "tvshow.nfo"), payload))
        return output
    if plan.mode not in {"tv", "mixed"}:
        return []
    tmdb_id = plan.metadata.get("tmdb_id")
    title = plan.metadata.get("title")
    year = plan.metadata.get("year")
    if (
        isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
        or not isinstance(title, str)
        or not title
        or not isinstance(year, str)
    ):
        return []
    series_root = (
        str(plan.metadata.get("series_root"))
        if plan.mode == "mixed" and plan.metadata.get("series_root")
        else plan.target_root
    )
    payload = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<tvshow>\n"
        f"  <title>{html.escape(title)}</title>\n"
        f"  <year>{html.escape(year)}</year>\n"
        f"  <tmdbid>{tmdb_id}</tmdbid>\n"
        f'  <uniqueid type="tmdb" default="true">{tmdb_id}</uniqueid>\n'
        "</tvshow>\n"
    ).encode("utf-8")
    return [*output, (join_remote_fn(series_root, "tvshow.nfo"), payload)]


def _planned_tv_episode_nfos_impl(
    plan: Plan,
    *,
    collision_key_fn: Callable[[str], str],
    join_remote_fn: Callable[[str, str], str],
    normalize_remote_path_fn: Callable[[str], str],
    path_is_within_fn: Callable[[str, str], bool],
    split_remote_fn: Callable[[str], tuple[str, str]],
    plan_error: Callable[[str], Exception],
    is_planned_bonus_fn: Callable[[str], bool],
) -> list[tuple[str, bytes]]:
    """Generate one deterministic episode sidecar for every planned TV video.

    The plan currently carries no authoritative TMDB episode object ID, so the
    sidecar deliberately does not invent one.  Season/episode/range, canonical
    show identity, title and year are still sufficient for a correct local NFO.
    """
    identities: list[tuple[str, Mapping[str, Any]]] = []
    if plan.mode == "batch":
        members = plan.metadata.get("member_tv")
        if isinstance(members, Mapping):
            identities.extend(
                (normalize_remote_path_fn(root), identity)
                for root, identity in members.items()
                if isinstance(root, str) and isinstance(identity, Mapping)
            )
    elif plan.mode in {"tv", "mixed"}:
        root = (
            str(plan.metadata.get("series_root"))
            if plan.mode == "mixed" and plan.metadata.get("series_root")
            else plan.target_root
        )
        identities.append((normalize_remote_path_fn(root), plan.metadata))
    if not identities:
        return []

    output: list[tuple[str, bytes]] = []
    seen: set[str] = set()
    for item in plan.files:
        if item.media_kind != "video" or is_planned_bonus_fn(item.final_name):
            continue
        target_dir = normalize_remote_path_fn(item.target_dir)
        matches = [
            pair for pair in identities
            if path_is_within_fn(target_dir, pair[0])
        ]
        if not matches:
            continue
        series_root, identity = max(matches, key=lambda pair: len(pair[0]))
        token = re.search(
            r"(?:^|[ ._-])S0*(\d{1,3})E0*(\d{1,4})(?:-E0*(\d{1,4}))?(?:$|[ ._-])",
            Path(item.final_name).stem,
            re.IGNORECASE,
        )
        if token is None:
            continue
        season = int(token.group(1))
        episode = int(token.group(2))
        end_episode = int(token.group(3)) if token.group(3) else episode
        show_title = str(identity.get("title") or split_remote_fn(series_root)[1]).strip()
        year = str(identity.get("year") or "").strip()
        stem = Path(item.final_name).stem
        title_tail = re.split(
            r"\s+-\s+S\d{2,3}E\d{2,4}(?:-E\d{2,4})?\s+-\s+",
            stem,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        episode_title = title_tail[1].strip() if len(title_tail) == 2 else stem
        target = join_remote_fn(item.target_dir, f"{stem}.nfo")
        key = collision_key_fn(target)
        if key in seen:
            raise plan_error(f"多个视频生成同一集 NFO 目标: {target}")
        seen.add(key)
        range_fields = (
            f"  <displayepisode>{episode}-{end_episode}</displayepisode>\n"
            if end_episode != episode else ""
        )
        payload = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            "<episodedetails>\n"
            f"  <title>{html.escape(episode_title)}</title>\n"
            f"  <showtitle>{html.escape(show_title)}</showtitle>\n"
            f"  <year>{html.escape(year)}</year>\n"
            f"  <season>{season}</season>\n"
            f"  <episode>{episode}</episode>\n"
            f"{range_fields}"
            "</episodedetails>\n"
        ).encode("utf-8")
        output.append((target, payload))
    return output


def _planned_nfos_impl(
    plan: Plan,
    *,
    planned_tv_nfos_fn: Callable[[Plan], list[tuple[str, bytes]]],
    planned_tv_episode_nfos_fn: Callable[[Plan], list[tuple[str, bytes]]],
    planned_movie_nfos_fn: Callable[[Plan], list[tuple[str, bytes]]],
) -> list[tuple[str, bytes]]:
    return [
        *planned_tv_nfos_fn(plan),
        *planned_tv_episode_nfos_fn(plan),
        *planned_movie_nfos_fn(plan),
    ]


def planned_artwork(plan: Plan) -> list[tuple[str, str, str]]:
    return _planned_artwork_impl(
        plan,
        join_remote_fn=join_remote,
        collision_key_fn=_fallback_collision_key,
        normalize_remote_path_fn=normalize_remote_path,
        is_planned_bonus_fn=is_planned_bonus,
    )


def planned_movie_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    return _planned_movie_nfos_impl(
        plan,
        join_remote_fn=join_remote,
        normalize_remote_path_fn=normalize_remote_path,
        is_planned_bonus_fn=is_planned_bonus,
    )


def planned_tv_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    return _planned_tv_nfos_impl(plan, join_remote_fn=join_remote)


def planned_tv_episode_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    return _planned_tv_episode_nfos_impl(
        plan,
        collision_key_fn=_fallback_collision_key,
        join_remote_fn=join_remote,
        normalize_remote_path_fn=normalize_remote_path,
        path_is_within_fn=_fallback_path_is_within,
        split_remote_fn=split_remote,
        plan_error=_plan_error,
        is_planned_bonus_fn=is_planned_bonus,
    )


def planned_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    return _planned_nfos_impl(
        plan,
        planned_tv_nfos_fn=planned_tv_nfos,
        planned_tv_episode_nfos_fn=planned_tv_episode_nfos,
        planned_movie_nfos_fn=planned_movie_nfos,
    )


__all__ = [
    "bind_compat_runtime",
    "planned_artwork",
    "planned_movie_nfos",
    "planned_nfos",
    "planned_tv_episode_nfos",
    "planned_tv_nfos",
]


_COMPAT_RUNTIME: ModuleType | None = None
_COMPAT_IMPLEMENTATIONS = {name: globals()[name] for name in __all__ if name != "bind_compat_runtime"}


def _compat_dispatch(name: str):
    original = _COMPAT_IMPLEMENTATIONS[name]

    def dispatch(*args: Any, **kwargs: Any) -> Any:
        runtime = _COMPAT_RUNTIME
        current = getattr(runtime, name, original) if runtime is not None else original
        if current is not original and current is not dispatch:
            return current(*args, **kwargs)
        return original(*args, **kwargs)

    dispatch.__name__ = name
    dispatch.__qualname__ = name
    dispatch.__doc__ = original.__doc__
    dispatch.__plan_artifacts_runtime_dispatch__ = True
    return dispatch


def bind_compat_runtime(runtime: ModuleType) -> None:
    """Preserve runtime overrides for direct module callers."""
    global _COMPAT_RUNTIME
    _COMPAT_RUNTIME = runtime
    for name in _COMPAT_IMPLEMENTATIONS:
        current = globals().get(name)
        if current is _COMPAT_IMPLEMENTATIONS[name]:
            globals()[name] = _compat_dispatch(name)
