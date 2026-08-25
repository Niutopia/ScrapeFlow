"""TMDB episode coordinates for the currently selected RootJob.

The catalog is a tiny read-through cache, not an audit service: callers ask
for one TV identity while planning or closing gaps for their current task.
"""

from __future__ import annotations

import copy
import re
from datetime import date
from collections.abc import Callable, Mapping, Sequence


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdecimal() and int(value) > 0:
        return int(value)
    return None


def _metadata(work: Mapping[str, object]) -> dict[str, object]:
    """Flatten the normal WorkUnit identity shape without accepting paths."""
    result: dict[str, object] = {}
    nested = work.get("metadata")
    if isinstance(nested, Mapping):
        result.update(nested)
    identity = work.get("identity")
    if isinstance(identity, Mapping):
        result.update(identity)
    result.update({
        key: value for key, value in work.items()
        if key not in {"metadata", "identity"}
    })
    return result


class TmdbEpisodeCatalog:
    """Fetch published season/episode coordinates for one TV identity.

    Failed or malformed TMDB responses cache as ``None`` for the lifetime of
    this short-lived object.  That makes the caller fail closed without a
    second background worker, audit budget, or retry queue.
    """

    def __init__(
        self,
        client: object | None,
        *,
        today: Callable[[], date] | None = None,
    ) -> None:
        self.client = client
        self.today = today or date.today
        self._cache: dict[
            tuple[int, date], dict[int, list[dict[str, object]]] | None
        ] = {}

    @staticmethod
    def _published(value: object, current_day: date) -> bool:
        if not isinstance(value, str) or _DATE_RE.fullmatch(value) is None:
            return False
        try:
            return date.fromisoformat(value) <= current_day
        except ValueError:
            return False

    def _fetch(
        self,
        tmdb_id: int,
        current_day: date,
    ) -> dict[int, list[dict[str, object]]] | None:
        getter = getattr(self.client, "get", None)
        if not callable(getter):
            return None
        try:
            show = getter(f"/tv/{tmdb_id}")
        except Exception:
            return None
        if not isinstance(show, Mapping):
            return None
        seasons = show.get("seasons")
        if not isinstance(seasons, list):
            return None
        output: dict[int, list[dict[str, object]]] = {}
        for raw_season in seasons:
            if not isinstance(raw_season, Mapping):
                continue
            season = raw_season.get("season_number")
            if (
                isinstance(season, bool)
                or not isinstance(season, int)
                or season < 0
            ):
                continue
            try:
                payload = getter(f"/tv/{tmdb_id}/season/{season}")
            except Exception:
                # A partial series response cannot prove which coordinates
                # are missing, so do not turn it into partial gap evidence.
                return None
            episodes = payload.get("episodes") if isinstance(payload, Mapping) else None
            if not isinstance(episodes, list):
                return None
            rows: list[dict[str, object]] = []
            for episode in episodes:
                if not isinstance(episode, Mapping):
                    continue
                number = episode.get("episode_number")
                if (
                    isinstance(number, bool)
                    or not isinstance(number, int)
                    or number <= 0
                    or not self._published(episode.get("air_date"), current_day)
                ):
                    continue
                row: dict[str, object] = {
                    "season_number": season,
                    "episode_number": number,
                    "name": str(episode.get("name") or "").strip(),
                    "air_date": str(episode.get("air_date") or ""),
                }
                aliases = episode.get("title_aliases")
                if isinstance(aliases, list):
                    values = [
                        value.strip() for value in aliases
                        if isinstance(value, str) and value.strip()
                    ][:8]
                    if values:
                        row["title_aliases"] = values
                rows.append(row)
            if rows:
                output[season] = rows
        return output

    def __call__(
        self,
        work: Mapping[str, object],
    ) -> dict[int, list[dict[str, object]]] | None:
        metadata = _metadata(work)
        if str(metadata.get("media_type") or metadata.get("type") or "").casefold() == "movie":
            return {}
        tmdb_id = _positive_int(metadata.get("tmdb_id"))
        if tmdb_id is None:
            return None
        current_day = self.today()
        key = (tmdb_id, current_day)
        if key not in self._cache:
            self._cache[key] = self._fetch(tmdb_id, current_day)
        return copy.deepcopy(self._cache[key])

    def prefetch(
        self,
        works: Sequence[Mapping[str, object]],
        *,
        max_workers: int | None = None,
    ) -> None:
        """Warm requested identities sequentially for compatibility.

        ``max_workers`` is intentionally ignored: normal local operation has
        one worker and no independently configurable TMDB audit pool.
        """
        del max_workers
        for work in works:
            if isinstance(work, Mapping):
                self(work)


__all__ = ["TmdbEpisodeCatalog"]
