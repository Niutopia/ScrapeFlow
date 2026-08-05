"""Short-lived official TMDB reads for the one-time movie audit only."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Mapping

from engine.scrapeflow.one_time_library_completion import canonical_digest


SNAPSHOT_KIND = "one_time_official_tmdb_movie_snapshot"


def seal_movie_snapshot(payload: Mapping[str, Any], *, fetched_at: str) -> dict[str, Any]:
    tmdb_id = payload.get("id")
    if type(tmdb_id) is not int or tmdb_id <= 0:
        raise ValueError("TMDB 电影快照缺少有效 ID")
    core = {
        "schema_version": 1,
        "kind": SNAPSHOT_KIND,
        "media_type": "movie",
        "tmdb_id": tmdb_id,
        "fetched_at": fetched_at,
        "source": "https://api.themoviedb.org/3",
        "payload": deepcopy(dict(payload)),
        "payload_sha256": canonical_digest(payload),
    }
    # Validate the timestamp before allowing the record to be persisted.
    datetime.fromisoformat(fetched_at)
    return {**core, "snapshot_sha256": canonical_digest(core)}


def validate_movie_snapshot(
    value: Mapping[str, Any], *, expected_tmdb_id: int,
    now: datetime | None = None, max_age_seconds: int = 3600,
) -> dict[str, Any]:
    if value.get("kind") != SNAPSHOT_KIND or value.get("media_type") != "movie":
        raise ValueError("TMDB 电影快照类型无效")
    if value.get("tmdb_id") != expected_tmdb_id:
        raise ValueError("TMDB 电影快照 ID 与作品不一致")
    payload = value.get("payload")
    if not isinstance(payload, Mapping) or payload.get("id") != expected_tmdb_id:
        raise ValueError("TMDB 电影快照 payload 身份不一致")
    if value.get("payload_sha256") != canonical_digest(payload):
        raise ValueError("TMDB 电影快照 payload digest 无效")
    snapshot_digest = value.get("snapshot_sha256")
    if not isinstance(snapshot_digest, str) or re.fullmatch(r"[0-9a-f]{64}", snapshot_digest) is None:
        raise ValueError("TMDB 电影快照 digest 格式无效")
    core = {key: item for key, item in value.items() if key != "snapshot_sha256"}
    if snapshot_digest != canonical_digest(core):
        raise ValueError("TMDB 电影快照已被改动")
    try:
        fetched = datetime.fromisoformat(str(value.get("fetched_at")))
    except ValueError as exc:
        raise ValueError("TMDB 电影快照时间无效") from exc
    if fetched.tzinfo is None:
        raise ValueError("TMDB 电影快照时间缺少时区")
    current = now or datetime.now(timezone.utc)
    age = (current - fetched).total_seconds()
    if age < 0 or age > max_age_seconds:
        raise ValueError("TMDB 电影快照已过期")
    return deepcopy(dict(payload))


class SealedMovieSnapshotClient:
    """TMDB-compatible reader restricted to sealed `/movie/<id>` snapshots."""

    def __init__(self, root: Path, *, max_age_seconds: int = 3600) -> None:
        self.root = root
        self.max_age_seconds = max_age_seconds

    def get(self, path: str, **_params: Any) -> dict[str, Any]:
        match = re.fullmatch(r"/movie/([1-9][0-9]*)", path)
        if match is None:
            raise ValueError("一次性 TMDB 快照只允许读取精确电影 ID")
        tmdb_id = int(match.group(1))
        value = json.loads((self.root / f"movie-{tmdb_id}.json").read_text(encoding="utf-8"))
        if not isinstance(value, Mapping):
            raise ValueError("TMDB 电影快照必须是对象")
        return validate_movie_snapshot(
            value, expected_tmdb_id=tmdb_id, max_age_seconds=self.max_age_seconds,
        )
