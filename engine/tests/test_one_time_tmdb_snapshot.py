from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from engine.scrapeflow.one_time_tmdb_snapshot import (
    SealedMovieSnapshotClient,
    seal_movie_snapshot,
    validate_movie_snapshot,
)


class OneTimeTMDBSnapshotTests(unittest.TestCase):
    now = datetime(2026, 8, 3, 4, 0, tzinfo=timezone.utc)

    def snapshot(self) -> dict:
        return seal_movie_snapshot(
            {"id": 99, "title": "Movie A", "release_date": "2026-01-02"},
            fetched_at=(self.now - timedelta(minutes=5)).isoformat(),
        )

    def test_valid_snapshot_returns_isolated_payload(self):
        value = self.snapshot()
        payload = validate_movie_snapshot(value, expected_tmdb_id=99, now=self.now)
        self.assertEqual(payload["id"], 99)
        payload["title"] = "changed"
        self.assertEqual(value["payload"]["title"], "Movie A")

    def test_tamper_wrong_id_and_expiry_fail_closed(self):
        tampered = self.snapshot()
        tampered["payload"]["title"] = "changed"
        with self.assertRaisesRegex(ValueError, "payload digest"):
            validate_movie_snapshot(tampered, expected_tmdb_id=99, now=self.now)
        with self.assertRaisesRegex(ValueError, "ID"):
            validate_movie_snapshot(self.snapshot(), expected_tmdb_id=100, now=self.now)
        with self.assertRaisesRegex(ValueError, "过期"):
            validate_movie_snapshot(
                self.snapshot(), expected_tmdb_id=99,
                now=self.now + timedelta(hours=2),
            )

    def test_client_only_accepts_exact_movie_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fresh = seal_movie_snapshot(
                {"id": 99, "title": "Movie A", "release_date": "2026-01-02"},
                fetched_at=datetime.now(timezone.utc).isoformat(),
            )
            (root / "movie-99.json").write_text(json.dumps(fresh), encoding="utf-8")
            client = SealedMovieSnapshotClient(root)
            self.assertEqual(client.get("/movie/99")["id"], 99)
            with self.assertRaisesRegex(ValueError, "只允许"):
                client.get("/tv/99")


if __name__ == "__main__":
    unittest.main()
