"""Tests for the P14 gap-ledger -> replenishment-request bridge."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.gap_ledger import Gap, save_gap_ledger
from engine.scrapeflow.work_units import WorkUnitRecord, save_work_unit_records

from local.scrapeflow_api.replenishment_bridge import (
    gap_ledger_requests,
    gap_ledger_selection,
)


def _work_unit(
    root_task_id: str,
    work_unit_id: str,
    *,
    media_type: str,
    tmdb_id: int,
    title: str,
) -> WorkUnitRecord:
    return WorkUnitRecord(
        work_unit_id=work_unit_id,
        root_task_id=root_task_id,
        boundary_key=work_unit_id,
        source_paths=(f"/待刮削/{work_unit_id}",),
        source_revision=1,
        role="single_work",
        media_context="tv" if media_type == "tv" else "movie",
        identity_status="confirmed",
        identity={
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "title": title,
            "year": "2011",
        },
    )


def _episode_gap(
    root_task_id: str,
    work_unit_id: str,
    *,
    media_type: str,
    tmdb_id: int,
    season: int,
    episode: int,
    status: str = "open",
) -> Gap:
    token = f"S{season:02d}E{episode:02d}"
    return Gap(
        gap_id=f"{work_unit_id}::missing_episode::{token}",
        root_task_id=root_task_id,
        work_unit_id=work_unit_id,
        kind="missing_episode",
        media_type=media_type,
        tmdb_id=tmdb_id,
        season=season,
        episodes=(episode,),
        subtitle_path=None,
        subtitle_language=None,
        status=status,
    )


def _seed_state(state_root: Path, root_task_id: str = "root-1") -> None:
    """Seed a gap ledger and a work-unit ledger with two identities."""
    save_work_unit_records(state_root, root_task_id, [
        _work_unit(
            root_task_id, "unit-tv",
            media_type="tv", tmdb_id=35507, title="Fate/Zero",
        ),
        _work_unit(
            root_task_id, "unit-movie",
            media_type="movie", tmdb_id=10378, title="The Big Short",
        ),
    ])
    save_gap_ledger(state_root, root_task_id, [
        _episode_gap(
            root_task_id, "unit-tv",
            media_type="tv", tmdb_id=35507, season=1, episode=2,
        ),
        _episode_gap(
            root_task_id, "unit-tv",
            media_type="tv", tmdb_id=35507, season=1, episode=3,
        ),
        _episode_gap(
            root_task_id, "unit-tv",
            media_type="tv", tmdb_id=35507, season=1, episode=4,
            status="closed",
        ),
        Gap(
            gap_id="unit-tv::missing_season::S02",
            root_task_id=root_task_id,
            work_unit_id="unit-tv",
            kind="missing_season",
            media_type="tv",
            tmdb_id=35507,
            season=2,
            episodes=(),
            subtitle_path=None,
            subtitle_language=None,
            status="open",
        ),
        Gap(
            gap_id="unit-tv::missing_subtitle::zh",
            root_task_id=root_task_id,
            work_unit_id="unit-tv",
            kind="missing_subtitle",
            media_type="tv",
            tmdb_id=35507,
            season=None,
            episodes=(),
            subtitle_path="/quark/影视/番剧/Fate Zero/Season 01/S01E01.mkv",
            subtitle_language="zh",
            status="open",
        ),
        Gap(
            gap_id="unit-movie::missing_media::main",
            root_task_id=root_task_id,
            work_unit_id="unit-movie",
            kind="missing_media",
            media_type="movie",
            tmdb_id=10378,
            season=None,
            episodes=(),
            subtitle_path=None,
            subtitle_language=None,
            status="open",
        ),
    ])


class GapLedgerRequestTests(unittest.TestCase):
    def test_groups_open_gaps_by_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            _seed_state(state_root)
            requests = gap_ledger_requests(state_root, "root-1")

            # One request per (media_type, tmdb_id): tv/35507 and movie/10378.
            self.assertEqual(
                {(r["media"]["media_type"], r["media"]["tmdb_id"]) for r in requests},
                {("tv", 35507), ("movie", 10378)},
            )
            by_key = {
                (r["media"]["media_type"], r["media"]["tmdb_id"]): r
                for r in requests
            }

            tv = by_key[("tv", 35507)]
            # Closed S01E04 is excluded; three open tv gaps remain.
            self.assertEqual(
                [row["id"] for row in tv["gaps"]],
                ["S01E02", "S01E03", "S02", "unit-tv::missing_subtitle::zh"],
            )

            movie = by_key[("movie", 10378)]
            self.assertEqual(
                [row["id"] for row in movie["gaps"]],
                ["unit-movie::missing_media::main"],
            )

    def test_request_field_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            _seed_state(state_root)
            requests = gap_ledger_requests(state_root, "root-1")
            tv = next(r for r in requests if r["media"]["tmdb_id"] == 35507)

            self.assertEqual(tv["tier"], "magnet")
            self.assertEqual(tv["media"]["tmdb_id"], 35507)
            self.assertEqual(tv["media"]["media_type"], "tv")
            self.assertEqual(tv["media"]["title"], "Fate/Zero")
            self.assertEqual(tv["media"]["original_title"], "")
            self.assertEqual(tv["media"]["aliases"], ["Fate/Zero"])
            # Durable exclusion memory and legacy rules are the runtime's job.
            self.assertEqual(tv["excluded_candidates"], [])
            self.assertNotIn("rules", tv)

            episode = next(row for row in tv["gaps"] if row["kind"] == "missing_episode")
            self.assertEqual(episode["id"], "S01E02")
            self.assertEqual(episode["season"], 1)
            self.assertEqual(episode["episodes"], [2])
            self.assertEqual(episode["title"], "")

            season = next(row for row in tv["gaps"] if row["kind"] == "missing_season")
            self.assertEqual(season["id"], "S02")
            self.assertEqual(season["season"], 2)
            self.assertEqual(season["episodes"], [])

            subtitle = next(row for row in tv["gaps"] if row["kind"] == "missing_subtitle")
            self.assertEqual(subtitle["id"], "unit-tv::missing_subtitle::zh")
            self.assertEqual(subtitle["subtitle_language"], "zh")
            self.assertEqual(
                subtitle["path"],
                "/quark/影视/番剧/Fate Zero/Season 01/S01E01.mkv",
            )

            movie = next(r for r in requests if r["media"]["tmdb_id"] == 10378)
            self.assertEqual(movie["media"]["media_type"], "movie")
            self.assertEqual(movie["media"]["title"], "The Big Short")
            media_gap = movie["gaps"][0]
            self.assertEqual(media_gap["id"], "unit-movie::missing_media::main")
            self.assertEqual(media_gap["kind"], "missing_media")
            self.assertIsNone(media_gap["season"])
            self.assertEqual(media_gap["title"], "The Big Short")

    def test_empty_ledger_yields_no_requests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self.assertEqual(gap_ledger_requests(state_root, "root-missing"), [])


class GapLedgerSelectionTests(unittest.TestCase):
    def _tv_request(self) -> dict:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            _seed_state(state_root)
            requests = gap_ledger_requests(state_root, "root-1")
            return next(r for r in requests if r["media"]["tmdb_id"] == 35507)

    def test_selection_runs_search_and_select(self) -> None:
        request = self._tv_request()
        seen: list[dict] = []

        def fake_search(req):
            seen.append(dict(req))
            return {
                "candidates": [{
                    "provider": "magnet",
                    "locator": "torrent:https://example.test/fate-s01e02.torrent",
                    "release_name": "[Group] Fate Zero S01E02 1080p",
                    "title": "Fate/Zero",
                    "year": "2011",
                    "files": ["Fate.Zero.S01E02.mkv"],
                    "file_coverage": ["S01E02"],
                    "acquisition": {"kind": "torrent"},
                }],
            }

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            _seed_state(state_root)
            bundle = gap_ledger_selection(
                state_root, "root-1", request, search_runner=fake_search,
            )

        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0]["tier"], "magnet")
        self.assertEqual(bundle["status"], "partial")
        self.assertEqual(bundle["covered_gap_ids"], ["S01E02"])
        self.assertIn("S01E03", bundle["uncovered_gap_ids"])
        self.assertEqual(bundle["selections"][0]["provider"], "magnet")
        self.assertEqual(
            bundle["selections"][0]["selected_gap_ids"], ["S01E02"],
        )

    def test_selection_rejects_non_mapping_search_result(self) -> None:
        request = self._tv_request()
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            with self.assertRaises(TypeError):
                gap_ledger_selection(
                    state_root, "root-1", request,
                    search_runner=lambda req: ["not", "a", "mapping"],
                )


if __name__ == "__main__":
    unittest.main()
