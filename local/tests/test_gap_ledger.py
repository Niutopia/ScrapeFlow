"""Tests for the J-node per-work-unit Gap ledger."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.gap_ledger import (
    close_gap,
    discover_episode_gaps,
    gap_token,
    load_gap_ledger,
    parse_gap_token,
    record_attempt,
    register_subtitle_gap,
)


class GapLedgerTests(unittest.TestCase):
    def _open_episode_gaps(self, state_root: Path, root_task_id: str = "root-g"):
        return discover_episode_gaps(
            state_root,
            root_task_id,
            "unit-a",
            media_type="tv",
            tmdb_id=35507,
            expected_by_season={1: list(range(1, 11))},
            actual_tokens=["S01E01"],
        )

    def test_discover_episode_gaps_binds_precise_coordinates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            gaps = self._open_episode_gaps(state_root)
            self.assertEqual(len(gaps), 9)
            for gap in gaps:
                self.assertEqual(gap.work_unit_id, "unit-a")
                self.assertEqual(gap.kind, "missing_episode")
                self.assertEqual(gap.status, "open")
                self.assertEqual(gap.season, 1)
                self.assertEqual(len(gap.episodes), 1)
                self.assertNotIn(gap.episodes[0], (1,))
            tokens = {f"S01E{episode:02d}" for episode in range(2, 11)}
            self.assertEqual(
                {gap.gap_id.rsplit("::", 1)[1] for gap in gaps}, tokens,
            )
            # Idempotent: a second discovery adds no duplicates.
            again = self._open_episode_gaps(state_root)
            self.assertEqual(len(again), 9)

    def test_close_gap_is_audit_proven_and_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            gaps = self._open_episode_gaps(state_root)
            target = gaps[0]
            closed = close_gap(state_root, "root-g", target.gap_id)
            self.assertEqual(closed.status, "closed")
            # Re-discovery keeps the closed gap untouched.
            again = self._open_episode_gaps(state_root)
            self.assertEqual(len(again), 9)
            self.assertEqual(
                {gap.status for gap in again}, {"open", "closed"},
            )
            with self.assertRaises(KeyError):
                close_gap(state_root, "root-g", "missing-gap")

    def test_attempt_lifecycle_never_closes_a_gap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            gaps = self._open_episode_gaps(state_root)
            gap_id = gaps[0].gap_id
            updated = record_attempt(
                state_root, "root-g", gap_id,
                attempt_id="attempt-1",
                provider="magnet",
                tier="magnet",
                locator="torrent:https://example/1.torrent",
                status="candidate_failed",
                error="manifest mismatch",
            )
            self.assertEqual(updated.status, "open")  # attempts never close
            self.assertEqual(len(updated.attempts), 1)
            in_doubt = record_attempt(
                state_root, "root-g", gap_id,
                attempt_id="attempt-2",
                provider="quark_share",
                tier="quark_share",
                locator="quark_share:abc",
                status="in_doubt",
                external_task_id="task-9",
                staged_paths=["/staging/attempt-2/file.mkv"],
            )
            self.assertEqual(len(in_doubt.attempts), 2)
            self.assertEqual(in_doubt.attempts[1].status, "in_doubt")
            self.assertEqual(in_doubt.attempts[1].external_task_id, "task-9")
            self.assertEqual(in_doubt.status, "open")
            with self.assertRaises(ValueError):
                record_attempt(
                    state_root, "root-g", gap_id,
                    attempt_id="bad", provider="m", tier=None, locator=None,
                    status="bogus",
                )
            with self.assertRaises(KeyError):
                record_attempt(
                    state_root, "root-g", "missing", attempt_id="a",
                    provider="m", tier=None, locator=None, status="submitted",
                )

    def test_subtitle_gap_registration_and_dedupe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            gap = register_subtitle_gap(
                state_root, "root-g", "unit-a",
                media_type="tv", tmdb_id=35507,
                subtitle_path="/library/番剧/Fate Zero/Season 01/S01E02.mkv",
                subtitle_language="zh",
            )
            self.assertEqual(gap.kind, "missing_subtitle")
            self.assertEqual(gap.subtitle_language, "zh")
            self.assertEqual(gap.status, "open")
            again = register_subtitle_gap(
                state_root, "root-g", "unit-a",
                media_type="tv", tmdb_id=35507,
                subtitle_path="/library/番剧/Fate Zero/Season 01/S01E02.mkv",
                subtitle_language="zh",
            )
            self.assertEqual(again.gap_id, gap.gap_id)
            self.assertEqual(len(load_gap_ledger(state_root, "root-g")), 1)
            with self.assertRaises(ValueError):
                register_subtitle_gap(
                    state_root, "root-g", "unit-a",
                    media_type="tv", tmdb_id=35507,
                    subtitle_path="", subtitle_language="zh",
                )

    def test_token_helpers_and_corrupt_ledger(self) -> None:
        self.assertEqual(gap_token(1, 9), "S01E09")
        self.assertEqual(parse_gap_token("S02E11"), (2, 11))
        self.assertIsNone(parse_gap_token("E11"))
        self.assertIsNone(parse_gap_token("S02E11.mkv"))
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            (state_root / "gap_ledger_root-g.json").write_text(
                "{{broken", encoding="utf-8",
            )
            self.assertEqual(load_gap_ledger(state_root, "root-g"), [])


if __name__ == "__main__":
    unittest.main()
