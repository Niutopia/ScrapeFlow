"""Tests for the P14 root-task replenishment orchestrator (fake-only)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from engine.scrapeflow.gap_ledger import (
    AcquisitionAttempt,
    Gap,
    load_gap_ledger,
    save_gap_ledger,
)
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scrapeflow.work_units import WorkUnitRecord, save_work_unit_records

from local.scrapeflow_api.root_replenishment import (
    load_root_replenishment_state,
    run_root_replenishment,
    save_root_replenishment_state,
)
from local.scrapeflow_api.simple_engine_runner import SimpleEngineRunner

from local.tests.test_library_index import IndexAList
from local.tests.test_simple_engine_runner import FAKE_VIDEO_SIZE
from local.tests.test_work_unit_identity import FakeTMDBClient


class _InDoubtError(Exception):
    failure_scope = "in_doubt"

    def __init__(self, task_id: str) -> None:
        super().__init__("可能已提交")
        self.task_id = task_id


class _InfraError(Exception):
    failure_scope = "infrastructure"


class _CandidateError(Exception):
    failure_scope = "candidate"


class _FakeMaterializer:
    """Injected materializer double with an observable ``acquire``."""

    def __init__(
        self,
        *,
        delivery: dict[str, Any] | None = None,
        error: Exception | None = None,
        events: list[dict[str, Any]] | None = None,
        on_acquire=None,
        reconcile_delivery: dict[str, Any] | None = None,
        reconcile_error: Exception | None = None,
        reconcile_events: list[dict[str, Any]] | None = None,
    ) -> None:
        self.delivery = delivery
        self.error = error
        self.events = events
        self.on_acquire = on_acquire
        self.reconcile_delivery = reconcile_delivery
        self.reconcile_error = reconcile_error
        self.reconcile_events = reconcile_events

    def acquire(self, request, selections, *, staging_root, workspace, alist):
        if self.on_acquire is not None:
            self.on_acquire(staging_root)
        if self.events is not None:
            self.events.append({
                "request": dict(request),
                "selections": [dict(row) for row in selections],
                "staging_root": staging_root,
            })
        if self.error is not None:
            raise self.error
        return self.delivery if self.delivery is not None else {
            "staging_root": staging_root,
            "files": [],
        }

    def reconcile_existing_task(
        self, request, selections, *, staging_root, workspace, alist, external_task_id,
    ):
        if self.reconcile_events is not None:
            self.reconcile_events.append({
                "staging_root": staging_root,
                "external_task_id": external_task_id,
                "selections": [dict(row) for row in selections],
            })
        if self.reconcile_error is not None:
            raise self.reconcile_error
        return self.reconcile_delivery


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


def _magnet_search(gap_token: str):
    def search(request):
        return {
            "candidates": [{
                "provider": "magnet",
                "locator": f"torrent:https://example.test/{gap_token}.torrent",
                "release_name": f"[Group] Fate Zero {gap_token} 1080p",
                "title": "Fate/Zero",
                "year": "2011",
                "files": [f"Fate.Zero.{gap_token}.mkv"],
                "file_coverage": [gap_token],
                "acquisition": {"kind": "torrent"},
            }],
        }
    return search


def _empty_search(request):
    return {"candidates": []}


def _coverage_planner(files: list[tuple[str, str]]):
    def planner(request, _alist, _tmdb) -> Plan:
        media_type = str(request.media_type)
        mode = "tv" if media_type == "tv" else "movie"
        target = (
            f"/library/番剧/Work ({request.tmdb_id})"
            if mode == "tv"
            else f"/library/电影/Work ({request.tmdb_id})"
        )
        planned = [
            PlannedFile(
                source_path=f"{request.source_path}/{name}",
                source_dir=request.source_path,
                original_name=name,
                final_name=name,
                target_dir=target,
                media_kind=kind,
                source_size=FAKE_VIDEO_SIZE,
            )
            for name, kind in files
        ]
        return Plan(
            mode=mode,
            source_root=request.source_path,
            target_root=target,
            files=planned,
            warnings=[],
            metadata={
                "tmdb_id": request.tmdb_id,
                "title": "Work",
                "year": "2020",
                "media_type": media_type,
            },
        )
    return planner


class RootReplenishmentTests(unittest.TestCase):
    def _seed_tv_gap(
        self,
        state_root: Path,
        *,
        root_task_id: str = "root-1",
        work_unit_id: str = "unit-tv",
        season: int = 1,
        episode: int = 2,
        status: str = "open",
    ) -> None:
        save_work_unit_records(state_root, root_task_id, [
            _work_unit(
                root_task_id, work_unit_id,
                media_type="tv", tmdb_id=35507, title="Fate/Zero",
            ),
        ])
        save_gap_ledger(state_root, root_task_id, [
            _episode_gap(
                root_task_id, work_unit_id,
                media_type="tv", tmdb_id=35507, season=season, episode=episode,
                status=status,
            ),
        ])

    def _runner(self, state_root: Path, *, planner=None):
        return SimpleEngineRunner(
            state_root,
            alist=IndexAList({}),
            tmdb=FakeTMDBClient(search_results={}),
            planner=planner,
            executor=lambda plan: {"ok": True},
            validate=False,
            library_root="/library",
        )

    def _set_tier(self, state_root: Path, root_task_id: str, tier: str) -> None:
        state = load_root_replenishment_state(state_root, root_task_id)
        state["tier"] = tier
        save_root_replenishment_state(state_root, root_task_id, state)

    # --- state / no-op / bridge integration ---------------------------------

    def test_initial_tier_is_quark_share(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            state = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(state["tier"], "quark_share")
            self.assertIsNone(state["waiting"])
            self.assertEqual(state["last_attempt_at"], None)

    def test_state_round_trip_persists(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            state = load_root_replenishment_state(state_root, "root-1")
            state["tier"] = "alist_offline"
            state["waiting"] = "retry_wait"
            state["last_attempt_at"] = "2026-01-01T00:00:00Z"
            state["attempt_log"] = [{"gap_id": "g", "tier": "alist_offline", "outcome": "candidate"}]
            save_root_replenishment_state(state_root, "root-1", state)
            loaded = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(loaded["tier"], "alist_offline")
            self.assertEqual(loaded["waiting"], "retry_wait")
            self.assertEqual(loaded["last_attempt_at"], "2026-01-01T00:00:00Z")
            self.assertEqual(loaded["attempt_log"][0]["gap_id"], "g")

    def test_no_open_gaps_is_noop_without_materializer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root, status="closed")
            runner = self._runner(state_root)
            calls: list[str] = []

            result = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_empty_search,
                materializer_factory=lambda tier: (calls.append(tier) or None),
            )
            self.assertEqual(result["tier"], "quark_share")
            self.assertEqual(result["requests_built"], 0)
            self.assertEqual(result["gaps_closed"], [])
            self.assertIsNone(result["waiting"])
            self.assertEqual(calls, [])  # materializer factory never invoked

    def test_closed_gaps_excluded_from_requests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root, episode=2, status="closed")
            self._seed_tv_gap(state_root, episode=3, status="open")
            runner = self._runner(state_root)
            seen_tokens: list[list[str]] = []

            def search(request):
                seen_tokens.append([row["id"] for row in request["gaps"]])
                return {"candidates": []}

            run_root_replenishment(runner, state_root, "root-1", search_runner=search)
            # Only the open S01E03 reaches the search boundary; S01E02 is closed.
            self.assertEqual(seen_tokens, [["S01E03"]])

    def test_idempotent_rerun_after_all_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            runner = self._runner(
                state_root, planner=_coverage_planner([("S01E02.mkv", "video")]),
            )
            events: list[dict[str, Any]] = []

            first = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(events=events),
            )
            self.assertEqual(first["gaps_closed"], ["unit-tv::missing_episode::S01E02"])

            second = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(events=events),
            )
            self.assertEqual(second["requests_built"], 0)
            self.assertEqual(second["gaps_closed"], [])
            self.assertEqual(len(events), 1)  # materializer ran once, not again

    # --- tier progression ---------------------------------------------------

    def test_tier_advances_only_after_complete_no_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            runner = self._runner(state_root)
            result = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_empty_search,
                materializer_factory=lambda tier: _FakeMaterializer(),
            )
            self.assertEqual(result["tier"], "alist_offline")
            self.assertIsNone(result["waiting"])
            persisted = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(persisted["tier"], "alist_offline")

    def test_infrastructure_keeps_tier_and_waits_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            runner = self._runner(
                state_root, planner=_coverage_planner([("S01E02.mkv", "video")]),
            )
            result = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(
                    error=_InfraError("auth down"),
                ),
            )
            self.assertEqual(result["tier"], "magnet")
            self.assertEqual(result["waiting"], "retry_wait")
            self.assertEqual(result["gaps_closed"], [])
            self.assertEqual(
                result["attempts"][0]["outcome"], "infrastructure",
            )

    def test_in_doubt_keeps_tier_and_never_resubmits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            runner = self._runner(
                state_root, planner=_coverage_planner([("S01E02.mkv", "video")]),
            )
            events: list[dict[str, Any]] = []

            first = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(
                    error=_InDoubtError("task-1"), events=events,
                ),
            )
            self.assertEqual(first["tier"], "magnet")
            self.assertEqual(first["waiting"], "waiting_reconcile")
            self.assertEqual(len(events), 1)

            second = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(events=events),
            )
            # The in-flight gap was remembered and never re-submitted.
            self.assertEqual(second["requests_built"], 0)
            self.assertEqual(len(events), 1)
            self.assertEqual(second["waiting"], "waiting_reconcile")

    # --- attempt ordering / coverage proof / pause --------------------------

    def test_record_attempt_happens_before_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            runner = self._runner(
                state_root, planner=_coverage_planner([("S01E02.mkv", "video")]),
            )

            def on_acquire(_staging_root: str) -> None:
                gap = next(
                    g for g in load_gap_ledger(state_root, "root-1")
                    if g.gap_id == "unit-tv::missing_episode::S01E02"
                )
                statuses = [a.status for a in gap.attempts]
                self.assertIn("submitted", statuses)

            run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(
                    on_acquire=on_acquire,
                ),
            )

    def test_close_gap_only_after_coverage_proof(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            runner = self._runner(
                state_root, planner=_coverage_planner([("S01E02.mkv", "video")]),
            )
            result = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(),
            )
            self.assertEqual(result["gaps_closed"], ["unit-tv::missing_episode::S01E02"])
            gap = next(
                g for g in load_gap_ledger(state_root, "root-1")
                if g.gap_id == "unit-tv::missing_episode::S01E02"
            )
            self.assertEqual(gap.status, "closed")

    def test_missing_coverage_leaves_gap_open_records_candidate_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            runner = self._runner(
                state_root, planner=_coverage_planner([("S01E03.mkv", "video")]),
            )
            result = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(),
            )
            self.assertEqual(result["gaps_closed"], [])
            self.assertEqual(result["attempts"][0]["outcome"], "candidate")
            gap = next(
                g for g in load_gap_ledger(state_root, "root-1")
                if g.gap_id == "unit-tv::missing_episode::S01E02"
            )
            self.assertEqual(gap.status, "open")
            self.assertIn("candidate_failed", [a.status for a in gap.attempts])

    def test_pause_gate_blocks_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            runner = self._runner(
                state_root, planner=_coverage_planner([("S01E02.mkv", "video")]),
            )
            paused = [False]

            def search(request):
                paused[0] = True  # pause after search, before materialize
                return _magnet_search("S01E02")(request)

            events: list[dict[str, Any]] = []
            result = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=search,
                materializer_factory=lambda tier: _FakeMaterializer(events=events),
                pause_requested=lambda: paused[0],
            )
            self.assertEqual(events, [])  # no materializer call
            self.assertEqual(result["gaps_closed"], [])
            gap = next(
                g for g in load_gap_ledger(state_root, "root-1")
                if g.gap_id == "unit-tv::missing_episode::S01E02"
            )
            self.assertNotIn("submitted", [a.status for a in gap.attempts])

    def test_season_gap_requires_all_listed_episodes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            save_work_unit_records(state_root, "root-1", [
                _work_unit(
                    "root-1", "unit-tv",
                    media_type="tv", tmdb_id=35507, title="Fate/Zero",
                ),
            ])
            save_gap_ledger(state_root, "root-1", [
                Gap(
                    gap_id="unit-tv::missing_season::S02",
                    root_task_id="root-1",
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
                _episode_gap(
                    "root-1", "unit-tv",
                    media_type="tv", tmdb_id=35507, season=2, episode=1,
                ),
                _episode_gap(
                    "root-1", "unit-tv",
                    media_type="tv", tmdb_id=35507, season=2, episode=2,
                ),
            ])
            self._set_tier(state_root, "root-1", "magnet")
            # Materializer covers only S02E01; the season gap needs E01+E02.
            runner = self._runner(
                state_root, planner=_coverage_planner([("S02E01.mkv", "video")]),
            )
            result = run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=_magnet_search("S02E01"),
                materializer_factory=lambda tier: _FakeMaterializer(),
            )
            season_gap = next(
                g for g in load_gap_ledger(state_root, "root-1")
                if g.gap_id == "unit-tv::missing_season::S02"
            )
            self.assertEqual(season_gap.status, "open")
            self.assertNotIn("unit-tv::missing_season::S02", result["gaps_closed"])

    def test_empty_identity_titles_are_enriched_from_tmdb(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            save_work_unit_records(state_root, "root-1", [
                WorkUnitRecord(
                    work_unit_id="unit-tv",
                    root_task_id="root-1",
                    boundary_key="unit-tv",
                    source_paths=("/待刮削/unit-tv",),
                    source_revision=1,
                    role="single_work",
                    media_context="tv",
                    identity_status="confirmed",
                    identity={"media_type": "tv", "tmdb_id": 35507},
                ),
            ])
            save_gap_ledger(state_root, "root-1", [
                _episode_gap(
                    "root-1", "unit-tv",
                    media_type="tv", tmdb_id=35507, season=1, episode=2,
                ),
            ])
            self._set_tier(state_root, "root-1", "magnet")

            class DetailsTMDB:
                def get(self, path, **params):
                    return {
                        "/tv/35507": {
                            "name": "Fate/Zero",
                            "original_name": "Fate/Zero",
                        },
                    }.get(path)

            seen: list[dict[str, Any]] = []

            def search(request):
                seen.append(dict(request))
                return {"candidates": []}

            runner = self._runner(state_root)
            runner.tmdb = DetailsTMDB()
            run_root_replenishment(
                runner, state_root, "root-1", search_runner=search,
            )
            self.assertEqual(len(seen), 1)
            media = seen[0]["media"]
            self.assertEqual(media["title"], "Fate/Zero")
            self.assertIn("Fate/Zero", media["aliases"])

    def test_rejected_candidates_are_excluded_on_the_next_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            runner = self._runner(state_root)
            seen: list[dict[str, Any]] = []

            def search(request):
                seen.append(dict(request))
                return _magnet_search("S01E02")(request)

            # First run: the materializer rejects the candidate; the locator
            # must be remembered durably.
            run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=search,
                materializer_factory=lambda tier: _FakeMaterializer(error=_CandidateError()),
            )
            state = load_root_replenishment_state(state_root, "root-1")
            locators = (state.get("candidate_failures_by_provider") or {}).get("magnet") or []
            self.assertIn("torrent:https://example.test/S01E02.torrent", locators)
            # Second run: the failed locator arrives as an excluded candidate.
            run_root_replenishment(
                runner, state_root, "root-1",
                search_runner=search,
                materializer_factory=lambda tier: _FakeMaterializer(),
            )
            second = seen[-1]
            excluded = [
                row.get("locator")
                for row in (second.get("excluded_candidates") or [])
                if isinstance(row, dict)
            ]
            self.assertIn("torrent:https://example.test/S01E02.torrent", excluded)

    def test_subtitle_gaps_never_enter_the_video_tiers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            save_work_unit_records(state_root, "root-1", [
                _work_unit(
                    "root-1", "unit-tv",
                    media_type="tv", tmdb_id=35507, title="Fate/Zero",
                ),
            ])
            save_gap_ledger(state_root, "root-1", [
                Gap(
                    gap_id="unit-tv::missing_subtitle::zh",
                    root_task_id="root-1",
                    work_unit_id="unit-tv",
                    kind="missing_subtitle",
                    media_type="tv",
                    tmdb_id=35507,
                    season=None,
                    episodes=(),
                    subtitle_path="/library/番剧/Fate Zero/S01E01.mkv",
                    subtitle_language="zh",
                    status="open",
                ),
            ])
            self._set_tier(state_root, "root-1", "magnet")
            events: list[dict[str, Any]] = []
            result = run_root_replenishment(
                self._runner(state_root),
                state_root, "root-1",
                search_runner=_magnet_search("S01E01"),
                materializer_factory=lambda tier: _FakeMaterializer(events=events),
            )
            # The subtitle channel owns subtitle gaps; the video tiers must
            # never search or acquire for them.
            self.assertEqual(events, [])
            self.assertEqual(result["requests_built"], 0)
            self.assertEqual(result["gaps_closed"], [])
            gap = next(
                g for g in load_gap_ledger(state_root, "root-1")
                if g.gap_id == "unit-tv::missing_subtitle::zh"
            )
            self.assertEqual(gap.status, "open")
            self.assertEqual(gap.attempts, ())


    # --- waiting_reconcile re-entry ----------------------------------------

    def _seed_parked_attempt(
        self,
        state_root: Path,
        *,
        root_task_id: str = "root-1",
        token: str = "S01E02",
        season: int = 1,
        episode: int = 2,
        attempt_tier: str = "alist_offline",
        attempt_id: str = "attempt-9",
        task_id: str = "task-9",
        locator: str = "alist_offline:0123456789012345678901234567890123456789",
    ) -> str:
        """Seed a parked in_doubt attempt: open gap + ledger attempt + durable
        materializer state + the in-flight token.  Returns the staging root."""
        save_work_unit_records(state_root, root_task_id, [
            _work_unit(
                root_task_id, "unit-tv",
                media_type="tv", tmdb_id=35507, title="Fate/Zero",
            ),
        ])
        gap = _episode_gap(
            root_task_id, "unit-tv",
            media_type="tv", tmdb_id=35507, season=season, episode=episode,
        )
        gap = Gap(
            gap_id=gap.gap_id,
            root_task_id=gap.root_task_id,
            work_unit_id=gap.work_unit_id,
            kind=gap.kind,
            media_type=gap.media_type,
            tmdb_id=gap.tmdb_id,
            season=gap.season,
            episodes=gap.episodes,
            subtitle_path=None,
            subtitle_language=None,
            status=gap.status,
            attempts=(
                AcquisitionAttempt(
                    attempt_id=attempt_id,
                    provider=attempt_tier,
                    tier=attempt_tier,
                    locator=locator,
                    status="in_doubt",
                    external_task_id=task_id,
                ),
            ),
        )
        save_gap_ledger(state_root, root_task_id, [gap])
        staging = f"/library/ScrapeFlow/补源/{root_task_id}/{attempt_id}"
        workspace = state_root / "replenishment_workspace" / root_task_id / attempt_id
        workspace.mkdir(parents=True, exist_ok=True)
        (workspace / "alist_offline_attempt.json").write_text(json.dumps({
            "provider": attempt_tier,
            "attempt_id": attempt_id,
            "staging_root": staging,
            "task_id": task_id,
            "locator": locator,
            "selected_gap_ids": [token],
            "acquisition": {
                "kind": "alist_offline",
                "magnet_url": (
                    "magnet:?xt=urn:btih:0123456789012345678901234567890123456789"
                ),
                "torrent_url": "https://example.test/example.torrent",
                "expected_files": [{
                    "path": f"Fate.Zero.{token}.mkv",
                    "size": FAKE_VIDEO_SIZE,
                    "gap_ids": [token],
                }],
            },
            "updated_at": "2026-01-01T00:00:00Z",
        }))
        state = load_root_replenishment_state(state_root, root_task_id)
        state["tier"] = "alist_offline"
        state["in_flight_gap_ids"] = {token: task_id}
        save_root_replenishment_state(state_root, root_task_id, state)
        return staging

    def test_reconcile_closes_parked_gap_and_unparks_token(self) -> None:
        """A finished AList task finalizes through the writer closure."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            staging = self._seed_parked_attempt(state_root)
            reconcile_events: list[dict[str, Any]] = []
            delivery = {
                "lane": "alist_offline",
                "attempt_id": "attempt-9",
                "staging_root": staging,
                "files": [{
                    "path": f"{staging}/Fate.Zero.S01E02.mkv",
                    "size": FAKE_VIDEO_SIZE,
                    "kind": "video",
                    "gap_ids": ["S01E02"],
                }],
                "external_task_id": "task-9",
            }
            result = run_root_replenishment(
                self._runner(
                    state_root, planner=_coverage_planner([
                        ("Fate.Zero.S01E02.mkv", "video"),
                    ]),
                ),
                state_root, "root-1",
                search_runner=_empty_search,
                materializer_factory=lambda tier: _FakeMaterializer(
                    reconcile_delivery=delivery,
                    reconcile_events=reconcile_events,
                ),
            )
            self.assertEqual(reconcile_events, [{
                "staging_root": staging,
                "external_task_id": "task-9",
                "selections": [{
                    "provider": "alist_offline",
                    "locator": "alist_offline:0123456789012345678901234567890123456789",
                    "selected_gap_ids": ["S01E02"],
                    "acquisition": {
                        "kind": "alist_offline",
                        "magnet_url": (
                            "magnet:?xt=urn:btih:"
                            "0123456789012345678901234567890123456789"
                        ),
                        "torrent_url": "https://example.test/example.torrent",
                        "expected_files": [{
                            "path": "Fate.Zero.S01E02.mkv",
                            "size": FAKE_VIDEO_SIZE,
                            "gap_ids": ["S01E02"],
                        }],
                    },
                }],
            }])
            self.assertEqual(result["requests_built"], 0)
            self.assertEqual(result["gaps_closed"], ["unit-tv::missing_episode::S01E02"])
            self.assertIsNone(result["waiting"])
            persisted = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(persisted["in_flight_gap_ids"], {})
            gap = next(iter(load_gap_ledger(state_root, "root-1")))
            self.assertEqual(gap.status, "closed")

    def test_reconcile_candidate_failure_unparks_and_excludes_locator(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            locator = "alist_offline:0123456789012345678901234567890123456789"
            self._seed_parked_attempt(state_root, locator=locator)
            result = run_root_replenishment(
                self._runner(state_root),
                state_root, "root-1",
                search_runner=_empty_search,
                materializer_factory=lambda tier: _FakeMaterializer(
                    reconcile_error=_CandidateError("任务失败"),
                ),
            )
            self.assertIsNone(result["waiting"])
            self.assertEqual(result["gaps_closed"], [])
            persisted = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(persisted["in_flight_gap_ids"], {})
            self.assertEqual(
                persisted["candidate_failures_by_provider"]["alist_offline"],
                [locator],
            )

    def test_reconcile_infrastructure_keeps_token_parked_with_retry_wait(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_parked_attempt(state_root)
            result = run_root_replenishment(
                self._runner(state_root),
                state_root, "root-1",
                search_runner=_empty_search,
                materializer_factory=lambda tier: _FakeMaterializer(
                    reconcile_error=_InfraError("AList 不可达"),
                ),
            )
            self.assertEqual(result["waiting"], "retry_wait")
            persisted = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(
                persisted["in_flight_gap_ids"], {"S01E02": "task-9"},
            )

    def test_reconcile_legacy_quark_magnet_parked_token_unparks(self) -> None:
        """A token parked by the removed lane re-enters the current ladder."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_parked_attempt(state_root, attempt_tier="quark_magnet")
            events: list[dict[str, Any]] = []
            result = run_root_replenishment(
                self._runner(state_root),
                state_root, "root-1",
                search_runner=_empty_search,
                materializer_factory=lambda tier: _FakeMaterializer(events=events),
            )
            self.assertEqual(events, [])
            self.assertIsNone(result["waiting"])
            persisted = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(persisted["in_flight_gap_ids"], {})



if __name__ == "__main__":
    unittest.main()
