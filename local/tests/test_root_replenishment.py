"""Tests for the P14 root-task replenishment orchestrator (fake-only)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from engine.scrapeflow.gap_ledger import (
    Gap,
    load_gap_ledger,
    save_gap_ledger,
)
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scrapeflow.work_units import WorkUnitRecord, save_work_unit_records

from local.scrapeflow_api.root_replenishment import (
    PreUpgradeAListStateError,
    load_root_replenishment_state,
    run_root_replenishment,
    save_root_replenishment_state,
)
from local.scrapeflow_api.replenishment_tiers import (
    EXHAUSTION_MIN_DISTINCT_LOCATORS,
    MAGNET_REQUIRED_SOURCES,
)
from local.scrapeflow_api.simple_engine_runner import (
    EnginePauseRequested,
    SimpleEngineRunner,
)

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

    def acquire(
        self,
        request,
        selections,
        *,
        staging_root,
        workspace,
        alist,
        pause_requested=None,
    ):
        del pause_requested
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
        self, request, selections, *, staging_root, workspace, alist,
        external_task_id, pause_requested=None,
    ):
        del pause_requested
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


def _quark_share_search(gap_token: str):
    def search(request):
        del request
        return {"candidates": [{
            "provider": "quark_share",
            "locator": "quark_share:final-candidate",
            "release_name": f"Fate Zero {gap_token} 1080p",
            "title": "Fate/Zero",
            "year": "2011",
            "files": [f"Fate.Zero.{gap_token}.mkv"],
            "file_coverage": [gap_token],
            "acquisition": {
                "kind": "quark_fast_save",
                "share_id": "fixture-share",
                "share_url": "https://pan.quark.cn/s/fixture-share",
                "file_id_by_gap": {gap_token: ["fixture-file"]},
            },
        }]}
    return search


def _empty_search(request):
    return {"candidates": []}


def _complete_no_candidate_search(*completed_sources: str):
    def search(request):
        del request
        return {
            "candidates": [],
            "search_complete_no_candidates": True,
            "completed_sources": list(completed_sources),
            "unchecked_secondary_candidates": 0,
        }
    return search


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
            state["tier"] = "magnet"
            state["waiting"] = "retry_wait"
            state["last_attempt_at"] = "2026-01-01T00:00:00Z"
            state["attempt_log"] = [{"gap_id": "g", "tier": "magnet", "outcome": "candidate"}]
            save_root_replenishment_state(state_root, "root-1", state)
            loaded = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(loaded["tier"], "magnet")
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
                search_runner=_complete_no_candidate_search("pansou"),
                materializer_factory=lambda tier: _FakeMaterializer(),
            )
            self.assertEqual(result["tier"], "magnet")
            self.assertIsNone(result["waiting"])
            persisted = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(persisted["tier"], "magnet")

    def test_empty_selection_without_raw_completion_proof_does_not_advance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)

            result = run_root_replenishment(
                self._runner(state_root), state_root, "root-1",
                search_runner=_empty_search,
            )

            self.assertEqual(result["tier_before"], "quark_share")
            self.assertEqual(result["tier"], "quark_share")
            self.assertEqual(result["waiting"], "retry_wait")
            self.assertEqual(result["attempts"][0]["outcome"], "infrastructure")
            self.assertEqual(
                load_root_replenishment_state(state_root, "root-1")[
                    "exhaustion_proof_by_provider"
                ],
                {},
            )

    def test_search_exception_without_locator_retries_same_tier(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)

            def search(_request):
                raise _CandidateError("bad source")

            result = run_root_replenishment(
                self._runner(state_root), state_root, "root-1",
                search_runner=search,
            )

            self.assertEqual(result["tier"], "quark_share")
            self.assertEqual(result["waiting"], "retry_wait")
            self.assertEqual(result["attempts"][0]["outcome"], "infrastructure")
            self.assertEqual(
                load_root_replenishment_state(state_root, "root-1")[
                    "candidate_failures_by_provider"
                ],
                {},
            )

    def test_each_identity_needs_its_own_completion_proof_before_advancing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            save_work_unit_records(state_root, "root-1", [
                _work_unit(
                    "root-1", "unit-tv",
                    media_type="tv", tmdb_id=35507, title="Fate/Zero",
                ),
                _work_unit(
                    "root-1", "unit-movie",
                    media_type="movie", tmdb_id=10378, title="The Big Short",
                ),
            ])
            save_gap_ledger(state_root, "root-1", [
                _episode_gap(
                    "root-1", "unit-tv",
                    media_type="tv", tmdb_id=35507, season=1, episode=2,
                ),
                Gap(
                    gap_id="unit-movie::missing_media::main",
                    root_task_id="root-1",
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

            def search(request):
                if request["media"]["tmdb_id"] == 35507:
                    return _complete_no_candidate_search("pansou")(request)
                return {"candidates": []}

            result = run_root_replenishment(
                self._runner(state_root), state_root, "root-1",
                search_runner=search,
            )

            self.assertEqual(result["requests_built"], 2)
            self.assertEqual(result["tier"], "quark_share")
            self.assertEqual(result["waiting"], "retry_wait")
            self.assertEqual(
                load_root_replenishment_state(state_root, "root-1")[
                    "exhaustion_proof_by_provider"
                ],
                {},
            )

    def test_pause_after_search_does_not_certify_or_advance_tier(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            paused = [False]

            def search(request):
                paused[0] = True
                return _complete_no_candidate_search("pansou")(request)

            result = run_root_replenishment(
                self._runner(state_root), state_root, "root-1",
                search_runner=search,
                pause_requested=lambda: paused[0],
            )

            self.assertEqual(result["tier_before"], "quark_share")
            self.assertEqual(result["tier"], "quark_share")
            self.assertEqual(result["attempts"], [])
            self.assertEqual(
                load_root_replenishment_state(state_root, "root-1")[
                    "exhaustion_proof_by_provider"
                ],
                {},
            )

    def test_pause_after_candidate_failure_cannot_cross_exhaustion_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            state = load_root_replenishment_state(state_root, "root-1")
            state["candidate_failures_by_provider"] = {
                "quark_share": [
                    f"quark_share:old-{index}"
                    for index in range(EXHAUSTION_MIN_DISTINCT_LOCATORS - 1)
                ],
            }
            save_root_replenishment_state(state_root, "root-1", state)
            paused = [False]

            class PausingCandidateMaterializer(_FakeMaterializer):
                def acquire(self, *args, **kwargs):
                    del args, kwargs
                    paused[0] = True
                    raise _CandidateError("share expired")

            result = run_root_replenishment(
                self._runner(state_root), state_root, "root-1",
                search_runner=_quark_share_search("S01E02"),
                materializer_factory=lambda tier: PausingCandidateMaterializer(),
                pause_requested=lambda: paused[0],
            )

            self.assertEqual(result["tier"], "quark_share")
            persisted = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(persisted["tier"], "quark_share")
            self.assertEqual(
                len(persisted["candidate_failures_by_provider"]["quark_share"]),
                EXHAUSTION_MIN_DISTINCT_LOCATORS,
            )

    def test_incomplete_second_request_blocks_candidate_threshold_transition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            save_work_unit_records(state_root, "root-1", [
                _work_unit(
                    "root-1", "unit-tv",
                    media_type="tv", tmdb_id=35507, title="Fate/Zero",
                ),
                _work_unit(
                    "root-1", "unit-movie",
                    media_type="movie", tmdb_id=10378, title="The Big Short",
                ),
            ])
            save_gap_ledger(state_root, "root-1", [
                _episode_gap(
                    "root-1", "unit-tv",
                    media_type="tv", tmdb_id=35507, season=1, episode=2,
                ),
                Gap(
                    gap_id="unit-movie::missing_media::main",
                    root_task_id="root-1",
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
            state = load_root_replenishment_state(state_root, "root-1")
            state["candidate_failures_by_provider"] = {
                "quark_share": [
                    f"quark_share:old-{index}"
                    for index in range(EXHAUSTION_MIN_DISTINCT_LOCATORS - 1)
                ],
            }
            save_root_replenishment_state(state_root, "root-1", state)

            def search(request):
                if request["media"]["tmdb_id"] == 35507:
                    return _quark_share_search("S01E02")(request)
                return {"candidates": []}  # no raw completion proof

            result = run_root_replenishment(
                self._runner(state_root), state_root, "root-1",
                search_runner=search,
                materializer_factory=lambda tier: _FakeMaterializer(
                    error=_CandidateError("share expired"),
                ),
            )

            self.assertEqual(result["tier"], "quark_share")
            self.assertEqual(result["waiting"], "retry_wait")
            persisted = load_root_replenishment_state(state_root, "root-1")
            self.assertEqual(persisted["tier"], "quark_share")
            self.assertEqual(
                len(persisted["candidate_failures_by_provider"]["quark_share"]),
                EXHAUSTION_MIN_DISTINCT_LOCATORS,
            )

    def test_magnet_requires_complete_raw_source_proof_before_exhaustion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")

            partial = run_root_replenishment(
                self._runner(state_root), state_root, "root-1",
                search_runner=_complete_no_candidate_search("animetosho"),
            )
            self.assertEqual(partial["tier"], "magnet")
            self.assertEqual(partial["waiting"], "retry_wait")

            def completed_via_raw_telemetry(_request):
                return {
                    "candidates": [],
                    "search_complete": True,
                    "source_telemetry": {
                        source: {
                            "source_exhausted": True,
                            "infrastructure_failures": 0,
                        }
                        for source in MAGNET_REQUIRED_SOURCES
                    },
                }

            complete = run_root_replenishment(
                self._runner(state_root), state_root, "root-1",
                search_runner=completed_via_raw_telemetry,
            )
            self.assertEqual(complete["tier"], "magnet")
            self.assertEqual(complete["state"]["status"], "exhausted")

    def test_unrelated_source_telemetry_does_not_change_share_tier_proof(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)

            def search(_request):
                return {
                    "candidates": [],
                    "search_complete_no_candidates": True,
                    "source_telemetry": {
                        "PanSou": {
                            "source_exhausted": True,
                            "infrastructure_failures": 0,
                        },
                        "unrelated-diagnostic-source": {
                            "source_exhausted": False,
                            "infrastructure_failures": 1,
                        },
                    },
                }

            result = run_root_replenishment(
                self._runner(state_root), state_root, "root-1",
                search_runner=search,
            )

            self.assertEqual(result["tier_before"], "quark_share")
            self.assertEqual(result["tier"], "magnet")

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

    def test_child_plan_and_write_receive_the_root_pause_predicate(self) -> None:
        """A root-scope closure must reach both provider child boundaries."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            runner = self._runner(
                state_root, planner=_coverage_planner([("S01E02.mkv", "video")]),
            )
            plan_checks: list[object] = []
            execute_checks: list[object] = []
            original_plan = runner.plan_job
            original_execute = runner.execute_job

            def recording_plan(*args, **kwargs):
                plan_checks.append(kwargs.get("pause_requested"))
                return original_plan(*args, **kwargs)

            def recording_execute(*args, **kwargs):
                execute_checks.append(kwargs.get("pause_requested"))
                return original_execute(*args, **kwargs)

            runner.plan_job = recording_plan  # type: ignore[method-assign]
            runner.execute_job = recording_execute  # type: ignore[method-assign]
            pause = lambda: False
            result = run_root_replenishment(
                runner,
                state_root,
                "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(),
                pause_requested=pause,
            )

        self.assertEqual(result["gaps_closed"], ["unit-tv::missing_episode::S01E02"])
        self.assertEqual(len(plan_checks), 1)
        self.assertEqual(len(execute_checks), 1)
        self.assertIs(plan_checks[0], execute_checks[0])
        self.assertTrue(callable(plan_checks[0]))
        self.assertFalse(plan_checks[0]())

    def test_paused_child_writer_cannot_close_gap_from_its_plan(self) -> None:
        """Coverage is accepted only after the child reaches executed."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            paused = {"value": False}

            def pausing_executor(_plan):
                paused["value"] = True
                raise EnginePauseRequested("fixture pause during child writer")

            runner = self._runner(
                state_root, planner=_coverage_planner([("S01E02.mkv", "video")]),
            )
            runner.executor = pausing_executor
            result = run_root_replenishment(
                runner,
                state_root,
                "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda tier: _FakeMaterializer(),
                pause_requested=lambda: paused["value"],
            )
            gap = next(
                row for row in load_gap_ledger(state_root, "root-1")
                if row.gap_id == "unit-tv::missing_episode::S01E02"
            )

        self.assertEqual(result["gaps_closed"], [])
        self.assertEqual(gap.status, "open")
        self.assertEqual(result["attempts"], [])
        self.assertNotIn("closed", [row["outcome"] for row in result["attempts"]])

    def test_pause_raised_inside_child_plan_stops_without_retry_classification(self) -> None:
        """A plan-time pause is a clean round stop, not infrastructure evidence."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            self._set_tier(state_root, "root-1", "magnet")
            paused = {"value": False}
            runner = self._runner(
                state_root,
                planner=_coverage_planner([("S01E02.mkv", "video")]),
            )
            planned_children: list[object] = []

            def pausing_plan(*_args, **_kwargs):
                planned_children.append(object())
                paused["value"] = True
                raise EnginePauseRequested("fixture pause during child plan")

            runner.plan_job = pausing_plan  # type: ignore[method-assign]
            result = run_root_replenishment(
                runner,
                state_root,
                "root-1",
                search_runner=_magnet_search("S01E02"),
                materializer_factory=lambda _tier: _FakeMaterializer(),
                pause_requested=lambda: paused["value"],
            )
            gap = next(
                row for row in load_gap_ledger(state_root, "root-1")
                if row.gap_id == "unit-tv::missing_episode::S01E02"
            )

        self.assertEqual(len(planned_children), 1)
        self.assertEqual(result["gaps_closed"], [])
        self.assertEqual(result["attempts"], [])
        self.assertEqual(result["tier"], "magnet")
        self.assertEqual(gap.status, "open")

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


    # --- pre-upgrade AList state refusal -----------------------------------

    def test_pre_upgrade_alist_state_is_refused_before_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            self._seed_tv_gap(state_root)
            state = load_root_replenishment_state(state_root, "root-1")
            state["tier"] = "alist_offline"
            save_root_replenishment_state(state_root, "root-1", state)
            calls: list[str] = []

            with self.assertRaisesRegex(PreUpgradeAListStateError, "人工确认并清理"):
                run_root_replenishment(
                    self._runner(state_root),
                    state_root,
                    "root-1",
                    search_runner=lambda _request: (_ for _ in ()).throw(
                        AssertionError("retired state must not search"),
                    ),
                    materializer_factory=lambda tier: calls.append(tier),
                )
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
