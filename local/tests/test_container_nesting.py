"""Tests for the Fate-style container nesting rule (P13)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from engine.scrapeflow.models import Plan, PlannedFile
from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import apply_work_unit_override
from engine.scrapeflow.work_units import load_work_unit_records

from local.scrapeflow_api.library_index import reconcile_root_work_units
from local.scrapeflow_api.simple_engine_runner import SimpleEngineRunner
from local.scrapeflow_api.unit_execution import (
    _clean_container_name,
    execute_new_work_units,
)

from local.tests.test_library_index import IndexAList
from local.tests.test_simple_engine_runner import FAKE_VIDEO_BYTES, FAKE_VIDEO_SIZE


def _recording_planner(events: list[dict[str, Any]]):
    def planner(request, _alist, _tmdb) -> Plan:
        events.append({
            "source_path": request.source_path,
            "parent_path": request.parent_path,
            "media_type": request.media_type,
            "tmdb_id": request.tmdb_id,
        })
        target = f"{request.parent_path.rstrip('/')}/Work ({request.tmdb_id})"
        return Plan(
            mode="tv" if request.media_type == "tv" else "movie",
            source_root=request.source_path,
            target_root=target,
            files=[PlannedFile(
                source_path=f"{request.source_path}/S01E01.mkv",
                source_dir=request.source_path,
                original_name="S01E01.mkv",
                final_name="S01E01.mkv",
                target_dir=target,
                media_kind="video",
                source_size=FAKE_VIDEO_SIZE,
            )],
            warnings=[],
            metadata={"tmdb_id": request.tmdb_id, "title": "Work", "year": "2020"},
        )

    return planner


class ContainerNestingTests(unittest.TestCase):
    def _setup(self, files: dict[str, bytes]):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = IndexAList(dict(files))
        events: list[dict[str, Any]] = []
        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=object(),
            planner=_recording_planner(events),
            validate=False,
            library_root="/library",
            executor=lambda plan: {"ok": True},
        )
        return state_root, alist, runner, events

    def _root(self, runner, source, shelf="anime"):
        pending = runner.create_pending_job(source)
        return runner.start_automatic_job(pending.id, target_shelf=shelf).id

    def test_clean_container_name_strips_release_noise(self) -> None:
        self.assertEqual(_clean_container_name("[TUDO&Ygm] Fate 1080P"), "Fate")
        self.assertEqual(_clean_container_name("1.刀剑神域 合集"), "刀剑神域 合集")
        self.assertEqual(_clean_container_name("Fate"), "Fate")
        self.assertIsNone(_clean_container_name("无！！！32生"))
        self.assertIsNone(_clean_container_name(""))

    def test_multi_tv_container_nests_everything_under_one_folder(self) -> None:
        files = {
            "/incoming/[TUDO] Fate 1080P/命运之夜/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/[TUDO] Fate 1080P/卫宫家今天的饭/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/[TUDO] Fate 1080P/魔法少女伊莉雅/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/[TUDO] Fate 1080P/天之杯/movie.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, events = self._setup(files)
        root_id = self._root(runner, "/incoming/[TUDO] Fate 1080P")
        analyze_root_boundaries(
            alist, "/incoming/[TUDO] Fate 1080P",
            root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        self.assertEqual(len(records), 4)
        apply_work_unit_override(
            state_root, root_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        apply_work_unit_override(
            state_root, root_id, records[1].work_unit_id,
            media_type="tv", tmdb_id=102,
        )
        apply_work_unit_override(
            state_root, root_id, records[2].work_unit_id,
            media_type="tv", tmdb_id=103,
        )
        apply_work_unit_override(
            state_root, root_id, records[3].work_unit_id,
            media_type="movie", tmdb_id=201,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_id)

        execute_new_work_units(runner, state_root, root_id)

        parents = {event["parent_path"] for event in events}
        self.assertEqual(parents, {"/library/番剧/Fate"})
        self.assertEqual(len(events), 4)

    def test_single_tv_container_puts_movies_under_the_main_series_root(self) -> None:
        files = {
            "/incoming/刀剑神域/第一季/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/刀剑神域/序列之争/movie.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, events = self._setup(files)
        root_id = self._root(runner, "/incoming/刀剑神域")
        analyze_root_boundaries(
            alist, "/incoming/刀剑神域", root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        self.assertEqual(len(records), 2)
        apply_work_unit_override(
            state_root, root_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=45782,
        )
        apply_work_unit_override(
            state_root, root_id, records[1].work_unit_id,
            media_type="movie", tmdb_id=413594,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_id)

        execute_new_work_units(runner, state_root, root_id)

        self.assertEqual(len(events), 2)
        # The main TV unit plans at the shelf root and owns the container.
        self.assertEqual(events[0]["parent_path"], "/library/番剧")
        self.assertEqual(events[0]["tmdb_id"], 45782)
        # The movie nests under the main unit's real planned target root.
        self.assertEqual(events[1]["parent_path"], "/library/番剧/Work (45782)")
        self.assertEqual(events[1]["tmdb_id"], 413594)

    def test_junk_container_name_falls_back_to_first_tv_boundary(self) -> None:
        files = {
            "/incoming/无！！！32生/Show A/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/无！！！32生/Show B/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, events = self._setup(files)
        root_id = self._root(runner, "/incoming/无！！！32生")
        analyze_root_boundaries(
            alist, "/incoming/无！！！32生", root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        apply_work_unit_override(
            state_root, root_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        apply_work_unit_override(
            state_root, root_id, records[1].work_unit_id,
            media_type="tv", tmdb_id=102,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_id)

        execute_new_work_units(runner, state_root, root_id)

        parents = {event["parent_path"] for event in events}
        self.assertEqual(parents, {"/library/番剧/Show A"})


if __name__ == "__main__":
    unittest.main()
