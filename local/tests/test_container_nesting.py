"""Tests for the Fate-style container nesting rule (P13)."""

from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any

from engine.scrapeflow.canonical_work_tree import (
    CanonicalWork,
    WorkIdentity,
    plan_canonical_work_tree,
)
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.work_units import WorkUnitRecord
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import apply_work_unit_override
from engine.scrapeflow.work_units import load_work_unit_records, save_work_unit_records

from local.scrapeflow_api.library_index import reconcile_root_work_units
from local.scrapeflow_api.simple_engine_runner import EngineJob, SimpleEngineRunner
from local.scrapeflow_api.unit_execution import (
    _clean_container_name,
    _container_layout_targets,
    execute_new_work_units,
    load_work_acceptance,
)

from local.tests.test_library_index import IndexAList
from local.tests.test_simple_engine_runner import (
    FAKE_VIDEO_BYTES,
    FAKE_VIDEO_SIZE,
)


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

        def successful_writer(plan: Plan) -> dict[str, object]:
            # Container siblings may use an accepted main TV work root as
            # their next planner parent.  Model the H readback fact that a
            # real successful writer establishes: the concrete work root is
            # now a visible formal-library directory.
            alist.ensure_directory(str(plan.target_root))
            return {"ok": True}

        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=object(),
            planner=_recording_planner(events),
            validate=False,
            library_root="/library",
            executor=successful_writer,
        )
        return state_root, alist, runner, events

    def _root(self, runner, source, shelf="anime"):
        pending = runner.create_pending_job(source)
        return runner.start_automatic_job(pending.id, target_shelf=shelf).id

    def _record_for_source_leaf(self, records, leaf: str):
        matches = [
            record
            for record in records
            if record.source_paths
            and record.source_paths[0].rstrip("/").rsplit("/", 1)[-1] == leaf
        ]
        self.assertEqual(len(matches), 1, leaf)
        return matches[0]

    def test_clean_container_name_strips_release_noise(self) -> None:
        self.assertEqual(_clean_container_name("[TUDO&Ygm] Fate 1080P"), "Fate")
        self.assertEqual(_clean_container_name("1.刀剑神域 合集"), "刀剑神域 合集")
        self.assertEqual(
            _clean_container_name("Fate全系列 硬字幕+软字幕 4K+1080P"),
            "Fate",
        )
        self.assertEqual(_clean_container_name("W 五等分的花嫁"), "五等分的花嫁")
        self.assertEqual(
            _clean_container_name("H 寒蝉鸣泣之时全系列 外挂+内嵌字幕"),
            "寒蝉鸣泣之时",
        )
        self.assertEqual(
            _clean_container_name("魔法禁书目录 S01-S03合集"),
            "魔法禁书目录",
        )
        self.assertEqual(_clean_container_name("Fate"), "Fate")
        self.assertIsNone(_clean_container_name("无！！！32生"))
        self.assertIsNone(_clean_container_name(""))

    def test_main_tv_owns_container_root_and_movie_nests_below(self) -> None:
        """A single TV beside an independent film must not re-nest the TV."""
        works = [
            CanonicalWork(
                member_key="tv",
                identity=WorkIdentity("tmdb.tv", 65945),
                title="甲铁城的卡巴内瑞",
                leaf_name="甲铁城的卡巴内瑞",
            ),
            CanonicalWork(
                member_key="movie",
                identity=WorkIdentity("tmdb.movie", 1000000),
                title="海门决战",
                leaf_name="海门决战 (2019)",
            ),
        ]
        # Without a root identity the TV is wrongly re-nested under a
        # same-named directory-only container (the pre-fix behaviour).
        sibling = plan_canonical_work_tree(
            works,
            container_root="/library/番剧/甲铁城的卡巴内瑞",
            root_identity=None,
            allow_family_boundaries=False,
        )
        self.assertEqual(
            {placement.identity: placement.target_root for placement in sibling.placements}[
                WorkIdentity("tmdb.tv", 65945)
            ],
            "/library/番剧/甲铁城的卡巴内瑞/甲铁城的卡巴内瑞",
        )
        # The main TV identity owns the container root, so the film becomes a
        # child leaf and the TV stays in place.
        tree = plan_canonical_work_tree(
            works,
            container_root="/library/番剧/甲铁城的卡巴内瑞",
            root_identity=WorkIdentity("tmdb.tv", 65945),
            allow_family_boundaries=False,
        )
        roots = {placement.identity: placement.target_root for placement in tree.placements}
        self.assertEqual(
            roots[WorkIdentity("tmdb.tv", 65945)],
            "/library/番剧/甲铁城的卡巴内瑞",
        )
        self.assertEqual(
            roots[WorkIdentity("tmdb.movie", 1000000)],
            "/library/番剧/甲铁城的卡巴内瑞/海门决战 (2019)",
        )

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
        for leaf, media_type, tmdb_id in (
            ("命运之夜", "tv", 101),
            ("卫宫家今天的饭", "tv", 102),
            ("魔法少女伊莉雅", "tv", 103),
            ("天之杯", "movie", 201),
        ):
            apply_work_unit_override(
                state_root,
                root_id,
                self._record_for_source_leaf(records, leaf).work_unit_id,
                media_type=media_type,
                tmdb_id=tmdb_id,
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
            state_root,
            root_id,
            self._record_for_source_leaf(records, "第一季").work_unit_id,
            media_type="tv",
            tmdb_id=45782,
        )
        apply_work_unit_override(
            state_root,
            root_id,
            self._record_for_source_leaf(records, "序列之争").work_unit_id,
            media_type="movie",
            tmdb_id=413594,
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
            state_root,
            root_id,
            self._record_for_source_leaf(records, "Show A").work_unit_id,
            media_type="tv",
            tmdb_id=101,
        )
        apply_work_unit_override(
            state_root,
            root_id,
            self._record_for_source_leaf(records, "Show B").work_unit_id,
            media_type="tv",
            tmdb_id=102,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_id)

        execute_new_work_units(runner, state_root, root_id)

        parents = {event["parent_path"] for event in events}
        self.assertEqual(parents, {"/library/番剧/Show A"})

    def test_multiple_tv_identities_are_direct_children_of_pure_container(self) -> None:
        files = {
            "/incoming/刀剑神域/第一季/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/刀剑神域/第二季/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/刀剑神域/外传GGO/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/刀剑神域/序列之争/movie.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, events = self._setup(files)
        root_id = self._root(runner, "/incoming/刀剑神域")
        analyze_root_boundaries(
            alist, "/incoming/刀剑神域", root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        self.assertEqual(len(records), 4)
        for leaf, media_type, tmdb_id in (
            ("第一季", "tv", 45782),
            ("第二季", "tv", 45782),
            ("外传GGO", "tv", 78204),
            ("序列之争", "movie", 413594),
        ):
            apply_work_unit_override(
                state_root,
                root_id,
                self._record_for_source_leaf(records, leaf).work_unit_id,
                media_type=media_type,
                tmdb_id=tmdb_id,
            )
        reconcile_root_work_units(alist, "/library", state_root, root_id)

        execute_new_work_units(runner, state_root, root_id)

        self.assertEqual(len(events), 4)
        main = [e for e in events if e["tmdb_id"] == 45782]
        self.assertEqual({e["parent_path"] for e in main}, {"/library/番剧/刀剑神域"})
        # A second confirmed TV identity is a sibling, not a child of the
        # first TV.  A movie without a proved family marker is also a direct
        # child of the pure intake container.
        for event in events:
            self.assertEqual(event["parent_path"], "/library/番剧/刀剑神域")

    def test_oad_nests_under_unique_tmdb_alias_parent(self) -> None:
        files = {
            "/incoming/Collection/Main/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/Collection/Adventure/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/Collection/Adventure OAD/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, events = self._setup(files)
        root_id = self._root(runner, "/incoming/Collection")
        analyze_root_boundaries(
            alist, "/incoming/Collection", root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        self.assertEqual(len(records), 3)
        for leaf, tmdb_id, title, official, aliases in (
            ("Main", 100, "Collection Main", ["Collection Main"], []),
            ("Adventure", 200, "Adventure of Sinbad", ["Adventure of Sinbad"], ["Sinbad no Bouken"]),
            ("Adventure OAD", 300, "Adventure of Sinbad (OAD)", ["Adventure of Sinbad (OAD)"], ["Adventure of Sinbad OVA"]),
        ):
            record = self._record_for_source_leaf(records, leaf)
            apply_work_unit_override(
                state_root, root_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            current = load_work_unit_records(state_root, root_id)
            index = next(i for i, item in enumerate(current) if item.work_unit_id == record.work_unit_id)
            identity = dict(current[index].identity or {})
            identity.update({
                "title": title,
                "decision_trace": {
                    "official_titles": official,
                    "aliases_checked": aliases,
                },
            })
            current[index] = replace(current[index], identity=identity)
            save_work_unit_records(state_root, root_id, current)
        reconcile_root_work_units(alist, "/library", state_root, root_id)
        planned_layout = _container_layout_targets(
            runner, runner.get_job(root_id), load_work_unit_records(state_root, root_id),
        )
        oad = self._record_for_source_leaf(
            load_work_unit_records(state_root, root_id), "Adventure OAD",
        )
        adventure = self._record_for_source_leaf(
            load_work_unit_records(state_root, root_id), "Adventure",
        )
        self.assertEqual(planned_layout[oad.work_unit_id]["relation"], "nested_special")
        self.assertEqual(planned_layout[oad.work_unit_id]["parent_tmdb_id"], 200)
        self.assertEqual(
            planned_layout[adventure.work_unit_id]["parent_path"],
            "/library/番剧/Collection",
        )
        self.assertEqual(
            planned_layout[oad.work_unit_id]["parent_path"],
            "/library/番剧/Collection/Adventure of Sinbad",
        )
        self.assertEqual(
            planned_layout[oad.work_unit_id]["target_root"],
            "/library/番剧/Collection/Adventure of Sinbad/Adventure of Sinbad (OAD)",
        )

        execute_new_work_units(runner, state_root, root_id)

        self.assertEqual(len(events), 3)
        oad_event = next(event for event in events if event["tmdb_id"] == 300)
        self.assertEqual(oad_event["parent_path"], "/library/番剧/Collection/Work (200)")


if __name__ == "__main__":
    unittest.main()
