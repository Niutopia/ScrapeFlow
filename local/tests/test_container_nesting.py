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

from local.scrapeflow_api.library_index import (
    SingleSeasonEpisodeProof,
    reconcile_root_work_units,
)
from local.scrapeflow_api.simple_engine_runner import EngineJob, SimpleEngineRunner
from local.scrapeflow_api.unit_execution import (
    _clean_container_name,
    _container_layout_targets,
    _container_plan,
    _is_physical_special_record,
    execute_new_work_units,
    load_work_acceptance,
)

from local.tests.test_library_index import IndexAList, StrictBareEpisodeTMDB
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
    def _setup(self, files: dict[str, bytes], tmdb: object | None = None):
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
            tmdb=tmdb if tmdb is not None else object(),
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

    def test_established_library_container_absorbs_a_later_root(self) -> None:
        """A later root joins the container the library already established.

        An earlier `钢之炼金术师` root wrote the 2003 series plus two films under
        `/library/番剧/钢之炼金术师/`.  When the FA remake and its own film come
        back as a new root, the single-TV rule planned the TV at the shelf root
        and produced the sibling tree `/library/番剧/钢之炼金术师 FA`.  The
        library's own structure is the evidence: work child directories and no
        season directory of its own means this is a container to join.
        """
        files = {
            "/incoming/钢之炼金术师/FA 全02集/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/钢之炼金术师/FA 全02集/S01E02.mkv": FAKE_VIDEO_BYTES,
            "/incoming/钢之炼金术师/剧场版/movie.mkv": FAKE_VIDEO_BYTES,
            # 早前根建立的容器：两个作品子目录，容器自身没有季目录
            "/library/番剧/钢之炼金术师/钢之炼金术师/Season 01/old.mkv": FAKE_VIDEO_BYTES,
            "/library/番剧/钢之炼金术师/钢之炼金术师-香巴拉的征服者 (2005)/old.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, events = self._setup(files)
        root_id = self._root(runner, "/incoming/钢之炼金术师")
        analyze_root_boundaries(
            alist, "/incoming/钢之炼金术师", root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        self.assertEqual(len(records), 2, msg=[r.display_label for r in records])
        apply_work_unit_override(
            state_root, root_id,
            self._record_for_source_leaf(records, "FA 全02集").work_unit_id,
            media_type="tv", tmdb_id=31911,
        )
        apply_work_unit_override(
            state_root, root_id,
            self._record_for_source_leaf(records, "剧场版").work_unit_id,
            media_type="movie", tmdb_id=80518,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_id)
        execute_new_work_units(runner, state_root, root_id)

        parents = {event["parent_path"] for event in events}
        self.assertEqual(parents, {"/library/番剧/钢之炼金术师"})
        self.assertNotIn("/library/番剧", parents)

    def test_existing_work_root_with_seasons_is_not_treated_as_a_container(self) -> None:
        """A plain work root owning its seasons keeps the ordinary layout."""
        files = {
            "/incoming/刀剑神域/第一季/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/刀剑神域/序列之争/movie.mkv": FAKE_VIDEO_BYTES,
            # 已有的是作品根本体（自己带季目录），不是容器
            "/library/番剧/刀剑神域/Season 01/old.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, events = self._setup(files)
        root_id = self._root(runner, "/incoming/刀剑神域")
        analyze_root_boundaries(
            alist, "/incoming/刀剑神域", root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        apply_work_unit_override(
            state_root, root_id,
            self._record_for_source_leaf(records, "第一季").work_unit_id,
            media_type="tv", tmdb_id=45782,
        )
        apply_work_unit_override(
            state_root, root_id,
            self._record_for_source_leaf(records, "序列之争").work_unit_id,
            media_type="movie", tmdb_id=413594,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_id)
        execute_new_work_units(runner, state_root, root_id)
        self.assertIn("/library/番剧", {event["parent_path"] for event in events})

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

    def test_multi_season_main_tv_owns_root_with_single_season_tv_child(self) -> None:
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
        # Model the durable B/W season ownership used by the real pipeline.
        # Two different TV identities alone are not enough to nominate a
        # main series; the same identity must be proved across several
        # seasons while every sibling TV is single-season.
        current = load_work_unit_records(state_root, root_id)
        season_by_leaf = {"第一季": 1, "第二季": 2, "外传GGO": 1}
        current = [
            replace(
                record,
                claimed_seasons=(season_by_leaf[record.display_label],),
            )
            if record.display_label in season_by_leaf
            else record
            for record in current
        ]
        save_work_unit_records(state_root, root_id, current)
        reconcile_root_work_units(alist, "/library", state_root, root_id)

        current = load_work_unit_records(state_root, root_id)
        _ordered, container_parent, main_tmdb = _container_plan(
            runner, runner.get_job(root_id), current,
        )
        layout = _container_layout_targets(
            runner, runner.get_job(root_id), current,
        )

        self.assertEqual(main_tmdb, 45782)
        self.assertIsNone(container_parent)
        for record in current:
            tmdb_id = (record.identity or {}).get("tmdb_id")
            if tmdb_id == 45782:
                self.assertEqual(layout[record.work_unit_id]["relation"], "main_tv")
                self.assertEqual(
                    layout[record.work_unit_id]["parent_path"], "/library/番剧",
                )
            else:
                self.assertEqual(
                    layout[record.work_unit_id]["relation"], "nested_under_main",
                )

    def test_rick_shaped_root_uses_main_tmdb_title_not_release_bundle_name(self) -> None:
        source = "/incoming/瑞克和MD 1-9季+日漫版 内封+内嵌字幕 4K+1080P"
        season_names = ("一", "二", "三", "四", "五", "六", "七", "八", "九")
        files = {
            f"{source}/第{name}季（20{season:02d}）全1集 内封字幕/"
            f"S{season:02d}E01.mkv": FAKE_VIDEO_BYTES
            for season, name in enumerate(season_names, 1)
        }
        anime_scope = f"{source}/瑞克和莫蒂：日漫版（2024）全1集"
        files[f"{anime_scope}/S01E01.mkv"] = FAKE_VIDEO_BYTES
        state_root, alist, runner, _events = self._setup(files)
        root_id = self._root(runner, source, shelf="us_tv")
        analyze_root_boundaries(
            alist, source, root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        self.assertEqual(len(records), 2)
        main = next(record for record in records if len(record.claimed_seasons) > 1)
        anime = next(record for record in records if record.work_unit_id != main.work_unit_id)
        self.assertEqual(anime.claimed_seasons, (1,))
        apply_work_unit_override(
            state_root, root_id, main.work_unit_id,
            media_type="tv", tmdb_id=60625,
        )
        apply_work_unit_override(
            state_root, root_id, anime.work_unit_id,
            media_type="tv", tmdb_id=202282,
        )
        current = load_work_unit_records(state_root, root_id)
        normalized = []
        for record in current:
            identity = dict(record.identity or {})
            if record.work_unit_id == main.work_unit_id:
                identity["title"] = "瑞克和莫蒂"
                normalized.append(replace(record, identity=identity))
            else:
                identity["title"] = "瑞克和莫蒂：日漫版"
                normalized.append(replace(record, identity=identity))
        current = normalized
        save_work_unit_records(state_root, root_id, current)

        ordered, container_parent, main_tmdb = _container_plan(
            runner, runner.get_job(root_id), current,
        )
        layout = _container_layout_targets(
            runner, runner.get_job(root_id), current,
        )

        self.assertEqual(main_tmdb, 60625)
        self.assertIsNone(container_parent)
        self.assertEqual(ordered[0].work_unit_id, main.work_unit_id)
        self.assertEqual(layout[main.work_unit_id]["relation"], "main_tv")
        self.assertEqual(
            layout[main.work_unit_id]["target_root"],
            "/library/欧美剧/瑞克和莫蒂",
        )
        self.assertEqual(
            layout[anime.work_unit_id]["relation"],
            "nested_under_main",
        )
        self.assertNotIn(
            "瑞克和MD 1-9季+日漫版",
            str(layout[main.work_unit_id]["target_root"]),
        )

    def test_special_or_version_record_never_becomes_main_tv(self) -> None:
        files = {
            "/incoming/Same Identity/Regular/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/Same Identity/Backup/S02E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _events = self._setup(files)
        root_id = self._root(runner, "/incoming/Same Identity")
        analyze_root_boundaries(
            alist, "/incoming/Same Identity",
            root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        regular = self._record_for_source_leaf(records, "Regular")
        backup = self._record_for_source_leaf(records, "Backup")
        current = [
            replace(
                regular,
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 700},
                claimed_seasons=(1,),
            ),
            replace(
                backup,
                role="version_group",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 700},
                claimed_seasons=(2,),
            ),
        ]
        save_work_unit_records(state_root, root_id, current)

        _ordered, container_parent, main_tmdb = _container_plan(
            runner, runner.get_job(root_id), current,
        )
        layout = _container_layout_targets(
            runner, runner.get_job(root_id), current,
        )
        self.assertIsNone(main_tmdb)
        self.assertIsNotNone(container_parent)
        self.assertNotIn(
            "main_tv",
            {item["relation"] for item in layout.values()},
        )

    def test_stale_uncertain_tv_identity_cannot_steal_main_root(self) -> None:
        files = {
            "/incoming/Stale Identity/Main/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/Stale Identity/Confirmed/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _events = self._setup(files)
        root_id = self._root(runner, "/incoming/Stale Identity")
        analyze_root_boundaries(
            alist, "/incoming/Stale Identity",
            root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        stale = self._record_for_source_leaf(records, "Main")
        confirmed = self._record_for_source_leaf(records, "Confirmed")
        current = [
            replace(
                stale,
                identity_status="uncertain",
                identity={"media_type": "tv", "tmdb_id": 701},
                claimed_seasons=(1, 2),
            ),
            replace(
                confirmed,
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 702},
                claimed_seasons=(1,),
            ),
        ]
        save_work_unit_records(state_root, root_id, current)
        _ordered, container_parent, main_tmdb = _container_plan(
            runner, runner.get_job(root_id), current,
        )
        self.assertEqual(main_tmdb, 702)
        self.assertIsNone(container_parent)

    def test_bundled_ova_files_do_not_demote_a_multiseason_main(self) -> None:
        """A verified multi-season cohort keeps main-TV candidacy despite OVA extras.

        A release cohort routinely bundles OVA files beside the regular
        episodes.  Those marker files are extra coverage of the same work, so
        the cohort must stay a regular TV: demoting it would hand the
        container's main slot to an unrelated single-season sibling and plan
        the regular seasons below that sibling's work root.
        """
        source = "/incoming/示例合集"
        files = {
            f"{source}/示例剧 第一季/S01E01.mkv": FAKE_VIDEO_BYTES,
            f"{source}/示例剧 第一季/S01E11(OVA).mkv": FAKE_VIDEO_BYTES,
            f"{source}/示例剧 第二季/S02E01.mkv": FAKE_VIDEO_BYTES,
            f"{source}/示例剧 第三季/S03E01.mkv": FAKE_VIDEO_BYTES,
            f"{source}/示例剧 爆焰外传/[Ygm] Example Spinoff [01][1080P].mkv": FAKE_VIDEO_BYTES,
            f"{source}/示例剧 剧场版 传说/movie.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _events = self._setup(files)
        root_id = self._root(runner, source)
        analyze_root_boundaries(
            alist, source, root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        cohort = [
            record for record in records
            if record.claimed_seasons == (1, 2, 3)
        ]
        self.assertEqual(len(cohort), 1)
        main = cohort[0]
        spinoff = self._record_for_source_leaf(records, "示例剧 爆焰外传")
        movie = self._record_for_source_leaf(records, "示例剧 剧场版 传说")
        current = [
            replace(
                main,
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 9202,
                    "title": "示例剧",
                    "decision_trace": {
                        "physical_special_markers": ["OVA"],
                        "official_titles": ["示例剧"],
                    },
                },
                reconciliation_outcome="new_work",
            ),
            replace(
                spinoff,
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 9101,
                    "title": "示例剧 爆焰",
                    "decision_trace": {
                        "official_titles": ["示例剧 爆焰"],
                    },
                },
                reconciliation_outcome="duplicate_complete",
                matched_work_root="/library/番剧/示例合集/示例剧 爆焰",
            ),
            replace(movie, identity_status="uncertain", identity=None),
        ]
        save_work_unit_records(state_root, root_id, current)

        # The single-season marker shape stays demotable: the boundary can
        # derive one claimed season from a season word in the directory name
        # alone (``第二季 OVA``), so only the ≥2 structural proof wins.
        self.assertFalse(_is_physical_special_record(current[0]))
        self.assertTrue(
            _is_physical_special_record(
                replace(current[0], claimed_seasons=(1,))
            )
        )

        _ordered, container_parent, main_tmdb = _container_plan(
            runner, runner.get_job(root_id), current,
        )
        layout = _container_layout_targets(
            runner, runner.get_job(root_id), current,
        )
        self.assertIsNone(main_tmdb)
        self.assertEqual(container_parent, "/library/番剧/示例合集")
        self.assertEqual(layout[main.work_unit_id]["relation"], "direct_tv")
        self.assertEqual(
            layout[main.work_unit_id]["target_root"],
            "/library/番剧/示例合集/示例剧",
        )
        self.assertNotIn(
            "示例剧 爆焰",
            str(layout[main.work_unit_id]["target_root"]),
        )

    def test_proved_split_season_parts_stay_main_tv_despite_special_markers(self) -> None:
        """D-proved split-season parts are regular seasons, not auxiliaries.

        A split-season release ships one directory per official season of the
        same TMDB identity, and those directories may still bundle OVA files
        (``physical_special_markers``).  When every part carries a durable
        single-season episode proof, the parts must stay main-TV candidates
        that collectively own the shelf-root family: demoting the marked parts
        would leave no main-TV candidate at all and chain each season under
        the previous sibling's work root instead of one shared show dir.
        """
        source = "/incoming/某科学的超电磁炮"
        files = {
            f"{source}/某科学的超电磁炮/[Group] Railgun [01].mkv": FAKE_VIDEO_BYTES,
        }
        for season, leaf in ((2, "某科学的超电磁炮 S"), (3, "某科学的超电磁炮 T")):
            for episode in range(1, season + 1):
                files[f"{source}/{leaf}/[Group] Railgun [{episode:02d}].mkv"] = (
                    FAKE_VIDEO_BYTES
                )
        tmdb = StrictBareEpisodeTMDB(30977, {1: 1, 2: 2, 3: 3})
        state_root, alist, runner, events = self._setup(files, tmdb=tmdb)
        root_id = self._root(runner, source)
        analyze_root_boundaries(
            alist, source, root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        self.assertEqual(len(records), 3)

        season_by_leaf = {
            "某科学的超电磁炮": 1,
            "某科学的超电磁炮 S": 2,
            "某科学的超电磁炮 T": 3,
        }

        def proof_for(season: int) -> dict[str, object]:
            return SingleSeasonEpisodeProof(
                tmdb_id=30977,
                season=season,
                episode_count=season,
                episode_tokens=tuple(
                    f"S{season:02d}E{episode:02d}"
                    for episode in range(1, season + 1)
                ),
                evidence_kind="tmdb_single_positive_season_bracketed_episodes",
            ).as_dict()

        current = []
        for record in records:
            leaf = record.source_paths[0].rstrip("/").rsplit("/", 1)[-1]
            markers = ["OVA"] if leaf in {"某科学的超电磁炮 S", "某科学的超电磁炮 T"} else []
            current.append(replace(
                record,
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 30977,
                    "title": "某科学的超电磁炮",
                    "decision_trace": {
                        "physical_special_markers": markers,
                        "official_titles": ["某科学的超电磁炮"],
                    },
                },
                reconciliation_outcome="new_work",
                reconciliation_evidence=proof_for(season_by_leaf[leaf]),
            ))
        save_work_unit_records(state_root, root_id, current)

        # The D proof defeats the physical-special demotion for every part…
        for record in current:
            self.assertFalse(_is_physical_special_record(record))
        # …while a marker-bearing record without the proof stays demotable.
        self.assertTrue(
            _is_physical_special_record(
                replace(current[1], reconciliation_evidence=None)
            )
        )

        _ordered, container_parent, main_tmdb = _container_plan(
            runner, runner.get_job(root_id), current,
        )
        layout = _container_layout_targets(runner, runner.get_job(root_id), current)
        self.assertEqual(main_tmdb, 30977)
        self.assertIsNone(container_parent)
        for record in current:
            self.assertEqual(layout[record.work_unit_id]["relation"], "main_tv")
            self.assertEqual(
                layout[record.work_unit_id]["parent_path"], "/library/番剧",
            )

        execute_new_work_units(runner, state_root, root_id)

        self.assertEqual(len(events), 3)
        self.assertEqual(
            {event["parent_path"] for event in events}, {"/library/番剧"},
        )

    def test_same_identity_specials_all_nest_under_the_main_sibling_root(self) -> None:
        """Two same-identity specials must not chain under each other.

        A special unit shares its parent's TMDB identity, and the family root
        map is keyed by that identity.  Without excluding nested specials from
        registering their own (deeper) roots, the second special would plan
        below the first special's directory instead of below the regular
        sibling's show root.
        """
        files = {
            "/incoming/Same Family/Main/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/Same Family/Main OVA/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/Same Family/Main SP/S01E01.mkv": FAKE_VIDEO_BYTES,
            # The main unit's planned work root must exist on the remote
            # before the nested specials can anchor to it.
            "/library/番剧/Work (500)/tvshow.nfo": b"<tvshow/>",
        }
        state_root, alist, runner, events = self._setup(files)
        root_id = self._root(runner, "/incoming/Same Family")
        analyze_root_boundaries(
            alist, "/incoming/Same Family",
            root_task_id=root_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_id)
        self.assertEqual(len(records), 3)
        current = []
        for record in records:
            leaf = record.source_paths[0].rstrip("/").rsplit("/", 1)[-1]
            markers = ["OVA"] if leaf == "Main OVA" else (["SP"] if leaf == "Main SP" else [])
            current.append(replace(
                record,
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 500,
                    "title": "Main",
                    "decision_trace": {
                        "physical_special_markers": markers,
                        "official_titles": ["Main"],
                    },
                },
                reconciliation_outcome="new_work",
            ))
        save_work_unit_records(state_root, root_id, current)

        layout = _container_layout_targets(runner, runner.get_job(root_id), current)
        main = self._record_for_source_leaf(current, "Main")
        # The same-identity specials are the main work's own nested specials,
        # not contested ownership: the main TV owns the container root (the
        # 刀剑神域 single-work shape), so a same-identity OVA beside its
        # regular season never demotes the root to a container wrapper.
        self.assertEqual(layout[main.work_unit_id]["relation"], "main_tv")
        for leaf in ("Main OVA", "Main SP"):
            special = self._record_for_source_leaf(current, leaf)
            self.assertEqual(
                layout[special.work_unit_id]["relation"], "nested_under_main",
            )
            # The parent resolves at execution time from the main unit's
            # executed target root; the events below prove both specials
            # land inside the same family root as the main unit.

        execute_new_work_units(runner, state_root, root_id)

        self.assertEqual(len(events), 3)
        family_root = "/library/番剧/Work (500)"
        for event in events:
            self.assertIn(event["parent_path"], {"/library/番剧", family_root})
        special_events = [
            event for event in events
            if event["parent_path"] == family_root
        ]
        self.assertEqual(len(special_events), 2)

    def test_fate_sub_series_prefix_grouping_tree(self) -> None:
        """The operator-confirmed 2026-08-30 franchise sub-series tree.

        命运之夜 family (2006 TV double-nested under its own label dir),
        命运-冠位指定 family, 空之境界 collection-as-subseries, 伊莉雅 movies
        inside the TV's own root, 黎明低语 inside 奇异赝品, seven single
        works flat. 冠位嘉年华 never folds into the FGO stem.
        """
        from local.scrapeflow_api.unit_execution import _sub_series_parents

        def rec(uid, mt, tid, title):
            return WorkUnitRecord(
                work_unit_id=uid, boundary_key=f"/x/{uid}",
                source_paths=(f"/x/{uid}",), display_label=title,
                role="series_container", media_context="tv",
                identity={"media_type": mt, "tmdb_id": tid, "title": title},
                claimed_seasons=(), root_task_id="r", source_revision=1,
            )

        records = [
            rec("a", "tv", 37858, "命运之夜"),
            rec("b", "tv", 45845, "命运之夜 前传"),
            rec("c", "tv", 61415, "命运之夜 无限剑制"),
            rec("d", "movie", 46304, "命运之夜-无限剑制 剧场版"),
            rec("e", "movie", 283984, "命运之夜——天之杯Ⅰ：恶兆之花"),
            rec("f", "movie", 428142, "命运／冠位指定 -序章-"),
            rec("g", "tv", 90677, "命运／冠位指定 绝对魔兽战线巴比伦尼亚"),
            rec("h", "movie", 637202, "命运／冠位指定 -神圣圆桌领域卡美洛- 前篇 漂泊的银之臂"),
            rec("i", "movie", 23150, "空之境界 第一章 俯瞰风景"),
            rec("j", "movie", 47747, "空之境界 终章"),
            rec("k", "tv", 63576, "魔法少女☆伊莉雅"),
            rec("l", "movie", 461083, "命运／万华描绘者 魔法少女☆伊莉雅剧场版 雪下的誓言"),
            rec("m", "tv", 229858, "命运／奇异赝品"),
            rec("n", "movie", 1145612, "命运／奇异赝品 黎明低语"),
            rec("o", "tv", 132848, "命运-冠位嘉年华"),
            rec("p", "tv", 76047, "卫宫家今天的饭"),
        ]
        out = _sub_series_parents("/番剧/Fate", records)
        byid = {r.work_unit_id: (r.identity or {}).get("title") for r in records}
        self.assertEqual(out["a"], "/番剧/Fate/命运之夜")
        self.assertEqual(out["b"], "/番剧/Fate/命运之夜")
        self.assertEqual(out["d"], "/番剧/Fate/命运之夜")
        self.assertEqual(out["f"], "/番剧/Fate/命运-冠位指定")
        self.assertEqual(out["g"], "/番剧/Fate/命运-冠位指定")
        self.assertEqual(out["h"], "/番剧/Fate/命运-冠位指定")
        self.assertEqual(out["i"], "/番剧/Fate/空之境界")
        self.assertEqual(out["j"], "/番剧/Fate/空之境界")
        self.assertEqual(out["l"], "/番剧/Fate/魔法少女☆伊莉雅")
        self.assertNotIn("k", out)  # TV root is its own family anchor
        self.assertEqual(out["n"], "/番剧/Fate/命运-奇异赝品")
        self.assertNotIn("m", out)
        for flat in ("o", "p"):
            self.assertNotIn(flat, out, byid[flat])

    def test_library_anchor_nests_a_lone_later_root_work(self) -> None:
        """A lone work joins a family directory the library already holds.

        巴比伦尼亚's shape: an earlier root built 命运-冠位指定/ (序章, 月光,
        所罗门, 卡美洛), and a later root carries only the Babylonia TV.  The
        current-root-only rule left it flat on the container; an existing
        library directory whose normalized name is a separator-bounded strict
        prefix of the work's title anchors it into the family.
        """
        from local.scrapeflow_api.unit_execution import _sub_series_parents

        def rec(uid, mt, tid, title):
            return WorkUnitRecord(
                work_unit_id=uid, boundary_key=f"/x/{uid}",
                source_paths=(f"/x/{uid}",), display_label=title,
                role="series_container", media_context="tv",
                identity={"media_type": mt, "tmdb_id": tid, "title": title},
                claimed_seasons=(), root_task_id="r", source_revision=1,
            )

        records = [
            rec("b", "tv", 90677, "命运-冠位指定 绝对魔兽战线巴比伦尼亚"),
            rec("c", "tv", 76047, "卫宫家今天的饭"),
        ]
        existing = [
            "命运-冠位指定",
            "命运-冠位指定-序章 (2016)",
            "命运-冠位指定 -月光-失落之室- (2017)",
            "卫宫家今天的饭",
            "poster.jpg",
        ]
        out = _sub_series_parents("/番剧/Fate", records, existing_children=existing)
        self.assertEqual(out["b"], "/番剧/Fate/命运-冠位指定")
        # An existing directory equal to the work's own root is the merge
        # path, never a family anchor.
        self.assertNotIn("c", out)

    def test_library_anchor_requires_an_existing_directory(self) -> None:
        """Without the existing family directory the work stays flat.

        Library evidence never invents a new top-level group: the anchor is
        an optimization of placement into what an earlier root built, so a
        family that does not exist yet keeps the fail-flat behaviour.
        """
        from local.scrapeflow_api.unit_execution import _sub_series_parents

        def rec(uid, mt, tid, title):
            return WorkUnitRecord(
                work_unit_id=uid, boundary_key=f"/x/{uid}",
                source_paths=(f"/x/{uid}",), display_label=title,
                role="series_container", media_context="tv",
                identity={"media_type": mt, "tmdb_id": tid, "title": title},
                claimed_seasons=(), root_task_id="r", source_revision=1,
            )

        records = [
            rec("b", "tv", 90677, "命运-冠位指定 绝对魔兽战线巴比伦尼亚"),
        ]
        out = _sub_series_parents("/番剧/Fate", records, existing_children=["卫宫家今天的饭"])
        self.assertNotIn("b", out)

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

    def test_collection_label_equal_to_container_root_never_doubles(self) -> None:
        """A collection whose label IS the container collapses onto it.

        紫罗兰永恒花园's shape: the library already holds the TV work root
        (so the container candidate is a work root, not a container), a later
        intake carries only the two films, and TMDB puts both films in the
        「紫罗兰永恒花园（系列）」 collection whose cleaned label equals the
        container's own name.  Nesting that collection would create
        ``/番剧/紫罗兰永恒花园/紫罗兰永恒花园/…`` — a same-named nested
        duplicate of the shelf rule the 空之境界 anchor case already rejects
        at the sub-series level.  The members must land directly inside the
        container.
        """

        class CollectionTMDB:
            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == "/movie/533514":
                    return {
                        "title": "紫罗兰永恒花园 剧场版",
                        "release_date": "2020-09-18",
                        "belongs_to_collection": {
                            "id": 1431054,
                            "name": "紫罗兰永恒花园（系列）",
                            "poster_path": "/collection-poster.jpg",
                            "backdrop_path": "/collection-backdrop.jpg",
                        },
                    }
                if path == "/movie/610892":
                    return {
                        "title": "紫罗兰永恒花园外传：永远与自动手记人偶",
                        "release_date": "2019-09-06",
                        "belongs_to_collection": {
                            "id": 1431054,
                            "name": "紫罗兰永恒花园（系列）",
                            "poster_path": "/collection-poster.jpg",
                            "backdrop_path": "/collection-backdrop.jpg",
                        },
                    }
                return {}

        source = "/incoming/紫罗兰永恒花园"
        files = {
            # The library work root already exists with its season — the
            # container candidate is therefore a work root, and the pure
            # container path names the same directory.
            "/library/番剧/紫罗兰永恒花园/Season 01/S01E01.mkv": FAKE_VIDEO_BYTES,
            f"{source}/紫罗兰永恒花园 剧场版 (2020)/movie.mkv": FAKE_VIDEO_BYTES,
            f"{source}/紫罗兰永恒花园外传-永远与自动手记人偶 (2019)/movie.mkv":
                FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner, _events = self._setup(files, tmdb=CollectionTMDB())
        root_id = self._root(runner, source)
        analyze_root_boundaries(
            _alist, source,
            root_task_id=root_id, state_root=state_root,
        )
        current = load_work_unit_records(state_root, root_id)
        self.assertEqual(len(current), 2)
        revised = []
        for leaf, tmdb_id, title in (
            ("紫罗兰永恒花园 剧场版 (2020)", 533514, "紫罗兰永恒花园 剧场版"),
            ("紫罗兰永恒花园外传-永远与自动手记人偶 (2019)", 610892,
             "紫罗兰永恒花园外传：永远与自动手记人偶"),
        ):
            record = self._record_for_source_leaf(current, leaf)
            revised.append(replace(
                record,
                identity_status="confirmed",
                identity={
                    "media_type": "movie",
                    "tmdb_id": tmdb_id,
                    "title": title,
                },
                reconciliation_outcome="new_work",
            ))
        save_work_unit_records(state_root, root_id, revised)
        current = revised

        layout = _container_layout_targets(
            runner, runner.get_job(root_id), current,
        )
        for record in current:
            target = layout[record.work_unit_id]
            self.assertEqual(
                target["parent_path"], "/library/番剧/紫罗兰永恒花园",
            )
            self.assertNotIn(
                "/紫罗兰永恒花园/紫罗兰永恒花园/",
                str(target["target_root"]),
            )
        roots = {
            (record.identity or {})["tmdb_id"]:
                layout[record.work_unit_id]["target_root"]
            for record in current
        }
        self.assertEqual(
            roots[533514], "/library/番剧/紫罗兰永恒花园/紫罗兰永恒花园 剧场版",
        )
        self.assertEqual(
            roots[610892],
            "/library/番剧/紫罗兰永恒花园/紫罗兰永恒花园外传-永远与自动手记人偶",
        )


if __name__ == "__main__":
    unittest.main()
