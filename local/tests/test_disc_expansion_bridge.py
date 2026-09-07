"""Focused regressions for the B→C disc-expansion bridge.

Covers the task-owned staging layout, the operator ruling store, scope
acceptance for expansion-born records, the orchestration pass (happy path,
park-on-unprovable, ruling rescue, per-scope transfer failure, pause
propagation, multi-source park), record serialization, and the terminal
staging consumption.  All fixtures are pure data plus in-memory doubles —
no filesystem writes outside ``tmp_path``, no network.
"""

from __future__ import annotations

import json
import posixpath
from types import SimpleNamespace

import pytest

from engine.scrapeflow import disc_expansion as de
from engine.scrapeflow import disc_expansion_bridge as bridge
from engine.scrapeflow.disc_image import (
    DiscInventory,
    DiscPlayItem,
    DiscPlaylist,
    InnerFile,
)
from engine.scrapeflow.placement import (
    EXPANSION_ROOT,
    PlacementDecision,
    placement_for,
    validate_routing,
)
from engine.scrapeflow.residual_policy import is_task_owned_staging_root
from engine.scrapeflow.source_inventory import (
    build_scoped_source_node,
    build_source_inventory,
)
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)
from local.tests.test_library_index import IndexAList

INGRESS = "/media/影视"
SCOPE = "/media/影视/无耻之徒 第一季"
SCOPE_BASENAME = "无耻之徒 第一季"
IMAGE = f"{SCOPE}/DISC1.iso"
ROOT_TASK = "root-1"
STAGING_ROOT = f"{INGRESS}/ScrapeFlow/展开/{ROOT_TASK}"
CLIP_SIZE = 15_000
IMAGE_SIZE = 40_000
EPISODE_MINUTES = (59, 61)


# ---------------------------------------------------------------- fakes


class DiscAList(IndexAList):
    """IndexAList plus the exact-info/upload/remove surface the bridge uses."""

    def __init__(self, files):
        super().__init__(files)
        self.removed: list[str] = []
        self.empty_dir_calls: list[str] = []

    def exact_file_info(self, path: str):
        payload = self.files.get(path)
        if payload is None:
            return None
        return {
            "name": posixpath.basename(path),
            "is_dir": False,
            "size": len(payload),
            "version": "v1",
        }

    def add_file(self, path: str, payload: bytes) -> None:
        current = ""
        for part in path.strip("/").split("/")[:-1]:
            current += "/" + part
            self.dirs.add(current)
        self.files[path] = payload

    def remove(self, parent: str, names: list[str]) -> None:
        for name in names:
            full = f"{parent.rstrip('/')}/{name}"
            self.removed.append(full)
            self.files.pop(full, None)

    def remove_empty_dir(self, path: str, refresh: bool = False) -> None:
        del refresh
        self.empty_dir_calls.append(path)
        if not self.list(path):
            self.dirs.discard(path.rstrip("/"))


class FakeExecutor:
    """Executor double that "uploads" into the fake AList and files state."""

    def __init__(self, alist: DiscAList, *, fail_images: frozenset[str] = frozenset()):
        self.alist = alist
        self.fail_images = fail_images
        self.states: dict[str, de.ExpansionTransferState] = {}
        self.calls: list[tuple[int, int]] = []

    def load_state(self, mapping):
        return self.states.get(mapping.target_path)

    def execute_mapping(self, mapping, *, inner_file):
        del inner_file
        self.calls.append((mapping.season, mapping.episode))
        if mapping.candidate.image_path in self.fail_images:
            raise de.DiscExpansionError("传输失败")
        state = de.ExpansionTransferState.from_mapping(mapping)
        state.status = "completed"
        state.inner_size = mapping.candidate.clip_size
        state.duration_seconds = mapping.candidate.duration_seconds
        state.output_bytes = mapping.candidate.clip_size
        state.md5 = "0" * 32
        state.sha1 = "0" * 40
        state.updated_at = "2026-09-03T00:00:00Z"
        self.states[mapping.target_path] = state
        self.alist.add_file(mapping.target_path, b"x" * mapping.candidate.clip_size)
        return state


def _inventory(
    image_path: str,
    durations_seconds=tuple(minutes * 60 for minutes in EPISODE_MINUTES),
) -> DiscInventory:
    clips = []
    playlists = []
    for index, duration in enumerate(durations_seconds, start=1):
        clip_id = f"{index:05d}"
        clips.append(InnerFile(
            inner_path=f"/BDMV/STREAM/{clip_id}.M2TS",
            size=CLIP_SIZE,
            extents=((100 * index, 10),),
        ))
        playlists.append(DiscPlaylist(
            inner_path=f"/BDMV/PLAYLIST/{clip_id}.mpls",
            play_items=(DiscPlayItem(
                clip_id=clip_id,
                codec_id="V_MPEG4",
                in_time=0,
                out_time=int(duration * 45_000),
            ),),
        ))
    return DiscInventory(
        image_path=image_path,
        kind="udf",
        inner_files=tuple(clips),
        structure="bdmv",
        playlists=tuple(playlists),
    )


def _snapshot_rows() -> list[dict[str, object]]:
    return [
        {"name": SCOPE_BASENAME, "is_dir": True, "full_path": SCOPE},
        {"name": "DISC1.iso", "is_dir": False, "size": IMAGE_SIZE, "full_path": IMAGE},
    ]


def _snapshot() -> dict[str, object]:
    return {"root": INGRESS, "rows": _snapshot_rows()}


def _parked_record(scope: str = SCOPE, *, root_task_id: str = ROOT_TASK) -> WorkUnitRecord:
    return WorkUnitRecord(
        work_unit_id=f"{root_task_id}::w{abs(hash(scope)) % 10000}",
        root_task_id=root_task_id,
        boundary_key=scope,
        source_paths=(scope,),
        source_revision=1,
        role="single_work",
        display_label=SCOPE_BASENAME,
        requires_content_expansion=True,
    )


def _plain_record(scope: str) -> WorkUnitRecord:
    return WorkUnitRecord(
        work_unit_id=f"{ROOT_TASK}::plain",
        root_task_id=ROOT_TASK,
        boundary_key=scope,
        source_paths=(scope,),
        source_revision=1,
        role="single_work",
        display_label=posixpath.basename(scope),
    )


def _roster_loader(*, minutes=EPISODE_MINUTES):
    def loader(_tmdb, _tmdb_id, requested_season):
        return de.SeasonEpisodeRoster(
            season=int(requested_season),
            episodes=tuple(
                (index + 1, value) for index, value in enumerate(minutes)
            ),
        )

    return loader


@pytest.fixture()
def wired(monkeypatch, tmp_path):
    """Wire the bridge onto fakes: identity, probe, executor, roster."""
    alist = DiscAList({IMAGE: b"i" * IMAGE_SIZE})
    executor = FakeExecutor(alist)
    monkeypatch.setattr(
        bridge, "_scope_identity",
        lambda *_args, **_kwargs: (34307, "无耻之徒"),
    )
    monkeypatch.setattr(
        bridge, "probe_disc_image_via_alist",
        lambda _alist, image_path, **_kwargs: _inventory(image_path),
    )
    return SimpleNamespace(
        alist=alist,
        executor=executor,
        state_root=tmp_path,
        monkeypatch=monkeypatch,
    )


# ---------------------------------------------------------------- layout


class TestStagingLayout:
    def test_staging_root_layout(self) -> None:
        assert bridge.expansion_staging_root("/quark/影视", ROOT_TASK) == (
            "/quark/影视/ScrapeFlow/展开/root-1"
        )

    def test_relative_media_root_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            bridge.expansion_staging_root("quark/影视", ROOT_TASK)

    def test_residual_policy_recognizes_expansion_staging(self) -> None:
        assert is_task_owned_staging_root(
            f"{STAGING_ROOT}/{SCOPE_BASENAME}/Season 01/无耻之徒 - S01E01.mkv"
        )

    def test_bare_marker_or_task_only_is_not_staging(self) -> None:
        assert not is_task_owned_staging_root(f"{INGRESS}/ScrapeFlow/展开")
        assert not is_task_owned_staging_root(STAGING_ROOT)
        assert not is_task_owned_staging_root(f"{INGRESS}/ScrapeFlow")


# ---------------------------------------------------------------- identity


class TestScopeIdentityContext:
    """The bridge's scope identity must see what the C lane would see."""

    def test_season_marked_scope_matches_as_tv_with_container_label(
        self, monkeypatch,
    ) -> None:
        root = "/media/影视/无耻之徒"
        scope = f"{root}/无耻之徒(美版) 第一季"
        image = f"{scope}/Shameless.US.S01-DISC1.iso"
        rows = [
            {"name": "无耻之徒(美版) 第一季", "is_dir": True, "full_path": scope},
            {
                "name": "Shameless.US.S01-DISC1.iso",
                "is_dir": False,
                "size": IMAGE_SIZE,
                "full_path": image,
            },
        ]
        inventory_root = build_source_inventory(rows, root)
        scoped = build_scoped_source_node(
            inventory_root,
            [scope],
            boundary_key=scope,
            display_label="无耻之徒(美版) 第一季",
        )
        record = WorkUnitRecord(
            work_unit_id="ctx-1",
            root_task_id=ROOT_TASK,
            boundary_key=scope,
            source_paths=(scope,),
            source_revision=1,
            role="single_work",
            display_label="无耻之徒(美版) 第一季",
            media_context="unknown",
            requires_content_expansion=True,
        )
        captured: dict[str, object] = {}

        def fake_auto_match(_tmdb, evidence, **kwargs):
            captured["evidence"] = evidence
            captured["kwargs"] = kwargs
            return (
                SimpleNamespace(media_type="tv", tmdb_id=34307, title="无耻之徒"),
                [],
            )

        monkeypatch.setattr(bridge, "auto_match_from_evidence", fake_auto_match)
        tmdb_id, title = bridge._scope_identity(
            object(),
            scoped,
            record,
            inventory_root=inventory_root,
            season=1,
            prefer_animation=False,
            min_confidence=0.70,
        )
        assert (tmdb_id, title) == (34307, "无耻之徒")
        evidence = captured["evidence"]
        # The scope's own season marker is TV evidence: a movie identity
        # cannot own a season, so the match stays inside the tv type.
        assert evidence.media_shape == "tv"
        # The intake container's label reaches the evidence the same way
        # the C lane's parent-label escalation would deliver it.
        assert evidence.parent_labels == ("无耻之徒",)
        # The image name contributes its romanized title query; the disc
        # ordinals stay out of it.
        assert "Shameless US" in evidence.representative_names

    def test_unmarked_scope_keeps_the_records_context(self, monkeypatch) -> None:
        root = "/media/影视/无耻之徒"
        scope = f"{root}/无耻之徒(美版) 电影盘"
        image = f"{scope}/Shameless.US.Movie.2014.BluRay.iso"
        rows = [
            {"name": "无耻之徒(美版) 电影盘", "is_dir": True, "full_path": scope},
            {
                "name": "Shameless.US.Movie.2014.BluRay.iso",
                "is_dir": False,
                "size": IMAGE_SIZE,
                "full_path": image,
            },
        ]
        inventory_root = build_source_inventory(rows, root)
        scoped = build_scoped_source_node(
            inventory_root,
            [scope],
            boundary_key=scope,
            display_label="无耻之徒(美版) 电影盘",
        )
        record = WorkUnitRecord(
            work_unit_id="ctx-2",
            root_task_id=ROOT_TASK,
            boundary_key=scope,
            source_paths=(scope,),
            source_revision=1,
            role="single_work",
            display_label="无耻之徒(美版) 电影盘",
            media_context="unknown",
            requires_content_expansion=True,
        )
        captured: dict[str, object] = {}

        def fake_auto_match(_tmdb, evidence, **kwargs):
            captured["evidence"] = evidence
            return (
                SimpleNamespace(media_type="tv", tmdb_id=34307, title="无耻之徒"),
                [],
            )

        monkeypatch.setattr(bridge, "auto_match_from_evidence", fake_auto_match)
        bridge._scope_identity(
            object(),
            scoped,
            record,
            inventory_root=inventory_root,
            season=None,
            prefer_animation=False,
            min_confidence=0.70,
        )
        # Without a season marker the record's own context decides.
        assert captured["evidence"].media_shape == "unknown"


# ---------------------------------------------------------------- rulings


class TestDiscRulings:
    def _ruling(self, scope: str = SCOPE) -> de.ScopeMappingRuling:
        return de.ScopeMappingRuling.from_mapping({
            "scope_path": scope,
            "season": 1,
            "assignments": [
                {"image_path": IMAGE, "playlist_inner_path": "/BDMV/PLAYLIST/00001.mpls", "episode": 1},
                {"image_path": IMAGE, "playlist_inner_path": "/BDMV/PLAYLIST/00002.mpls", "episode": 2},
            ],
            "operator": "operator",
            "note": "fixture",
            "filed_at": "2026-09-02T00:00:00+08:00",
        })

    def test_save_and_load_round_trip(self, tmp_path) -> None:
        bridge.save_disc_ruling(tmp_path, ROOT_TASK, self._ruling())
        loaded = bridge.load_disc_rulings(tmp_path, ROOT_TASK)
        assert set(loaded) == {SCOPE}
        assert loaded[SCOPE].assignments == self._ruling().assignments

    def test_save_replaces_same_scope(self, tmp_path) -> None:
        bridge.save_disc_ruling(tmp_path, ROOT_TASK, self._ruling())
        bridge.save_disc_ruling(tmp_path, ROOT_TASK, self._ruling("/media/影视/别的"))
        raw = json.loads(
            bridge.disc_rulings_path(tmp_path, ROOT_TASK).read_text(encoding="utf-8")
        )
        assert len(raw) == 2

    def test_absent_store_is_empty(self, tmp_path) -> None:
        assert bridge.load_disc_rulings(tmp_path, ROOT_TASK) == {}

    def test_malformed_store_fails_closed(self, tmp_path) -> None:
        path = bridge.disc_rulings_path(tmp_path, ROOT_TASK)
        path.write_text("{not json", encoding="utf-8")
        with pytest.raises(bridge.DiscExpansionBridgeError, match="损坏"):
            bridge.load_disc_rulings(tmp_path, ROOT_TASK)

    def test_duplicate_scope_fails_closed(self, tmp_path) -> None:
        bridge.save_disc_ruling(tmp_path, ROOT_TASK, self._ruling())
        path = bridge.disc_rulings_path(tmp_path, ROOT_TASK)
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw.append(dict(raw[0]))
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(bridge.DiscExpansionBridgeError, match="多条"):
            bridge.load_disc_rulings(tmp_path, ROOT_TASK)


# ---------------------------------------------------------------- scopes


class TestValidateUnitScopesForRoot:
    def test_ordinary_record_validates_against_ingress(self) -> None:
        record = _plain_record(f"{INGRESS}/别的作品")
        assert bridge.validate_unit_scopes_for_root(
            INGRESS, "/media/影视", ROOT_TASK, record,
        ) == (f"{INGRESS}/别的作品",)

    def test_ordinary_record_outside_ingress_fails(self) -> None:
        record = _plain_record("/somewhere/else")
        with pytest.raises(ValueError):
            bridge.validate_unit_scopes_for_root(
                INGRESS, "/media/影视", ROOT_TASK, record,
            )

    def test_expansion_record_validates_against_rederived_staging(self) -> None:
        record = replace_record(
            _plain_record(f"{STAGING_ROOT}/{SCOPE_BASENAME}"),
            disc_expansion={"basis": "duration-dp"},
        )
        assert bridge.validate_unit_scopes_for_root(
            INGRESS, "/media/影视", ROOT_TASK, record,
        ) == (f"{STAGING_ROOT}/{SCOPE_BASENAME}",)

    def test_expansion_marker_wins_over_a_wider_ingress(self) -> None:
        # The ingress here sits above the media root, so the foreign staging
        # tree is inside it; the expansion marker must still narrow the
        # record to this task's own staging root.
        record = replace_record(
            _plain_record("/media/影视/ScrapeFlow/展开/other-root/无耻之徒 第一季"),
            disc_expansion={"basis": "duration-dp"},
        )
        with pytest.raises(ValueError):
            bridge.validate_unit_scopes_for_root(
                "/media", "/media/影视", ROOT_TASK, record,
            )

    def test_foreign_task_staging_is_not_accepted(self) -> None:
        record = replace_record(
            _plain_record("/media/影视/ScrapeFlow/展开/other-root/无耻之徒 第一季"),
            disc_expansion={"basis": "duration-dp"},
        )
        with pytest.raises(ValueError):
            bridge.validate_unit_scopes_for_root(
                INGRESS, "/media/影视", ROOT_TASK, record,
            )


def replace_record(record: WorkUnitRecord, **changes) -> WorkUnitRecord:
    from dataclasses import replace

    return replace(record, **changes)


# ---------------------------------------------------------------- pass


class TestExpandRootDiscImages:
    def _run(self, wired, records, *, snapshot=None, pause_requested=None):
        return bridge.expand_root_disc_images(
            wired.alist,
            SimpleNamespace(),  # tmdb never reached: identity is patched
            wired.state_root,
            ROOT_TASK,
            records,
            snapshot if snapshot is not None else _snapshot(),
            media_root=INGRESS,
            executor=wired.executor,
            roster_loader=_roster_loader(),
            pause_requested=pause_requested,
        )

    def test_happy_path_converts_scope_and_merges_snapshot(self, wired) -> None:
        records = [_parked_record(), _plain_record(f"{INGRESS}/兄弟作品")]
        updated, changed = self._run(wired, records)

        assert changed is True
        converted = [r for r in updated if r.disc_expansion is not None]
        assert len(converted) == 1
        record = converted[0]
        provenance = record.disc_expansion
        assert provenance["basis"] == "duration-dp"
        assert provenance["tmdb_id"] == 34307
        assert provenance["season"] == 1
        assert provenance["source_scope"] == SCOPE
        assert provenance["staging_scope"] == f"{STAGING_ROOT}/{SCOPE_BASENAME}"
        assert [m["episode"] for m in provenance["members"]] == [1, 2]
        assert provenance["members"][0]["staged_path"] == (
            f"{STAGING_ROOT}/{SCOPE_BASENAME}/Season 01/无耻之徒 - S01E01.mkv"
        )
        assert record.expanded_from_scopes == (SCOPE,)
        assert not record.requires_content_expansion
        assert all(
            path.startswith(STAGING_ROOT) for path in record.source_paths
        )
        # The sibling record stays byte-identical and present.
        assert any(r.boundary_key == f"{INGRESS}/兄弟作品" for r in updated)
        assert len(updated) == 2

        # The persisted snapshot was widened and now includes staged rows.
        from engine.scrapeflow.root_boundaries import load_source_snapshot

        merged = load_source_snapshot(wired.state_root, ROOT_TASK)
        assert merged is not None
        assert merged["root"] == INGRESS
        staged_files = [
            row for row in merged["rows"]
            if str(row.get("full_path", "")).startswith(STAGING_ROOT)
        ]
        assert len(staged_files) >= 2  # season dir + episodes (+ scope dir)

    def test_second_pass_is_a_no_op(self, wired) -> None:
        records = [_parked_record()]
        updated, _changed = self._run(wired, records)
        again, changed_again = self._run(wired, updated)
        assert changed_again is False
        assert again == updated
        assert wired.executor.calls == [(1, 1), (1, 2)]

    def test_identity_tie_parks_one_scope_without_failing_the_root(self, wired) -> None:
        # AutoMatchAmbiguityError is a PlanError, none of the bridge's three
        # typed failures — historically it escaped the per-scope boundary and
        # failed the whole root.  The module's contract says the scope parks
        # and its siblings keep expanding.
        from engine.scrapeflow.identity_matching import AutoMatchAmbiguityError

        scope2 = f"{INGRESS}/无耻之徒 第二季"
        image2 = f"{scope2}/DISC1.iso"
        alist = DiscAList({IMAGE: b"i" * IMAGE_SIZE, image2: b"i" * IMAGE_SIZE})
        executor = FakeExecutor(alist)
        snapshot = {
            "root": INGRESS,
            "rows": [
                {"name": SCOPE_BASENAME, "is_dir": True, "full_path": SCOPE},
                {"name": "DISC1.iso", "is_dir": False, "size": IMAGE_SIZE, "full_path": IMAGE},
                {"name": posixpath.basename(scope2), "is_dir": True, "full_path": scope2},
                {"name": "DISC1.iso", "is_dir": False, "size": IMAGE_SIZE, "full_path": image2},
            ],
        }
        def identity(_tmdb, scoped_node, _record, **_kwargs):
            if str(scoped_node.path) == SCOPE:
                raise AutoMatchAmbiguityError(
                    "自动匹配前两名证据无法区分，拒绝自动选择: "
                    "tv/1906 无耻之徒 (100.0%); tv/34307 无耻之徒 (100.0%)",
                    candidates=[],
                )
            return (34307, "无耻之徒")

        wired.monkeypatch.setattr(bridge, "_scope_identity", identity)
        updated, changed = bridge.expand_root_disc_images(
            alist,
            SimpleNamespace(),
            wired.state_root,
            ROOT_TASK,
            [_parked_record(SCOPE), _parked_record(scope2)],
            snapshot,
            media_root=INGRESS,
            executor=executor,
            roster_loader=_roster_loader(),
        )
        assert changed is True
        parked = [r for r in updated if r.attention and "意外失败" in r.attention]
        assert len(parked) == 1
        assert parked[0].source_paths == (SCOPE,)
        assert parked[0].requires_content_expansion
        assert "无法区分" in parked[0].attention
        assert "AutoMatchAmbiguityError" in parked[0].attention
        converted = [r for r in updated if r.disc_expansion is not None]
        assert len(converted) == 1
        assert converted[0].source_paths[0].startswith(STAGING_ROOT)
        assert converted[0].source_paths[0] != parked[0].source_paths[0]

    def test_merged_tree_reaches_both_scopes_from_widened_root(self, wired) -> None:
        # The production shape: the ingress snapshot is rooted at the intake
        # directory (its walk lists children only), so widening the merged
        # root to the media root orphans BOTH chains — the ingress scope and
        # the staged scope — unless the merge synthesizes the missing
        # ancestor directory rows.  Regression for the 09-05 production
        # deadlock where every unit parked 来源范围在 B 快照中不存在 and the
        # closure was rescued by a manual snapshot patch.
        from engine.scrapeflow.root_boundaries import load_source_snapshot
        from engine.scrapeflow.source_inventory import (
            collect_all_files,
        )

        media_root = "/media/影视"
        ingress = f"{media_root}/待刮削/无耻之徒"
        nested_scope = f"{ingress}/{SCOPE_BASENAME}"
        nested_image = f"{nested_scope}/DISC1.iso"
        # Fresh doubles: the wired fixture's alist is bound to the flat path
        # shape, and IndexAList derives its dirs from the files at birth.
        alist = DiscAList({nested_image: b"i" * IMAGE_SIZE})
        executor = FakeExecutor(alist)
        snapshot = {
            "root": ingress,
            "rows": [
                {"name": SCOPE_BASENAME, "is_dir": True, "full_path": nested_scope},
                {"name": "DISC1.iso", "is_dir": False, "size": IMAGE_SIZE, "full_path": nested_image},
            ],
        }
        updated, changed = bridge.expand_root_disc_images(
            alist,
            SimpleNamespace(),
            wired.state_root,
            ROOT_TASK,
            [_parked_record(nested_scope)],
            snapshot,
            media_root=media_root,
            executor=executor,
            roster_loader=_roster_loader(),
        )
        assert changed is True
        converted = [r for r in updated if r.disc_expansion is not None]
        assert len(converted) == 1

        merged = load_source_snapshot(wired.state_root, ROOT_TASK)
        assert merged is not None
        assert merged["root"] == media_root
        inventory = build_source_inventory(merged["rows"], merged["root"])
        # The staged scope resolves from the widened root, files included.
        staged_paths = list(converted[0].source_paths)
        staged_node = build_scoped_source_node(
            inventory, staged_paths,
            boundary_key=staged_paths[0],
            display_label=SCOPE_BASENAME,
        )
        staged_files = [f.path for f in collect_all_files(staged_node)]
        assert any(p.endswith("无耻之徒 - S01E01.mkv") for p in staged_files)
        assert any(p.endswith("无耻之徒 - S01E02.mkv") for p in staged_files)
        # The ingress-side scope (a parked sibling re-entering the pipeline)
        # resolves too — its own ancestor chain was completed.
        build_scoped_source_node(
            inventory, [nested_scope],
            boundary_key=nested_scope,
            display_label=SCOPE_BASENAME,
        )

    def test_merge_snapshot_ancestor_completion_is_idempotent(self) -> None:
        ingress = "/media/影视/待刮削/无耻之徒"
        staged_scope = f"{STAGING_ROOT}/{SCOPE_BASENAME}"
        snapshot = {
            "root": ingress,
            "rows": [
                {"name": SCOPE_BASENAME, "is_dir": True, "full_path": f"{ingress}/{SCOPE_BASENAME}"},
            ],
        }
        staged_rows = [
            bridge._synthetic_dir_row(staged_scope),
            {"name": "Season 01", "is_dir": True, "full_path": f"{staged_scope}/Season 01"},
        ]
        first = bridge._merge_snapshot(snapshot, staged_rows, STAGING_ROOT)
        again = bridge._merge_snapshot(first, [], STAGING_ROOT)
        paths = [str(row.get("full_path")) for row in again["rows"]]
        assert len(paths) == len(set(paths))
        # Both ancestor chains exist exactly once after the second merge.
        for ancestor in (
            "/media/影视/待刮削",
            ingress,
            "/media/影视/ScrapeFlow",
            "/media/影视/ScrapeFlow/展开",
            "/media/影视/ScrapeFlow/展开/" + ROOT_TASK,
        ):
            assert paths.count(ancestor) == 1


    def test_unprovable_scope_parks_without_ruling(self, wired) -> None:
        # The roster runtimes drift far out of tolerance, so the discs alone
        # cannot prove the mapping and no ruling is on file.
        records = [_parked_record()]
        with wired.monkeypatch.context() as ctx:
            ctx.setattr(
                bridge, "probe_disc_image_via_alist",
                lambda _a, image_path, **_kw: _inventory(image_path),
            )
            updated, changed = bridge.expand_root_disc_images(
                wired.alist,
                SimpleNamespace(),
                wired.state_root,
                ROOT_TASK,
                records,
                _snapshot(),
                media_root=INGRESS,
                executor=wired.executor,
                roster_loader=_roster_loader(minutes=(30, 32)),
            )
        assert changed is False
        parked = updated[0]
        assert parked.disc_expansion is None
        assert parked.requires_content_expansion
        assert parked.attention is not None
        assert "无法从镜像自身证明" in parked.attention
        # No snapshot was rewritten.
        from engine.scrapeflow.root_boundaries import load_source_snapshot

        assert load_source_snapshot(wired.state_root, ROOT_TASK) is None

    def test_filed_ruling_rescues_unprovable_scope(self, wired) -> None:
        # The real S03 shape: in playlist-name order the durations misalign
        # with the roster beyond tolerance (the third playlist lands +137s on
        # E03), so the order-preserving DP counts zero assignments.  A filed
        # ruling may reorder — it is only bound per-assignment to stay within
        # tolerance of the published runtime.
        durations = (3520, 3189, 3317)
        roster_minutes = (58, 55, 53)
        with wired.monkeypatch.context() as ctx:
            ctx.setattr(
                bridge, "probe_disc_image_via_alist",
                lambda _a, image_path, **_kw: _inventory(image_path, durations),
            )
            # Without a ruling the scope parks.
            parked, changed = bridge.expand_root_disc_images(
                wired.alist,
                SimpleNamespace(),
                wired.state_root,
                ROOT_TASK,
                [_parked_record()],
                _snapshot(),
                media_root=INGRESS,
                executor=wired.executor,
                roster_loader=_roster_loader(minutes=roster_minutes),
            )
            assert changed is False
            assert "无法从镜像自身证明" in (parked[0].attention or "")

            bridge.save_disc_ruling(
                wired.state_root,
                ROOT_TASK,
                de.ScopeMappingRuling.from_mapping({
                    "scope_path": SCOPE,
                    "season": 1,
                    "assignments": [
                        {"image_path": IMAGE, "playlist_inner_path": "/BDMV/PLAYLIST/00001.mpls", "episode": 1},
                        {"image_path": IMAGE, "playlist_inner_path": "/BDMV/PLAYLIST/00003.mpls", "episode": 2},
                        {"image_path": IMAGE, "playlist_inner_path": "/BDMV/PLAYLIST/00002.mpls", "episode": 3},
                    ],
                    "operator": "operator",
                    "note": "fixture",
                    "filed_at": "2026-09-02T00:00:00+08:00",
                }),
            )
            updated, changed = bridge.expand_root_disc_images(
                wired.alist,
                SimpleNamespace(),
                wired.state_root,
                ROOT_TASK,
                [_parked_record()],
                _snapshot(),
                media_root=INGRESS,
                executor=wired.executor,
                roster_loader=_roster_loader(minutes=roster_minutes),
            )
        assert changed is True
        assert updated[0].disc_expansion["basis"] == "operator-ruling"
        assert [m["episode"] for m in updated[0].disc_expansion["members"]] == [1, 2, 3]

    def test_transfer_failure_parks_only_that_scope(self, wired) -> None:
        other_scope = "/media/影视/无耻之徒 第二季"
        other_image = f"{other_scope}/DISC9.iso"
        wired.executor.fail_images = frozenset({other_image})
        wired.alist.add_file(other_image, b"i" * IMAGE_SIZE)
        snapshot = {
            "root": INGRESS,
            "rows": _snapshot_rows() + [
                {"name": "无耻之徒 第二季", "is_dir": True, "full_path": other_scope},
                {"name": "DISC9.iso", "is_dir": False, "size": IMAGE_SIZE, "full_path": other_image},
            ],
        }
        records = [_parked_record(), _parked_record(other_scope)]
        updated, changed = self._run(wired, records, snapshot=snapshot)

        assert changed is True
        failed = [r for r in updated if r.attention and "传输失败" in r.attention]
        assert len(failed) == 1
        assert failed[0].source_paths == (other_scope,)
        assert failed[0].disc_expansion is None
        # The healthy sibling was converted and persisted.
        converted = [r for r in updated if r.disc_expansion is not None]
        assert len(converted) == 1
        assert converted[0].disc_expansion["source_scope"] == SCOPE
        assert (1, 2) in wired.executor.calls  # first scope fully transferred

    def test_pause_propagates_and_persists_nothing(self, wired) -> None:
        records = [_parked_record()]
        with pytest.raises(bridge.DiscExpansionPauseRequested):
            self._run(wired, records, pause_requested=lambda: True)
        from engine.scrapeflow.root_boundaries import load_source_snapshot

        assert load_source_snapshot(wired.state_root, ROOT_TASK) is None
        assert wired.executor.calls == []

    def test_probe_failure_parks_the_scope_not_the_root(self, wired) -> None:
        from engine.scrapeflow.disc_image import DiscImageError

        def broken_probe(_alist, image_path, **_kwargs):
            raise DiscImageError(f"镜像不可读: {image_path}")

        wired.monkeypatch.setattr(bridge, "probe_disc_image_via_alist", broken_probe)
        records = [_parked_record()]
        updated, changed = self._run(wired, records)
        assert changed is False
        assert "镜像探测失败" in (updated[0].attention or "")
        assert updated[0].disc_expansion is None

    def test_multi_source_scope_parks_for_manual_handling(self, wired) -> None:
        record = replace_record(
            _parked_record(), source_paths=(SCOPE, "/media/影视/别的范围"),
        )
        updated, changed = self._run(wired, [record])
        assert changed is False
        assert "人工处理" in (updated[0].attention or "")
        assert updated[0].disc_expansion is None


class TestPlacementLane:
    """F must accept expansion staging as a production source lane."""

    PRODUCTION_STAGING = (
        "/quark/影视/ScrapeFlow/展开/engine-1/无耻之徒 第一季/Season 01"
    )
    PRODUCTION_TARGET = "/quark/影视/欧美剧/无耻之徒"

    def test_expansion_scope_routes_to_category_root(self) -> None:
        context = validate_routing(
            self.PRODUCTION_STAGING, self.PRODUCTION_TARGET,
        )
        assert context.production_library
        assert context.category_root == "/quark/影视/欧美剧"

    def test_expansion_rule_is_labelled(self) -> None:
        decision = placement_for(
            self.PRODUCTION_STAGING, self.PRODUCTION_TARGET,
        )
        assert isinstance(decision, PlacementDecision)
        assert decision.rule == "system_expansion_to_direct_category"

    def test_expansion_marker_root_itself_is_refused(self) -> None:
        with pytest.raises(ValueError, match="补源/展开"):
            validate_routing(
                EXPANSION_ROOT, self.PRODUCTION_TARGET,
            )

    def test_unrelated_scrapeflow_subtree_is_still_refused(self) -> None:
        with pytest.raises(ValueError, match="补源/展开"):
            validate_routing(
                "/quark/影视/ScrapeFlow/别的", self.PRODUCTION_TARGET,
            )


class TestRecordSerialization:
    def test_disc_expansion_round_trip(self) -> None:
        record = replace_record(
            _plain_record(f"{STAGING_ROOT}/{SCOPE_BASENAME}"),
            disc_expansion={
                "basis": "duration-dp",
                "tmdb_id": 34307,
                "members": [{"episode": 1, "staged_path": "/x.mkv"}],
            },
            expanded_from_scopes=(SCOPE,),
        )
        payload = record.as_dict()
        assert payload["disc_expansion"]["basis"] == "duration-dp"
        assert payload["expanded_from_scopes"] == [SCOPE]
        parsed = WorkUnitRecord.from_dict(payload)
        assert parsed.disc_expansion == record.disc_expansion
        assert parsed.expanded_from_scopes == (SCOPE,)

    def test_legacy_payload_without_fields_parses(self) -> None:
        payload = _plain_record(f"{INGRESS}/x").as_dict()
        payload.pop("disc_expansion")
        payload.pop("expanded_from_scopes")
        parsed = WorkUnitRecord.from_dict(payload)
        assert parsed.disc_expansion is None
        assert parsed.expanded_from_scopes == ()


# ---------------------------------------------------------------- terminal


class _CleanupRunner:
    def __init__(self, alist: DiscAList, library_root: str) -> None:
        self.alist = alist
        self.library_root = library_root

    def source_directory_exists(self, path: str) -> bool:
        parent, name = posixpath.split(path.rstrip("/"))
        return any(
            item.get("is_dir") and item.get("name") == name
            for item in self.alist.list(parent, refresh=True)
        )


STAGED_FILE = f"{STAGING_ROOT}/{SCOPE_BASENAME}/Season 01/无耻之徒 - S01E01.mkv"


def _expansion_record() -> WorkUnitRecord:
    return replace_record(
        _plain_record(f"{STAGING_ROOT}/{SCOPE_BASENAME}"),
        disc_expansion={
            "basis": "duration-dp",
            "members": [{"episode": 1, "staged_path": STAGED_FILE}],
        },
    )


class TestCleanupExpansionStagingRoot:
    def _cleanup(self, tmp_path, alist, *, pause_requested=None):
        from local.scrapeflow_api.root_pipeline import _cleanup_expansion_staging_root

        runner = _CleanupRunner(alist, INGRESS)
        save_work_unit_records(tmp_path, ROOT_TASK, [_expansion_record()])
        return _cleanup_expansion_staging_root(
            runner,
            tmp_path,
            SimpleNamespace(id=ROOT_TASK),
            pause_requested=pause_requested,
        )

    def test_declared_survivor_and_dirs_are_consumed(self, tmp_path) -> None:
        alist = DiscAList({})
        alist.add_file(STAGED_FILE, b"x" * 10)
        note = self._cleanup(tmp_path, alist)
        assert note is None
        assert alist.removed == [STAGED_FILE]
        assert STAGED_FILE not in alist.files
        assert not any(
            path.startswith(STAGING_ROOT) for path in alist.files
        )
        # Every staging directory was offered for removal.
        assert f"{STAGING_ROOT}/{SCOPE_BASENAME}/Season 01" in alist.empty_dir_calls

    def test_undeclared_file_keeps_the_tree(self, tmp_path) -> None:
        stray = f"{STAGING_ROOT}/{SCOPE_BASENAME}/Season 01/不明的残留.mkv"
        alist = DiscAList({})
        alist.add_file(STAGED_FILE, b"x" * 10)
        alist.add_file(stray, b"y" * 10)
        note = self._cleanup(tmp_path, alist)
        assert note is not None and "未声明" in note
        assert STAGED_FILE in alist.files
        assert stray in alist.files
        assert alist.removed == []

    def test_no_expansion_records_means_no_cleanup(self, tmp_path) -> None:
        from local.scrapeflow_api.root_pipeline import _cleanup_expansion_staging_root

        alist = DiscAList({})
        alist.add_file(STAGED_FILE, b"x" * 10)
        save_work_unit_records(tmp_path, ROOT_TASK, [_plain_record(f"{INGRESS}/x")])
        runner = _CleanupRunner(alist, INGRESS)
        assert _cleanup_expansion_staging_root(
            runner, tmp_path, SimpleNamespace(id=ROOT_TASK),
        ) is None
        assert STAGED_FILE in alist.files

    def test_pause_leaves_the_tree_for_rerun(self, tmp_path) -> None:
        alist = DiscAList({})
        alist.add_file(STAGED_FILE, b"x" * 10)
        note = self._cleanup(tmp_path, alist, pause_requested=lambda: True)
        assert note is not None and "暂停边界" in note
        assert STAGED_FILE in alist.files
        assert alist.removed == []

