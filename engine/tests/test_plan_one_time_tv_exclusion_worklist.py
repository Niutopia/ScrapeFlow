from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from engine.scrapeflow.one_time_library_completion import (
    build_one_time_library_plan,
    build_one_time_title_batches,
    build_one_time_worklist,
    canonical_digest,
    one_time_worklist_is_valid,
    seal_one_time_worklist,
)
from engine.scrapeflow.one_time_movie_member_scope import validate_exact_movie_member_scope
from engine.tools.plan_one_time_movie_scope_worklist import build_phase_two_movie_worklist
from engine.tools.plan_one_time_tv_exclusion_worklist import (
    _write_new_json,
    build_phase_three_tv_exclusion_worklist,
    phase_three_tv_exclusion_worklist_is_valid,
)


class PhaseThreeTVExclusionWorklistTests(unittest.TestCase):
    movie_stem = "/quark/影视/番剧/Parent-00/Movie (2026)"

    def movie_project(self) -> dict:
        return {
            "title": "Movie", "official_title": "Movie", "tmdb_ids": [900],
            "media_type": "movie", "target_root": self.movie_stem,
            "video_files": [self.movie_stem + ".mkv"], "issues": [],
            "regular_missing": [], "optional_missing": [],
            "metadata_issues": [{"code": "subtitle_gap"}],
        }

    def tv_batch(self, number: int, *, contained: bool = False) -> dict:
        root = f"/quark/影视/番剧/Parent-{number:02d}"
        nested = (
            [root + "/黑执事", root + "/黑执事/OVA"]
            if contained else
            [self.movie_stem] if number == 0 else [root + "/Nested Movie"]
        )
        identity_core = {
            "status": "exact", "tmdb_id": 1000 + number,
            "media_type": "tv", "title": f"Parent {number}",
        }
        return {
            "target_root": root, "category": "番剧",
            "identity": {**identity_core, "identity_sha256": canonical_digest(identity_core)},
            "discovery_lanes": [], "title_work_key": f"{1000 + number:064x}",
            "first_action": "blocked_scope_review", "mutation_allowed": False,
            "read_only_audit_allowed": False,
            "scope_blockers": [{"reason": "nested_title_identity", "target_roots": nested}],
        }

    def phase_one(self) -> dict:
        project = self.movie_project()
        library = build_one_time_library_plan({"projects": [project]})
        movie_batches = build_one_time_title_batches(library)
        movie = movie_batches["batches"][0]
        movie["scope_blockers"] = [{"reason": "exact_movie_member_scope_required"}]
        movie["read_only_audit_allowed"] = False
        tv = [self.tv_batch(index, contained=index == 22) for index in range(23)]
        all_batches = [movie, *tv]
        report = build_one_time_worklist(
            global_control={"paused": True, "persistent": True},
            inbox_plan={"mutation": False, "sources": []}, library_plan=library,
            cleanup_plan={"mutation": False, "actions": []},
        )
        report["observations"]["title_batches"] = {
            "schema_version": 1, "kind": "one_time_title_batches",
            "title_count": len(all_batches), "blocked_title_scope_count": len(all_batches),
            "batches": all_batches,
        }
        report["inputs"] = {"fixture": "phase-one"}
        return seal_one_time_worklist(report)

    def phase_two(self, phase_one: dict) -> dict:
        return build_phase_two_movie_worklist(
            phase_one=phase_one, phase_one_path="/sealed/phase1.json",
            projects=[self.movie_project()], confirmed_subtitle_gaps=[],
            pending_subtitle_verification=[],
            runtime_control={"paused": True, "persistent": True},
            input_evidence={"fixture": "phase-two"},
        )

    def movie_audit_scope(self, phase_two: dict) -> dict:
        batch = phase_two["observations"]["title_batches"]["batches"][0]
        candidate = batch["movie_member_candidate"]
        paths = sorted([self.movie_stem + ".mkv", self.movie_stem + ".nfo"], key=str.casefold)
        movie_scope = validate_exact_movie_member_scope({
            "schema_version": 1, "kind": "one_time_exact_movie_member_scope",
            **{key: candidate[key] for key in (
                "category", "target_stem", "parent_root", "video_path", "nfo_path",
                "tmdb_id", "title", "candidate_sha256",
            )},
            "member_paths": paths, "member_paths_sha256": canonical_digest(paths),
        })
        member_core = {"target_stem": self.movie_stem, "entries": paths}
        member = {**member_core, "inventory_sha256": canonical_digest(member_core)}
        parent_core = {"parent_root": candidate["parent_root"], "entries": paths}
        parent = {**parent_core, "inventory_sha256": canonical_digest(parent_core)}
        inventory_core = {"member_inventory": member, "parent_inventory": parent}
        inventory = {**inventory_core, "inventory_sha256": canonical_digest(inventory_core)}
        return {
            "schema_version": 1, "title_work_key": batch["title_work_key"],
            "worklist_sha256": phase_two["worklist_sha256"],
            "scope": [], "scope_sha256": canonical_digest([movie_scope]),
            "source_scope_kind": "one_time_exact_movie_member_scope",
            "movie_member_scopes": [movie_scope],
            "before_inventory": inventory, "after_inventory": deepcopy(inventory),
            "remote_mutations": False, "scheduler_dispatch": False,
        }

    def build(self, phase_one: dict, phase_two: dict, scopes: list[dict]) -> dict:
        return build_phase_three_tv_exclusion_worklist(
            phase_one=phase_one, phase_one_path="/sealed/phase1.json",
            phase_two=phase_two, phase_two_path="/sealed/phase2.json",
            phase_two_audit_scopes=scopes,
            runtime_control={"paused": True, "persistent": True},
            input_evidence={"fixture": "phase-three"},
        )

    def test_builds_23_tv_batches_with_22_allowed_and_contained_one_blocked(self):
        phase_one = self.phase_one(); phase_two = self.phase_two(phase_one)
        before_one = json.dumps(phase_one, ensure_ascii=False, sort_keys=True)
        before_two = json.dumps(phase_two, ensure_ascii=False, sort_keys=True)

        phase_three = self.build(phase_one, phase_two, [self.movie_audit_scope(phase_two)])

        self.assertTrue(phase_three_tv_exclusion_worklist_is_valid(phase_three))
        self.assertTrue(one_time_worklist_is_valid(phase_three))
        batches = phase_three["observations"]["title_batches"]
        self.assertEqual(batches["title_count"], 23)
        self.assertEqual(batches["blocked_title_scope_count"], 1)
        self.assertEqual(sum(row["read_only_audit_allowed"] is True for row in batches["batches"]), 22)
        blocked = next(row for row in batches["batches"] if not row["read_only_audit_allowed"])
        self.assertEqual(
            blocked["phase3_scope_resolution"]["reason"],
            "conservative_tv_exclusion_scope_rejected",
        )
        self.assertNotIn("tv_exclusion_scope", blocked)
        self.assertEqual(json.dumps(phase_one, ensure_ascii=False, sort_keys=True), before_one)
        self.assertEqual(json.dumps(phase_two, ensure_ascii=False, sort_keys=True), before_two)
        self.assertEqual(phase_three["dispatchable_count"], 0)

    def test_binds_verified_movie_members_and_uses_stem_fallback_when_absent(self):
        phase_one = self.phase_one(); phase_two = self.phase_two(phase_one)
        phase_three = self.build(phase_one, phase_two, [self.movie_audit_scope(phase_two)])
        batches = phase_three["observations"]["title_batches"]["batches"]
        first = next(row for row in batches if row["target_root"].endswith("Parent-00"))
        second = next(row for row in batches if row["target_root"].endswith("Parent-01"))
        self.assertEqual(first["phase3_scope_resolution"]["phase2_movie_scope_count"], 1)
        self.assertEqual(len(first["tv_exclusion_scope"]["excluded_member_paths"]), 2)
        self.assertEqual(second["phase3_scope_resolution"]["phase2_movie_scope_count"], 0)
        self.assertEqual(second["phase3_scope_resolution"]["conservative_stem_exclusion_count"], 1)
        self.assertEqual(second["tv_exclusion_scope"]["excluded_member_paths"], [])

    def test_rejects_stale_or_unverified_phase_two_scope(self):
        phase_one = self.phase_one(); phase_two = self.phase_two(phase_one)
        stale = self.movie_audit_scope(phase_two)
        stale["worklist_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "过期 worklist"):
            self.build(phase_one, phase_two, [stale])

    def test_inherits_only_tv_blocked_by_the_unique_nested_identity_blocker(self):
        phase_one = self.phase_one(); phase_two = self.phase_two(phase_one)
        extra = self.tv_batch(99)
        extra["scope_blockers"].append({"reason": "missing_exact_identity"})
        phase_one_core = {key: deepcopy(value) for key, value in phase_one.items() if key != "worklist_sha256"}
        batches = phase_one_core["observations"]["title_batches"]["batches"]
        batches.append(extra)
        phase_one = seal_one_time_worklist(phase_one_core)
        # Phase 2 remains bound to the previous phase-1 digest, so rebuild it.
        phase_two = self.phase_two(phase_one)
        phase_three = self.build(phase_one, phase_two, [])
        roots = {
            row["target_root"]
            for row in phase_three["observations"]["title_batches"]["batches"]
        }
        self.assertNotIn(extra["target_root"], roots)
        self.assertEqual(len(roots), 23)

    def test_output_is_o_excl_and_preserves_existing_bytes(self):
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "phase-three.json"
            output.write_bytes(b"already sealed\n")
            with self.assertRaises(FileExistsError):
                _write_new_json(output, {"replacement": True})
            self.assertEqual(output.read_bytes(), b"already sealed\n")
            self.assertEqual(list(output.parent.glob("*.tmp-*")), [])


if __name__ == "__main__":
    unittest.main()
