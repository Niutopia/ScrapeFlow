from __future__ import annotations

from copy import deepcopy
import unittest

from engine.scrapeflow.one_time_library_completion import (
    build_one_time_worklist,
    canonical_digest,
    seal_one_time_worklist,
)
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    seal_exact_tv_exclusion_scope_from_predecessor_batch,
)
from engine.tools.prepare_one_time_title_import import (
    prepare_import,
    validate_scope_closure_binding,
)


class PrepareOneTimeTitleImportTests(unittest.TestCase):
    def setUp(self):
        self.target = {
            "media_type": "tv", "target_root": "/quark/影视/番剧/Example",
            "category": "番剧", "tmdb_id": 42, "title": "Example",
        }
        self.scope_sha = canonical_digest([self.target])
        worklist = build_one_time_worklist(
            global_control={"paused": True, "persistent": True},
            inbox_plan={"sources": []}, library_plan={"lanes": []},
            cleanup_plan={"actions": []},
        )
        worklist["observations"]["title_batches"] = {"batches": [{
            "target_root": self.target["target_root"],
            "category": self.target["category"],
            "identity": {
                "status": "exact", "media_type": "tv", "tmdb_id": 42,
                "title": "Example",
            },
            "title_work_key": "b" * 64,
            "read_only_audit_allowed": True,
        }]}
        self.worklist = seal_one_time_worklist(worklist)
        inventory_sha = "a" * 64
        self.scope = {
            "title_work_key": "b" * 64,
            "worklist_sha256": self.worklist["worklist_sha256"],
            "scope": [self.target], "scope_sha256": self.scope_sha,
            "before_inventory": {"inventory_sha256": inventory_sha},
            "after_inventory": {"inventory_sha256": inventory_sha},
        }

    def closure(self, *, episode=True, subtitle=False):
        gaps = [{
            "kind": "missing_episode", "lane": "s00", "season": 0,
            "episode": 2, "label": "S00E02", "title": "Special",
            "season_name": "Specials", "expected_episode_count": 2,
            "target_root": self.target["target_root"], "media_type": "tv",
            "tmdb_id": 42, "media": {**self.target, "original_title": "Example"},
        }] if episode else []
        confirmed = [{
            "target_root": self.target["target_root"], "video_path": "E01.mkv",
        }] if subtitle else []
        summary = {
            "episode_gap_count": len(gaps),
            "confirmed_subtitle_gap_count": len(confirmed),
            "pending_subtitle_verification_count": 0,
            "complete": not gaps and not confirmed,
        }
        core = {
            "schema_version": 1, "audited_at": "2026-08-03T00:00:00+00:00",
            "source_plan_sha256": self.scope_sha,
            "source_scope_kind": "one_time_exact_title_scope",
            "title_targets": [self.target],
            "title_targets_sha256": canonical_digest([self.target]),
            "policy": {}, "subtitle_inventories": {}, "episode_gaps": gaps,
            "subtitle_refinement": {
                "confirmed_missing_chinese": confirmed,
                "pending_review_or_probe": [], "resolved_with_chinese": [],
            },
            "probe_evidence": {}, "summary": summary,
        }
        return {**core, "evidence_sha256": canonical_digest(core)}

    def phase3_fixture(self):
        predecessor_batch = {
            "target_root": self.target["target_root"],
            "category": self.target["category"],
            "identity": {
                "status": "exact", "media_type": "tv", "tmdb_id": 42,
                "title": "Example",
            },
            "title_work_key": "d" * 64,
            "read_only_audit_allowed": False,
            "scope_blockers": [{
                "reason": "nested_title_identity",
                "target_roots": [
                    self.target["target_root"] + "/Nested Movie (2025)",
                ],
            }],
        }
        tv_scope = seal_exact_tv_exclusion_scope_from_predecessor_batch(
            predecessor_batch,
        )
        phase_batch = {
            "target_root": self.target["target_root"],
            "category": self.target["category"],
            "identity": dict(predecessor_batch["identity"]),
            "title_work_key": "e" * 64,
            "read_only_audit_allowed": True,
            "tv_exclusion_scope": tv_scope,
        }
        report = build_one_time_worklist(
            global_control={"paused": True, "persistent": True},
            inbox_plan={"sources": []}, library_plan={"lanes": []},
            cleanup_plan={"actions": []},
        )
        report["observations"]["title_batches"] = {"batches": [phase_batch]}
        worklist = seal_one_time_worklist(report)
        scope_sha = canonical_digest([tv_scope])
        scope = {
            "title_work_key": phase_batch["title_work_key"],
            "worklist_sha256": worklist["worklist_sha256"],
            "scope": [self.target], "scope_sha256": scope_sha,
            "source_scope_kind": "one_time_exact_tv_root_with_nested_exclusions",
            "tv_exclusion_scopes": [tv_scope],
            "before_inventory": {"inventory_sha256": "f" * 64},
            "after_inventory": {"inventory_sha256": "f" * 64},
        }
        closure = self.closure()
        closure["source_plan_sha256"] = scope_sha
        closure["source_scope_kind"] = "one_time_exact_tv_root_with_nested_exclusions"
        closure["tv_exclusion_scopes"] = [tv_scope]
        closure_core = {
            key: value for key, value in closure.items() if key != "evidence_sha256"
        }
        closure["evidence_sha256"] = canonical_digest(closure_core)
        return worklist, scope, closure, phase_batch, tv_scope

    def test_prepares_episode_request_without_dispatch(self):
        result = prepare_import(
            self.worklist, self.scope, self.closure(),
            title_work_key="b" * 64,
            live_control={"paused": True, "persistent": True},
        )
        self.assertFalse(result["dispatch_allowed"])
        self.assertEqual(result["actions"][0]["kind"], "episode_replenishment")
        self.assertEqual(result["actions"][0]["request"]["gaps"][0]["id"], "S00E02")
        self.assertEqual(result["safety"]["jobs_created"], 0)

    def test_rejects_scope_from_previous_worklist(self):
        stale = deepcopy(self.scope)
        stale["worklist_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "过期 worklist"):
            prepare_import(
                self.worklist, stale, self.closure(),
                title_work_key="b" * 64,
                live_control={"paused": True, "persistent": True},
            )

    def test_phase3_tv_exclusion_scope_prepares_without_weakening_binding(self):
        worklist, scope, closure, batch, tv_scope = self.phase3_fixture()
        result = prepare_import(
            worklist, scope, closure,
            title_work_key=batch["title_work_key"],
            live_control={"paused": True, "persistent": True},
        )
        self.assertEqual(result["scope_sha256"], canonical_digest([tv_scope]))
        self.assertEqual(result["actions"][0]["kind"], "episode_replenishment")
        self.assertFalse(result["dispatch_allowed"])

    def test_phase3_rejects_scope_or_closure_exclusions_not_equal_to_batch(self):
        worklist, scope, closure, batch, _tv_scope = self.phase3_fixture()
        changed_scope = deepcopy(scope)
        changed = deepcopy(changed_scope["tv_exclusion_scopes"][0])
        changed["excluded_roots"].append(
            self.target["target_root"] + "/Other Nested (2024)",
        )
        changed["excluded_roots"].sort(key=str.casefold)
        changed["excluded_roots_sha256"] = canonical_digest(changed["excluded_roots"])
        changed_core = {
            key: value for key, value in changed.items() if key != "scope_sha256"
        }
        changed["scope_sha256"] = canonical_digest(changed_core)
        changed_scope["tv_exclusion_scopes"] = [changed]
        changed_scope["scope_sha256"] = canonical_digest([changed])
        with self.assertRaisesRegex(ValueError, "worklist 封存批次"):
            validate_scope_closure_binding(
                worklist, changed_scope, closure,
                title_work_key=batch["title_work_key"],
            )

        changed_closure = deepcopy(closure)
        changed_closure["tv_exclusion_scopes"] = [changed]
        with self.assertRaisesRegex(ValueError, "复核证据"):
            validate_scope_closure_binding(
                worklist, scope, changed_closure,
                title_work_key=batch["title_work_key"],
            )

    def test_phase3_rejects_noncanonical_exclusion_digest_everywhere(self):
        worklist, scope, closure, batch, _tv_scope = self.phase3_fixture()
        broken = deepcopy(scope)
        broken["tv_exclusion_scopes"][0]["excluded_roots_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "排除根 digest"):
            validate_scope_closure_binding(
                worklist, broken, closure,
                title_work_key=batch["title_work_key"],
            )

    def test_phase3_batch_cannot_downgrade_to_ordinary_tv_scope(self):
        worklist, scope, closure, batch, _tv_scope = self.phase3_fixture()
        ordinary_scope = deepcopy(scope)
        ordinary_scope.pop("tv_exclusion_scopes")
        ordinary_scope.pop("source_scope_kind")
        ordinary_scope["scope_sha256"] = canonical_digest([self.target])
        ordinary_closure = self.closure()
        with self.assertRaisesRegex(ValueError, "不得降级"):
            validate_scope_closure_binding(
                worklist, ordinary_scope, ordinary_closure,
                title_work_key=batch["title_work_key"],
            )

    def test_ordinary_tv_and_phase3_cannot_mix_scope_types(self):
        ordinary_scope = deepcopy(self.scope)
        ordinary_scope["tv_exclusion_scopes"] = []
        with self.assertRaisesRegex(ValueError, "不得降级"):
            validate_scope_closure_binding(
                self.worklist, ordinary_scope, self.closure(),
                title_work_key="b" * 64,
            )

        worklist, scope, closure, batch, _tv_scope = self.phase3_fixture()
        phase3_scope = deepcopy(scope)
        phase3_scope["movie_member_scopes"] = []
        with self.assertRaisesRegex(ValueError, "不得携带电影成员"):
            validate_scope_closure_binding(
                worklist, phase3_scope, closure,
                title_work_key=batch["title_work_key"],
            )


if __name__ == "__main__":
    unittest.main()
