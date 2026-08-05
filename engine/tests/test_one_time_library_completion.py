from __future__ import annotations

from copy import deepcopy
import unittest
from datetime import datetime, timezone

from engine.scrapeflow.one_time_library_completion import (
    build_cleanup_plan,
    build_one_time_worklist,
    build_inbox_discovery_plan,
    build_one_time_library_plan,
    build_one_time_title_batches,
    dispatch_gate,
    one_time_worklist_is_valid,
    seal_one_time_worklist,
)


class OneTimeLibraryCompletionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.projects = [{
            "title": "黑执事",
            "official_title": "黑执事",
            "original_title": "黒執事",
            "tmdb_ids": [50712],
            "target_root": "/quark/影视/番剧/黑执事",
            "regular_missing": [{"season": 1, "episode": 1}],
            "optional_missing": [],
            "metadata_issues": [],
        }]

    @staticmethod
    def inventory(name: str, *, videos: int = 1, files: int | None = None):
        file_count = videos if files is None else files
        return {
            "path": f"/quark/影视/待刮削/{name}",
            "file_count": file_count,
            "bytes": file_count * 100,
            "extensions": {".mkv": videos} if videos else {},
            "files": [
                {"path": f"/quark/影视/待刮削/{name}/{index}.mkv", "size": 100}
                for index in range(videos)
            ],
        }

    def test_inbox_discovery_is_idempotent_and_inherits_unique_formal_identity(self):
        inventory = [self.inventory("H 4k 黑执事")]
        first = build_inbox_discovery_plan(inventory, self.projects)
        second = build_inbox_discovery_plan(inventory, self.projects)
        self.assertEqual(first, second)
        source = first["sources"][0]
        self.assertEqual(source["disposition"], "merge_and_replenish_existing_work")
        self.assertEqual(
            [row["action"] for row in source["actions"]],
            ["safe_merge_new_media", "replenish_existing_work"],
        )
        self.assertEqual(
            source["actions"][1]["gaps"],
            [{"season": 1, "episode": 1}],
        )
        self.assertEqual(source["identity"]["tmdb_id"], 50712)
        self.assertEqual(source["identity"]["category"], "番剧")
        self.assertEqual(source["identity_resolution"], "formal_exact_alias")

    def test_repeated_submission_ignores_mutable_file_timestamp(self):
        first_inventory = self.inventory("时间戳幂等样本")
        first_inventory["files"][0]["modified"] = "2026-08-01T00:00:00Z"
        second_inventory = self.inventory("时间戳幂等样本")
        second_inventory["files"][0]["modified"] = "2026-08-03T00:00:00Z"
        first = build_inbox_discovery_plan(
            [first_inventory], self.projects, unmatched_media_category="番剧",
        )["sources"][0]
        second = build_inbox_discovery_plan(
            [second_inventory], self.projects, unmatched_media_category="番剧",
        )["sources"][0]
        self.assertEqual(first["inventory_fingerprint"], second["inventory_fingerprint"])
        self.assertEqual(first["idempotency_key"], second["idempotency_key"])

    def test_inbox_discovery_fails_closed_on_ambiguous_identity(self):
        projects = [*self.projects, {
            "title": "黑执事 新章", "official_title": "黑执事 新章",
            "aliases": ["黑执事"],
            "tmdb_ids": [999], "target_root": "/quark/影视/番剧/黑执事 新章",
        }]
        plan = build_inbox_discovery_plan(
            [self.inventory("黑执事全系列")], projects,
        )
        source = plan["sources"][0]
        self.assertEqual(
            source["disposition"], "create_or_reuse_identity_resolution_task",
        )
        self.assertTrue(source["requires_unique_tmdb_plan"])
        self.assertIsNone(source["identity"]["tmdb_id"])
        self.assertEqual(source["identity_resolution"], "formal_ancestor_category_only")

    def test_cross_category_strong_identity_complete_routes_input_to_cleanup(self):
        projects = [{**self.projects[0],
            "target_root": "/quark/影视/番剧/黑执事",
            "regular_missing": [{"season": 1, "episode": 1}],
        }, {**self.projects[0],
            "target_root": "/quark/影视/美剧/黑执事",
            "regular_missing": [], "optional_missing": [],
        }]
        plan = build_inbox_discovery_plan([self.inventory("黑执事")], projects)
        source = plan["sources"][0]
        self.assertEqual(source["identity_resolution"], "formal_cross_category_strong_identity")
        self.assertTrue(source["identity"]["formal_complete"])
        self.assertEqual(len(source["identity"]["formal_locations"]), 2)
        self.assertEqual(source["disposition"], "cleanup_duplicate_input")
        self.assertEqual(source["reconciliation_action"], "cleanup_duplicate_input_directory")
        self.assertEqual(source["actions"][0]["action"], "cleanup_duplicate_input_directory")
        self.assertFalse(source["actions"][0]["delete_unproven_files"])
        self.assertFalse(source["reconciliation_mutation"])
        self.assertEqual(plan["schedulable_count"], 0)
        self.assertEqual(plan["completed_identity_cleanup_count"], 1)

    def test_true_gap_deduplicates_two_sources_and_is_order_independent(self):
        inputs = [self.inventory("H 4k 黑执事"), self.inventory("黑执事 全系列")]
        first = build_inbox_discovery_plan(inputs, self.projects)
        second = build_inbox_discovery_plan(list(reversed(inputs)), self.projects)
        self.assertEqual(first, second)
        self.assertEqual(first["schedulable_count"], 1)
        self.assertEqual(first["true_gap_count"], 1)
        self.assertEqual(first["duplicate_identity_count"], 1)
        duplicate = next(
            row for row in first["sources"]
            if row["disposition"] == "coalesced_duplicate_submission"
        )
        self.assertIsNone(duplicate["blocker"])
        self.assertEqual(duplicate["actions"][0]["action"], "coalesce_duplicate_submission")
        self.assertIn("duplicate_of_source", duplicate)

    def test_duplicate_submission_prefers_existing_task_and_remains_idempotent(self):
        existing_source = self.inventory("Z 黑执事")
        historical = [{
            "id": "b" * 12, "source": existing_source["path"],
            "parent": "/quark/影视/番剧", "media_type": "tv", "tmdb_id": 50712,
            "phase": "queued",
        }]
        inputs = [self.inventory("A 黑执事"), existing_source]
        plan = build_inbox_discovery_plan(
            inputs, self.projects, historical_tasks=historical,
        )
        repeated = build_inbox_discovery_plan(
            list(reversed(inputs)), self.projects, historical_tasks=historical,
        )
        self.assertEqual(plan, repeated)
        primary = next(row for row in plan["sources"] if row["disposition"] == "reuse_existing_task")
        duplicate = next(row for row in plan["sources"] if row["disposition"] == "coalesced_duplicate_submission")
        self.assertEqual(primary["source"], existing_source["path"])
        self.assertEqual(duplicate["duplicate_of_source"], existing_source["path"])
        self.assertEqual(plan["schedulable_count"], 1)

    def test_same_alias_with_different_tmdb_ids_remains_ambiguous(self):
        projects = [self.projects[0], {**self.projects[0],
            "tmdb_ids": [999], "target_root": "/quark/影视/美剧/黑执事",
        }]
        source = build_inbox_discovery_plan(
            [self.inventory("黑执事")], projects,
        )["sources"][0]
        self.assertEqual(source["disposition"], "identity_evidence_required")
        self.assertIsNone(source["blocker"])
        self.assertEqual(source["actions"][0]["evidence_reason"], "ambiguous_formal_identity")
        self.assertIsNone(source["identity"])

    def test_non_video_and_empty_sources_never_create_jobs(self):
        no_video = self.inventory("黑执事小说", videos=0, files=12)
        no_video["extensions"] = {".epub": 12}
        empty = self.inventory("黑执事（待删）", videos=0, files=0)
        plan = build_inbox_discovery_plan([no_video, empty], self.projects)
        by_source = {row["source"]: row for row in plan["sources"]}
        self.assertNotIn("no_video_media", str(plan))
        self.assertEqual(by_source[no_video["path"]]["disposition"], "non_media_input_review")
        self.assertTrue(by_source[no_video["path"]]["actions"][0]["requires_later_review"])
        self.assertEqual(by_source[empty["path"]]["disposition"], "cleanup_empty_input_review")

    def test_unverified_archive_is_inspected_before_task_and_unmatched_video_resolves_identity(self):
        archive = self.inventory("青春猪头少年", videos=0, files=1)
        archive["extensions"] = {".zip": 1}
        archive["files"] = [{"path": archive["path"] + "/media.zip", "size": 100}]
        video = self.inventory("夏日口袋", videos=2)
        projects = [*self.projects, {
            "title": "青春猪头少年不会梦到兔女郎学姐",
            "official_title": "青春猪头少年不会梦到兔女郎学姐",
            "tmdb_ids": [82739],
            "target_root": "/quark/影视/番剧/青春猪头少年/青春猪头少年不会梦到兔女郎学姐",
            "regular_missing": [{"season": 1, "episode": 1}],
            "optional_missing": [],
        }]
        plan = build_inbox_discovery_plan(
            [archive, video], projects, unmatched_media_category="番剧",
        )
        by_source = {row["source"]: row for row in plan["sources"]}
        self.assertEqual(
            by_source[archive["path"]]["disposition"],
            "archive_media_verification",
        )
        self.assertEqual(
            by_source[archive["path"]]["actions"][0]["action"],
            "inspect_archive_media_members",
        )
        self.assertEqual(
            by_source[archive["path"]]["identity_resolution"],
            "formal_exact_alias",
        )
        # A unique ancestor alias can carry the exact child TMDB identity.
        self.assertEqual(by_source[archive["path"]]["identity"]["tmdb_id"], 82739)
        self.assertEqual(
            by_source[video["path"]]["identity_resolution"],
            "operator_default_category_unresolved",
        )
        self.assertTrue(by_source[video["path"]]["requires_unique_tmdb_plan"])
        self.assertEqual(by_source[video["path"]]["identity"]["media_type"], "auto")

        archive["verified_media_archive_count"] = 1
        verified = build_inbox_discovery_plan(
            [archive], projects, unmatched_media_category="番剧",
        )["sources"][0]
        self.assertEqual(verified["disposition"], "merge_and_replenish_existing_work")
        self.assertEqual(verified["actions"][0]["action"], "safe_merge_new_media")

    def test_partial_download_has_specific_later_review_state(self):
        partial = self.inventory("未完成下载", videos=0, files=1)
        partial["extensions"] = {".pptd": 1}
        partial["files"] = [{
            "path": partial["path"] + "/episode.mkv.pptd", "size": 100,
        }]
        row = build_inbox_discovery_plan(
            [partial], self.projects,
        )["sources"][0]
        self.assertEqual(row["disposition"], "partial_media_transfer_review")
        self.assertEqual(row["actions"][0]["action"], "review_partial_media_transfer")
        self.assertTrue(row["actions"][0]["requires_later_review"])

    def test_historical_identity_is_reconciled_against_current_complete_library(self):
        complete = [{**self.projects[0], "regular_missing": [], "optional_missing": []}]
        source = self.inventory("H 4k 黑执事")
        historical = [{
            "id": "a" * 12, "source": source["path"],
            "parent": "/quark/影视/番剧", "media_type": "tv", "tmdb_id": 50712,
        }]
        row = build_inbox_discovery_plan(
            [source], complete, historical_tasks=historical,
        )["sources"][0]
        self.assertEqual(row["identity_resolution"], "historical_exact_source_current_formal_identity")
        self.assertEqual(row["disposition"], "cleanup_duplicate_input")

    def test_formal_identity_without_gap_evidence_does_not_schedule(self):
        unknown = [{
            "title": "黑执事", "official_title": "黑执事", "tmdb_ids": [50712],
            "target_root": "/quark/影视/番剧/黑执事",
        }]
        row = build_inbox_discovery_plan(
            [self.inventory("黑执事")], unknown,
        )["sources"][0]
        self.assertEqual(row["disposition"], "formal_gap_evidence_review")
        self.assertIsNone(row["blocker"])
        self.assertTrue(row["actions"][0]["requires_later_review"])

    def test_out_of_scope_inventory_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "越界"):
            build_inbox_discovery_plan([{
                "path": "/quark/影视/ScrapeFlow/备份/x", "file_count": 0,
                "bytes": 0, "extensions": {}, "files": [],
            }], self.projects)

    def test_one_time_plan_separates_confirmed_subtitles_from_pending_verification(self):
        projects = [{**self.projects[0],
            "regular_missing": [{"label": "S01E01"}],
            "optional_missing": [{"label": "S00E01"}],
            "metadata_issues": [{"code": "missing_poster"}],
        }, {
            "title": "电影", "official_title": "电影", "tmdb_ids": [1],
            "target_root": "/quark/影视/电影/电影 (2026)",
            "regular_missing": [], "optional_missing": [], "metadata_issues": [],
        }]
        subtitles = [{
            "target_root": "/quark/影视/番剧/黑执事",
            "video_path": "/quark/影视/番剧/黑执事/Season 01/E01.mkv",
            "status": "gap",
            "confirmation": "confirmed_missing_chinese_subtitle",
        }, {
            "target_root": "/quark/影视/电影/电影 (2026)",
            "video_path": "/quark/影视/电影/电影 (2026)/movie.mkv",
            "classification": "confirmed_missing_chinese",
        }]
        pending = [{
            "target_root": "/quark/影视/番剧/黑执事",
            "video_path": "/quark/影视/番剧/黑执事/Season 01/E02.mkv",
            "status": "gap",
            "pending_reason": "text_stream_content_undetermined",
            "embedded_probe": {"status": "subtitle_stream_language_unknown"},
        }]
        plan = build_one_time_library_plan(
            {"projects": projects}, confirmed_subtitle_gaps=subtitles,
            subtitle_verification_items=pending,
        )
        self.assertEqual(
            sorted(row["lane"] for row in plan["lanes"]),
            [
                "metadata", "regular_video", "s00_video", "subtitle", "subtitle",
                "subtitle_verification",
            ],
        )
        subtitle_lanes = [row for row in plan["lanes"] if row["lane"] == "subtitle"]
        self.assertEqual(sum(row["gap_count"] for row in subtitle_lanes), 2)
        self.assertTrue(all("replace_video" in row["delivery_policy"]["forbidden"] for row in subtitle_lanes))
        verification = next(
            row for row in plan["lanes"] if row["lane"] == "subtitle_verification"
        )
        self.assertEqual(verification["gap_count"], 0)
        self.assertEqual(verification["item_count"], 1)
        self.assertEqual(plan["verification_action_count"], 1)
        self.assertEqual(plan["gap_count"], 5)
        self.assertIn("replace_video", verification["verification_policy"]["forbidden"])

    def test_global_pause_produces_zero_dispatchable_actions(self):
        inbox = build_inbox_discovery_plan([self.inventory("黑执事")], self.projects)
        library = build_one_time_library_plan({"projects": self.projects})
        cleanup = {"actions": [{"action": "remove_empty_directory"}]}
        cycle = build_one_time_worklist(
            global_control={"paused": True, "persistent": True},
            inbox_plan=inbox, library_plan=library, cleanup_plan=cleanup,
        )
        self.assertFalse(cycle["dispatch_gate"]["allowed"])
        self.assertEqual(cycle["dispatchable_count"], 0)
        self.assertEqual(cycle["dispatchable"], [])

    def test_title_batches_group_lanes_but_never_dispatch_discovery_as_work(self):
        projects = [{
            **self.projects[0],
            "regular_missing": [{"label": "S01E01"}],
            "optional_missing": [{"label": "S00E01"}],
        }]
        library = build_one_time_library_plan({"projects": projects})
        batches = build_one_time_title_batches(library)
        self.assertFalse(batches["mutation"])
        self.assertFalse(batches["dispatch_allowed"])
        self.assertEqual(batches["title_count"], 1)
        self.assertEqual(len(batches["batches"][0]["discovery_lanes"]), 2)
        self.assertEqual(
            batches["batches"][0]["first_action"],
            "fresh_exact_title_read_only_audit",
        )
        self.assertFalse(batches["batches"][0]["discovery_is_completion_proof"])
        self.assertTrue(batches["batches"][0]["read_only_audit_allowed"])
        self.assertTrue(batches["policy"]["synthetic_media_journal_forbidden"])

    def test_title_batches_block_ambiguous_identity(self):
        library = build_one_time_library_plan({"projects": [{
            "title": "unknown", "tmdb_ids": [],
            "target_root": "/quark/影视/番剧/unknown",
            "regular_missing": [{"label": "S01E01"}],
        }]})
        batches = build_one_time_title_batches(library)
        self.assertEqual(batches["title_count"], 0)
        self.assertEqual(batches["blocked_lane_count"], 1)
        self.assertEqual(batches["blocked"][0]["reason"], "missing_exact_identity")

    def test_parent_title_batch_is_blocked_when_catalog_has_nested_identity(self):
        parent = {
            **self.projects[0],
            "regular_missing": [{"label": "S01E01"}],
        }
        child = {
            "title": "Movie", "official_title": "Movie", "tmdb_ids": [99],
            "media_type": "movie",
            "target_root": "/quark/影视/番剧/黑执事/Movie (2026)",
            "regular_missing": [], "optional_missing": [], "metadata_issues": [],
        }
        batches = build_one_time_title_batches(
            build_one_time_library_plan({"projects": [parent, child]}),
        )
        self.assertEqual(batches["blocked_title_scope_count"], 1)
        self.assertFalse(batches["batches"][0]["read_only_audit_allowed"])
        self.assertEqual(
            batches["batches"][0]["scope_blockers"][0]["reason"],
            "nested_title_identity",
        )

    def test_duplicate_tmdb_identity_roots_are_not_independent_batches(self):
        first = {
            "title": "Movie", "official_title": "Movie", "tmdb_ids": [99],
            "media_type": "movie", "target_root": "/quark/影视/电影/Movie part1",
            "regular_missing": [], "optional_missing": [], "metadata_issues": [],
        }
        second = {
            **first, "target_root": "/quark/影视/电影/Movie part2",
            "metadata_issues": [{"code": "multipart_review"}],
        }
        batches = build_one_time_title_batches(
            build_one_time_library_plan({"projects": [first, second]}),
        )
        self.assertEqual(batches["title_count"], 1)
        self.assertFalse(batches["batches"][0]["read_only_audit_allowed"])
        self.assertEqual(
            batches["batches"][0]["scope_blockers"][0]["reason"],
            "duplicate_tmdb_identity_roots",
        )

    def test_movie_stem_gets_sealed_exact_member_candidate(self):
        movie = {
            "title": "Movie", "official_title": "Movie", "tmdb_ids": [99],
            "media_type": "movie", "target_root": "/quark/影视/电影/Movie (2026)",
            "video_files": ["/quark/影视/电影/Movie (2026).mkv"],
            "issues": [],
            "regular_missing": [], "optional_missing": [],
            "metadata_issues": [{"code": "subtitle_gap"}],
        }
        batches = build_one_time_title_batches(
            build_one_time_library_plan({"projects": [movie]}),
        )
        batch = batches["batches"][0]
        self.assertTrue(batch["read_only_audit_allowed"])
        self.assertEqual(batch["scope_blockers"], [])
        self.assertEqual(
            batch["movie_member_candidate"]["video_path"],
            "/quark/影视/电影/Movie (2026).mkv",
        )
        self.assertEqual(
            batch["movie_member_candidate"]["nfo_path"],
            "/quark/影视/电影/Movie (2026).nfo",
        )

    def test_movie_member_candidate_blocks_multiple_or_mismatched_video(self):
        base = {
            "title": "Movie", "official_title": "Movie", "tmdb_ids": [99],
            "media_type": "movie", "target_root": "/quark/影视/电影/Movie (2026)",
            "issues": [], "regular_missing": [], "optional_missing": [],
            "metadata_issues": [{"code": "subtitle_gap"}],
        }
        for videos, reason in (
            ([base["target_root"] + ".mkv", base["target_root"] + ".mp4"],
             "movie_member_video_identity_ambiguous"),
            (["/quark/影视/电影/Other.mkv"], "movie_member_video_stem_mismatch"),
        ):
            with self.subTest(reason=reason):
                batches = build_one_time_title_batches(build_one_time_library_plan({
                    "projects": [{**base, "video_files": videos}],
                }))
                self.assertFalse(batches["batches"][0]["read_only_audit_allowed"])
                self.assertEqual(batches["batches"][0]["scope_blockers"][0]["reason"], reason)

    def test_movie_member_candidate_blocks_identity_and_year_audit_issues(self):
        movie = {
            "title": "Movie", "official_title": "Movie", "tmdb_ids": [99],
            "media_type": "movie", "target_root": "/quark/影视/电影/Movie (2026)",
            "video_files": ["/quark/影视/电影/Movie (2026).mkv"],
            "issues": [{"code": "movie_year_tmdb_mismatch"}],
            "regular_missing": [], "optional_missing": [],
            "metadata_issues": [{"code": "subtitle_gap"}],
        }
        batch = build_one_time_title_batches(
            build_one_time_library_plan({"projects": [movie]}),
        )["batches"][0]
        self.assertFalse(batch["read_only_audit_allowed"])
        self.assertEqual(batch["scope_blockers"][0]["reason"], "movie_member_identity_issue")


    def test_dispatch_gate_requires_one_strict_boolean_pause_field(self):
        for paused in (None, 0, 1, "false", [], {}):
            with self.subTest(paused=paused):
                gate = dispatch_gate({"paused": paused, "persistent": True})
                self.assertFalse(gate["allowed"])
                self.assertEqual(gate["reason"], "global_control_incomplete")
        self.assertTrue(dispatch_gate({"paused": False, "persistent": True})["allowed"])

    def test_sealed_worklist_rejects_any_persisted_field_tampering(self):
        cycle = build_one_time_worklist(
            global_control={"paused": True, "persistent": True},
            inbox_plan={"sources": []},
            library_plan={"lanes": []},
            cleanup_plan={"actions": []},
        )
        cycle["inputs"] = {"live_audit_digest": "a" * 64}
        sealed = seal_one_time_worklist(cycle)
        self.assertTrue(one_time_worklist_is_valid(sealed))
        tampered = deepcopy(sealed)
        tampered["inputs"]["live_audit_digest"] = "b" * 64
        self.assertFalse(one_time_worklist_is_valid(tampered))

    def test_cleanup_needs_empty_source_success_journal_and_clean_reaudit(self):
        empty = self.inventory("黑执事（待删）", videos=0, files=0)
        discovery = build_inbox_discovery_plan([empty], self.projects)
        journal = {
            "success": True,
            "records": [{"action": "files-committed", "status": "ok"}],
        }
        evidence = [{
            "id": "a" * 12, "source": empty["path"], "phase": "completed",
            "target_root": "/quark/影视/番剧/黑执事", "journal": journal,
        }]
        blocked = build_cleanup_plan(
            discovery, task_evidence=evidence, clean_audit_targets=[],
        )
        self.assertEqual(blocked["action_count"], 0)
        self.assertIn("post_commit_reaudit_not_clean", blocked["blocked"][0]["blockers"])
        allowed = build_cleanup_plan(
            discovery, task_evidence=evidence,
            clean_audit_targets=["/quark/影视/番剧/黑执事"],
        )
        self.assertEqual(allowed["action_count"], 1)
        self.assertEqual(allowed["actions"][0]["action"], "remove_empty_directory")
        self.assertFalse(allowed["delete_files_allowed"])


if __name__ == "__main__":
    unittest.main()
