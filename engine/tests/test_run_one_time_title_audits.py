from __future__ import annotations

import json
from pathlib import Path
import tempfile
import threading
import unittest

from engine.scrapeflow.one_time_library_completion import (
    canonical_digest,
    seal_one_time_worklist,
)
from engine.scrapeflow.one_time_movie_member_scope import (
    seal_movie_member_candidate,
    validate_exact_movie_member_scope,
)
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    seal_exact_tv_exclusion_scope_from_predecessor_batch,
)
from engine.tools.run_one_time_title_audits import (
    AuditResult,
    atomic_json,
    evidence_is_verified,
    run_audits,
)


class OneTimeTitleAuditRunnerTests(unittest.TestCase):
    def batch(self, number: int) -> dict:
        target = {
            "media_type": "tv",
            "target_root": f"/quark/影视/番剧/Example-{number}",
            "category": "番剧",
            "tmdb_id": number,
            "title": f"Example {number}",
        }
        return {
            "target_root": target["target_root"],
            "category": target["category"],
            "identity": {
                "status": "exact", "tmdb_id": target["tmdb_id"],
                "media_type": "tv", "title": target["title"],
                "identity_sha256": canonical_digest(target),
            },
            "title_work_key": f"{number:064x}",
            "read_only_audit_allowed": True,
            "mutation_allowed": False,
        }

    def worklist(self, batches: list[dict]) -> dict:
        return seal_one_time_worklist({
            "schema_version": 1,
            "kind": "one_time_library_completion_worklist",
            "dispatch_gate": {
                "allowed": False, "reason": "global_pause_active",
                "proof": {"paused": True, "persistent": True},
            },
            "observations": {"title_batches": {"batches": batches}},
        })

    def movie_batch(self, number: int = 99) -> dict:
        stem = f"/quark/影视/电影/Shared/Movie-{number} (2026)"
        identity_core = {
            "status": "exact", "tmdb_id": number,
            "media_type": "movie", "title": f"Movie {number}",
        }
        candidate = seal_movie_member_candidate({
            "category": "电影", "target_stem": stem,
            "parent_root": "/quark/影视/电影/Shared",
            "video_path": stem + ".mkv", "nfo_path": stem + ".nfo",
            "tmdb_id": number, "title": f"Movie {number}",
        })
        return {
            "target_root": stem, "category": "电影",
            "identity": {
                **identity_core,
                "identity_sha256": canonical_digest(identity_core),
            },
            "movie_member_candidate": candidate,
            "title_work_key": f"{number:064x}",
            "read_only_audit_allowed": True,
            "mutation_allowed": False,
        }

    def tv_exclusion_batch(self, number: int = 42) -> dict:
        root = f"/quark/影视/番剧/Parent-{number}"
        identity_core = {
            "status": "exact", "tmdb_id": number,
            "media_type": "tv", "title": f"Parent {number}",
        }
        predecessor = {
            "target_root": root, "category": "番剧",
            "identity": {
                **identity_core,
                "identity_sha256": canonical_digest(identity_core),
            },
            "title_work_key": "d" * 64,
            "read_only_audit_allowed": False,
            "scope_blockers": [{
                "reason": "nested_title_identity",
                "target_roots": [root + "/Nested Movie (2025)"],
            }],
        }
        scope = seal_exact_tv_exclusion_scope_from_predecessor_batch(predecessor)
        return {
            "target_root": root, "category": "番剧",
            "identity": predecessor["identity"],
            "tv_exclusion_scope": scope,
            "title_work_key": f"{number + 1000:064x}",
            "read_only_audit_allowed": True,
            "mutation_allowed": False,
        }

    def write_evidence(self, directory: Path, worklist: dict, batch: dict) -> None:
        target = {
            "media_type": batch["identity"]["media_type"],
            "target_root": batch["target_root"],
            "category": batch["category"],
            "tmdb_id": batch["identity"]["tmdb_id"],
            "title": batch["identity"]["title"],
        }
        scope_sha = canonical_digest([target])
        inventory_sha = canonical_digest({"title": target["title"]})
        closure_core = {
            "schema_version": 1,
            "audited_at": "2026-08-03T00:00:00+00:00",
            "source_plan_sha256": scope_sha,
            "source_scope_kind": "one_time_exact_title_scope",
            "title_targets": [target],
            "title_targets_sha256": canonical_digest([target]),
            "policy": {"remote_mutations": False},
            "subtitle_inventories": {},
            "episode_gaps": [],
            "subtitle_refinement": {
                "confirmed_missing_chinese": [],
                "pending_review_or_probe": [],
                "resolved_with_chinese": [],
            },
            "probe_evidence": {},
            "summary": {
                "episode_gap_count": 0,
                "confirmed_subtitle_gap_count": 0,
                "pending_subtitle_verification_count": 0,
                "complete": True,
            },
        }
        closure = {
            **closure_core, "evidence_sha256": canonical_digest(closure_core),
        }
        scope = {
            "schema_version": 1,
            "title_work_key": batch["title_work_key"],
            "worklist_sha256": worklist["worklist_sha256"],
            "scope": [target], "scope_sha256": scope_sha,
            "before_inventory": {"inventory_sha256": inventory_sha},
            "after_inventory": {"inventory_sha256": inventory_sha},
            "remote_mutations": False, "scheduler_dispatch": False,
        }
        status_core = {
            "schema_version": 1, "status": "complete",
            "title_work_key": batch["title_work_key"],
            "scope_sha256": scope_sha, "inventory_sha256": inventory_sha,
            "evidence_sha256": closure["evidence_sha256"],
            "remote_mutations": False,
        }
        atomic_json(directory / "title-scope.json", scope)
        atomic_json(directory / "title-closure.json", closure)
        atomic_json(directory / "status.json", {
            **status_core, "status_sha256": canonical_digest(status_core),
        })

    def write_movie_evidence(self, directory: Path, worklist: dict, batch: dict) -> None:
        candidate = batch["movie_member_candidate"]
        member_paths = [candidate["video_path"], candidate["nfo_path"]]
        movie_scope = validate_exact_movie_member_scope({
            "schema_version": 1, "kind": "one_time_exact_movie_member_scope",
            **{key: candidate[key] for key in (
                "category", "target_stem", "parent_root", "video_path",
                "nfo_path", "tmdb_id", "title", "candidate_sha256",
            )},
            "member_paths": member_paths,
            "member_paths_sha256": canonical_digest(member_paths),
        })
        scopes = [movie_scope]
        target = {
            "media_type": "movie", "target_root": candidate["target_stem"],
            "category": "电影", "tmdb_id": candidate["tmdb_id"],
            "title": candidate["title"],
        }
        scope_sha = canonical_digest(scopes)
        member_core = {
            "target_stem": candidate["target_stem"],
            "entries": [{"path": candidate["video_path"], "size": 100}],
        }
        member = {**member_core, "inventory_sha256": canonical_digest(member_core)}
        parent_core = {
            "parent_root": candidate["parent_root"],
            "entries": [
                {"path": candidate["video_path"], "size": 100},
                {"path": candidate["nfo_path"], "size": 10},
                {"path": candidate["parent_root"] + "/Sibling.mkv", "size": 200},
            ],
        }
        parent = {**parent_core, "inventory_sha256": canonical_digest(parent_core)}
        inventory_core = {"member_inventory": member, "parent_inventory": parent}
        envelope = {
            **inventory_core,
            "inventory_sha256": canonical_digest(inventory_core),
        }
        closure_core = {
            "schema_version": 1,
            "audited_at": "2026-08-03T00:00:00+00:00",
            "source_plan_sha256": scope_sha,
            "source_scope_kind": "one_time_exact_movie_member_scope",
            "title_targets": [target],
            "title_targets_sha256": canonical_digest([target]),
            "movie_member_scopes": scopes,
            "policy": {"remote_mutations": False},
            "subtitle_inventories": {}, "episode_gaps": [],
            "subtitle_refinement": {
                "confirmed_missing_chinese": [],
                "pending_review_or_probe": [], "resolved_with_chinese": [],
            },
            "probe_evidence": {},
            "summary": {
                "episode_gap_count": 0, "confirmed_subtitle_gap_count": 0,
                "pending_subtitle_verification_count": 0, "complete": True,
            },
        }
        closure = {**closure_core, "evidence_sha256": canonical_digest(closure_core)}
        scope_record = {
            "schema_version": 1, "title_work_key": batch["title_work_key"],
            "worklist_sha256": worklist["worklist_sha256"],
            "scope": [target], "scope_sha256": scope_sha,
            "source_scope_kind": "one_time_exact_movie_member_scope",
            "movie_member_scopes": scopes,
            "before_inventory": envelope, "after_inventory": envelope,
            "remote_mutations": False, "scheduler_dispatch": False,
        }
        status_core = {
            "schema_version": 1, "status": "complete",
            "title_work_key": batch["title_work_key"],
            "scope_sha256": scope_sha,
            "inventory_sha256": envelope["inventory_sha256"],
            "evidence_sha256": closure["evidence_sha256"],
            "remote_mutations": False,
        }
        atomic_json(directory / "title-scope.json", scope_record)
        atomic_json(directory / "title-closure.json", closure)
        atomic_json(directory / "status.json", {
            **status_core, "status_sha256": canonical_digest(status_core),
        })

    def write_tv_exclusion_evidence(self, directory: Path, worklist: dict, batch: dict) -> None:
        tv_scope = batch["tv_exclusion_scope"]
        scopes = [tv_scope]
        target = {
            "media_type": "tv", "target_root": batch["target_root"],
            "category": batch["category"], "tmdb_id": batch["identity"]["tmdb_id"],
            "title": batch["identity"]["title"],
        }
        scope_sha = canonical_digest(scopes)
        inventory_core = {
            "target_root": batch["target_root"],
            "excluded_roots": tv_scope["excluded_roots"],
            "excluded_member_paths_sha256": tv_scope["excluded_member_paths_sha256"],
            "entries": [{
                "path": batch["target_root"] + "/Season 01/S01E01.mkv",
                "size": 100,
            }],
        }
        inventory = {
            **inventory_core,
            "inventory_sha256": canonical_digest(inventory_core),
        }
        closure_core = {
            "schema_version": 1, "audited_at": "2026-08-03T00:00:00+00:00",
            "source_plan_sha256": scope_sha,
            "source_scope_kind": "one_time_exact_tv_root_with_nested_exclusions",
            "title_targets": [target],
            "title_targets_sha256": canonical_digest([target]),
            "tv_exclusion_scopes": scopes,
            "policy": {"remote_mutations": False},
            "subtitle_inventories": {}, "episode_gaps": [],
            "subtitle_refinement": {
                "confirmed_missing_chinese": [],
                "pending_review_or_probe": [], "resolved_with_chinese": [],
            },
            "probe_evidence": {},
            "summary": {
                "episode_gap_count": 0, "confirmed_subtitle_gap_count": 0,
                "pending_subtitle_verification_count": 0, "complete": True,
            },
        }
        closure = {**closure_core, "evidence_sha256": canonical_digest(closure_core)}
        scope_record = {
            "schema_version": 1, "title_work_key": batch["title_work_key"],
            "worklist_sha256": worklist["worklist_sha256"],
            "scope": [target], "scope_sha256": scope_sha,
            "source_scope_kind": "one_time_exact_tv_root_with_nested_exclusions",
            "tv_exclusion_scopes": scopes,
            "before_inventory": inventory, "after_inventory": inventory,
            "remote_mutations": False, "scheduler_dispatch": False,
        }
        status_core = {
            "schema_version": 1, "status": "complete",
            "title_work_key": batch["title_work_key"],
            "scope_sha256": scope_sha,
            "inventory_sha256": inventory["inventory_sha256"],
            "evidence_sha256": closure["evidence_sha256"],
            "remote_mutations": False,
        }
        atomic_json(directory / "title-scope.json", scope_record)
        atomic_json(directory / "title-closure.json", closure)
        atomic_json(directory / "status.json", {
            **status_core, "status_sha256": canonical_digest(status_core),
        })

    def mark_pending_subtitle(self, directory: Path) -> None:
        closure = json.loads((directory / "title-closure.json").read_text())
        closure["subtitle_refinement"]["pending_review_or_probe"] = [
            {"video_path": "/quark/影视/番剧/Example/E01.mkv"},
        ]
        closure["summary"]["pending_subtitle_verification_count"] = 1
        closure["summary"]["complete"] = False
        core = {key: value for key, value in closure.items() if key != "evidence_sha256"}
        closure["evidence_sha256"] = canonical_digest(core)
        atomic_json(directory / "title-closure.json", closure)
        status = json.loads((directory / "status.json").read_text())
        status["status"] = "incomplete"
        status["evidence_sha256"] = closure["evidence_sha256"]
        status_core = {key: value for key, value in status.items() if key != "status_sha256"}
        status["status_sha256"] = canonical_digest(status_core)
        atomic_json(directory / "status.json", status)

    def test_verified_existing_evidence_is_skipped_on_resume(self):
        batch = self.batch(1)
        worklist = self.worklist([batch])
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            directory = root / batch["title_work_key"]
            self.write_evidence(directory, worklist, batch)

            def forbidden(_key: str, _directory: Path) -> AuditResult:
                self.fail("verified evidence must not be relaunched")

            result = run_audits(
                worklist, root, max_workers=3, pause_unchanged=lambda: True,
                invoke=forbidden, progress_path=root / "progress.json",
            )
            self.assertEqual(result["verified_skipped"], 1)
            self.assertEqual(result["completed_this_run"], 0)
            progress = json.loads((root / "progress.json").read_text())
            self.assertFalse(progress["remote_mutations"])
            self.assertFalse(progress["scheduler_dispatch"])

    def test_tampered_evidence_is_not_skipped(self):
        batch = self.batch(2)
        worklist = self.worklist([batch])
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            directory = root / batch["title_work_key"]
            self.write_evidence(directory, worklist, batch)
            status = json.loads((directory / "status.json").read_text())
            status["inventory_sha256"] = "0" * 64
            atomic_json(directory / "status.json", status)
            self.assertFalse(evidence_is_verified(directory, worklist, batch))
            calls: list[str] = []

            def invoke(key: str, target: Path) -> AuditResult:
                calls.append(key)
                self.write_evidence(target, worklist, batch)
                return AuditResult(key, 0)

            result = run_audits(
                worklist, root, max_workers=1, pause_unchanged=lambda: True,
                invoke=invoke,
            )
            self.assertEqual(calls, [batch["title_work_key"]])
            self.assertEqual(result["completed_this_run"], 1)

    def test_movie_evidence_binds_exact_members_and_parent_fingerprint(self):
        batch = self.movie_batch()
        worklist = self.worklist([batch])
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw) / batch["title_work_key"]
            self.write_movie_evidence(directory, worklist, batch)
            self.assertTrue(evidence_is_verified(directory, worklist, batch))

            scope = json.loads((directory / "title-scope.json").read_text())
            after = scope["after_inventory"]
            parent = after["parent_inventory"]
            parent["entries"].append({"path": "/quark/影视/电影/Shared/New.mkv", "size": 1})
            parent_core = {key: value for key, value in parent.items() if key != "inventory_sha256"}
            parent["inventory_sha256"] = canonical_digest(parent_core)
            envelope_core = {
                "member_inventory": after["member_inventory"],
                "parent_inventory": parent,
            }
            after["inventory_sha256"] = canonical_digest(envelope_core)
            atomic_json(directory / "title-scope.json", scope)
            self.assertFalse(evidence_is_verified(directory, worklist, batch))

    def test_movie_evidence_rejects_scope_not_matching_sealed_candidate(self):
        batch = self.movie_batch()
        worklist = self.worklist([batch])
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw) / batch["title_work_key"]
            self.write_movie_evidence(directory, worklist, batch)
            scope = json.loads((directory / "title-scope.json").read_text())
            scope["movie_member_scopes"][0]["tmdb_id"] = 100
            atomic_json(directory / "title-scope.json", scope)
            self.assertFalse(evidence_is_verified(directory, worklist, batch))

    def test_tv_exclusion_evidence_binds_roots_members_and_fingerprints(self):
        batch = self.tv_exclusion_batch()
        worklist = self.worklist([batch])
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw) / batch["title_work_key"]
            self.write_tv_exclusion_evidence(directory, worklist, batch)
            self.assertTrue(evidence_is_verified(directory, worklist, batch))

            closure = json.loads((directory / "title-closure.json").read_text())
            closure["tv_exclusion_scopes"][0]["excluded_roots"].append(
                batch["target_root"] + "/Unsealed",
            )
            core = {key: value for key, value in closure.items() if key != "evidence_sha256"}
            closure["evidence_sha256"] = canonical_digest(core)
            atomic_json(directory / "title-closure.json", closure)
            self.assertFalse(evidence_is_verified(directory, worklist, batch))

    def test_reruns_only_verified_titles_that_still_need_ocr(self):
        pending, resolved = self.batch(3), self.batch(4)
        worklist = self.worklist([pending, resolved])
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            for batch in (pending, resolved):
                self.write_evidence(root / batch["title_work_key"], worklist, batch)
            self.mark_pending_subtitle(root / pending["title_work_key"])
            calls: list[str] = []

            def invoke(key: str, directory: Path) -> AuditResult:
                calls.append(key)
                self.write_evidence(directory, worklist, pending)
                return AuditResult(key, 0)

            result = run_audits(
                worklist, root, max_workers=1, pause_unchanged=lambda: True,
                invoke=invoke, rerun_pending_subtitles=True,
            )
            self.assertEqual(calls, [pending["title_work_key"]])
            self.assertEqual(result["verified_skipped"], 1)
            self.assertEqual(result["completed_this_run"], 1)

    def test_runs_at_most_three_title_scopes_concurrently(self):
        batches = [self.batch(index) for index in range(10, 16)]
        worklist = self.worklist(batches)
        barrier = threading.Barrier(3)
        guard = threading.Lock()
        active = 0
        maximum = 0

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)

            def invoke(key: str, directory: Path) -> AuditResult:
                nonlocal active, maximum
                with guard:
                    active += 1
                    maximum = max(maximum, active)
                if int(key, 16) < 13:
                    barrier.wait(timeout=2)
                batch = next(row for row in batches if row["title_work_key"] == key)
                self.write_evidence(directory, worklist, batch)
                with guard:
                    active -= 1
                return AuditResult(key, 0)

            result = run_audits(
                worklist, root, max_workers=3, pause_unchanged=lambda: True,
                invoke=invoke,
            )
            self.assertEqual(maximum, 3)
            self.assertEqual(result["completed_this_run"], 6)
            self.assertEqual(result["failed"], 0)
            self.assertTrue(all(
                (root / batch["title_work_key"] / "title-closure.json").exists()
                for batch in batches
            ))

    def test_refuses_to_start_if_global_pause_changed(self):
        worklist = self.worklist([self.batch(30)])
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaisesRegex(ValueError, "持久暂停证据已变化"):
                run_audits(
                    worklist, Path(raw), max_workers=3,
                    pause_unchanged=lambda: False,
                    invoke=lambda key, path: AuditResult(key, 0),
                )

    def test_refuses_more_than_three_workers(self):
        worklist = self.worklist([])
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaisesRegex(ValueError, "1..3"):
                run_audits(
                    worklist, Path(raw), max_workers=4,
                    pause_unchanged=lambda: True,
                    invoke=lambda key, path: AuditResult(key, 0),
                )


if __name__ == "__main__":
    unittest.main()
