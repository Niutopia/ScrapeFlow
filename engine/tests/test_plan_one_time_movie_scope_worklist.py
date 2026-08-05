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
    one_time_worklist_is_valid,
    seal_one_time_worklist,
)
from engine.tools.plan_one_time_movie_scope_worklist import (
    _write_new_json,
    build_phase_two_movie_worklist,
    phase_two_movie_worklist_is_valid,
)


class PhaseTwoMovieWorklistTests(unittest.TestCase):
    target = "/quark/影视/电影/Movie A (2026)"

    def project(self) -> dict:
        return {
            "title": "Movie A",
            "official_title": "Movie A",
            "tmdb_ids": [99],
            "media_type": "movie",
            "target_root": self.target,
            "video_files": [self.target + ".mkv"],
            "issues": [],
            "regular_missing": [],
            "optional_missing": [],
            "metadata_issues": [{"code": "subtitle_gap"}],
        }

    def phase_one(self, *extra_blockers: dict) -> dict:
        library = build_one_time_library_plan({"projects": [self.project()]})
        title_batches = build_one_time_title_batches(library)
        batch = title_batches["batches"][0]
        batch["scope_blockers"] = [
            {"reason": "exact_movie_member_scope_required"},
            *deepcopy(list(extra_blockers)),
        ]
        batch["read_only_audit_allowed"] = False
        title_batches["blocked_title_scope_count"] = 1
        report = build_one_time_worklist(
            global_control={"paused": True, "persistent": True},
            inbox_plan={"mutation": False, "sources": []},
            library_plan=library,
            cleanup_plan={"mutation": False, "actions": []},
        )
        report["observations"]["title_batches"] = title_batches
        report["inputs"] = {"fixture": "phase-one"}
        return seal_one_time_worklist(report)

    def build(self, phase_one: dict) -> dict:
        return build_phase_two_movie_worklist(
            phase_one=phase_one,
            phase_one_path="/sealed/worklist.json",
            projects=[self.project()],
            confirmed_subtitle_gaps=[],
            pending_subtitle_verification=[],
            runtime_control={"paused": True, "persistent": True},
            input_evidence={"fixture": "phase-two"},
        )

    def test_builds_independent_valid_phase_two_without_changing_phase_one(self):
        phase_one = self.phase_one()
        before = json.dumps(phase_one, ensure_ascii=False, sort_keys=True)

        phase_two = self.build(phase_one)

        self.assertEqual(json.dumps(phase_one, ensure_ascii=False, sort_keys=True), before)
        self.assertTrue(one_time_worklist_is_valid(phase_one))
        self.assertTrue(phase_two_movie_worklist_is_valid(phase_two))
        self.assertEqual(
            phase_two["phase"]["predecessor_worklist_sha256"],
            phase_one["worklist_sha256"],
        )
        batch = phase_two["observations"]["title_batches"]["batches"][0]
        self.assertTrue(batch["read_only_audit_allowed"])
        self.assertEqual(batch["scope_blockers"], [])
        self.assertEqual(batch["movie_member_candidate"]["target_stem"], self.target)

    def test_preserves_every_predecessor_blocker_except_implementation_marker(self):
        blocker = {
            "reason": "duplicate_tmdb_identity_roots",
            "target_roots": [self.target, self.target + " copy"],
        }
        phase_two = self.build(self.phase_one(blocker))
        batches = phase_two["observations"]["title_batches"]
        batch = batches["batches"][0]

        self.assertFalse(batch["read_only_audit_allowed"])
        self.assertEqual(batches["blocked_title_scope_count"], 1)
        self.assertIn(blocker, batch["scope_blockers"])
        self.assertNotIn(
            "exact_movie_member_scope_required",
            {row.get("reason") for row in batch["scope_blockers"]},
        )

    def test_rejects_missing_phase_one_movie_from_current_audit(self):
        with self.assertRaisesRegex(ValueError, "缺少 phase-1 电影目标"):
            build_phase_two_movie_worklist(
                phase_one=self.phase_one(),
                phase_one_path="/sealed/worklist.json",
                projects=[],
                confirmed_subtitle_gaps=[],
                pending_subtitle_verification=[],
                runtime_control={"paused": True, "persistent": True},
                input_evidence={},
            )

    def test_output_create_is_exclusive_and_preserves_existing_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "phase-two.json"
            original = b"already sealed\n"
            output.write_bytes(original)

            with self.assertRaises(FileExistsError):
                _write_new_json(output, {"replacement": True})

            self.assertEqual(output.read_bytes(), original)
            self.assertEqual(list(output.parent.glob("*.tmp-*")), [])


if __name__ == "__main__":
    unittest.main()
