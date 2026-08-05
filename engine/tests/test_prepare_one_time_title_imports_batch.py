from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from engine.scrapeflow.one_time_library_completion import canonical_digest, seal_one_time_worklist
from engine.tools.prepare_one_time_title_imports_batch import main, prepare_batch


class PrepareOneTimeTitleImportsBatchTests(unittest.TestCase):
    control = {"paused": True, "persistent": True}

    def batch(self, number: int) -> dict:
        target = {
            "media_type": "tv", "target_root": f"/quark/影视/番剧/Show-{number}",
            "category": "番剧", "tmdb_id": number, "title": f"Show {number}",
        }
        return {
            "target_root": target["target_root"], "category": "番剧",
            "identity": {
                "status": "exact", "tmdb_id": number, "media_type": "tv",
                "title": target["title"], "identity_sha256": canonical_digest(target),
            },
            "title_work_key": f"{number:064x}",
            "read_only_audit_allowed": True, "mutation_allowed": False,
        }

    def worklist(self, batches: list[dict]) -> dict:
        return seal_one_time_worklist({
            "schema_version": 1, "kind": "one_time_library_completion_worklist",
            "dispatch_gate": {"allowed": False, "reason": "global_pause_active"},
            "observations": {"title_batches": {"batches": batches}},
        })

    def write_audit(self, root: Path, worklist: dict, batch: dict, *, complete: bool) -> None:
        target = {
            "media_type": "tv", "target_root": batch["target_root"],
            "category": batch["category"], "tmdb_id": batch["identity"]["tmdb_id"],
            "title": batch["identity"]["title"],
        }
        scope_sha = canonical_digest([target])
        inventory_sha = canonical_digest(target)
        gaps = [] if complete else [{
            "kind": "missing_episode", "lane": "s00", "season": 0,
            "episode": 2, "label": "S00E02", "title": "Special",
            "season_name": "Specials", "expected_episode_count": 2,
            "target_root": target["target_root"], "media_type": "tv",
            "tmdb_id": target["tmdb_id"],
            "media": {**target, "original_title": target["title"]},
        }]
        summary = {
            "episode_gap_count": len(gaps), "confirmed_subtitle_gap_count": 0,
            "pending_subtitle_verification_count": 0, "complete": complete,
        }
        closure_core = {
            "schema_version": 1, "audited_at": "2026-08-03T00:00:00+00:00",
            "source_plan_sha256": scope_sha,
            "source_scope_kind": "one_time_exact_title_scope",
            "title_targets": [target], "title_targets_sha256": scope_sha,
            "policy": {"remote_mutations": False}, "subtitle_inventories": {},
            "episode_gaps": gaps,
            "subtitle_refinement": {
                "confirmed_missing_chinese": [], "pending_review_or_probe": [],
                "resolved_with_chinese": [],
            },
            "probe_evidence": {}, "summary": summary,
        }
        closure = {**closure_core, "evidence_sha256": canonical_digest(closure_core)}
        scope = {
            "schema_version": 1, "title_work_key": batch["title_work_key"],
            "worklist_sha256": worklist["worklist_sha256"], "scope": [target],
            "scope_sha256": scope_sha,
            "before_inventory": {"inventory_sha256": inventory_sha},
            "after_inventory": {"inventory_sha256": inventory_sha},
            "remote_mutations": False, "scheduler_dispatch": False,
        }
        directory = root / batch["title_work_key"]
        directory.mkdir(parents=True)
        (directory / "title-scope.json").write_text(json.dumps(scope), encoding="utf-8")
        (directory / "title-closure.json").write_text(json.dumps(closure), encoding="utf-8")

    def test_prepares_only_incomplete_titles_and_skips_complete(self):
        incomplete, complete = self.batch(1), self.batch(2)
        worklist = self.worklist([incomplete, complete])
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw); audits = base / "audits"; output = base / "imports"
            self.write_audit(audits, worklist, incomplete, complete=False)
            self.write_audit(audits, worklist, complete, complete=True)
            result = prepare_batch(
                worklist, audits, output, read_pause_control=lambda: self.control,
            )
            self.assertEqual(result["prepared"], [incomplete["title_work_key"]])
            self.assertEqual(result["complete_skipped"], [complete["title_work_key"]])
            prepared = json.loads((output / f'{incomplete["title_work_key"]}.json').read_text())
            self.assertEqual(prepared["status"], "prepared_not_dispatched")
            self.assertFalse(prepared["dispatch_allowed"])
            self.assertEqual(prepared["safety"]["jobs_created"], 0)
            self.assertFalse((output / f'{complete["title_work_key"]}.json').exists())

    def test_missing_and_invalid_evidence_are_failures(self):
        missing, invalid = self.batch(10), self.batch(11)
        worklist = self.worklist([missing, invalid])
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw); audits = base / "audits"; output = base / "imports"
            self.write_audit(audits, worklist, invalid, complete=False)
            closure_path = audits / invalid["title_work_key"] / "title-closure.json"
            closure = json.loads(closure_path.read_text())
            closure["summary"]["episode_gap_count"] = 99
            closure_path.write_text(json.dumps(closure), encoding="utf-8")
            result = prepare_batch(
                worklist, audits, output, read_pause_control=lambda: self.control,
            )
            self.assertEqual(set(result["failures"]), {
                missing["title_work_key"], invalid["title_work_key"],
            })
            self.assertEqual(result["prepared"], [])

    def test_progress_and_summary_are_atomic_digested_envelopes(self):
        batch = self.batch(20); worklist = self.worklist([batch])
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw); audits = base / "audits"; output = base / "imports"
            self.write_audit(audits, worklist, batch, complete=True)
            prepare_batch(worklist, audits, output, read_pause_control=lambda: self.control)
            for name, kind in (
                ("batch-progress.json", "one_time_title_import_batch_progress"),
                ("batch-summary.json", "one_time_title_import_batch_summary"),
            ):
                value = json.loads((output / name).read_text())
                digest = value.pop("batch_sha256")
                self.assertEqual(value["kind"], kind)
                self.assertEqual(digest, canonical_digest(value))
                self.assertFalse(value["remote_mutations"])
                self.assertFalse(value["scheduler_dispatch"])

    def test_pause_change_stops_remaining_preparations(self):
        batches = [self.batch(30), self.batch(31)]
        worklist = self.worklist(batches)
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw); audits = base / "audits"; output = base / "imports"
            for batch in batches:
                self.write_audit(audits, worklist, batch, complete=False)
            result = prepare_batch(
                worklist, audits, output,
                read_pause_control=lambda: {
                    "paused": False, "persistent": True,
                },
            )
            self.assertTrue(result["stopped_for_pause_change"])
            self.assertEqual(set(result["failures"]), {
                batch["title_work_key"] for batch in batches
            })
            self.assertEqual(result["prepared"], [])

    def test_cli_returns_nonzero_when_evidence_is_missing(self):
        worklist = self.worklist([self.batch(40)])
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            worklist_path = base / "worklist.json"
            control_path = base / "control.json"
            worklist_path.write_text(json.dumps(worklist), encoding="utf-8")
            control_path.write_text(json.dumps({"paused": True}), encoding="utf-8")
            with patch(
                "engine.tools.prepare_one_time_title_imports_batch.read_live_pause_control",
                return_value=self.control,
            ):
                code = main([
                    "--worklist", str(worklist_path),
                    "--audits-root", str(base / "audits"),
                    "--output-dir", str(base / "imports"),
                    "--global-control", str(control_path),
                ])
            self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
