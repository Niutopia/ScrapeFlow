"""Focused tests for explicit work-level subtitle opt-outs."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from local.scrapeflow_api.simple_library_audit import (
    automatic_works_from_engine_jobs,
    build_automatic_library_gaps,
)
from local.scrapeflow_api.subtitle_policy import (
    SUBTITLE_POLICY_KIND,
    SUBTITLE_POLICY_SCHEMA_VERSION,
    load_subtitle_policy_overrides,
    subtitle_policy_override_path,
)


SHOW = "/library/Show"
VIDEO = f"{SHOW}/Show.S01E01.mkv"


def _report() -> dict[str, object]:
    return {
        "status": "completed",
        "complete": True,
        "inventory": [{
            "type": "file", "path": VIDEO, "size": 100, "version": "v1",
        }],
        "observations": {"media_directories": []},
    }


class SubtitlePolicyOverrideTests(unittest.TestCase):
    def test_hard_subtitle_override_suppresses_only_confirmed_missing_gap(self) -> None:
        work = {
            "tmdb_id": 100, "title": "Show", "media_type": "tv",
            "target_root": SHOW, "season": 1, "expected_episodes": {1: [1]},
        }
        missing = build_automatic_library_gaps(
            _report(), [work], required_subtitle_language="zh",
            subtitle_checker=lambda _path: {"status": "missing", "source": "embedded"},
            subtitle_policy_overrides=[{
                "id": "show-hard-sub", "tmdb_id": 100,
                "target_root": SHOW, "mode": "hard_subtitle",
            }],
        )
        self.assertFalse(any(row["kind"] == "missing_subtitle" for row in missing["gaps"]))
        self.assertEqual(len(missing["subtitle_policy_overrides"]), 1)
        self.assertEqual(missing["subtitle_policy_overrides"][0]["id"], "show-hard-sub")

        unknown = build_automatic_library_gaps(
            _report(), [{**work, "subtitle_policy": "hard_subtitle"}],
            required_subtitle_language="zh",
            subtitle_checker=lambda _path: {"status": "unknown", "reason": "ffprobe_timeout"},
        )
        self.assertTrue(any(row["kind"] == "unknown_subtitle_evidence" for row in unknown["unknowns"]))
        self.assertFalse(unknown["subtitle_policy_overrides"])

    def test_state_file_is_strict_and_matches_exact_work_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = subtitle_policy_override_path(directory)
            assert path is not None
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({
                "schema_version": SUBTITLE_POLICY_SCHEMA_VERSION,
                "kind": SUBTITLE_POLICY_KIND,
                "records": [{
                    "id": "show-none", "target_root": SHOW, "tmdb_id": 100,
                    "mode": "no_subtitle", "languages": ["zh"],
                    "reason": "operator-confirmed",
                }],
            }, ensure_ascii=False), encoding="utf-8")
            rows = load_subtitle_policy_overrides(directory)
            self.assertEqual(rows[0]["mode"], "no_subtitle")
            self.assertEqual(rows[0]["languages"], ["zh"])

            path.write_text(json.dumps({
                "schema_version": SUBTITLE_POLICY_SCHEMA_VERSION,
                "kind": SUBTITLE_POLICY_KIND,
                "records": [{
                    "id": "bad", "target_root": "/library/../escape", "tmdb_id": 100,
                    "mode": "hard_subtitle",
                }],
            }), encoding="utf-8")
            self.assertEqual(load_subtitle_policy_overrides(directory), [])

    def test_nested_engine_metadata_preserves_zh_hant_policy_and_language_scope(self) -> None:
        job = {
            "id": "job-nested-policy",
            "phase": "executed",
            "created_at": "2026-08-01T00:00:00Z",
            "updated_at": "2026-08-01T00:00:00Z",
            "request": {},
            "plan": {
                "mode": "tv",
                "target_root": SHOW,
                "metadata": {
                    "tmdb_id": 100,
                    "title": "Show",
                    "media_type": "tv",
                    "series_root": SHOW,
                    "subtitle_policy": {
                        "mode": "hard_subtitle",
                        "languages": ["zh-Hant"],
                    },
                },
            },
            "summary": {"identity": {"tmdb_id": 100, "media_type": "tv"}},
        }
        works = automatic_works_from_engine_jobs([job])
        self.assertEqual(len(works), 1)
        self.assertEqual(works[0]["subtitle_policy"]["languages"], ["zh-Hant"])

        result = build_automatic_library_gaps(
            _report(), works, required_subtitle_language="zh-Hant",
            subtitle_checker=lambda _path: {"status": "missing", "source": "embedded"},
        )
        self.assertFalse(any(row["kind"] == "missing_subtitle" for row in result["gaps"]))

        mismatch = build_automatic_library_gaps(
            _report(), works, required_subtitle_language="en",
            subtitle_checker=lambda _path: {"status": "missing", "source": "embedded"},
        )
        self.assertTrue(any(row["kind"] == "missing_subtitle" for row in mismatch["gaps"]))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
