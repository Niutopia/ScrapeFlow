from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from local.scrapeflow_api.simple_library_audit import (
    build_automatic_library_gaps,
    make_alist_subtitle_checker,
    probe_remote_subtitle_content,
)


def _report(sidecar: bytes) -> tuple[dict[str, object], object]:
    video = "/library/Show/Show.S01E01.mkv"
    subtitle = "/library/Show/Show.S01E01.zh.srt"

    class Client:
        def read_file_prefix(self, path: str, *, max_bytes: int) -> bytes:
            del max_bytes
            if path == subtitle:
                return sidecar
            raise AssertionError(path)

    report: dict[str, object] = {
        "status": "completed",
        "complete": True,
        "clean": True,
        "inventory": [
            {"type": "file", "path": video, "size": 100, "version": "v1"},
            {"type": "file", "path": subtitle, "size": len(sidecar), "version": "s1"},
        ],
        "observations": {"media_directories": []},
    }
    return report, Client()


class SubtitleSidecarEvidenceTests(unittest.TestCase):
    def _run(self, payload: bytes) -> dict[str, object]:
        report, client = _report(payload)
        checker = make_alist_subtitle_checker(client, "zh")
        with patch(
            "local.scrapeflow_api.simple_library_audit.probe_remote_subtitle_streams",
            return_value={"status": "unknown", "reason": "ffprobe_timeout"},
        ):
            return build_automatic_library_gaps(
                report,
                [{
                    "tmdb_id": 7,
                    "title": "Show",
                    "media_type": "tv",
                    "target_root": "/library/Show",
                    "season": 1,
                    "expected_episodes": {1: [1]},
                }],
                required_subtitle_language="zh",
                subtitle_checker=checker,
            )

    def test_explicit_zh_filename_does_not_override_traditional_content(self) -> None:
        result = self._run(
            "1\n00:00:01,000 --> 00:00:02,000\n這是一個測試\n".encode("utf-8")
        )
        self.assertFalse(any(row.get("kind") == "missing_subtitle" for row in result["gaps"]))
        self.assertTrue(any(row.get("kind") == "unknown_subtitle_evidence" for row in result["unknowns"]))

    def test_simplified_content_satisfies_even_when_video_probe_is_unknown(self) -> None:
        result = self._run(
            "1\n00:00:01,000 --> 00:00:02,000\n这是一个测试\n".encode("utf-8")
        )
        self.assertFalse(any(row.get("kind") == "missing_subtitle" for row in result["gaps"]))
        self.assertFalse(any(row.get("kind") == "unknown_subtitle_evidence" for row in result["unknowns"]))

    def test_unreadable_sidecar_stays_unknown_not_missing(self) -> None:
        result = self._run(b"not a subtitle")
        self.assertFalse(any(row.get("kind") == "missing_subtitle" for row in result["gaps"]))
        self.assertTrue(any(row.get("kind") == "unknown_subtitle_evidence" for row in result["unknowns"]))

    def test_sidecar_verdict_reuses_exact_path_size_version_ledger_key(self) -> None:
        report, client = _report(
            "1\n00:00:01,000 --> 00:00:02,000\n这是一个测试\n".encode("utf-8")
        )
        del report
        subtitle = "/library/Show/Show.S01E01.zh.srt"
        row = [{"path": subtitle, "size": 28, "version": "s1"}]
        with tempfile.TemporaryDirectory() as directory, patch(
            "local.scrapeflow_api.simple_library_audit.probe_remote_subtitle_content",
            wraps=probe_remote_subtitle_content,
        ) as probe:
            first = make_alist_subtitle_checker(client, "zh", state_root=directory)
            self.assertEqual(first.sidecar_evidence(row)["status"], "satisfied")
            second = make_alist_subtitle_checker(client, "zh", state_root=directory)
            self.assertEqual(second.sidecar_evidence(row)["status"], "satisfied")
            self.assertEqual(probe.call_count, 1)
            ledger_path = Path(directory) / "library-audit" / "subtitle-evidence-ledger.json"
            payload = json.loads(ledger_path.read_text(encoding="utf-8"))
        self.assertEqual(len(payload["entries"]), 1)
        key = next(iter(payload["entries"]))
        decoded = json.loads(key)
        self.assertEqual(decoded[1:5], [subtitle, 28, "s1", "zh"])

    def test_ledger_language_coordinate_does_not_cross_reuse_simplified_and_traditional(self) -> None:
        payload = "1\n00:00:01,000 --> 00:00:02,000\n這是一個測試\n".encode("utf-8")
        report, client = _report(payload)
        del report
        subtitle = "/library/Show/Show.S01E01.zh.srt"
        row = [{"path": subtitle, "size": len(payload), "version": "s1"}]
        with tempfile.TemporaryDirectory() as directory, patch(
            "local.scrapeflow_api.simple_library_audit.probe_remote_subtitle_content",
            wraps=probe_remote_subtitle_content,
        ) as probe:
            traditional = make_alist_subtitle_checker(
                client, "zh-Hant", state_root=directory,
            )
            self.assertEqual(traditional.sidecar_evidence(row)["status"], "satisfied")
            simplified = make_alist_subtitle_checker(
                client, "zh", state_root=directory,
            )
            # The exact same path/size/version must be probed under the
            # different required language and must not inherit the
            # traditional verdict as simplified evidence.
            self.assertEqual(simplified.sidecar_evidence(row)["status"], "missing")
            self.assertEqual(probe.call_count, 2)
            ledger_path = Path(directory) / "library-audit" / "subtitle-evidence-ledger.json"
            payload_json = json.loads(ledger_path.read_text(encoding="utf-8"))
        self.assertEqual(len(payload_json["entries"]), 2)
        languages = {json.loads(key)[4] for key in payload_json["entries"]}
        self.assertEqual(languages, {"zh", "zht"})

    def test_sidecar_without_version_still_obeys_probe_cardinality_budget(self) -> None:
        class Client:
            def read_file_prefix(self, path: str, *, max_bytes: int) -> bytes:
                del path, max_bytes
                return b"not a subtitle"

        rows = [
            {"path": f"/library/Show/Show.S01E01.{suffix}.srt"}
            for suffix in ("a", "b")
        ]
        with patch.dict(os.environ, {"SCRAPEFLOW_SUBTITLE_CONTENT_PROBE_MAX_FILES": "1"}), patch(
            "local.scrapeflow_api.simple_library_audit.probe_remote_subtitle_content",
            return_value={"status": "unknown", "reason": "subtitle_decode_or_format_unknown"},
        ) as probe:
            checker = make_alist_subtitle_checker(Client(), "zh")
            result = checker.sidecar_evidence(rows)
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(probe.call_count, 1)


if __name__ == "__main__":
    unittest.main()
