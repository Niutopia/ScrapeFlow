from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from local.scrapeflow_api.content_identity_overrides import (
    FATE_CHILD_VIDEO_PATH,
    FATE_PARENT_NFO_PATH,
    FATE_PARENT_VIDEO_PATH,
    FATE_MOVIE_TARGET_ROOT,
    FATE_CHILD_MEDIA_EVIDENCE,
    _expected_record,
    apply_content_identity_overrides,
    probe_content_identity,
)
from local.scrapeflow_api.simple_library_audit import build_automatic_library_gaps


TV_ROOT = "/quark/影视/番剧/Fate/命运／奇异赝品"
TV_NFO = f"{TV_ROOT}/tvshow.nfo"
SEASON_ROOT = f"{TV_ROOT}/Season 01"
TV_EPISODE = f"{SEASON_ROOT}/命运／奇异赝品 - S01E01 - 英灵事件.mkv"


class ContentIdentityOverrideTests(unittest.TestCase):
    @staticmethod
    def _write_state(root: str | Path, record: dict[str, object] | None = None) -> None:
        path = Path(root) / "library-audit" / "content-identity-overrides.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        row = copy.deepcopy(record or _expected_record())
        path.write_text(
            json.dumps({
                "schema_version": 1,
                "kind": "content_identity_overrides",
                "records": [row],
            }, ensure_ascii=False),
            encoding="utf-8",
        )

    @staticmethod
    def _report(*, include_child: bool = True, child_size: int = 3419664950) -> dict[str, object]:
        inventory: list[dict[str, object]] = [
            {"path": "/quark/影视/番剧", "type": "directory"},
            {"path": FATE_MOVIE_TARGET_ROOT, "type": "directory"},
            {"path": TV_ROOT, "type": "directory"},
            {"path": SEASON_ROOT, "type": "directory"},
            {
                "path": FATE_PARENT_VIDEO_PATH,
                "type": "file",
                "size": 5742279376,
                "version": "2026-07-26T13:27:59.410Z",
            },
            {
                "path": FATE_PARENT_NFO_PATH,
                "type": "file",
                "size": 213,
                "version": "2026-07-26T13:34:44.427Z",
            },
            {
                "path": TV_NFO,
                "type": "file",
                "size": 200,
                "version": "2026-07-26T13:34:07.219Z",
            },
            {
                "path": TV_EPISODE,
                "type": "file",
                "size": 2000000000,
                "version": "2026-07-27T05:32:51.374Z",
            },
        ]
        if include_child:
            inventory.append({
                "path": FATE_CHILD_VIDEO_PATH,
                "type": "file",
                "size": child_size,
                "version": "2026-07-26T13:27:48.068Z",
            })
        return {
            "status": "completed",
            "complete": True,
            "clean": True,
            "inventory": inventory,
            "observations": {
                "media_directories": [
                    {
                        "path": FATE_MOVIE_TARGET_ROOT,
                        "video_count": 1,
                        "subtitle_count": 1,
                        "nfo_count": 1,
                        "poster_count": 1,
                        "has_nfo": True,
                        "has_poster": True,
                        "nfo_path": FATE_PARENT_NFO_PATH,
                        "metadata_source": None,
                    },
                    {
                        "path": TV_ROOT,
                        "video_count": 1 if include_child else 0,
                        "subtitle_count": 0,
                        "nfo_count": 1,
                        "poster_count": 1,
                        "has_nfo": True,
                        "has_poster": True,
                        "nfo_path": TV_NFO,
                        "metadata_source": None,
                    },
                    {
                        "path": SEASON_ROOT,
                        "video_count": 1,
                        "subtitle_count": 0,
                        "nfo_count": 1,
                        "poster_count": 1,
                        "has_nfo": True,
                        "has_poster": True,
                        "nfo_path": TV_NFO,
                        "metadata_source": TV_ROOT,
                    },
                ],
            },
        }

    @staticmethod
    def _works() -> list[dict[str, object]]:
        return [
            {
                "tmdb_id": 1145612,
                "title": "命运／奇异赝品 黎明低语",
                "year": "2023",
                "media_type": "movie",
                "target_root": FATE_MOVIE_TARGET_ROOT,
                "identity_source": "library_nfo",
                "identity_sources": ["library_nfo"],
                "nfo_path": FATE_PARENT_NFO_PATH,
                "identity_scope": {
                    "kind": "video_file",
                    "video_path": FATE_PARENT_VIDEO_PATH,
                },
            },
            {
                "tmdb_id": 229858,
                "title": "命运／奇异赝品",
                "year": "2024",
                "media_type": "tv",
                "target_root": TV_ROOT,
                "identity_source": "library_nfo",
                "identity_sources": ["library_nfo"],
                "nfo_path": TV_NFO,
                "identity_scope": {
                    "kind": "tv_metadata_source",
                    "metadata_source": TV_ROOT,
                    "media_paths": [SEASON_ROOT],
                    "video_paths": [TV_EPISODE],
                },
                "expected_episodes": {1: [1]},
            },
        ]

    def test_exact_witness_adds_only_child_file_to_movie_scope(self) -> None:
        report = self._report()
        with tempfile.TemporaryDirectory() as temporary:
            self._write_state(temporary)
            works, diagnostics = apply_content_identity_overrides(
                report,
                self._works(),
                object(),
                temporary,
                probe=lambda _client, _record: (True, "matched"),
            )

        movie = next(row for row in works if row["tmdb_id"] == 1145612)
        self.assertEqual(
            movie["identity_scope"],
            {
                "kind": "video_files",
                "video_paths": sorted([FATE_CHILD_VIDEO_PATH, FATE_PARENT_VIDEO_PATH], key=str.casefold),
            },
        )
        tv = next(row for row in works if row["tmdb_id"] == 229858)
        self.assertEqual(tv["identity_scope"]["video_paths"], [TV_EPISODE])
        self.assertEqual([row["path"] for row in diagnostics["applied"]], [FATE_CHILD_VIDEO_PATH])
        self.assertEqual(diagnostics["rejected"], [])

        semantic = build_automatic_library_gaps(report, works)
        self.assertFalse(any(
            row.get("kind") == "unknown_library_work"
            and FATE_CHILD_VIDEO_PATH in row.get("uncovered_video_paths", [])
            for row in semantic["unknowns"]
        ))

    def test_media_probe_mismatch_keeps_tv_boundary_unknown(self) -> None:
        report = self._report()
        with tempfile.TemporaryDirectory() as temporary:
            self._write_state(temporary)
            works, diagnostics = apply_content_identity_overrides(
                report,
                self._works(),
                object(),
                temporary,
                probe=lambda _client, _record: (False, "ffprobe_evidence_mismatch"),
            )
        movie = next(row for row in works if row["tmdb_id"] == 1145612)
        self.assertEqual(movie["identity_scope"]["kind"], "video_file")
        self.assertEqual(diagnostics["applied"], [])
        self.assertEqual(diagnostics["rejected"][0]["reason"], "ffprobe_evidence_mismatch")
        semantic = build_automatic_library_gaps(report, works)
        self.assertTrue(any(
            row.get("kind") == "unknown_library_work"
            and FATE_CHILD_VIDEO_PATH in row.get("uncovered_video_paths", [])
            for row in semantic["unknowns"]
        ))

    def test_parent_same_name_and_year_is_not_a_witness_target(self) -> None:
        report = self._report(include_child=False)
        other_path = "/quark/影视/番剧/Other/命运／奇异赝品 黎明低语 (2023).mkv"
        report["inventory"].extend([
            {"path": "/quark/影视/番剧/Other", "type": "directory"},
            {"path": other_path, "type": "file", "size": 3419664950, "version": "same"},
        ])
        report["observations"]["media_directories"].append({
            "path": "/quark/影视/番剧/Other",
            "video_count": 1,
            "subtitle_count": 0,
            "nfo_count": 0,
            "poster_count": 0,
            "has_nfo": False,
            "has_poster": False,
            "nfo_path": None,
            "metadata_source": None,
        })
        with tempfile.TemporaryDirectory() as temporary:
            self._write_state(temporary)
            works, diagnostics = apply_content_identity_overrides(
                report,
                self._works(),
                object(),
                temporary,
                probe=lambda _client, _record: (True, "matched"),
            )
        self.assertEqual(diagnostics["applied"], [])
        semantic = build_automatic_library_gaps(report, works)
        self.assertTrue(any(
            row.get("kind") == "unknown_library_work"
            and row.get("path") == "/quark/影视/番剧/Other"
            and other_path in row.get("uncovered_video_paths", [])
            for row in semantic["unknowns"]
        ))

    def test_hand_edited_state_cannot_redirect_the_allowlist(self) -> None:
        report = self._report()
        record = _expected_record()
        record["path"] = FATE_PARENT_VIDEO_PATH
        with tempfile.TemporaryDirectory() as temporary:
            self._write_state(temporary, record)
            works, diagnostics = apply_content_identity_overrides(
                report,
                self._works(),
                object(),
                temporary,
                probe=lambda _client, _record: (True, "matched"),
            )
        self.assertEqual(diagnostics["applied"], [])
        self.assertEqual(diagnostics["rejected"], [])
        movie = next(row for row in works if row["tmdb_id"] == 1145612)
        self.assertEqual(movie["identity_scope"]["kind"], "video_file")

    def test_inventory_size_or_version_mismatch_is_fail_closed(self) -> None:
        report = self._report(child_size=3419664951)
        with tempfile.TemporaryDirectory() as temporary:
            self._write_state(temporary)
            works, diagnostics = apply_content_identity_overrides(
                report,
                self._works(),
                object(),
                temporary,
                probe=lambda _client, _record: (True, "matched"),
            )
        self.assertEqual(diagnostics["applied"], [])
        self.assertEqual(diagnostics["rejected"][0]["reason"], "inventory_identity_mismatch")
        movie = next(row for row in works if row["tmdb_id"] == 1145612)
        self.assertEqual(movie["identity_scope"]["kind"], "video_file")

    def test_existing_broad_movie_scope_cannot_be_widened(self) -> None:
        report = self._report()
        works = self._works()
        works[0]["identity_scope"] = {
            "kind": "video_files",
            "video_paths": [FATE_PARENT_VIDEO_PATH, "/quark/影视/番剧/Fate/unreviewed.mkv"],
        }
        with tempfile.TemporaryDirectory() as temporary:
            self._write_state(temporary)
            updated, diagnostics = apply_content_identity_overrides(
                report,
                works,
                object(),
                temporary,
                probe=lambda _client, _record: (True, "matched"),
            )
        self.assertEqual(diagnostics["applied"], [])
        self.assertEqual(diagnostics["rejected"][0]["reason"], "parent_movie_identity_missing")
        self.assertEqual(updated[0]["identity_scope"], works[0]["identity_scope"])

    def test_real_probe_requires_every_recorded_media_field(self) -> None:
        record = _expected_record()
        evidence = record["evidence"]
        assert isinstance(evidence, dict)
        format_evidence = evidence["format"]
        assert isinstance(format_evidence, dict)
        streams = []
        for key in ("video", "audio"):
            row = copy.deepcopy(evidence[key])
            assert isinstance(row, dict)
            tags = {
                field: row.pop(field)
                for field in ("language", "title")
                if field in row
            }
            if tags:
                row["tags"] = tags
            streams.append(row)
        for raw in evidence["subtitles"]:
            row = copy.deepcopy(raw)
            tags = {
                field: row.pop(field)
                for field in ("language", "title")
                if field in row
            }
            row["tags"] = tags
            streams.append(row)
        payload = {
            "format": {
                "format_name": format_evidence["format_name"],
                "duration": format_evidence["duration_ms"] / 1000,
                "tags": {
                    field: format_evidence[field]
                    for field in ("title", "encoder", "creation_time")
                },
            },
            "streams": streams,
            "chapters": [],
        }

        class PrefixClient:
            def read_file_prefix(self, _path: str, *, max_bytes: int) -> bytes:
                return b"x" * max_bytes

        class Digest:
            def hexdigest(self) -> str:
                return str(evidence["prefix_sha256"])

        completed = SimpleNamespace(
            returncode=0,
            stdout=json.dumps(payload).encode("utf-8"),
        )
        with (
            patch("local.scrapeflow_api.content_identity_overrides.hashlib.sha256", return_value=Digest()),
            patch("local.scrapeflow_api.content_identity_overrides.shutil.which", return_value="ffprobe"),
            patch("local.scrapeflow_api.content_identity_overrides.subprocess.run", return_value=completed),
        ):
            self.assertEqual(probe_content_identity(PrefixClient(), record), (True, "matched"))

        payload["format"]["duration"] = 3354.076
        with (
            patch("local.scrapeflow_api.content_identity_overrides.hashlib.sha256", return_value=Digest()),
            patch("local.scrapeflow_api.content_identity_overrides.shutil.which", return_value="ffprobe"),
            patch("local.scrapeflow_api.content_identity_overrides.subprocess.run", return_value=SimpleNamespace(
                returncode=0,
                stdout=json.dumps(payload).encode("utf-8"),
            )),
        ):
            self.assertEqual(probe_content_identity(PrefixClient(), record), (False, "ffprobe_evidence_mismatch"))


if __name__ == "__main__":
    unittest.main()
