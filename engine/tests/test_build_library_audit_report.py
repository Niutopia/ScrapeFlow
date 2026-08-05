import unittest

from engine.tools.build_library_audit_report import build


class BuildDashboardAuditTests(unittest.TestCase):
    def test_preserves_exact_live_scan_time_for_report_freshness(self):
        snapshot_at = "2026-07-29T06:12:34.567890+00:00"
        audit = build({
            "audited_at": snapshot_at,
            "summary": {"series": 0, "movies": 0},
            "projects": [], "collection_roots": [], "movies": [],
        }, {}, {}, audited_at="2026-07-29")
        self.assertEqual(audit["audited_at"], "2026-07-29")
        self.assertEqual(audit["snapshot_at"], snapshot_at)

    def test_preserves_structured_subtitle_gaps_for_replenishment(self):
        gap = {
            "media_type": "tv",
            "target_root": "/library/Example",
            "season": 1,
            "episode": 2,
            "label": "S01E02",
            "reason_code": "required_subtitle_language_missing",
            "video_path": "/library/Example/Example - S01E02.mkv",
            "required_languages": ["zh-CN"],
        }
        audit = build({
            "summary": {"series": 0, "movies": 0},
            "projects": [], "collection_roots": [], "movies": [],
            "missing_subtitles": [gap],
            "subtitle_policy": {"scope": "external_sidecar"},
        }, {}, {}, audited_at="2026-07-29")
        self.assertEqual(audit["missing_subtitles"], [gap])
        self.assertEqual(audit["subtitle_policy"]["scope"], "external_sidecar")

    def test_movie_subtitle_gap_is_not_duplicated_as_metadata(self):
        gap = {
            "media_type": "movie",
            "target_root": "/quark/影视/番剧/Example/Film (2026)",
            "video_path": "/quark/影视/番剧/Example/Film (2026).mkv",
            "reason_code": "missing_external_subtitle",
        }
        audit = build({
            "summary": {"series": 0, "movies": 1},
            "projects": [],
            "collection_roots": [],
            "missing_subtitles": [gap],
            "movies": [{
                "title": "Film",
                "target_stem": "/quark/影视/番剧/Example/Film (2026)",
                "issues": [{
                    "code": "movie_video_subtitle_gap",
                    "severity": "high",
                    "message": "电影视频缺少符合语言要求的外挂字幕。",
                }],
            }],
        }, {}, {}, audited_at="2026-07-31")

        self.assertEqual(audit["missing_subtitles"], [gap])
        self.assertEqual(audit["metadata_issues"], 0)
        self.assertFalse(any(
            row["category"] == "metadata" for row in audit["shows"]
        ))

    def test_real_movie_metadata_issue_remains_visible(self):
        audit = build({
            "summary": {"series": 0, "movies": 1},
            "projects": [], "collection_roots": [],
            "movies": [{
                "title": "Film",
                "target_stem": "/quark/影视/番剧/Example/Film (2026)",
                "issues": [{
                    "code": "movie_nfo_video_pair_mismatch",
                    "severity": "critical",
                    "message": "电影 NFO 必须且只能对应一个同名视频。",
                }],
            }],
        }, {}, {}, audited_at="2026-07-31")

        self.assertEqual(audit["metadata_issues"], 1)
        self.assertEqual(
            audit["shows"][0]["missing"][0]["label"],
            "movie_nfo_video_pair_mismatch",
        )

    def test_separates_resource_gaps_from_semantic_remediation(self):
        live = {
            "summary": {"series": 2, "movies": 0},
            "projects": [
                {"title": "Wrong", "target_root": "/wrong", "tmdb_ids": [1], "regular_missing": [{"season": 1, "episode": 1, "label": "S01E01", "title": "One"}], "optional_missing": [], "issues": []},
                {"title": "Missing", "target_root": "/missing", "tmdb_ids": [2], "regular_missing": [{"season": 1, "episode": 2, "label": "S01E02", "title": "Two"}], "optional_missing": [], "issues": []},
            ],
            "collection_roots": [],
            "movies": [],
        }
        boundaries = {"semantic_remediations": [{
            "title": "Wrong", "target_root": "/wrong", "suppress_core_missing": True,
            "label": "误映射", "detail": "资源存在但编号错误",
        }]}
        audit = build(live, {}, boundaries, audited_at="2026-07-27")
        self.assertEqual(audit["total_missing"], 1)
        self.assertEqual(audit["metadata_issues"], 1)
        self.assertEqual({row["category"] for row in audit["shows"]}, {"core", "metadata"})

    def test_groups_uncovered_files_by_work_instead_of_flooding_review(self):
        live = {
            "summary": {"series": 0, "movies": 0},
            "projects": [],
            "collection_roots": [],
            "movies": [],
            "uncovered_media": [
                "/quark/影视/美剧/纸牌屋/S01/Show.S01E01.mkv",
                "/quark/影视/美剧/纸牌屋/S01/Show.S01E02.mkv",
                "/quark/影视/美剧/行尸走肉/S01/Dead.S01E01.mkv",
            ],
        }
        audit = build(live, {}, {}, audited_at="2026-07-27")
        self.assertEqual(audit["metadata_issues"], 2)
        self.assertEqual(
            {row["target_root"] for row in audit["shows"]},
            {"/quark/影视/美剧/纸牌屋", "/quark/影视/美剧/行尸走肉"},
        )

    def test_empty_library_root_is_a_visible_metadata_issue(self):
        live = {
            "summary": {"series": 0, "movies": 0},
            "projects": [], "collection_roots": [], "movies": [],
            "empty_library_roots": ["/quark/影视/番剧/东京食尸鬼"],
        }
        audit = build(live, {}, {}, audited_at="2026-07-27")
        self.assertEqual(audit["metadata_issues"], 1)
        self.assertEqual(audit["shows"][0]["missing"][0]["label"], "空壳目录")


if __name__ == "__main__":
    unittest.main()
