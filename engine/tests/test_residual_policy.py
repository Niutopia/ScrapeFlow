import unittest

from engine.scrapeflow.residual_policy import (
    BLOCK_UNKNOWN,
    DEFER_SUBTITLE_PER_VIDEO,
    DELETE_AFTER_REMOTE_ROLLBACK,
    classify_residual,
    recoverable_delete_reason,
)


class ResidualPolicyTests(unittest.TestCase):
    def test_documents_comics_and_detached_audio_require_remote_rollback_delete(self):
        for path in (
            "/release/小说.docx",
            "/release/manga.cbz",
            "/release/audio/Commentary.flac",
            "/release/book.pdf",
        ):
            with self.subTest(path=path):
                self.assertEqual(
                    classify_residual(path).action,
                    DELETE_AFTER_REMOTE_ROLLBACK,
                )

    def test_delete_reason_is_centralized_and_never_labels_a_blocker(self):
        audio = classify_residual("/release/commentary.flac")
        self.assertEqual(
            recoverable_delete_reason(audio), "外挂音轨/独立音频附件",
        )
        self.assertIsNone(
            recoverable_delete_reason(classify_residual("/release/main.mkv")),
        )

    def test_manga_images_need_explicit_directory_context(self):
        self.assertEqual(
            classify_residual("/release/漫画/page001.jpg").action,
            DELETE_AFTER_REMOTE_ROLLBACK,
        )
        self.assertEqual(
            classify_residual("/release/poster.jpg").action,
            BLOCK_UNKNOWN,
        )

    def test_every_subtitle_format_is_deferred_to_exact_video_closure(self):
        for suffix in ("ass", "srt", "sup", "mks", "idx", "sub"):
            with self.subTest(suffix=suffix):
                decision = classify_residual(f"/release/E01.zh-CN.{suffix}")
                self.assertEqual(decision.action, DEFER_SUBTITLE_PER_VIDEO)

    def test_unknown_video_and_archive_fail_closed(self):
        self.assertEqual(
            classify_residual("/release/maybe-main.mkv").action,
            BLOCK_UNKNOWN,
        )
        self.assertEqual(
            classify_residual("/release/unknown.7z").action,
            BLOCK_UNKNOWN,
        )

    def test_explicit_theme_video_is_delete_candidate(self):
        decision = classify_residual("/release/Show.NCOP1.mkv")
        self.assertEqual(decision.action, DELETE_AFTER_REMOTE_ROLLBACK)
        self.assertEqual(decision.kind, "theme_or_promo_video")

    def test_planner_verified_cleanup_reason_is_stable_evidence(self):
        decision = classify_residual(
            "/release/unusual-name.mkv",
            reason="经特典目录与同集正片交叉确认的片头/片尾视频",
        )
        self.assertEqual(decision.action, DELETE_AFTER_REMOTE_ROLLBACK)
        self.assertEqual(decision.kind, "planner_verified_non_feature")


if __name__ == "__main__":
    unittest.main()
