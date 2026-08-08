import unittest

from engine.scrapeflow.core import MEDIA_EXTS, SUBTITLE_EXTS, VIDEO_EXTS
from engine.scrapeflow.media_policy import (
    ARCHIVE_EXTENSIONS,
    AUDIO_EXTENSIONS,
    DOCUMENT_EXTENSIONS,
    FONT_EXTENSIONS,
    IMAGE_EXTENSIONS,
    MANIFEST_EXTENSIONS,
    SUBTITLE_EXTENSIONS,
    TEMPORARY_EXTENSIONS,
    VIDEO_EXTENSIONS,
    classify_filename,
    is_archive_filename,
    is_temporary_filename,
)
from engine.scrapeflow.media_quality import VIDEO_FILE_EXTENSIONS
from engine.scrapeflow.residual_policy import (
    ARCHIVE_EXTENSIONS as RESIDUAL_ARCHIVE_EXTENSIONS,
    AUDIO_EXTENSIONS as RESIDUAL_AUDIO_EXTENSIONS,
    DOCUMENT_EXTENSIONS as RESIDUAL_DOCUMENT_EXTENSIONS,
    FONT_EXTENSIONS as RESIDUAL_FONT_EXTENSIONS,
    IMAGE_EXTENSIONS as RESIDUAL_IMAGE_EXTENSIONS,
    MANIFEST_EXTENSIONS as RESIDUAL_MANIFEST_EXTENSIONS,
    REBUILDABLE_DOWNLOAD_TEMP_SUFFIXES,
    SUBTITLE_EXTENSIONS as RESIDUAL_SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS as RESIDUAL_VIDEO_EXTENSIONS,
)
from engine.tools import _replenishment_local_adapter_impl as torrent_adapter
from local.scrapeflow_api import automatic_replenishment, replenishment
from local.scrapeflow_api.simple_library_audit import (
    POSTER_SUFFIXES,
    SUBTITLE_SUFFIXES,
    TEMPORARY_SUFFIXES,
    VIDEO_SUFFIXES,
)


class MediaPolicyTests(unittest.TestCase):
    def test_consumers_share_the_same_collections(self):
        self.assertEqual(VIDEO_EXTS, VIDEO_EXTENSIONS)
        self.assertEqual(VIDEO_FILE_EXTENSIONS, VIDEO_EXTENSIONS)
        self.assertEqual(RESIDUAL_VIDEO_EXTENSIONS, VIDEO_EXTENSIONS)
        self.assertEqual(torrent_adapter.VIDEO_EXTENSIONS, VIDEO_EXTENSIONS)
        self.assertEqual(replenishment._VIDEO_SUFFIXES, VIDEO_EXTENSIONS)
        self.assertEqual(automatic_replenishment._VIDEO_EXTENSIONS, VIDEO_EXTENSIONS)
        self.assertEqual(VIDEO_SUFFIXES, VIDEO_EXTENSIONS)

        self.assertEqual(SUBTITLE_EXTS, SUBTITLE_EXTENSIONS)
        self.assertEqual(RESIDUAL_SUBTITLE_EXTENSIONS, SUBTITLE_EXTENSIONS)
        self.assertEqual(torrent_adapter.SUBTITLE_EXTENSIONS, SUBTITLE_EXTENSIONS)
        self.assertEqual(automatic_replenishment._SUBTITLE_EXTENSIONS, SUBTITLE_EXTENSIONS)
        self.assertEqual(SUBTITLE_SUFFIXES, SUBTITLE_EXTENSIONS)
        self.assertEqual(MEDIA_EXTS, VIDEO_EXTENSIONS | SUBTITLE_EXTENSIONS)

    def test_required_video_edges_are_consistent(self):
        for suffix in (".iso", ".mts", ".strm", ".flv", ".rmvb"):
            with self.subTest(suffix=suffix):
                self.assertIn(suffix, VIDEO_EXTENSIONS)
                self.assertEqual(classify_filename(f"release{suffix}"), "video")
                self.assertTrue(replenishment._candidate_has_video_file({
                    "files": [f"episode{suffix}"],
                }))

    def test_auxiliary_categories_are_explicit_and_retained(self):
        expected = {
            ".mks": "subtitle",
            ".flac": "audio",
            ".pdf": "document",
            ".ttf": "font",
            ".jpg": "image",
            ".sfv": "manifest",
            ".7z": "archive",
            ".part": "temporary",
        }
        for suffix, category in expected.items():
            with self.subTest(suffix=suffix):
                self.assertEqual(classify_filename(f"item{suffix}"), category)

    def test_split_archive_and_temp_names(self):
        for name in (
            "show.part01.rar",
            "show.rar",
            "show.r00",
            "show.zip.001",
            "show.7z.002",
        ):
            with self.subTest(name=name):
                self.assertTrue(is_archive_filename(name))
        for name in ("video.mkv.part", "download.crdownload", ".scraper-tmp-abc"):
            with self.subTest(name=name):
                self.assertTrue(is_temporary_filename(name))

    def test_residual_aliases_cover_all_policy_classes(self):
        self.assertEqual(RESIDUAL_ARCHIVE_EXTENSIONS, ARCHIVE_EXTENSIONS)
        self.assertEqual(RESIDUAL_AUDIO_EXTENSIONS, AUDIO_EXTENSIONS)
        self.assertEqual(RESIDUAL_DOCUMENT_EXTENSIONS, DOCUMENT_EXTENSIONS)
        self.assertEqual(RESIDUAL_FONT_EXTENSIONS, FONT_EXTENSIONS)
        self.assertEqual(RESIDUAL_IMAGE_EXTENSIONS, IMAGE_EXTENSIONS)
        self.assertEqual(RESIDUAL_MANIFEST_EXTENSIONS, MANIFEST_EXTENSIONS)
        self.assertEqual(REBUILDABLE_DOWNLOAD_TEMP_SUFFIXES, TEMPORARY_EXTENSIONS)
        self.assertEqual(POSTER_SUFFIXES, {".avif", ".gif", ".jpeg", ".jpg", ".png", ".webp"})
        self.assertEqual(TEMPORARY_SUFFIXES, TEMPORARY_EXTENSIONS)


if __name__ == "__main__":
    unittest.main()
