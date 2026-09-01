import unittest

from engine.scrapeflow.core import (
    MEDIA_EXTS,
    SUBTITLE_EXTS,
    VIDEO_EXTS,
    make_unique_media_names,
)
from engine.scrapeflow.media_policy import (
    ARCHIVE_EXTENSIONS,
    AUDIO_EXTENSIONS,
    DISC_IMAGE_EXTENSIONS,
    DOCUMENT_EXTENSIONS,
    FONT_EXTENSIONS,
    IMAGE_EXTENSIONS,
    MANIFEST_EXTENSIONS,
    POSTER_EXTENSIONS,
    SUBTITLE_EXTENSIONS,
    TEMPORARY_EXTENSIONS,
    VIDEO_EXTENSIONS,
    classify_filename,
    is_archive_filename,
    is_container_candidate_filename,
    is_disc_image_filename,
    is_executable_filename,
    is_temporary_filename,
)
from engine.scrapeflow.media_quality import VIDEO_FILE_EXTENSIONS, media_kind
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
    KEEP_UNPLANNED,
    classify_residual,
)
from engine.tools import _replenishment_local_adapter_impl as torrent_adapter
from local.scrapeflow_api import provider_materializers, replenishment


class MediaPolicyTests(unittest.TestCase):
    def test_consumers_share_the_same_collections(self):
        self.assertEqual(VIDEO_EXTS, VIDEO_EXTENSIONS)
        self.assertEqual(VIDEO_FILE_EXTENSIONS, VIDEO_EXTENSIONS)
        self.assertEqual(RESIDUAL_VIDEO_EXTENSIONS, VIDEO_EXTENSIONS)
        self.assertEqual(torrent_adapter.VIDEO_EXTENSIONS, VIDEO_EXTENSIONS)
        self.assertEqual(replenishment._VIDEO_SUFFIXES, VIDEO_EXTENSIONS)
        self.assertEqual(provider_materializers._VIDEO_EXTENSIONS, VIDEO_EXTENSIONS)

        self.assertEqual(SUBTITLE_EXTS, SUBTITLE_EXTENSIONS)
        self.assertEqual(RESIDUAL_SUBTITLE_EXTENSIONS, SUBTITLE_EXTENSIONS)
        self.assertEqual(torrent_adapter.SUBTITLE_EXTENSIONS, SUBTITLE_EXTENSIONS)
        self.assertEqual(provider_materializers._SUBTITLE_EXTENSIONS, SUBTITLE_EXTENSIONS)
        self.assertEqual(MEDIA_EXTS, VIDEO_EXTENSIONS | SUBTITLE_EXTENSIONS)

    def test_required_video_edges_are_consistent(self):
        for suffix in (".mts", ".strm", ".flv", ".rmvb"):
            with self.subTest(suffix=suffix):
                self.assertIn(suffix, VIDEO_EXTENSIONS)
                self.assertEqual(classify_filename(f"release{suffix}"), "video")
                self.assertTrue(replenishment._candidate_has_video_file({
                    "files": [f"episode{suffix}"],
                }))

    def test_optical_disc_images_are_opaque_not_direct_video_or_cleanup(self):
        for suffix in (".iso", ".img", ".bin", ".mdf"):
            with self.subTest(suffix=suffix):
                name = f"release{suffix}"
                self.assertIn(suffix, DISC_IMAGE_EXTENSIONS)
                self.assertNotIn(suffix, VIDEO_EXTENSIONS)
                self.assertTrue(is_disc_image_filename(name))
                self.assertEqual(classify_filename(name), "disc_image")
                self.assertEqual(media_kind(name, video_exts=VIDEO_EXTENSIONS), "disc_image")
                self.assertFalse(replenishment._candidate_has_video_file({
                    "files": [f"episode{suffix}"],
                }))
                residual = classify_residual(f"/incoming/{name}")
                self.assertEqual(residual.action, KEEP_UNPLANNED)
                self.assertFalse(residual.can_cleanup)
                self.assertEqual(
                    residual.kind,
                    "disc_image_requires_content_expansion",
                )

    def test_cue_sheet_is_an_audio_sidecar_not_an_opaque_disc_image(self):
        # A CD-audio CUE is a plain-text track index beside FLAC tracks; it
        # can never be mounted or hide media, so a soundtrack OST folder must
        # not park the whole release at the content-expansion gate.
        name = "CLAYMORE TV Animation O. S. T..cue"
        self.assertNotIn(".cue", DISC_IMAGE_EXTENSIONS)
        self.assertFalse(is_disc_image_filename(name))
        self.assertFalse(is_container_candidate_filename(name))
        self.assertEqual(classify_filename(name), "audio")
        self.assertNotIn(".cue", VIDEO_EXTENSIONS)
        self.assertFalse(replenishment._candidate_has_video_file({
            "files": [name],
        }))
        residual = classify_residual(f"/incoming/OST/{name}")
        self.assertEqual(residual.action, KEEP_UNPLANNED)
        self.assertFalse(residual.can_cleanup)
        self.assertEqual(residual.kind, "detached_audio")
        # The raw payload of a cue (a lone .bin) remains an opaque image.
        self.assertTrue(is_disc_image_filename("soundtrack.bin"))

    def test_container_candidate_policy_requires_magic_proof_for_iso_and_exe(self):
        for name in ("feature.iso", "feature.img", "release.7z", "wrapper.exe"):
            with self.subTest(name=name):
                self.assertTrue(is_container_candidate_filename(name))
        self.assertTrue(is_executable_filename("wrapper.exe"))
        self.assertFalse(is_container_candidate_filename("episode.mkv"))

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
        self.assertEqual(POSTER_EXTENSIONS, {".avif", ".gif", ".jpeg", ".jpg", ".png", ".webp"})
        self.assertIn(".part", TEMPORARY_EXTENSIONS)


if __name__ == "__main__":
    unittest.main()


class LanguagelessSubtitleSidecarNameTests(unittest.TestCase):
    """A language-less external subtitle takes the plain sidecar name."""

    BASE = "银魂 - S06E10 - 激光这个词能让所有人为之心动"

    def test_single_languageless_sidecar_has_no_marker(self) -> None:
        # ``.subtitle`` was never a language tag: players showed "subtitle" as
        # the track name, and the reviewed layout wants the plain sidecar.
        self.assertEqual(
            make_unique_media_names(self.BASE, [
                {"name": "ep.mkv", "size": 1},
                {"name": "[Ygm] Gintama'  [61][Ma10p_2160p][x265_flac_ass].ass", "size": 2},
            ]),
            [f"{self.BASE}.mkv", f"{self.BASE}.ass"],
        )

    def test_second_languageless_sidecar_still_disambiguates(self) -> None:
        self.assertEqual(
            make_unique_media_names(self.BASE, [
                {"name": "ep.mkv", "size": 1},
                {"name": "a.ass", "size": 2},
                {"name": "b.ass", "size": 3},
            ]),
            [f"{self.BASE}.mkv", f"{self.BASE}.ass", f"{self.BASE}.subtitle2.ass"],
        )

    def test_detected_language_still_wins(self) -> None:
        self.assertEqual(
            make_unique_media_names(self.BASE, [
                {"name": "ep.mkv", "size": 1},
                {"name": "release.chs.ass", "size": 2},
            ]),
            [f"{self.BASE}.mkv", f"{self.BASE}.zh-CN.ass"],
        )


