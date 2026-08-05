import unittest
from unittest.mock import patch

from engine.tools.refine_subtitle_audit import (
    CONTENT_CLASSIFIER_VERSION,
    classify_subtitle_content, classify_subtitle_streams,
    decode_ffprobe_packet_hexdump, formal_library_category,
    extract_remote_text_subtitle_packets, extract_remote_text_subtitle_stream,
    invalidate_stale_content_cache,
    invalidate_stale_stream_content_cache, invalidate_stream_probe_cache,
    probe_remote_subtitle_streams, refine_rows,
    text_streams_to_extract,
)


class RefineSubtitleAuditTests(unittest.TestCase):
    def test_decodes_ffprobe_packet_hexdump_from_partial_json(self):
        payload = (
            b'{"packets":[{"data":"\\n00000000: 4865 6c6c 6f20 e4b8 96e7 958c  Hello ......\\n"}'
        )
        self.assertEqual(decode_ffprobe_packet_hexdump(payload), "Hello 世界".encode())

    def test_only_three_formal_library_roots_are_classified(self):
        self.assertEqual(formal_library_category("/quark/影视/番剧/A/a.mkv"), "番剧")
        self.assertIsNone(formal_library_category("/quark/影视/待刮削/A/a.mkv"))
        self.assertIsNone(formal_library_category("/quark/影视/ScrapeFlow/补源/A.mkv"))

    def test_content_detection_separates_chinese_japanese_and_unknown(self):
        chinese = "\n".join(
            f"Dialogue: 0,0:00:0{i}.00,0:00:01.00,Default,,0,0,0,,这是第{i}条中文字幕，我们现在开始。"
            for i in range(5)
        ).encode()
        japanese = "\n".join("これは日本語の字幕です。" for _ in range(5)).encode()
        utf16 = "\n".join(
            f"这是第{i}条中文字幕，我们现在开始。" for i in range(5)
        ).encode("utf-16")
        self.assertEqual(classify_subtitle_content(chinese)["status"], "chinese")
        self.assertEqual(classify_subtitle_content(japanese, ".srt")["status"], "japanese")
        self.assertEqual(classify_subtitle_content(utf16, ".srt")["status"], "chinese")
        self.assertEqual(classify_subtitle_content(b"tiny")["status"], "undetermined")
        self.assertEqual(classify_subtitle_content(b"binary", ".sup")["status"], "undetermined")

    def test_traditional_chinese_does_not_satisfy_zh_cn(self):
        traditional = "\n".join(
            f"Dialogue: 0,0:00:0{i}.00,0:00:01.00,Default,,0,0,0,,這是第{i}條繁體中文字幕，我們現在開始。"
            for i in range(5)
        ).encode()
        result = classify_subtitle_content(traditional)
        self.assertEqual(result["status"], "non_chinese")
        self.assertEqual(result["language_variant"], "traditional_chinese")
        self.assertGreater(result["traditional_markers"], result["simplified_markers"])

    def test_ass_header_without_dialogue_is_not_negative_language_evidence(self):
        header = (
            "[Script Info]\nScriptType: v4.00+\n"
            "[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour\n"
            "Style: Default,Arial,20,&H00FFFFFF\n"
        ).encode()
        result = classify_subtitle_content(header, ".ass")
        self.assertEqual(result["status"], "undetermined")
        self.assertEqual(result["timed_text_lines"], 0)

    def test_ass_header_does_not_hide_later_chinese_dialogue(self):
        header = "[Script Info]\nScriptType: v4.00+\n" + ("Format: English metadata fields\n" * 20)
        body = "\n".join(
            f"Dialogue: 0,0:00:0{i}.00,0:00:01.00,Default,,0,0,0,,这是第{i}条中文字幕，我们现在开始。"
            for i in range(5)
        )
        self.assertEqual(
            classify_subtitle_content((header + body).encode(), ".ass")["status"],
            "chinese",
        )

    def test_utf8_bom_with_one_damaged_trailer_byte_stays_chinese(self):
        dialogue = "\n".join(
            f"Dialogue: 0,0:00:0{i}.00,0:00:01.00,Default,,0,0,0,,这是第{i}条中文字幕，我们现在开始。"
            for i in range(5)
        )
        witness = classify_subtitle_content(
            b"\xef\xbb\xbf" + dialogue.encode("utf-8") + b"\x8f",
        )
        self.assertEqual(witness["status"], "chinese")
        self.assertEqual(witness["encoding"], "utf-8-sig")
        self.assertEqual(witness["decode_replacement_characters"], 1)

    def test_stale_utf16_negative_cache_is_reprobed_without_dropping_positive_evidence(self):
        cache = {"external_content": {
            "/v/stale.ass": {"status": "japanese", "encoding": "utf-16"},
            "/v/real-ja.ass": {"status": "japanese", "encoding": "utf-8-sig"},
            "/v/new-utf16-ja.ass": {
                "status": "japanese", "encoding": "utf-16",
                "classifier_version": CONTENT_CLASSIFIER_VERSION,
            },
            "/v/confirmed.ass": {
                "status": "chinese", "encoding": "utf-16",
                "classifier_version": CONTENT_CLASSIFIER_VERSION,
            },
            "/v/legacy.srt": {"status": "undetermined", "reason": "unknown_encoding"},
        }}

        self.assertEqual(invalidate_stale_content_cache(cache), 2)
        self.assertEqual(set(cache["external_content"]), {
            "/v/real-ja.ass", "/v/new-utf16-ja.ass", "/v/confirmed.ass",
        })

    def test_stream_retry_invalidation_is_explicit_and_preserves_positive_evidence(self):
        cache = {"stream_content": {
            "timeout": {
                "status": "undetermined",
                "reason": "packet_capture_timeout_without_language_evidence",
            },
            "unclear": {"status": "undetermined", "reason": "packet_content_undetermined"},
            "nonzero": {"status": "undetermined", "reason": "ffprobe_packet_nonzero_exit"},
            "chinese": {"status": "chinese"},
        }}
        self.assertEqual(invalidate_stream_probe_cache(
            cache, retry_timeouts=True, retry_undetermined=False,
        ), 1)
        self.assertEqual(set(cache["stream_content"]), {"unclear", "nonzero", "chinese"})
        self.assertEqual(invalidate_stream_probe_cache(
            cache, retry_timeouts=False, retry_undetermined=True,
        ), 2)
        self.assertEqual(set(cache["stream_content"]), {"chinese"})

    def test_stale_negative_stream_cache_is_reprobed(self):
        cache = {"stream_content": {
            "old": {"status": "non_chinese", "encoding": "utf-8-sig"},
            "current": {
                "status": "non_chinese",
                "classifier_version": CONTENT_CLASSIFIER_VERSION,
            },
            "positive": {
                "status": "chinese", "classifier_version": CONTENT_CLASSIFIER_VERSION,
            },
        }}
        self.assertEqual(invalidate_stale_stream_content_cache(cache), 1)
        self.assertEqual(set(cache["stream_content"]), {"current", "positive"})

    def test_stream_detection_keeps_unknown_language_out_of_confirmed_missing(self):
        self.assertEqual(classify_subtitle_streams([])["status"], "no_subtitle_stream")
        self.assertEqual(classify_subtitle_streams([{
            "index": 2, "codec_name": "ass", "tags": {"language": "zho"},
        }])["status"], "embedded_chinese")
        self.assertEqual(classify_subtitle_streams([{
            "index": 2, "codec_name": "ass", "tags": {"language": "jpn"},
        }])["status"], "embedded_non_chinese_only")
        self.assertEqual(classify_subtitle_streams([{
            "index": 2, "codec_name": "ass", "tags": {},
        }])["status"], "subtitle_stream_language_unknown")
        self.assertEqual(classify_subtitle_streams([{
            "index": 2, "codec_name": "ass", "tags": {"language": "und"},
        }])["status"], "subtitle_stream_language_unknown")
        self.assertEqual(classify_subtitle_streams([{
            "index": 2, "codec_name": "ass",
            "tags": {"language": "und", "title": "MAI&Kamigami.简日双语"},
        }])["status"], "embedded_chinese")
        self.assertEqual(classify_subtitle_streams([{
            "index": 2, "codec_name": "ass",
            "tags": {"language": "und", "title": "SC"},
        }])["status"], "embedded_chinese")
        self.assertEqual(classify_subtitle_streams([{
            "index": 2, "codec_name": "ass",
            "tags": {"language": "und", "title": "TC"},
        }])["status"], "embedded_non_chinese_only")

    def test_required_language_missing_row_resolves_on_chinese_companion_content(self):
        # A ``.ja.ass`` sidecar may carry Simplified Chinese text; content
        # evidence must satisfy the video even though the filename hint is
        # not zh-CN.
        row = {
            "reason_code": "required_subtitle_language_missing",
            "video_path": "/quark/影视/番剧/A/Season 01/A - S01E30 - 南之勇者.mkv",
            "companion_subtitles": ["/quark/影视/番剧/A/Season 01/A - S01E30 - 南之勇者.ja.ass"],
            "candidate_subtitles": ["/quark/影视/番剧/A/Season 01/A - S01E30 - 南之勇者.ja.ass"],
        }
        result = refine_rows(
            [row], content_results={
                "/quark/影视/番剧/A/Season 01/A - S01E30 - 南之勇者.ja.ass": {
                    "status": "chinese",
                },
            }, probe_results={},
        )
        self.assertEqual(len(result["resolved_with_chinese"]), 1)
        self.assertEqual(result["resolved_with_chinese"][0]["resolution"], "external_chinese_content_confirmed")
        self.assertEqual(len(result["confirmed_missing_chinese"]), 0)

    def test_required_language_missing_row_stays_unresolved_on_traditional_companion(self):
        # A content-verified Traditional Chinese companion is not zh-CN and
        # must not resolve the video; it remains a confirmed gap.
        row = {
            "reason_code": "required_subtitle_language_missing",
            "video_path": "/quark/影视/番剧/A/Season 02/A - S02E01.mkv",
            "companion_subtitles": ["/quark/影视/番剧/A/Season 02/A - S02E01.zh-TW.ass"],
            "candidate_subtitles": ["/quark/影视/番剧/A/Season 02/A - S02E01.zh-TW.ass"],
        }
        result = refine_rows(
            [row], content_results={
                "/quark/影视/番剧/A/Season 02/A - S02E01.zh-TW.ass": {
                    "status": "non_chinese",
                    "language_variant": "traditional_chinese",
                },
            }, probe_results={row["video_path"]: {"status": "no_subtitle_stream"}},
        )
        self.assertEqual(len(result["resolved_with_chinese"]), 0)
        self.assertEqual(len(result["confirmed_missing_chinese"]), 1)

    def test_required_language_missing_row_pends_on_undetermined_companion_content(self):
        # A companion whose content stayed undetermined — too short for
        # language evidence, a transient remote read failure, or a bitmap
        # sidecar that text classification cannot read — must never confirm
        # the gap: the filename hint is not language evidence.
        row = {
            "reason_code": "required_subtitle_language_missing",
            "video_path": "/quark/影视/番剧/B/B - S01E01.mkv",
            "companion_subtitles": ["/quark/影视/番剧/B/B - S01E01.srt"],
            "candidate_subtitles": ["/quark/影视/番剧/B/B - S01E01.srt"],
        }
        for reason in (
            "insufficient_language_evidence", "HTTPError", "binary_or_bitmap_subtitle",
        ):
            with self.subTest(reason=reason):
                result = refine_rows(
                    [row], content_results={
                        "/quark/影视/番剧/B/B - S01E01.srt": {
                            "status": "undetermined", "reason": reason,
                        },
                    },
                    probe_results={row["video_path"]: {"status": "no_subtitle_stream"}},
                )
                self.assertEqual(len(result["resolved_with_chinese"]), 0)
                self.assertEqual(len(result["confirmed_missing_chinese"]), 0)
                self.assertEqual(len(result["pending_review_or_probe"]), 1)
                pending = result["pending_review_or_probe"][0]
                self.assertEqual(
                    pending["pending_reason"],
                    "external_subtitle_language_undetermined",
                )
                self.assertEqual(
                    pending["external_content_evidence"],
                    [{"status": "undetermined", "reason": reason}],
                )

    def test_mixed_chinese_and_undetermined_companions_resolve_on_chinese(self):
        # Any single Chinese-verified companion satisfies the video; the
        # undetermined sibling must not drag the row back to pending.
        row = {
            "reason_code": "required_subtitle_language_missing",
            "video_path": "/quark/影视/番剧/C/C - S01E01.mkv",
            "companion_subtitles": [
                "/quark/影视/番剧/C/C - S01E01.ja.ass",
                "/quark/影视/番剧/C/C - S01E01.zh-TW.ass",
            ],
            "candidate_subtitles": [
                "/quark/影视/番剧/C/C - S01E01.ja.ass",
                "/quark/影视/番剧/C/C - S01E01.zh-TW.ass",
            ],
        }
        result = refine_rows(
            [row], content_results={
                "/quark/影视/番剧/C/C - S01E01.ja.ass": {"status": "chinese"},
                "/quark/影视/番剧/C/C - S01E01.zh-TW.ass": {
                    "status": "undetermined", "reason": "HTTPError",
                },
            }, probe_results={},
        )
        self.assertEqual(len(result["resolved_with_chinese"]), 1)
        self.assertEqual(
            result["resolved_with_chinese"][0]["resolution"],
            "external_chinese_content_confirmed",
        )
        self.assertEqual(len(result["confirmed_missing_chinese"]), 0)
        self.assertEqual(len(result["pending_review_or_probe"]), 0)

    def test_all_non_chinese_companions_still_confirm_the_gap(self):
        # Only an explicit non-Chinese verdict on every companion keeps the
        # confirmation path open.
        row = {
            "reason_code": "required_subtitle_language_missing",
            "video_path": "/quark/影视/番剧/D/D - S01E01.mkv",
            "companion_subtitles": [
                "/quark/影视/番剧/D/D - S01E01.ja.ass",
                "/quark/影视/番剧/D/D - S01E01.en.srt",
            ],
            "candidate_subtitles": [
                "/quark/影视/番剧/D/D - S01E01.ja.ass",
                "/quark/影视/番剧/D/D - S01E01.en.srt",
            ],
        }
        result = refine_rows(
            [row], content_results={
                "/quark/影视/番剧/D/D - S01E01.ja.ass": {
                    "status": "non_chinese", "language_variant": "japanese",
                },
                "/quark/影视/番剧/D/D - S01E01.en.srt": {"status": "non_chinese"},
            },
            probe_results={row["video_path"]: {"status": "no_subtitle_stream"}},
        )
        self.assertEqual(len(result["confirmed_missing_chinese"]), 1)
        self.assertEqual(len(result["pending_review_or_probe"]), 0)

    def test_empty_companions_preserve_original_confirm_and_pending_paths(self):
        # No companions: rows keep their pre-existing behavior — the
        # ``required_subtitle_language_missing`` reason confirms from probe
        # evidence alone, while ``subtitle_language_unverified`` stays
        # conservative and pends.
        missing = {
            "reason_code": "required_subtitle_language_missing",
            "video_path": "/quark/影视/番剧/E/E - S01E01.mkv",
            "companion_subtitles": [],
        }
        confirmed = refine_rows(
            [missing], content_results={},
            probe_results={missing["video_path"]: {"status": "no_subtitle_stream"}},
        )
        self.assertEqual(len(confirmed["confirmed_missing_chinese"]), 1)
        self.assertEqual(len(confirmed["pending_review_or_probe"]), 0)

        unverified = {
            "reason_code": "subtitle_language_unverified",
            "video_path": "/quark/影视/番剧/E/E - S01E01.mkv",
            "companion_subtitles": [],
        }
        pending = refine_rows(
            [unverified], content_results={},
            probe_results={unverified["video_path"]: {"status": "no_subtitle_stream"}},
        )
        self.assertEqual(len(pending["confirmed_missing_chinese"]), 0)
        self.assertEqual(len(pending["pending_review_or_probe"]), 1)
        self.assertEqual(
            pending["pending_review_or_probe"][0]["pending_reason"],
            "external_subtitle_language_undetermined",
        )

    def test_stem_mismatch_row_ignores_chinese_candidate_content(self):
        # A mismatched-stem candidate is evidence of a pairing problem, not a
        # satisfied video, even when its content is Chinese.
        row = {
            "reason_code": "subtitle_video_stem_mismatch",
            "video_path": "/quark/影视/番剧/A/Season 01/A - S01E01.mkv",
            "companion_subtitles": [],
            "candidate_subtitles": ["/quark/影视/番剧/A/Season 01/A - S01E02.zh-CN.ass"],
        }
        result = refine_rows(
            [row], content_results={
                "/quark/影视/番剧/A/Season 01/A - S01E02.zh-CN.ass": {"status": "chinese"},
            }, probe_results={row["video_path"]: {"status": "no_subtitle_stream"}},
        )
        self.assertEqual(len(result["resolved_with_chinese"]), 0)
        self.assertEqual(len(result["confirmed_missing_chinese"]), 1)

    def test_refinement_emits_only_confirmed_and_pending_categories(self):
        rows = [
            {"reason_code": "subtitle_language_unverified", "video_path": "/v/has.mkv", "companion_subtitles": ["/v/has.ass"]},
            {"reason_code": "missing_external_subtitle", "video_path": "/v/none.mkv", "companion_subtitles": []},
            {"reason_code": "missing_external_subtitle", "video_path": "/v/fail.mkv", "companion_subtitles": []},
        ]
        result = refine_rows(
            rows,
            content_results={"/v/has.ass": {"status": "chinese"}},
            probe_results={
                "/v/none.mkv": {"status": "no_subtitle_stream"},
                "/v/fail.mkv": {"status": "probe_failed"},
            },
        )
        self.assertEqual(len(result["resolved_with_chinese"]), 1)
        self.assertEqual(len(result["confirmed_missing_chinese"]), 1)
        self.assertEqual(len(result["pending_review_or_probe"]), 1)

    def test_unknown_text_stream_requires_extracted_content_evidence(self):
        row = {
            "reason_code": "missing_external_subtitle",
            "video_path": "/v/text.mkv",
            "companion_subtitles": [],
        }
        probe = {
            "status": "subtitle_stream_language_unknown",
            "streams": [{
                "index": 3, "codec_name": "ass", "classification": "unknown",
            }],
        }
        resolved = refine_rows(
            [row], content_results={}, probe_results={"/v/text.mkv": probe},
            stream_results={"/v/text.mkv#stream=3": {"status": "chinese"}},
        )
        self.assertEqual(len(resolved["resolved_with_chinese"]), 1)
        pending = refine_rows(
            [row], content_results={}, probe_results={"/v/text.mkv": probe},
            stream_results={"/v/text.mkv#stream=3": {"status": "undetermined"}},
        )
        self.assertEqual(len(pending["pending_review_or_probe"]), 1)

    def test_fast_seek_stream_extraction_classifies_bounded_ass_output(self):
        class FakeAList:
            @staticmethod
            def file_link(_path, refresh=True):
                self.assertTrue(refresh)
                return "https://cdn.example/video.mkv?token=secret", {}

        sample = ("[Events]\n" + "".join(
            f"Dialogue: 0,0:05:0{index}.00,0:05:0{index + 1}.00,Default,,0,0,0,,"
            "这个字幕已经确认是简体中文的内容\n"
            for index in range(5)
        )).encode()
        completed = type("Completed", (), {
            "returncode": 0, "stdout": sample, "stderr": b"",
        })()
        with patch("engine.tools.refine_subtitle_audit.shutil.which", return_value="/usr/bin/ffmpeg"), \
             patch("engine.tools.refine_subtitle_audit.subprocess.run", return_value=completed) as run:
            result = extract_remote_text_subtitle_stream(
                FakeAList(), "/v/episode.mkv", 2, timeout=30,
            )
        command = run.call_args.args[0]
        self.assertEqual(result["status"], "chinese")
        self.assertLess(command.index("-ss"), command.index("-i"))
        self.assertEqual(command[command.index("-ss") + 1], "0")
        self.assertEqual(command[command.index("-map") + 1], "0:2")
        self.assertIn("pipe:1", command)

    def test_bitmap_stream_stays_pending_for_ocr(self):
        row = {
            "reason_code": "missing_external_subtitle",
            "video_path": "/v/pgs.mkv",
            "companion_subtitles": [],
        }
        result = refine_rows(
            [row], content_results={}, probe_results={"/v/pgs.mkv": {
                "status": "subtitle_stream_language_unknown",
                "streams": [{
                    "index": 4, "codec_name": "hdmv_pgs_subtitle",
                    "classification": "unknown",
                }],
            }}, stream_results={},
        )
        self.assertEqual(
            result["pending_review_or_probe"][0]["pending_reason"],
            "bitmap_subtitle_ocr_required",
        )

    def test_all_unknown_text_streams_are_scheduled_for_content_evidence(self):
        probes = {"/v/multi.mkv": {
            "status": "subtitle_stream_language_unknown",
            "streams": [
                {"index": 3, "codec_name": "ass", "classification": "unknown"},
                {"index": 4, "codec_name": "subrip", "classification": "unknown"},
                {"index": 5, "codec_name": "ass", "classification": "non_chinese"},
                {"index": 6, "codec_name": "hdmv_pgs_subtitle", "classification": "unknown"},
            ],
        }}
        self.assertEqual(text_streams_to_extract(probes), {
            "/v/multi.mkv#stream=3": ("/v/multi.mkv", 3),
            "/v/multi.mkv#stream=4": ("/v/multi.mkv", 4),
        })

    def test_failed_probe_keeps_redacted_bounded_stderr(self):
        class FakeAList:
            @staticmethod
            def file_link(_path, refresh=True):
                self.assertTrue(refresh)
                return (
                    "https://cdn.example/video.mkv?token=secret",
                    {"Authorization": "Bearer secret"},
                )

        completed = type("Completed", (), {
            "returncode": 1,
            "stdout": "",
            "stderr": (
                "HTTP 403 for https://cdn.example/video.mkv?token=secret "
                "Authorization=Bearer secret"
            ),
        })()
        with patch("engine.tools.refine_subtitle_audit.shutil.which", return_value="/usr/bin/ffprobe"), \
             patch("engine.tools.refine_subtitle_audit.subprocess.run", return_value=completed):
            result = probe_remote_subtitle_streams(FakeAList(), "/v/fail.mkv")
        self.assertEqual(result["status"], "probe_failed")
        self.assertIn("HTTP 403", result["stderr"])
        self.assertNotIn("secret", result["stderr"])
        self.assertLessEqual(len(result["stderr"]), 500)

    def test_packet_probe_uses_second_window_only_when_first_is_undetermined(self):
        class FakeAList:
            @staticmethod
            def file_link(_path, refresh=True):
                return "https://cdn.example/video.mkv", {}

        completed = type("Completed", (), {
            "returncode": 0, "stdout": b"packet-json", "stderr": b"",
        })()
        chinese = "\n".join(
            f"{i},0,Default,,0,0,0,,这是第{i}条中文字幕，我们现在开始。"
            for i in range(5)
        ).encode()
        with patch("engine.tools.refine_subtitle_audit.shutil.which", return_value="/usr/bin/ffprobe"), \
             patch("engine.tools.refine_subtitle_audit.subprocess.run", return_value=completed) as run, \
             patch("engine.tools.refine_subtitle_audit.decode_ffprobe_packet_hexdump", side_effect=[b"tiny", chinese]):
            result = extract_remote_text_subtitle_packets(
                FakeAList(), "/v/multi-window.mkv", 3,
            )
        self.assertEqual(result["status"], "chinese")
        self.assertEqual(len(result["capture_attempts"]), 2)
        self.assertIn("%+180", run.call_args_list[0].args[0])
        self.assertIn("300%+180", run.call_args_list[1].args[0])


if __name__ == "__main__":
    unittest.main()
