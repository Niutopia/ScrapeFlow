from __future__ import annotations

import unittest

from engine.scrapeflow.subtitle_content import (
    classify_bilingual_subtitle_content,
    classify_subtitle_content,
    extract_subtitle_body,
    merge_bilingual_subtitle,
    normalize_subtitle_language,
    parse_subtitle_document,
    validate_exported_srt_sidecar,
    validate_managed_subtitle_content,
)


def _utf8(text: str) -> bytes:
    return text.encode("utf-8")


class SubtitleContentTests(unittest.TestCase):
    def test_managed_selector_prefers_same_file_bilingual_then_sc_then_tc(self) -> None:
        bilingual = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n"
            "这是简体中文字幕。\nこれはテストです。\n\n"
            "2\n00:00:04,000 --> 00:00:06,000\n"
            "我们现在开始测试。\nいま始めます。\n"
        )
        simplified = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n这是简体中文字幕。\n"
        )
        traditional = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n這是繁體中文字幕。\n"
        )
        bilingual_verdict = validate_managed_subtitle_content(
            bilingual, "ja", declared_size=len(bilingual),
        )
        simplified_verdict = validate_managed_subtitle_content(
            simplified, "ja", declared_size=len(simplified),
        )
        traditional_verdict = validate_managed_subtitle_content(
            traditional, "ja", declared_size=len(traditional),
        )
        self.assertEqual(bilingual_verdict["status"], "satisfied")
        self.assertEqual(bilingual_verdict["preference"], 0)
        self.assertEqual(bilingual_verdict["selection"], "bilingual")
        self.assertEqual(simplified_verdict["preference"], 1)
        self.assertEqual(traditional_verdict["preference"], 2)
        # Two independent Chinese files are merely two fallback lanes; the
        # validator never synthesizes a bilingual result from them.
        self.assertNotEqual(simplified_verdict["selection"], "bilingual")
        self.assertNotEqual(traditional_verdict["selection"], "bilingual")

    def test_managed_selector_accepts_traditional_bilingual_same_file(self) -> None:
        bilingual = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n"
            "這是繁體中文字幕。\nこれはテストです。\n"
        )
        verdict = validate_managed_subtitle_content(
            bilingual, "ja", declared_size=len(bilingual),
        )
        self.assertEqual(verdict["status"], "satisfied")
        self.assertEqual(verdict["preference"], 0)
        self.assertEqual(verdict["chinese_language"], "traditional_chinese")

    def test_managed_selector_rejects_size_drift_and_non_srt(self) -> None:
        valid = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n这是简体中文字幕。\n"
        )
        drift = validate_managed_subtitle_content(
            valid, declared_size=len(valid) + 1,
        )
        ass = validate_managed_subtitle_content(
            _utf8(
                "[Events]\n"
                "Dialogue: 0,0:00:01.00,0:00:03.00,Default,,0,0,0,,这是测试\n"
            ),
        )
        self.assertEqual(drift["status"], "unknown")
        self.assertEqual(ass["status"], "unknown")

    def test_exported_sc_and_tc_srt_txt_are_independent_normalized_candidates(self) -> None:
        simplified = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n这是简体中文字幕。\n"
        )
        traditional = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n這是繁體中文字幕。\n"
        )
        sc = validate_exported_srt_sidecar(
            "Show.S01E01.sc.srt.txt", simplified, declared_size=len(simplified),
        )
        tc = validate_exported_srt_sidecar(
            "Show.S01E01.tc.srt.txt", traditional, declared_size=len(traditional),
        )
        self.assertIsNotNone(sc)
        self.assertIsNotNone(tc)
        assert sc is not None and tc is not None
        self.assertEqual(sc.normalized_name, "Show.S01E01.zh-CN.srt")
        self.assertEqual(tc.normalized_name, "Show.S01E01.zh-TW.srt")
        self.assertEqual(sc.marker, "sc")
        self.assertEqual(tc.marker, "tc")
        self.assertIsNone(validate_exported_srt_sidecar(
            "Show.S01E01.sc.srt.txt", b"not an SRT", declared_size=10,
        ))

    def test_exported_ass_txt_sidecar_is_promoted_with_format_proof(self) -> None:
        """A ``.sc/.tc.ass.txt`` export is promoted to a canonical ASS sidecar.

        The declared export format must match the parsed document, so SRT
        bytes behind an ``.ass.txt`` suffix never pass.
        """
        ass = _utf8(
            "[Events]\n"
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, "
            "MarginV, Effect, Text\n"
            "Dialogue: 0,0:00:01.00,0:00:03.00,Default,,0,0,0,,这是简体字幕\n"
        )
        sc = validate_exported_srt_sidecar(
            "Show[01].sc.ass.txt", ass, declared_size=len(ass),
        )
        self.assertIsNotNone(sc)
        assert sc is not None
        self.assertEqual(sc.normalized_name, "Show[01].zh-CN.ass")
        self.assertEqual(sc.language, "zh-CN")
        self.assertEqual(sc.format, "ass")
        tc = validate_exported_srt_sidecar(
            "Show[01].tc.ass.txt", ass, declared_size=len(ass),
        )
        self.assertIsNotNone(tc)
        assert tc is not None
        self.assertEqual(tc.normalized_name, "Show[01].zh-TW.ass")
        # SRT content behind an .ass.txt suffix is a format mismatch.
        srt = _utf8("1\n00:00:01,000 --> 00:00:03,000\n这是简体中文字幕。\n")
        self.assertIsNone(validate_exported_srt_sidecar(
            "Show[01].sc.ass.txt", srt, declared_size=len(srt),
        ))
        # ASS content behind an .srt.txt suffix is likewise rejected.
        self.assertIsNone(validate_exported_srt_sidecar(
            "Show[01].sc.srt.txt", ass, declared_size=len(ass),
        ))

    def test_srt_simplified_chinese_satisfies_zh(self) -> None:
        result = classify_subtitle_content(
            _utf8("1\n00:00:01,000 --> 00:00:02,000\n这是一个测试\n"),
            "zh",
        )
        self.assertEqual(result["status"], "satisfied")
        self.assertEqual(result["classification"], "simplified_chinese")
        self.assertEqual(result["format"], "srt")

    def test_ass_and_utf16_are_supported(self) -> None:
        text = (
            "[Events]\r\n"
            "Dialogue: 0,0:00:00.00,0:00:01.00,Default,,0,0,0,,"
            "{\\an8}这是测试\\N下一行\r\n"
        )
        result = classify_subtitle_content(text.encode("utf-16"), "zh")
        self.assertEqual(result["status"], "satisfied")
        self.assertEqual(result["format"], "ass")
        self.assertEqual(extract_subtitle_body(text)[0], "ass")

    def test_traditional_and_japanese_cannot_satisfy_simplified_lane(self) -> None:
        traditional = classify_subtitle_content(
            _utf8("1\n00:00:01,000 --> 00:00:02,000\n這是一個測試\n"),
            "zh",
        )
        japanese = classify_subtitle_content(
            _utf8("WEBVTT\n\n00:00.000 --> 00:01.000\nこれはテストです\n"),
            "zh",
        )
        self.assertEqual(traditional["status"], "missing")
        self.assertEqual(traditional["classification"], "traditional_chinese")
        self.assertEqual(japanese["status"], "missing")
        self.assertEqual(japanese["classification"], "japanese")

    def test_ambiguous_or_malformed_content_is_unknown(self) -> None:
        ambiguous = classify_subtitle_content(
            _utf8("1\n00:00:01,000 --> 00:00:02,000\n你好\n"),
            "zh",
        )
        malformed = classify_subtitle_content(_utf8("not a subtitle"), "zh")
        self.assertEqual(ambiguous["status"], "unknown")
        self.assertEqual(malformed["status"], "unknown")

    def test_common_encodings_are_decoded(self) -> None:
        simplified = "1\n00:00:01,000 --> 00:00:02,000\n这是测试\n"
        self.assertEqual(
            classify_subtitle_content(simplified.encode("gb18030"), "zh")["status"],
            "satisfied",
        )
        traditional = "1\n00:00:01,000 --> 00:00:02,000\n這是測試\n"
        self.assertEqual(
            classify_subtitle_content(traditional.encode("big5"), "zh")["classification"],
            "traditional_chinese",
        )

    def test_language_aliases_are_explicit(self) -> None:
        self.assertEqual(normalize_subtitle_language("zh"), "simplified_chinese")
        self.assertEqual(normalize_subtitle_language("zh-Hant"), "traditional_chinese")
        self.assertEqual(normalize_subtitle_language("ja"), "japanese")
        self.assertEqual(normalize_subtitle_language("en-US"), "english")
        self.assertEqual(normalize_subtitle_language("ko"), "korean")
        self.assertIsNone(normalize_subtitle_language("xx"))

    def test_strict_srt_merge_puts_chinese_then_original_in_one_file(self) -> None:
        chinese = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n"
            "这是一个简体中文测试。\n\n"
            "2\n00:00:04,000 --> 00:00:06,000\n"
            "我们现在开始。\n"
        )
        japanese = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n"
            "これはテストです。\n\n"
            "2\n00:00:04,000 --> 00:00:06,000\n"
            "いま始めます。\n"
        )
        result = merge_bilingual_subtitle(chinese, japanese, "ja")
        self.assertEqual(result["status"], "satisfied")
        self.assertEqual(result["format"], "srt")
        self.assertEqual(result["cue_count"], 2)
        merged = result["content"]
        self.assertIsInstance(merged, bytes)
        text = merged.decode("utf-8")
        self.assertIn("这是一个简体中文测试。\nこれはテストです。", text)
        self.assertIn("我们现在开始。\nいま始めます。", text)
        self.assertEqual(
            classify_bilingual_subtitle_content(merged, "ja")["status"],
            "satisfied",
        )

    def test_strict_vtt_merge_preserves_timing_settings_and_utf8_output(self) -> None:
        chinese = _utf8(
            "WEBVTT\n\n"
            "cue-1\n00:01.000 --> 00:03.000 line:80%\n"
            "这是简体中文字幕。\n"
        )
        english = _utf8(
            "WEBVTT\n\n"
            "cue-1\n00:01.000 --> 00:03.000 line:20%\n"
            "This is a subtitle, and it will show you the words.\n"
        )
        result = merge_bilingual_subtitle(chinese, english, "en")
        self.assertEqual(result["status"], "satisfied")
        self.assertEqual(result["format"], "vtt")
        text = result["content"].decode("utf-8")
        self.assertIn("00:00:01.000 --> 00:00:03.000 line:80%", text)
        self.assertIn("这是简体中文字幕。\nThis is a subtitle", text)
        self.assertEqual(text.count("-->") , 1)

    def test_merge_rejects_timing_or_count_mismatch_without_bytes(self) -> None:
        chinese = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n这是简体中文字幕。\n"
        )
        shifted = _utf8(
            "1\n00:00:01,001 --> 00:00:03,000\nこれはテストです。\n"
        )
        shifted_result = merge_bilingual_subtitle(chinese, shifted, "ja")
        self.assertEqual(shifted_result["status"], "unknown")
        self.assertEqual(shifted_result["reason"], "subtitle_timing_mismatch")
        self.assertNotIn("content", shifted_result)

        extra = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\nこれはテストです。\n\n"
            "2\n00:00:04,000 --> 00:00:05,000\nもう一つです。\n"
        )
        count_result = merge_bilingual_subtitle(chinese, extra, "ja")
        self.assertEqual(count_result["reason"], "subtitle_cue_count_mismatch")
        self.assertNotIn("content", count_result)

    def test_merge_rejects_multiline_source_or_reversed_bilingual_cues(self) -> None:
        chinese = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n这是简体中文字幕。\n第二行\n"
        )
        japanese = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\nこれはテストです。\n"
        )
        result = merge_bilingual_subtitle(chinese, japanese, "ja")
        self.assertEqual(result["reason"], "subtitle_multiline_cue_unsupported")
        self.assertNotIn("content", result)

        reversed_lines = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n"
            "これはテストです。\n这是简体中文字幕。\n"
        )
        verdict = classify_bilingual_subtitle_content(reversed_lines, "ja")
        self.assertEqual(verdict["status"], "unknown")
        self.assertEqual(verdict["reason"], "bilingual_cue_language_order_not_proven")

    def test_bilingual_validator_proves_each_cue_not_just_the_aggregate(self) -> None:
        mixed = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n"
            "这是简体中文字幕。\nこれはテストです。\n\n"
            "2\n00:00:04,000 --> 00:00:06,000\n"
            "This line is only English, and it has enough words.\n"
            "This line is only English, and it has enough words.\n"
        )

        verdict = classify_bilingual_subtitle_content(mixed, "ja")

        self.assertEqual(verdict["status"], "unknown")
        self.assertEqual(verdict["reason"], "bilingual_cue_language_order_not_proven")

    def test_merge_rejects_format_mismatch_and_unproven_languages(self) -> None:
        chinese = _utf8(
            "1\n00:00:01,000 --> 00:00:03,000\n这是简体中文字幕。\n"
        )
        japanese_vtt = _utf8(
            "WEBVTT\n\n00:01.000 --> 00:03.000\nこれはテストです。\n"
        )
        result = merge_bilingual_subtitle(chinese, japanese_vtt, "ja")
        self.assertEqual(result["reason"], "subtitle_format_mismatch")
        self.assertNotIn("content", result)

        wrong_language = merge_bilingual_subtitle(
            chinese,
            _utf8("1\n00:00:01,000 --> 00:00:03,000\n這是繁體中文字幕。\n"),
            "ja",
        )
        self.assertEqual(wrong_language["reason"], "original_language_not_proven")
        self.assertNotIn("content", wrong_language)

    def test_merge_rejects_vtt_style_or_region_blocks_without_rewriting_them(self) -> None:
        chinese = _utf8(
            "WEBVTT\n\nSTYLE\n::cue { color: yellow; }\n\n"
            "00:01.000 --> 00:03.000\n这是简体中文字幕。\n"
        )
        japanese = _utf8(
            "WEBVTT\n\n00:01.000 --> 00:03.000\nこれはテストです。\n"
        )
        result = merge_bilingual_subtitle(chinese, japanese, "ja")
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["reason"], "subtitle_decode_or_format_unknown")
        self.assertNotIn("content", result)

    def test_parser_rejects_archive_and_malformed_payloads(self) -> None:
        self.assertIsNone(parse_subtitle_document(b"PK\x03\x04not a subtitle"))
        self.assertIsNone(parse_subtitle_document(
            _utf8("1\n00:00:01,000 --> 00:00:03,000\n")
        ))

    def test_ass_merge_is_strict_and_uses_ass_line_break(self) -> None:
        chinese = _utf8(
            "[Script Info]\n[Events]\n"
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
            "Dialogue: 0,0:00:01.00,0:00:03.00,Default,,0,0,0,,这是简体中文字幕。\n"
        )
        japanese = _utf8(
            "[Script Info]\n[Events]\n"
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
            "Dialogue: 0,0:00:01.00,0:00:03.00,Default,,0,0,0,,これはテストです。\n"
        )
        result = merge_bilingual_subtitle(chinese, japanese, "ja")
        self.assertEqual(result["status"], "satisfied")
        self.assertIn("这是简体中文字幕。\\Nこれはテストです。", result["content"].decode())


if __name__ == "__main__":
    unittest.main()
