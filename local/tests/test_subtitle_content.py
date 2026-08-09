from __future__ import annotations

import unittest

from engine.scrapeflow.subtitle_content import (
    classify_subtitle_content,
    extract_subtitle_body,
    normalize_subtitle_language,
)


def _utf8(text: str) -> bytes:
    return text.encode("utf-8")


class SubtitleContentTests(unittest.TestCase):
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
        self.assertIsNone(normalize_subtitle_language("xx"))


if __name__ == "__main__":
    unittest.main()
