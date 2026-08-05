import unittest

from engine.scrapeflow.burned_in_subtitle_ocr import (
    OCR_POLICY_VERSION, apply_ocr_resolution,
    classify_burned_in_ocr_windows, plan_window_offsets,
)


def _tsv(text: str, *, confidence: int = 92, top: int = 800) -> str:
    return (
        "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n"
        f"5\t1\t1\t1\t1\t1\t400\t{top}\t800\t80\t{confidence}\t{text}\n"
    )


def _window(offset: int, texts: list[str]) -> dict:
    return {"offset_seconds": offset, "frames": [
        {"status": "ocr_success", "width": 1920, "height": 1080,
         "frame_sha256": f"{offset}-{index}", "tsv": _tsv(text)}
        for index, text in enumerate(texts)
    ]}


class BurnedInSubtitleOcrTests(unittest.TestCase):
    def test_confirmed_ocr_moves_current_gap_to_resolved(self):
        path = "/quark/影视/番剧/A/A S01E01.mkv"
        refined = {
            "confirmed_missing_chinese": [{"video_path": path, "title": "A"}],
            "pending_review_or_probe": [],
            "resolved_with_chinese": [],
        }
        result = apply_ocr_resolution(refined, {path: {
            "status": "burned_in_chinese_confirmed",
            "policy_version": OCR_POLICY_VERSION,
            "confidence": "high",
        }})
        self.assertEqual(result["confirmed_missing_chinese"], [])
        self.assertEqual(len(result["resolved_with_chinese"]), 1)
        self.assertEqual(
            result["resolved_with_chinese"][0]["resolution"],
            "burned_in_simplified_chinese_ocr_confirmed",
        )

    def test_pending_or_stale_ocr_never_resolves_a_gap(self):
        current = "/quark/影视/番剧/A/A S01E01.mkv"
        unknown = "/quark/影视/番剧/B/B S01E01.mkv"
        refined = {
            "confirmed_missing_chinese": [{"video_path": current}],
            "pending_review_or_probe": [],
            "resolved_with_chinese": [],
        }
        for evidence in (
            {current: {"status": "pending", "policy_version": OCR_POLICY_VERSION}},
            {current: {"status": "burned_in_chinese_confirmed", "policy_version": 3}},
            {unknown: {"status": "burned_in_chinese_confirmed", "policy_version": OCR_POLICY_VERSION}},
            {current: "malformed"},
        ):
            result = apply_ocr_resolution(refined, evidence)
            self.assertEqual(len(result["confirmed_missing_chinese"]), 1)
            self.assertEqual(result["resolved_with_chinese"], [])

    def test_window_plan_excludes_opening_tail_and_requires_six_windows(self):
        offsets, reason = plan_window_offsets(1800)
        self.assertIsNone(reason)
        self.assertEqual(len(offsets), 6)
        self.assertGreaterEqual(offsets[0], 240)
        self.assertLessEqual(offsets[-1], 1680)
        self.assertTrue(all(b - a >= 120 for a, b in zip(offsets, offsets[1:])))
        self.assertEqual(plan_window_offsets(300), ([], "video_too_short"))

    def test_two_windows_with_persistent_chinese_are_positive(self):
        result = classify_burned_in_ocr_windows([
            _window(300, ["我们现在开始", "我们现在开始", "下一句话来了"]),
            _window(900, ["这是今天的字幕", "这是今天的字幕", "继续前进吧"]),
        ])
        self.assertEqual(result["status"], "burned_in_chinese_confirmed")
        self.assertEqual(result["confidence"], "high")

    def test_two_windows_with_traditional_chinese_do_not_satisfy_zh_cn(self):
        result = classify_burned_in_ocr_windows([
            _window(300, ["我們現在開始", "我們現在開始", "下一句話來了"]),
            _window(900, ["這是今天的字幕", "這是今天的字幕", "繼續前進吧"]),
        ])
        self.assertNotEqual(result["status"], "burned_in_chinese_confirmed")

    def test_one_title_card_window_never_confirms_burned_in_subtitles(self):
        result = classify_burned_in_ocr_windows([
            _window(60, ["第三话新的开始", "第三话新的开始", "第三话新的开始"]),
            *[_window(offset, ["", "", ""]) for offset in (300, 600, 900, 1200, 1500)],
        ])
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["reason"], "single_window_chinese_candidate")

    def test_same_persistent_text_across_windows_is_static_overlay_pending(self):
        result = classify_burned_in_ocr_windows([
            _window(offset, ["这是官方版本", "这是官方版本", "这是官方版本"])
            for offset in (300, 900, 1500)
        ])
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["reason"], "static_overlay_only")

    def test_adequate_clean_windows_produce_only_bounded_negative(self):
        result = classify_burned_in_ocr_windows([
            _window(offset, ["ordinary scene"] * 6)
            for offset in (300, 600, 900, 1200, 1500, 1800)
        ])
        self.assertEqual(result["status"], "no_burned_in_chinese_evidence")
        self.assertEqual(result["confidence"], "bounded_negative")

    def test_all_empty_ocr_frames_fail_closed(self):
        result = classify_burned_in_ocr_windows([
            _window(offset, [""] * 6)
            for offset in (300, 600, 900, 1200, 1500, 1800)
        ])
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["reason"], "ocr_control_text_not_detected")

    def test_bottom_band_han_without_simplified_marker_fails_closed(self):
        windows = []
        for offset in (300, 600, 900, 1200, 1500, 1800):
            windows.append({"offset_seconds": offset, "frames": [{
                "status": "ocr_success", "width": 1920, "height": 1080,
                "ocr_lines": [{
                    "text": "我知道了", "confidence": 92,
                    "left": 500, "top": 800, "width": 600, "height": 80,
                }],
            }] * 6})
        result = classify_burned_in_ocr_windows(windows)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["reason"], "bottom_band_han_detected_but_zh_cn_not_confirmed")

    def test_low_confidence_or_upper_screen_text_is_ignored(self):
        low = _tsv("这是错误识别", confidence=20)
        upper = _tsv("节目标题文字", top=100)
        windows = []
        for offset in (300, 600, 900, 1200, 1500, 1800):
            windows.append({"offset_seconds": offset, "frames": [
                {"status": "ocr_success", "width": 1920, "height": 1080,
                 "tsv": low if index % 2 == 0 else upper}
                for index in range(6)
            ]})
        result = classify_burned_in_ocr_windows(windows)
        self.assertEqual(result["status"], "no_burned_in_chinese_evidence")

    def test_failed_frames_keep_result_pending(self):
        result = classify_burned_in_ocr_windows([{
            "offset_seconds": offset,
            "frames": [{"status": "timeout"}, {"status": "timeout"}],
        } for offset in (300, 600, 900)])
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["reason"], "insufficient_successful_ocr_windows")


if __name__ == "__main__":
    unittest.main()
