from __future__ import annotations

import unittest
from pathlib import Path
import struct
import tempfile

from engine.tools.probe_burned_in_subtitles import (
    _duration_from_ffmpeg_stderr, _png_dimensions, _rapidocr_lines,
)


class ProbeBurnedInSubtitlesTests(unittest.TestCase):
    def test_parses_ffmpeg_duration(self) -> None:
        stderr = "Input #0, matroska, from 'redacted':\n  Duration: 00:23:41.53, start: 0.000000"
        self.assertEqual(_duration_from_ffmpeg_stderr(stderr), 1421.53)

    def test_missing_duration_is_pending_evidence(self) -> None:
        self.assertIsNone(_duration_from_ffmpeg_stderr("Invalid data found"))

    def test_empty_ocr_result_is_successful_empty_frame(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "frame.png"
            path.write_bytes(
                b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR"
                + struct.pack(">II", 1920, 1080)
            )
            width, height, lines = _rapidocr_lines(lambda _: None, path)
        self.assertEqual((width, height), (1920, 1080))
        self.assertEqual(lines, [])

    def test_invalid_png_header_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "frame.png"
            path.write_bytes(b"not a png")
            with self.assertRaisesRegex(ValueError, "invalid_png_header"):
                _png_dimensions(path)


if __name__ == "__main__":
    unittest.main()
