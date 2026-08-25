from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.video_admission import (
    FFPROBE_ANALYZE_DURATION_US,
    FFPROBE_PROBE_BYTES,
    FFPROBE_RW_TIMEOUT_US,
    VideoAdmissionError,
    probe_local_video_stream,
    probe_remote_video_stream,
)


class _Completed:
    returncode = 0
    stdout = b'{"streams":[{"codec_type":"audio"},{"codec_type":"video"}]}'


class VideoAdmissionTests(unittest.TestCase):
    def test_local_and_remote_use_the_same_bounded_probe_contract(self) -> None:
        commands: list[list[str]] = []

        def run(command, **kwargs):
            self.assertEqual(kwargs["timeout"], 60)
            self.assertFalse(kwargs["text"])
            commands.append(list(command))
            return _Completed()

        class Client:
            def file_link(self, path: str, *, refresh: bool = True):
                self.path = path
                self.refresh = refresh
                return "https://media.example.test/object?sig=redacted", {
                    "Referer": "https://alist.example.test/",
                }

        with tempfile.TemporaryDirectory() as temporary:
            local = Path(temporary) / "video.mkv"
            local.write_bytes(b"fixture")
            local_result = probe_local_video_stream(
                local, ffprobe_path="/usr/bin/ffprobe", runner=run,
            )
        remote_result = probe_remote_video_stream(
            Client(), "/staging/video.mkv",
            ffprobe_path="/usr/bin/ffprobe", runner=run,
        )

        self.assertEqual(local_result["status"], "satisfied")
        self.assertEqual(remote_result["status"], "satisfied")
        self.assertEqual(len(commands), 2)
        for command in commands:
            self.assertNotIn("-nostdin", command)
            self.assertIn(str(FFPROBE_RW_TIMEOUT_US), command)
            self.assertIn(str(FFPROBE_PROBE_BYTES), command)
            self.assertIn(str(FFPROBE_ANALYZE_DURATION_US), command)
            self.assertEqual(command.count("-show_entries"), 1)
        self.assertNotIn("-headers", commands[0])
        self.assertIn("-headers", commands[1])

    def test_extension_without_video_stream_fails_closed(self) -> None:
        class AudioOnly:
            returncode = 0
            stdout = b'{"streams":[{"codec_type":"audio"}]}'

        with tempfile.TemporaryDirectory() as temporary:
            local = Path(temporary) / "fake.mkv"
            local.write_bytes(b"not-video")
            with self.assertRaises(VideoAdmissionError) as raised:
                probe_local_video_stream(
                    local,
                    ffprobe_path="/usr/bin/ffprobe",
                    runner=lambda *_args, **_kwargs: AudioOnly(),
                )
        self.assertEqual(raised.exception.reason, "video_stream_missing")
        self.assertTrue(raised.exception.candidate_invalid)

    def test_remote_probe_rejects_unsafe_header_before_subprocess(self) -> None:
        calls = 0

        def run(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            return _Completed()

        class Client:
            def file_link(self, _path: str, *, refresh: bool = True):
                del refresh
                return "https://media.example.test/object", {
                    "Referer": "ok\r\nX-Injected: yes",
                }

        with self.assertRaises(VideoAdmissionError) as raised:
            probe_remote_video_stream(
                Client(), "/staging/video.mkv",
                ffprobe_path="/usr/bin/ffprobe", runner=run,
            )
        self.assertEqual(raised.exception.reason, "unsafe_provider_headers")
        self.assertEqual(calls, 0)

    def test_timeout_is_not_misclassified_as_bad_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            local = Path(temporary) / "slow.mkv"
            local.write_bytes(b"fixture")
            with self.assertRaises(VideoAdmissionError) as raised:
                probe_local_video_stream(
                    local,
                    ffprobe_path="/usr/bin/ffprobe",
                    runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                        subprocess.TimeoutExpired("ffprobe", 60)
                    ),
                )
        self.assertEqual(raised.exception.reason, "ffprobe_timeout")
        self.assertFalse(raised.exception.candidate_invalid)


if __name__ == "__main__":
    unittest.main()
