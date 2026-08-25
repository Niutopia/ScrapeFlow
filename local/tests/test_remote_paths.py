"""Focused AList provider-basename policy regression tests."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from engine.scrapeflow.core import (
    AListClient,
    _validate_remote_basename,
    _validate_remote_source_basename,
)
from engine.scrapeflow.remote_paths import (
    is_provider_safe_basename,
    provider_safe_basename,
    safe_name,
    validate_provider_safe_basename,
)


class ProviderSafeBasenameTests(unittest.TestCase):
    def test_sanitizer_neutralizes_literal_and_compatibility_dot_runs(self) -> None:
        cases = {
            "Episode...Title.mkv": "Episode-Title.mkv",
            "Episode．．Title.mkv": "Episode-Title.mkv",
            "Episode…Title.mkv": "Episode.Title.mkv",
        }

        for raw_name, expected_name in cases.items():
            with self.subTest(raw_name=raw_name):
                self.assertFalse(is_provider_safe_basename(raw_name))
                self.assertEqual(provider_safe_basename(raw_name), expected_name)
                self.assertEqual(safe_name(raw_name, max_bytes=240), expected_name)
                self.assertTrue(is_provider_safe_basename(expected_name))

    def test_sanitizer_replaces_compatibility_reserved_characters(self) -> None:
        raw_name = "Episode？Title：Part／A.mkv"
        expected_name = "Episode-Title-Part-A.mkv"

        self.assertFalse(is_provider_safe_basename(raw_name))
        self.assertEqual(provider_safe_basename(raw_name), expected_name)
        self.assertTrue(is_provider_safe_basename(expected_name))

    def test_provider_validator_rejects_provider_visible_unsafe_forms(self) -> None:
        unsafe_names = (
            "Episode...Title.mkv",
            "Episode．．Title.mkv",
            "Episode…Title.mkv",
            "Episode？Title.mkv",
            "Episode：Title.mkv",
            "Episode／Title.mkv",
        )

        for name in unsafe_names:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    validate_provider_safe_basename(name)
                with self.assertRaises(ValueError):
                    _validate_remote_basename(name)

        valid_name = "Episode，Title.mkv"
        self.assertEqual(validate_provider_safe_basename(valid_name), valid_name)
        self.assertEqual(_validate_remote_basename(valid_name), valid_name)

    def test_source_validator_retains_internal_dot_runs_but_rejects_path_segments(self) -> None:
        self.assertEqual(
            _validate_remote_source_basename("release [x264....mp4"),
            "release [x264....mp4",
        )
        self.assertEqual(
            _validate_remote_source_basename(
                "Seraph of the End：Vampire Reign[01].mkv"
            ),
            "Seraph of the End：Vampire Reign[01].mkv",
        )
        for name in (".", "..", "a/b", "a\\b", "a／b"):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    _validate_remote_source_basename(name)

    def test_move_validates_existing_members_as_source_names(self) -> None:
        client = AListClient(
            "http://localhost:5244",
            "user",
            "password",
            allow_insecure_http=True,
        )
        with patch.object(client, "call") as call:
            client.move(
                "/source",
                "/destination",
                ["Seraph of the End：Vampire Reign[01].mkv"],
            )
            call.assert_called_once_with(
                "move",
                {
                    "src_dir": "/source",
                    "dst_dir": "/destination",
                    "names": ["Seraph of the End：Vampire Reign[01].mkv"],
                },
            )
        for name in ("a／b", "a\\b", ".."):
            with self.subTest(name=name), patch.object(client, "call") as call:
                with self.assertRaises(ValueError):
                    client.move("/source", "/destination", [name])
                call.assert_not_called()

    def test_core_basename_gate_prevents_unsafe_rename_dispatch(self) -> None:
        client = AListClient(
            "http://localhost:5244",
            "user",
            "password",
            allow_insecure_http=True,
        )

        with patch.object(client, "call") as call:
            with self.assertRaises(ValueError):
                client.rename("/library/source.mkv", "Episode...Title.mkv")
            call.assert_not_called()

            client.rename("/library/source.mkv", "Episode，Title.mkv")
            call.assert_called_once_with(
                "rename",
                {"path": "/library/source.mkv", "name": "Episode，Title.mkv"},
            )
