"""Regression coverage: a letter-suffixed bracket ordinal.

``[23B]`` is an alternate cut with its own release coordinate, omitted from
the strict ``1..N`` run like a fractional episode.
"""

from __future__ import annotations

import unittest

from engine.scrapeflow.source_inventory import SourceFile
from local.scrapeflow_api.library_index import _is_letter_variant_episode_video


def _video(path: str, size: int = 100) -> SourceFile:
    return SourceFile(
        path=path,
        name=path.rsplit("/", 1)[-1],
        size=size,
        object_type="video",
        modified="",
    )


class LetterVariantBracketOrdinalTests(unittest.TestCase):
    """``[23B]`` is an alternate cut, not an integer-run member."""

    def test_letter_variant_is_detected_and_excluded_from_run(self) -> None:
        plain = _video("/incoming/Show/[G] Show [23].mkv")
        beta = _video("/incoming/Show/[G] Show [23B].mkv")
        self.assertFalse(_is_letter_variant_episode_video(plain))
        self.assertTrue(_is_letter_variant_episode_video(beta))
        self.assertTrue(
            _is_letter_variant_episode_video(
                _video("/incoming/Show/[G] Show [23β].mkv")
            )
        )
        # A version revision re-releases the same ordinal: not a variant.
        self.assertFalse(
            _is_letter_variant_episode_video(
                _video("/incoming/Show/[G] Show [02v2].mkv")
            )
        )
        # A second season marker is not a letter variant either.
        self.assertFalse(
            _is_letter_variant_episode_video(
                _video("/incoming/Show/[G] Show S02E04 [1080p].mkv")
            )
        )


if __name__ == "__main__":
    unittest.main()
