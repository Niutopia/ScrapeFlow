"""Regression tests for the release lexicon consumers (data, not logic).

AGENTS.md §7 forbids per-work title branches in business code; these tests pin
the data-table behavior so a lexicon entry can be moved/edited without
silently changing what the matcher sends to TMDB.
"""

from __future__ import annotations

import unittest

from engine.scrapeflow.data.release_lexicon import (
    CROSS_SCRIPT_SEASON_ALIASES,
    QUERY_VARIANT_ALIASES,
    SOURCE_NAME_CORRECTIONS,
    SOURCE_QUERY_OVERRIDES,
)
from engine.scrapeflow.identity_matching import (
    _query_from_source,
    _search_query_variants,
)


class ReleaseLexiconTests(unittest.TestCase):
    def test_source_query_overrides_map_abbreviations_to_canonical_queries(self) -> None:
        for pattern, replacement in SOURCE_QUERY_OVERRIDES:
            self.assertNotEqual(pattern, "")
            self.assertNotEqual(replacement, "")
        self.assertEqual(
            _query_from_source("/incoming/86 不存"),
            "86 -不存在的战区-",
        )

    def test_source_name_corrections_apply_before_the_query(self) -> None:
        for wrong, correct in SOURCE_NAME_CORRECTIONS:
            self.assertNotEqual(wrong, correct)
        self.assertIn("白色相簿", _query_from_source("/incoming/白色相薄 (2013)"))
        self.assertIn(
            "最弱无败神装机龙",
            _query_from_source("/incoming/最弱无败神龙 S01"),
        )

    def test_lexical_variants_are_query_additions(self) -> None:
        variants = _search_query_variants("物语系列")
        self.assertIn("故事系列", variants)
        variants = _search_query_variants("神圣之星")
        self.assertIn("圣星", variants)

    def test_query_variant_aliases_are_query_additions(self) -> None:
        for pattern, replacement in QUERY_VARIANT_ALIASES:
            self.assertNotEqual(pattern, "")
            self.assertNotEqual(replacement, "")
        variants = _search_query_variants("末日三问")
        self.assertIn("末日时在做什么？有没有空？可以来拯救吗？", variants)
        variants = _search_query_variants("杖与剑的魔法谭 S01")
        self.assertTrue(any("杖与剑的魔剑谭" in variant for variant in variants))

    def test_cross_script_season_aliases_are_data(self) -> None:
        for latin, cjk_aliases in CROSS_SCRIPT_SEASON_ALIASES.items():
            self.assertTrue(latin)
            self.assertTrue(cjk_aliases)
        self.assertIn("illya", CROSS_SCRIPT_SEASON_ALIASES)
        self.assertIn("伊莉雅", CROSS_SCRIPT_SEASON_ALIASES["illya"])


if __name__ == "__main__":
    unittest.main()
