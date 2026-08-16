"""Bounded release-title lexicon (plain data, not business logic).

AGENTS.md §7 forbids per-work title branches in business code.  These tables
are the only place where concrete release spellings live; matchers consume
them as plain data and every entry must keep a regression test.  Adding a
release spelling here does not grant an identity — candidate scoring, year
evidence and ambiguity margins remain authoritative.
"""

from __future__ import annotations

# Query-variant aliases: ``(regex, replacement)`` applied to search queries.
QUERY_VARIANT_ALIASES: tuple[tuple[str, str], ...] = (
    (r"^\s*末日三问\s*$", "末日时在做什么？有没有空？可以来拯救吗？"),
    (r"杖与剑的魔法谭", "杖与剑的魔剑谭"),
    (r"瑞克和\s*MD", "瑞克和莫蒂"),
)

# Exact source-name corrections applied before the TMDB query.
SOURCE_NAME_CORRECTIONS: tuple[tuple[str, str], ...] = (
    ("白色相薄", "白色相簿"),
    ("最弱无败神龙", "最弱无败神装机龙"),
)

# Bounded one-way lexical variants applied to query text.
LEXICAL_VARIANTS: tuple[tuple[str, str], ...] = (
    ("神圣之", "圣"),
)

# Bounded bidirectional lexical variants (either spelling may be the query).
BIDIRECTIONAL_LEXICAL_VARIANTS: tuple[tuple[str, str], ...] = (
    ("物语", "故事"),
)

# Regex-guarded canonical queries for abbreviated release folder labels.
SOURCE_QUERY_OVERRIDES: tuple[tuple[str, str], ...] = (
    (r"86.*(?:不存(?!在)|ZDZQ)", "86 -不存在的战区-"),
)

# Cross-script season identity hints: latin fragment -> CJK aliases.
CROSS_SCRIPT_SEASON_ALIASES: dict[str, tuple[str, ...]] = {
    "illya": ("伊莉雅", "イリヤ"),
}

__all__ = [
    "BIDIRECTIONAL_LEXICAL_VARIANTS",
    "CROSS_SCRIPT_SEASON_ALIASES",
    "LEXICAL_VARIANTS",
    "QUERY_VARIANT_ALIASES",
    "SOURCE_NAME_CORRECTIONS",
    "SOURCE_QUERY_OVERRIDES",
]
