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

# Special-context release tokens used by the special-context classifier.
# Everything here is a concrete release spelling, never a generic label.
SPECIAL_CONTEXT_RELEASE_TOKENS: tuple[str, ...] = (
    "通往大人的阶梯",
    "最大的危机",
    r"fate[ ._/-]*prototype",
    "柯里乌斯之梦",
    r"coleus[ ._-]*no[ ._-]*yume",
    "课外授业篇",
    "課外授業編",
    r"kagai[ ._-]*jugy[oō][ ._-]*hen",
)

# Special-label rules: ``(token regex, returned label)`` for labeled extras.
SPECIAL_LABEL_RULES: tuple[tuple[str, str], ...] = (
    (
        r"(?:^|[\s._\-\[\]()])MagiRepo(?:$|[\s._\-\[\]()])",
        "特典动画广告/Animated Magia Report Commercial",
    ),
)

# Normalized parent-directory aliases (release spelling -> official title).
NORMALIZED_PARENT_ALIASES: dict[str, str] = {
    "daisanhikoushoujotai": "第三飞行少女队",
}

# Edition-mapping rules for release labels.  The mapping logic in the planner
# is generic and gated on official TMDB title variants; these regexes are the
# only data it needs.  ``warning_template`` is the user-facing summary label.
RELEASE_EDITION_RULES: dict[str, str] = {
    "marker": r"mini[ ._-]*todo|minitodo|ミニ届",
    "romeo_title": r"romeo.*juliet|罗密欧.*朱丽叶|羅密歐.*朱麗葉|ロミオ.*ジュリエット",
    "after_title": r"after[ ._-]*story|epilogue|后日谈|後日談|后篇|後篇|それから",
    "romeo_file": r"romeo.*juliet|\b(?:2D|3D)(?:[ ._-]*ver)?\b",
    "after_file": r"epilogue|sorekara|それから",
    "warning_template": (
        "{count} 个 Mini Todoke 2D/3D/后日谈视频或字幕"
        "已根据多语言官方「罗密欧与朱丽叶」/后日谈标题"
        "映射到 Season 00；3D 作为同集版本保留"
    ),
}

# Authoritative data for fractional specials whose local release number does
# NOT appear in any TMDB Season 00 title.  The planner's generic evidence
# gate cannot bridge ``[24.5]`` to ``第0话 Reflection`` on its own; this map
# is the data-ized answer, consulted only for the exact (tmdb_id, "N.5")
# pair and treated as explicit official evidence.
FRACTIONAL_SPECIAL_ALIASES: dict[int, dict[str, tuple[str, str]]] = {
    45782: {
        "18.5": ("S00E23", "第18.5话 Recollection"),
        "24.5": ("S00E24", "第0话 Reflection"),
        "36.5": ("S00E25", "第12.5话 回忆"),
    },
}

__all__ = [
    "BIDIRECTIONAL_LEXICAL_VARIANTS",
    "CROSS_SCRIPT_SEASON_ALIASES",
    "FRACTIONAL_SPECIAL_ALIASES",
    "LEXICAL_VARIANTS",
    "NORMALIZED_PARENT_ALIASES",
    "QUERY_VARIANT_ALIASES",
    "RELEASE_EDITION_RULES",
    "SOURCE_NAME_CORRECTIONS",
    "SOURCE_QUERY_OVERRIDES",
    "SPECIAL_CONTEXT_RELEASE_TOKENS",
    "SPECIAL_LABEL_RULES",
]
