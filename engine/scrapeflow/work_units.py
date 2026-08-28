"""WorkUnit domain models, identity evidence, and lifecycle records.

A WorkUnit represents a single distinct creative work discovered within an
IntakeSource (or RootJob). While a single RootJob might contain a series container
with multiple works (e.g. Fate/Zero, Fate/stay night), each WorkUnit has its own:
  - IdentityEvidence (names, years, episode structure, parent container clues)
  - TMDB identity match (AutoMatch)
  - Reconciliation outcome (new_work, merge_existing, duplicate, gap)
  - Lifecycle state (pending -> confirmed / uncertain / failed)

All classes are immutable/frozen dataclasses with serialization helpers and atomic
file persistence.
"""

from __future__ import annotations

import json
import re
import tempfile
import unicodedata
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from engine.scrapeflow.boundary_analysis import (
    WorkCandidate,
    _season_number_from_directory_name,
)
from engine.scrapeflow.media_policy import DISC_IMAGE_INSPECTION_REQUIRED
from engine.scrapeflow.source_inventory import (
    SourceFile,
    SourceNode,
    collect_all_files,
)


# ---------------------------------------------------------------------------
# Episode & Title Patterns
# ---------------------------------------------------------------------------

_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")

_SEASON_EPISODE_RE = re.compile(
    r"""
    (?:^|[^A-Za-z0-9])
    S\s*0*(\d{1,3})\s*E\s*0*(\d{1,4})
    (?:$|[^0-9])
    """,
    re.IGNORECASE | re.VERBOSE,
)

# A filename with an explicit season >= 2 coordinate describes that later
# season's air date, never the work's first-air year.  Treating such a year
# as work-year evidence would wrongly reject the canonical entry, which first
# aired years before the continuation season.
_CONTINUATION_SEASON_RE = re.compile(
    r"""
    (?:^|[^A-Za-z0-9])
    S\s*0*([2-9]\d{0,2})\s*E\s*0*\d{1,4}
    (?:$|[^0-9])
    """,
    re.IGNORECASE | re.VERBOSE,
)

_STANDALONE_EPISODE_RE = re.compile(
    r"""
    (?:^|[^A-Za-z0-9])
    (?:EP?|第|E)\s*0*(\d{1,4})\s*(?:[话話集期]|$|[^0-9])
    """,
    re.IGNORECASE | re.VERBOSE,
)

_BRACKETED_EPISODE_RE = re.compile(
    r"""
    \[\s*0*(\d{1,4})\s*(?:v\d+)?\s*\]
    """,
    re.IGNORECASE | re.VERBOSE,
)

_SPECIAL_KEYWORD_RE = re.compile(
    r"""
    (?:^|[\s._\-\[(])
    (?:SP|SPECIAL|OVA|OAV|OAD|EXTRA|EXTRAS|TOKUTEN|特典|特别篇|特辑)
    (?:[\s._\-)\]]|0*(\d{1,3})|$)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Physical-release labels are identity *context*, not episode coordinates.
# Keep this grammar separate from ``extract_episode_pattern``: an OAD/OVA
# ordinal is release-local and must not be silently turned into TMDB Season
# 00 (or Season 01).  The matcher below is intentionally bounded to the
# common marker vocabulary and accepts both ``OAD01``/``OAD 01`` and
# ``01 OAD`` forms.  A marker without a number is retained as context but
# does not contribute to a complete numbered run.
_PHYSICAL_SPECIAL_MARKER_RE = re.compile(
    r"(?<![A-Za-z])(?P<marker>OVA|OAV|OAD|SP|SPECIAL)"
    r"(?:[\s._-]*(?:SERIES|系列))?"
    r"[\s._-]*[\[(]?\s*0*(?P<number>\d{1,3})\s*[\])]?(?!\d)",
    re.IGNORECASE,
)
_REVERSE_PHYSICAL_SPECIAL_MARKER_RE = re.compile(
    r"(?<![A-Za-z0-9])0*(?P<number>\d{1,3})[\s._-]*"
    r"(?P<marker>OVA|OAV|OAD|SP|SPECIAL)(?![A-Za-z])",
    re.IGNORECASE,
)
_PHYSICAL_SPECIAL_TOKEN_RE = re.compile(
    r"(?<![A-Za-z])(?P<marker>OVA|OAV|OAD|SP|SPECIAL)(?![A-Za-z])",
    re.IGNORECASE,
)
_STANDALONE_SPECIAL_NUMBER_RE = re.compile(
    r"(?:^|[\s._\-\[\](){}])0*(\d{1,3})(?=$|[\s._\-\[\](){}])"
)

# A file with a title immediately before an explicit episode coordinate is
# particularly strong *search* evidence.  This stays deliberately narrower
# than episode parsing: it is used only to choose a small, representative
# sample from an already-owned work-unit tree, never to assert a season or an
# identity by itself.
_TITLE_BEARING_EPISODE_MARKER_RE = re.compile(
    r"""
    (?:^|[\s._+\-\[\](){}])
    (?:
        S\s*0*\d{1,3}\s*E\s*0*\d{1,4}
        |
        E\s*0*\d{1,4}(?:\s*[\-–—~～]\s*E\s*0*\d{1,4})?
    )
    (?=$|[\s._+\-\[\](){}])
    """,
    re.IGNORECASE | re.VERBOSE,
)

_KNOWN_RESOLUTIONS = frozenset({480, 576, 720, 1080, 2160, 4320})
_KNOWN_CODECS = frozenset({"264", "265", "x264", "x265", "h264", "h265", "hevc", "av1", "10bit", "8bit"})

# A directory containing ``01.mp4`` … ``12.mp4`` is common, but an ordinal
# alone is never a work title, an episode coordinate, or a season assertion.
# Keep this grammar deliberately much narrower than the generic episode parser:
# quality tags, decimals, ranges, versions, zero, and mixed video names remain
# unproven.  It is retained only as a fail-closed source-shape flag for C/U;
# it must not synthesize ``S01E01`` … ``S01EN`` evidence.
_NAKED_NUMERIC_VIDEO_STEM_RE = re.compile(r"^0*([1-9]\d{0,2})$")
_CJK_IDENTITY_CHAR_RE = re.compile(r"[\u3400-\u9fff\u3040-\u30ff]")
_CJK_GENERIC_BOUNDARY_LABELS = frozenset({
    "全集", "全季", "合集", "资源", "资源文档", "文档", "文件", "视频",
    "影片", "动画", "動漫", "番剧", "番劇", "电视剧", "電視劇", "剧集",
    "劇集", "未命名", "未知", "发布包", "無標題發布包", "无标题发布包",
    "アニメ", "アニメ全集",
})
_CJK_BOUNDARY_RELEASE_NOISE_RE = re.compile(
    r"(?:\b(?:19|20)\d{2}\b|\b(?:4k|8k|2160p|1080p|720p|480p)\b|"
    r"(?:全|共)\s*\d{1,4}\s*(?:集|话|話|期)|"
    r"(?:简中|繁中|简繁|繁简|中字|双语|內封|内封|內嵌|内嵌|外挂|"
    r"蓝光|藍光|高清|超清|无删减|無刪減|完整版))",
    re.IGNORECASE,
)
_MIN_NAKED_NUMERIC_EPISODE_RUN = 4


# ---------------------------------------------------------------------------
# EpisodePattern
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class EpisodePattern:
    """Structural summary of episodes and seasons detected in source files."""

    season_numbers: tuple[int, ...]
    episode_numbers: tuple[int, ...]
    total_episodes: int
    has_specials: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "season_numbers": list(self.season_numbers),
            "episode_numbers": list(self.episode_numbers),
            "total_episodes": self.total_episodes,
            "has_specials": self.has_specials,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> EpisodePattern:
        return cls(
            season_numbers=tuple(int(x) for x in raw.get("season_numbers") or ()),
            episode_numbers=tuple(int(x) for x in raw.get("episode_numbers") or ()),
            total_episodes=int(raw.get("total_episodes", 0)),
            has_specials=bool(raw.get("has_specials", False)),
        )


def extract_episode_pattern(
    files_or_node: Sequence[SourceFile | str] | SourceNode,
) -> EpisodePattern | None:
    """Extract regular seasons/episodes and specials from files or directory node."""
    if isinstance(files_or_node, SourceNode):
        all_files = collect_all_files(files_or_node)
        filenames = [f.name for f in all_files if f.object_type == "video"]
    else:
        filenames = [
            f.name if isinstance(f, SourceFile) else str(f)
            for f in files_or_node
        ]

    if not filenames:
        return None

    seasons: set[int] = set()
    episodes: set[int] = set()
    has_specials = False

    for name in filenames:
        # Check special indicators
        if _SPECIAL_KEYWORD_RE.search(name):
            has_specials = True

        # Check SxxExx
        se_match = _SEASON_EPISODE_RE.search(name)
        if se_match:
            s_num = int(se_match.group(1))
            e_num = int(se_match.group(2))
            if s_num == 0:
                has_specials = True
            else:
                seasons.add(s_num)
                episodes.add(e_num)
            continue

        # Check standalone episode pattern (e.g. EP01, 第01集, E01)
        ep_match = _STANDALONE_EPISODE_RE.search(name)
        if ep_match:
            val = int(ep_match.group(1))
            if val not in _KNOWN_RESOLUTIONS and str(val) not in _KNOWN_CODECS:
                episodes.add(val)
                seasons.add(1)
            continue

        # Check bracketed numbers: [01], [12]
        for b_match in _BRACKETED_EPISODE_RE.finditer(name):
            val = int(b_match.group(1))
            if val not in _KNOWN_RESOLUTIONS and str(val) not in _KNOWN_CODECS and not (1900 <= val <= 2099):
                episodes.add(val)
                seasons.add(1)

    if not episodes and not seasons and not has_specials:
        return None

    sorted_seasons = tuple(sorted(seasons))
    sorted_episodes = tuple(sorted(episodes))
    total_episodes = len(sorted_episodes)

    return EpisodePattern(
        season_numbers=sorted_seasons,
        episode_numbers=sorted_episodes,
        total_episodes=total_episodes,
        has_specials=has_specials,
    )


# ---------------------------------------------------------------------------
# IdentityEvidence
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class IdentityEvidence:
    """Aggregated naming and structural clues used for TMDB matching."""

    work_unit_id: str
    boundary_label: str
    parent_labels: tuple[str, ...]
    representative_names: tuple[str, ...]
    normalized_titles: tuple[str, ...]
    years: tuple[int, ...]
    episode_pattern: EpisodePattern | None
    media_shape: str   # "movie" | "tv" | "mixed" | "unknown"
    aliases: tuple[str, ...]
    # A strictly observed root-level ``01`` … ``N`` video run.  This says
    # nothing about season or episode coordinates; every such run makes C/U
    # fail closed unless the separately recorded CJK fallback is eligible.
    strict_naked_numeric_video_run: bool = False
    # The only narrow release-label fallback: B/W called it TV-shaped and its
    # boundary is a meaningful CJK title carrying an explicit year.
    naked_numeric_cjk_release_eligible: bool = False
    # Years observed on this unit's OWN season-labeled subdirectories
    # (``第一季（2020）全24集``).  They anchor the naked-numeric year gate the
    # same way a boundary-label year does, but they are packaging metadata,
    # never ordinary work-year evidence: a continuation-season year must not
    # poison the aggregate ``years`` used by ordinary scoring.
    naked_numeric_owned_season_years: tuple[int, ...] = ()
    # Explicit physical special context observed in owned video names/paths.
    # These fields never assert a TMDB season.  ``special_episode_count`` is
    # populated only for a complete, unique, contiguous 1..N run where every
    # owned video carries the same marker family and an attached ordinal.
    special_markers: tuple[str, ...] = ()
    special_episode_numbers: tuple[int, ...] = ()
    special_episode_count: int | None = None
    special_numbered_run_complete: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "work_unit_id": self.work_unit_id,
            "boundary_label": self.boundary_label,
            "parent_labels": list(self.parent_labels),
            "representative_names": list(self.representative_names),
            "normalized_titles": list(self.normalized_titles),
            "years": list(self.years),
            "episode_pattern": self.episode_pattern.as_dict() if self.episode_pattern else None,
            "media_shape": self.media_shape,
            "aliases": list(self.aliases),
            "strict_naked_numeric_video_run": self.strict_naked_numeric_video_run,
            "naked_numeric_cjk_release_eligible": self.naked_numeric_cjk_release_eligible,
            "naked_numeric_owned_season_years": list(
                self.naked_numeric_owned_season_years
            ),
            "special_markers": list(self.special_markers),
            "special_episode_numbers": list(self.special_episode_numbers),
            "special_episode_count": self.special_episode_count,
            "special_numbered_run_complete": self.special_numbered_run_complete,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> IdentityEvidence:
        ep_raw = raw.get("episode_pattern")
        return cls(
            work_unit_id=str(raw["work_unit_id"]),
            boundary_label=str(raw["boundary_label"]),
            parent_labels=tuple(str(x) for x in raw.get("parent_labels") or ()),
            representative_names=tuple(str(x) for x in raw.get("representative_names") or ()),
            normalized_titles=tuple(str(x) for x in raw.get("normalized_titles") or ()),
            years=tuple(int(x) for x in raw.get("years") or ()),
            episode_pattern=EpisodePattern.from_dict(ep_raw) if isinstance(ep_raw, Mapping) else None,
            media_shape=str(raw.get("media_shape", "unknown")),
            aliases=tuple(str(x) for x in raw.get("aliases") or ()),
            # ``naked_numeric_video_run`` was a short-lived pre-split field.
            # Reading it as an observed run remains safely fail-closed; it
            # does not grant the new CJK release fallback.
            strict_naked_numeric_video_run=bool(
                raw.get(
                    "strict_naked_numeric_video_run",
                    raw.get("naked_numeric_video_run", False),
                )
            ),
            naked_numeric_cjk_release_eligible=bool(
                raw.get("naked_numeric_cjk_release_eligible", False)
            ),
            naked_numeric_owned_season_years=tuple(
                int(x)
                for x in raw.get("naked_numeric_owned_season_years") or ()
                if isinstance(x, int) and not isinstance(x, bool)
            ),
            special_markers=tuple(
                str(x).upper() for x in raw.get("special_markers") or ()
            ),
            special_episode_numbers=tuple(
                int(x) for x in raw.get("special_episode_numbers") or ()
            ),
            special_episode_count=(
                int(raw["special_episode_count"])
                if raw.get("special_episode_count") is not None
                else None
            ),
            special_numbered_run_complete=bool(
                raw.get("special_numbered_run_complete", False)
            ),
        )


def _clean_noise_tags(text: str) -> str:
    """Remove common release group bracket tags, resolutions, and codecs."""
    cleaned = re.sub(r"\[[^\]]*\]|\([^)]*(?:1080|2160|720|x26|hevc)[^)]*\)", " ", text)
    cleaned = re.sub(
        r"\b(?:4k|8k|2160p|1080p|720p|480p|bluray|blu-ray|web-?dl|webrip|x26[45]|hevc|av1|10bit|aac|flac|dts)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _naked_numeric_video_ordinal(name: str) -> int | None:
    """Return an exact file-stem ordinal, never a title/quality fragment."""
    stem = unicodedata.normalize("NFKC", Path(name).stem).strip()
    match = _NAKED_NUMERIC_VIDEO_STEM_RE.fullmatch(stem)
    if match is None:
        return None
    value = int(match.group(1))
    return value if 0 < value <= 999 else None


def _is_generic_cjk_boundary_composition(text: str) -> bool:
    """Return whether all CJK label text is generic release vocabulary.

    Release folders often concatenate otherwise harmless generic nouns, such
    as ``电视剧全集`` or ``发布包合集``.  Exact-set membership is insufficient:
    a title is meaningful only when some CJK text remains after the label can
    no longer be segmented entirely into the bounded generic vocabulary.  This
    is a lexical safety check, not a work-title lookup.
    """
    value = unicodedata.normalize("NFKC", str(text or "")).strip()
    if not value:
        return False
    terms = tuple(sorted(_CJK_GENERIC_BOUNDARY_LABELS, key=len, reverse=True))
    reachable = [False] * (len(value) + 1)
    reachable[0] = True
    for index in range(len(value)):
        if not reachable[index]:
            continue
        for term in terms:
            if value.startswith(term, index):
                reachable[index + len(term)] = True
    return reachable[-1]


def _meaningful_cjk_boundary_label(label: str) -> bool:
    """Whether a boundary label can safely anchor naked-number TV evidence.

    This is not a title matcher and never supplies an identity itself.  It
    merely rejects the release-only labels that would make ``01.mp4`` a
    dangerous search query: bare package metadata, generic resource labels,
    and labels too short to be a meaningful CJK work name stay unproven.
    Cross-script title matching remains entirely in ``identity_matching``.
    """
    text = unicodedata.normalize("NFKC", str(label or "")).strip()
    raw_cjk = "".join(_CJK_IDENTITY_CHAR_RE.findall(text))
    if _is_generic_cjk_boundary_composition(raw_cjk):
        return False
    cleaned = _CJK_BOUNDARY_RELEASE_NOISE_RE.sub(" ", text)
    title_cjk = "".join(_CJK_IDENTITY_CHAR_RE.findall(cleaned))
    # A release-only prefix can look non-generic before ``(2024)``/``全12集``
    # is stripped (for example ``发布包（2024）全12集``).  Re-check the
    # remaining identity text rather than letting the removed metadata supply
    # the apparent title length.
    if _is_generic_cjk_boundary_composition(title_cjk):
        return False
    return len(title_cjk) >= 3


def _has_strict_naked_numeric_video_run(
    video_files: Sequence[SourceFile],
) -> bool:
    """Prove a complete primary ``01`` … ``N`` video run without coordinates.

    The source must contain at least four videos, every video filename must be
    exactly one positive numeric stem, the ordinals must be unique, and their
    sorted values must equal ``1..N``.  Any episode suffix, trailer, special,
    duplicate encode, or missing number invalidates the entire structural
    fallback rather than being silently excluded.
    """
    if len(video_files) < _MIN_NAKED_NUMERIC_EPISODE_RUN:
        return False
    numbers = [_naked_numeric_video_ordinal(file.name) for file in video_files]
    if any(number is None for number in numbers):
        return False
    concrete = tuple(sorted(int(number) for number in numbers if number is not None))
    if len(set(concrete)) != len(concrete):
        return False
    if concrete != tuple(range(1, len(concrete) + 1)):
        return False
    return True


def _physical_special_marker_evidence(
    video_files: Sequence[SourceFile],
) -> tuple[tuple[str, ...], tuple[int, ...], int | None, bool]:
    """Extract bounded OVA/OAV/OAD/SP context from one owned video scope.

    This is deliberately a source-shape fact, not an identity resolver.  A
    marker in a parent directory supplies context for a bare child ordinal,
    while ordinary files in the same scope invalidate the complete numbered
    run.  The returned count is therefore conservative: it is non-``None``
    only when every video has one marker family, one positive ordinal, no
    duplicate, and the exact contiguous run ``1..N``.
    """
    if not video_files:
        return (), (), None, False
    markers: set[str] = set()
    numbers: list[int] = []
    all_numbered = True
    for file in video_files:
        text = unicodedata.normalize(
            "NFKC", f"{file.path} {file.name}"
        )
        direct = list(_PHYSICAL_SPECIAL_MARKER_RE.finditer(text))
        reverse = list(_REVERSE_PHYSICAL_SPECIAL_MARKER_RE.finditer(text))
        tokens = {
            str(match.group("marker")).upper()
            for match in (
                _PHYSICAL_SPECIAL_TOKEN_RE.finditer(text)
            )
        }
        markers.update(tokens)
        number: int | None = None
        # Prefer a marker-attached ordinal.  A year is never a release ordinal.
        # ``OAD 2016 [01]`` places the release year between the marker and
        # the real bracketed ordinal, so examine every marker-attached match
        # and keep the first non-year value instead of silently dropping the
        # ordinal just because it is not adjacent to the marker.
        if direct:
            for match in direct:
                value = int(match.group("number"))
                if not 1900 <= value <= 2099:
                    number = value
                    break
        elif reverse:
            for match in reverse:
                value = int(match.group("number"))
                if not 1900 <= value <= 2099:
                    number = value
                    break
        else:
            # ``OAD/01.mkv`` is common.  Only use a bare basename ordinal
            # when a marker appears in an ancestor path segment; this avoids
            # treating an ordinary ``Show [01]`` file as a special.
            marker_in_parent = "/" in text and bool(
                _PHYSICAL_SPECIAL_TOKEN_RE.search(
                    text.rsplit("/", 1)[0]
                )
            )
            if marker_in_parent:
                basename = Path(file.name).stem
                bare = _STANDALONE_SPECIAL_NUMBER_RE.fullmatch(basename)
                if bare is not None:
                    value = int(bare.group(1))
                    if not 1900 <= value <= 2099:
                        number = value
        if number is None:
            # ``Show OAD 2016 [01]`` carries the marker in its own stem while
            # the release ordinal sits in a standalone bracket behind the
            # year, out of reach of the marker-attached patterns above.
            # Accept exactly one standalone delimited ordinal from that
            # marker-bearing stem so the run keeps its release-local number.
            stem = unicodedata.normalize("NFKC", Path(file.name).stem)
            if _PHYSICAL_SPECIAL_TOKEN_RE.search(stem):
                standalone = {
                    int(match.group(1))
                    for match in _STANDALONE_SPECIAL_NUMBER_RE.finditer(stem)
                    if 0 < int(match.group(1)) <= 999
                }
                if len(standalone) == 1:
                    number = next(iter(standalone))
        if number is None or number <= 0 or number > 999:
            all_numbered = False
        else:
            numbers.append(number)

    marker_tuple = tuple(sorted(markers))
    numbers_tuple = tuple(sorted(set(numbers)))
    complete = bool(
        all_numbered
        and len(marker_tuple) == 1
        and len(numbers) == len(video_files)
        and len(numbers_tuple) == len(numbers)
        and numbers_tuple == tuple(range(1, len(numbers) + 1))
    )
    return (
        marker_tuple,
        numbers_tuple,
        len(numbers_tuple) if complete else None,
        complete,
    )


def physical_special_marker_evidence(
    files_or_node: Sequence[SourceFile] | SourceNode,
) -> tuple[tuple[str, ...], tuple[int, ...], int | None, bool]:
    """Expose the shared bounded physical-special source-shape grammar.

    C/U and D/F deliberately use this exact same parser.  It returns marker
    families, observed ordinals, a count only for a complete numbered run, and
    the corresponding completeness flag; callers must still prove TMDB
    identity and target season independently.
    """
    if isinstance(files_or_node, SourceNode):
        videos = [
            file for file in collect_all_files(files_or_node)
            if file.object_type == "video"
        ]
    else:
        videos = list(files_or_node)
    return _physical_special_marker_evidence(videos)


def is_physical_special_video_file(file: SourceFile) -> bool:
    """Whether one video file carries a physical-special marker with an ordinal.

    This is the single-file predicate behind the shared bounded physical-special
    grammar (``physical_special_marker_evidence``).  A marker may also live in
    a parent directory and supply context for a bare child ordinal
    (``OAD/01.mkv``); the whole-scope completeness decision still runs through
    ``physical_special_marker_evidence``, never through this predicate alone.
    """
    text = unicodedata.normalize("NFKC", f"{file.path} {file.name}")
    if _PHYSICAL_SPECIAL_MARKER_RE.search(text):
        return True
    if _REVERSE_PHYSICAL_SPECIAL_MARKER_RE.search(text):
        return True
    marker_in_parent = "/" in text and bool(
        _PHYSICAL_SPECIAL_TOKEN_RE.search(text.rsplit("/", 1)[0])
    )
    if marker_in_parent:
        basename = Path(file.name).stem
        return _STANDALONE_SPECIAL_NUMBER_RE.fullmatch(basename) is not None
    return False


def _representative_episode_title_key(name: str) -> str | None:
    """Return a stable key when a filename carries a title before ``SxxExx``.

    Root-level releases sometimes put a current season's anonymous
    ``S09E01`` files next to older season directories whose media filenames
    retain the real title.  Sampling only the first files in tree order then
    starves C of the useful evidence.  This helper recognizes only an
    explicit title-prefixed episode marker; it does not turn bare ordinals or
    a directory name into title evidence.
    """
    stem = Path(name).stem.strip(" ._-")
    marker = _TITLE_BEARING_EPISODE_MARKER_RE.search(stem)
    if marker is None or marker.start() == 0:
        return None
    prefix = _clean_noise_tags(stem[:marker.start()]).strip(" ._-")
    key = "".join(
        char for char in unicodedata.normalize("NFKC", prefix).casefold()
        if char.isalnum()
    )
    return key if any(char.isalpha() for char in key) else None


def _select_representative_video_files(
    video_files: Sequence[SourceFile],
    *,
    limit: int = 5,
    title_bearing_limit: int = 4,
) -> list[SourceFile]:
    """Choose a bounded, deterministic video sample for identity evidence.

    Prefer distinct title-bearing episode filenames anywhere inside the exact
    owned source subtree, then fill the remaining slots using the historical
    tree order.  This retains bounded TMDB input and lets ordinary films or
    opaque releases continue to use their first media filenames unchanged.
    """
    selected: list[SourceFile] = []
    selected_paths: set[str] = set()
    title_keys: set[str] = set()
    for media in video_files:
        key = _representative_episode_title_key(media.name)
        if key is None or key in title_keys:
            continue
        title_keys.add(key)
        selected.append(media)
        selected_paths.add(media.path)
        if len(selected) >= min(limit, title_bearing_limit):
            break
    for media in video_files:
        if media.path in selected_paths:
            continue
        selected.append(media)
        selected_paths.add(media.path)
        if len(selected) >= limit:
            break
    return selected


def _owned_season_directory_years(node: SourceNode) -> tuple[int, ...]:
    """Collect years from this unit's own season-labeled subdirectories.

    A multi-season release frequently keeps its year evidence on the season
    folders (``第一季（2020）全24集``) while the boundary label itself carries
    only the title and a season span.  Only a direct child whose label asserts
    an explicit season ordinal contributes: the year then describes one owned
    season of this very unit, never a neighbouring container or a file date.
    The years stay packaging anchors — they are deliberately NOT folded into
    the aggregate ``years``, because a continuation-season year describes a
    later air date, not the work's first-air year.
    """
    years: set[int] = set()
    for child in node.children:
        if _season_number_from_directory_name(child.name) is None:
            continue
        years.update(int(y) for y in _YEAR_RE.findall(child.name))
    return tuple(sorted(years))


def extract_identity_evidence(
    candidate: WorkCandidate,
    node: SourceNode | None = None,
    *,
    parent_labels: Sequence[str] = (),
) -> IdentityEvidence:
    """Build IdentityEvidence from candidate boundary analysis and file node."""
    years: set[int] = set()

    # Extract years from candidate label and parent labels
    for y_str in _YEAR_RE.findall(candidate.display_label):
        years.add(int(y_str))
    for p_label in parent_labels:
        for y_str in _YEAR_RE.findall(p_label):
            years.add(int(y_str))

    representative_names: list[str] = [candidate.display_label]
    aliases: list[str] = []

    # Extract title subparts (e.g. "Fate/Zero", "Fate - Stay Night")
    for sep in [" - ", " / ", "／", "：", ":"]:
        if sep in candidate.display_label:
            parts = [p.strip() for p in candidate.display_label.split(sep) if p.strip()]
            if len(parts) > 1:
                aliases.extend(parts)

    episode_pattern: EpisodePattern | None = None
    strict_naked_numeric_video_run = False
    naked_numeric_cjk_release_eligible = False
    owned_season_years: tuple[int, ...] = ()
    special_markers: tuple[str, ...] = ()
    special_episode_numbers: tuple[int, ...] = ()
    special_episode_count: int | None = None
    special_numbered_run_complete = False
    if node is not None:
        owned_season_years = _owned_season_directory_years(node)
        video_files = [f for f in collect_all_files(node) if f.object_type == "video"]
        if video_files:
            episode_pattern = extract_episode_pattern(video_files)
            (
                special_markers,
                special_episode_numbers,
                special_episode_count,
                special_numbered_run_complete,
            ) = physical_special_marker_evidence(video_files)
            strict_naked_numeric_video_run = _has_strict_naked_numeric_video_run(
                video_files
            )
            # This shape intentionally has no known S/E coordinates.  Today's
            # generic parser does not recognize bare ``01`` stems, but clear
            # any future parser result too: a pure ordinal run must never be
            # silently reinterpreted as season one.
            if strict_naked_numeric_video_run:
                episode_pattern = None
            # ``01.mp4`` is not a queryable title.  A full naked-number run
            # can only activate a stricter C/U identity guard when B/W already
            # sees a TV-shaped unit and this boundary itself carries enough
            # non-generic CJK title signal plus an explicit year.  It
            # deliberately does *not* add an EpisodePattern: bare ordinals do
            # not prove S01E01…S01EN.
            naked_numeric_cjk_release_eligible = (
                candidate.proposed_media_context == "tv"
                and _meaningful_cjk_boundary_label(candidate.display_label)
                and (
                    bool(_YEAR_RE.search(candidate.display_label))
                    or bool(owned_season_years)
                )
                and strict_naked_numeric_video_run
            )
            for f in _select_representative_video_files(video_files):
                if _CONTINUATION_SEASON_RE.search(f.name) is None:
                    for y_str in _YEAR_RE.findall(f.name):
                        years.add(int(y_str))
                # A naked ordinal is structural evidence only.  Adding it to
                # ``representative_names`` would later let ``01`` be sent to
                # TMDB as a movie title if the meaningful boundary query did
                # not produce a candidate.
                if _naked_numeric_video_ordinal(f.name) is not None:
                    continue
                cleaned_name = _clean_noise_tags(Path(f.name).stem)
                if cleaned_name and cleaned_name not in representative_names:
                    representative_names.append(cleaned_name)

    # Derive normalized titles
    normalized_titles: list[str] = []
    base_clean = _clean_noise_tags(candidate.display_label)
    if base_clean:
        normalized_titles.append(base_clean)

    # Clean without year
    without_year = _YEAR_RE.sub(" ", base_clean)
    without_year = re.sub(r"\s+", " ", without_year).strip(" -_()（）[]【】")
    if without_year and without_year not in normalized_titles:
        normalized_titles.append(without_year)

    return IdentityEvidence(
        work_unit_id=candidate.work_unit_id,
        boundary_label=candidate.display_label,
        parent_labels=tuple(parent_labels),
        representative_names=tuple(representative_names),
        normalized_titles=tuple(normalized_titles),
        years=tuple(sorted(years)),
        episode_pattern=episode_pattern,
        media_shape=candidate.proposed_media_context,
        aliases=tuple(sorted(set(aliases))),
        strict_naked_numeric_video_run=strict_naked_numeric_video_run,
        naked_numeric_cjk_release_eligible=naked_numeric_cjk_release_eligible,
        naked_numeric_owned_season_years=owned_season_years,
        special_markers=special_markers,
        special_episode_numbers=special_episode_numbers,
        special_episode_count=special_episode_count,
        special_numbered_run_complete=special_numbered_run_complete,
    )


# ---------------------------------------------------------------------------
# WorkUnitRecord
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True, slots=True)
class WorkUnitRecord:
    """Persistent execution state for one distinct creative work."""

    work_unit_id: str
    root_task_id: str
    boundary_key: str
    source_paths: tuple[str, ...]
    source_revision: int
    role: str
    # Boundary-analysis display evidence is durable.  A multi-directory
    # cohort has a synthetic boundary key and must not lose the human title
    # evidence to that implementation-only key during C/U.
    display_label: str = ""
    # Positive seasons explicitly asserted by B/W (for example an otherwise
    # empty S11 sibling in a verified season cohort).  This is a source-boundary
    # fact, not an identity override or an externally injected expectation.
    claimed_seasons: tuple[int, ...] = ()
    # True only when B/W saw opaque disc-image containers in this exact
    # source scope.  It stays durable so identity overrides, retries, D/E
    # lanes, and F cannot mistake title confirmation for content inspection.
    requires_content_expansion: bool = False
    media_context: str = "unknown"
    identity_status: str = "pending"  # "pending" | "confirmed" | "uncertain" | "failed"
    identity: dict[str, Any] | None = None
    candidate_identities: tuple[dict[str, Any], ...] = ()
    reconciliation_outcome: str | None = None
    matched_work_root: str | None = None
    # Narrow, system-derived D evidence used to revalidate a later F request.
    # It is not an identity override and must never be populated from a
    # browser/API payload.  It records only a fully proved, grammar-tagged
    # unqualified-episode to unique-TMDB-season mapping.
    reconciliation_evidence: dict[str, Any] | None = None
    writer_job_id: str | None = None
    # Per-unit E-lane state (P12).  Values:
    #   duplicate_consumed / existing_gap_registered / existing_gap_held /
    #   merge_done.  ``None`` means the lane has not finished yet.
    lane_status: str | None = None
    lane_detail: str | None = None
    # Known-gap coordinates the E2 lane must register (from the D verdict).
    uncovered_tokens: tuple[str, ...] = ()
    # J-step outcome after a successful formal write.  ``None`` means the
    # unit has not yet reached J; ``registered`` means the official catalog
    # was checked and the ledger was durably read back (even when no gaps
    # were found); ``attention`` is evidence/catalog insufficiency; and
    # ``failed`` is a local ledger persistence/readback failure.  Keeping it
    # on the WorkUnit rather than an EngineJob summary makes a completed
    # writer carrier safe to retry at J without writing media a second time.
    gap_status: str | None = None
    gap_detail: str | None = None
    attention: str | None = None
    # A completed WorkUnit may later need a generic, writer-backed hierarchy
    # correction after its parent-family rule improves.  This is separate from
    # ``writer_job_id``: the latter remains the immutable original media-write
    # carrier, while this field records the exact relocation carrier/readback.
    layout_repair: dict[str, Any] | None = None
    updated_at: str = field(default_factory=_now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "work_unit_id": self.work_unit_id,
            "root_task_id": self.root_task_id,
            "boundary_key": self.boundary_key,
            "source_paths": list(self.source_paths),
            "source_revision": self.source_revision,
            "role": self.role,
            "display_label": self.display_label,
            "claimed_seasons": list(self.claimed_seasons),
            "requires_content_expansion": self.requires_content_expansion,
            "media_context": self.media_context,
            "identity_status": self.identity_status,
            "identity": dict(self.identity) if self.identity else None,
            "candidate_identities": [dict(c) for c in self.candidate_identities],
            "reconciliation_outcome": self.reconciliation_outcome,
            "matched_work_root": self.matched_work_root,
            "reconciliation_evidence": (
                dict(self.reconciliation_evidence)
                if self.reconciliation_evidence is not None else None
            ),
            "writer_job_id": self.writer_job_id,
            "lane_status": self.lane_status,
            "lane_detail": self.lane_detail,
            "uncovered_tokens": list(self.uncovered_tokens),
            "gap_status": self.gap_status,
            "gap_detail": self.gap_detail,
            "attention": self.attention,
            "layout_repair": (
                dict(self.layout_repair)
                if self.layout_repair is not None else None
            ),
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> WorkUnitRecord:
        ident = raw.get("identity")
        cand_list = raw.get("candidate_identities") or ()
        reconciliation_evidence = raw.get("reconciliation_evidence")
        layout_repair = raw.get("layout_repair")
        return cls(
            work_unit_id=str(raw["work_unit_id"]),
            root_task_id=str(raw["root_task_id"]),
            boundary_key=str(raw["boundary_key"]),
            source_paths=tuple(str(x) for x in raw.get("source_paths") or ()),
            source_revision=int(raw.get("source_revision", 1)),
            role=str(raw.get("role", "single_work")),
            display_label=str(raw.get("display_label") or ""),
            claimed_seasons=tuple(
                sorted({
                    int(value)
                    for value in (raw.get("claimed_seasons") or ())
                    if isinstance(value, int)
                    and not isinstance(value, bool)
                    and value > 0
                })
            ),
            requires_content_expansion=raw.get("requires_content_expansion") is True,
            media_context=str(raw.get("media_context", "unknown")),
            identity_status=str(raw.get("identity_status", "pending")),
            identity=dict(ident) if isinstance(ident, Mapping) else None,
            candidate_identities=tuple(dict(c) for c in cand_list if isinstance(c, Mapping)),
            reconciliation_outcome=str(raw["reconciliation_outcome"]) if raw.get("reconciliation_outcome") else None,
            matched_work_root=str(raw["matched_work_root"]) if raw.get("matched_work_root") else None,
            reconciliation_evidence=(
                dict(reconciliation_evidence)
                if isinstance(reconciliation_evidence, Mapping) else None
            ),
            writer_job_id=str(raw["writer_job_id"]) if raw.get("writer_job_id") else None,
            lane_status=str(raw["lane_status"]) if raw.get("lane_status") else None,
            lane_detail=str(raw["lane_detail"]) if raw.get("lane_detail") else None,
            uncovered_tokens=tuple(
                str(value) for value in (raw.get("uncovered_tokens") or ())
            ),
            gap_status=str(raw["gap_status"]) if raw.get("gap_status") else None,
            gap_detail=str(raw["gap_detail"]) if raw.get("gap_detail") else None,
            attention=str(raw["attention"]) if raw.get("attention") else None,
            layout_repair=(
                dict(layout_repair)
                if isinstance(layout_repair, Mapping) else None
            ),
            updated_at=str(raw.get("updated_at") or _now()),
        )


def create_work_units_from_candidates(
    candidates: Sequence[WorkCandidate],
    root_task_id: str,
    *,
    source_revision: int = 1,
) -> list[WorkUnitRecord]:
    """Convert boundary WorkCandidates into initial pending WorkUnitRecords."""
    now_str = _now()
    records: list[WorkUnitRecord] = []
    for cand in candidates:
        requires_content_expansion = bool(cand.requires_content_expansion)
        record = WorkUnitRecord(
            work_unit_id=cand.work_unit_id,
            root_task_id=root_task_id,
            boundary_key=cand.boundary_key,
            source_paths=cand.source_paths,
            source_revision=source_revision,
            role=cand.boundary_evidence.role.value,
            display_label=cand.display_label,
            claimed_seasons=tuple(sorted({
                int(value)
                for value in cand.claimed_seasons
                if isinstance(value, int) and not isinstance(value, bool) and value > 0
            })),
            requires_content_expansion=requires_content_expansion,
            media_context=cand.proposed_media_context,
            identity_status=(
                "uncertain" if requires_content_expansion else "pending"
            ),
            identity=None,
            candidate_identities=(),
            reconciliation_outcome=None,
            matched_work_root=None,
            reconciliation_evidence=None,
            writer_job_id=None,
            gap_status=None,
            gap_detail=None,
            attention=(
                "；".join(cand.boundary_evidence.reasons)
                if requires_content_expansion else None
            ),
            updated_at=now_str,
        )
        records.append(record)
    return records


# ---------------------------------------------------------------------------
# Atomic Persistence
# ---------------------------------------------------------------------------

def _records_path(state_dir: Path, root_task_id: str) -> Path:
    return state_dir / f"work_units_{root_task_id}.json"


def save_work_unit_records(
    state_dir: Path,
    root_task_id: str,
    records: Sequence[WorkUnitRecord],
) -> None:
    """Atomically persist work unit records for a root task."""
    state_dir.mkdir(parents=True, exist_ok=True)
    target = _records_path(state_dir, root_task_id)
    payload = json.dumps(
        [r.as_dict() for r in records],
        indent=2,
        ensure_ascii=False,
    )
    with tempfile.NamedTemporaryFile("w", dir=state_dir, delete=False, encoding="utf-8") as tmp:
        tmp.write(payload)
        tmp.flush()
        tmp_path = Path(tmp.name)
    tmp_path.replace(target)


def load_work_unit_records(
    state_dir: Path,
    root_task_id: str,
) -> list[WorkUnitRecord]:
    """Load persisted work unit records for a root task; return empty if absent."""
    path = _records_path(state_dir, root_task_id)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [WorkUnitRecord.from_dict(item) for item in data if isinstance(item, Mapping)]
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return []


__all__ = [
    "EpisodePattern",
    "extract_episode_pattern",
    "physical_special_marker_evidence",
    "IdentityEvidence",
    "extract_identity_evidence",
    "WorkUnitRecord",
    "create_work_units_from_candidates",
    "save_work_unit_records",
    "load_work_unit_records",
]
