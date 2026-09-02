"""Focused regressions for the disc-expansion mapping proof.

Covers the order-preserving duration DP (including its bonus-playlist skip
semantics), the scope derivation that consumes it, and the operator ruling
lane that resolves scopes whose discs declare no readable episode order.
All fixtures are pure data — no filesystem, no network.
"""

from __future__ import annotations

import pytest

from engine.scrapeflow import disc_expansion as de


def _roster(minutes: list[int], *, season: int = 1) -> de.SeasonEpisodeRoster:
    return de.SeasonEpisodeRoster(
        season=season,
        episodes=tuple(
            (index + 1, value) for index, value in enumerate(minutes)
        ),
    )


def _candidate(
    *,
    duration_seconds: float,
    playlist: str = "/BDMV/PLAYLIST/00051.mpls",
    image: str = "memory://disc1.iso",
    disc_ordinal: int | None = 1,
    playlist_ordinal: int | None = None,
) -> de.PlaylistCandidate:
    if playlist_ordinal is None:
        playlist_ordinal = int(playlist.rsplit("/", 1)[-1].split(".")[0])
    return de.PlaylistCandidate(
        image_path=image,
        image_size=25_000_000_000,
        image_version="v1",
        playlist_inner_path=playlist,
        clip_inner_path="/BDMV/STREAM/00005.m2ts",
        clip_size=20_000_000_000,
        duration_seconds=duration_seconds,
        disc_ordinal=disc_ordinal,
        playlist_ordinal=playlist_ordinal,
    )


class TestOrderPreservingAssignment:
    def test_exact_bijection_still_proves(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [61 * 60, 59 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment == [1, 2]

    def test_bonus_playlist_above_threshold_is_skipped(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [59 * 60, 61 * 60, 20 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment == [1, 2, None]

    def test_leading_bonus_playlist_is_skipped(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [20 * 60, 59 * 60, 61 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment == [None, 1, 2]

    def test_two_skip_patterns_are_ambiguous(self) -> None:
        # Both (59, 61, skip) and (skip, 59, 61) cover the roster.
        assignment = de._unique_order_preserving_assignment(
            [59 * 60, 59 * 60, 61 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment is None

    def test_too_few_playlists_fail_closed(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [59 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment is None

    def test_unpublished_runtime_fails_closed(self) -> None:
        roster = de.SeasonEpisodeRoster(
            season=1, episodes=((1, 59), (2, None))
        )
        assignment = de._unique_order_preserving_assignment(
            [59 * 60, 61 * 60],
            roster.episodes,
            tolerance_seconds=120.0,
        )
        assert assignment is None

    def test_out_of_tolerance_playlist_fails_closed(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [59 * 60, 90 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment is None


class TestDeriveScopeExpansion:
    def test_skipped_bonus_playlist_is_reported(self) -> None:
        plan = de.derive_scope_expansion(
            scope_path="/source/season-1",
            season=1,
            roster=_roster([59, 61]),
            candidates=[
                _candidate(
                    duration_seconds=59 * 60,
                    playlist="/BDMV/PLAYLIST/00051.mpls",
                ),
                _candidate(
                    duration_seconds=61 * 60,
                    playlist="/BDMV/PLAYLIST/00052.mpls",
                ),
                _candidate(
                    duration_seconds=20 * 60,
                    playlist="/BDMV/PLAYLIST/00204.mpls",
                ),
            ],
            staging_root="/staging/root",
            work_name="作品",
        )
        assert plan.proven
        assert plan.basis == "duration-dp"
        assert plan.skipped_playlists == ("/BDMV/PLAYLIST/00204.mpls",)
        assert [m.episode for m in plan.mappings] == [1, 2]
        assert plan.mappings[0].target_path == (
            "/staging/root/season-1/Season 01/作品 - S01E01.mkv"
        )


def _s03_style_candidates() -> list[de.PlaylistCandidate]:
    return [
        _candidate(
            duration_seconds=3_520,
            playlist="/BDMV/PLAYLIST/00051.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_317,
            playlist="/BDMV/PLAYLIST/00057.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_189,
            playlist="/BDMV/PLAYLIST/00058.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_086,
            playlist="/BDMV/PLAYLIST/00054.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_370,
            playlist="/BDMV/PLAYLIST/00055.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_267,
            playlist="/BDMV/PLAYLIST/00056.mpls",
            image="memory://s03d1.iso",
        ),
    ]


def _s03_style_ruling() -> de.ScopeMappingRuling:
    return de.ScopeMappingRuling.from_mapping(
        {
            "scope_path": "/source/season-3",
            "season": 3,
            "assignments": [
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00051.mpls",
                    "episode": 1,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00057.mpls",
                    "episode": 2,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00058.mpls",
                    "episode": 3,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00054.mpls",
                    "episode": 4,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00055.mpls",
                    "episode": 5,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00056.mpls",
                    "episode": 6,
                },
            ],
            "operator": "operator",
            "note": "fixture",
            "filed_at": "2026-09-02T00:00:00+08:00",
        }
    )


class TestScopeMappingRuling:
    def test_happy_path_applies_ruling(self) -> None:
        plan = de.apply_scope_mapping_ruling(
            scope_path="/source/season-3",
            season=3,
            roster=_roster([58, 55, 53, 51, 56, 54], season=3),
            candidates=_s03_style_candidates(),
            ruling=_s03_style_ruling(),
            staging_root="/staging/root",
            work_name="作品",
        )
        assert plan.proven
        assert plan.basis == "operator-ruling"
        assert [m.episode for m in plan.mappings] == [1, 2, 3, 4, 5, 6]
        assert plan.mappings[1].candidate.playlist_inner_path == (
            "/BDMV/PLAYLIST/00057.mpls"
        )
        assert plan.mappings[1].target_path == (
            "/staging/root/season-3/Season 03/作品 - S03E02.mkv"
        )

    def test_wrong_scope_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="别的来源范围"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/other",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=_s03_style_candidates(),
                ruling=_s03_style_ruling(),
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_playlist_set_mismatch_is_rejected(self) -> None:
        candidates = _s03_style_candidates()[:-1]
        with pytest.raises(ValueError, match="集合与镜像候选不一致"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=candidates,
                ruling=_s03_style_ruling(),
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_episode_set_mismatch_is_rejected(self) -> None:
        ruling = _s03_style_ruling()
        object.__setattr__(
            ruling,
            "assignments",
            ruling.assignments[:-1]
            + (
                ("memory://s03d1.iso", "/BDMV/PLAYLIST/00056.mpls", 7),
            ),
        )
        with pytest.raises(ValueError, match="集号集合"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=_s03_style_candidates(),
                ruling=ruling,
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_duplicate_episode_is_rejected(self) -> None:
        ruling = _s03_style_ruling()
        object.__setattr__(
            ruling,
            "assignments",
            ruling.assignments[:-1]
            + (
                ("memory://s03d1.iso", "/BDMV/PLAYLIST/00056.mpls", 5),
            ),
        )
        with pytest.raises(ValueError, match="同一集"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=_s03_style_candidates(),
                ruling=ruling,
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_duration_contradiction_is_rejected(self) -> None:
        # Swapping E05/E06 keeps the playlist and episode sets intact but
        # puts the 3370s playlist on the 54-minute episode: +130s.
        ruling = _s03_style_ruling()
        object.__setattr__(
            ruling,
            "assignments",
            ruling.assignments[:4]
            + (
                ("memory://s03d1.iso", "/BDMV/PLAYLIST/00055.mpls", 6),
                ("memory://s03d1.iso", "/BDMV/PLAYLIST/00056.mpls", 5),
            ),
        )
        with pytest.raises(ValueError, match="与时长矛盾"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=_s03_style_candidates(),
                ruling=ruling,
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_from_mapping_requires_provenance(self) -> None:
        with pytest.raises(ValueError, match="operator"):
            de.ScopeMappingRuling.from_mapping(
                {
                    "scope_path": "/source/season-3",
                    "season": 3,
                    "assignments": [
                        {
                            "image_path": "memory://s03d1.iso",
                            "playlist_inner_path": "/BDMV/PLAYLIST/00051.mpls",
                            "episode": 1,
                        }
                    ],
                    "note": "n",
                    "filed_at": "t",
                }
            )
