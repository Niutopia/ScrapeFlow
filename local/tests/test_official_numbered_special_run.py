"""Tests for the official title-embedded ordinal special-run alignment."""

from __future__ import annotations

import re
import unittest

from engine.scrapeflow.core import (
    EpisodeKey,
    _map_explicit_special_release_runs,
    _map_unnumbered_specials,
    _official_numbered_title_special_run,
    _official_special_title_ordinal,
)


class OfficialNumberedTitleSpecialRunTests(unittest.TestCase):
    def test_title_ordinal_extracts_and_rejects_row_position_placeholders(self) -> None:
        self.assertEqual(_official_special_title_ordinal("迷你动画「〇〇」第1话：启程"), 1)
        self.assertEqual(_official_special_title_ordinal("OO Magic Episode 12: Road"), 12)
        # An untranslated placeholder whose entire title is the row position
        # restates the episode number; it is not mini-series evidence.
        self.assertIsNone(_official_special_title_ordinal("第 12 集"))
        self.assertIsNone(_official_special_title_ordinal("Episode 5"))
        self.assertIsNone(_official_special_title_ordinal("旅途的记忆"))

    def test_ordinal_run_aligns_across_interleaved_unnumbered_rows(self) -> None:
        variants = {
            1: ["第1话 迷你动画 启程"],
            2: ["第2话 迷你动画 山道"],
            3: ["第3话 迷你动画 归途"],
            5: ["特别篇"],
            6: ["第4话 迷你动画 旅途"],
        }
        self.assertEqual(
            _official_numbered_title_special_run(variants, run_length=4),
            (1, 2, 3, 6),
        )

    def test_ordinal_run_requires_complete_ordinals_and_unique_alignment(self) -> None:
        # The zh-TW placeholder for one row must not collide with another
        # row's real ``第2话`` title; the placeholder is rejected first, so
        # the real numbered row supplies the ordinal.
        variants = {
            1: ["第1话 迷你动画"],
            2: ["第 2 集"],
            5: ["第2话 迷你动画"],
        }
        self.assertEqual(
            _official_numbered_title_special_run(variants, run_length=2),
            (1, 5),
        )
        # Incomplete official ordinals stay fail-closed.
        self.assertIsNone(
            _official_numbered_title_special_run(
                {1: ["第1话"], 3: ["第3话"]}, run_length=2,
            )
        )
        # Two official rows claiming the same ordinal are ambiguous.
        self.assertIsNone(
            _official_numbered_title_special_run(
                {1: ["第1话 甲"], 2: ["第1话 乙"], 3: ["第2话 丙"]},
                run_length=2,
            )
        )

    def test_special_release_mapper_aligns_reset_run_by_official_ordinals(self) -> None:
        source = "/incoming/Show"
        items = [
            {
                "name": f"Mini Anime [{number:02d}].mkv",
                "full_path": source + f"/SPs/Mini Anime [{number:02d}].mkv",
            }
            for number in (1, 2)
        ]
        variants = {
            1: ["第1话 迷你动画 启程"],
            2: ["第2话 迷你动画 山道"],
            4: ["感谢节目"],
        }
        warnings = _map_explicit_special_release_runs(
            items, variants, {1: "2020-02-01", 2: "2020-02-08", 4: "2020-03-01"},
        )
        self.assertEqual(
            [item.get("_episode_key_override") for item in items],
            [1, 2],
        )
        self.assertTrue(
            all(item.get("_episode_kind_override") == "special" for item in items)
        )
        self.assertEqual(len(warnings), 1)
        self.assertIn("第N话", warnings[0])
        self.assertIn("SP01–SP02", warnings[0])

        # Without embedded ordinals the mapper leaves the run unmapped.
        plain = [
            {"name": "Mini Anime 01.mkv", "full_path": source + "/SPs/Mini Anime 01.mkv"},
            {"name": "Mini Anime 02.mkv", "full_path": source + "/SPs/Mini Anime 02.mkv"},
        ]
        self.assertEqual(
            _map_explicit_special_release_runs(
                plain,
                {1: ["迷你动画 启程"], 2: ["迷你动画 山道"]},
                {1: "2020-02-01", 2: "2020-02-08"},
            ),
            [],
        )
        self.assertNotIn("_episode_key_override", plain[0])


class SameMarkerOvaRunTimelineTests(unittest.TestCase):
    """A same-marker OVA run splits the official window by release order."""

    @staticmethod
    def _files(source: str, names: list[str]) -> list[dict[str, object]]:
        return [
            {"name": name, "full_path": f"{source}/{name}"} for name in names
        ]

    def test_same_marker_run_splits_window_by_release_ordinal(self) -> None:
        # After a complete 11-episode season the release ships two extra
        # videos under one marker: [12(OVA)] and [13(OVA)].  The marker
        # spellings (1 bucket) cannot pair one-to-one with the two official
        # window slots, so the run must split by release ordinal.
        source = "/incoming/Show 3"
        regular = [
            f"[Grp] Show 3 [{number:02d}][2160p].mkv" for number in range(1, 12)
        ]
        ova = [
            "[Grp] Show 3 [12(OVA)][2160p].mkv",
            "[Grp] Show 3 [13(OVA)][2160p].mkv",
        ]
        files = self._files(source, regular + ova)
        groups = {
            EpisodeKey("regular", number): [files[index]]
            for index, number in enumerate(range(1, 12))
        }
        special_titles = {
            EpisodeKey("special", 3): "『Show 3』OVA1「红流」",
            EpisodeKey("special", 4): "『Show 3』OVA2「冒牌货」",
        }
        warnings = _map_unnumbered_specials(
            files,
            groups,
            special_titles,
            "Show 3",
            season=3,
            special_season_candidates={
                3: [EpisodeKey("special", 3), EpisodeKey("special", 4)],
            },
        )
        self.assertEqual(
            [item["name"] for item in groups[EpisodeKey("special", 3)]],
            ["[Grp] Show 3 [12(OVA)][2160p].mkv"],
        )
        self.assertEqual(
            [item["name"] for item in groups[EpisodeKey("special", 4)]],
            ["[Grp] Show 3 [13(OVA)][2160p].mkv"],
        )
        self.assertEqual(len(warnings), 2)
        self.assertIn("按发行序自动映射为 SP03", warnings[0])
        self.assertIn("按发行序自动映射为 SP04", warnings[1])

    def test_run_count_mismatch_stays_fail_closed(self) -> None:
        # Three pending videos but only two unused window slots: the run
        # neither pairs markers one-to-one nor exhausts the window, so
        # nothing is paired.  (One bucket against one slot stays with the
        # one-to-one branch, which legitimately treats the bucket as
        # duplicate versions of that slot.)
        source = "/incoming/Show 3"
        regular = [
            f"[Grp] Show 3 [{number:02d}][2160p].mkv" for number in range(1, 12)
        ]
        ova = [
            "[Grp] Show 3 [12(OVA)][2160p].mkv",
            "[Grp] Show 3 [13(OVA)][2160p].mkv",
            "[Grp] Show 3 [14(OVA)][2160p].mkv",
        ]
        files = self._files(source, regular + ova)
        groups = {
            EpisodeKey("regular", number): [files[index]]
            for index, number in enumerate(range(1, 12))
        }
        warnings = _map_unnumbered_specials(
            files,
            groups,
            {
                EpisodeKey("special", 3): "『Show 3』OVA1「红流」",
                EpisodeKey("special", 4): "『Show 3』OVA2「冒牌货」",
            },
            "Show 3",
            season=3,
            special_season_candidates={
                3: [EpisodeKey("special", 3), EpisodeKey("special", 4)],
            },
        )
        self.assertEqual(warnings, [])
        self.assertNotIn(EpisodeKey("special", 3), groups)
        self.assertNotIn(EpisodeKey("special", 4), groups)

    def test_ambiguous_ordinals_stay_fail_closed(self) -> None:
        # Without distinct release ordinals the pairing order is undefined,
        # so the run must stay unmapped instead of guessing.
        source = "/incoming/Show 3"
        regular = [
            f"[Grp] Show 3 [{number:02d}][2160p].mkv" for number in range(1, 12)
        ]
        ova = [
            "[Grp] Show 3 (OVA A)[2160p].mkv",
            "[Grp] Show 3 (OVA B)[2160p].mkv",
        ]
        files = self._files(source, regular + ova)
        groups = {
            EpisodeKey("regular", number): [files[index]]
            for index, number in enumerate(range(1, 12))
        }
        warnings = _map_unnumbered_specials(
            files,
            groups,
            {
                EpisodeKey("special", 3): "『Show 3』OVA1「红流」",
                EpisodeKey("special", 4): "『Show 3』OVA2「冒牌货」",
            },
            "Show 3",
            season=3,
            special_season_candidates={
                3: [EpisodeKey("special", 3), EpisodeKey("special", 4)],
            },
        )
        self.assertEqual(warnings, [])
        self.assertNotIn(EpisodeKey("special", 3), groups)
        self.assertNotIn(EpisodeKey("special", 4), groups)


if __name__ == "__main__":
    unittest.main()


class SuffixOavOnAirVersionTests(unittest.TestCase):
    """A ``[N OAV]`` unnumbered-special bucket entry can prove On-Air Version."""

    @staticmethod
    def _files(source: str, names: list[str]) -> list[dict[str, object]]:
        return [
            {"name": name, "full_path": f"{source}/{name}"} for name in names
        ]

    def test_unnumbered_oav_bucket_entry_is_claimed_as_on_air_version(self) -> None:
        # The episode parser routes ``[13 OAV]`` to the unnumbered ``special
        # 0`` bucket (its ordinal deliberately never becomes special 13), so
        # the On-Air disambiguation must also claim matching members from
        # that bucket.  With the complete boundary evidence — official season
        # ends at E13, a plain E13 video, and E14 as the separately numbered
        # extra — the OAV is the E13 On-Air Version, not a Season 00 special.
        from engine.scrapeflow.core import _remap_suffix_oav_on_air_versions

        source = "/incoming/Show"
        names = [
            f"[Grp] Show [{number:02d}][1080p].mkv" for number in range(1, 13)
        ] + [
            "[Grp] Show [13 OAV][1080p].mkv",
            "[Grp] Show [13][1080p].mkv",
            "[Grp] Show [14][1080p].mkv",
        ]
        files = self._files(source, names)
        groups: dict = {}
        for item in files:
            key = (
                EpisodeKey("special", 0)
                if "OAV" in str(item["name"])
                else EpisodeKey("regular", int(re.search(r"\[(\d+)", str(item["name"])).group(1)))
            )
            groups.setdefault(key, []).append(item)
        warnings = _remap_suffix_oav_on_air_versions(groups, {1: 13})
        self.assertEqual(len(warnings), 1)
        self.assertIn("On-Air Version", warnings[0])
        on_air = groups[EpisodeKey("regular", 13)]
        self.assertEqual(
            sorted(str(item["name"]) for item in on_air),
            ["[Grp] Show [13 OAV][1080p].mkv", "[Grp] Show [13][1080p].mkv"],
        )
        claimed = [
            item for item in on_air
            if str(item["name"]) == "[Grp] Show [13 OAV][1080p].mkv"
        ]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0].get("_edition_override"), "On-Air Version")
        self.assertEqual(claimed[0].get("_episode_kind_override"), "regular")
        self.assertEqual(claimed[0].get("_episode_key_override"), 13)
        # The unnumbered bucket is gone once every member was claimed.
        self.assertNotIn(EpisodeKey("special", 0), groups)

    def test_unnumbered_bucket_keeps_non_matching_members(self) -> None:
        # Only the members whose own name carries the ``N OAV`` suffix are
        # claimed from the shared unnumbered bucket; an unrelated generic
        # ``OVA`` file stays behind.
        from engine.scrapeflow.core import _remap_suffix_oav_on_air_versions

        source = "/incoming/Show"
        names = [
            f"[Grp] Show [{number:02d}][1080p].mkv" for number in range(1, 13)
        ] + [
            "[Grp] Show [13 OAV][1080p].mkv",
            "[Grp] Show [13][1080p].mkv",
            "[Grp] Show [14][1080p].mkv",
            "[Grp] Show OVA[1080p].mkv",
        ]
        files = self._files(source, names)
        groups: dict = {}
        for item in files:
            name = str(item["name"])
            if "OVA" in name and "OAV" not in name:
                key = EpisodeKey("special", 0)
            elif "OAV" in name:
                key = EpisodeKey("special", 0)
            else:
                key = EpisodeKey("regular", int(re.search(r"\[(\d+)", name).group(1)))
            groups.setdefault(key, []).append(item)
        warnings = _remap_suffix_oav_on_air_versions(groups, {1: 13})
        self.assertEqual(len(warnings), 1)
        remaining = [str(i["name"]) for i in groups[EpisodeKey("special", 0)]]
        self.assertEqual(remaining, ["[Grp] Show OVA[1080p].mkv"])

    def test_boundary_without_plain_video_stays_fail_closed(self) -> None:
        # The three-way boundary is still required: without a plain E13 the
        # OAV bucket member is not claimed.
        from engine.scrapeflow.core import _remap_suffix_oav_on_air_versions

        source = "/incoming/Show"
        names = [
            f"[Grp] Show [{number:02d}][1080p].mkv" for number in range(1, 13)
        ] + [
            "[Grp] Show [13 OAV][1080p].mkv",
            "[Grp] Show [14][1080p].mkv",
        ]
        files = self._files(source, names)
        groups: dict = {}
        for item in files:
            key = (
                EpisodeKey("special", 0)
                if "OAV" in str(item["name"])
                else EpisodeKey("regular", int(re.search(r"\[(\d+)", str(item["name"])).group(1)))
            )
            groups.setdefault(key, []).append(item)
        warnings = _remap_suffix_oav_on_air_versions(groups, {1: 13})
        self.assertEqual(warnings, [])
        self.assertIn(EpisodeKey("special", 0), groups)
        self.assertNotIn(EpisodeKey("regular", 13), groups)
