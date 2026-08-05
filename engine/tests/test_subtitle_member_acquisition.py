import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

from engine.scrapeflow.quark_fast_save_bridge import (
    QuarkFastSaveBridge, QuarkMagnetOfflineBridge,
)
from engine.scrapeflow.subtitle_member_acquisition import (
    bind_source_manifest, build_search_batches, build_verified_cache_selection,
    materialize_verified_members,
    fetch_torrent_subtitle_member_after_cloud_exhaustion,
    plan_subtitle_member_acquisition, quark_bridge_selection,
    quark_magnet_bridge_selection, validate_source_manifest,
)
from engine.tools.subtitle_executor import canonical_digest


def chinese_payload():
    return "\n".join(
        f"Dialogue: 0,0:00:0{i}.00,0:00:01.00,Default,,0,0,0,,这是第{i}条简体中文字幕。"
        for i in range(8)
    ).encode()


def request_fixture():
    request = {
        "request_id": "req1", "video_path": "/quark/影视/番剧/My Show/Season 01/My Show - S01E02.mkv",
        "target_root": "/quark/影视/番剧/My Show", "title": "My Show", "media_type": "tv",
        "season": 1, "episodes": [2], "lane": "ensure_external_zh_CN",
    }
    return {
        "schema_version": 1, "kind": "subtitle_requests", "request_sha256": "r",
        "requests": [request],
    }, {
        "selection_sha256": "s", "failures": [{
            "request_id": "req1", "status": "no_verified_zh_CN_candidate",
        }],
    }


def tv_request(**overrides):
    request = {
        "request_id": "req-anime",
        "video_path": "/quark/影视/番剧/暗杀教室/Season 01/暗杀教室 - S01E13.mkv",
        "target_root": "/quark/影视/番剧/暗杀教室",
        "title": "暗杀教室", "media_type": "tv",
        "season": 1, "episodes": [13], "lane": "ensure_external_zh_CN",
    }
    request.update(overrides)
    return request


def planning_fixture(request):
    return {
        "schema_version": 1, "kind": "subtitle_requests", "request_sha256": "r",
        "requests": [request],
    }, {
        "selection_sha256": "s", "failures": [{
            "request_id": request["request_id"], "status": "no_verified_zh_CN_candidate",
        }],
    }


def plan_with(request, release_name, video_path, *, extra_videos=()):
    """Plan one request against one release-style source manifest."""
    payload = chinese_payload()
    files = [
        {"path": video_path, "size": 1000, "file_id": "v"},
        {"path": video_path[:-4] + ".zh-CN.ass", "size": len(payload), "file_id": "s"},
    ]
    files.extend(
        {"path": video, "size": 1000, "file_id": f"extra{i}"}
        for i, video in enumerate(extra_videos)
    )
    manifest = bind_source_manifest(
        provider="quark_share", locator="quark_share:anime-plan",
        release_name=release_name, search_request_ids=[request["request_id"]],
        acquisition={"share_id": "anime-plan"}, files=files,
    )
    return plan_subtitle_member_acquisition(*planning_fixture(request), [manifest])


class PairedReleaseIdentityTests(unittest.TestCase):
    """Release-style source names ([NN]/EP NN/E NN/bare) pair only with proof."""

    def test_release_bracket_episode_matches_with_proven_season(self):
        # 暗杀教室 S1 [13]：发布名携带 S01 凭证 → 成功
        plan = plan_with(tv_request(), "[ANi] 暗杀教室 S01 [1080P]", "pack/[ANi] 暗杀教室 - [13].mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 1)
        # 完全没有季凭证 → 禁止宽松猜测 → 不匹配
        plan = plan_with(tv_request(), "[ANi] 暗杀教室 01-13 [1080P]", "pack/[ANi] 暗杀教室 - [13].mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 0)
        self.assertEqual(plan["unresolved"][0]["status"], "retryable_search_required")

    def test_single_season_folder_proves_season_without_release_marker(self):
        request = tv_request(
            episodes=[3],
            video_path="/quark/影视/番剧/暗杀教室/Season 01/暗杀教室 - S01E03.mkv",
        )
        plan = plan_with(request, "[ANi] 暗杀教室 01-13 [1080P]", "pack/Season 01/03.mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 1)

    def test_manifest_scope_single_season_consensus(self):
        request = tv_request(
            episodes=[3],
            video_path="/quark/影视/番剧/暗杀教室/Season 01/暗杀教室 - S01E03.mkv",
        )
        # 同一 manifest 所有视频同属 Season 01 → [03] 可证明为第 1 季
        plan = plan_with(
            request, "[ANi] 暗杀教室 01-13 [1080P]", "pack/[ANi] 暗杀教室 - [03].mkv",
            extra_videos=["pack/Season 01/04.mkv"],
        )
        self.assertEqual(plan["summary"]["planned_requests"], 1)
        # manifest 跨两季 → 季证明缺失 → 不匹配
        plan = plan_with(
            request, "[ANi] 暗杀教室 01-13 [1080P]", "pack/[ANi] 暗杀教室 - [03].mkv",
            extra_videos=["pack/Season 01/04.mkv", "pack/Season 02/01.mkv"],
        )
        self.assertEqual(plan["summary"]["planned_requests"], 0)

    def test_chinese_season_marker_proves_season(self):
        request = tv_request(
            episodes=[3],
            video_path="/quark/影视/番剧/暗杀教室/Season 01/暗杀教室 - S01E03.mkv",
        )
        plan = plan_with(request, "[字幕组] 暗杀教室 第一季 [简繁]", "pack/[字幕组] 暗杀教室 - [03].mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 1)

    def test_season_zero_never_matches_plain_main_episode(self):
        request = tv_request(
            season=0, episodes=[3],
            video_path="/quark/影视/番剧/暗杀教室/Season 00/暗杀教室 - S00E03.mkv",
            target_root="/quark/影视/番剧/暗杀教室/Season 00",
        )
        # 普通 [03] 主集 → 绝不匹配 S00E03
        plan = plan_with(request, "[ANi] 暗杀教室 S01 [1080P]", "pack/[ANi] 暗杀教室 - [03].mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 0)
        # 明确特殊标记 + ordinal 一致 → 匹配
        for video in (
            "pack/暗杀教室 特典 - [03].mkv",
            "pack/暗杀教室 - [SP03].mkv",
            "pack/SP/03.mkv",
        ):
            plan = plan_with(request, "[ANi] 暗杀教室 S01 [1080P]", video)
            self.assertEqual(plan["summary"]["planned_requests"], 1, video)
        # 特殊内容反过来不能配主集请求
        main = tv_request(
            episodes=[3],
            video_path="/quark/影视/番剧/暗杀教室/Season 01/暗杀教室 - S01E03.mkv",
        )
        plan = plan_with(main, "[ANi] 暗杀教室 S01 [1080P]", "pack/暗杀教室 - [SP03].mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 0)

    def test_season_zero_sxxeyy_specials_still_pair(self):
        request = tv_request(
            season=0, episodes=[3],
            video_path="/quark/影视/番剧/暗杀教室/Season 00/暗杀教室 - S00E03.mkv",
            target_root="/quark/影视/番剧/暗杀教室/Season 00",
        )
        plan = plan_with(request, "[ANi] 暗杀教室 S00 [1080P]", "pack/暗杀教室 - S00E03.mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 1)

    def test_wrong_season_request_never_matches_release_ordinal(self):
        # S2 请求 vs 只含 S1 凭证的发布 → 不匹配
        request = tv_request(
            season=2, episodes=[3],
            video_path="/quark/影视/番剧/暗杀教室/Season 02/暗杀教室 - S02E03.mkv",
            target_root="/quark/影视/番剧/暗杀教室/Season 02",
        )
        plan = plan_with(request, "[ANi] 暗杀教室 S01 [1080P]", "pack/[ANi] 暗杀教室 - [03].mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 0)
        # 同季请求在同发布下仍可配对
        same = tv_request(
            episodes=[3],
            video_path="/quark/影视/番剧/暗杀教室/Season 01/暗杀教室 - S01E03.mkv",
        )
        plan = plan_with(same, "[ANi] 暗杀教室 S01 [1080P]", "pack/[ANi] 暗杀教室 - [03].mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 1)

    def test_cross_language_title_pairs_with_identity_credential(self):
        # 请求标题与目标库路径均为中文，别名提供英文身份凭证 → 可配对
        request = tv_request(aliases=["Assassination Classroom"])
        plan = plan_with(
            request, "[ANi] Assassination Classroom S01 [1080P]",
            "pack/Assassination Classroom - [13].mkv",
        )
        self.assertEqual(plan["summary"]["planned_requests"], 1)
        # 没有身份凭证 → 中英标题差异无法配对
        no_alias = dict(request); no_alias.pop("aliases")
        plan = plan_with(
            no_alias, "[ANi] Assassination Classroom S01 [1080P]",
            "pack/Assassination Classroom - [13].mkv",
        )
        self.assertEqual(plan["summary"]["planned_requests"], 0)

    def test_ep_and_bare_ordinal_forms_match_with_season_proof(self):
        request = tv_request(
            episodes=[3],
            video_path="/quark/影视/番剧/暗杀教室/Season 01/暗杀教室 - S01E03.mkv",
        )
        for video in (
            "pack/暗杀教室 EP 03.mkv",
            "pack/暗杀教室 EP03.mkv",
            "pack/暗杀教室 E 03.mkv",
            "pack/暗杀教室 E03.mkv",
            "pack/暗杀教室 03 [1080P].mkv",
        ):
            plan = plan_with(request, "[ANi] 暗杀教室 S01 [1080P]", video)
            self.assertEqual(plan["summary"]["planned_requests"], 1, video)
        # 无季凭证的裸集数 → 失败
        plan = plan_with(request, "[ANi] 暗杀教室 01-13 [1080P]", "pack/暗杀教室 03 [1080P].mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 0)

    def test_multi_episode_request_matches_any_proven_ordinal(self):
        plan = plan_with(
            tv_request(episodes=[13, 14]), "[ANi] 暗杀教室 S01 [1080P]",
            "pack/[ANi] 暗杀教室 - [14].mkv",
        )
        self.assertEqual(plan["summary"]["planned_requests"], 1)

    def test_ambiguous_ordinal_fails_closed(self):
        for video in ("pack/暗杀教室 - [13] [12].mkv", "pack/暗杀教室 - 13-14.mkv"):
            plan = plan_with(tv_request(), "[ANi] 暗杀教室 S01 [1080P]", video)
            self.assertEqual(plan["summary"]["planned_requests"], 0, video)

    def test_sxxeyy_exact_matching_is_preserved(self):
        plan = plan_with(tv_request(), "[ANi] 暗杀教室 S01 01-13 [1080P]", "pack/暗杀教室 - S01E13.mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 1)
        wrong = tv_request(
            season=2, episodes=[13],
            video_path="/quark/影视/番剧/暗杀教室/Season 02/暗杀教室 - S02E13.mkv",
            target_root="/quark/影视/番剧/暗杀教室/Season 02",
        )
        plan = plan_with(wrong, "[ANi] 暗杀教室 S01 01-13 [1080P]", "pack/暗杀教室 - S01E13.mkv")
        self.assertEqual(plan["summary"]["planned_requests"], 0)


class SubtitleMemberAcquisitionTests(unittest.TestCase):
    def test_search_batch_is_subtitle_only_and_grouped(self):
        requests, selection = request_fixture()
        result = build_search_batches(requests, selection)
        self.assertEqual(result["summary"], {
            "unmatched_requests": 1, "search_required_requests": 1,
            "ambiguity_resolution_requests": 0,
            "verification_only_requests": 0, "batches": 1,
        })
        policy = result["batches"][0]["member_policy"]
        self.assertFalse(policy["include_video"])
        self.assertTrue(policy["video_members_are_metadata_only"])

    def test_verification_lane_never_enters_search_or_acquisition(self):
        request = tv_request(
            request_id="pending",
            lane="subtitle_verification",
            video_path="/quark/影视/番剧/暗杀教室/Season 01/暗杀教室 - S01E13.mkv",
        )
        requests, selection = planning_fixture(request)
        search = build_search_batches(requests, selection)
        self.assertFalse(search["batches"])
        self.assertEqual(search["summary"]["search_required_requests"], 0)
        self.assertEqual(search["summary"]["verification_only_requests"], 1)

        payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="quark_share", locator="quark_share:pending",
            release_name="暗杀教室 S01E13", search_request_ids=["pending"],
            acquisition={"share_id": "pending"}, files=[
                {"path": "暗杀教室 - S01E13.mkv", "size": 1000, "file_id": "v"},
                {"path": "暗杀教室 - S01E13.ass", "size": len(payload), "file_id": "s"},
            ],
        )
        plan = plan_subtitle_member_acquisition(requests, selection, [manifest])
        self.assertFalse(plan["acquisitions"])
        self.assertFalse(plan["unresolved"])
        self.assertEqual(plan["summary"]["search_required_requests"], 0)
        self.assertEqual(plan["summary"]["verification_only_requests"], 1)

    def test_manifest_digest_and_complete_listing_are_mandatory(self):
        payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="quark_share", locator="quark_share:abc", release_name="My Show S01E02",
            search_request_ids=["req1"], acquisition={"share_id": "abc"}, files=[
                {"path": "My Show - S01E02.mkv", "size": 1000, "file_id": "video"},
                {"path": "My Show - S01E02.zh-CN.ass", "size": len(payload), "file_id": "subtitle"},
            ],
        )
        self.assertEqual(validate_source_manifest(manifest)["files"][1]["file_id"], "subtitle")
        tampered = dict(manifest); tampered["release_name"] = "Other"
        with self.assertRaisesRegex(ValueError, "digest"):
            validate_source_manifest(tampered)

    def test_quark_plan_selects_only_subtitle_file_id_and_bridge_accepts_it(self):
        requests, selection = request_fixture(); payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="quark_share", locator="quark_share:abc", release_name="My Show S01E02",
            search_request_ids=["req1"], acquisition={
                "share_id": "abc", "share_url": "https://pan.quark.cn/s/abc",
            }, files=[
                {"path": "pack/My Show - S01E02.mkv", "size": 1000, "file_id": "video"},
                {"path": "pack/My Show - S01E02.zh-CN.ass", "size": len(payload), "file_id": "subtitle"},
            ],
        )
        plan = plan_subtitle_member_acquisition(requests, selection, [manifest])
        self.assertEqual(plan["summary"]["planned_requests"], 1)
        self.assertEqual(plan["summary"]["video_members_selected"], 0)
        item = plan["acquisitions"][0]
        self.assertFalse(item["include_video"])
        self.assertEqual(item["transport"]["selected_file_ids"], ["subtitle"])
        self.assertFalse(item["transport"]["may_launch_or_restart_quark"])
        self.assertFalse(item["transport"]["allow_ui_activation"])
        bridge = quark_bridge_selection(item)
        dry = QuarkFastSaveBridge.dry_run(bridge, "/quark/影视/ScrapeFlow/字幕获取")
        self.assertEqual(dry["file_names"], ["My Show - S01E02.zh-CN.ass"])

    def test_torrent_plan_selects_only_subtitle_index_and_cloud_bridge_accepts_it(self):
        requests, selection = request_fixture(); payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="torrent", locator="torrent:https://example.invalid/a.torrent",
            release_name="My Show S01E02", search_request_ids=["req1"],
            acquisition={"infohash": "a" * 40, "torrent_url": "https://example.invalid/a.torrent"},
            files=[
                {"path": "pack/My Show - S01E02.mkv", "size": 1000, "torrent_index": 1},
                {"path": "pack/My Show - S01E02.ass", "size": len(payload), "torrent_index": 2},
            ],
        )
        item = plan_subtitle_member_acquisition(requests, selection, [manifest])["acquisitions"][0]
        self.assertEqual(item["transport"]["selected_torrent_indices"], [2])
        bridge = quark_magnet_bridge_selection(item)
        dry = QuarkMagnetOfflineBridge.dry_run(bridge, "/quark/影视/ScrapeFlow/字幕获取")
        self.assertEqual([row["torrent_index"] for row in dry["expected_files"]], [2])

    def test_local_torrent_fallback_requires_permanent_cloud_exhaustion(self):
        requests, selection = request_fixture(); payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="torrent", locator="torrent:https://example.invalid/a.torrent",
            release_name="My Show S01E02", search_request_ids=["req1"],
            acquisition={"infohash": "a" * 40, "torrent_url": "https://example.invalid/a.torrent"},
            files=[
                {"path": "pack/My Show - S01E02.mkv", "size": 1000, "torrent_index": 1},
                {"path": "pack/My Show - S01E02.ass", "size": len(payload), "torrent_index": 2},
            ],
        )
        item = plan_subtitle_member_acquisition(requests, selection, [manifest])["acquisitions"][0]
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "not permanently unlocked"):
                fetch_torrent_subtitle_member_after_cloud_exhaustion(
                    item, cloud_exhaustion_proof={}, workspace_root=Path(directory),
                )

    @mock.patch("engine.tools.replenishment_local_adapter._download_torrent")
    @mock.patch("engine.scrapeflow.subtitle_member_acquisition.subprocess.run")
    def test_local_torrent_fetch_selects_one_subtitle_index(self, run, download):
        requests, selection = request_fixture(); payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="torrent", locator="torrent:https://example.invalid/a.torrent",
            release_name="My Show S01E02", search_request_ids=["req1"],
            acquisition={"infohash": "a" * 40, "torrent_url": "https://example.invalid/a.torrent"},
            files=[
                {"path": "pack/My Show - S01E02.mkv", "size": 1000, "torrent_index": 1},
                {"path": "pack/My Show - S01E02.ass", "size": len(payload), "torrent_index": 2},
            ],
        )
        item = plan_subtitle_member_acquisition(requests, selection, [manifest])["acquisitions"][0]
        download.return_value = {
            "infohash": "a" * 40,
            "files": {1: {"path": "pack/My Show - S01E02.mkv", "size": 1000},
                      2: {"path": "pack/My Show - S01E02.ass", "size": len(payload)}},
        }
        def complete(command, **_kwargs):
            target = Path(next(value.split("=", 1)[1] for value in command if value.startswith("--dir=")))
            (target / "pack").mkdir(parents=True)
            (target / "pack/My Show - S01E02.ass").write_bytes(payload)
            return mock.Mock(returncode=0, stdout="")
        run.side_effect = complete
        with tempfile.TemporaryDirectory() as directory:
            fetched = fetch_torrent_subtitle_member_after_cloud_exhaustion(
                item, cloud_exhaustion_proof={
                    "provider": "quark_magnet", "permanent": True,
                    "search_complete": True, "attempts": 30, "minimum_attempts": 30,
                    "all_candidates_resource_failed_or_absent": True,
                }, workspace_root=Path(directory),
            )
        self.assertEqual(fetched, payload)
        self.assertIn("--select-file=2", run.call_args.args[0])

    def test_unpaired_or_wrong_identity_stays_retryable(self):
        requests, selection = request_fixture(); payload = chinese_payload()
        manifests = [bind_source_manifest(
            provider="quark_share", locator=f"quark_share:{name}", release_name=name,
            search_request_ids=["req1"], acquisition={"share_id": name}, files=files,
        ) for name, files in (
            ("unpaired", [{"path": "My Show - S01E02.ass", "size": len(payload), "file_id": "s"}]),
            ("wrong", [
                {"path": "My Show - S01E03.mkv", "size": 1000, "file_id": "v"},
                {"path": "My Show - S01E03.ass", "size": len(payload), "file_id": "s"},
            ]),
        )]
        plan = plan_subtitle_member_acquisition(requests, selection, manifests)
        self.assertEqual(plan["summary"]["planned_requests"], 0)
        self.assertEqual(plan["unresolved"][0]["status"], "retryable_search_required")

    def test_materialization_rechecks_language_digest_and_is_idempotent(self):
        requests, selection = request_fixture(); payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="quark_share", locator="quark_share:abc", release_name="My Show S01E02",
            search_request_ids=["req1"], acquisition={"share_id": "abc"}, files=[
                {"path": "My Show - S01E02.mkv", "size": 1000, "file_id": "v"},
                {"path": "My Show - S01E02.ass", "size": len(payload), "file_id": "s"},
            ],
        )
        plan = plan_subtitle_member_acquisition(requests, selection, [manifest])
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def fetch(item):
                calls.append(item)
                self.assertFalse(item["include_video"])
                return payload
            first = materialize_verified_members(
                plan, approved_plan_sha256=plan["plan_sha256"], fetch_member=fetch,
                cache_root=root / "cache", journal_path=root / "journal.json",
            )
            self.assertEqual(first["status"], "success")
            self.assertEqual(first["records"][0]["status"], "verified_zh_CN")
            materialize_verified_members(
                plan, approved_plan_sha256=plan["plan_sha256"], fetch_member=fetch,
                cache_root=root / "cache", journal_path=root / "journal.json",
            )
            self.assertEqual(len(calls), 1)

    def test_materialization_rejects_non_confirmed_lane_before_fetch_or_journal(self):
        requests, selection = request_fixture(); payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="quark_share", locator="quark_share:abc", release_name="My Show S01E02",
            search_request_ids=["req1"], acquisition={"share_id": "abc"}, files=[
                {"path": "My Show - S01E02.mkv", "size": 1000, "file_id": "v"},
                {"path": "My Show - S01E02.ass", "size": len(payload), "file_id": "s"},
            ],
        )
        plan = plan_subtitle_member_acquisition(requests, selection, [manifest])
        tampered_core = {
            key: plan[key] for key in (
                "schema_version", "kind", "requests_sha256", "source_selection_sha256",
                "acquisitions", "unresolved",
            )
        }
        tampered_core["acquisitions"] = [
            {**tampered_core["acquisitions"][0], "lane": "subtitle_verification"}
        ]
        tampered = {**tampered_core, "plan_sha256": canonical_digest(tampered_core)}
        with tempfile.TemporaryDirectory() as directory:
            journal_path = Path(directory) / "journal.json"
            with self.assertRaisesRegex(ValueError, "non-confirmed lane"):
                materialize_verified_members(
                    tampered, approved_plan_sha256=tampered["plan_sha256"],
                    fetch_member=lambda _item: self.fail("must not fetch"),
                    cache_root=Path(directory) / "cache", journal_path=journal_path,
                )
            self.assertFalse(journal_path.exists())

    def test_non_chinese_payload_is_retryable_and_never_cached(self):
        requests, selection = request_fixture(); payload = b"English subtitle line.\n" * 30
        manifest = bind_source_manifest(
            provider="quark_share", locator="quark_share:abc", release_name="My Show S01E02",
            search_request_ids=["req1"], acquisition={"share_id": "abc"}, files=[
                {"path": "My Show - S01E02.mkv", "size": 1000, "file_id": "v"},
                {"path": "My Show - S01E02.ass", "size": len(payload), "file_id": "s"},
            ],
        )
        plan = plan_subtitle_member_acquisition(requests, selection, [manifest])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            journal = materialize_verified_members(
                plan, approved_plan_sha256=plan["plan_sha256"], fetch_member=lambda _item: payload,
                cache_root=root / "cache", journal_path=root / "journal.json",
            )
            self.assertEqual(journal["status"], "retryable_incomplete")
            self.assertEqual(journal["records"][0]["status"], "retryable_not_verified_zh_CN")
            self.assertEqual(journal["records"][0]["content_witness_version"], 2)
            self.assertFalse((root / "cache").exists())

    def test_legacy_non_chinese_witness_is_revalidated_once(self):
        requests, selection = request_fixture(); payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="quark_share", locator="quark_share:abc", release_name="My Show S01E02",
            search_request_ids=["req1"], acquisition={"share_id": "abc"}, files=[
                {"path": "My Show - S01E02.mkv", "size": 1000, "file_id": "v"},
                {"path": "My Show - S01E02.ass", "size": len(payload), "file_id": "s"},
            ],
        )
        plan = plan_subtitle_member_acquisition(requests, selection, [manifest])
        calls = 0
        def fetch(_item):
            nonlocal calls
            calls += 1
            return payload
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); journal_path = root / "journal.json"
            journal_path.write_text(json.dumps({
                "schema_version": 1,
                "kind": "subtitle_member_acquisition_journal",
                "plan_sha256": plan["plan_sha256"],
                "status": "retryable_incomplete",
                "records": [{
                    "request_id": "req1",
                    "source_manifest_sha256": manifest["manifest_sha256"],
                    "member_path": "My Show - S01E02.ass",
                    "status": "retryable_not_verified_zh_CN",
                }],
            }), encoding="utf-8")
            result = materialize_verified_members(
                plan, approved_plan_sha256=plan["plan_sha256"], fetch_member=fetch,
                cache_root=root / "cache", journal_path=journal_path,
            )
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["records"][-1]["status"], "verified_zh_CN")
            self.assertEqual(result["records"][-1]["content_witness_version"], 2)
            materialize_verified_members(
                plan, approved_plan_sha256=plan["plan_sha256"], fetch_member=fetch,
                cache_root=root / "cache", journal_path=journal_path,
            )
            self.assertEqual(calls, 1)

    def test_transient_fetch_failure_retries_on_next_run(self):
        requests, selection = request_fixture(); payload = chinese_payload()
        manifest = bind_source_manifest(
            provider="quark_share", locator="quark_share:abc", release_name="My Show S01E02",
            search_request_ids=["req1"], acquisition={"share_id": "abc"}, files=[
                {"path": "My Show - S01E02.mkv", "size": 1000, "file_id": "v"},
                {"path": "My Show - S01E02.ass", "size": len(payload), "file_id": "s"},
            ],
        )
        plan = plan_subtitle_member_acquisition(requests, selection, [manifest])
        calls = 0
        def flaky(_item):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("temporary")
            return payload
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); journal_path = root / "journal.json"
            first = materialize_verified_members(
                plan, approved_plan_sha256=plan["plan_sha256"], fetch_member=flaky,
                cache_root=root / "cache", journal_path=journal_path,
            )
            self.assertEqual(first["status"], "retryable_incomplete")
            second = materialize_verified_members(
                plan, approved_plan_sha256=plan["plan_sha256"], fetch_member=flaky,
                cache_root=root / "cache", journal_path=journal_path,
            )
            self.assertEqual(second["status"], "success")
            self.assertEqual([row["status"] for row in second["records"]], [
                "retryable_fetch_failed", "verified_zh_CN",
            ])

    def test_verified_cache_reenters_create_only_selection_with_exact_video_pair(self):
        requests, _selection = request_fixture(); payload = chinese_payload()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); cache = root / "cache" / "req1" / "payload.ass"
            cache.parent.mkdir(parents=True); cache.write_bytes(payload)
            digest = __import__("hashlib").sha256(payload).hexdigest()
            prepared = build_verified_cache_selection(requests, {
                "records": [{
                    "request_id": "req1", "status": "verified_zh_CN",
                    "cache_path": str(cache), "payload_sha256": digest,
                    "payload_size": len(payload),
                }],
            }, cache_root=root / "cache")
        row = prepared["selection"]["selections"][0]
        self.assertEqual(row["candidate_source_kind"], "local_verified_cache")
        self.assertEqual(row["identity_method"], "manifest_paired_video_stem")
        self.assertTrue(row["target_path"].endswith(".zh-CN.ass"))

    def test_verified_cache_cannot_promote_verification_request(self):
        requests, _selection = request_fixture()
        requests["requests"][0]["lane"] = "subtitle_verification"
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "verification-only"):
                build_verified_cache_selection(requests, {
                    "records": [{
                        "request_id": "req1", "status": "verified_zh_CN",
                        "cache_path": str(Path(directory) / "missing.ass"),
                        "payload_sha256": "0" * 64, "payload_size": 1,
                    }],
                }, cache_root=Path(directory))

    def test_zero_acquisition_terminal_journal_still_obeys_pause_boundary(self):
        requests, selection = request_fixture()
        plan = plan_subtitle_member_acquisition(requests, selection, [])
        @contextmanager
        def paused():
            raise RuntimeError("global_pause_active")
            yield
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal.json"
            with self.assertRaisesRegex(RuntimeError, "global_pause_active"):
                materialize_verified_members(
                    plan, approved_plan_sha256=plan["plan_sha256"],
                    fetch_member=lambda _item: b"", cache_root=Path(directory) / "cache",
                    journal_path=journal, item_guard=paused,
                )
            self.assertFalse(journal.exists())


if __name__ == "__main__":
    unittest.main()
