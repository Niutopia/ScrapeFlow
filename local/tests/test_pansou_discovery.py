"""Tests for truthful first-tier PanSou discovery."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from engine.tools.replenishment_adapter.pansou import (
    PanSouDiscovery,
    quark_share_inspector,
)
from engine.tools.replenishment_adapter.search import ReplenishmentSearchService
from local.scrapeflow_api.replenishment import select_replenishment_candidates


def _request() -> dict[str, object]:
    return {
        "version": 2,
        "tier": "quark_share",
        "media": {
            "title": "Example Show",
            "aliases": ["Example Show"],
            "year": "2020",
            "tmdb_id": 123,
        },
        "gaps": [{
            "id": "S01E01",
            "kind": "missing_episode",
            "season": 1,
            "episode": 1,
            "episodes": [1],
        }],
        "query_groups": [{
            "season": 1,
            "episodes": [1],
            "token": "S01E01",
        }],
        "search_queries": ["Example Show S01E01"],
        "rules": {
            "require_title_identity": True,
            "require_name_coverage": True,
            "file_listing_restricts_claimed_coverage": True,
        },
    }


def _response(*links: str) -> dict[str, object]:
    rows = [
        {
            "message_id": str(index),
            "unique_id": f"fixture-{index}",
            "channel": "fixture-channel",
            "datetime": "2026-08-10T00:00:00Z",
            "title": "Example Show S01E01 1080p",
            "content": "",
            "links": [{
                "type": "quark",
                "url": link,
                "password": "",
                "datetime": "2026-08-10T00:00:00Z",
                "work_title": "Example Show S01E01 1080p",
            }],
        }
        for index, link in enumerate(links, start=1)
    ]
    return {
        "code": 0,
        "message": "success",
        "data": {
            "total": len(rows),
            "results": rows,
            "merged_by_type": {"quark": []},
        },
    }


class PanSouDiscoveryTests(unittest.TestCase):
    def _discovery(
        self,
        response,
        *,
        inspector=None,
        max_queries=4,
        max_links=64,
    ):
        calls: list[dict[str, object]] = []

        def transport(endpoint, payload, token, timeout):
            calls.append({
                "endpoint": endpoint,
                "payload": dict(payload),
                "token": token,
                "timeout": timeout,
            })
            if isinstance(response, BaseException):
                raise response
            return response

        discovery = PanSouDiscovery(
            enabled=True,
            url="http://pansou:8888",
            token="fixture-token",
            inspector=inspector or (lambda _pwd_id, _passcode: [{
                "file_id": "share-fid-1",
                "path": "Example.Show.S01E01.1080p.mkv",
                "size": 2_000_000,
            }]),
            transport=transport,
            timeout=10,
            max_queries=max_queries,
            max_links=max_links,
        )
        return discovery, calls

    def test_share_inspector_lazily_authenticates_uninitialised_alist_client(self) -> None:
        class FakeAList:
            token = None

            def __init__(self) -> None:
                self.login_calls = 0

            def login(self) -> str:
                self.login_calls += 1
                self.token = "fixture-token"
                return self.token

        class FakeBridge:
            def __init__(self) -> None:
                self.sessions: list[object] = []

            def inspect_share(self, session, *, pwd_id, passcode):  # noqa: ANN001
                self.sessions.append(session)
                self.pwd_id = pwd_id
                self.passcode = passcode
                return []

        client = FakeAList()
        bridge = FakeBridge()
        session = object()
        with patch(
            "engine.tools.replenishment_adapter.pansou.delegated_quark_session",
            return_value=session,
        ) as delegated:
            inspector = quark_share_inspector(
                client,
                "/quark/影视",
                bridge=bridge,
            )
            self.assertEqual(inspector("fixtureShare01", ""), [])

        self.assertEqual(client.login_calls, 1)
        delegated.assert_called_once_with(client, "/quark/影视")
        self.assertEqual(bridge.sessions, [session])
        self.assertEqual(bridge.pwd_id, "fixtureShare01")

    def test_official_response_becomes_exact_runnable_quark_share(self) -> None:
        discovery, calls = self._discovery(
            _response("https://pan.quark.cn/s/fixtureShare01"),
        )

        result = ReplenishmentSearchService(pansou=discovery).run(_request())
        selection = select_replenishment_candidates(
            _request(), result["candidates"], current_tier="quark_share",
        )

        self.assertEqual(result["completed_sources"], ["pansou"])
        self.assertTrue(result["search_complete"])
        self.assertFalse(result["search_complete_no_candidates"])
        self.assertEqual(result["unchecked_secondary_candidates"], 0)
        self.assertEqual(len(result["candidates"]), 1)
        candidate = result["candidates"][0]
        self.assertEqual(candidate["provider"], "quark_share")
        self.assertEqual(candidate["locator"], "quark_share:fixtureShare01")
        self.assertEqual(candidate["acquisition"]["kind"], "quark_fast_save")
        self.assertEqual(
            candidate["acquisition"]["file_id_by_gap"],
            {"S01E01": ["share-fid-1"]},
        )
        self.assertEqual(
            candidate["acquisition"]["file_path_by_id"],
            {"share-fid-1": "Example.Show.S01E01.1080p.mkv"},
        )
        self.assertEqual(selection["status"], "complete")
        self.assertEqual(len(selection["selections"]), 1)
        self.assertEqual(calls[0]["endpoint"], "http://pansou:8888/api/search")
        self.assertEqual(calls[0]["payload"]["res"], "all")
        self.assertEqual(calls[0]["payload"]["cloud_types"], ["quark"])

    def test_complete_zero_result_is_the_only_zero_candidate_proof(self) -> None:
        discovery, _calls = self._discovery(_response())

        result = discovery.run(_request())

        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["completed_sources"], ["pansou"])
        self.assertTrue(result["search_complete_no_candidates"])
        self.assertEqual(result["unchecked_secondary_candidates"], 0)

    def test_query_cursor_resumes_after_bounded_window(self) -> None:
        discovery, calls = self._discovery(_response(), max_queries=2)
        request = _request()
        with patch(
            "engine.tools.replenishment_adapter.pansou._impl._compact_dynamic_search_terms",
            return_value=["Example Show S01E01", "Example Show season", "Example Show"],
        ):
            first = discovery.run(request)
            self.assertFalse(first["search_complete_no_candidates"])
            self.assertEqual(
                first["source_telemetry"]["PanSou"]["query_cursor"]["offset"],
                2,
            )
            request["pansou_query_cursor"] = first["source_telemetry"]["PanSou"]["query_cursor"]
            second = discovery.run(request)

        self.assertTrue(second["search_complete_no_candidates"])
        self.assertEqual(second["unchecked_secondary_candidates"], 0)
        # The share-pack lane reorders deterministic terms: season/bare-show
        # queries precede per-episode queries because Quark share titles are
        # pack-level and episode coverage is proven by manifest inspection.
        self.assertEqual(
            [call["payload"]["kw"] for call in calls],
            ["Example Show season", "Example Show", "Example Show S01E01"],
        )

    def test_share_pack_lane_puts_broad_terms_before_episode_terms(self) -> None:
        discovery, calls = self._discovery(_response(), max_queries=12)
        with patch(
            "engine.tools.replenishment_adapter.pansou._impl._compact_dynamic_search_terms",
            return_value=[
                "Example Show S01E01", "Alias S01E02", "Example Show S01",
                "Example Show", "别名标题", "Alias Show",
            ],
        ):
            discovery.run(_request())

        self.assertEqual(
            [call["payload"]["kw"] for call in calls],
            ["Example Show S01", "Example Show", "别名标题", "Alias Show",
             "Example Show S01E01", "Alias S01E02"],
        )

    def test_documented_direct_search_response_is_accepted(self) -> None:
        documented_direct_response = _response(
            "https://pan.quark.cn/s/fixtureShare01",
        )["data"]
        discovery, _calls = self._discovery(documented_direct_response)

        result = discovery.run(_request())

        self.assertEqual(
            result["candidates"][0]["locator"],
            "quark_share:fixtureShare01",
        )

    def test_absent_merged_map_is_an_empty_result_not_a_protocol_failure(self) -> None:
        # Upstream drops ``merged_by_type`` when the cloud-type filter matched
        # nothing.  Treating that as malformed made every zero-Quark query an
        # infrastructure failure, which no exhaustion proof can survive, so
        # the whole first tier could never advance.
        payload = _response("https://pan.quark.cn/s/fixtureShare01")
        payload["data"].pop("merged_by_type")
        discovery, _calls = self._discovery(payload)

        result = discovery.run(_request())

        self.assertNotIn("failure_scope", result)
        self.assertEqual(
            result["source_telemetry"]["PanSou"]["infrastructure_failures"], 0,
        )
        self.assertTrue(result["search_complete"])
        self.assertEqual(len(result["candidates"]), 1)

    def test_result_row_without_links_is_skipped_rather_than_fatal(self) -> None:
        # A matched post carrying no link of the requested type is ordinary
        # filtered output; it must not poison the query it appears in.
        payload = _response("https://pan.quark.cn/s/fixtureShare01")
        payload["data"]["results"].append({
            "message_id": "99",
            "unique_id": "fixture-99",
            "channel": "fixture-channel",
            "datetime": "2026-08-10T00:00:00Z",
            "title": "求助 有没有这部",
            "content": "",
            "links": None,
        })
        payload["data"]["total"] = len(payload["data"]["results"])
        discovery, _calls = self._discovery(payload)

        result = discovery.run(_request())

        self.assertNotIn("failure_scope", result)
        self.assertEqual(result["unchecked_secondary_candidates"], 0)
        self.assertTrue(result["search_complete"])
        self.assertEqual(len(result["candidates"]), 1)

    def test_wrongly_typed_links_remain_a_protocol_failure(self) -> None:
        payload = _response("https://pan.quark.cn/s/fixtureShare01")
        payload["data"]["results"][0]["links"] = "not-a-list"
        discovery, _calls = self._discovery(payload)

        result = discovery.run(_request())

        self.assertEqual(result["failure_scope"], "infrastructure")
        self.assertFalse(result["search_complete"])

    def test_disabled_source_is_explicitly_incomplete(self) -> None:
        discovery = PanSouDiscovery(enabled=False, url="", inspector=None)

        result = discovery.run(_request())

        self.assertFalse(result["search_complete"])
        self.assertFalse(result["search_complete_no_candidates"])
        self.assertEqual(result["completed_sources"], [])
        self.assertEqual(result["failure_scope"], "infrastructure")
        self.assertEqual(
            result["source_telemetry"]["PanSou"]["status"], "incomplete",
        )

    def test_enabled_source_without_url_is_explicitly_incomplete(self) -> None:
        discovery = PanSouDiscovery(enabled=True, url="", inspector=lambda *_args: [])

        result = discovery.run(_request())

        self.assertFalse(result["search_complete"])
        self.assertFalse(result["search_complete_no_candidates"])
        self.assertEqual(result["failure_scope"], "infrastructure")
        self.assertFalse(result["source_telemetry"]["PanSou"]["configured"])

    def test_transport_failure_never_completes_pansou(self) -> None:
        discovery, _calls = self._discovery(TimeoutError("fixture timeout"))

        result = discovery.run(_request())

        self.assertFalse(result["search_complete"])
        self.assertFalse(result["search_complete_no_candidates"])
        self.assertEqual(result["completed_sources"], [])
        self.assertEqual(result["failure_scope"], "infrastructure")
        self.assertGreaterEqual(
            result["source_telemetry"]["PanSou"]["infrastructure_failures"],
            1,
        )

    def test_unchecked_link_cap_prevents_exhaustion_proof(self) -> None:
        discovery, _calls = self._discovery(
            _response(
                "https://pan.quark.cn/s/fixtureShare01",
                "https://pan.quark.cn/s/fixtureShare02",
            ),
            max_links=1,
        )

        result = discovery.run(_request())

        self.assertFalse(result["search_complete"])
        self.assertFalse(result["search_complete_no_candidates"])
        self.assertEqual(result["completed_sources"], [])
        self.assertEqual(result["unchecked_secondary_candidates"], 1)

    def test_scoped_reviewed_miss_is_skipped_before_link_cap(self) -> None:
        inspected: list[str] = []

        def inspector(pwd_id, _passcode):
            inspected.append(pwd_id)
            return [{
                "file_id": f"fid-{pwd_id}",
                "path": "Example.Show.S01E01.1080p.mkv",
                "size": 2_000_000,
            }]

        discovery, _calls = self._discovery(
            _response(
                "https://pan.quark.cn/s/fixtureShare01",
                "https://pan.quark.cn/s/fixtureShare02",
            ),
            inspector=inspector,
            max_links=1,
        )
        request = _request()
        request["reviewed_resource_miss_locators"] = [
            "quark_share:fixtureShare01",
        ]

        result = discovery.run(request)

        self.assertEqual(inspected, ["fixtureShare02"])
        self.assertEqual(
            result["candidates"][0]["locator"],
            "quark_share:fixtureShare02",
        )
        telemetry = result["source_telemetry"]["PanSou"]
        self.assertEqual(telemetry["previously_reviewed_miss_count"], 1)
        self.assertTrue(result["search_complete"])
        self.assertEqual(result["unchecked_secondary_candidates"], 0)

    def test_query_cap_cannot_become_a_complete_zero_candidate_proof(self) -> None:
        discovery, calls = self._discovery(
            _response(),
            max_queries=1,
        )

        result = discovery.run(_request())

        self.assertEqual(len(calls), 1)
        self.assertFalse(result["search_complete"])
        self.assertFalse(result["search_complete_no_candidates"])
        self.assertEqual(result["completed_sources"], [])
        self.assertGreater(result["unchecked_secondary_candidates"], 0)
        self.assertGreater(
            result["source_telemetry"]["PanSou"]["query_terms_unchecked"],
            0,
        )

    def test_previously_failed_share_cannot_become_zero_candidate_proof(self) -> None:
        discovery, _calls = self._discovery(
            _response("https://pan.quark.cn/s/fixtureShare01"),
        )
        request = _request()
        request["excluded_candidates"] = [{
            "provider": "quark_share",
            "locator": "quark_share:fixtureShare01",
        }]

        result = discovery.run(request)

        self.assertEqual(result["candidates"], [])
        self.assertFalse(result["search_complete"])
        self.assertFalse(result["search_complete_no_candidates"])
        self.assertGreater(result["unchecked_secondary_candidates"], 0)
        self.assertEqual(
            result["source_telemetry"]["PanSou"]["preexcluded_candidate_count"],
            1,
        )

    def test_url_passcode_is_used_for_read_only_inspection_and_candidate(self) -> None:
        inspected: list[tuple[str, str]] = []

        def inspector(pwd_id, passcode):
            inspected.append((pwd_id, passcode))
            return [{
                "file_id": "share-fid-1",
                "path": "Example.Show.S01E01.1080p.mkv",
                "size": 2_000_000,
            }]

        discovery, _calls = self._discovery(
            _response("https://pan.quark.cn/s/fixtureShare01?pwd=Ab_12"),
            inspector=inspector,
        )

        result = discovery.run(_request())

        self.assertEqual(inspected, [("fixtureShare01", "Ab_12")])
        self.assertEqual(
            result["candidates"][0]["acquisition"]["passcode"],
            "Ab_12",
        )

    def test_malformed_share_manifest_is_a_candidate_miss_not_infrastructure(self) -> None:
        discovery, _calls = self._discovery(
            _response("https://pan.quark.cn/s/fixtureShare01"),
            inspector=lambda _pwd_id, _passcode: "not-a-manifest",
        )

        result = discovery.run(_request())

        self.assertEqual(result["candidates"], [])
        self.assertTrue(result["search_complete"])
        self.assertTrue(result["search_complete_no_candidates"])
        self.assertNotIn("failure_scope", result)
        self.assertEqual(
            result["source_telemetry"]["PanSou"]["resource_failed_locators"],
            ["quark_share:fixtureShare01"],
        )
        self.assertEqual(
            result["source_telemetry"]["PanSou"]["reviewed_resource_miss_locators"],
            ["quark_share:fixtureShare01"],
        )

    def test_zero_total_without_result_arrays_is_a_complete_empty_result(self) -> None:
        # The live PanSou API omits both ``results`` and ``merged_by_type``
        # when total=0.  This is a valid zero-hit response, not an outage.
        discovery, _calls = self._discovery({
            "code": 0,
            "message": "success",
            "data": {"total": 0},
        })

        result = discovery.run(_request())

        self.assertTrue(result["search_complete"])
        self.assertTrue(result["search_complete_no_candidates"])
        self.assertEqual(result["completed_sources"], ["pansou"])

    def test_coverage_miss_becomes_a_scoped_reviewed_miss(self) -> None:
        discovery, _calls = self._discovery(
            _response("https://pan.quark.cn/s/fixtureShare01"),
            inspector=lambda _pwd_id, _passcode: [{
                "file_id": "fixture-fid",
                "path": "Other.Show.S02E02.1080p.mkv",
                "size": 2_000_000,
            }],
        )

        result = discovery.run(_request())

        self.assertEqual(result["candidates"], [])
        self.assertEqual(
            result["source_telemetry"]["PanSou"]["reviewed_resource_miss_locators"],
            ["quark_share:fixtureShare01"],
        )

    def test_infrastructure_failure_during_manifest_inspection_is_not_a_miss(self) -> None:
        def unavailable(_pwd_id, _passcode):
            raise OSError("fixture Quark unavailable")

        discovery, _calls = self._discovery(
            _response("https://pan.quark.cn/s/fixtureShare01"),
            inspector=unavailable,
        )

        result = discovery.run(_request())

        self.assertFalse(result["search_complete"])
        self.assertFalse(result["search_complete_no_candidates"])
        self.assertEqual(result["completed_sources"], [])
        self.assertEqual(result["failure_scope"], "infrastructure")
        self.assertGreater(result["unchecked_secondary_candidates"], 0)


if __name__ == "__main__":
    unittest.main()
