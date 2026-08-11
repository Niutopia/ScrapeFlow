"""Tests for truthful first-tier PanSou discovery."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.tools.replenishment_adapter.pansou import PanSouDiscovery
from engine.tools.replenishment_adapter.search import ReplenishmentSearchService
from local.simple_server import SimpleApplication
from local.scrapeflow_api.simple_engine_runner import SimpleEngineRunner
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

    def test_application_wires_the_real_pansou_search_service(self) -> None:
        class MinimalAList:
            def list(self, _path, refresh=False):
                del refresh
                return []

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = MinimalAList()
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=object(),
                validate=False,
                library_root="/library",
            )
            with patch.object(SimpleApplication, "_start_startup_thread"):
                application = SimpleApplication(
                    state_root=state_root,
                    remote_root="/quark/影视",
                    remote=alist,
                    engine_runner=runner,
                )
            self.addCleanup(application.close)

            runtime = application._get_automatic_replenishment()  # noqa: SLF001

            self.assertIsInstance(runtime.search, ReplenishmentSearchService)
            self.assertIsInstance(runtime.search._pansou, PanSouDiscovery)  # noqa: SLF001
            self.assertIsNotNone(runtime.search._pansou.inspector)  # noqa: SLF001

    def test_application_derives_provider_staging_from_one_acceptance_root(self) -> None:
        class MinimalAList:
            def list(self, _path, refresh=False):
                del refresh
                return []

        media_root = "/quark/影视/ScrapeFlow/验收/run-20260811-e30a0b8"
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = MinimalAList()
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=object(),
                validate=False,
                library_root=media_root,
            )
            with patch.object(SimpleApplication, "_start_startup_thread"):
                application = SimpleApplication(
                    state_root=state_root,
                    remote_root=media_root,
                    remote=alist,
                    engine_runner=runner,
                )
            self.addCleanup(application.close)

            runtime = application._get_automatic_replenishment()  # noqa: SLF001

            self.assertEqual(
                runtime.staging_root,
                f"{media_root}/ScrapeFlow/补源",
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
