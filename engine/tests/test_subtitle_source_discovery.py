import json
import os
import time
import urllib.error
from unittest import mock
import unittest

from engine.scrapeflow.subtitle_source_discovery import (
    _mikan_rows,
    _provider_request,
    discover_quark_manifests,
    discover_torrent_manifests,
)


def batch_fixture():
    return {
        "search_batch_id": "a" * 24,
        "title": "My Show", "target_root": "/quark/影视/番剧/My Show",
        "request_ids": ["req1"], "query_terms": ["My Show S01E02"],
        "providers": ["quark_share", "torrent"],
    }


class SubtitleSourceDiscoveryTests(unittest.TestCase):
    def test_mikan_parser_accepts_only_public_download_enclosures(self):
        rows = _mikan_rows(b"""<rss><channel>
          <item><title>Good</title><enclosure
            url='https://mikanani.me/Download/20260804/good.torrent'/></item>
          <item><title>Foreign</title><enclosure
            url='https://example.com/Download/bad.torrent'/></item>
          <item><title>Query</title><enclosure
            url='https://mikanani.me/Download/bad.torrent?token=x'/></item>
        </channel></rss>""")

        self.assertEqual(rows, [(
            "Good", "https://mikanani.me/Download/20260804/good.torrent", "",
        )])

    def test_episode_batch_queries_are_compacted_to_season_and_range(self):
        batch = batch_fixture()
        batch["query_terms"] = [
            "My Show", *(f"My Show S01E{episode:02d}" for episode in range(1, 25)),
        ]
        request = _provider_request(batch)
        self.assertEqual(request["original_query_count"], 25)
        self.assertEqual(request["search_queries"], [
            "My Show", "My Show S01", "My Show S01E01-E24",
        ])

    def test_season_zero_uses_latin_alias_and_special_markers(self):
        batch = batch_fixture()
        batch.update({
            "title": "黑色五叶草", "aliases": ["Black Clover"],
            "query_terms": [
                "黑色五叶草", "Black Clover",
                "黑色五叶草 S00E01", "Black Clover S00E01",
                "黑色五叶草 S00E02", "Black Clover S00E02",
            ],
        })

        request = _provider_request(batch)

        self.assertEqual(request["media"]["aliases"], ["Black Clover"])
        self.assertEqual(request["search_queries"], [
            "黑色五叶草", "Black Clover",
            "黑色五叶草 S00", "Black Clover S00",
            "Black Clover OVA", "Black Clover Special",
            "Black Clover S00E01", "Black Clover S00E02",
        ])

    def test_quark_search_records_complete_manifest_without_save_or_ui(self):
        calls = []
        manifests, telemetry = discover_quark_manifests(
            batch_fixture(),
            search_links=lambda _request: ([{
                "share_id": "share", "release_name": "My Show S01E02",
                "share_url": "https://pan.quark.cn/s/share", "passcode": "",
            }], {"search_complete": True}),
            inspect_share=lambda row: calls.append(row) or [
                {"file_id": "video", "path": "pack/My Show - S01E02.mkv", "size": 1000},
                {"file_id": "sub", "path": "pack/My Show - S01E02.ass", "size": 200},
            ],
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(manifests), 1)
        self.assertEqual(len(manifests[0]["files"]), 2)
        self.assertTrue(manifests[0]["manifest_complete"])
        self.assertEqual(telemetry["save_operations"], 0)
        self.assertEqual(telemetry["ui_operations"], 0)

    def test_quark_bounded_page_is_not_falsely_complete_and_candidate_failure_is_excluded(self):
        class Expired(RuntimeError):
            failure_scope = "candidate"

        manifests, telemetry = discover_quark_manifests(
            batch_fixture(),
            search_links=lambda _request: ([{
                "share_id": "expired", "release_name": "Expired",
            }], {"search_complete": True, "available_discovered": 31}),
            inspect_share=lambda _row: (_ for _ in ()).throw(Expired()),
        )
        self.assertFalse(manifests)
        self.assertFalse(telemetry["search_complete"])
        self.assertEqual(telemetry["resource_failed_locators"], ["quark_share:expired"])
        self.assertEqual(telemetry["infrastructure_failures"], 0)

    def test_quark_candidate_failure_completes_when_every_candidate_is_covered(self):
        class PermanentlyRejected(RuntimeError):
            failure_scope = "candidate"

        manifests, telemetry = discover_quark_manifests(
            batch_fixture(),
            search_links=lambda _request: ([
                {"share_id": "good", "release_name": "Good"},
                {"share_id": "rejected", "release_name": "Rejected"},
            ], {"search_complete": True, "available_discovered": 2}),
            inspect_share=lambda row: (
                [{"file_id": "sub", "path": "My Show - S01E02.ass", "size": 200}]
                if row["share_id"] == "good"
                else (_ for _ in ()).throw(PermanentlyRejected())
            ),
        )
        self.assertEqual(len(manifests), 1)
        self.assertTrue(telemetry["search_complete"])
        self.assertEqual(
            telemetry["resource_failed_locators"], ["quark_share:rejected"],
        )
        self.assertEqual(telemetry["infrastructure_failures"], 0)

    def test_quark_infrastructure_failure_never_completes_covered_search(self):
        manifests, telemetry = discover_quark_manifests(
            batch_fixture(),
            search_links=lambda _request: ([{
                "share_id": "offline", "release_name": "Offline",
            }], {"search_complete": True, "available_discovered": 1}),
            inspect_share=lambda _row: (_ for _ in ()).throw(
                RuntimeError("transport unavailable")
            ),
        )
        self.assertFalse(manifests)
        self.assertFalse(telemetry["search_complete"])
        self.assertEqual(telemetry["resource_failed_locators"], [])
        self.assertEqual(telemetry["infrastructure_failures"], 1)

    def test_torrent_old_infohash_is_excluded_before_result_cap(self):
        old_hash, new_hash = "a" * 40, "b" * 40
        rss = f"""<rss xmlns:nyaa='https://nyaa.si/xmlns/nyaa'><channel>
          <item><title>Old</title><link>https://nyaa.si/download/old.torrent</link><nyaa:infoHash>{old_hash}</nyaa:infoHash></item>
          <item><title>My Show S01E02</title><link>https://nyaa.si/download/new.torrent</link><nyaa:infoHash>{new_hash}</nyaa:infoHash></item>
        </channel></rss>""".encode()
        downloads = []

        def fetch(url, **_kwargs):
            return rss if "nyaa.si" in url else json.dumps([]).encode()

        def download(url, _destination, **_kwargs):
            downloads.append(url)
            return {"infohash": new_hash, "files": {
                1: {"path": "pack/My Show - S01E02.mkv", "size": 1000},
                2: {"path": "pack/My Show - S01E02.ass", "size": 200},
            }}

        manifests, telemetry = discover_torrent_manifests(
            batch_fixture(), fetch_bytes=fetch, download_torrent=download,
            existing_locators={f"torrent_infohash:{old_hash}"},
            max_candidates=1, deadline=time.monotonic() + 10,
        )
        self.assertEqual(downloads, ["https://nyaa.si/download/new.torrent"])
        self.assertEqual(manifests[0]["acquisition"]["infohash"], new_hash)
        self.assertEqual(telemetry["payload_downloads"], 0)
        self.assertEqual(telemetry["video_members_selected"], 0)

    def test_tokyotosho_manifest_is_discovered_without_browser_ui(self):
        infohash = "b" * 40
        page = (
            "<html><body>"
            f"<a href='magnet:?xt=urn:btih:{infohash}'></a>"
            "<a href='https://tracker.example/show.torrent'>My Show S01E02</a>"
            "</body></html>"
        ).encode()

        def fetch(url, **_kwargs):
            if "tokyo-tosho.net" in url:
                return page
            if "animetosho" in url:
                return json.dumps([]).encode()
            return b"<rss><channel/></rss>"

        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_URL": "https://tokyo-tosho.net",
        }):
            manifests, telemetry = discover_torrent_manifests(
                batch_fixture(), fetch_bytes=fetch,
                download_torrent=lambda *_args, **_kwargs: {
                    "infohash": infohash,
                    "files": {
                        1: {"path": "pack/My Show - S01E02.mkv", "size": 1000},
                        2: {"path": "pack/My Show - S01E02.ass", "size": 200},
                    },
                },
                deadline=time.monotonic() + 10,
            )
        self.assertEqual(len(manifests), 1)
        self.assertEqual(manifests[0]["acquisition"]["infohash"], infohash)
        self.assertTrue(telemetry["search_complete"])
        self.assertEqual(telemetry["required_sources"], ["tokyotosho", "animetosho"])
        self.assertTrue(telemetry["sources"]["tokyotosho"]["search_complete"])

    def test_mikan_optional_manifest_expands_subtitle_discovery(self):
        infohash = "8" * 40
        rss = b"""<rss><channel><item>
          <title>My Show S01E02 Chinese</title>
          <enclosure url='https://mikanani.me/Download/20260804/show.torrent'/>
        </item></channel></rss>"""
        downloads = []

        def fetch(url, **_kwargs):
            if "mikanani.me" in url:
                return rss
            if "animetosho" in url:
                return json.dumps([]).encode()
            return b"<rss><channel/></rss>"

        def download(url, _destination, **_kwargs):
            downloads.append(url)
            return {"infohash": infohash, "files": {
                1: {"path": "pack/My Show - S01E02.mkv", "size": 1000},
                2: {"path": "pack/My Show - S01E02.zh-CN.ass", "size": 200},
            }}

        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "0",
        }):
            manifests, telemetry = discover_torrent_manifests(
                batch_fixture(), fetch_bytes=fetch, download_torrent=download,
                deadline=time.monotonic() + 10,
            )

        self.assertEqual(downloads, [
            "https://mikanani.me/Download/20260804/show.torrent",
        ])
        self.assertEqual(len(manifests), 1)
        self.assertFalse(telemetry["sources"]["mikan"]["required"])
        self.assertTrue(telemetry["sources"]["mikan"]["search_complete"])
        self.assertEqual(telemetry["required_sources"], ["animetosho"])
        self.assertTrue(telemetry["search_complete"])

    def test_mikan_broad_feed_cannot_starve_exact_episode_queries(self):
        batch = batch_fixture()
        batch["query_terms"] = [
            "My Show", "My Show S00E01", "My Show S00E02",
        ]
        broad_items = "".join(
            "<item><title>Broad {index}</title><enclosure "
            "url='https://mikanani.me/Download/broad-{index}.torrent'/></item>".format(
                index=index,
            )
            for index in range(10)
        )

        def rss_item(name):
            return (
                "<rss><channel><item><title>{name}</title><enclosure "
                "url='https://mikanani.me/Download/{name}.torrent'/></item>"
                "</channel></rss>"
            ).format(name=name).encode()

        fetched_mikan = []

        def fetch(url, **_kwargs):
            if "animetosho" in url:
                return json.dumps([]).encode()
            if "mikanani.me" not in url:
                return b"<rss><channel/></rss>"
            fetched_mikan.append(url)
            if "S00E01" in url:
                return rss_item("exact-e01")
            if "S00E02" in url:
                return rss_item("exact-e02")
            if "S00" in url:
                return rss_item("season-zero")
            if "OVA" in url:
                return rss_item("ova")
            if "Special" in url:
                return rss_item("special")
            return f"<rss><channel>{broad_items}</channel></rss>".encode()

        downloads = []

        def download(url, _destination, **_kwargs):
            downloads.append(url)
            return {"infohash": str(len(downloads)) * 40, "files": {
                1: {"path": url.rsplit("/", 1)[-1] + ".ass", "size": 200},
            }}

        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "0",
        }):
            manifests, telemetry = discover_torrent_manifests(
                batch, fetch_bytes=fetch, download_torrent=download,
                max_candidates=6, deadline=time.monotonic() + 10,
            )

        self.assertEqual(len(fetched_mikan), 6)
        self.assertEqual(len(manifests), 6)
        self.assertLessEqual(len(downloads), 6)
        self.assertTrue(any("exact-e01" in url for url in downloads))
        self.assertTrue(any("exact-e02" in url for url in downloads))
        mikan = telemetry["sources"]["mikan"]
        self.assertEqual(mikan["query_attempts"], len(fetched_mikan))
        self.assertEqual(mikan["query_attempts"], 6)
        self.assertTrue(mikan["hit_cap"])
        self.assertEqual(
            [row["selected_candidates"] for row in mikan["queries"]],
            [1, 1, 1, 1, 1, 1],
        )
        self.assertEqual(telemetry["optional_sources_capped"], ["mikan"])
        self.assertTrue(telemetry["search_complete"])

    def test_nyaa_tls_failure_does_not_block_required_subtitle_sources(self):
        def fetch(url, **_kwargs):
            if "nyaa.si" in url:
                raise RuntimeError("TLS unavailable")
            if "animetosho" in url:
                return json.dumps([]).encode()
            return b"<html><body>No results</body></html>"

        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_URL": "https://tokyo-tosho.net",
        }):
            manifests, telemetry = discover_torrent_manifests(
                batch_fixture(), fetch_bytes=fetch,
                download_torrent=lambda *_args, **_kwargs: self.fail(
                    "empty required indexes must not download metainfo"
                ),
                deadline=time.monotonic() + 10,
            )
        self.assertEqual(manifests, [])
        self.assertTrue(telemetry["search_complete"])
        self.assertFalse(telemetry["sources"]["nyaa"]["required"])
        self.assertEqual(telemetry["sources"]["nyaa"]["query_responses"], 0)
        self.assertEqual(telemetry["required_infrastructure_failures"], 0)

    def test_nyaa_metainfo_failure_is_diagnostic_not_a_required_blocker(self):
        nyaa = b"""<rss><channel><item>
          <title>My Show S01E02</title>
          <link>https://nyaa.si/download/optional.torrent</link>
        </item></channel></rss>"""

        def fetch(url, **_kwargs):
            if "nyaa.si" in url:
                return nyaa
            if "animetosho" in url:
                return json.dumps([]).encode()
            return b"<html><body>No results</body></html>"

        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "1",
        }):
            manifests, telemetry = discover_torrent_manifests(
                batch_fixture(), fetch_bytes=fetch,
                download_torrent=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    RuntimeError("optional metainfo unavailable")
                ),
                deadline=time.monotonic() + 10,
            )
        self.assertEqual(manifests, [])
        self.assertTrue(telemetry["search_complete"])
        self.assertEqual(telemetry["infrastructure_failures"], 1)
        self.assertEqual(telemetry["required_infrastructure_failures"], 0)
        self.assertEqual(telemetry["metainfo_attempted"], 1)

    def test_animetosho_uses_archived_metainfo_instead_of_upstream_nyaa_url(self):
        infohash = "c" * 40
        feed = json.dumps([{
            "title": "My Show S01E02",
            "torrent_url": "https://nyaa.si/download/123.torrent",
            "torrent_name": "[Group] My Show - 02.mkv",
            "info_hash": infohash,
        }]).encode()
        downloads = []

        def fetch(url, **_kwargs):
            if "animetosho" in url:
                return feed
            return b"<rss><channel/></rss>"

        def download(url, _destination, **_kwargs):
            downloads.append(url)
            return {"infohash": infohash, "files": {
                1: {"path": "My Show - 02.mkv", "size": 1000},
                2: {"path": "My Show - 02.ass", "size": 200},
            }}

        manifests, telemetry = discover_torrent_manifests(
            batch_fixture(), fetch_bytes=fetch, download_torrent=download,
            deadline=time.monotonic() + 10,
        )
        archived = (
            "https://storage.animetosho.org/torrent/" + infohash
            + "/%5BGroup%5D%20My%20Show%20-%2002.torrent"
        )
        self.assertEqual(downloads, [archived])
        self.assertEqual(manifests[0]["acquisition"]["torrent_url"], archived)
        self.assertEqual(telemetry["required_infrastructure_failures"], 0)

    def test_required_metainfo_permanent_http_rejection_is_candidate_failure(self):
        infohash = "d" * 40
        feed = json.dumps([{
            "title": "My Show S01E02", "torrent_name": "gone",
            "torrent_url": "https://nyaa.si/download/gone.torrent",
            "info_hash": infohash,
        }]).encode()

        def fetch(url, **_kwargs):
            if "animetosho" in url:
                return feed
            return b"<rss><channel/></rss>"

        def download(url, *_args, **_kwargs):
            cause = urllib.error.HTTPError(url, 404, "Not Found", {}, None)
            raise RuntimeError("HTTP read failed") from cause

        manifests, telemetry = discover_torrent_manifests(
            batch_fixture(), fetch_bytes=fetch, download_torrent=download,
            deadline=time.monotonic() + 10,
        )
        locator = (
            "torrent:https://storage.animetosho.org/torrent/" + infohash
            + "/gone.torrent"
        )
        self.assertEqual(manifests, [])
        self.assertTrue(telemetry["search_complete"])
        self.assertEqual(telemetry["resource_failed_locators"], [locator])
        self.assertEqual(telemetry["required_infrastructure_failures"], 0)

    def test_required_metainfo_timeout_remains_infrastructure_failure(self):
        infohash = "e" * 40
        feed = json.dumps([{
            "title": "My Show S01E02", "torrent_name": "timeout",
            "torrent_url": "https://nyaa.si/download/timeout.torrent",
            "info_hash": infohash,
        }]).encode()

        def fetch(url, **_kwargs):
            if "animetosho" in url:
                return feed
            return b"<rss><channel/></rss>"

        manifests, telemetry = discover_torrent_manifests(
            batch_fixture(), fetch_bytes=fetch,
            download_torrent=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("transport timed out")
            ),
            deadline=time.monotonic() + 10,
        )
        self.assertEqual(manifests, [])
        self.assertFalse(telemetry["search_complete"])
        self.assertEqual(telemetry["resource_failed_locators"], [])
        self.assertEqual(telemetry["required_infrastructure_failures"], 1)

    def test_animetosho_legacy_upstream_outage_is_not_required_infrastructure(self):
        infohash = "f" * 40
        feed = json.dumps([{
            "title": "My Show legacy release",
            "torrent_name": "",
            "torrent_url": "https://nyaa.si/download/legacy.torrent",
            "info_hash": infohash,
        }]).encode()

        def fetch(url, **_kwargs):
            if "animetosho" in url:
                return feed
            return b"<rss><channel/></rss>"

        manifests, telemetry = discover_torrent_manifests(
            batch_fixture(), fetch_bytes=fetch,
            download_torrent=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("optional upstream TLS failure")
            ),
            deadline=time.monotonic() + 10,
        )
        self.assertEqual(manifests, [])
        self.assertTrue(telemetry["search_complete"])
        self.assertEqual(telemetry["infrastructure_failures"], 1)
        self.assertEqual(telemetry["required_infrastructure_failures"], 0)

    def test_tokyotosho_upstream_nyaa_outage_is_not_required_infrastructure(self):
        infohash = "9" * 40
        page = (
            "<html><body>"
            f"<a href='magnet:?xt=urn:btih:{infohash}'></a>"
            "<a href='https://nyaa.si/download/legacy.torrent'>My Show S01E02</a>"
            "</body></html>"
        ).encode()

        def fetch(url, **_kwargs):
            if "tokyo-tosho.net" in url:
                return page
            if "animetosho" in url:
                return json.dumps([]).encode()
            return b"<rss><channel/></rss>"

        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_URL": "https://tokyo-tosho.net",
        }):
            manifests, telemetry = discover_torrent_manifests(
                batch_fixture(), fetch_bytes=fetch,
                download_torrent=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    RuntimeError("optional upstream TLS failure")
                ),
                deadline=time.monotonic() + 10,
            )
        self.assertEqual(manifests, [])
        self.assertTrue(telemetry["search_complete"])
        self.assertEqual(telemetry["infrastructure_failures"], 1)
        self.assertEqual(telemetry["required_infrastructure_failures"], 0)


if __name__ == "__main__":
    unittest.main()
