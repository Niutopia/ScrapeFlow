"""End-to-end tests for the source -> provider -> child -> cleanup loop."""

from __future__ import annotations

import json
import posixpath
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from local.scrapeflow_api.automatic_replenishment import (
    AutomaticReplenishmentRuntime,
    FixedTierAutomaticMaterializer,
    QuarkFastSaveAutomaticMaterializer,
    QuarkMagnetAutomaticMaterializer,
)
from engine.scrapeflow.replenishment_matching import (
    coverage_tokens,
    expanded_episode_ids,
)
from local.scrapeflow_api.replenishment import (
    _identity_matches,
    _coverage_tokens,
    _expanded_episode_ids,
    build_replenishment_request,
    enrich_replenishment_plan_aliases,
    select_replenishment_candidates,
    _swarm_preference,
)
from local.scrapeflow_api.simple_engine_runner import EngineJob, SimpleEngineRunner
from local.scrapeflow_api.replenishment_tiers import TIER_LOCAL_MAGNET
from engine.scrapeflow.models import Plan, PlannedFile
from engine.tools._replenishment_local_adapter_impl import (
    _acquire,
    _bencode,
    _download_torrent,
    _ensure_automatic_staging_root,
    _dmhy_search_terms,
    _search_dmhy,
    _compact_dynamic_search_terms,
    _explicit_episode_search_terms,
    _animetosho_search_terms,
    _optional_bare_alias_terms,
    _search_animetosho,
    _search_nyaa,
    _search_tokyotosho,
    _torrent_candidate,
    _torrent_manifest,
    _verify_manifest,
)


class MemoryAList:
    def __init__(self) -> None:
        self.tree: dict[str, list[dict[str, object]]] = {
            "/quark/影视/ScrapeFlow/补源": [],
        }

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.tree.get(path, [])]

    def mkdir(self, path: str) -> None:
        if path in self.tree:
            return
        parent = posixpath.dirname(path) or "/"
        name = posixpath.basename(path)
        self.tree.setdefault(parent, [])
        if not any(row.get("name") == name for row in self.tree[parent]):
            self.tree[parent].append({"name": name, "is_dir": True})
        self.tree[path] = []

    def remove(self, parent: str, names: list[str]) -> None:
        for name in names:
            self.tree[parent] = [row for row in self.tree.get(parent, []) if row.get("name") != name]
            child = posixpath.join(parent, name)
            self.tree.pop(child, None)

    def remove_empty_dir(self, path: str) -> bool:
        if self.tree.get(path):
            return False
        parent = posixpath.dirname(path) or "/"
        name = posixpath.basename(path)
        self.tree[parent] = [row for row in self.tree.get(parent, []) if row.get("name") != name]
        self.tree.pop(path, None)
        return True


class FakeSearch:
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []

    def run(self, request):
        self.requests.append(dict(request))
        return {
            "candidates": [{
                "provider": "magnet",
                "locator": "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
                "release_name": "Example Show S01E01 1080p",
                "title": "Example Show",
                "year": "2020",
                "files": ["Example.Show.S01E01.mkv"],
                "acquisition": {"kind": "torrent"},
            }],
        }


def _ready_delivery(
    staging_root: str,
    files: list[dict[str, object]],
    **extra: object,
) -> dict[str, object]:
    return {
        "status": "ready",
        "lane": TIER_LOCAL_MAGNET,
        "attempt_id": posixpath.basename(staging_root.rstrip("/")),
        "staging_root": staging_root,
        "files": files,
        **extra,
    }


class FakeMaterializer:
    def __init__(self, alist: MemoryAList) -> None:
        self.alist = alist
        self.calls: list[str] = []

    def acquire(self, request, selections, *, staging_root, workspace, alist):
        del request, selections, workspace, alist
        self.calls.append(staging_root)
        parent = posixpath.dirname(staging_root)
        self.alist.mkdir(parent)
        self.alist.mkdir(staging_root)
        self.alist.tree[staging_root] = [{
            "name": "Example.Show.S01E01.mkv", "is_dir": False, "size": 123,
        }]
        return _ready_delivery(
            staging_root,
            [{
                "path": f"{staging_root}/Example.Show.S01E01.mkv",
                "size": 123,
                "kind": "video",
                "gap_ids": ["S01E01"],
            }],
        )


class FakeEngine:
    def __init__(self) -> None:
        self.planned: list[dict[str, object]] = []
        self.executed: list[str] = []
        self.internal_children: list[tuple[str, str]] = []
        self.counter = 0

    def plan_job(self, request, *, internal_child_of: str | None = None):
        self.counter += 1
        self.planned.append(dict(request))
        job_id = f"engine-child-{self.counter}"
        summary: dict[str, object] = {"mode": "tv"}
        if internal_child_of is not None:
            self.internal_children.append((job_id, internal_child_of))
            summary.update({
                "internal_child": True,
                "root_job_id": internal_child_of,
            })
        return EngineJob(
            id=job_id, phase="planned",
            created_at="2026-08-07T00:00:00Z", updated_at="2026-08-07T00:00:00Z",
            request=dict(request), plan={}, summary=summary,
        )

    def execute_automatic(self, job_id):
        self.executed.append(job_id)
        return EngineJob(
            id=job_id, phase="executed",
            created_at="2026-08-07T00:00:00Z", updated_at="2026-08-07T00:00:01Z",
            request=self.planned[-1], plan={}, summary={"mode": "tv"},
            execution={"files": [{
                "target": "/quark/影视/番剧/Example Show/Season 01/Example.Show.S01E01.mkv",
                "size": 123,
            }]},
        )


class FakeSubtitleEngine(FakeEngine):
    """Small sidecar-writer stand-in that records the exact audited pairing."""

    def __init__(self) -> None:
        super().__init__()
        self.installed_subtitles: list[dict[str, object]] = []

    def install_subtitle_sidecar(
        self,
        source_path: str,
        target_path: str,
        *,
        expected_size: int,
        video_path: str,
    ) -> dict[str, object]:
        self.installed_subtitles.append({
            "source_path": source_path,
            "target_path": target_path,
            "expected_size": expected_size,
            "video_path": video_path,
        })
        return {
            "status": "executed",
            "source_path": source_path,
            "target_path": target_path,
            "size": expected_size,
        }


def _example_root_job(job_id: str = "engine-cancel-root") -> EngineJob:
    plan = {
        "mode": "tv",
        "source_root": "/quark/影视/待刮削/Example Show",
        "target_root": "/quark/影视/番剧/Example Show",
        "metadata": {
            "tmdb_id": 7, "title": "Example Show", "original_title": "Example Show",
            "year": "2020", "series_root": "/quark/影视/番剧/Example Show",
        },
        "scan_report": {"resource_gaps": [{
            "id": "S01E01", "kind": "missing_episode", "label": "Example Show S01E01",
            "reason": "missing", "files": [],
        }]},
    }
    return EngineJob(
        id=job_id, phase="executed",
        created_at="2026-08-07T00:00:00Z", updated_at="2026-08-07T00:00:00Z",
        request={
            "source_path": "/quark/影视/待刮削/Example Show",
            "parent_path": "/quark/影视/番剧", "media_type": "tv", "tmdb_id": 7,
            "season": 1,
        }, plan=plan, summary={"mode": "tv"},
    )


class AutomaticReplenishmentTests(unittest.TestCase):
    def test_exact_episode_terms_survive_many_tmdb_aliases(self) -> None:
        request = {
            "media": {
                "title": "会长是女仆大人！",
                "aliases": [
                    "会长是女仆大人！", "Kaichou wa Maid-sama!",
                    "Kaichō wa Maid-sama!", "Президент студсовета — горничная!",
                    "Maid-Sama!", "The Class President Is a Maid!",
                ],
            },
            "query_groups": [{
                "season": 0, "episodes": [2],
                "episode_titles": ["是特别附送喔！"],
            }],
            "rules": {"optional_discovery_only": True},
        }
        terms = _explicit_episode_search_terms(request, maximum=2)
        self.assertEqual(
            terms,
            [
                "会长是女仆大人！ S00E02",
                "Kaichou wa Maid-sama! S00E02",
            ],
        )

    def test_dmhy_regular_season_query_keeps_romanized_season_alias(self) -> None:
        request = {
            "media": {
                "title": "关于我转生变成史莱姆这档事",
                "aliases": [
                    "That Time I Got Reincarnated as a Slime",
                    "Tensei Shitara Slime Datta Ken",
                ],
            },
            "query_groups": [{"season": 4, "episodes": [17]}],
            "gaps": [{"season": 4, "episode": 17, "kind": "missing_episode"}],
        }
        terms = _dmhy_search_terms(request)
        self.assertIn("Tensei Shitara Slime Datta Ken S4", terms)
        exact_terms = _compact_dynamic_search_terms(request, maximum=4)
        self.assertIn(
            "Tensei Shitara Slime Datta Ken S04E17", exact_terms,
        )

    def test_animetosho_s00_queries_prioritize_specific_movie_aliases(self) -> None:
        request = {
            "media": {
                "title": "约会大作战",
                "aliases": ["デート・ア・ライブ", "Date A Live"],
            },
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "episode": 5, "title": "约会大作战：万由里裁决",
                "title_aliases": [
                    "Episode 5", "第5話",
                    "劇場版 デート・ア・ライブ 万由里ジャッジメント",
                    "Gekijouban Date A Live: Mayuri Judgement",
                    "Date A Live The Movie: Mayuri Judgement",
                ],
            }],
            "query_groups": [{
                "season": 0, "episodes": [5],
                "episode_titles": [
                    "约会大作战：万由里裁决", "Episode 5", "第5話",
                    "劇場版 デート・ア・ライブ 万由里ジャッジメント",
                    "Gekijouban Date A Live: Mayuri Judgement",
                    "Date A Live The Movie: Mayuri Judgement",
                ],
            }],
            "rules": {"optional_discovery_only": True},
        }
        terms = _animetosho_search_terms(request, maximum=4)
        self.assertEqual(terms[:3], [
            "Gekijouban Date A Live: Mayuri Judgement",
            "Date A Live The Movie: Mayuri Judgement",
            "约会大作战：万由里裁决",
        ])
        self.assertNotIn("Episode 5", terms)
        self.assertNotIn("第5話", terms)
        dmhy_terms = _dmhy_search_terms(request)
        self.assertEqual(dmhy_terms[:2], [
            "Gekijouban Date A Live: Mayuri Judgement",
            "Date A Live The Movie: Mayuri Judgement",
        ])

    def test_s00_movie_title_rows_are_preflight_prioritized_for_animetosho(self) -> None:
        """A matching row late in a bounded feed is inspected before 28 misses."""
        request = {
            "media": {
                "tmdb_id": 46004, "title": "约会大作战",
                "aliases": ["约会大作战", "Date A Live"],
            },
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "episodes": [5], "label": "约会大作战 S00E05",
                "title": "万由里裁决",
                "title_aliases": [
                    "Episode 5", "Date A Live Movie: Mayuri Judgment",
                    "Mayuri Judgment",
                ],
            }],
            "query_groups": [{
                "season": 0, "episodes": [5],
                "episode_titles": [
                    "万由里裁决", "Episode 5",
                    "Date A Live Movie: Mayuri Judgment", "Mayuri Judgment",
                ],
            }],
            "rules": {"optional_discovery_only": True},
        }
        desired_hash = f"{29:040x}"
        desired_url = (
            "https://storage.animetosho.org/torrent/"
            f"{desired_hash}/mayuri.torrent"
        )
        desired_release = "[Group] Date A Live Movie: Mayuri Judgment (1080p)"
        feed = [
            {
                "title": f"[Other] Unrelated Release {index}",
                "torrent_url": (
                    "https://storage.animetosho.org/torrent/"
                    f"{index:040x}/other-{index}.torrent"
                ),
                "info_hash": f"{index:040x}",
            }
            for index in range(1, 29)
        ]
        feed.append({
            "title": desired_release, "torrent_url": desired_url,
            "info_hash": desired_hash,
        })
        calls: list[str] = []

        def download(url: str, *_args: object, **_kwargs: object) -> dict[str, object]:
            calls.append(url)
            if url != desired_url:
                raise RuntimeError("unrelated raw row")
            return {
                "infohash": desired_hash,
                "files": {1: {
                    "path": "Date A Live Movie Mayuri Judgment 1080p.mkv",
                    "size": 123,
                }},
            }

        with patch(
            "engine.tools._replenishment_local_adapter_impl._fetch_bytes",
            return_value=json.dumps(feed).encode("utf-8"),
        ), patch(
            "engine.tools._replenishment_local_adapter_impl._download_torrent",
            side_effect=download,
        ):
            candidates = _search_animetosho(
                request, set(), deadline=time.monotonic() + 10,
            )

        self.assertEqual(calls[0], desired_url)
        self.assertEqual([row["provider"] for row in candidates], ["quark_magnet", "magnet"])
        local = next(row for row in candidates if row["provider"] == "magnet")
        self.assertEqual(local["release_name"], desired_release)
        self.assertEqual(
            local["acquisition"]["file_index_by_gap"], {"S00E05": [1]},
        )

    def test_s00_movie_title_rows_are_preflight_prioritized_for_dmhy(self) -> None:
        """DMHY detail/Torrent preflight likewise does not starve a late match."""
        request = {
            "media": {
                "tmdb_id": 46004, "title": "约会大作战",
                "aliases": ["约会大作战", "Date A Live"],
            },
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "episodes": [5], "label": "约会大作战 S00E05",
                "title": "万由里裁决",
                "title_aliases": ["Date A Live Movie: Mayuri Judgment"],
            }],
            "query_groups": [{
                "season": 0, "episodes": [5],
                "episode_titles": [
                    "万由里裁决", "Date A Live Movie: Mayuri Judgment",
                ],
            }],
            "rules": {"optional_discovery_only": True},
        }
        desired_release = "[Group] Date A Live Movie: Mayuri Judgment (1080p)"
        desired_detail = "https://share.dmhy.org/topics/view/29.html"
        desired_torrent = "https://dl.dmhy.org/mayuri.torrent"
        desired_hash = f"{29:040x}"
        items = [
            "<item><title>[Other] Unrelated Release " + str(index)
            + "</title><link>https://share.dmhy.org/topics/view/"
            + str(index) + ".html</link></item>"
            for index in range(1, 29)
        ]
        items.append(
            "<item><title>" + desired_release + "</title><link>"
            + desired_detail + "</link></item>"
        )
        rss = ("<rss><channel>" + "".join(items) + "</channel></rss>").encode("utf-8")
        calls: list[str] = []

        def fetch(url: str, **_kwargs: object) -> bytes:
            if "rss.xml" in url:
                return rss
            torrent_name = "mayuri.torrent" if url == desired_detail else "other.torrent"
            return (
                '<a href="https://dl.dmhy.org/' + torrent_name + '">torrent</a>'
            ).encode("utf-8")

        def download(url: str, *_args: object, **_kwargs: object) -> dict[str, object]:
            calls.append(url)
            if url != desired_torrent:
                raise RuntimeError("unrelated raw row")
            return {
                "infohash": desired_hash,
                "files": {1: {
                    "path": "Date A Live Movie Mayuri Judgment 1080p.mkv",
                    "size": 123,
                }},
            }

        with patch(
            "engine.tools._replenishment_local_adapter_impl._fetch_bytes",
            side_effect=fetch,
        ), patch(
            "engine.tools._replenishment_local_adapter_impl._download_torrent",
            side_effect=download,
        ):
            candidates = _search_dmhy(
                request, set(), deadline=time.monotonic() + 10,
            )

        self.assertEqual(calls[0], desired_torrent)
        self.assertEqual([row["provider"] for row in candidates], ["quark_magnet", "magnet"])
        self.assertTrue(all(row["release_name"] == desired_release for row in candidates))

    def test_s00_title_preflight_priority_still_requires_manifest_coverage(self) -> None:
        """A matching raw name cannot bypass the exact S00 file-title gate."""
        request = {
            "media": {
                "tmdb_id": 46004, "title": "约会大作战",
                "aliases": ["约会大作战", "Date A Live"],
            },
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "episodes": [5], "label": "约会大作战 S00E05",
                "title": "万由里裁决",
                "title_aliases": ["Date A Live Movie: Mayuri Judgment"],
            }],
            "query_groups": [{
                "season": 0, "episodes": [5],
                "episode_titles": ["Date A Live Movie: Mayuri Judgment"],
            }],
            "rules": {"optional_discovery_only": True},
        }
        infohash = "a" * 40
        feed = json.dumps([{
            "title": "Date A Live Movie: Mayuri Judgment",
            "torrent_url": "https://storage.animetosho.org/torrent/"
            + infohash + "/wrong-manifest.torrent",
            "info_hash": infohash,
        }]).encode("utf-8")
        with patch(
            "engine.tools._replenishment_local_adapter_impl._fetch_bytes",
            return_value=feed,
        ), patch(
            "engine.tools._replenishment_local_adapter_impl._download_torrent",
            return_value={
                "infohash": infohash,
                "files": {1: {"path": "Other Show S01E01.mkv", "size": 123}},
            },
        ):
            candidates = _search_animetosho(
                request, set(), deadline=time.monotonic() + 10,
            )

        self.assertEqual(candidates, [])

    def test_s00_title_preflight_priority_does_not_relax_identity_selection(self) -> None:
        """A bare movie alias remains unusable without the TV identity."""
        request = {
            "media": {
                "tmdb_id": 46004, "title": "约会大作战",
                "aliases": ["约会大作战", "Date A Live"],
            },
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "episodes": [5], "label": "约会大作战 S00E05",
                "title": "万由里裁决",
                "title_aliases": ["Mayuri Judgment"],
            }],
            "query_groups": [{"season": 0, "episodes": [5]}],
            "rules": {"optional_discovery_only": True},
        }
        selected = select_replenishment_candidates(request, [{
            "provider": "magnet",
            "locator": "torrent:https://example.test/mayuri.torrent",
            "release_name": "[Group] Mayuri Judgment (1080p)",
            "files": ["Mayuri Judgment 1080p.mkv"],
            "availability": "metadata_verified",
            "acquisition": {"kind": "torrent"},
        }])

        self.assertEqual(selected["status"], "no_match")
        self.assertEqual(
            selected["rejection_reasons"], {"title_identity_mismatch": 1},
        )

    def test_date_a_live_s00_gap_aliases_do_not_authorize_generic_or_cross_work_release(self) -> None:
        """A gap-local movie title cannot become a general TV work alias."""
        request = {
            "media": {
                "tmdb_id": 46004, "title": "约会大作战",
                "aliases": ["约会大作战", "Date A Live"],
            },
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "episodes": [5], "label": "约会大作战 S00E05",
                "title": "万由里裁决", "title_aliases": ["Mayuri Judgement"],
            }],
            "rules": {"optional_discovery_only": True},
        }

        def candidate(name: str, token: str) -> dict[str, object]:
            return {
                "provider": "magnet",
                "locator": f"torrent:https://example.test/{token}.torrent",
                "release_name": name,
                "files": [f"{name}.mkv"],
                "file_coverage": ["S00E05"],
                "acquisition": {
                    "kind": "torrent",
                    "url": f"https://example.test/{token}.torrent",
                    "file_index_by_gap": {"S00E05": [1]},
                    "file_size_by_index": {"1": 100},
                    "file_path_by_index": {"1": f"{name}.mkv"},
                },
            }

        selected = select_replenishment_candidates(request, [
            candidate("[Group] Mayuri Judgement S00E05", "generic"),
            candidate("[Group] Other Show - Mayuri Judgement S00E05", "cross-work"),
        ])

        self.assertEqual(selected["status"], "no_match")
        self.assertEqual(
            selected["rejection_reasons"], {"title_identity_mismatch": 2},
        )

    def test_s00_continuation_alias_requires_base_or_current_gap_evidence(self) -> None:
        """A sibling season cannot satisfy an S00 movie by its sequel alias alone."""
        request = {
            "media": {
                "tmdb_id": 46004, "title": "约会大作战",
                "aliases": [
                    "约会大作战", "Date A Live", "Date A Live II",
                    "Date A Live S02", "Date A Live Season 2",
                ],
            },
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "episodes": [5], "label": "约会大作战 S00E05",
                "title": "万由里裁决", "title_aliases": ["Mayuri Judgement"],
            }],
            "rules": {"optional_discovery_only": True},
        }
        for index, continuation in enumerate((
            "Date A Live II", "Date A Live S02", "Date A Live Season 2",
        )):
            with self.subTest(continuation=continuation):
                url = f"https://example.test/date-a-live-sibling-{index}.torrent"
                sibling = {
                    "provider": "magnet", "locator": f"torrent:{url}",
                    "release_name": f"[Group] {continuation} S00E05 (1080p)",
                    "files": [f"{continuation} S00E05 1080p.mkv"],
                    "file_coverage": ["S00E05"],
                    "acquisition": {
                        "kind": "torrent", "url": url,
                        "file_index_by_gap": {"S00E05": [1]},
                        "file_size_by_index": {"1": 100},
                        "file_path_by_index": {
                            "1": f"{continuation} S00E05 1080p.mkv",
                        },
                    },
                }
                rejected = select_replenishment_candidates(request, [sibling])
                self.assertEqual(rejected["status"], "no_match")
                self.assertEqual(
                    rejected["rejection_reasons"], {"title_identity_mismatch": 1},
                )

        vague_title_request = dict(request)
        vague_title_request["gaps"] = [{
            **request["gaps"][0], "title_aliases": ["Judgement"],
        }]
        vague_title = {
            "provider": "magnet",
            "locator": "torrent:https://example.test/date-a-live-ii-vague.torrent",
            "release_name": "[Group] Date A Live II S00E05 - Judgement (1080p)",
            "files": ["Date A Live II S00E05 Judgement 1080p.mkv"],
            "file_coverage": ["S00E05"],
            "acquisition": {
                "kind": "torrent",
                "url": "https://example.test/date-a-live-ii-vague.torrent",
                "file_index_by_gap": {"S00E05": [1]},
                "file_size_by_index": {"1": 100},
                "file_path_by_index": {
                    "1": "Date A Live II S00E05 Judgement 1080p.mkv",
                },
            },
        }
        vague_rejected = select_replenishment_candidates(
            vague_title_request, [vague_title],
        )
        self.assertEqual(vague_rejected["status"], "no_match")
        self.assertEqual(
            vague_rejected["rejection_reasons"], {"title_identity_mismatch": 1},
        )

        # A current-gap title is an exception only in this S00 continuation
        # case; it remains gap-local and is not appended to media.aliases.
        titled = _torrent_candidate(
            request, "[Group] Date A Live II S00E05 - Mayuri Judgement (1080p)",
            "https://example.test/date-a-live-ii-mayuri.torrent", {
                "infohash": "a" * 40,
                "files": {1: {
                    "path": "Date A Live II S00E05 Mayuri Judgement 1080p.mkv",
                    "size": 100,
                }},
            },
        )
        self.assertIsNotNone(titled)
        assert titled is not None
        allowed = select_replenishment_candidates(request, [titled])
        self.assertEqual(allowed["status"], "complete")
        self.assertEqual(allowed["covered_gap_ids"], ["S00E05"])

        regular_season_request = {
            "media": {"title": "Date A Live II", "aliases": ["Date A Live II"]},
            "gaps": [{
                "id": "S02E01", "kind": "missing_episode", "season": 2,
                "episodes": [1],
            }],
        }
        self.assertTrue(_identity_matches(regular_season_request, {
            "release_name": "[Group] Date A Live II S02E01 (1080p)",
            "files": ["Date A Live II S02E01 1080p.mkv"],
        }))

    def test_nyaa_mirror_fallback_repairs_only_the_known_comment_wrapper(self) -> None:
        """A failed canonical Nyaa fetch may use the fixed HTTPS mirror only."""
        source_url = "https://nyaa.si/view/2143012/torrent"
        mirror_url = "https://nyaa.land/view/2143012/torrent"
        valid = _bencode({
            b"comment": b"https://nyaa.land/view/2143012",
            b"info": {b"length": 1, b"name": b"episode.mkv"},
        })
        malformed = valid.replace(
            b"30:https://nyaa.land/view/2143012",
            b"28:https://nyaa.land/view/2143012",
            1,
        )
        self.assertNotEqual(malformed, valid)
        calls: list[str] = []

        def fetch(url: str, **_kwargs: object) -> bytes:
            calls.append(url)
            if url == source_url:
                raise RuntimeError("TLS origin unavailable")
            self.assertEqual(url, mirror_url)
            return malformed

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "candidate.torrent"
            with patch(
                "engine.tools._replenishment_local_adapter_impl._fetch_bytes",
                side_effect=fetch,
            ):
                manifest = _download_torrent(
                    source_url, destination, timeout=1, attempts=1,
                )
            self.assertEqual(destination.read_bytes(), valid)
        self.assertEqual(calls, [source_url, mirror_url])
        self.assertEqual(manifest["infohash"], _torrent_manifest(valid)["infohash"])

    def test_tokyotosho_nyaa_view_link_needs_matching_feed_btih(self) -> None:
        """A Nyaa ``/view/.../torrent`` row is safe only when its BTIH agrees."""
        infohash = "3bf1035badbadc64d72051d7d335fae8b5a88667"
        page = (
            f'<a href="magnet:?xt=urn:btih:{infohash}">magnet</a>'
            '<a href="https://nyaa.si/view/2143012/torrent">'
            '[SubsPlease] Tensei Shitara Slime Datta Ken S4 - 17 (1080p).mkv'
            '</a>'
        ).encode("utf-8")
        request = {
            "media": {
                "tmdb_id": 82684,
                "title": "Tensei Shitara Slime Datta Ken",
                "aliases": ["Tensei Shitara Slime Datta Ken"],
            },
            "gaps": [{
                "id": "S04E17", "kind": "missing_episode", "season": 4,
                "episodes": [17], "label": "Slime S04E17", "title": "",
            }],
            "query_groups": [{"season": 4, "episodes": [17]}],
        }
        manifest = {
            "infohash": infohash,
            "files": {1: {
                "path": "[SubsPlease] Tensei Shitara Slime Datta Ken S4 - 17 (1080p).mkv",
                "size": 123,
            }},
        }
        deadline = time.monotonic() + 10
        with patch(
            "engine.tools._replenishment_local_adapter_impl._fetch_bytes",
            return_value=page,
        ), patch(
            "engine.tools._replenishment_local_adapter_impl._download_torrent",
            return_value=manifest,
        ):
            candidates = _search_tokyotosho(request, set(), deadline=deadline)
        self.assertEqual([row["provider"] for row in candidates], ["quark_magnet", "magnet"])
        local = next(row for row in candidates if row["provider"] == "magnet")
        self.assertEqual(local["infohash"], infohash)
        self.assertEqual(
            local["acquisition"]["file_index_by_gap"], {"S04E17": [1]},
        )

        with patch(
            "engine.tools._replenishment_local_adapter_impl._fetch_bytes",
            return_value=page,
        ), patch(
            "engine.tools._replenishment_local_adapter_impl._download_torrent",
            return_value={**manifest, "infohash": "f" * 40},
        ):
            mismatched = _search_tokyotosho(
                request, set(), deadline=time.monotonic() + 10,
            )
        self.assertEqual(mismatched, [])
        self.assertIn(f"torrent:{infohash}", mismatched.resource_failed_locators)

    def test_public_swarm_observations_are_additive_to_nyaa_and_animetosho(self) -> None:
        """Provider swarm fields reach ranking only after manifest validation."""
        infohash = "3bf1035badbadc64d72051d7d335fae8b5a88667"
        request = {
            "media": {
                "tmdb_id": 82684,
                "title": "Tensei Shitara Slime Datta Ken",
                "aliases": ["Tensei Shitara Slime Datta Ken"],
            },
            "gaps": [{
                "id": "S04E17", "kind": "missing_episode", "season": 4,
                "episodes": [17], "label": "Slime S04E17", "title": "",
            }],
            "query_groups": [{"season": 4, "episodes": [17]}],
        }
        release = "[SubsPlease] Tensei Shitara Slime Datta Ken S4 - 17 (1080p).mkv"
        manifest = {
            "infohash": infohash,
            "files": {1: {"path": release, "size": 123}},
        }
        nyaa_feed = f"""<?xml version=\"1.0\"?>
<rss xmlns:nyaa=\"https://nyaa.si/xmlns/nyaa\"><channel><item>
<title>{release}</title><link>https://nyaa.si/download/2143012.torrent</link>
<nyaa:infoHash>{infohash}</nyaa:infoHash><nyaa:seeders>0</nyaa:seeders>
<nyaa:leechers>4</nyaa:leechers>
</item></channel></rss>""".encode("utf-8")
        with patch(
            "engine.tools._replenishment_local_adapter_impl._fetch_bytes",
            return_value=nyaa_feed,
        ), patch(
            "engine.tools._replenishment_local_adapter_impl._download_torrent",
            return_value=manifest,
        ):
            nyaa = _search_nyaa(request, set(), deadline=time.monotonic() + 10)

        self.assertEqual([row["provider"] for row in nyaa], ["quark_magnet", "magnet"])
        nyaa_local = next(row for row in nyaa if row["provider"] == "magnet")
        self.assertEqual(nyaa_local["seeders"], 0)
        self.assertEqual(nyaa_local["leechers"], 4)
        self.assertEqual(_swarm_preference(nyaa_local)[0], 0)

        animetosho_feed = json.dumps([{
            "title": release,
            "torrent_url": "https://storage.animetosho.org/torrent/" + infohash + "/sample.torrent",
            "info_hash": infohash,
            "seeders": 7,
            "leechers": 3,
            "tracker_updated": int(time.time()),
        }]).encode("utf-8")
        with patch(
            "engine.tools._replenishment_local_adapter_impl._fetch_bytes",
            return_value=animetosho_feed,
        ), patch(
            "engine.tools._replenishment_local_adapter_impl._download_torrent",
            return_value=manifest,
        ):
            animetosho = _search_animetosho(
                request, set(), deadline=time.monotonic() + 10,
            )

        self.assertEqual(
            [row["provider"] for row in animetosho],
            ["quark_magnet", "magnet"],
        )
        animetosho_local = next(row for row in animetosho if row["provider"] == "magnet")
        self.assertEqual(animetosho_local["seeders"], 7)
        self.assertEqual(animetosho_local["leechers"], 3)
        self.assertEqual(_swarm_preference(animetosho_local)[0], 2)

    def test_swarm_liveness_breaks_only_valid_candidate_ties(self) -> None:
        """Fresh positive seed beats neutral/zero only after hard gates."""
        request = {
            "media": {
                "tmdb_id": 7,
                "title": "Example Show",
                "aliases": ["Example Show"],
                "year": "2020",
            },
            "gaps": [{
                "id": "S01E01", "kind": "missing_episode", "season": 1,
                "episodes": [1], "label": "Example Show S01E01",
            }],
        }
        observed_at = time.time()

        def candidate(locator: str, release_name: str, **swarm: object) -> dict[str, object]:
            return {
                "provider": "magnet",
                "locator": locator,
                "release_name": release_name,
                "title": "Example Show",
                "year": "2020",
                "files": ["Example.Show.S01E01.mkv"],
                "resolution": "1080p",
                "acquisition": {"kind": "torrent"},
                **swarm,
            }

        positive = candidate(
            "torrent:https://example.invalid/positive.torrent",
            "Example Show S01E01 1080p", seeders=12,
            swarm_observed_at=observed_at,
            updated_at="2020-01-01T00:00:00Z",
        )
        zero = candidate(
            "torrent:https://example.invalid/zero.torrent",
            "AAA Example Show S01E01 1080p", seeders=0,
            swarm_observed_at=observed_at,
            updated_at="2030-01-01T00:00:00Z",
        )
        neutral = candidate(
            "torrent:https://example.invalid/neutral.torrent",
            "ZZZ Example Show S01E01 1080p",
        )
        stale = candidate(
            "torrent:https://example.invalid/stale.torrent",
            "Example Show S01E01 stale 1080p", seeders=99,
            swarm_observed_at=observed_at - 7 * 60 * 60,
        )
        malformed = candidate(
            "torrent:https://example.invalid/malformed.torrent",
            "Example Show S01E01 malformed 1080p", seeders="-1",
            swarm_observed_at=observed_at,
        )
        wrong_identity = candidate(
            "torrent:https://example.invalid/wrong.torrent",
            "Other Show S01E01 1080p", seeders=999,
            swarm_observed_at=observed_at, title="Other Show",
            files=["Other.Show.S01E01.mkv"],
        )

        selected = select_replenishment_candidates(
            request, [zero, neutral, stale, malformed, wrong_identity, positive],
        )

        self.assertEqual(selected["selections"][0]["locator"], positive["locator"])
        self.assertEqual(selected["rejection_reasons"], {"title_identity_mismatch": 1})
        self.assertEqual(_swarm_preference(positive), (2, 12, 0))
        self.assertEqual(_swarm_preference(zero), (0, 0, 0))
        self.assertEqual(_swarm_preference(neutral), (1, 0, 0))
        self.assertEqual(_swarm_preference(stale), (1, 0, 0))
        self.assertEqual(_swarm_preference(malformed), (1, 0, 0))

    def test_optional_torrent_requires_episode_specific_evidence(self) -> None:
        """A generic S00 audit label must not select an entire series pack."""
        request = {
            "media": {
                "title": "斩·赤红之瞳！",
                "aliases": ["斩·赤红之瞳！", "Akame ga Kill!"],
            },
            "gaps": [{
                "id": "S00E25", "kind": "missing_episode", "season": 0,
                "label": "斩·赤红之瞳！ S00E25", "title": "",
            }],
            "rules": {"optional_discovery_only": True},
        }
        ordinary_pack = {
            "infohash": "a" * 40,
            "files": {
                1: {
                    "path": "[DBD-Raws][斩!赤红之瞳][01][1080P].mkv",
                    "size": 100,
                },
                2: {
                    "path": "[DBD-Raws][斩!赤红之瞳][24][1080P].mkv",
                    "size": 100,
                },
            },
        }

        # The old label fallback normalized the series title and mapped both
        # ordinary members to S00E25.  No explicit episode title, S00 token,
        # or source alias means this must remain a search miss.
        self.assertIsNone(_torrent_candidate(
            request, "[DBD-Raws] 斩!赤红之瞳", "https://example.test/ordinary.torrent",
            ordinary_pack,
        ))

        explicit_s00 = {
            "infohash": "b" * 40,
            "files": {
                1: {
                    "path": "[DBD-Raws][斩!赤红之瞳][S00E25][1080P].mkv",
                    "size": 100,
                },
            },
        }
        candidate = _torrent_candidate(
            request, "[DBD-Raws] 斩!赤红之瞳", "https://example.test/s00.torrent",
            explicit_s00,
        )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate["file_coverage"], ["S00E25"])
        self.assertEqual(candidate["acquisition"]["file_index_by_gap"], {"S00E25": [1]})

    def test_optional_title_and_source_alias_remain_specific_evidence(self) -> None:
        base_request = {
            "media": {"title": "Akame ga Kill!", "aliases": ["Akame ga Kill!"]},
            "gaps": [{
                "id": "S00E25", "kind": "missing_episode", "season": 0,
                "label": "Akame ga Kill! S00E25", "title": "Theater 25",
            }],
            "rules": {"optional_discovery_only": True},
        }
        title_candidate = _torrent_candidate(
            base_request, "[DBD-Raws] Akame ga Kill!",
            "https://example.test/title.torrent", {
                "infohash": "c" * 40,
                "files": {
                    1: {"path": "Akame ga Kill! Theater 25.mkv", "size": 100},
                    2: {"path": "Akame ga Kill! Theater 24.mkv", "size": 100},
                },
            },
        )
        self.assertIsNotNone(title_candidate)
        assert title_candidate is not None
        self.assertEqual(
            title_candidate["acquisition"]["file_index_by_gap"], {"S00E25": [1]},
        )

        source_alias_request = {
            **base_request,
            "gaps": [{
                "id": "S00E25", "kind": "missing_episode", "season": 0,
                "label": "Akame ga Kill! S00E25", "title": "",
                "source_episode_aliases": [{
                    "season": 1, "episode": 25,
                    "series_titles": ["Akame ga Kill!"],
                }],
            }],
        }
        alias_candidate = _torrent_candidate(
            source_alias_request, "[DBD-Raws] Akame ga Kill!",
            "https://example.test/source.torrent", {
                "infohash": "d" * 40,
                "files": {
                    1: {"path": "Akame ga Kill! S01E25.mkv", "size": 100},
                    2: {"path": "Akame ga Kill! S01E24.mkv", "size": 100},
                },
            },
        )
        self.assertIsNotNone(alias_candidate)
        assert alias_candidate is not None
        self.assertEqual(
            alias_candidate["acquisition"]["file_index_by_gap"], {"S00E25": [1]},
        )

    def test_date_a_live_bonus_pack_never_selects_multiple_videos_for_one_gap(self) -> None:
        """Official-title matching must not launder Bonus/Scans into S00E05."""
        request = {
            "media": {
                "title": "约会大作战",
                "aliases": ["约会大作战", "Date A Live"],
            },
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "episode": 5, "label": "约会大作战 S00E05",
                "title": "万由里裁决",
                "title_aliases": ["Date A Live Movie: Mayuri Judgment"],
            }],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {
            "infohash": "1" * 40,
            "files": {
                1: {
                    "path": (
                        "Date A Live Movie Mayuri Judgment/Bonus/"
                        "Date A Live Movie Mayuri Judgment Bonus.mkv"
                    ),
                    "size": 123,
                },
                2: {
                    "path": "Date A Live Movie Mayuri Judgment 1080p.mkv",
                    "size": 456,
                },
                3: {
                    "path": "Date A Live Movie Mayuri Judgment/Scans/booklet.jpg",
                    "size": 789,
                },
            },
        }

        self.assertIsNone(_torrent_candidate(
            request, "[Group] Date A Live Movie: Mayuri Judgment (1080p)",
            "https://example.test/mayuri-bonus.torrent", manifest,
        ))

    def test_date_a_live_unique_primary_video_remains_eligible(self) -> None:
        """The P0 ambiguity guard must not regress a correct one-file special."""
        request = {
            "media": {
                "title": "约会大作战",
                "aliases": ["约会大作战", "Date A Live"],
            },
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "episode": 5, "label": "约会大作战 S00E05",
                "title": "万由里裁决",
                "title_aliases": ["Date A Live Movie: Mayuri Judgment"],
            }],
            "rules": {"optional_discovery_only": True},
        }
        candidate = _torrent_candidate(
            request, "[Group] Date A Live Movie: Mayuri Judgment (1080p)",
            "https://example.test/mayuri-main.torrent", {
                "infohash": "2" * 40,
                "files": {1: {
                    "path": "Date A Live Movie Mayuri Judgment 1080p.mkv",
                    "size": 456,
                }},
            },
        )

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(
            candidate["acquisition"]["file_index_by_gap"], {"S00E05": [1]},
        )

    def test_multi_episode_pack_allows_one_unique_primary_video_per_gap(self) -> None:
        request = {
            "media": {"title": "Example Show", "aliases": ["Example Show"]},
            "gaps": [
                {"id": "S01E01", "kind": "missing_episode", "season": 1, "episode": 1},
                {"id": "S01E02", "kind": "missing_episode", "season": 1, "episode": 2},
            ],
            "query_groups": [{"season": 1, "episodes": [1, 2]}],
        }
        candidate = _torrent_candidate(
            request, "Example Show S01 1080p", "https://example.test/two-episodes.torrent", {
                "infohash": "3" * 40,
                "files": {
                    1: {"path": "Example.Show.S01E01.1080p.mkv", "size": 123},
                    2: {"path": "Example.Show.S01E02.1080p.mkv", "size": 456},
                },
            },
        )

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(
            candidate["acquisition"]["file_index_by_gap"],
            {"S01E01": [1], "S01E02": [2]},
        )

    def test_unique_new_episode_can_carry_one_exact_chinese_companion(self) -> None:
        request = {
            "media": {"title": "Example Show", "aliases": ["Example Show"]},
            "gaps": [
                {"id": "S01E01", "kind": "missing_episode", "season": 1, "episode": 1},
            ],
            "query_groups": [{"season": 1, "episodes": [1]}],
        }
        candidate = _torrent_candidate(
            request, "Example Show S01E01 1080p", "https://example.test/one-companion.torrent", {
                "infohash": "5" * 40,
                "files": {
                    1: {"path": "Example.Show.S01E01.1080p.mkv", "size": 123},
                    2: {"path": "Example.Show.S01E01.1080p.chs.ass", "size": 45},
                },
            },
        )

        self.assertIsNotNone(candidate)
        assert candidate is not None
        acquisition = candidate["acquisition"]
        self.assertEqual(
            acquisition["companion_subtitle_index_by_media_gap"], {"S01E01": [2]},
        )
        self.assertEqual(acquisition["file_path_by_index"]["2"], "Example.Show.S01E01.1080p.chs.ass")
        self.assertEqual(acquisition["file_size_by_index"]["2"], 45)

    def test_actual_torrent_companion_bridges_selection_delivery_and_fresh_child_move(self) -> None:
        """Exercise the real adapter contract through the runtime sidecar write.

        Network/torrent/AList transport is mocked, but the test uses the
        actual candidate mapper, selector, materializer implementation and
        returned acquisition provenance.  It protects the otherwise easy to
        miss hand-off where a valid manifest companion reached staging but was
        absent from the runtime result.
        """
        request = {
            "media": {
                "tmdb_id": 7, "title": "Example Show", "year": "2020",
                "aliases": ["Example Show", "示例剧集"],
            },
            "gaps": [{
                "id": "S01E01", "kind": "missing_episode", "season": 1,
                "episodes": [1], "label": "Example Show S01E01", "title": "One",
            }],
        }
        manifest = {
            "infohash": "6" * 40,
            "files": {
                1: {"path": "Example.Show.S01E01.1080p.mkv", "size": 123},
                2: {"path": "Example.Show.S01E01.1080p.CHS.ass", "size": 321},
            },
        }
        candidate = _torrent_candidate(
            request, "Example Show S01E01 1080p",
            "https://example.test/actual-companion.torrent", manifest,
        )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        selected = select_replenishment_candidates(request, [candidate])
        self.assertEqual(selected["status"], "complete")
        self.assertEqual(selected["selections"][0]["selected_gap_ids"], ["S01E01"])

        root_job = _example_root_job("engine-actual-adapter-companion")
        plan = dict(root_job.plan)
        metadata = dict(plan["metadata"])
        metadata["aliases"] = ["Example Show", "示例剧集"]
        plan["metadata"] = metadata
        root_job = replace(root_job, plan=plan)

        class SelectedSearch:
            def run(self, _request):
                return {"candidates": [candidate]}

        class ActualAdapterMaterializer:
            def __init__(self) -> None:
                self.result: dict[str, object] | None = None

            def acquire(self, run_request, selections, *, staging_root, workspace, alist):
                wrapper = {
                    "request": dict(run_request),
                    "selection": {"selections": [dict(row) for row in selections]},
                    "automatic_staging_root": staging_root,
                    "automatic_staging_parent": posixpath.dirname(posixpath.dirname(staging_root)),
                }
                self.result = _acquire(wrapper, workspace, client=alist)
                return self.result

        class FreshMoveEngine(FakeSubtitleEngine):
            def __init__(self, alist: MemoryAList) -> None:
                super().__init__()
                self.alist = alist

            def execute_automatic(self, job_id):
                self.executed.append(job_id)
                source_root = str(self.planned[-1]["source_path"])
                source_name = next(
                    str(row["name"])
                    for row in self.alist.list(source_root, refresh=True)
                    if not row.get("is_dir") and str(row.get("name", "")).endswith(".mkv")
                )
                source = posixpath.join(source_root, source_name)
                target = (
                    "/quark/影视/番剧/Example Show/Season 01/"
                    "Example.Show.S01E01.mkv"
                )
                return EngineJob(
                    id=job_id, phase="executed",
                    created_at="2026-08-07T00:00:00Z", updated_at="2026-08-07T00:00:01Z",
                    request=self.planned[-1], plan={}, summary={"mode": "tv"},
                    execution={"files": [{
                        "source": source, "target": target, "size": 123, "status": "moved",
                    }]},
                )

        with tempfile.TemporaryDirectory() as temporary:
            temporary_root = Path(temporary)
            video = temporary_root / "Example.Show.S01E01.1080p.mkv"
            subtitle = temporary_root / "Example.Show.S01E01.1080p.CHS.ass"
            video.write_bytes(b"v" * 123)
            subtitle.write_bytes(b"s" * 321)
            alist = MemoryAList()
            engine = FreshMoveEngine(alist)
            materializer = ActualAdapterMaterializer()

            def stage_upload(client, root, row):
                client.tree.setdefault(root, []).append({
                    "name": str(row["remote_name"]), "is_dir": False,
                    "size": int(row["size"]),
                })

            with patch(
                "engine.tools._replenishment_local_adapter_impl._preflight",
                return_value={"candidates": [{
                    "manifest": manifest,
                    "torrent_path": str(temporary_root / "candidate.torrent"),
                }]},
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._payload_is_complete",
                return_value=True,
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._find_download",
                side_effect=lambda _payload, path, _size: video if path.endswith(".mkv") else subtitle,
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._verify_video_payload",
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._automatic_upload",
                side_effect=stage_upload,
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._verify_remote_uploads",
            ):
                runtime = AutomaticReplenishmentRuntime(
                    temporary_root / "runtime", engine_runner=engine, alist=alist,
                    search=SelectedSearch(), materializer=materializer,
                    staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=1,
                )
                outcome = runtime.run_for_job(root_job)

        result = materializer.result
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["lane"], TIER_LOCAL_MAGNET)
        self.assertTrue(str(result["attempt_id"]).startswith("attempt-"))
        self.assertEqual(
            result["companion_subtitle_index_by_media_gap"], {"S01E01": [2]},
        )
        self.assertEqual(
            result["file_index_by_gap"], {"S01E01": [1]},
        )
        self.assertEqual(
            [row["kind"] for row in result["files"]], ["video", "subtitle"],
        )
        self.assertTrue(engine.planned[0]["source_path"].endswith("/media"))
        self.assertEqual(outcome["unresolved_gaps"], [])
        self.assertEqual(len(engine.installed_subtitles), 1)
        installed = engine.installed_subtitles[0]
        self.assertIn("/subtitles/", str(installed["source_path"]))
        self.assertEqual(
            installed["target_path"],
            "/quark/影视/番剧/Example Show/Season 01/Example.Show.S01E01.zh.ass",
        )

    def test_serialized_episode_selection_cannot_reintroduce_multiple_videos(self) -> None:
        """The pre-download manifest gate repeats the adapter's uniqueness rule."""
        selection = {
            "infohash": "4" * 40,
            "selected_gap_ids": ["S01E01"],
            "acquisition": {
                "kind": "torrent",
                "file_index_by_gap": {"S01E01": [1, 2]},
                "file_size_by_index": {"1": 123, "2": 456},
                "file_path_by_index": {
                    "1": "Example.Show.S01E01.1080p.mkv",
                    "2": "Example.Show.S01E01.alt.1080p.mkv",
                },
            },
        }
        manifest = {
            "infohash": "4" * 40,
            "files": {
                1: {"path": "Example.Show.S01E01.1080p.mkv", "size": 123},
                2: {"path": "Example.Show.S01E01.alt.1080p.mkv", "size": 456},
            },
        }

        with self.assertRaisesRegex(ValueError, "唯一 torrent 视频"):
            _verify_manifest(selection, manifest)

    def test_optional_release_level_s00_does_not_promote_plain_member_numbers(self) -> None:
        request = {
            "media": {"title": "Example Show", "aliases": ["Example Show"]},
            "gaps": [{
                "id": "S00E25", "kind": "missing_episode", "season": 0,
                "label": "Example Show S00E25", "title": "",
            }],
            "rules": {"optional_discovery_only": True},
        }
        self.assertIsNone(_torrent_candidate(
            request, "Example Show S00", "https://example.test/plain.torrent", {
                "infohash": "e" * 40,
                "files": {1: {"path": "Example Show [25].mkv", "size": 100}},
            },
        ))

    def test_episode_coverage_does_not_promote_title_number_to_range_endpoint(self) -> None:
        value = "86 - Eighty Six - S00E01 - 86 - Eighty Six.mkv"

        # Both copies of the provider-neutral matcher must agree with the
        # planner: the numeric title is not an implicit E86 endpoint.
        self.assertEqual(expanded_episode_ids(value), {"S00E01"})
        self.assertEqual(_expanded_episode_ids(value), {"S00E01"})
        self.assertEqual(
            coverage_tokens([value], default_seasons={0}),
            {"S00E01"},
        )
        self.assertEqual(
            _coverage_tokens([value], default_seasons={0}),
            {"S00E01"},
        )

        # An explicit Season 00 range must not be reinterpreted under a
        # request's Season 01 default by the bare-E fallback.
        cross_season = "86 - Eighty Six - S00E01-E02.mkv"
        self.assertEqual(
            coverage_tokens([cross_season], default_seasons={1}),
            {"S00E01", "S00E02"},
        )
        self.assertEqual(
            _coverage_tokens([cross_season], default_seasons={1}),
            {"S00E01", "S00E02"},
        )

        # The season-default fallback is also fail-closed for a bare E token;
        # only an explicitly marked second endpoint forms a range.
        bare_title = "86 - Eighty Six - E01 - 86 - Eighty Six.mkv"
        self.assertEqual(
            coverage_tokens([bare_title], default_seasons={1}),
            {"S01E01"},
        )
        self.assertEqual(
            _coverage_tokens([bare_title], default_seasons={1}),
            {"S01E01"},
        )
        bare_range = "86 - Eighty Six - E01-E02.mkv"
        self.assertEqual(
            coverage_tokens([bare_range], default_seasons={1}),
            {"S01E01", "S01E02"},
        )
        self.assertEqual(
            _coverage_tokens([bare_range], default_seasons={1}),
            {"S01E01", "S01E02"},
        )

        # Numeric bracket text after an explicit coordinate is likewise not
        # an implicit extra episode.
        bracketed_title = "86 - Eighty Six - S00E01-E02 [86].mkv"
        self.assertEqual(
            coverage_tokens([bracketed_title], default_seasons={1}),
            {"S00E01", "S00E02"},
        )
        self.assertEqual(
            _coverage_tokens([bracketed_title], default_seasons={1}),
            {"S00E01", "S00E02"},
        )

        # A marked endpoint remains an intentional range in both parsers.
        ranged = "86 - Eighty Six - S00E01-E02.mkv"
        expected = {"S00E01", "S00E02"}
        self.assertEqual(expanded_episode_ids(ranged), expected)
        self.assertEqual(_expanded_episode_ids(ranged), expected)
        ep_marked = "86 - Eighty Six - S00EP01-EP02.mkv"
        self.assertEqual(expanded_episode_ids(ep_marked), expected)
        self.assertEqual(_expanded_episode_ids(ep_marked), expected)

    def test_beansub_dual_numbering_maps_tmdb_82684_s04e17_to_local_ordinal(self) -> None:
        """BeanSub's title-bracket ``[... S4][17_89]`` is local E17."""
        path = (
            "[BeanSub][Tensei Shitara Slime Datta Ken S4]"
            "[17_89][1080P][简繁].mp4"
        )
        # Keep the provider-neutral parser and the Local compatibility copy in
        # lockstep.  The season marker is mandatory; a bare pair cannot inherit
        # a request default and accidentally claim another season.
        self.assertEqual(expanded_episode_ids(path), {"S04E17"})
        self.assertEqual(_expanded_episode_ids(path), {"S04E17"})
        self.assertEqual(
            coverage_tokens([path], default_seasons={4}), {"S04E17"},
        )
        self.assertEqual(
            _coverage_tokens([path], default_seasons={4}), {"S04E17"},
        )
        self.assertEqual(
            coverage_tokens(["[BeanSub] Slime [17_89].mp4"], default_seasons={4}),
            set(),
        )

        request = {
            "media": {
                "tmdb_id": 82684,
                "title": "关于我转生变成史莱姆这档事",
                "aliases": ["关于我转生变成史莱姆这档事", "That Time I Got Reincarnated as a Slime"],
            },
            "gaps": [{
                "id": "S04E17", "kind": "missing_episode", "season": 4,
                "episode": 17,
                "label": "关于我转生变成史莱姆这档事 S04E17",
                "title": "集结之地卢贝利欧斯",
            }],
        }
        candidate = _torrent_candidate(
            request,
            "[BeanSub] 关于我转生变成史莱姆这档事 S04-89 [1080P]",
            "https://example.test/beansub-82684-s04e17.torrent",
            {
                "infohash": "f" * 40,
                "files": {1: {"path": path, "size": 123}},
            },
        )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate["file_coverage"], ["S04E17"])
        self.assertNotIn("S04E89", candidate["file_coverage"])
        self.assertEqual(
            candidate["acquisition"]["file_index_by_gap"], {"S04E17": [1]},
        )

    def test_dual_numbering_conflicts_fail_closed(self) -> None:
        conflicting = "[S4][17_89][S4][18_90].mkv"
        self.assertEqual(expanded_episode_ids(conflicting), set())
        self.assertEqual(_expanded_episode_ids(conflicting), set())
        # A canonical SxxEyy token remains authoritative when it disagrees
        # with a dual-number label; the dual pair cannot add a second claim.
        canonical_conflict = "S04E18 [S4][17_89].mkv"
        self.assertEqual(expanded_episode_ids(canonical_conflict), {"S04E18"})
        self.assertEqual(_expanded_episode_ids(canonical_conflict), {"S04E18"})

    def test_mixed_torrent_maps_only_exact_audited_subtitle_companion(self) -> None:
        """A mixed candidate may carry a sidecar, but never guess its episode."""
        subtitle_id = "missing_subtitle:7:S01E02:zh"
        request = {
            "media": {
                "tmdb_id": 7, "title": "Example Show",
                "aliases": ["Example Show"],
            },
            "gaps": [
                {
                    "id": "S01E01", "kind": "missing_episode",
                    "season": 1, "episodes": [1],
                    "label": "Example Show S01E01", "title": "One",
                },
                {
                    "id": subtitle_id, "kind": "missing_subtitle",
                    "label": "Example Show S01E02 中文字幕",
                    "path": (
                        "/quark/影视/番剧/Example Show/Season 01/"
                        "Example.Show.S01E02.mkv"
                    ),
                    "subtitle_language": "zh",
                },
            ],
        }
        candidate = _torrent_candidate(
            request, "Example Show S01E01", "https://example.test/mixed.torrent",
            {
                "infohash": "a" * 40,
                "files": {
                    1: {"path": "Example.Show.S01E01.mkv", "size": 123},
                    2: {"path": "Example.Show.S01E02.zh.srt", "size": 321},
                    3: {"path": "Example.Show.S01E03.zh.srt", "size": 321},
                },
            },
        )

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(
            candidate["acquisition"]["file_index_by_gap"],
            {"S01E01": [1], subtitle_id: [2]},
        )
        self.assertNotIn("Example.Show.S01E03.zh.srt", candidate["files"])

    def test_subtitle_only_selector_never_expands_one_sidecar_to_all_gaps(self) -> None:
        """One exact manifest sidecar must leave its sibling audit uncovered."""
        first_id = "missing_subtitle:7:S01E01:zh"
        second_id = "missing_subtitle:7:S01E02:zh"
        request = {
            "media": {
                "tmdb_id": 7, "title": "Example Show",
                "aliases": ["Example Show"],
            },
            "gaps": [
                {
                    "id": first_id, "kind": "missing_subtitle",
                    "label": "Example Show S01E01 中文字幕",
                    "title": "Example Show",
                    "path": (
                        "/quark/影视/番剧/Example Show/Season 01/"
                        "Example.Show.S01E01.mkv"
                    ),
                    "subtitle_language": "zh",
                },
                {
                    "id": second_id, "kind": "missing_subtitle",
                    "label": "Example Show S01E02 中文字幕",
                    "title": "Example Show",
                    "path": (
                        "/quark/影视/番剧/Example Show/Season 01/"
                        "Example.Show.S01E02.mkv"
                    ),
                    "subtitle_language": "zh",
                },
            ],
        }
        candidate = _torrent_candidate(
            request, "Example Show S01", "https://example.test/one-sidecar.torrent",
            {
                "infohash": "b" * 40,
                "files": {
                    1: {"path": "Example.Show.S01E01.CHS.ass", "size": 321},
                },
            },
        )

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(
            candidate["acquisition"]["file_index_by_gap"], {first_id: [1]},
        )
        selected = select_replenishment_candidates(request, [candidate])
        self.assertEqual(selected["status"], "partial")
        self.assertEqual(selected["covered_gap_ids"], [first_id])
        self.assertEqual(selected["uncovered_gap_ids"], [second_id])
        self.assertEqual(selected["selections"][0]["selected_gap_ids"], [first_id])

    def test_subtitle_torrent_accepts_trusted_same_work_bare_ordinal(self) -> None:
        """Common ``Show - 02 [CHS]`` sidecars map to one audited TV video."""
        subtitle_id = "missing_subtitle:7:S01E02:zh"
        request = {
            "media": {
                "tmdb_id": 7, "title": "Example Show",
                "aliases": ["Example Show"],
            },
            "gaps": [{
                "id": subtitle_id, "kind": "missing_subtitle",
                "label": "Example Show S01E02 中文字幕",
                "path": (
                    "/quark/影视/番剧/Example Show/Season 01/"
                    "Example.Show.S01E02.mkv"
                ),
                "subtitle_language": "zh",
            }],
        }
        candidate = _torrent_candidate(
            request, "[Group] Example Show S01", "https://example.test/bare-ordinal.torrent",
            {
                "infohash": "c" * 40,
                "files": {
                    1: {
                        "path": "[Group] Example Show - 02 [1080p][CHS].ass",
                        "size": 321,
                    },
                },
            },
        )

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(
            candidate["acquisition"]["file_index_by_gap"], {subtitle_id: [1]},
        )
        selected = select_replenishment_candidates(request, [candidate])
        self.assertEqual(selected["status"], "complete")
        self.assertEqual(selected["selections"][0]["selected_gap_ids"], [subtitle_id])

        plain_ordinal = _torrent_candidate(
            request, "[Group] Example Show S01", "https://example.test/plain-ordinal.torrent",
            {
                "infohash": "e" * 40,
                "files": {
                    1: {
                        "path": "[Group] Example Show 02 [1080p][CHS].ass",
                        "size": 321,
                    },
                },
            },
        )
        self.assertIsNotNone(plain_ordinal)
        assert plain_ordinal is not None
        self.assertEqual(
            plain_ordinal["acquisition"]["file_index_by_gap"], {subtitle_id: [1]},
        )

    def test_subtitle_bare_ordinal_fails_closed_on_conflicting_season(self) -> None:
        """Ordinal fallback cannot turn a Season 2 sidecar into Season 4."""
        subtitle_id = "missing_subtitle:7:S04E07:zh"
        request = {
            "media": {
                "tmdb_id": 7, "title": "Example Show",
                "aliases": ["Example Show"],
            },
            "gaps": [{
                "id": subtitle_id, "kind": "missing_subtitle",
                "label": "Example Show S04E07 中文字幕",
                "path": (
                    "/quark/影视/番剧/Example Show/Season 04/"
                    "Example.Show.S04E07.mkv"
                ),
                "subtitle_language": "zh",
            }],
        }

        def candidate(release_name: str, member_path: str, infohash: str):
            return _torrent_candidate(
                request, release_name, f"https://example.test/{infohash}.torrent",
                {
                    "infohash": infohash * 40,
                    "files": {1: {"path": member_path, "size": 321}},
                },
            )

        # This is the production-shaped regression: Chinese ``第2季 ep 7``
        # used to inherit the audited default S04 through the bare-ordinal
        # fallback.
        self.assertIsNone(candidate(
            "[Group] Example Show S04", "Example Show 第2季 ep 7 [CHS].ass", "a",
        ))
        # A path with no season claim remains a valid same-work ordinal.
        self.assertIsNotNone(candidate(
            "[Group] Example Show", "Example Show 07 [CHS].ass", "b",
        ))
        # An explicit matching season is valid; only conflicting evidence is
        # rejected.
        same_season = candidate(
            "[Group] Example Show S04", "Example Show 第4季 ep 7 [CHS].ass", "c",
        )
        self.assertIsNotNone(same_season)
        assert same_season is not None
        self.assertEqual(
            same_season["acquisition"]["file_index_by_gap"], {subtitle_id: [1]},
        )
        # Provider release metadata is evidence too; a bare member cannot
        # override an incompatible release-level season marker.
        self.assertIsNone(candidate(
            "[Group] Example Show S02", "Example Show 07 [CHS].ass", "d",
        ))

    def test_subtitle_torrent_rejects_bare_ordinal_without_work_identity(self) -> None:
        """A same-number sidecar from another work cannot use the fallback."""
        subtitle_id = "missing_subtitle:7:S01E02:zh"
        request = {
            "media": {
                "tmdb_id": 7, "title": "Example Show",
                "aliases": ["Example Show"],
            },
            "gaps": [{
                "id": subtitle_id, "kind": "missing_subtitle",
                "label": "Example Show S01E02 中文字幕",
                "path": (
                    "/quark/影视/番剧/Example Show/Season 01/"
                    "Example.Show.S01E02.mkv"
                ),
                "subtitle_language": "zh",
            }],
        }

        candidate = _torrent_candidate(
            request, "Example Show S01", "https://example.test/other-work.torrent",
            {
                "infohash": "d" * 40,
                "files": {
                    1: {"path": "[Group] Other Show - 02 [CHS].ass", "size": 321},
                },
            },
        )

        self.assertIsNone(candidate)
        # A correctly named containing folder cannot launder a conflicting
        # work name in the actual subtitle member.
        nested_cross_work = _torrent_candidate(
            request, "Example Show S01", "https://example.test/nested-other-work.torrent",
            {
                "infohash": "f" * 40,
                "files": {
                    1: {
                        "path": "Example Show/[Group] Other Show - 02 [CHS].ass",
                        "size": 321,
                    },
                },
            },
        )
        self.assertIsNone(nested_cross_work)

    def test_subtitle_torrent_keeps_movie_matching_stem_strict(self) -> None:
        """The TV ordinal fallback never turns a quality-tagged movie into a match."""
        subtitle_id = "missing_subtitle:9:movie:zh"
        request = {
            "media": {
                "tmdb_id": 9, "title": "Example Film",
                "aliases": ["Example Film"],
            },
            "gaps": [{
                "id": subtitle_id, "kind": "missing_subtitle",
                "label": "Example Film 中文字幕",
                "path": "/quark/影视/电影/Example Film (2023).mkv",
                "subtitle_language": "zh",
            }],
        }

        candidate = _torrent_candidate(
            request, "Example Film 2023", "https://example.test/movie.torrent",
            {
                "infohash": "e" * 40,
                "files": {
                    1: {
                        "path": "Example.Film.2023.1080p.BluRay.CHS.ass",
                        "size": 321,
                    },
                },
            },
        )

        self.assertIsNone(candidate)

    def test_mixed_delivery_isolates_media_and_subtitle_task_subroots(self) -> None:
        """The local adapter must expose only `/media` to a child planner."""
        subtitle_id = "missing_subtitle:7:S01E02:zh"
        request = {
            "media": {"tmdb_id": 7, "title": "Example Show"},
            "gaps": [
                {
                    "id": "S01E01", "kind": "missing_episode", "season": 1,
                    "episodes": [1], "label": "Example Show S01E01",
                },
                {
                    "id": subtitle_id, "kind": "missing_subtitle",
                    "label": "Example Show S01E02 中文字幕",
                    "path": (
                        "/quark/影视/番剧/Example Show/Season 01/"
                        "Example.Show.S01E02.mkv"
                    ),
                    "subtitle_language": "zh",
                },
            ],
        }
        manifest = {
            "infohash": "a" * 40,
            "files": {
                1: {"path": "Example.Show.S01E01.mkv", "size": 123},
                2: {"path": "Example.Show.S01E02.zh.srt", "size": 321},
            },
        }
        selection = {
            "provider": "magnet", "release_name": "Example Show S01E01",
            "infohash": "a" * 40, "selected_gap_ids": ["S01E01", subtitle_id],
            "acquisition": {
                "kind": "torrent", "url": "https://example.test/mixed.torrent",
                "file_index_by_gap": {"S01E01": [1], subtitle_id: [2]},
                "file_size_by_index": {"1": 123, "2": 321},
                "file_path_by_index": {
                    "1": "Example.Show.S01E01.mkv",
                    "2": "Example.Show.S01E02.zh.srt",
                },
            },
        }

        class Client:
            def __init__(self) -> None:
                self.created: list[str] = []

            def mkdir(self, path: str) -> None:
                self.created.append(path)

        with tempfile.TemporaryDirectory() as temporary:
            temporary_root = Path(temporary)
            workspace = temporary_root / "workspace"
            workspace.mkdir()
            video = temporary_root / "Example.Show.S01E01.mkv"
            subtitle = temporary_root / "Example.Show.S01E02.zh.srt"
            video.write_bytes(b"v" * 123)
            subtitle.write_bytes(b"s" * 321)
            staging = "/quark/影视/ScrapeFlow/补源/engine-test/attempt-test"
            uploads: list[tuple[str, str]] = []
            verified: list[tuple[str, list[str]]] = []

            def record_upload(_client, root, row):
                uploads.append((root, str(row["remote_name"])))

            def record_visibility(_client, root, rows):
                verified.append((root, [str(row["remote_name"]) for row in rows]))

            with patch(
                "engine.tools._replenishment_local_adapter_impl._preflight",
                return_value={"candidates": [{
                    "manifest": manifest, "torrent_path": str(temporary_root / "candidate.torrent"),
                }]},
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._payload_is_complete",
                return_value=True,
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._find_download",
                side_effect=lambda _payload, path, _size: video if path.endswith(".mkv") else subtitle,
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._verify_video_payload",
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._automatic_upload",
                side_effect=record_upload,
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._verify_remote_uploads",
                side_effect=record_visibility,
            ):
                result = _acquire(
                    {
                        "request": request,
                        "selection": {"selections": [selection]},
                        "automatic_staging_parent": "/quark/影视/ScrapeFlow/补源",
                        "automatic_staging_root": staging,
                    },
                    workspace,
                    client=Client(),
                )

        self.assertEqual(result["media_staging_root"], staging + "/media")
        self.assertEqual(result["subtitle_staging_root"], staging + "/subtitles")
        self.assertEqual(
            [root for root, _name in uploads],
            [staging + "/media", staging + "/subtitles"],
        )
        self.assertEqual([root for root, _names in verified], [staging + "/media", staging + "/subtitles"])
        self.assertEqual(
            [row["path"] for row in result["files"]],
            [
                staging + "/media/S01E01 - Example.Show.S01E01.mkv",
                # Staging names are filename-sanitized; the durable files
                # map, rather than the human-readable basename, keeps the
                # exact subtitle-gap association.
                staging + "/subtitles/missing_subtitle 7 S01E02 zh - Example.Show.S01E02.zh.srt",
            ],
        )

    def test_companion_delivery_returns_exact_manifest_provenance(self) -> None:
        """Runtime gets the one selected companion's map and delivered row."""
        request = {
            "media": {"tmdb_id": 7, "title": "Example Show"},
            "gaps": [{
                "id": "S01E01", "kind": "missing_episode", "season": 1,
                "episodes": [1], "label": "Example Show S01E01",
            }],
        }
        manifest = {
            "infohash": "c" * 40,
            "files": {
                1: {"path": "Example.Show.S01E01.mkv", "size": 123},
                2: {"path": "Example.Show.S01E01.CHS.ass", "size": 321},
            },
        }
        selection = {
            "provider": "magnet", "release_name": "Example Show S01E01",
            "infohash": "c" * 40, "selected_gap_ids": ["S01E01"],
            "acquisition": {
                "kind": "torrent", "url": "https://example.test/companion.torrent",
                "file_index_by_gap": {"S01E01": [1]},
                "companion_subtitle_index_by_media_gap": {"S01E01": [2]},
                "file_size_by_index": {"1": 123, "2": 321},
                "file_path_by_index": {
                    "1": "Example.Show.S01E01.mkv",
                    "2": "Example.Show.S01E01.CHS.ass",
                },
            },
        }

        class Client:
            def mkdir(self, _path: str) -> None:
                return

        with tempfile.TemporaryDirectory() as temporary:
            temporary_root = Path(temporary)
            workspace = temporary_root / "workspace"
            workspace.mkdir()
            video = temporary_root / "Example.Show.S01E01.mkv"
            subtitle = temporary_root / "Example.Show.S01E01.CHS.ass"
            video.write_bytes(b"v" * 123)
            subtitle.write_bytes(b"s" * 321)
            staging = "/quark/影视/ScrapeFlow/补源/engine-test/attempt-companion"

            with patch(
                "engine.tools._replenishment_local_adapter_impl._preflight",
                return_value={"candidates": [{
                    "manifest": manifest,
                    "torrent_path": str(temporary_root / "candidate.torrent"),
                }]},
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._payload_is_complete",
                return_value=True,
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._find_download",
                side_effect=lambda _payload, path, _size: video if path.endswith(".mkv") else subtitle,
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._verify_video_payload",
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._automatic_upload",
            ), patch(
                "engine.tools._replenishment_local_adapter_impl._verify_remote_uploads",
            ):
                result = _acquire(
                    {
                        "request": request,
                        "selection": {"selections": [selection]},
                        "automatic_staging_parent": "/quark/影视/ScrapeFlow/补源",
                        "automatic_staging_root": staging,
                    },
                    workspace,
                    client=Client(),
                )

        self.assertEqual(result["companion_subtitle_index_by_media_gap"], {"S01E01": [2]})
        self.assertEqual(result["file_index_by_gap"], {"S01E01": [1]})
        self.assertEqual(result["file_path_by_index"]["2"], "Example.Show.S01E01.CHS.ass")
        self.assertEqual(result["file_size_by_index"]["2"], 321)
        rows = result["files"]
        self.assertEqual(rows[0]["kind"], "video")
        self.assertEqual(rows[0]["manifest_index"], 1)
        self.assertEqual(rows[0]["gap_ids"], ["S01E01"])
        self.assertEqual(rows[1]["kind"], "subtitle")
        self.assertEqual(rows[1]["manifest_index"], 2)
        self.assertEqual(rows[1]["gap_ids"], ["S01E01"])
        self.assertEqual(rows[1]["companion_for_gap_ids"], ["S01E01"])
        self.assertEqual(rows[1]["subtitle_language"], "zh")

    def test_task_staging_parent_is_created_before_attempt(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.created: list[str] = []

            def mkdir(self, path: str) -> None:
                self.created.append(path)

        client = FakeClient()
        _ensure_automatic_staging_root(
            client,
            "/quark/影视/ScrapeFlow/补源",
            "/quark/影视/ScrapeFlow/补源/audit-root/attempt-one",
        )
        self.assertEqual(client.created, [
            "/quark/影视/ScrapeFlow/补源",
            "/quark/影视/ScrapeFlow/补源/audit-root",
            "/quark/影视/ScrapeFlow/补源/audit-root/attempt-one",
        ])

    def test_staging_cleanup_falls_back_after_noop_remove_empty(self) -> None:
        """A stale successful empty-dir response must not retry a good child."""
        class NoopRemoveEmptyAList(MemoryAList):
            def __init__(self) -> None:
                super().__init__()
                self.remove_calls: list[tuple[str, list[str]]] = []
                self.remove_empty_calls: list[str] = []

            def remove_empty_dir(self, path: str) -> bool:
                self.remove_empty_calls.append(path)
                return True  # Backend acknowledges, but leaves stale visibility.

            def remove(self, parent: str, names: list[str]) -> None:
                self.remove_calls.append((parent, list(names)))
                super().remove(parent, names)

        with tempfile.TemporaryDirectory() as temporary:
            alist = NoopRemoveEmptyAList()
            staging = "/quark/影视/ScrapeFlow/补源"
            task = f"{staging}/audit-root"
            attempt = f"{task}/attempt-one"
            alist.mkdir(task)
            alist.mkdir(attempt)
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=object(), alist=alist,
                search=object(), materializer=object(), staging_root=staging,
            )

            runtime._remove_staging(attempt)

        self.assertIn(attempt, alist.remove_empty_calls)
        self.assertIn((task, ["attempt-one"]), alist.remove_calls)
        self.assertIn((staging, ["audit-root"]), alist.remove_calls)
        self.assertEqual(alist.tree[staging], [])

    def test_staging_cleanup_keeps_nonempty_task_parent(self) -> None:
        """A sibling attempt makes the task parent shared; never force-remove it."""
        class NoopRemoveEmptyAList(MemoryAList):
            def __init__(self) -> None:
                super().__init__()
                self.remove_calls: list[tuple[str, list[str]]] = []

            def remove_empty_dir(self, path: str) -> bool:
                return True

            def remove(self, parent: str, names: list[str]) -> None:
                self.remove_calls.append((parent, list(names)))
                super().remove(parent, names)

        with tempfile.TemporaryDirectory() as temporary:
            alist = NoopRemoveEmptyAList()
            staging = "/quark/影视/ScrapeFlow/补源"
            task = f"{staging}/audit-root"
            attempt = f"{task}/attempt-one"
            sibling = f"{task}/attempt-two"
            alist.mkdir(task)
            alist.mkdir(attempt)
            alist.mkdir(sibling)
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=object(), alist=alist,
                search=object(), materializer=object(), staging_root=staging,
            )

            runtime._remove_staging(attempt)

        self.assertNotIn((staging, ["audit-root"]), alist.remove_calls)
        self.assertIn("audit-root", {str(row["name"]) for row in alist.tree[staging]})
        self.assertIn(sibling, alist.tree)

    def test_staging_cleanup_treats_missing_remote_path_as_already_clean(self) -> None:
        """A child that removed the attempt must not create a false cleanup error."""
        class MissingPathAList(MemoryAList):
            def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
                if path not in self.tree:
                    raise RuntimeError("failed get dir: object not found")
                return super().list(path, refresh=refresh)

        with tempfile.TemporaryDirectory() as temporary:
            alist = MissingPathAList()
            staging = "/quark/影视/ScrapeFlow/补源"
            task = f"{staging}/audit-root"
            attempt = f"{task}/attempt-one"
            # The child already removed both task-owned directories.  The
            # staging parent itself remains visible, as it does in AList.
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=object(), alist=alist,
                search=object(), materializer=object(), staging_root=staging,
            )

            runtime._remove_staging(attempt)

    def test_authoritative_tmdb_alias_allows_release_language_variant(self) -> None:
        plan = {
            "mode": "tv",
            "target_root": "/quark/影视/番剧/86-不存在的战区-",
            "metadata": {
                "tmdb_id": 100565,
                "title": "86-不存在的战区-",
                "original_title": "86-不存在的战区-",
                "year": "2021",
                "media_type": "tv",
            },
            "scan_report": {"resource_gaps": [{
                "id": "S00E01", "kind": "missing_episode",
                "label": "86-不存在的战区- S00E01",
                "season": 0, "episode": 1,
            }]},
        }

        class FakeTMDB:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get(self, path: str) -> dict[str, object]:
                self.calls.append(path)
                if path == "/tv/100565":
                    return {
                        "name": "Eighty Six",
                        "original_name": "86 - Eighty-Six",
                    }
                if path == "/tv/100565/alternative_titles":
                    return {"results": [
                        {"iso_3166_1": "JP", "title": "86 – EIGHTY-SIX"},
                    ]}
                raise AssertionError(f"unexpected TMDB endpoint: {path}")

        tmdb = FakeTMDB()
        enriched = enrich_replenishment_plan_aliases(plan, tmdb)
        request = build_replenishment_request(
            enriched, job_id="audit-root", round_number=1,
        )
        self.assertEqual(tmdb.calls, [
            "/tv/100565", "/tv/100565/alternative_titles",
        ])
        self.assertIn("Eighty Six", request["media"]["aliases"])
        self.assertIn(
            _optional_bare_alias_terms(request, maximum=1)[0],
            {"Eighty Six", "86 - Eighty-Six", "86 – EIGHTY-SIX"},
        )
        candidate = {
            "release_name": "[DKB] 86 - Eighty-Six - S00E01 1080p",
            "files": ["86.Eighty.Six.S00E01.mkv"],
        }
        self.assertTrue(_identity_matches(request, candidate))

    def test_tv_en_us_primary_is_front_loaded_for_date_a_live_s00(self) -> None:
        """An authoritative English TV name survives a crowded alias list."""
        plan = {
            "mode": "tv",
            "target_root": "/quark/影视/番剧/约会大作战",
            "metadata": {
                "tmdb_id": 46004, "title": "约会大作战",
                "original_title": "デート・ア・ライブ", "media_type": "tv",
                "aliases": [f"本地别名{index}" for index in range(30)],
            },
            "scan_report": {"resource_gaps": [{
                "id": "S00E05", "kind": "missing_episode",
                "label": "约会大作战 S00E05", "season": 0, "episode": 5,
                "title_aliases": ["Date A Live The Movie: Mayuri Judgement"],
            }]},
        }

        class FakeTMDB:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str | None]] = []

            def get(self, path: str, **params: object) -> dict[str, object]:
                language = params.get("language")
                language_name = language if isinstance(language, str) else None
                self.calls.append((path, language_name))
                if path == "/tv/46004" and language_name is None:
                    return {
                        "name": "约会大作战",
                        "original_name": "デート・ア・ライブ",
                    }
                if path == "/tv/46004" and language_name == "en-US":
                    return {
                        "name": "Date A Live",
                        "original_name": "デート・ア・ライブ",
                    }
                if path == "/tv/46004/alternative_titles":
                    return {"results": [{"title": "Date a Live II"}]}
                raise AssertionError(f"unexpected TMDB endpoint: {path} {language_name}")

        tmdb = FakeTMDB()
        enriched = enrich_replenishment_plan_aliases(plan, tmdb)
        aliases = enriched["metadata"]["aliases"]
        self.assertEqual(aliases[:3], ["约会大作战", "デート・ア・ライブ", "Date A Live"])
        self.assertEqual(len(aliases), 24)
        self.assertEqual(tmdb.calls, [
            ("/tv/46004", None),
            ("/tv/46004", "en-US"),
            ("/tv/46004/alternative_titles", None),
        ])

        request = build_replenishment_request(
            enriched, job_id="date-a-live", round_number=1,
        )
        candidate = _torrent_candidate(
            request, "[Group] Date A Live The Movie: Mayuri Judgement (1080p)",
            "https://example.test/date-a-live-mayuri.torrent", {
                "infohash": "f" * 40,
                "files": {1: {
                    "path": "Date A Live The Movie Mayuri Judgement 1080p.mkv",
                    "size": 100,
                }},
            },
        )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        selected = select_replenishment_candidates(request, [candidate])
        self.assertEqual(selected["status"], "complete")
        self.assertEqual(selected["covered_gap_ids"], ["S00E05"])

    def test_tv_en_us_primary_failure_keeps_default_aliases(self) -> None:
        """The optional locale fetch must not discard a usable local primary."""
        plan = {
            "mode": "tv",
            "metadata": {
                "tmdb_id": 46004, "title": "约会大作战",
                "original_title": "デート・ア・ライブ", "media_type": "tv",
            },
            "scan_report": {"resource_gaps": []},
        }

        class FailingEnglishTMDB:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str | None]] = []

            def get(self, path: str, **params: object) -> dict[str, object]:
                language = params.get("language")
                language_name = language if isinstance(language, str) else None
                self.calls.append((path, language_name))
                if path == "/tv/46004" and language_name is None:
                    return {
                        "name": "约会大作战",
                        "original_name": "デート・ア・ライブ",
                    }
                if path == "/tv/46004" and language_name == "en-US":
                    raise RuntimeError("localized endpoint unavailable")
                if path == "/tv/46004/alternative_titles":
                    return {"results": []}
                raise AssertionError(f"unexpected TMDB endpoint: {path} {language_name}")

        tmdb = FailingEnglishTMDB()
        enriched = enrich_replenishment_plan_aliases(plan, tmdb)
        self.assertEqual(tmdb.calls, [
            ("/tv/46004", None),
            ("/tv/46004", "en-US"),
            ("/tv/46004/alternative_titles", None),
        ])
        self.assertEqual(
            enriched["metadata"]["aliases"],
            ["约会大作战", "デート・ア・ライブ"],
        )

    def test_primary_movie_titles_survive_unavailable_alternative_titles(self) -> None:
        plan = {
            "mode": "movie",
            "target_root": "/quark/影视/电影/本地片名 (2024)",
            "metadata": {
                "tmdb_id": 42,
                "title": "本地片名",
                "original_title": "本地片名",
                "year": "2024",
                "media_type": "movie",
            },
            "scan_report": {"resource_gaps": []},
        }

        class FakeTMDB:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get(self, path: str) -> dict[str, object]:
                self.calls.append(path)
                if path == "/movie/42":
                    return {
                        "title": "Canonical Release Title",
                        "original_title": "Original Release Title",
                    }
                raise RuntimeError("alternative titles temporarily unavailable")

        tmdb = FakeTMDB()
        enriched = enrich_replenishment_plan_aliases(plan, tmdb)
        aliases = enriched["metadata"]["aliases"]
        self.assertEqual(tmdb.calls, [
            "/movie/42", "/movie/42/alternative_titles",
        ])
        self.assertIn("Canonical Release Title", aliases)
        self.assertIn("Original Release Title", aliases)

    def test_s00_mushishi_movie_aliases_require_tv_and_special_identity(self) -> None:
        plan = {
            "mode": "tv",
            "target_root": "/quark/影视/番剧/虫师",
            "metadata": {
                "tmdb_id": 26867, "title": "虫师", "original_title": "蟲師",
                "media_type": "tv",
            },
            "scan_report": {"resource_gaps": [{
                "id": "S00E06", "kind": "missing_episode",
                "label": "虫师 S00E06", "season": 0, "episode": 6,
                "title": "鈴之滴", "title_aliases": ["旧本地别名"],
            }]},
        }

        class FakeTMDB:
            def __init__(self) -> None:
                self.calls: list[tuple[str, dict[str, object]]] = []

            def get(self, path: str, **params: object) -> dict[str, object]:
                self.calls.append((path, params))
                if path == "/tv/26867":
                    return {"name": "Mushishi", "original_name": "蟲師"}
                if path == "/tv/26867/alternative_titles":
                    return {"results": [{"title": "Mushishi"}]}
                if path == "/search/movie":
                    self.assert_query = params.get("query")
                    return {"results": [
                        {"id": 2686706, "title": "蟲師 鈴之滴"},
                        {"id": 999, "title": "鈴之滴"},
                    ]}
                if path == "/movie/2686706":
                    return {
                        "id": 2686706,
                        "title": "Mushishi: The Bell Droplet",
                        "original_title": "蟲師 鈴之滴",
                    }
                if path == "/movie/2686706/alternative_titles":
                    return {"titles": [
                        {"title": "Mushishi - Bell Droplets", "iso_3166_1": "US"},
                    ]}
                raise AssertionError(f"unexpected TMDB endpoint: {path}")

        tmdb = FakeTMDB()
        enriched = enrich_replenishment_plan_aliases(plan, tmdb)
        gap = enriched["scan_report"]["resource_gaps"][0]
        self.assertIn(("/search/movie", {"query": "鈴之滴"}), tmdb.calls)
        self.assertIn("Mushishi: The Bell Droplet", gap["title_aliases"])
        self.assertIn("Mushishi - Bell Droplets", gap["title_aliases"])
        self.assertIn("旧本地别名", gap["title_aliases"])

    def test_s00_date_a_live_movie_aliases_use_the_explicit_special_title(self) -> None:
        plan = {
            "mode": "tv",
            "target_root": "/quark/影视/番剧/约会大作战",
            "metadata": {
                "tmdb_id": 46004, "title": "约会大作战",
                "original_title": "デート・ア・ライブ", "media_type": "tv",
            },
            "scan_report": {"resource_gaps": [{
                "id": "S00E05", "kind": "missing_episode",
                "label": "约会大作战 S00E05", "season": 0, "episode": 5,
                "title": "万由里裁决", "title_aliases": ["第5話"],
            }]},
        }

        class FakeTMDB:
            def get(self, path: str, **params: object) -> dict[str, object]:
                if path == "/tv/46004":
                    return {"name": "Date A Live", "original_name": "デート・ア・ライブ"}
                if path == "/tv/46004/alternative_titles":
                    return {"results": [{"title": "Date A Live"}]}
                if path == "/search/movie":
                    self.query = params.get("query")
                    return {"results": [{
                        "id": 4600405,
                        "title": "约会大作战：万由里裁决",
                        "original_title": "劇場版 デート・ア・ライブ 万由里ジャッジメント",
                    }]}
                if path == "/movie/4600405":
                    return {
                        "id": 4600405,
                        "title": "Date A Live Movie: Mayuri Judgment",
                        "original_title": "劇場版 デート・ア・ライブ 万由里ジャッジメント",
                    }
                if path == "/movie/4600405/alternative_titles":
                    return {"titles": [{"title": "Mayuri Judgment"}]}
                raise AssertionError(f"unexpected TMDB endpoint: {path}")

        tmdb = FakeTMDB()
        enriched = enrich_replenishment_plan_aliases(plan, tmdb)
        gap = enriched["scan_report"]["resource_gaps"][0]
        self.assertEqual(tmdb.query, "万由里裁决")
        self.assertIn("Date A Live Movie: Mayuri Judgment", gap["title_aliases"])
        self.assertIn("Mayuri Judgment", gap["title_aliases"])

    def test_s00_movie_alias_enrichment_is_fail_closed_for_ambiguity_or_failure(self) -> None:
        def make_plan(title: str) -> dict[str, object]:
            return {
                "mode": "tv",
                "metadata": {
                    "tmdb_id": 26867, "title": "虫师", "original_title": "蟲師",
                    "media_type": "tv",
                },
                "scan_report": {"resource_gaps": [{
                    "id": "S00E06", "kind": "missing_episode",
                    "label": "虫师 S00E06", "season": 0, "episode": 6,
                    "title": title, "title_aliases": ["原有别名"],
                }]},
            }

        class AmbiguousTMDB:
            def __init__(self) -> None:
                self.movie_calls = 0

            def get(self, path: str, **params: object) -> dict[str, object]:
                if path == "/tv/26867":
                    return {"name": "Mushishi", "original_name": "蟲師"}
                if path == "/tv/26867/alternative_titles":
                    return {"results": []}
                if path == "/search/movie":
                    return {"results": [
                        {"id": 1, "title": "蟲師 鈴之滴"},
                        {"id": 2, "title": "Mushishi 鈴之滴"},
                    ]}
                self.movie_calls += 1
                raise AssertionError("ambiguous search must not fetch movie details")

        ambiguous = AmbiguousTMDB()
        result = enrich_replenishment_plan_aliases(make_plan("鈴之滴"), ambiguous)
        self.assertEqual(result["scan_report"]["resource_gaps"][0]["title_aliases"], ["原有别名"])
        self.assertEqual(ambiguous.movie_calls, 0)

        class FailedDetailsTMDB:
            def get(self, path: str, **params: object) -> dict[str, object]:
                if path == "/tv/26867":
                    return {"name": "Mushishi", "original_name": "蟲師"}
                if path == "/tv/26867/alternative_titles":
                    return {"results": []}
                if path == "/search/movie":
                    return {"results": [{"id": 3, "title": "蟲師 鈴之滴"}]}
                if path == "/movie/3":
                    raise RuntimeError("TMDB movie details unavailable")
                raise AssertionError(f"unexpected TMDB endpoint: {path}")

        failed = enrich_replenishment_plan_aliases(make_plan("鈴之滴"), FailedDetailsTMDB())
        self.assertEqual(failed["scan_report"]["resource_gaps"][0]["title_aliases"], ["原有别名"])

    def test_s00_movie_alias_enrichment_skips_large_special_batches(self) -> None:
        gaps = [{
            "id": f"S00E{index:02d}", "kind": "missing_episode",
            "label": f"虫师 S00E{index:02d}", "season": 0, "episode": index,
            "title": f"鈴之滴 {index}",
        } for index in range(1, 5)]
        plan = {
            "mode": "tv",
            "metadata": {
                "tmdb_id": 26867, "title": "虫师", "original_title": "蟲師",
                "media_type": "tv",
            },
            "scan_report": {"resource_gaps": gaps},
        }

        class CountingTMDB:
            def __init__(self) -> None:
                self.search_calls = 0

            def get(self, path: str, **params: object) -> dict[str, object]:
                if path == "/tv/26867":
                    return {"name": "Mushishi"}
                if path == "/tv/26867/alternative_titles":
                    return {"results": []}
                if path == "/search/movie":
                    self.search_calls += 1
                raise AssertionError(f"unexpected TMDB endpoint: {path}")

        tmdb = CountingTMDB()
        enriched = enrich_replenishment_plan_aliases(plan, tmdb)
        self.assertEqual(tmdb.search_calls, 0)
        self.assertNotIn("title_aliases", enriched["scan_report"]["resource_gaps"][0])

    def test_gap_state_redacts_configured_credentials_before_persistence(self) -> None:
        password = "gap-password-not-public"
        token = "gap-token-not-public"
        api_key = "gap-api-key-not-public"
        with patch.dict(
            "os.environ",
            {
                "SCRAPEFLOW_TEST_PASSWORD": password,
                "SCRAPEFLOW_TEST_TOKEN": token,
                "SCRAPEFLOW_TEST_API_KEY": api_key,
            },
            clear=False,
        ):
            with tempfile.TemporaryDirectory() as temporary:
                alist = MemoryAList()
                runtime = AutomaticReplenishmentRuntime(
                    Path(temporary),
                    engine_runner=FakeEngine(),
                    alist=alist,
                    search=FakeSearch(),
                    materializer=FakeMaterializer(alist),
                )
                path = runtime._write_gap(  # noqa: SLF001 - durable boundary
                    {
                        "id": "S01E01",
                        "job_id": "redaction-root",
                        "phase": "retry_wait",
                        "error": (
                            f"password={password}; token={token}; api_key={api_key}"
                        ),
                        "outcome": {
                            "error": RuntimeError(
                                f"provider password={password}; token={token}"
                            ),
                        },
                        "gap": {"authorization": token},
                    }
                )
                persisted_gap = path.read_text(encoding="utf-8")

        for secret in (password, token, api_key):
            self.assertNotIn(secret, persisted_gap)
        self.assertIn("<redacted>", persisted_gap)

    def test_one_gap_runs_search_staging_child_and_owned_cleanup(self) -> None:
        plan = {
            "mode": "tv",
            "source_root": "/quark/影视/待刮削/Example Show",
            "target_root": "/quark/影视/番剧/Example Show",
            "metadata": {
                "tmdb_id": 7, "title": "Example Show", "original_title": "Example Show",
                "year": "2020", "series_root": "/quark/影视/番剧/Example Show",
            },
            "scan_report": {"resource_gaps": [{
                "id": "S01E01", "kind": "missing_episode", "label": "Example Show S01E01",
                "reason": "missing", "files": [],
            }]},
        }
        root_job = EngineJob(
            id="engine-root", phase="executed",
            created_at="2026-08-07T00:00:00Z", updated_at="2026-08-07T00:00:00Z",
            request={
                "source_path": "/quark/影视/待刮削/Example Show",
                "parent_path": "/quark/影视/番剧", "media_type": "tv", "tmdb_id": 7,
                "season": 1,
            }, plan=plan, summary={"mode": "tv"},
        )
        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            search = FakeSearch()
            materializer = FakeMaterializer(alist)
            engine = FakeEngine()
            progress: list[str] = []
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=search, materializer=materializer,
                staging_root="/quark/影视/ScrapeFlow/补源",
                max_candidate_rounds=1,
                progress=lambda _job, phase, _details: progress.append(phase),
            )

            outcome = runtime.run_for_job(root_job)

            self.assertEqual(outcome["unresolved_gaps"], [])
            self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], ["S01E01"])
            self.assertEqual(len(search.requests), 1)
            self.assertEqual(len(engine.executed), 1)
            self.assertEqual(engine.internal_children, [("engine-child-1", "engine-root")])
            self.assertEqual(
                progress,
                [
                    "gap_discovering", "provider_searching", "acquiring",
                    "staging_verifying", "child_planning", "child_executing",
                    "final_verifying", "cleaning",
                ],
            )
            self.assertTrue(engine.planned[0]["source_path"].startswith(
                "/quark/影视/ScrapeFlow/补源/engine-root/attempt-",
            ))
            # The attempt and its empty per-job parent are gone; the shared
            # staging root itself is never removed.
            self.assertEqual(alist.tree["/quark/影视/ScrapeFlow/补源"], [])
            state_files = list((Path(temporary) / "gaps").rglob("*.json"))
            self.assertEqual(len(state_files), 1)
            state = json.loads(state_files[0].read_text(encoding="utf-8"))
            self.assertEqual(state["phase"], "resolved")

            # A provider retry updates the same state instead of creating a
            # second random gap record.
            again = runtime.run_for_job(root_job)
            self.assertEqual(again["already_resolved_gap_ids"], ["S01E01"])
            self.assertEqual(len(list((Path(temporary) / "gaps").rglob("*.json"))), 1)
            self.assertEqual(len(search.requests), 1)

            audit_plan = {
                **plan,
                "scan_report": {"resource_gaps": [{
                    **plan["scan_report"]["resource_gaps"][0],
                    "source": "automatic_library_audit",
                }]},
            }
            audit_job = replace(root_job, id="engine-audit-repeat", plan=audit_plan)
            runtime.run_for_job(audit_job)
            audit_again = runtime.run_for_job(audit_job)
            self.assertEqual(audit_again["already_resolved_gap_ids"], [])
            self.assertEqual(len(search.requests), 3)

    def test_delivery_contract_rejects_formal_library_fields_before_child(self) -> None:
        root_job = _example_root_job("engine-delivery-formal-field")

        class FormalFieldMaterializer(FakeMaterializer):
            def acquire(self, request, selections, *, staging_root, workspace, alist):
                delivery = super().acquire(
                    request, selections, staging_root=staging_root,
                    workspace=workspace, alist=alist,
                )
                files = delivery["files"]
                assert isinstance(files, list)
                files[0]["target_root"] = "/quark/影视/番剧/Example Show"
                return delivery

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            engine = FakeEngine()
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=FakeSearch(), materializer=FormalFieldMaterializer(alist),
                staging_root="/quark/影视/ScrapeFlow/补源",
                max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)

        self.assertIn("正式库字段", str(outcome["outcomes"][0]["error"]))
        self.assertEqual(engine.planned, [])
        self.assertEqual(engine.executed, [])

    def test_delivery_contract_requires_declared_files_to_match_alist_readback(self) -> None:
        root_job = _example_root_job("engine-delivery-readback-mismatch")

        class SizeMismatchMaterializer(FakeMaterializer):
            def acquire(self, request, selections, *, staging_root, workspace, alist):
                delivery = super().acquire(
                    request, selections, staging_root=staging_root,
                    workspace=workspace, alist=alist,
                )
                files = delivery["files"]
                assert isinstance(files, list)
                files[0]["size"] = 456
                return delivery

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            engine = FakeEngine()
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=FakeSearch(), materializer=SizeMismatchMaterializer(alist),
                staging_root="/quark/影视/ScrapeFlow/补源",
                max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)

        self.assertIn("AList 回读不一致", str(outcome["outcomes"][0]["error"]))
        self.assertEqual(engine.planned, [])
        self.assertEqual(engine.executed, [])

    def test_quark_share_fast_save_wins_before_local_torrent(self) -> None:
        root_job = _example_root_job("engine-quark-share-root")

        class ShareFirstSearch(FakeSearch):
            def run(self, request):
                self.requests.append(dict(request))
                return {"candidates": [{
                    "provider": "quark_share",
                    "locator": "quark_share:fixture-share",
                    "release_name": "Example Show S01E01 1080p",
                    "title": "Example Show",
                    "year": "2020",
                    "files": ["Example.Show.S01E01.mkv"],
                    "file_coverage": ["S01E01"],
                    "acquisition": {
                        "kind": "quark_fast_save",
                        "share_id": "fixture-share",
                        "share_url": "https://pan.quark.cn/s/fixture-share",
                        "file_id_by_gap": {"S01E01": ["share-fid"]},
                        "file_path_by_id": {"share-fid": "Example.Show.S01E01.mkv"},
                        "file_size_by_id": {"share-fid": 123},
                        "save_strategy": "server_side_copy",
                        "requires_share_revalidation": True,
                    },
                }, {
                    "provider": "magnet",
                    "locator": "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
                    "release_name": "Example Show S01E01 1080p",
                    "title": "Example Show",
                    "year": "2020",
                    "files": ["Example.Show.S01E01.mkv"],
                    "acquisition": {"kind": "torrent"},
                }]}

        class FakeQuarkBridge:
            def __init__(self, alist: MemoryAList) -> None:
                self.alist = alist
                self.calls: list[tuple[str, object]] = []

            def execute(self, selection, destination, session):
                self.calls.append((destination, session))
                self.alist.tree[destination] = [{
                    "name": "Example.Show.S01E01.mkv",
                    "is_dir": False,
                    "size": 123,
                }]
                return {
                    "status": "submitted",
                    "task_id": "quark-task-1",
                    "expected_files": [{
                        "name": "Example.Show.S01E01.mkv",
                        "size": 123,
                        "gap_ids": ["S01E01"],
                    }],
                }

        class UnexpectedLocalTorrent:
            def acquire(self, *_args, **_kwargs):
                raise AssertionError("local Torrent must not run after quark_share success")

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            engine = FakeEngine()
            bridge = FakeQuarkBridge(alist)
            materializer = FixedTierAutomaticMaterializer(
                quark_share=QuarkFastSaveAutomaticMaterializer(
                    bridge=bridge,
                    session_factory=lambda _alist, _staging: object(),
                ),
                local_torrent=UnexpectedLocalTorrent(),
            )
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=ShareFirstSearch(), materializer=materializer,
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)

        self.assertEqual(outcome["unresolved_gaps"], [])
        self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], ["S01E01"])
        self.assertEqual(len(bridge.calls), 1)
        self.assertEqual(engine.executed, ["engine-child-1"])
        self.assertTrue(engine.planned[0]["source_path"].startswith(
            "/quark/影视/ScrapeFlow/补源/engine-quark-share-root/attempt-",
        ))

    def test_quark_magnet_offline_wins_before_local_torrent(self) -> None:
        root_job = _example_root_job("engine-quark-magnet-root")
        infohash = "0123456789012345678901234567890123456789"

        class MagnetFirstSearch(FakeSearch):
            def run(self, request):
                self.requests.append(dict(request))
                return {"candidates": [{
                    "provider": "quark_magnet",
                    "locator": f"quark_magnet:{infohash}",
                    "release_name": "Example Show S01E01 1080p",
                    "title": "Example Show",
                    "year": "2020",
                    "files": ["Example/Example.Show.S01E01.mkv"],
                    "file_coverage": ["S01E01"],
                    "acquisition": {
                        "kind": "quark_magnet_offline",
                        "magnet_url": f"magnet:?xt=urn:btih:{infohash}",
                        "expected_files": [{
                            "torrent_index": 1,
                            "path": "Example/Example.Show.S01E01.mkv",
                            "size": 123,
                            "gap_ids": ["S01E01"],
                        }],
                    },
                }, {
                    "provider": "magnet",
                    "locator": (
                        "magnet:?xt=urn:btih:"
                        "0123456789012345678901234567890123456789"
                    ),
                    "release_name": "Example Show S01E01 1080p",
                    "title": "Example Show",
                    "year": "2020",
                    "files": ["Example.Show.S01E01.mkv"],
                    "acquisition": {"kind": "torrent"},
                }]}

        class FakeQuarkMagnetBridge:
            def __init__(self, alist: MemoryAList) -> None:
                self.alist = alist
                self.calls: list[str] = []

            def execute(self, selection, destination, *, task_id=None, on_task_id=None):
                if task_id is None and on_task_id is not None:
                    on_task_id("quark-magnet-task-1")
                self.calls.append(destination)
                media_dir = f"{destination}/Example"
                self.alist.mkdir(media_dir)
                self.alist.tree[media_dir] = [{
                    "name": "Example.Show.S01E01.mkv",
                    "is_dir": False,
                    "size": 123,
                }]
                return {
                    "status": "submitted",
                    "task_id": task_id or "quark-magnet-task-1",
                    "expected_files": [{
                        "path": "Example/Example.Show.S01E01.mkv",
                        "size": 123,
                        "gap_ids": ["S01E01"],
                    }],
                }

        class UnexpectedLocalTorrent:
            def acquire(self, *_args, **_kwargs):
                raise AssertionError("local Torrent must not run after quark_magnet success")

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            engine = FakeEngine()
            bridge = FakeQuarkMagnetBridge(alist)
            materializer = FixedTierAutomaticMaterializer(
                quark_magnet=QuarkMagnetAutomaticMaterializer(bridge=bridge),
                local_torrent=UnexpectedLocalTorrent(),
            )
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=MagnetFirstSearch(), materializer=materializer,
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)

        self.assertEqual(outcome["unresolved_gaps"], [])
        self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], ["S01E01"])
        self.assertEqual(len(bridge.calls), 1)
        self.assertEqual(engine.executed, ["engine-child-1"])
        self.assertTrue(engine.planned[0]["source_path"].startswith(
            "/quark/影视/ScrapeFlow/补源/engine-quark-magnet-root/attempt-",
        ))

    def test_quark_magnet_materializer_reuses_persisted_task_id(self) -> None:
        infohash = "0123456789012345678901234567890123456789"
        selection = {
            "provider": "quark_magnet",
            "locator": f"quark_magnet:{infohash}",
            "release_name": "Example Show S01E01 1080p",
            "selected_gap_ids": ["S01E01"],
            "acquisition": {
                "kind": "quark_magnet_offline",
                "magnet_url": f"magnet:?xt=urn:btih:{infohash}",
                "expected_files": [{
                    "torrent_index": 1,
                    "path": "Example.Show.S01E01.mkv",
                    "size": 123,
                    "gap_ids": ["S01E01"],
                }],
            },
        }

        class ReusingBridge:
            def __init__(self) -> None:
                self.task_ids: list[str | None] = []

            def execute(self, _selection, _destination, *, task_id=None, on_task_id=None):
                self.task_ids.append(task_id)
                if task_id is None:
                    task_id = "quark-magnet-task-1"
                    if on_task_id is not None:
                        on_task_id(task_id)
                return {
                    "status": "submitted",
                    "task_id": task_id,
                    "expected_files": [{
                        "path": "Example.Show.S01E01.mkv",
                        "size": 123,
                        "gap_ids": ["S01E01"],
                    }],
                }

        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary) / "workspace"
            staging = "/quark/影视/ScrapeFlow/补源/root/attempt-reuse"
            bridge = ReusingBridge()
            materializer = QuarkMagnetAutomaticMaterializer(bridge=bridge)
            alist = MemoryAList()

            first = materializer.acquire(
                {},
                [selection],
                staging_root=staging,
                workspace=workspace,
                alist=alist,
            )
            second = materializer.acquire(
                {},
                [selection],
                staging_root=staging,
                workspace=workspace,
                alist=alist,
            )

        self.assertEqual(bridge.task_ids, [None, "quark-magnet-task-1"])
        self.assertEqual(first["external_task_id"], "quark-magnet-task-1")
        self.assertEqual(second["external_task_id"], "quark-magnet-task-1")

    def test_partial_child_output_does_not_resolve_unwritten_episode(self) -> None:
        plan = {
            "mode": "tv",
            "source_root": "/quark/影视/待刮削/Example Show",
            "target_root": "/quark/影视/番剧/Example Show",
            "metadata": {
                "tmdb_id": 7, "title": "Example Show", "original_title": "Example Show",
                "year": "2020", "series_root": "/quark/影视/番剧/Example Show",
            },
            "scan_report": {"resource_gaps": [
                {"id": "ignored", "kind": "missing_episode", "label": "Example Show S01E01", "reason": "missing"},
                {"id": "ignored-too", "kind": "missing_episode", "label": "Example Show S01E02", "reason": "missing"},
            ]},
        }
        root_job = EngineJob(
            id="engine-partial-root", phase="executed",
            created_at="2026-08-07T00:00:00Z", updated_at="2026-08-07T00:00:00Z",
            request={
                "source_path": "/quark/影视/待刮削/Example Show",
                "parent_path": "/quark/影视/番剧", "media_type": "tv", "tmdb_id": 7,
                "season": 1,
            }, plan=plan, summary={"mode": "tv"},
        )

        class PartialSearch(FakeSearch):
            def run(self, request):
                self.requests.append(dict(request))
                return {"candidates": [{
                    "provider": "magnet",
                    "locator": "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
                    "release_name": "Example Show S01E01-E02 1080p",
                    "title": "Example Show", "year": "2020",
                    "files": ["Example.Show.S01E01.mkv"],
                    "acquisition": {"kind": "torrent"},
                }]}

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=FakeEngine(), alist=alist,
                search=PartialSearch(), materializer=FakeMaterializer(alist),
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)
            states = {
                path.stem: json.loads(path.read_text(encoding="utf-8"))
                for path in (Path(temporary) / "gaps" / root_job.id).glob("*.json")
            }

        self.assertTrue(any(row.get("error") for row in outcome["outcomes"]))
        self.assertEqual(states["S01E01"]["phase"], "resolved")
        self.assertEqual(states["S01E02"]["phase"], "retry_wait")

    def test_known_movie_gap_uses_the_same_child_pipeline(self) -> None:
        plan = {
            "mode": "movie",
            "source_root": "/quark/影视/待刮削/Example Movie",
            "target_root": "/quark/影视/电影/Example Movie",
            "metadata": {
                "tmdb_id": 8, "title": "Example Movie", "original_title": "Example Movie",
                "year": "2020", "media_type": "movie",
            },
            "scan_report": {"resource_gaps": [{
                "id": "missing_media:8:Example Movie",
                "kind": "missing_media",
                "label": "Example Movie",
                "reason": "正式库缺少正片",
            }]},
        }
        root_job = EngineJob(
            id="engine-movie-root", phase="executed",
            created_at="2026-08-07T00:00:00Z", updated_at="2026-08-07T00:00:00Z",
            request={
                "source_path": "/quark/影视/待刮削/Example Movie",
                "parent_path": "/quark/影视/电影", "media_type": "movie", "tmdb_id": 8,
            }, plan=plan, summary={"mode": "movie"},
        )

        class MovieSearch:
            def run(self, _request):
                return {"candidates": [{
                    "provider": "magnet",
                    "locator": "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
                    "release_name": "Example Movie 2020 1080p",
                    "title": "Example Movie",
                    "year": "2020",
                    "files": ["Example.Movie.2020.mkv"],
                    "acquisition": {"kind": "torrent"},
                }]}

        class MovieMaterializer(FakeMaterializer):
            def acquire(self, request, selections, *, staging_root, workspace, alist):
                super().acquire(
                    request, selections, staging_root=staging_root,
                    workspace=workspace, alist=alist,
                )
                self.alist.tree[staging_root] = [{
                    "name": "Example.Movie.2020.mkv", "is_dir": False, "size": 456,
                }]
                return _ready_delivery(
                    staging_root,
                    [{
                        "path": f"{staging_root}/Example.Movie.2020.mkv",
                        "size": 456,
                        "kind": "video",
                        "gap_ids": ["missing_media:8:Example Movie"],
                    }],
                )

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            engine = FakeEngine()
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=MovieSearch(), materializer=MovieMaterializer(alist),
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)

        self.assertEqual(
            outcome["outcomes"][0]["resolved_gap_ids"],
            ["missing_media:8:Example Movie"],
        )
        self.assertEqual(engine.planned[0]["media_type"], "movie")
        self.assertEqual(engine.executed, ["engine-child-1"])

    def test_subtitle_gap_uses_sidecar_lane_without_video_child(self) -> None:
        """A missing subtitle must not download or re-organize the video again."""
        video_path = "/quark/影视/番剧/Example Show/Season 01/Example.Show.S01E01.mkv"
        subtitle_gap_id = "missing_subtitle:7:S01E01:zh"
        plan = {
            "mode": "tv",
            "source_root": "/quark/影视/待刮削/Example Show",
            "target_root": "/quark/影视/番剧/Example Show",
            "metadata": {
                "tmdb_id": 7, "title": "Example Show", "original_title": "Example Show",
                "year": "2020", "series_root": "/quark/影视/番剧/Example Show",
            },
            "scan_report": {"resource_gaps": [{
                "id": subtitle_gap_id,
                "kind": "missing_subtitle",
                "label": "Example Show S01E01 中文字幕",
                "reason": "缺少中文字幕",
                "path": video_path,
                "subtitle_language": "zh",
            }]},
        }
        root_job = EngineJob(
            id="engine-subtitle-root", phase="executed",
            created_at="2026-08-07T00:00:00Z", updated_at="2026-08-07T00:00:00Z",
            request={
                "source_path": "/quark/影视/待刮削/Example Show",
                "parent_path": "/quark/影视/番剧", "media_type": "tv", "tmdb_id": 7,
            }, plan=plan, summary={"mode": "tv"},
        )

        class SubtitleSearch:
            def run(self, _request):
                return {"candidates": [{
                    "provider": "magnet",
                    "locator": "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
                    "release_name": "Example Show S01E01 中文字幕",
                    "title": "Example Show",
                    "year": "2020",
                    "files": ["Example.Show.S01E01.zh.srt"],
                    "file_coverage": [subtitle_gap_id],
                    "acquisition": {
                        "kind": "torrent",
                        "file_index_by_gap": {subtitle_gap_id: [1]},
                    },
                }]}

        class SubtitleMaterializer(FakeMaterializer):
            def acquire(self, request, selections, *, staging_root, workspace, alist):
                del request, selections, workspace, alist
                self.calls.append(staging_root)
                parent = posixpath.dirname(staging_root)
                self.alist.mkdir(parent)
                self.alist.mkdir(staging_root)
                source = f"{staging_root}/{subtitle_gap_id} - Example.Show.S01E01.zh.srt"
                self.alist.tree[staging_root] = [{
                    "name": posixpath.basename(source), "is_dir": False, "size": 321,
                }]
                return _ready_delivery(
                    staging_root,
                    [{
                        "path": source,
                        "size": 321,
                        "kind": "subtitle",
                        "gap_ids": [subtitle_gap_id],
                    }],
                )

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            engine = FakeSubtitleEngine()
            progress: list[str] = []
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=SubtitleSearch(), materializer=SubtitleMaterializer(alist),
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=1,
                progress=lambda _job, phase, _details: progress.append(phase),
            )

            outcome = runtime.run_for_job(root_job)

            self.assertEqual(outcome["unresolved_gaps"], [])
            self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], [subtitle_gap_id])
            self.assertNotIn("child_job_id", outcome["outcomes"][0])
            self.assertEqual(engine.planned, [])
            self.assertEqual(engine.executed, [])
            self.assertEqual(len(engine.installed_subtitles), 1)
            installed = engine.installed_subtitles[0]
            self.assertTrue(str(installed["source_path"]).endswith(
                f"/{subtitle_gap_id} - Example.Show.S01E01.zh.srt",
            ))
            self.assertEqual(
                installed["target_path"],
                "/quark/影视/番剧/Example Show/Season 01/Example.Show.S01E01.zh.srt",
            )
            self.assertEqual(installed["expected_size"], 321)
            self.assertEqual(installed["video_path"], video_path)
            self.assertEqual(
                progress,
                [
                    "gap_discovering", "provider_searching", "acquiring",
                    "staging_verifying", "subtitle_installing", "final_verifying", "cleaning",
                ],
            )
            self.assertEqual(alist.tree["/quark/影视/ScrapeFlow/补源"], [])

    def test_mixed_media_request_installs_only_its_explicit_subtitle_gap(self) -> None:
        """A video child may reuse an exact subtitle companion after its move."""
        root_job = _example_root_job("engine-mixed-subtitle-root")
        subtitle_id = "missing_subtitle:7:S01E02:zh"
        subtitle_video = (
            "/quark/影视/番剧/Example Show/Season 01/"
            "Example.Show.S01E02.mkv"
        )
        plan = dict(root_job.plan)
        plan["scan_report"] = {"resource_gaps": [
            {
                "id": "S01E01", "kind": "missing_episode",
                "season": 1, "episodes": [1],
                "label": "Example Show S01E01", "title": "One",
                "reason": "missing",
            },
            {
                "id": subtitle_id, "kind": "missing_subtitle",
                "label": "Example Show S01E02 中文字幕",
                "reason": "缺少中文字幕", "path": subtitle_video,
                "subtitle_language": "zh",
            },
        ]}
        root_job = replace(root_job, plan=plan)

        class MixedSearch:
            def run(self, _request):
                return {"candidates": [{
                    "provider": "magnet",
                    "locator": "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
                    "release_name": "Example Show S01E01 1080p",
                    "title": "Example Show", "year": "2020",
                    "files": [
                        "Example.Show.S01E01.mkv",
                        "Example.Show.S01E02.zh.srt",
                    ],
                    "file_coverage": ["S01E01", subtitle_id],
                    "acquisition": {
                        "kind": "torrent",
                        "file_index_by_gap": {
                            "S01E01": [1], subtitle_id: [2],
                        },
                    },
                }]}

        class MixedMaterializer(FakeMaterializer):
            def acquire(self, request, selections, *, staging_root, workspace, alist):
                del request, selections, workspace, alist
                self.calls.append(staging_root)
                parent = posixpath.dirname(staging_root)
                self.alist.mkdir(parent)
                self.alist.mkdir(staging_root)
                media_root = f"{staging_root}/media"
                subtitle_root = f"{staging_root}/subtitles"
                self.alist.mkdir(media_root)
                self.alist.mkdir(subtitle_root)
                video = f"{media_root}/S01E01 - Example.Show.S01E01.mkv"
                subtitle = f"{subtitle_root}/{subtitle_id} - Example.Show.S01E02.zh.srt"
                self.alist.tree[staging_root] = [
                    {"name": "media", "is_dir": True},
                    {"name": "subtitles", "is_dir": True},
                ]
                self.alist.tree[media_root] = [
                    {"name": posixpath.basename(video), "is_dir": False, "size": 123},
                ]
                self.alist.tree[subtitle_root] = [
                    {"name": posixpath.basename(subtitle), "is_dir": False, "size": 321},
                ]
                return _ready_delivery(
                    staging_root,
                    [
                        {
                            "path": video, "size": 123,
                            "kind": "video", "gap_ids": ["S01E01"],
                        },
                        {
                            "path": subtitle, "size": 321,
                            "kind": "subtitle", "gap_ids": [subtitle_id],
                        },
                    ],
                    media_staging_root=media_root,
                    subtitle_staging_root=subtitle_root,
                )

        class StrictPlannerEngine(FakeSubtitleEngine):
            """Approximate the production planner's subtitle rejection gate."""

            def __init__(self, alist):
                super().__init__()
                self.alist = alist

            def plan_job(self, request, *, internal_child_of=None):
                source = str(request.get("source_path") or "")
                if not source.endswith("/media"):
                    raise AssertionError("mixed child planner received the parent staging root")
                rows = self.alist.list(source, refresh=True)
                if any(
                    not row.get("is_dir")
                    and Path(str(row.get("name") or "")).suffix.casefold()
                    in {".ass", ".idx", ".srt", ".ssa", ".sub", ".sup", ".vtt"}
                    for row in rows
                ):
                    raise AssertionError("subtitle leaked into the media child planner")
                return super().plan_job(request, internal_child_of=internal_child_of)

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            engine = StrictPlannerEngine(alist)
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=MixedSearch(), materializer=MixedMaterializer(alist),
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)

        self.assertEqual(
            outcome["outcomes"][0]["resolved_gap_ids"],
            ["S01E01", subtitle_id],
        )
        self.assertEqual(engine.executed, ["engine-child-1"])
        # The real planner receives only the media lane.  If the subtitle
        # sibling leaked into this source root, strict TV planning would mark
        # it as a problem file and the provider child could not be committed.
        self.assertTrue(engine.planned[0]["source_path"].endswith("/media"))
        self.assertEqual(len(engine.installed_subtitles), 1)
        installed = engine.installed_subtitles[0]
        self.assertIn("/subtitles/", str(installed["source_path"]))
        self.assertEqual(installed["video_path"], subtitle_video)
        self.assertEqual(
            installed["target_path"],
            "/quark/影视/番剧/Example Show/Season 01/Example.Show.S01E02.zh.srt",
        )
        self.assertEqual(installed["expected_size"], 321)

    def _run_new_media_companion(
        self, companion_member: str | None, *, child_status: str = "moved",
    ) -> tuple[dict[str, object], FakeSubtitleEngine]:
        """Exercise one media-only request with an optional new-video sidecar.

        The request intentionally has no ``missing_subtitle`` row: before the
        video exists there is no audited formal path for the ordinary sidecar
        lane.  A valid companion must therefore be bound to the newly moved
        child target, not to a guessed future name.
        """
        root_job = _example_root_job("engine-new-media-companion")
        plan = dict(root_job.plan)
        metadata = dict(plan["metadata"])
        metadata["aliases"] = ["Example Show", "示例剧集"]
        plan["metadata"] = metadata
        plan["scan_report"] = {"resource_gaps": [{
            "id": "S01E01", "kind": "missing_episode", "season": 1,
            "episodes": [1], "label": "Example Show S01E01", "title": "One",
            "reason": "missing",
        }]}
        root_job = replace(root_job, plan=plan)

        class CompanionSearch:
            def run(self, _request):
                files = ["Example.Show.S01E01.mkv"]
                acquisition: dict[str, object] = {
                    "kind": "torrent",
                    "file_index_by_gap": {"S01E01": [1]},
                    "file_size_by_index": {"1": 123},
                    "file_path_by_index": {"1": "Example.Show.S01E01.mkv"},
                }
                if companion_member is not None:
                    files.append(companion_member)
                    acquisition.update({
                        "companion_subtitle_index_by_media_gap": {"S01E01": [2]},
                        "file_size_by_index": {"1": 123, "2": 321},
                        "file_path_by_index": {
                            "1": "Example.Show.S01E01.mkv", "2": companion_member,
                        },
                    })
                return {"candidates": [{
                    "provider": "magnet",
                    "locator": "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
                    "release_name": "Example Show S01E01 1080p",
                    "title": "Example Show", "year": "2020",
                    "files": files, "file_coverage": ["S01E01"],
                    "acquisition": acquisition,
                }]}

        class CompanionMaterializer(FakeMaterializer):
            def acquire(self, _request, selections, *, staging_root, workspace, alist):
                del workspace, alist
                self.calls.append(staging_root)
                self.alist.mkdir(posixpath.dirname(staging_root))
                self.alist.mkdir(staging_root)
                selected_acquisition = dict(selections[0]["acquisition"])
                if companion_member is None:
                    video = f"{staging_root}/S01E01 - Example.Show.S01E01.mkv"
                    self.alist.tree[staging_root] = [{
                        "name": posixpath.basename(video), "is_dir": False, "size": 123,
                    }]
                    return _ready_delivery(
                        staging_root,
                        [{
                            "path": video, "size": 123, "kind": "video",
                            "manifest_index": 1, "gap_ids": ["S01E01"],
                        }],
                        **selected_acquisition,
                    )
                media_root = f"{staging_root}/media"
                subtitle_root = f"{staging_root}/subtitles"
                self.alist.mkdir(media_root)
                self.alist.mkdir(subtitle_root)
                video = f"{media_root}/S01E01 - Example.Show.S01E01.mkv"
                subtitle = f"{subtitle_root}/S01E01 - {Path(companion_member).name}"
                self.alist.tree[staging_root] = [
                    {"name": "media", "is_dir": True},
                    {"name": "subtitles", "is_dir": True},
                ]
                self.alist.tree[media_root] = [{
                    "name": posixpath.basename(video), "is_dir": False, "size": 123,
                }]
                self.alist.tree[subtitle_root] = [{
                    "name": posixpath.basename(subtitle), "is_dir": False, "size": 321,
                }]
                return _ready_delivery(
                    staging_root,
                    [
                        {
                            "path": video, "size": 123, "kind": "video",
                            "manifest_index": 1, "gap_ids": ["S01E01"],
                        },
                        {
                            "path": subtitle, "size": 321, "kind": "subtitle",
                            "manifest_index": 2, "gap_ids": ["S01E01"],
                            "companion_for_gap_ids": ["S01E01"],
                            "subtitle_language": "chs",
                        },
                    ],
                    media_staging_root=media_root,
                    subtitle_staging_root=subtitle_root,
                    **selected_acquisition,
                )

        class FreshMoveEngine(FakeSubtitleEngine):
            def execute_automatic(self, job_id):
                self.executed.append(job_id)
                source_root = str(self.planned[-1]["source_path"])
                source = f"{source_root}/S01E01 - Example.Show.S01E01.mkv"
                target = (
                    "/quark/影视/番剧/Example Show/Season 01/"
                    "Example.Show.S01E01.mkv"
                )
                return EngineJob(
                    id=job_id, phase="executed",
                    created_at="2026-08-07T00:00:00Z", updated_at="2026-08-07T00:00:01Z",
                    request=self.planned[-1], plan={}, summary={"mode": "tv"},
                    execution={"files": [{
                        "source": source, "target": target, "size": 123, "status": child_status,
                    }]},
                )

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            engine = FreshMoveEngine()
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=CompanionSearch(), materializer=CompanionMaterializer(alist),
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)
        return outcome, engine

    def test_new_media_exact_chs_companion_is_installed_after_fresh_child_move(self) -> None:
        """A missing episode can carry its exact CHS sidecar after its move."""
        outcome, engine = self._run_new_media_companion(
            "Example.Show.S01E01.CHS.ass",
        )

        # There was no missing_subtitle request gap, so the video is the only
        # resolved work coordinate while its newly created formal target gets
        # the companion sidecar with the same basename.
        self.assertEqual(outcome["unresolved_gaps"], [])
        self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], ["S01E01"])
        self.assertEqual(len(engine.installed_subtitles), 1)
        installed = engine.installed_subtitles[0]
        self.assertEqual(
            installed["target_path"],
            "/quark/影视/番剧/Example Show/Season 01/Example.Show.S01E01.zh.ass",
        )
        self.assertEqual(
            installed["video_path"],
            "/quark/影视/番剧/Example Show/Season 01/Example.Show.S01E01.mkv",
        )
        companions = outcome["outcomes"][0]["companion_subtitles"]
        self.assertEqual(len(companions), 1)
        self.assertTrue(companions[0]["companion"])

    def test_new_media_companion_rejects_wrong_episode_and_wrong_season(self) -> None:
        """A candidate's video still completes; mismatched sidecars never write."""
        for member in (
            "Example.Show.S01E02.CHS.ass",
            "Example.Show.S02E01.CHS.ass",
        ):
            with self.subTest(member=member):
                outcome, engine = self._run_new_media_companion(member)
                self.assertEqual(outcome["unresolved_gaps"], [])
                self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], ["S01E01"])
                self.assertEqual(engine.installed_subtitles, [])
                self.assertNotIn("companion_subtitles", outcome["outcomes"][0])

    def test_new_media_companion_rejects_bare_and_cross_work_members(self) -> None:
        """A coordinate/language tag alone cannot launder another work's sidecar."""
        for member in (
            "S01E01.CHS.ass",
            "Other.Show.S01E01.CHS.ass",
        ):
            with self.subTest(member=member):
                outcome, engine = self._run_new_media_companion(member)
                self.assertEqual(outcome["unresolved_gaps"], [])
                self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], ["S01E01"])
                self.assertEqual(engine.installed_subtitles, [])

    def test_new_media_companion_requires_a_fresh_child_video_move(self) -> None:
        """An idempotent/already-present child target must not receive a sidecar."""
        outcome, engine = self._run_new_media_companion(
            "Example.Show.S01E01.CHS.ass", child_status="already_present",
        )

        self.assertEqual(outcome["unresolved_gaps"], [])
        self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], ["S01E01"])
        self.assertEqual(engine.installed_subtitles, [])
        self.assertNotIn("companion_subtitles", outcome["outcomes"][0])

    def test_new_media_without_legal_companion_still_writes_media_only(self) -> None:
        """No candidate sidecar must not block the media child or synthesize one."""
        outcome, engine = self._run_new_media_companion(None)

        self.assertEqual(outcome["unresolved_gaps"], [])
        self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], ["S01E01"])
        self.assertEqual(engine.installed_subtitles, [])
        self.assertNotIn("companion_subtitles", outcome["outcomes"][0])

    def test_mixed_staging_passes_only_media_subroot_to_real_engine_planner(self) -> None:
        """The durable child plan must never present a sidecar to its planner."""
        root_job = _example_root_job("engine-isolated-planner-root")
        subtitle_id = "missing_subtitle:7:S01E02:zh"
        subtitle_video = (
            "/quark/影视/番剧/Example Show/Season 01/"
            "Example.Show.S01E02.mkv"
        )
        plan = dict(root_job.plan)
        plan["scan_report"] = {"resource_gaps": [
            {
                "id": "S01E01", "kind": "missing_episode",
                "season": 1, "episodes": [1],
                "label": "Example Show S01E01", "title": "One",
                "reason": "missing",
            },
            {
                "id": subtitle_id, "kind": "missing_subtitle",
                "label": "Example Show S01E02 中文字幕",
                "reason": "缺少中文字幕", "path": subtitle_video,
                "subtitle_language": "zh",
            },
        ]}
        root_job = replace(root_job, plan=plan)

        class MixedSearch:
            def run(self, _request):
                return {"candidates": [{
                    "provider": "magnet",
                    "locator": "magnet:?xt=urn:btih:0123456789012345678901234567890123456789",
                    "release_name": "Example Show S01E01 1080p",
                    "title": "Example Show", "year": "2020",
                    "files": ["Example.Show.S01E01.mkv", "Example.Show.S01E02.zh.srt"],
                    "file_coverage": ["S01E01", subtitle_id],
                    "acquisition": {
                        "kind": "torrent",
                        "file_index_by_gap": {
                            "S01E01": [1], subtitle_id: [2],
                        },
                    },
                }]}

        class IsolatedMaterializer(FakeMaterializer):
            def acquire(self, request, selections, *, staging_root, workspace, alist):
                del request, selections, workspace, alist
                self.calls.append(staging_root)
                parent = posixpath.dirname(staging_root)
                self.alist.mkdir(parent)
                self.alist.mkdir(staging_root)
                media_root = f"{staging_root}/media"
                subtitle_root = f"{staging_root}/subtitles"
                self.alist.mkdir(media_root)
                self.alist.mkdir(subtitle_root)
                video = f"{media_root}/S01E01 - Example.Show.S01E01.mkv"
                subtitle = f"{subtitle_root}/{subtitle_id} - Example.Show.S01E02.zh.srt"
                self.alist.tree[staging_root] = [
                    {"name": "media", "is_dir": True},
                    {"name": "subtitles", "is_dir": True},
                ]
                self.alist.tree[media_root] = [
                    {"name": posixpath.basename(video), "is_dir": False, "size": 123},
                ]
                self.alist.tree[subtitle_root] = [
                    {"name": posixpath.basename(subtitle), "is_dir": False, "size": 321},
                ]
                return _ready_delivery(
                    staging_root,
                    [
                        {
                            "path": video, "size": 123,
                            "kind": "video", "gap_ids": ["S01E01"],
                        },
                        {
                            "path": subtitle, "size": 321,
                            "kind": "subtitle", "gap_ids": [subtitle_id],
                        },
                    ],
                    media_staging_root=media_root,
                    subtitle_staging_root=subtitle_root,
                )

        class RealPlannerRunner(SimpleEngineRunner):
            def __init__(self, state_root: Path, alist: MemoryAList) -> None:
                self.planner_sources: list[str] = []
                self.installed_subtitles: list[dict[str, object]] = []

                def planner(request, passed_alist, _tmdb):
                    self.planner_sources.append(request.source_path)
                    rows = passed_alist.list(request.source_path, refresh=True)
                    names = [str(row.get("name") or "") for row in rows]
                    if names != ["S01E01 - Example.Show.S01E01.mkv"]:
                        raise AssertionError(f"child planner saw non-media staging rows: {names}")
                    return Plan(
                        mode="tv",
                        source_root=request.source_path,
                        target_root="/quark/影视/番剧/Example Show",
                        files=[PlannedFile(
                            source_path=(
                                f"{request.source_path}/S01E01 - Example.Show.S01E01.mkv"
                            ),
                            source_dir=request.source_path,
                            original_name="S01E01 - Example.Show.S01E01.mkv",
                            final_name="Example.Show.S01E01.mkv",
                            target_dir="/quark/影视/番剧/Example Show/Season 01",
                            media_kind="video", source_size=123,
                        )],
                        warnings=[],
                        metadata={"tmdb_id": 7, "title": "Example Show"},
                    )

                super().__init__(
                    state_root, alist=alist, tmdb=object(), planner=planner,
                    validate=False,
                )

            def execute_automatic(self, job_id: str) -> EngineJob:
                job = self.get_job(job_id)
                return replace(
                    job, phase="executed", error=None,
                    execution={"files": [{
                        "target": (
                            "/quark/影视/番剧/Example Show/Season 01/"
                            "Example.Show.S01E01.mkv"
                        ),
                        "size": 123,
                    }]},
                )

            def install_subtitle_sidecar(
                self, source_path: str, target_path: str, *, expected_size: int,
                video_path: str,
            ) -> dict[str, object]:
                self.installed_subtitles.append({
                    "source_path": source_path, "target_path": target_path,
                    "expected_size": expected_size, "video_path": video_path,
                })
                return {"status": "moved", "size": expected_size}

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            engine = RealPlannerRunner(Path(temporary) / "engine", alist)
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary) / "runtime", engine_runner=engine, alist=alist,
                search=MixedSearch(), materializer=IsolatedMaterializer(alist),
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)

        self.assertEqual(outcome["outcomes"][0]["resolved_gap_ids"], ["S01E01", subtitle_id])
        self.assertEqual(len(engine.planner_sources), 1)
        self.assertTrue(engine.planner_sources[0].endswith("/media"))
        self.assertEqual(len(engine.installed_subtitles), 1)
        self.assertTrue(
            str(engine.installed_subtitles[0]["source_path"]).endswith(
                "/subtitles/" + subtitle_id + " - Example.Show.S01E02.zh.srt"
            )
        )

    def test_pause_after_failed_attempt_stops_before_next_candidate_round(self) -> None:
        """A killed downloader must not immediately start another round."""
        root_job = _example_root_job()
        paused = {"value": False}

        class FailingMaterializer(FakeMaterializer):
            def acquire(self, request, selections, *, staging_root, workspace, alist):
                self.calls.append(staging_root)
                paused["value"] = True
                raise RuntimeError("aria2 stopped by operator")

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            search = FakeSearch()
            materializer = FailingMaterializer(alist)
            engine = FakeEngine()
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=search, materializer=materializer,
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=3,
                cancel_requested=lambda _job: paused["value"],
            )
            outcome = runtime.run_for_job(root_job)
            state_path = next((Path(temporary) / "gaps" / root_job.id).glob("*.json"))
            state = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertTrue(outcome["cancelled"])
        self.assertEqual(len(search.requests), 1)
        self.assertEqual(len(materializer.calls), 1)
        self.assertEqual(engine.planned, [])
        self.assertEqual(engine.executed, [])
        self.assertEqual(state["phase"], "retry_wait")
        self.assertIn("暂停", str(state["error"]))

    def test_pause_after_materialization_blocks_child_write(self) -> None:
        """A pause after staging is ready must block child planning/execution."""
        root_job = _example_root_job("engine-child-cancel-root")
        paused = {"value": False}

        class PausingMaterializer(FakeMaterializer):
            def acquire(self, request, selections, *, staging_root, workspace, alist):
                result = super().acquire(
                    request, selections, staging_root=staging_root,
                    workspace=workspace, alist=alist,
                )
                paused["value"] = True
                return result

        with tempfile.TemporaryDirectory() as temporary:
            alist = MemoryAList()
            search = FakeSearch()
            materializer = PausingMaterializer(alist)
            engine = FakeEngine()
            runtime = AutomaticReplenishmentRuntime(
                Path(temporary), engine_runner=engine, alist=alist,
                search=search, materializer=materializer,
                staging_root="/quark/影视/ScrapeFlow/补源", max_candidate_rounds=3,
                cancel_requested=lambda _job: paused["value"],
            )
            outcome = runtime.run_for_job(root_job)
            state_path = next((Path(temporary) / "gaps" / root_job.id).glob("*.json"))
            state = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertTrue(outcome["cancelled"])
        self.assertEqual(len(search.requests), 1)
        self.assertEqual(len(materializer.calls), 1)
        self.assertEqual(engine.planned, [])
        self.assertEqual(engine.executed, [])
        self.assertEqual(state["phase"], "retry_wait")

    def test_candidate_failure_exclusion_survives_runtime_recreation(self) -> None:
        """A failed no-peer release is not retried after an API recreation."""
        root_job = _example_root_job("engine-durable-candidate-root")
        unavailable = {
            "provider": "magnet",
            "locator": "torrent:https://example.invalid/no-peer.torrent",
            "infohash": "a" * 40,
            "release_name": "Example Show S01E01 no peers 1080p",
            "title": "Example Show",
            "year": "2020",
            "files": ["Example.Show.S01E01.mkv"],
            "acquisition": {"kind": "torrent"},
        }
        fallback = {
            "provider": "magnet",
            "locator": "torrent:https://example.invalid/seeded.torrent",
            "infohash": "b" * 40,
            "release_name": "Example Show S01E01 seeded 1080p",
            "title": "Example Show",
            "year": "2020",
            "files": ["Example.Show.S01E01.mkv"],
            "acquisition": {"kind": "torrent"},
        }

        class Search:
            def __init__(self) -> None:
                self.requests: list[dict[str, object]] = []

            def run(self, request):
                self.requests.append(json.loads(json.dumps(request)))
                if len(self.requests) == 1:
                    return {"candidates": [dict(unavailable)]}
                return {"candidates": [dict(unavailable), dict(fallback)]}

        class CandidateDownloadFailure(RuntimeError):
            exclude_candidate = True
            failure_stage = "candidate_download"

            def __init__(self, candidate: dict[str, object]) -> None:
                super().__init__("aria2c 下载失败: no peers")
                self.candidate = dict(candidate)

        class NoPeerMaterializer:
            def __init__(self) -> None:
                self.selections: list[list[dict[str, object]]] = []

            def acquire(self, _request, selections, *, staging_root, workspace, alist):
                del staging_root, workspace, alist
                self.selections.append([dict(row) for row in selections])
                raise CandidateDownloadFailure(self.selections[-1][0])

        class RecordingMaterializer(FakeMaterializer):
            def __init__(self, alist: MemoryAList) -> None:
                super().__init__(alist)
                self.selections: list[list[dict[str, object]]] = []

            def acquire(self, request, selections, *, staging_root, workspace, alist):
                self.selections.append([dict(row) for row in selections])
                return super().acquire(
                    request,
                    selections,
                    staging_root=staging_root,
                    workspace=workspace,
                    alist=alist,
                )

        with tempfile.TemporaryDirectory() as temporary:
            state_root = Path(temporary)
            alist = MemoryAList()
            search = Search()
            engine = FakeEngine()
            first = AutomaticReplenishmentRuntime(
                state_root,
                engine_runner=engine,
                alist=alist,
                search=search,
                materializer=NoPeerMaterializer(),
                staging_root="/quark/影视/ScrapeFlow/补源",
                max_candidate_rounds=1,
            )

            failed = first.run_for_job(root_job)
            state_path = state_root / "gaps" / root_job.id / "S01E01.json"
            failed_state = json.loads(state_path.read_text(encoding="utf-8"))

            successful_materializer = RecordingMaterializer(alist)
            recreated = AutomaticReplenishmentRuntime(
                state_root,
                engine_runner=engine,
                alist=alist,
                search=search,
                materializer=successful_materializer,
                staging_root="/quark/影视/ScrapeFlow/补源",
                max_candidate_rounds=1,
            )
            succeeded = recreated.run_for_job(root_job)

        expected_exclusion = {
            "provider": "magnet",
            "locator": unavailable["locator"],
            "infohash": unavailable["infohash"],
            "release_name": unavailable["release_name"],
        }
        self.assertTrue(any(row.get("error") for row in failed["outcomes"]))
        self.assertEqual(failed_state["excluded_candidates"], [expected_exclusion])
        self.assertEqual(search.requests[1]["excluded_candidates"], [expected_exclusion])
        self.assertEqual(
            [row["locator"] for row in successful_materializer.selections[0]],
            [fallback["locator"]],
        )
        self.assertEqual(succeeded["outcomes"][0]["resolved_gap_ids"], ["S01E01"])

    def test_unclassified_failure_does_not_persist_candidate_exclusion(self) -> None:
        """Only a materializer's explicit candidate verdict becomes durable."""
        root_job = _example_root_job("engine-unclassified-failure")

        class InfrastructureFailureMaterializer:
            def acquire(self, _request, _selections, *, staging_root, workspace, alist):
                del staging_root, workspace, alist
                raise RuntimeError("temporary AList connection failure")

        with tempfile.TemporaryDirectory() as temporary:
            state_root = Path(temporary)
            runtime = AutomaticReplenishmentRuntime(
                state_root,
                engine_runner=FakeEngine(),
                alist=MemoryAList(),
                search=FakeSearch(),
                materializer=InfrastructureFailureMaterializer(),
                staging_root="/quark/影视/ScrapeFlow/补源",
                max_candidate_rounds=1,
            )
            outcome = runtime.run_for_job(root_job)
            state_path = state_root / "gaps" / root_job.id / "S01E01.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertTrue(any(row.get("error") for row in outcome["outcomes"]))
        self.assertNotIn("excluded_candidates", state)

    def test_durable_candidate_exclusions_are_bounded_and_sanitized(self) -> None:
        rows = [
            {
                "provider": "magnet",
                "locator": f"torrent:https://example.invalid/{index}.torrent",
                "infohash": f"{index:040x}",
            }
            for index in range(25)
        ]
        rows.append({"provider": "magnet", "locator": "x" * 4097})

        exclusions = AutomaticReplenishmentRuntime._merge_excluded_candidates(rows)

        self.assertEqual(len(exclusions), 24)
        self.assertEqual(exclusions[0]["infohash"], f"{1:040x}")
        self.assertEqual(exclusions[-1]["infohash"], f"{24:040x}")


if __name__ == "__main__":
    unittest.main()
