"""C/U regressions for meaningful CJK roots with naked numeric episodes."""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import resolve_work_unit_identities

from local.tests.test_root_boundaries import DictAList


class _CjkTVTMDB:
    """Only the meaningful boundary title may retrieve the TV candidate."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, path: str, **params: object) -> dict[str, object]:
        self.calls.append((path, dict(params)))
        query = str(params.get("query") or "")
        query_key = re.sub(r"[^\w\u3400-\u9fff]+", "", query.casefold())
        if path == "/search/tv" and "有意义中文剧名" in query_key:
            return {
                "results": [{
                    "id": 2001,
                    "name": "有意义中文剧名",
                    "first_air_date": "2024-07-01",
                    "genre_ids": [16],
                }],
            }
        if path == "/search/movie" and query.strip() == "01":
            return {
                "results": [{
                    "id": 2002,
                    "title": "01",
                    "release_date": "2024-01-01",
                    "genre_ids": [18],
                }],
            }
        if path == "/tv/2001":
            return {"number_of_episodes": 12}
        if path.endswith("/alternative_titles"):
            return {"results": [], "titles": []}
        return {"results": []}


class _GenericLabelTVTMDB:
    """A deliberately deceptive same-label TV candidate for fail-closed C/U."""

    def __init__(self, title: str, *, candidate_title: str | None = None) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.title = title
        self.candidate_title = candidate_title or title

    def get(self, path: str, **params: object) -> dict[str, object]:
        self.calls.append((path, dict(params)))
        if path == "/search/tv":
            return {
                "results": [{
                    "id": 2003,
                    "name": self.candidate_title,
                    "first_air_date": "2024-07-01",
                    "genre_ids": [16],
                }],
            }
        if path.endswith("/alternative_titles"):
            return {"results": [], "titles": []}
        return {"results": []}


class NakedNumericIdentityIntegrationTests(unittest.TestCase):
    def test_cjk_title_and_exact_twelve_file_run_resolve_without_numeric_query(self) -> None:
        """Exercise B→C with a source shape equivalent to the parked unit."""
        root = "/incoming/B 有意义中文剧名（2024）全12集 1080P"
        alist = DictAList({
            root: [
                *[
                    {
                        "name": f"{episode:02d}.mp4",
                        "is_dir": False,
                        "size": 300 * 1024 * 1024,
                    }
                    for episode in range(1, 13)
                ],
                {"name": "资源文档.docx", "is_dir": False, "size": 1024},
            ],
        })
        tmdb = _CjkTVTMDB()
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist,
                root,
                root_task_id="root-cjk-naked-numeric",
                state_root=state_root,
            )
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].media_context, "tv")

            resolved = resolve_work_unit_identities(
                tmdb,
                state_root,
                "root-cjk-naked-numeric",
                prefer_animation=True,
            )

        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0].identity_status, "confirmed")
        self.assertEqual(
            (resolved[0].identity or {}).get("tmdb_id"),
            2001,
        )
        search_queries = [
            str(params.get("query") or "")
            for path, params in tmdb.calls
            if path.startswith("/search/")
        ]
        self.assertIn("有意义中文剧名", search_queries)
        self.assertNotIn("01", search_queries)
        self.assertFalse(any(query.isdigit() for query in search_queries))

    def test_generic_label_with_exact_fake_tv_stays_uncertain(self) -> None:
        """A pure ordinal run cannot authenticate a generic release label."""
        root = "/incoming/全12集 1080P"
        alist = DictAList({
            root: [
                *[
                    {
                        "name": f"{episode:02d}.mp4",
                        "is_dir": False,
                        "size": 300 * 1024 * 1024,
                    }
                    for episode in range(1, 13)
                ],
            ],
        })
        tmdb = _GenericLabelTVTMDB("全12集 1080P")
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist,
                root,
                root_task_id="root-generic-naked-numeric",
                state_root=state_root,
            )
            self.assertEqual(len(records), 1)

            resolved = resolve_work_unit_identities(
                tmdb,
                state_root,
                "root-generic-naked-numeric",
                prefer_animation=True,
            )

        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0].identity_status, "uncertain")
        self.assertIsNone(resolved[0].identity)

    def test_release_only_cjk_labels_stay_uncertain_after_noise_cleanup(self) -> None:
        """Removing year/count metadata must not turn a generic noun into a title."""
        cases = (
            ("发布包（2024）全12集 1080P", "发布包"),
            ("资源文档（2024）全12集 1080P", "资源文档"),
            ("电视剧（2024）全12集 1080P", "电视剧"),
            ("电视剧全集（2024）全12集 1080P", "电视剧全集"),
            ("发布包合集（2024）全12集 1080P", "发布包合集"),
            ("资源合集（2024）全12集 1080P", "资源合集"),
        )
        for index, (label, fake_title) in enumerate(cases, start=1):
            with self.subTest(label=label):
                root = f"/incoming/{label}"
                alist = DictAList({
                    root: [
                        *[
                            {
                                "name": f"{episode:02d}.mp4",
                                "is_dir": False,
                                "size": 300 * 1024 * 1024,
                            }
                            for episode in range(1, 13)
                        ],
                    ],
                })
                tmdb = _GenericLabelTVTMDB(label, candidate_title=fake_title)
                with tempfile.TemporaryDirectory() as directory:
                    state_root = Path(directory)
                    analyze_root_boundaries(
                        alist,
                        root,
                        root_task_id=f"root-generic-release-{index}",
                        state_root=state_root,
                    )
                    resolved = resolve_work_unit_identities(
                        tmdb,
                        state_root,
                        f"root-generic-release-{index}",
                        prefer_animation=True,
                    )

                self.assertEqual(len(resolved), 1)
                self.assertEqual(resolved[0].identity_status, "uncertain")
                self.assertIsNone(resolved[0].identity)


if __name__ == "__main__":
    unittest.main()
