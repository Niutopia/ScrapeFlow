import ast
import inspect
import unittest

from engine.scrapeflow.canonical_work_tree import (
    CanonicalTreeError,
    CanonicalWork,
    WorkIdentity,
    plan_canonical_work_tree,
)


def work(key, kind, metadata_id, title, leaf=None):
    return CanonicalWork(
        member_key=key,
        identity=WorkIdentity(f"tmdb.{kind}", metadata_id),
        title=title,
        leaf_name=leaf or title,
        poster_path=f"/{key}.jpg",
    )


class CanonicalWorkTreeGoldenTests(unittest.TestCase):
    def roots(self, tree):
        return {row.member_key: row.target_root for row in tree.placements}

    def test_fate_keeps_container_and_independent_identity_leaves(self):
        tree = plan_canonical_work_tree(
            [
                work("illya", "tv", 63576, "魔法少女☆伊莉雅"),
                work(
                    "snow", "movie", 2, "魔法少女☆伊莉雅：雪下的誓言",
                    "魔法少女☆伊莉雅：雪下的誓言 (2017)",
                ),
                work(
                    "nameless", "movie", 3, "魔法少女☆伊莉雅：无名的少女",
                    "魔法少女☆伊莉雅：无名的少女 (2021)",
                ),
                work("stay-night", "tv", 37858, "命运之夜"),
            ],
            container_root="/library/Fate",
        )
        self.assertEqual(tree.container_root, "/library/Fate")
        self.assertEqual(self.roots(tree), {
            "illya": "/library/Fate/魔法少女☆伊莉雅",
            "snow": "/library/Fate/魔法少女☆伊莉雅/魔法少女☆伊莉雅：雪下的誓言 (2017)",
            "nameless": "/library/Fate/魔法少女☆伊莉雅/魔法少女☆伊莉雅：无名的少女 (2021)",
            "stay-night": "/library/Fate/命运之夜",
        })

    def test_white_album_different_ids_never_become_one_season_tree(self):
        first = WorkIdentity("tmdb.tv", 28502)
        tree = plan_canonical_work_tree(
            [
                work("white-album", "tv", 28502, "白色相簿"),
                work("white-album-2", "tv", 70072, "白色相簿2"),
            ],
            container_root="/library/白色相簿",
            root_identity=first,
        )
        self.assertEqual(self.roots(tree), {
            "white-album": "/library/白色相簿",
            "white-album-2": "/library/白色相簿/白色相簿2",
        })

    def test_rick_source_noise_is_replaced_by_confirmed_primary_title(self):
        main = WorkIdentity("tmdb.tv", 60625)
        tree = plan_canonical_work_tree(
            [
                work("main", "tv", 60625, "瑞克和莫蒂"),
                work("anime", "tv", 202102, "瑞克和莫蒂：日漫版"),
            ],
            container_root="/library/瑞克和MD 1-9季+日漫版",
            root_identity=main,
        )
        self.assertEqual(tree.container_root, "/library/瑞克和莫蒂")
        self.assertEqual(self.roots(tree), {
            "main": "/library/瑞克和莫蒂",
            "anime": "/library/瑞克和莫蒂/瑞克和莫蒂：日漫版",
        })

    def test_same_identity_versions_share_one_leaf(self):
        identity = WorkIdentity("tmdb.tv", 60625)
        tree = plan_canonical_work_tree(
            [
                work("season-7-4k", "tv", 60625, "瑞克和莫蒂"),
                work("season-7-1080p", "tv", 60625, "瑞克和莫蒂"),
            ],
            container_root="/library/noisy",
            root_identity=identity,
        )
        self.assertEqual(
            {row.target_root for row in tree.placements},
            {"/library/瑞克和莫蒂"},
        )

    def test_different_ids_with_same_leaf_fail_closed(self):
        with self.assertRaisesRegex(CanonicalTreeError, "same canonical leaf"):
            plan_canonical_work_tree(
                [
                    work("one", "tv", 1, "Same Title"),
                    work("two", "tv", 2, "Same Title"),
                ],
                container_root="/library/Franchise",
            )

    def test_scraper_has_one_canonical_rebase_authority(self):
        from engine import scraper

        module = ast.parse(inspect.getsource(scraper))
        callers = []
        for node in module.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if any(
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Name)
                and child.func.id == "_rebase_plan_identity_roots"
                for child in ast.walk(node)
            ):
                callers.append(node.name)
        self.assertEqual(callers, ["_plan_canonical_batch_tree"])

    def test_scraper_has_one_work_tree_planner_adapter(self):
        from engine import scraper

        module = ast.parse(inspect.getsource(scraper))

        def callers_of(name):
            return [
                node.name
                for node in module.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and any(
                    isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Name)
                    and child.func.id == name
                    for child in ast.walk(node)
                )
            ]

        self.assertEqual(
            callers_of("plan_canonical_work_tree"),
            ["_plan_canonical_batch_tree"],
        )
        self.assertEqual(callers_of("_combine_plans_as_batch"), ["build_tv_plan_smart"])
        functions = {
            node.name: node
            for node in module.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for name in (
            "_dedupe_confirmed_batch_movies",
            "build_collection_plan",
            "build_tagged_collection_plan",
            "build_batch_plan",
            "build_tv_plan_smart",
        ):
            with self.subTest(function=name):
                self.assertTrue(any(
                    isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Name)
                    and child.func.id == "_plan_canonical_batch_tree"
                    for child in ast.walk(functions[name])
                ))

    def test_retired_layout_bypasses_and_rick_folder_hardcode_are_absent(self):
        from engine import scraper

        source = inspect.getsource(scraper)
        for retired in (
            "_rebase_plan_target",
            "_rebase_confirmed_movie_root",
            "_subseries_family_title",
        ):
            self.assertNotIn(f"def {retired}", source)
        cleaner = inspect.getsource(scraper._clean_franchise_root_label)
        self.assertNotIn("瑞克和莫蒂", cleaner)
        self.assertNotIn("日漫版", cleaner)


if __name__ == "__main__":
    unittest.main()
