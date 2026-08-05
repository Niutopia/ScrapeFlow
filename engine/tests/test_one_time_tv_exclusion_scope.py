from __future__ import annotations

from copy import deepcopy
import unittest

from engine.scrapeflow.one_time_movie_member_scope import (
    canonical_digest,
    seal_movie_member_candidate,
    validate_exact_movie_member_scope,
)
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    ExactTVExclusionAListView,
    path_is_excluded_by_tv_scope,
    scan_exact_tv_exclusion_subtitle_inventory,
    seal_exact_tv_exclusion_scope_from_predecessor_batch,
    validate_exact_tv_exclusion_scope,
)
from engine.tools.audit_one_time_title_batch import exact_title_inventory


class FakeAList:
    def __init__(self, rows: list[dict]):
        self.rows = deepcopy(rows)
        self.calls: list[tuple[str, dict]] = []

    def walk(self, path: str, **kwargs) -> list[dict]:
        self.calls.append((path, dict(kwargs)))
        # Deliberately ignore excluded_roots so the sealed filtering view must
        # still prevent a buggy/hostile backend from leaking nested members.
        return deepcopy(self.rows)


class OneTimeTVExclusionScopeTests(unittest.TestCase):
    root = "/quark/影视/番剧/Parent"
    movie = root + "/Movie (2025)"
    ambiguous = root + "/Ambiguous (2024)"
    nested_tv = root + "/Spinoff"

    def predecessor(self, *, blockers: list[dict] | None = None) -> dict:
        identity_core = {
            "status": "exact", "tmdb_id": 42,
            "media_type": "tv", "title": "Parent",
        }
        return {
            "target_root": self.root, "category": "番剧",
            "identity": {
                **identity_core,
                "identity_sha256": canonical_digest(identity_core),
            },
            "title_work_key": "a" * 64,
            "read_only_audit_allowed": False,
            "scope_blockers": blockers if blockers is not None else [{
                "reason": "nested_title_identity",
                "target_roots": [self.ambiguous, self.movie, self.nested_tv],
            }],
        }

    def movie_scope(self) -> dict:
        candidate = seal_movie_member_candidate({
            "category": "番剧", "target_stem": self.movie,
            "parent_root": self.root, "video_path": self.movie + ".mkv",
            "nfo_path": self.movie + ".nfo", "tmdb_id": 99,
            "title": "Movie",
        })
        paths = [self.movie + "-poster.jpg", self.movie + ".mkv", self.movie + ".nfo"]
        return validate_exact_movie_member_scope({
            "schema_version": 1, "kind": "one_time_exact_movie_member_scope",
            **{key: candidate[key] for key in (
                "category", "target_stem", "parent_root", "video_path",
                "nfo_path", "tmdb_id", "title", "candidate_sha256",
            )},
            "member_paths": paths,
            "member_paths_sha256": canonical_digest(paths),
        })

    def scope(self) -> dict:
        return seal_exact_tv_exclusion_scope_from_predecessor_batch(
            self.predecessor(), movie_member_scopes=[self.movie_scope()],
        )

    def rows(self) -> list[dict]:
        return [
            {"full_path": self.root + "/Season 01/Parent - S01E01.mkv", "size": 100},
            {"full_path": self.root + "/Season 01/Parent - S01E01.zh-CN.ass", "size": 10},
            {"full_path": self.movie + ".mkv", "size": 200},
            {"full_path": self.movie + ".nfo", "size": 20},
            {"full_path": self.movie + ".zh-CN.ass", "size": 10},
            {"full_path": self.movie + "-poster.jpg", "size": 3},
            {"full_path": self.ambiguous + ".mkv", "size": 300},
            {"full_path": self.ambiguous + ".nfo", "size": 20},
            {"full_path": self.ambiguous + ".ass", "size": 10},
            {"full_path": self.nested_tv + "/Season 01/Spinoff - S01E01.mkv", "size": 400},
        ]

    def test_scope_is_derived_only_from_predecessor_nested_blocker(self):
        scope = self.scope()
        self.assertEqual(
            scope["excluded_roots"],
            [self.ambiguous, self.movie, self.nested_tv],
        )
        self.assertEqual(scope["excluded_member_paths"], self.movie_scope()["member_paths"])
        self.assertEqual(validate_exact_tv_exclusion_scope(scope), scope)

    def test_predecessor_order_is_canonicalized_before_scope_seal(self):
        roots = [self.nested_tv, self.movie, self.ambiguous]
        scope = seal_exact_tv_exclusion_scope_from_predecessor_batch(
            self.predecessor(blockers=[{
                "reason": "nested_title_identity", "target_roots": roots,
            }]),
        )
        self.assertEqual(scope["excluded_roots"], sorted(roots, key=str.casefold))

    def test_other_identity_or_tmdb_blocker_never_gets_relaxed(self):
        for extra in (
            {"reason": "duplicate_tmdb_identity_roots", "target_roots": [self.root]},
            {"reason": "missing_exact_identity"},
        ):
            with self.subTest(extra=extra):
                with self.assertRaisesRegex(ValueError, "其他 blocker|身份歧义"):
                    seal_exact_tv_exclusion_scope_from_predecessor_batch(
                        self.predecessor(blockers=[
                            self.predecessor()["scope_blockers"][0], extra,
                        ]),
                    )

    def test_exclusions_reject_outside_duplicate_and_contained_roots(self):
        invalid = (
            ["/quark/影视/番剧/Outside"],
            [self.movie, self.movie],
            [self.nested_tv, self.nested_tv + "/Child"],
        )
        for roots in invalid:
            with self.subTest(roots=roots):
                with self.assertRaises(ValueError):
                    seal_exact_tv_exclusion_scope_from_predecessor_batch(
                        self.predecessor(blockers=[{
                            "reason": "nested_title_identity",
                            "target_roots": roots,
                        }]),
                    )

    def test_resealed_member_path_for_similar_sibling_stem_is_rejected(self):
        scope = self.scope()
        scope["excluded_member_paths"] = [self.movie + "2.mkv"]
        scope["excluded_member_paths_sha256"] = canonical_digest(
            scope["excluded_member_paths"],
        )
        core = {key: value for key, value in scope.items() if key != "scope_sha256"}
        scope["scope_sha256"] = canonical_digest(core)
        # A similar sibling basename cannot be asserted as an exact phase-2
        # member of the sealed target stem.
        with self.assertRaisesRegex(ValueError, "没有归属于"):
            validate_exact_tv_exclusion_scope(scope)

    def test_view_filters_nested_tv_exact_movie_and_unresolved_movie_stem(self):
        scope = self.scope()
        client = FakeAList(self.rows())
        visible = ExactTVExclusionAListView(client, scope).walk(
            self.root, refresh=True, excluded_roots=scope["excluded_roots"],
        )
        paths = [row["full_path"] for row in visible]
        self.assertEqual(paths, [
            self.root + "/Season 01/Parent - S01E01.mkv",
            self.root + "/Season 01/Parent - S01E01.zh-CN.ass",
        ])
        self.assertTrue(path_is_excluded_by_tv_scope(self.movie + ".mkv", scope))
        self.assertTrue(path_is_excluded_by_tv_scope(self.ambiguous + ".nfo", scope))
        self.assertTrue(path_is_excluded_by_tv_scope(self.nested_tv + "/x.mkv", scope))
        self.assertFalse(path_is_excluded_by_tv_scope(paths[0], scope))

    def test_subtitle_inventory_never_contains_nested_direct_files(self):
        inventory = scan_exact_tv_exclusion_subtitle_inventory(
            FakeAList(self.rows()), self.scope(),
        )
        self.assertEqual(inventory["excluded_roots"], self.scope()["excluded_roots"])
        serialized = str(inventory["subtitle_inventory"])
        self.assertNotIn("Movie (2025)", serialized)
        self.assertNotIn("Ambiguous (2024)", serialized)
        self.assertNotIn("Spinoff", serialized)
        self.assertEqual(inventory["summary"]["videos"], 1)

    def test_fingerprint_ignores_nested_changes_but_tracks_parent_changes(self):
        scope = self.scope()
        first = exact_title_inventory(
            FakeAList(self.rows()), self.root, tv_exclusion_scope=scope,
        )
        nested_changed = self.rows()
        next(row for row in nested_changed if row["full_path"] == self.movie + ".mkv")["size"] = 999
        second = exact_title_inventory(
            FakeAList(nested_changed), self.root, tv_exclusion_scope=scope,
        )
        self.assertEqual(first["inventory_sha256"], second["inventory_sha256"])
        parent_changed = self.rows()
        parent_changed[0]["size"] = 101
        third = exact_title_inventory(
            FakeAList(parent_changed), self.root, tv_exclusion_scope=scope,
        )
        self.assertNotEqual(first["inventory_sha256"], third["inventory_sha256"])


if __name__ == "__main__":
    unittest.main()
