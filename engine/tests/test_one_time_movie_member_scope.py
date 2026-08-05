from __future__ import annotations

from copy import deepcopy
import unittest

from engine.scrapeflow.one_time_movie_member_scope import (
    canonical_digest,
    movie_scope_matches_candidate,
    resolve_exact_movie_member_scope,
    scan_exact_movie_member_subtitle_inventory,
    seal_movie_member_candidate,
    validate_exact_movie_member_scope,
)


class FakeAList:
    def __init__(self, rows: list[dict], nfo: bytes):
        self.rows = deepcopy(rows)
        self.nfo = nfo
        self.list_calls: list[tuple[str, bool]] = []
        self.read_calls: list[str] = []

    def list(self, path: str, *, refresh: bool = False) -> list[dict]:
        self.list_calls.append((path, refresh))
        return deepcopy(self.rows)

    def read_file_prefix(self, path: str, *, max_bytes: int) -> bytes:
        self.read_calls.append(path)
        return self.nfo[:max_bytes]


class FakeTMDB:
    def __init__(self, payload: dict | None = None):
        self.payload = payload or {
            "id": 99, "title": "Movie A", "release_date": "2026-01-02",
        }
        self.calls: list[str] = []

    def get(self, path: str) -> dict:
        self.calls.append(path)
        return deepcopy(self.payload)


class OneTimeMovieMemberScopeTests(unittest.TestCase):
    parent = "/quark/影视/电影/Shared"
    stem = parent + "/Movie A (2026)"

    def candidate(self) -> dict:
        return seal_movie_member_candidate({
            "category": "电影",
            "target_stem": self.stem,
            "parent_root": self.parent,
            "video_path": self.stem + ".mkv",
            "nfo_path": self.stem + ".nfo",
            "tmdb_id": 99,
            "title": "Movie A",
        })

    def nfo(self, *, tmdb_id: int = 99, year: str = "2026") -> bytes:
        return (
            f"<movie><title>Movie A</title><year>{year}</year>"
            f"<uniqueid type='tmdb'>{tmdb_id}</uniqueid></movie>"
        ).encode()

    def rows(self) -> list[dict]:
        return [
            {"name": "Movie B (2025).mkv", "is_dir": False, "size": 200},
            {"name": "Movie B (2025).zh-CN.ass", "is_dir": False, "size": 20},
            {"name": "Movie A (2026).nfo", "is_dir": False, "size": 100},
            {"name": "Movie A (2026).mkv", "is_dir": False, "size": 1000},
            {"name": "Movie A (2026).zh-CN.ass", "is_dir": False, "size": 10},
            {"name": "Movie A (2026)-poster.jpg", "is_dir": False, "size": 30},
        ]

    def resolve(self, rows: list[dict] | None = None, *, nfo: bytes | None = None, tmdb: dict | None = None):
        client = FakeAList(rows or self.rows(), nfo or self.nfo())
        result = resolve_exact_movie_member_scope(
            client, self.candidate(), tmdb=FakeTMDB(tmdb),
        )
        return client, result

    def test_shared_parent_resolves_only_exact_movie_member(self):
        client, result = self.resolve()
        scope = result["scope"]
        self.assertEqual(client.list_calls, [(self.parent, True)])
        self.assertEqual(client.read_calls, [self.stem + ".nfo"])
        self.assertEqual(scope["target_stem"], self.stem)
        self.assertEqual(scope["member_paths"], [
            self.stem + "-poster.jpg",
            self.stem + ".mkv",
            self.stem + ".nfo",
            self.stem + ".zh-CN.ass",
        ])
        self.assertNotIn("Movie B", "\n".join(scope["member_paths"]))
        self.assertEqual(len(result["parent_inventory"]["entries"]), 6)
        self.assertTrue(movie_scope_matches_candidate(scope, self.candidate()))

    def test_sibling_subtitle_never_satisfies_movie(self):
        rows = [
            row for row in self.rows()
            if row["name"] != "Movie A (2026).zh-CN.ass"
        ]
        _client, result = self.resolve(rows)
        inventory = scan_exact_movie_member_subtitle_inventory(None, result["scope"])
        self.assertEqual(inventory["summary"]["missing_subtitles"], 1)
        row = inventory["subtitle_inventory"][0]
        self.assertEqual(row["status"], "gap")
        self.assertEqual(row["candidate_subtitles"], [])
        self.assertNotIn("Movie B", str(inventory))

    def test_exact_chinese_sidecar_satisfies_only_its_member(self):
        _client, result = self.resolve()
        inventory = scan_exact_movie_member_subtitle_inventory(None, result["scope"])
        self.assertEqual(inventory["summary"]["missing_subtitles"], 0)
        self.assertEqual(
            inventory["subtitle_inventory"][0]["companion_subtitles"],
            [self.stem + ".zh-CN.ass"],
        )

    def test_multiple_same_stem_videos_fail_closed(self):
        rows = self.rows() + [{
            "name": "Movie A (2026).mp4", "is_dir": False, "size": 900,
        }]
        with self.assertRaisesRegex(ValueError, "必须且只能有一个"):
            self.resolve(rows)

    def test_missing_or_wrong_nfo_identity_and_year_fail_closed(self):
        without_nfo = [row for row in self.rows() if not row["name"].endswith(".nfo")]
        with self.assertRaisesRegex(ValueError, "缺少唯一同 stem NFO"):
            self.resolve(without_nfo)
        with self.assertRaisesRegex(ValueError, "唯一 TMDB"):
            self.resolve(nfo=self.nfo(tmdb_id=100))
        with self.assertRaisesRegex(ValueError, "年份"):
            self.resolve(nfo=self.nfo(year="2024"))
        with self.assertRaisesRegex(ValueError, "其他身份"):
            self.resolve(tmdb={"id": 100, "release_date": "2026-01-01"})

    def test_related_unknown_file_or_directory_fails_closed(self):
        for row in (
            {"name": "Movie A (2026).json", "is_dir": False},
            {"name": "Movie A (2026) extras", "is_dir": True},
        ):
            with self.subTest(row=row):
                with self.assertRaisesRegex(ValueError, "无法唯一归属"):
                    self.resolve(self.rows() + [row])

    def test_unrelated_sibling_is_allowed_but_changes_parent_fingerprint(self):
        _client, first = self.resolve()
        _client, second = self.resolve(self.rows() + [{
            "name": "Movie C (2024).mkv", "is_dir": False, "size": 55,
        }])
        self.assertEqual(first["scope"], second["scope"])
        self.assertEqual(
            first["member_inventory"]["inventory_sha256"],
            second["member_inventory"]["inventory_sha256"],
        )
        self.assertNotEqual(
            first["parent_inventory"]["inventory_sha256"],
            second["parent_inventory"]["inventory_sha256"],
        )

    def test_member_metadata_or_nfo_content_changes_fingerprint(self):
        _client, first = self.resolve()
        rows = self.rows()
        next(row for row in rows if row["name"].endswith(".mkv") and row["name"].startswith("Movie A"))["size"] = 1001
        _client, second = self.resolve(rows)
        self.assertNotEqual(
            first["member_inventory"]["inventory_sha256"],
            second["member_inventory"]["inventory_sha256"],
        )
        changed_nfo = self.nfo().replace(b"Movie A", b"Movie Alpha")
        _client, third = self.resolve(nfo=changed_nfo)
        self.assertNotEqual(
            first["member_inventory"]["inventory_sha256"],
            third["member_inventory"]["inventory_sha256"],
        )

    def test_scope_tampering_alias_and_unknown_member_are_rejected(self):
        _client, result = self.resolve()
        original = result["scope"]
        tampered = deepcopy(original)
        tampered["member_paths"].append(self.stem + ".json")
        tampered["member_paths"].sort(key=str.casefold)
        tampered["member_paths_sha256"] = canonical_digest(tampered["member_paths"])
        with self.assertRaisesRegex(ValueError, "无法归属"):
            validate_exact_movie_member_scope(tampered)

        alias = deepcopy(original)
        alias["member_paths"].append(self.stem.casefold() + ".mkv")
        alias["member_paths"].sort(key=str.casefold)
        alias["member_paths_sha256"] = canonical_digest(alias["member_paths"])
        with self.assertRaisesRegex(ValueError, "唯一"):
            validate_exact_movie_member_scope(alias)


if __name__ == "__main__":
    unittest.main()
