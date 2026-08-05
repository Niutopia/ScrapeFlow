from __future__ import annotations

import copy
import hashlib
import unittest

from engine.scrapeflow.formal_library_remediation import (
    FormalRemediationPlanError,
    build_formal_remediation_plan,
    canonical_digest,
    plan_hybrid_specs,
    validate_formal_remediation_plan,
)


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def audit(*, residuals=None):
    paths = [
        "/quark/影视/番剧/intake/snow.mkv", "/quark/影视/番剧/intake/anime.mkv",
        "/quark/影视/番剧/intake/wa2.mkv", "/quark/影视/番剧/intake/a.mkv",
        "/quark/影视/番剧/intake/b.mkv",
    ]
    paths.extend(row["path"] for row in (residuals or []) if isinstance(row, dict))
    return {
        "schema_version": 1,
        "library_root": "/quark/影视/番剧",
        "projects": [], "movies": [], "residual_inventory": residuals or [],
        "uncovered_media": [], "multipart_media": [],
        "inventory_paths": paths,
    }


def work(key, namespace, metadata_id, title, leaf):
    return {
        "member_key": key,
        "identity": {"namespace": namespace, "metadata_id": metadata_id},
        "title": title, "leaf_name": leaf, "poster_path": None,
    }


def operation(item, source, member, relative, payload=b"media", **extra):
    return {
        "item_id": item, "operation": "transfer", "kind": "media",
        "member_key": member, "source_path": source, "relative_path": relative,
        "expected_size": len(payload), "expected_sha256": sha(payload),
        "content_type": "video/x-matroska", "episode_key": None,
        "edition_key": None, "duplicate_group": None, "retained_path": None,
        "retained_size": None, "retained_sha256": None, **extra,
    }


def order(works, operations, *, root_identity=None, container="/quark/影视/番剧/Fate"):
    witnesses = {
        row["source_path"]: {
            "expected_size": row["expected_size"],
            "expected_sha256": row["expected_sha256"],
        }
        for row in operations
    }
    return {
        "schema_version": 1, "plan_id": "formal-20260805",
        "container_root": container, "root_identity": root_identity,
        "works": works, "operations": operations,
        "duplicate_edition_evidence": {}, "source_inventory": witnesses,
        "staged_artifacts": {},
        "rollback_root": "/quark/影视/ScrapeFlow/事务回滚",
    }


class FormalRemediationPlanTests(unittest.TestCase):
    def test_fate_movie_children_are_derived_only_by_canonical_tree(self):
        works = [
            work("illya", "tmdb.tv", 63576, "魔法少女☆伊莉雅", "魔法少女☆伊莉雅"),
            work("snow", "tmdb.movie", 2, "魔法少女☆伊莉雅：雪下的誓言", "魔法少女☆伊莉雅：雪下的誓言 (2017)"),
            work("nameless", "tmdb.movie", 3, "魔法少女☆伊莉雅：无名的少女", "魔法少女☆伊莉雅：无名的少女 (2021)"),
        ]
        ops = [operation("snow-movie", "/quark/影视/番剧/intake/snow.mkv", "snow", "雪下的誓言 (2017).mkv")]
        plan = build_formal_remediation_plan(audit(), order(works, ops))
        roots = {row["member_key"]: row["target_root"] for row in plan["canonical_tree"]["placements"]}
        self.assertEqual(roots["snow"], "/quark/影视/番剧/Fate/魔法少女☆伊莉雅/魔法少女☆伊莉雅：雪下的誓言 (2017)")
        self.assertTrue(validate_formal_remediation_plan(plan))
        self.assertEqual(plan_hybrid_specs(plan)[0].target_path, roots["snow"] + "/雪下的誓言 (2017).mkv")

    def test_rick_primary_and_anime_are_not_merged(self):
        works = [
            work("main", "tmdb.tv", 60625, "瑞克和莫蒂", "瑞克和莫蒂"),
            work("anime", "tmdb.tv", 202102, "瑞克和莫蒂：日漫版", "瑞克和莫蒂：日漫版"),
        ]
        ops = [operation("anime-01", "/quark/影视/番剧/intake/anime.mkv", "anime", "Season 01/瑞克和莫蒂：日漫版 S01E01.mkv")]
        value = order(
            works, ops,
            root_identity={"namespace": "tmdb.tv", "metadata_id": 60625},
            container="/quark/影视/番剧/瑞克和MD 1-9季+日漫版",
        )
        plan = build_formal_remediation_plan(audit(), value)
        roots = {row["member_key"]: row["target_root"] for row in plan["canonical_tree"]["placements"]}
        self.assertEqual(plan["canonical_tree"]["container_root"], "/quark/影视/番剧/瑞克和莫蒂")
        self.assertEqual(roots["anime"], "/quark/影视/番剧/瑞克和莫蒂/瑞克和莫蒂：日漫版")

    def test_white_album_distinct_ids_remain_distinct_leaves(self):
        works = [
            work("wa", "tmdb.tv", 28502, "白色相簿", "白色相簿"),
            work("wa2", "tmdb.tv", 70072, "白色相簿2", "白色相簿2"),
        ]
        ops = [operation("wa2-01", "/quark/影视/番剧/intake/wa2.mkv", "wa2", "Season 01/白色相簿2 S01E01.mkv")]
        value = order(
            works, ops,
            root_identity={"namespace": "tmdb.tv", "metadata_id": 28502},
            container="/quark/影视/番剧/白色相簿",
        )
        plan = build_formal_remediation_plan(audit(), value)
        roots = {row["member_key"]: row["target_root"] for row in plan["canonical_tree"]["placements"]}
        self.assertEqual(roots["wa2"], "/quark/影视/番剧/白色相簿/白色相簿2")

    def test_duplicate_episode_editions_fail_closed_without_exact_evidence(self):
        works = [work("show", "tmdb.tv", 1, "节目", "节目")]
        first = operation("one-a", "/quark/影视/番剧/intake/a.mkv", "show", "Season 01/节目 S01E01 A.mkv", episode_key="tmdb.tv:1:S01E01", edition_key="A")
        second = operation("one-b", "/quark/影视/番剧/intake/b.mkv", "show", "Season 01/节目 S01E01 B.mkv", episode_key="tmdb.tv:1:S01E01", edition_key="B", payload=b"other")
        value = order(works, [first, second], container="/quark/影视/番剧/节目")
        with self.assertRaisesRegex(FormalRemediationPlanError, "explicit exact-edition"):
            build_formal_remediation_plan(audit(), value)
        editions = [
            {"edition_key": "A", "source_sha256": first["expected_sha256"]},
            {"edition_key": "B", "source_sha256": second["expected_sha256"]},
        ]
        core = {"resolution": "retain_distinct_editions", "editions": editions}
        value["duplicate_edition_evidence"] = {
            "tmdb.tv:1:S01E01": {**core, "evidence_sha256": canonical_digest(core)}
        }
        self.assertTrue(validate_formal_remediation_plan(build_formal_remediation_plan(audit(), value)))

    def test_residual_delete_requires_audit_presence_and_hybrid_delete(self):
        path = "/quark/影视/番剧/节目/novel.docx"
        op = operation("residual", path, None, None, payload=b"doc")
        op.update({"operation": "delete", "kind": "residual", "member_key": None, "relative_path": None})
        value = order([work("show", "tmdb.tv", 1, "节目", "节目")], [op], container="/quark/影视/番剧/节目")
        with self.assertRaisesRegex(FormalRemediationPlanError, "not present"):
            build_formal_remediation_plan(audit(), value)
        plan = build_formal_remediation_plan(audit(residuals=[{"path": path}]), value)
        self.assertEqual(plan_hybrid_specs(plan)[0].operation, "delete")

    def test_six_duplicate_subtitle_deletes_require_exact_retained_sibling_evidence(self):
        works = [work("show", "tmdb.tv", 1, "节目", "节目")]
        operations = []
        evidence = {}
        inventory = []
        for episode in range(30, 36):
            source = f"/quark/影视/番剧/节目/Season 01/节目 S01E{episode:02d} (1).mks"
            retained = f"/quark/影视/番剧/节目/Season 01/节目 S01E{episode:02d}.mks"
            payload = f"duplicate-{episode}".encode()
            row = operation(
                f"subtitle-e{episode}", source, None, None, payload=payload,
                episode_key=f"tmdb.tv:1:S01E{episode:02d}", edition_key="duplicate",
            )
            row.update({
                "operation": "delete", "kind": "subtitle", "duplicate_group": f"e{episode}",
                "retained_path": retained, "retained_size": 99,
                "retained_sha256": "a" * 64,
            })
            operations.append(row)
            inventory.append(source)
            inventory.append(retained)
            core = {
                "resolution": "delete_exact_duplicate",
                "deleted": [{"item_id": row["item_id"], "source_path": source,
                              "size": len(payload), "sha256": row["expected_sha256"]}],
                "retained": [{"path": retained, "size": 99, "sha256": "a" * 64}],
            }
            evidence[f"e{episode}"] = {**core, "evidence_sha256": canonical_digest(core)}
        value = order(works, operations, container="/quark/影视/番剧/节目")
        value["duplicate_edition_evidence"] = evidence
        value["source_inventory"] = {
            row["source_path"]: {"expected_size": row["expected_size"], "expected_sha256": row["expected_sha256"]}
            for row in operations
        }
        for row in operations:
            value["source_inventory"][row["retained_path"]] = {
                "expected_size": row["retained_size"], "expected_sha256": row["retained_sha256"],
            }
        snapshot = audit()
        snapshot["inventory_paths"] = inventory
        plan = build_formal_remediation_plan(snapshot, value)
        self.assertEqual(len(plan_hybrid_specs(plan)), 6)

    def test_metadata_requires_exact_staged_artifact_and_plan_is_tamper_evident(self):
        source = "/quark/影视/ScrapeFlow/补源/staged/poster.jpg"
        op = operation("poster", source, "show", "poster.jpg", payload=b"jpeg")
        op.update({"kind": "artwork", "content_type": "image/jpeg"})
        value = order([work("show", "tmdb.tv", 1, "节目", "节目")], [op], container="/quark/影视/番剧/节目")
        with self.assertRaisesRegex(FormalRemediationPlanError, "staged artifact"):
            build_formal_remediation_plan(audit(), value)
        value["staged_artifacts"][source] = value["source_inventory"][source]
        plan = build_formal_remediation_plan(audit(), value)
        changed = copy.deepcopy(plan)
        changed["operations"][0]["target_path"] += ".other"
        self.assertFalse(validate_formal_remediation_plan(changed))

    def test_rehashed_handwritten_operation_cannot_bypass_scope(self):
        works = [work("show", "tmdb.tv", 1, "节目", "节目")]
        op = operation("episode", "/quark/影视/番剧/intake/a.mkv", "show", "Season 01/a.mkv")
        plan = build_formal_remediation_plan(audit(), order(works, [op], container="/quark/影视/番剧/节目"))
        forged = copy.deepcopy(plan)
        forged["operations"][0]["source_path"] = "/quark/影视/番剧/not-in-inventory.mkv"
        forged["source_inventory"][forged["operations"][0]["source_path"]] = forged["source_inventory"].pop(op["source_path"])
        core = {key: value for key, value in forged.items() if key != "plan_sha256"}
        forged["plan_sha256"] = canonical_digest(core)
        self.assertFalse(validate_formal_remediation_plan(forged))

    def test_overlapping_sources_and_targets_fail_before_specs(self):
        works = [work("show", "tmdb.tv", 1, "节目", "节目")]
        source = "/quark/影视/番剧/intake/a.mkv"
        first = operation("a", source, "show", "Season 01/a.mkv")
        second = operation("b", "/quark/影视/番剧/intake/b.mkv", "show", "Season 01/b.mkv")
        second["source_path"] = source
        value = order(works, [first, second], container="/quark/影视/番剧/节目")
        with self.assertRaisesRegex(FormalRemediationPlanError, "overlap"):
            build_formal_remediation_plan(audit(), value)


if __name__ == "__main__":
    unittest.main()
