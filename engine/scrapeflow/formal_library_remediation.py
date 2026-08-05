"""Immutable, plan-only remediation for the formal media library.

This module deliberately cannot talk to AList.  It binds an operator supplied
work order to one read-only audit snapshot, delegates every work placement to
``canonical_work_tree``, and emits exact hybrid-transaction specifications.
Execution and acceptance live in Local; Engine is never a commit authority.
"""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
import unicodedata
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import Any, Mapping, Sequence

from .canonical_work_tree import (
    CanonicalWork,
    WorkIdentity,
    plan_canonical_work_tree,
)
from .hybrid_remote_transaction import DEFAULT_ROLLBACK_ROOT, HybridTransferSpec


class FormalRemediationPlanError(ValueError):
    """The audit/work order cannot produce an unambiguous safe plan."""


_SHA256 = re.compile(r"\A[0-9a-f]{64}\Z")
_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


def canonical_json(value: Any) -> bytes:
    return (json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ) + "\n").encode("utf-8")


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _exact_path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.startswith("/") or value == "/":
        raise FormalRemediationPlanError(f"{label} must be a non-root absolute path")
    normalized = posixpath.normpath(value)
    if (
        normalized != value or "\\" in value or "%" in value
        or unicodedata.normalize("NFC", value) != value
        or any(unicodedata.category(character).startswith("C") for character in value)
    ):
        raise FormalRemediationPlanError(f"{label} is not an exact canonical path")
    return value


def _relative_path(value: object) -> str:
    if not isinstance(value, str) or not value or value.startswith("/"):
        raise FormalRemediationPlanError("relative_path must be a non-empty relative path")
    normalized = posixpath.normpath(value)
    if normalized != value or normalized in {".", ".."} or normalized.startswith("../"):
        raise FormalRemediationPlanError("relative_path escapes its canonical work root")
    return value


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise FormalRemediationPlanError(f"{label} must be an exact lowercase SHA-256")
    return value


def _size(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FormalRemediationPlanError("expected_size must be a non-negative integer")
    return value


def _identity(raw: Mapping[str, Any]) -> WorkIdentity:
    if set(raw) != {"namespace", "metadata_id"}:
        raise FormalRemediationPlanError("identity fields do not match schema")
    namespace, metadata_id = raw["namespace"], raw["metadata_id"]
    if not isinstance(namespace, str) or isinstance(metadata_id, bool) or not isinstance(metadata_id, int):
        raise FormalRemediationPlanError("identity has invalid field types")
    return WorkIdentity(namespace, metadata_id)


def _inside(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _audit_paths(audit: Mapping[str, Any]) -> set[str]:
    paths: set[str] = set()
    paths.update(value for value in audit.get("inventory_paths", []) if isinstance(value, str))
    for project in audit.get("projects", []):
        if isinstance(project, Mapping):
            for key in ("target_root",):
                if isinstance(project.get(key), str):
                    paths.add(str(project[key]))
            for key in ("video_files", "subtitle_files"):
                if isinstance(project.get(key), list):
                    paths.update(value for value in project[key] if isinstance(value, str))
    for movie in audit.get("movies", []):
        if isinstance(movie, Mapping):
            paths.update(value for value in movie.get("video_files", []) if isinstance(value, str))
            if isinstance(movie.get("target_stem"), str):
                paths.add(str(movie["target_stem"]))
    for row in audit.get("residual_inventory", []):
        if isinstance(row, Mapping) and isinstance(row.get("path"), str):
            paths.add(str(row["path"]))
    for key in ("uncovered_media", "multipart_media"):
        paths.update(value for value in audit.get(key, []) if isinstance(value, str))
    return paths


def _assert_pairwise_disjoint(operations: list[dict[str, Any]]) -> None:
    protected: list[tuple[str, str]] = []
    for operation in operations:
        protected.append((f"{operation['item_id']}.source", operation["source_path"]))
        if operation["target_path"] is not None:
            protected.append((f"{operation['item_id']}.target", operation["target_path"]))
        if operation.get("retained_path") is not None:
            protected.append((f"{operation['item_id']}.retained", operation["retained_path"]))
    for index, (left_label, left) in enumerate(protected):
        for right_label, right in protected[index + 1:]:
            left_key = unicodedata.normalize("NFKC", left).casefold()
            right_key = unicodedata.normalize("NFKC", right).casefold()
            if (left_key == right_key or left_key.startswith(right_key + "/")
                    or right_key.startswith(left_key + "/")):
                raise FormalRemediationPlanError(
                    f"remediation paths overlap: {left_label} and {right_label}"
                )


def _assert_duplicate_editions_are_explicit(
    operations: list[dict[str, Any]], evidence: Mapping[str, Any]
) -> None:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for operation in operations:
        episode_key = operation.get("episode_key")
        if operation["operation"] == "transfer" and episode_key:
            grouped.setdefault(str(episode_key), []).append(operation)
    for episode_key, rows in grouped.items():
        if len(rows) < 2:
            continue
        row = evidence.get(episode_key)
        if not isinstance(row, Mapping) or set(row) != {
            "resolution", "editions", "evidence_sha256"
        } or row.get("resolution") != "retain_distinct_editions":
            raise FormalRemediationPlanError(
                f"duplicate episode {episode_key!r} requires explicit exact-edition evidence"
            )
        editions = row.get("editions")
        if not isinstance(editions, list):
            raise FormalRemediationPlanError("duplicate edition evidence is malformed")
        expected = sorted(
            (
                {"edition_key": item.get("edition_key"),
                 "source_sha256": item["expected_sha256"]}
                for item in rows
            ),
            key=lambda item: (str(item["edition_key"]), str(item["source_sha256"])),
        )
        actual = sorted(editions, key=lambda item: (
            str(item.get("edition_key")) if isinstance(item, Mapping) else "",
            str(item.get("source_sha256")) if isinstance(item, Mapping) else "",
        ))
        if actual != expected or len({item["edition_key"] for item in expected}) != len(expected):
            raise FormalRemediationPlanError("duplicate edition evidence does not bind exact sources")
        core = {"resolution": row["resolution"], "editions": editions}
        if row.get("evidence_sha256") != canonical_digest(core):
            raise FormalRemediationPlanError("duplicate edition evidence digest mismatch")


def build_formal_remediation_plan(
    audit: Mapping[str, Any],
    work_order: Mapping[str, Any],
) -> dict[str, Any]:
    """Return an immutable plan; never access or mutate the remote library.

    ``work_order`` must contain confirmed TMDB identities and exact file
    witnesses.  Titles and source folder names never infer identity.
    """
    if (
        audit.get("schema_version") != 1
        or not isinstance(audit.get("library_root"), str)
        or not isinstance(audit.get("inventory_paths"), list)
    ):
        raise FormalRemediationPlanError("a complete read-only audit schema v1 is required")
    required = {
        "schema_version", "plan_id", "container_root", "root_identity", "works",
        "operations", "duplicate_edition_evidence", "source_inventory",
        "staged_artifacts",
        "rollback_root",
    }
    if set(work_order) != required or work_order.get("schema_version") != 1:
        raise FormalRemediationPlanError("work order fields do not match schema version 1")
    plan_id = work_order.get("plan_id")
    if not isinstance(plan_id, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", plan_id) is None:
        raise FormalRemediationPlanError("plan_id is not safe")
    container_root = _exact_path(work_order["container_root"], "container_root")
    library_root = _exact_path(audit["library_root"], "library_root")
    if not _inside(container_root, library_root):
        raise FormalRemediationPlanError("container_root is outside the audited library")
    rollback_root = _exact_path(work_order["rollback_root"], "rollback_root")
    if _inside(container_root, rollback_root) or _inside(rollback_root, container_root):
        raise FormalRemediationPlanError("formal tree and rollback root overlap")

    raw_works = work_order.get("works")
    if not isinstance(raw_works, list) or not raw_works:
        raise FormalRemediationPlanError("at least one confirmed work is required")
    works: list[CanonicalWork] = []
    for raw in raw_works:
        if not isinstance(raw, Mapping) or set(raw) != {
            "member_key", "identity", "title", "leaf_name", "poster_path"
        }:
            raise FormalRemediationPlanError("work fields do not match schema")
        works.append(CanonicalWork(
            member_key=str(raw["member_key"]), identity=_identity(raw["identity"]),
            title=str(raw["title"]), leaf_name=str(raw["leaf_name"]),
            poster_path=raw["poster_path"],
        ))
    raw_root_identity = work_order.get("root_identity")
    root_identity = None if raw_root_identity is None else _identity(raw_root_identity)
    tree = plan_canonical_work_tree(
        works, container_root=container_root, root_identity=root_identity,
    )
    placements = {row.member_key: row.target_root for row in tree.placements}
    observed_paths = _audit_paths(audit)
    source_inventory = work_order.get("source_inventory")
    if not isinstance(source_inventory, Mapping):
        raise FormalRemediationPlanError("source_inventory must be an object")
    staged_artifacts = work_order.get("staged_artifacts")
    if not isinstance(staged_artifacts, Mapping):
        raise FormalRemediationPlanError("staged_artifacts must be an object")

    raw_operations = work_order.get("operations")
    if not isinstance(raw_operations, list) or not raw_operations:
        raise FormalRemediationPlanError("at least one exact operation is required")
    duplicate_evidence = work_order.get("duplicate_edition_evidence")
    if not isinstance(duplicate_evidence, Mapping):
        raise FormalRemediationPlanError("duplicate_edition_evidence must be an object")
    operations: list[dict[str, Any]] = []
    item_ids: set[str] = set()
    for raw in raw_operations:
        if not isinstance(raw, Mapping) or set(raw) != {
            "item_id", "operation", "kind", "member_key", "source_path",
            "relative_path", "expected_size", "expected_sha256", "content_type",
            "episode_key", "edition_key", "duplicate_group", "retained_path",
            "retained_size", "retained_sha256",
        }:
            raise FormalRemediationPlanError("operation fields do not match schema")
        item_id, operation, kind = raw["item_id"], raw["operation"], raw["kind"]
        if not isinstance(item_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", item_id):
            raise FormalRemediationPlanError("operation item_id is not safe")
        if item_id in item_ids:
            raise FormalRemediationPlanError("duplicate operation item_id")
        item_ids.add(item_id)
        if operation not in {"transfer", "delete"} or kind not in {
            "media", "subtitle", "residual", "metadata", "artwork"
        }:
            raise FormalRemediationPlanError("unsupported formal remediation operation")
        source = _exact_path(raw["source_path"], "source_path")
        member_key = raw["member_key"]
        target: str | None = None
        relative: str | None = None
        if operation == "transfer":
            if not isinstance(member_key, str) or member_key not in placements:
                raise FormalRemediationPlanError("transfer lacks a confirmed canonical member")
            relative = _relative_path(raw["relative_path"])
            target = _exact_path(
                posixpath.join(placements[member_key], relative), "target_path"
            )
        elif raw["relative_path"] is not None or member_key is not None:
            raise FormalRemediationPlanError("delete must not invent a target/member")
        digest = _sha(raw["expected_sha256"], "expected_sha256")
        expected_size = _size(raw["expected_size"])
        witness = source_inventory.get(source)
        if (
            not isinstance(witness, Mapping)
            or set(witness) != {"expected_size", "expected_sha256"}
            or witness.get("expected_size") != expected_size
            or witness.get("expected_sha256") != digest
        ):
            raise FormalRemediationPlanError(
                f"source path lacks an exact approved inventory witness: {source}"
            )
        if operation == "delete" and kind != "residual":
            if kind not in {"subtitle", "media"}:
                raise FormalRemediationPlanError("only classified residuals or exact duplicate media may be deleted")
            duplicate_group = raw["duplicate_group"]
            retained_path = raw["retained_path"]
            retained_size = raw["retained_size"]
            retained_sha256 = raw["retained_sha256"]
            if not all(isinstance(value, str) and value for value in (duplicate_group, retained_path)):
                raise FormalRemediationPlanError("duplicate deletion requires group and retained path")
            retained_path = _exact_path(retained_path, "retained_path")
            retained_size = _size(retained_size)
            retained_sha256 = _sha(retained_sha256, "retained_sha256")
            retained_witness = source_inventory.get(retained_path)
            if (
                retained_path not in observed_paths
                or not _inside(retained_path, library_root)
                or not isinstance(retained_witness, Mapping)
                or retained_witness.get("expected_size") != retained_size
                or retained_witness.get("expected_sha256") != retained_sha256
            ):
                raise FormalRemediationPlanError("retained sibling is not bound to the complete audit inventory")
            if not isinstance(raw.get("episode_key"), str) or not isinstance(raw.get("edition_key"), str):
                raise FormalRemediationPlanError("duplicate deletion requires episode and edition identity")
            evidence = duplicate_evidence.get(duplicate_group)
            if not isinstance(evidence, Mapping) or evidence.get("resolution") != "delete_exact_duplicate":
                raise FormalRemediationPlanError("duplicate deletion lacks exact retained evidence")
            deleted = evidence.get("deleted")
            retained = evidence.get("retained")
            if not isinstance(deleted, list) or not isinstance(retained, list):
                raise FormalRemediationPlanError("duplicate deletion evidence is malformed")
            deleted_row = {
                "item_id": item_id, "source_path": source,
                "size": expected_size, "sha256": digest,
            }
            retained_row = {
                "path": retained_path, "size": retained_size, "sha256": retained_sha256,
            }
            if deleted != [deleted_row] or retained != [retained_row]:
                raise FormalRemediationPlanError("duplicate deletion evidence does not bind exact files")
            evidence_core = {"resolution": evidence["resolution"], "deleted": deleted, "retained": retained}
            if evidence.get("evidence_sha256") != canonical_digest(evidence_core):
                raise FormalRemediationPlanError("duplicate deletion evidence digest mismatch")
        elif operation == "delete":
            if any(raw.get(key) is not None for key in (
                "duplicate_group", "retained_path", "retained_size", "retained_sha256",
            )):
                raise FormalRemediationPlanError("ordinary residual deletion must not carry edition evidence")
        if kind not in {"metadata", "artwork"} and not _inside(source, library_root):
            raise FormalRemediationPlanError("media/residual source is outside the audited library root")
        if operation != "delete" and kind not in {"metadata", "artwork"} and source not in observed_paths:
            raise FormalRemediationPlanError(
                f"source path is not present in the complete pre-audit inventory: {source}"
            )
        if kind == "residual" and source not in observed_paths:
            raise FormalRemediationPlanError(
                f"residual deletion is not present in the audit snapshot: {source}"
            )
        if kind in {"metadata", "artwork"}:
            if not source.startswith("/quark/影视/ScrapeFlow/"):
                raise FormalRemediationPlanError("staged metadata/artwork must stay in ScrapeFlow staging")
            staged = staged_artifacts.get(source)
            if (
                not isinstance(staged, Mapping)
                or set(staged) != {"expected_size", "expected_sha256"}
                or staged.get("expected_size") != expected_size
                or staged.get("expected_sha256") != digest
            ):
                raise FormalRemediationPlanError(
                    f"source path is neither audited nor an exact staged artifact: {source}"
                )
        content_type = raw["content_type"]
        if not isinstance(content_type, str) or not content_type:
            raise FormalRemediationPlanError("content_type is required")
        if kind in {"metadata", "artwork"} and operation != "transfer":
            raise FormalRemediationPlanError("metadata/artwork must use exact-SHA transfer")
        operations.append({
            "item_id": item_id, "operation": operation, "kind": kind,
            "member_key": member_key, "source_path": source,
            "target_path": target, "relative_path": relative,
            "expected_size": expected_size,
            "expected_sha256": digest, "content_type": content_type,
            "episode_key": raw["episode_key"], "edition_key": raw["edition_key"],
            "duplicate_group": raw["duplicate_group"], "retained_path": raw["retained_path"],
            "retained_size": raw["retained_size"], "retained_sha256": raw["retained_sha256"],
        })
    _assert_pairwise_disjoint(operations)
    _assert_duplicate_editions_are_explicit(operations, duplicate_evidence)

    core = {
        "schema_version": 1,
        "kind": "formal-library-remediation-plan",
        "plan_id": plan_id,
        "audit_sha256": canonical_digest(audit),
        "library_root": library_root,
        "audit_inventory_paths": sorted(observed_paths),
        "rollback_root": rollback_root,
        "canonical_tree": {
            "container_root": tree.container_root,
            "root_identity": None if tree.root_identity is None else asdict(tree.root_identity),
            "identity_roots": [
                {"identity": asdict(identity), "target_root": root}
                for identity, root in tree.identity_roots
            ],
            "family_roots": list(tree.family_roots),
            "placements": [
                {"member_key": row.member_key, "identity": asdict(row.identity),
                 "target_root": row.target_root}
                for row in tree.placements
            ],
        },
        "operations": sorted(operations, key=lambda row: row["item_id"]),
        "duplicate_edition_evidence": duplicate_evidence,
        "source_inventory": source_inventory,
        "staged_artifacts": staged_artifacts,
        "commit_authority": "local_formal_maintenance_acceptance_only",
        "remote_mutations": False,
    }
    return {**core, "plan_sha256": canonical_digest(core)}


def validate_formal_remediation_plan(plan: Mapping[str, Any]) -> bool:
    required = {
        "schema_version", "kind", "plan_id", "audit_sha256", "library_root",
        "audit_inventory_paths",
        "rollback_root", "canonical_tree", "operations",
        "duplicate_edition_evidence", "source_inventory", "staged_artifacts",
        "commit_authority", "remote_mutations", "plan_sha256",
    }
    if set(plan) != required or plan.get("schema_version") != 1 or plan.get("kind") != "formal-library-remediation-plan":
        return False
    if plan.get("remote_mutations") is not False or plan.get("commit_authority") != "local_formal_maintenance_acceptance_only":
        return False
    for key in ("audit_sha256",):
        if not isinstance(plan.get(key), str) or _SHA256.fullmatch(plan[key]) is None:
            return False
    if not isinstance(plan.get("operations"), list) or not isinstance(plan.get("canonical_tree"), Mapping):
        return False
    if set(plan["canonical_tree"]) != {"container_root", "root_identity", "identity_roots", "family_roots", "placements"}:
        return False
    placements = {
        row.get("member_key"): row.get("target_root")
        for row in plan["canonical_tree"].get("placements", [])
        if isinstance(row, Mapping)
    }
    if not placements or any(not isinstance(key, str) or not isinstance(root, str) for key, root in placements.items()):
        return False
    source_inventory = plan.get("source_inventory")
    staged = plan.get("staged_artifacts")
    duplicate_evidence = plan.get("duplicate_edition_evidence")
    if not isinstance(source_inventory, Mapping) or not isinstance(staged, Mapping) or not isinstance(duplicate_evidence, Mapping):
        return False
    audit_inventory_paths = plan.get("audit_inventory_paths")
    if not isinstance(audit_inventory_paths, list) or not all(isinstance(path, str) for path in audit_inventory_paths):
        return False
    operations = plan["operations"]
    seen: set[str] = set()
    normalized_ops: list[dict[str, Any]] = []
    for row in operations:
        if not isinstance(row, Mapping) or set(row) != {
            "item_id", "operation", "kind", "member_key", "source_path", "target_path",
            "relative_path", "expected_size", "expected_sha256", "content_type",
            "episode_key", "edition_key", "duplicate_group", "retained_path",
            "retained_size", "retained_sha256",
        }:
            return False
        item_id = row.get("item_id")
        if not isinstance(item_id, str) or item_id in seen or _ID_RE.fullmatch(item_id) is None:
            return False
        seen.add(item_id)
        try:
            source = _exact_path(row["source_path"], "source_path")
            size = _size(row["expected_size"])
            digest = _sha(row["expected_sha256"], "expected_sha256")
        except (FormalRemediationPlanError, TypeError):
            return False
        witness = source_inventory.get(source)
        if not isinstance(witness, Mapping) or witness.get("expected_size") != size or witness.get("expected_sha256") != digest:
            return False
        operation, kind = row.get("operation"), row.get("kind")
        if operation not in {"transfer", "delete"} or kind not in {"media", "subtitle", "residual", "metadata", "artwork"}:
            return False
        if kind not in {"metadata", "artwork"} and source not in audit_inventory_paths:
            return False
        target = row.get("target_path")
        if operation == "transfer":
            member = row.get("member_key")
            relative = row.get("relative_path")
            if member not in placements or not isinstance(relative, str):
                return False
            try:
                expected_target = _exact_path(posixpath.join(placements[member], _relative_path(relative)), "target_path")
            except (FormalRemediationPlanError, TypeError):
                return False
            if target != expected_target:
                return False
            if kind not in {"metadata", "artwork"} and not _inside(source, str(plan["library_root"])):
                return False
        else:
            if target is not None or row.get("member_key") is not None or row.get("relative_path") is not None:
                return False
            if kind == "residual":
                if not _inside(source, str(plan["library_root"])):
                    return False
                if any(row.get(key) is not None for key in ("duplicate_group", "retained_path", "retained_size", "retained_sha256")):
                    return False
            elif kind not in {"subtitle", "media"} or not all(isinstance(row.get(key), str) and row.get(key) for key in ("episode_key", "edition_key", "duplicate_group")):
                return False
            else:
                evidence = duplicate_evidence.get(row["duplicate_group"])
                if not isinstance(evidence, Mapping) or evidence.get("resolution") != "delete_exact_duplicate":
                    return False
                if not isinstance(row.get("retained_path"), str):
                    return False
                try:
                    retained_path = _exact_path(row["retained_path"], "retained_path")
                    retained_size = _size(row["retained_size"])
                    retained_sha = _sha(row["retained_sha256"], "retained_sha256")
                except (FormalRemediationPlanError, TypeError):
                    return False
                if (
                    retained_path not in audit_inventory_paths
                    or not _inside(retained_path, str(plan["library_root"]))
                    or source_inventory.get(retained_path) != {
                        "expected_size": retained_size, "expected_sha256": retained_sha,
                    }
                ):
                    return False
                expected_deleted = [{"item_id": item_id, "source_path": source, "size": size, "sha256": digest}]
                expected_retained = [{"path": retained_path, "size": retained_size, "sha256": retained_sha}]
                if evidence.get("deleted") != expected_deleted or evidence.get("retained") != expected_retained or evidence.get("evidence_sha256") != canonical_digest({"resolution": evidence.get("resolution"), "deleted": evidence.get("deleted"), "retained": evidence.get("retained")}):
                    return False
        if kind in {"metadata", "artwork"}:
            if operation != "transfer" or not source.startswith("/quark/影视/ScrapeFlow/"):
                return False
            if staged.get(source) != {"expected_size": size, "expected_sha256": digest}:
                return False
        normalized_ops.append(dict(row))
    try:
        _assert_pairwise_disjoint(normalized_ops)
        for row in normalized_ops:
            HybridTransferSpec(
                batch_id=str(plan["plan_id"]), item_id=str(row["item_id"]),
                operation=str(row["operation"]), source_path=str(row["source_path"]),
                target_path=row["target_path"], expected_size=int(row["expected_size"]),
                expected_sha256=str(row["expected_sha256"]), content_type=str(row["content_type"]),
                rollback_root=str(plan["rollback_root"]),
            )
    except Exception:
        return False
    digest = plan.get("plan_sha256")
    if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
        return False
    return digest == canonical_digest({key: value for key, value in plan.items() if key != "plan_sha256"})


def plan_hybrid_specs(plan: Mapping[str, Any]) -> list[HybridTransferSpec]:
    """Translate a validated immutable plan to the only allowed file primitive."""
    if not validate_formal_remediation_plan(plan):
        raise FormalRemediationPlanError("formal remediation plan digest is invalid")
    return [HybridTransferSpec(
        batch_id=str(plan["plan_id"]), item_id=str(row["item_id"]),
        operation=str(row["operation"]), source_path=str(row["source_path"]),
        target_path=row["target_path"], expected_size=int(row["expected_size"]),
        expected_sha256=str(row["expected_sha256"]),
        content_type=str(row["content_type"]), rollback_root=str(plan["rollback_root"]),
    ) for row in plan["operations"]]


__all__ = [
    "FormalRemediationPlanError", "build_formal_remediation_plan", "canonical_digest",
    "canonical_json", "plan_hybrid_specs", "validate_formal_remediation_plan",
]
