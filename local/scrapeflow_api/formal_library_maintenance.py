"""Local-only acceptance coordinator for formal-library maintenance.

Engine may prepare and execute a sealed hybrid batch, but only this Local
acceptance boundary may commit it.  The two public phases intentionally leave
room for a fresh read-only post-audit between forward execution and commit.
"""

from __future__ import annotations

import json
import hashlib
import os
import re
from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any, Callable, Mapping

from engine.scrapeflow.formal_library_remediation import (
    canonical_digest,
    plan_hybrid_specs,
    validate_formal_remediation_plan,
)
from engine.scrapeflow.hybrid_remote_transaction import (
    HybridRemoteClient,
    abort_hybrid_batch,
    commit_hybrid_batch,
    load_sealed_batch_specs,
    prepare_hybrid_batch,
    run_hybrid_transfer,
)
from engine.scrapeflow.serialization import atomic_write_json


class FormalMaintenanceError(RuntimeError):
    """A maintenance execution/acceptance invariant is not satisfied."""


_SHA = re.compile(r"\A[0-9a-f]{64}\Z")


def _read_json(path: Path, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text("utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FormalMaintenanceError(f"cannot read {label}") from exc
    if not isinstance(value, Mapping):
        raise FormalMaintenanceError(f"{label} is not an object")
    return value


def _persistent_pause_snapshot(path: Path) -> tuple[Mapping[str, Any], str]:
    configured_state = os.environ.get("SCRAPEFLOW_STATE_DIR", "").strip()
    if configured_state:
        expected_path = (Path(configured_state).expanduser() / "global-control.json").resolve()
    else:
        expected_path = (Path(__file__).resolve().parents[2] / ".scrapeflow" / "global-control.json").resolve()
    if path.resolve() != expected_path:
        raise FormalMaintenanceError(
            "pause path is not the configured Local STATE_ROOT/global-control.json"
        )
    try:
        payload = path.read_bytes()
        value = json.loads(payload.decode("utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FormalMaintenanceError("cannot read durable global-control.json") from exc
    if not isinstance(value, Mapping):
        raise FormalMaintenanceError("durable global-control.json is not an object")
    if set(value) != {"version", "paused", "reason", "updated_at"}:
        raise FormalMaintenanceError("durable global-control.json fields are invalid")
    if value.get("version") != 1 or value.get("paused") is not True:
        raise FormalMaintenanceError("durable global control is not paused")
    if value.get("reason") is not None and not isinstance(value.get("reason"), str):
        raise FormalMaintenanceError("durable global pause reason is invalid")
    updated_at = value.get("updated_at")
    if not isinstance(updated_at, str):
        raise FormalMaintenanceError("durable global pause timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FormalMaintenanceError("durable global pause timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise FormalMaintenanceError("durable global pause timestamp lacks timezone")
    return value, hashlib.sha256(payload).hexdigest()


def persistent_pause_is_valid(path: Path) -> bool:
    try:
        _persistent_pause_snapshot(path)
    except FormalMaintenanceError:
        return False
    return True


def _require_plan(
    plan: Mapping[str, Any], approved_plan_sha256: str,
) -> None:
    if not validate_formal_remediation_plan(plan):
        raise FormalMaintenanceError("formal remediation plan is invalid")
    if not isinstance(approved_plan_sha256, str) or plan.get("plan_sha256") != approved_plan_sha256:
        raise FormalMaintenanceError("explicit approved plan SHA-256 does not match")


def _persist_immutable(path: Path, value: Mapping[str, Any]) -> None:
    if path.exists():
        if _read_json(path, "immutable maintenance receipt") != value:
            raise FormalMaintenanceError("immutable maintenance receipt conflicts")
        return
    atomic_write_json(path, dict(value), allow_nan=False, sort_keys=True)


def _strict_receipt(path: Path, *, kind: str, plan: Mapping[str, Any],
                    approved_plan_sha256: str) -> Mapping[str, Any]:
    value = _read_json(path, f"{kind} receipt")
    required = {
        "schema_version", "kind", "plan_id", "plan_sha256",
        "global_control_sha256", "committed", "requires", "receipt_sha256",
    } if kind == "formal-maintenance-forward-complete" else {
        "schema_version", "kind", "plan_id", "plan_sha256", "batch_id",
        "global_control_sha256", "commit_authority", "items", "receipt_sha256",
    }
    if set(value) != required or value.get("schema_version") != 1 or value.get("kind") != kind:
        raise FormalMaintenanceError(f"{kind} receipt schema is invalid")
    if value.get("plan_id") != plan.get("plan_id") or value.get("plan_sha256") != approved_plan_sha256:
        raise FormalMaintenanceError(f"{kind} receipt is bound to another plan")
    digest = value.get("receipt_sha256")
    if not isinstance(digest, str) or _SHA.fullmatch(digest) is None:
        raise FormalMaintenanceError(f"{kind} receipt digest is invalid")
    if digest != canonical_digest({key: item for key, item in value.items() if key != "receipt_sha256"}):
        raise FormalMaintenanceError(f"{kind} receipt digest mismatch")
    return value


def _remote_witness(client: HybridRemoteClient, path: str) -> dict[str, Any]:
    before = client.stat_exact(path)
    if before is None:
        raise FormalMaintenanceError(f"post-audit target is absent: {path}")
    size, digest = 0, hashlib.sha256()
    with client.open_reader(path) as reader:
        while chunk := reader.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    after = client.stat_exact(path)
    actual = digest.hexdigest()
    if (
        after is None or before.size != size or after.size != size
        or (before.version is not None and after.version is not None and before.version != after.version)
        or (before.sha256 is not None and before.sha256 != actual)
        or (after.sha256 is not None and after.sha256 != actual)
    ):
        raise FormalMaintenanceError(f"post-audit target changed during full read: {path}")
    return {"size": size, "sha256": actual}


def execute_formal_remediation(
    client: HybridRemoteClient,
    *,
    state_root: Path,
    pause_receipt_path: Path,
    plan: Mapping[str, Any],
    approved_plan_sha256: str,
) -> dict[str, Any]:
    """Seal and execute an approved batch without committing rollback data."""
    _require_plan(plan, approved_plan_sha256)
    _pause, pause_sha256 = _persistent_pause_snapshot(pause_receipt_path)
    plan_id = str(plan["plan_id"])
    batch_root = state_root / plan_id
    batch_root.mkdir(parents=True, exist_ok=True)
    _persist_immutable(batch_root / "formal-plan.json", plan)
    specs = plan_hybrid_specs(plan)
    forwarded = False
    try:
        prepared = prepare_hybrid_batch(client, state_root=state_root, specs=specs)
        sealed = {
            "schema_version": 1,
            "kind": "formal-maintenance-sealed",
            "plan_id": plan_id,
            "plan_sha256": approved_plan_sha256,
            "batch_id": plan_id,
            "global_control_sha256": pause_sha256,
            "commit_authority": "local_formal_maintenance_acceptance_only",
            "items": [
                {
                    "item_id": spec.item_id,
                    "operation": spec.operation,
                    "source_path": spec.source_path,
                    "target_path": spec.target_path,
                    "expected_size": result.size,
                    "expected_sha256": result.sha256,
                    "rollback_path": spec.rollback_path,
                }
                for spec, result in zip(specs, prepared, strict=True)
            ],
        }
        sealed["receipt_sha256"] = canonical_digest(sealed)
        _persist_immutable(batch_root / "maintenance-sealed.json", sealed)
        for spec in specs:
            _current, current_pause_sha256 = _persistent_pause_snapshot(pause_receipt_path)
            if current_pause_sha256 != pause_sha256:
                raise FormalMaintenanceError("durable global control changed during execution")
            forwarded = True
            run_hybrid_transfer(client, state_root=state_root, spec=spec)
        _current, current_pause_sha256 = _persistent_pause_snapshot(pause_receipt_path)
        if current_pause_sha256 != pause_sha256:
            raise FormalMaintenanceError("durable global control changed after execution")
        receipt = {
            "schema_version": 1,
            "kind": "formal-maintenance-forward-complete",
            "plan_id": plan_id,
            "plan_sha256": approved_plan_sha256,
            "global_control_sha256": pause_sha256,
            "committed": False,
            "requires": "fresh_read_only_post_audit_and_local_acceptance",
        }
        receipt["receipt_sha256"] = canonical_digest(receipt)
        _persist_immutable(batch_root / "maintenance-forward-complete.json", receipt)
        return receipt
    except Exception as primary:
        if forwarded:
            try:
                abort_hybrid_batch(client, state_root=state_root, specs=specs)
            except Exception as rollback_error:
                raise FormalMaintenanceError(
                    "forward execution failed and automatic restore is uncertain"
                ) from rollback_error
        raise


def build_formal_maintenance_acceptance(
    plan: Mapping[str, Any],
    *,
    post_audit_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Build immutable post-audit evidence bound to every planned operation."""
    if not validate_formal_remediation_plan(plan):
        raise FormalMaintenanceError("cannot accept an invalid plan")
    if not formal_post_audit_evidence_is_valid(post_audit_evidence):
        raise FormalMaintenanceError("formal post-audit evidence is invalid")
    library_audit = post_audit_evidence["library_audit"]
    if library_audit.get("library_root") != plan.get("library_root"):
        raise FormalMaintenanceError("post audit covers a different library root")
    if library_audit.get("canonical_tree_sha256") != canonical_digest(plan["canonical_tree"]):
        raise FormalMaintenanceError("post audit canonical tree is not the approved tree")
    target_witnesses = post_audit_evidence["target_witnesses"]
    absent_paths = post_audit_evidence["absent_paths"]
    blocking_issue_codes = post_audit_evidence["blocking_issue_codes"]
    if blocking_issue_codes:
        raise FormalMaintenanceError("post audit still contains blocking issues")
    absent = sorted(set(absent_paths))
    for operation in plan["operations"]:
        if operation["source_path"] not in absent:
            raise FormalMaintenanceError("post audit does not prove every source departed")
        target = operation["target_path"]
        witness_targets: list[tuple[str, int, str]] = []
        if target is not None:
            witness_targets.append((target, operation["expected_size"], operation["expected_sha256"]))
        if operation.get("retained_path") is not None:
            witness_targets.append((operation["retained_path"], operation["retained_size"], operation["retained_sha256"]))
        for witness_path, expected_size, expected_sha256 in witness_targets:
            witness = target_witnesses.get(witness_path)
            if not isinstance(witness, Mapping) or set(witness) != {"size", "sha256"}:
                raise FormalMaintenanceError("post audit lacks an exact retained/target witness")
            if witness.get("size") != expected_size or witness.get("sha256") != expected_sha256:
                raise FormalMaintenanceError("post audit retained/target witness differs from the plan")
    core = {
        "schema_version": 1,
        "kind": "formal-library-maintenance-acceptance",
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "pre_audit_sha256": plan["audit_sha256"],
        "post_audit_sha256": canonical_digest(library_audit),
        "post_audit_evidence_sha256": post_audit_evidence["evidence_sha256"],
        "post_audit_evidence": dict(post_audit_evidence),
        "canonical_tree_sha256": canonical_digest(plan["canonical_tree"]),
        "target_witnesses": dict(sorted(target_witnesses.items())),
        "absent_paths": absent,
        "blocking_issue_codes": [],
        "commit_authority": "local_formal_maintenance_acceptance_only",
    }
    return {**core, "acceptance_sha256": canonical_digest(core)}


def build_formal_post_audit_evidence(
    library_audit: Mapping[str, Any],
    *,
    target_witnesses: Mapping[str, Mapping[str, Any]],
    absent_paths: list[str],
    blocking_issue_codes: list[str],
) -> dict[str, Any]:
    """Seal a read-only library audit plus exact file witnesses."""
    if library_audit.get("schema_version") != 1 or not isinstance(
        library_audit.get("library_root"), str
    ):
        raise FormalMaintenanceError("post library audit schema is invalid")
    if not isinstance(library_audit.get("canonical_tree_sha256"), str) or _SHA.fullmatch(library_audit["canonical_tree_sha256"]) is None:
        raise FormalMaintenanceError("post audit lacks canonical tree evidence")
    for path, witness in target_witnesses.items():
        if (
            not isinstance(path, str)
            or not isinstance(witness, Mapping)
            or set(witness) != {"size", "sha256"}
            or isinstance(witness.get("size"), bool)
            or not isinstance(witness.get("size"), int)
            or witness["size"] < 0
            or not isinstance(witness.get("sha256"), str)
            or _SHA.fullmatch(witness["sha256"]) is None
        ):
            raise FormalMaintenanceError("post audit contains an invalid exact witness")
    if not all(isinstance(value, str) and value.startswith("/") for value in absent_paths):
        raise FormalMaintenanceError("post audit absent paths are invalid")
    if not all(isinstance(value, str) and value for value in blocking_issue_codes):
        raise FormalMaintenanceError("post audit blocking issue codes are invalid")
    derived_codes: set[str] = set()
    for group in (library_audit.get("projects", []), library_audit.get("movies", [])):
        if isinstance(group, list):
            for work in group:
                if isinstance(work, Mapping) and isinstance(work.get("issues"), list):
                    for issue in work["issues"]:
                        if isinstance(issue, Mapping) and issue.get("severity") in {"critical", "high"} and isinstance(issue.get("code"), str):
                            derived_codes.add(str(issue["code"]))
    for key in ("nfo_parse_errors", "uncovered_media", "duplicate_tmdb_ids"):
        if library_audit.get(key):
            derived_codes.add(key)
    if sorted(set(blocking_issue_codes)) != sorted(derived_codes):
        raise FormalMaintenanceError("post audit blocking issue codes are not derived from the audit")
    core = {
        "schema_version": 1,
        "kind": "formal-library-post-audit-evidence",
        "library_audit": dict(library_audit),
        "target_witnesses": dict(sorted(target_witnesses.items())),
        "absent_paths": sorted(set(absent_paths)),
        "blocking_issue_codes": sorted(set(blocking_issue_codes)),
        "remote_mutations": False,
    }
    return {**core, "evidence_sha256": canonical_digest(core)}


def formal_post_audit_evidence_is_valid(value: Mapping[str, Any]) -> bool:
    if (
        value.get("schema_version") != 1
        or value.get("kind") != "formal-library-post-audit-evidence"
        or value.get("remote_mutations") is not False
    ):
        return False
    digest = value.get("evidence_sha256")
    if not isinstance(digest, str) or _SHA.fullmatch(digest) is None:
        return False
    return digest == canonical_digest({key: item for key, item in value.items() if key != "evidence_sha256"})


def formal_maintenance_acceptance_is_valid(
    plan: Mapping[str, Any], acceptance: Mapping[str, Any],
) -> bool:
    try:
        rebuilt = build_formal_maintenance_acceptance(
            plan,
            post_audit_evidence=acceptance["post_audit_evidence"],
        )
    except (KeyError, TypeError, FormalMaintenanceError):
        return False
    return dict(acceptance) == rebuilt


def accept_and_commit_formal_remediation(
    client: HybridRemoteClient,
    *,
    state_root: Path,
    pause_receipt_path: Path,
    plan: Mapping[str, Any],
    approved_plan_sha256: str,
    acceptance: Mapping[str, Any],
    post_audit_reader: Callable[[], Mapping[str, Any]],
) -> dict[str, Any]:
    """Commit only after Local validates fresh, exact post-audit acceptance."""
    _require_plan(plan, approved_plan_sha256)
    _pause, pause_sha256 = _persistent_pause_snapshot(pause_receipt_path)
    if not formal_maintenance_acceptance_is_valid(plan, acceptance):
        raise FormalMaintenanceError("Local formal maintenance acceptance is invalid")
    fresh_audit = post_audit_reader()
    evidence = acceptance["post_audit_evidence"]
    fresh_audit = dict(fresh_audit)
    if fresh_audit.get("canonical_tree_sha256") != canonical_digest(plan["canonical_tree"]):
        raise FormalMaintenanceError("fresh post-audit canonical tree differs from plan")
    batch_root = state_root / str(plan["plan_id"])
    forward = _strict_receipt(
        batch_root / "maintenance-forward-complete.json",
        kind="formal-maintenance-forward-complete", plan=plan,
        approved_plan_sha256=approved_plan_sha256,
    )
    if forward.get("committed") is not False or forward.get("global_control_sha256") != pause_sha256:
        raise FormalMaintenanceError("global pause changed since sealed forward execution")
    sealed = _strict_receipt(
        batch_root / "maintenance-sealed.json", kind="formal-maintenance-sealed",
        plan=plan, approved_plan_sha256=approved_plan_sha256,
    )
    if sealed.get("commit_authority") != "local_formal_maintenance_acceptance_only":
        raise FormalMaintenanceError("sealed receipt has an invalid commit authority")
    _persist_immutable(batch_root / "maintenance-acceptance.json", acceptance)
    specs = plan_hybrid_specs(plan)
    try:
        sealed_specs = load_sealed_batch_specs(state_root=state_root, batch_id=str(plan["plan_id"]))
    except Exception as exc:
        raise FormalMaintenanceError("durable hybrid batch is not sealed") from exc
    if [spec.to_dict() for spec in sealed_specs] != [spec.to_dict() for spec in sorted(specs, key=lambda item: item.item_id)]:
        raise FormalMaintenanceError("sealed hybrid membership differs from approved plan")
    if sealed.get("batch_id") != plan.get("plan_id"):
        raise FormalMaintenanceError("sealed maintenance batch id is invalid")
    expected_items = [
        {
            "item_id": spec.item_id, "operation": spec.operation,
            "source_path": spec.source_path, "target_path": spec.target_path,
            "expected_size": spec.expected_size, "expected_sha256": spec.expected_sha256,
            "rollback_path": spec.rollback_path,
        }
        for spec in sorted(specs, key=lambda item: item.item_id)
    ]
    if sealed.get("items") != expected_items:
        raise FormalMaintenanceError("sealed maintenance item witnesses differ from plan")
    for spec in specs:
        if client.stat_exact(spec.source_path) is not None:
            raise FormalMaintenanceError("post-audit source is still present")
        if spec.target_path is not None:
            witness = _remote_witness(client, spec.target_path)
            expected = acceptance["target_witnesses"].get(spec.target_path)
            if witness != expected:
                raise FormalMaintenanceError("post-audit target differs from Local acceptance")
    for operation in plan["operations"]:
        retained = operation.get("retained_path")
        if retained is not None:
            witness = _remote_witness(client, retained)
            if witness != acceptance["target_witnesses"].get(retained):
                raise FormalMaintenanceError("post-audit retained sibling differs from Local acceptance")
    fresh_evidence = build_formal_post_audit_evidence(
        fresh_audit,
        target_witnesses=dict(acceptance["target_witnesses"]),
        absent_paths=list(acceptance["absent_paths"]),
        blocking_issue_codes=list(acceptance["blocking_issue_codes"]),
    )
    if fresh_evidence != evidence:
        raise FormalMaintenanceError("fresh Local post-audit evidence differs from acceptance")
    commit_hybrid_batch(client, state_root=state_root, specs=specs)
    receipt = {
        "schema_version": 1,
        "kind": "formal-maintenance-committed",
        "plan_id": plan["plan_id"],
        "plan_sha256": approved_plan_sha256,
        "acceptance_sha256": acceptance["acceptance_sha256"],
        "global_control_sha256": pause_sha256,
        "committed": True,
    }
    receipt["receipt_sha256"] = canonical_digest(receipt)
    _persist_immutable(batch_root / "maintenance-committed.json", receipt)
    return receipt


__all__ = [
    "FormalMaintenanceError", "accept_and_commit_formal_remediation",
    "build_formal_maintenance_acceptance", "build_formal_post_audit_evidence",
    "execute_formal_remediation", "formal_maintenance_acceptance_is_valid",
    "formal_post_audit_evidence_is_valid", "persistent_pause_is_valid",
]
