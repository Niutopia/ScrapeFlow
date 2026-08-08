"""Plain serialization and finalization for the current plan model."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
import json
from pathlib import Path
import re
from typing import Any, Mapping
import unicodedata

from .errors import PlanError
from .media_naming import subtitle_language
from .models import Plan, PlanNotice, PlannedCleanup, PlannedFile, PlannedProblem
from .remote_paths import join_remote, normalize_remote_path


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PlanError(f"{field} 必须是对象")
    return dict(value)


def _text(value: Any, field: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or (not optional and not value):
        raise PlanError(f"{field} 必须是字符串")
    return value


def _size(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PlanError(f"{field} 必须是非负整数")
    return value


def _list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise PlanError(f"{field} 必须是数组")
    return value


def plan_to_dict(plan: Plan) -> dict[str, Any]:
    """Serialize one current plan using only its current model fields."""
    if not isinstance(plan, Plan):
        raise TypeError("plan 必须是 Plan")
    return {
        "mode": plan.mode,
        "source_root": plan.source_root,
        "target_root": plan.target_root,
        "files": [asdict(item) for item in plan.files],
        "cleanup_files": [asdict(item) for item in plan.cleanup_files],
        "problem_files": [asdict(item) for item in plan.problem_files],
        "warnings": list(plan.warnings),
        "notices": [asdict(item) for item in plan.notices],
        "metadata": dict(plan.metadata),
        "decision_trace": dict(plan.decision_trace),
        "scan_report": dict(plan.scan_report),
    }


def plan_from_dict(raw: Mapping[str, Any]) -> Plan:
    """Parse the only supported plan shape: the current in-process model."""
    body = _object(raw, "plan")
    allowed = {
        "mode", "source_root", "target_root", "files", "cleanup_files",
        "problem_files", "warnings", "notices", "metadata",
        "decision_trace", "scan_report",
    }
    unknown = sorted(set(body) - allowed)
    if unknown:
        raise PlanError(f"计划包含未知字段: {', '.join(unknown)}")

    files: list[PlannedFile] = []
    for index, value in enumerate(_list(body.get("files"), "files")):
        item = _object(value, f"files[{index}]")
        files.append(PlannedFile(
            source_path=str(_text(item.get("source_path"), f"files[{index}].source_path")),
            source_dir=str(_text(item.get("source_dir"), f"files[{index}].source_dir")),
            original_name=str(_text(item.get("original_name"), f"files[{index}].original_name")),
            final_name=str(_text(item.get("final_name"), f"files[{index}].final_name")),
            target_dir=str(_text(item.get("target_dir"), f"files[{index}].target_dir")),
            media_kind=str(_text(item.get("media_kind"), f"files[{index}].media_kind")),
            episode_key=_text(item.get("episode_key"), f"files[{index}].episode_key", optional=True),
            source_size=_size(item.get("source_size"), f"files[{index}].source_size"),
            source_modified=_text(item.get("source_modified"), f"files[{index}].source_modified", optional=True),
        ))

    cleanup: list[PlannedCleanup] = []
    for index, value in enumerate(_list(body.get("cleanup_files", []), "cleanup_files")):
        item = _object(value, f"cleanup_files[{index}]")
        cleanup.append(PlannedCleanup(
            source_path=str(_text(item.get("source_path"), f"cleanup_files[{index}].source_path")),
            source_dir=str(_text(item.get("source_dir"), f"cleanup_files[{index}].source_dir")),
            original_name=str(_text(item.get("original_name"), f"cleanup_files[{index}].original_name")),
            reason=str(_text(item.get("reason"), f"cleanup_files[{index}].reason")),
            source_size=_size(item.get("source_size"), f"cleanup_files[{index}].source_size"),
            source_modified=_text(item.get("source_modified"), f"cleanup_files[{index}].source_modified", optional=True),
        ))

    problems: list[PlannedProblem] = []
    for index, value in enumerate(_list(body.get("problem_files", []), "problem_files")):
        item = _object(value, f"problem_files[{index}]")
        problems.append(PlannedProblem(
            source_path=str(_text(item.get("source_path"), f"problem_files[{index}].source_path")),
            reason=str(_text(item.get("reason"), f"problem_files[{index}].reason")),
            target_path=_text(item.get("target_path"), f"problem_files[{index}].target_path", optional=True),
        ))

    notices: list[PlanNotice] = []
    for index, value in enumerate(_list(body.get("notices", []), "notices")):
        item = _object(value, f"notices[{index}]")
        notices.append(PlanNotice(
            code=str(_text(item.get("code"), f"notices[{index}].code")),
            severity=str(_text(item.get("severity"), f"notices[{index}].severity")),
            message=str(_text(item.get("message"), f"notices[{index}].message")),
            details=_object(item.get("details", {}), f"notices[{index}].details"),
        ))

    warnings = _list(body.get("warnings", []), "warnings")
    if not all(isinstance(item, str) for item in warnings):
        raise PlanError("warnings 必须只包含字符串")
    return Plan(
        mode=str(_text(body.get("mode"), "mode")),
        source_root=str(_text(body.get("source_root"), "source_root")),
        target_root=str(_text(body.get("target_root"), "target_root")),
        files=files,
        cleanup_files=cleanup,
        problem_files=problems,
        warnings=list(warnings),
        notices=notices,
        metadata=_object(body.get("metadata", {}), "metadata"),
        decision_trace=_object(body.get("decision_trace", {}), "decision_trace"),
        scan_report=_object(body.get("scan_report", {}), "scan_report"),
    )


def _collision_key(value: str) -> str:
    """Use the current path/name comparison normalization without core imports."""
    return unicodedata.normalize("NFC", value).casefold().rstrip(" .")


def _subtitle_companion_key(target_dir: str, final_name: str) -> tuple[str, str]:
    """Map an external subtitle filename to its exact final-video stem.

    The current naming policy emits language suffixes such as ``.zh-CN`` or
    ``.subtitle2`` immediately before the subtitle extension.  Removing only
    that suffix preserves a video's `` - v2``/edition identity, so alternates
    cannot be coalesced across distinct final video files.
    """
    stem = Path(final_name).stem
    stem = re.sub(
        r"\.(?:zh-CN|zh-TW|en|ja)(?:\.\d+)*$|\.subtitle(?:\.\d+|\d*)$",
        "",
        stem,
        flags=re.IGNORECASE,
    )
    return _collision_key(normalize_remote_path(target_dir)), _collision_key(stem)


def _subtitle_track_rank(items: list[PlannedFile]) -> tuple[object, ...]:
    """Return a deterministic, current-policy rank for one logical track."""
    representative = min(items, key=lambda item: _collision_key(item.source_path))
    language_rank = {
        "zh-CN": 0,
        "zh-TW": 1,
        "en": 2,
        "ja": 3,
        None: 4,
    }.get(subtitle_language(representative.final_name), 5)
    # Keep the retired selector's narrow release heuristic: a same-language
    # copy under a literal ``子集化字幕`` source directory is less useful than
    # the ordinary release track.  This is only a deterministic tie-breaker;
    # it never grants cleanup authority to either source.
    source_key = unicodedata.normalize("NFKC", representative.source_path).casefold()
    subset_rank = 1 if re.search(r"(?:^|/)子集化字幕(?:/|$)", source_key) else 0
    extension_rank = {
        ".ass": 0,
        ".ssa": 1,
        ".srt": 2,
        ".vtt": 3,
        ".idx": 4,
        ".sub": 4,
        ".sup": 5,
    }.get(Path(representative.final_name).suffix.casefold(), 10)
    canonical_suffix_rank = 0 if re.search(
        r"\.(?:zh-CN|zh-TW|en|ja)$|\.subtitle$",
        Path(representative.final_name).stem,
        re.IGNORECASE,
    ) else 1
    return (
        language_rank,
        subset_rank,
        extension_rank,
        canonical_suffix_rank,
        _collision_key(representative.source_path),
    )


def _retain_one_subtitle_track_per_exact_video(plan: Plan) -> None:
    """Keep one logical subtitle track beside each exact planned video.

    ``.idx``/``.sub`` siblings have the same filename stem and therefore stay
    together as one logical track.  Other tracks remain untouched at source
    and are recorded as deferred evidence; they are never problem files or
    cleanup candidates.
    """
    video_keys = {
        _subtitle_companion_key(item.target_dir, item.final_name)
        for item in plan.files
        if item.media_kind == "video"
    }
    tracks: dict[tuple[str, str], dict[str, list[PlannedFile]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for item in plan.files:
        if item.media_kind != "subtitle":
            continue
        companion_key = _subtitle_companion_key(item.target_dir, item.final_name)
        if companion_key not in video_keys:
            continue
        track_key = _collision_key(Path(item.final_name).stem)
        tracks[companion_key][track_key].append(item)

    demoted_ids: set[int] = set()
    demoted: list[dict[str, str]] = []
    for track_groups in tracks.values():
        if len(track_groups) <= 1:
            continue
        preferred_key, preferred_items = min(
            track_groups.items(),
            key=lambda pair: _subtitle_track_rank(pair[1]),
        )
        preferred_path = min(
            (item.source_path for item in preferred_items),
            key=_collision_key,
        )
        for track_key, items in track_groups.items():
            if track_key == preferred_key:
                continue
            for item in items:
                demoted_ids.add(id(item))
                demoted.append({
                    "source_path": item.source_path,
                    "planned_target_path": join_remote(item.target_dir, item.final_name),
                    "action": "defer_until_exact_video_subtitle_closure",
                    "reason": "alternate_subtitle_track",
                    "preferred_source_path": preferred_path,
                })
    if not demoted_ids:
        return

    plan.files = [item for item in plan.files if id(item) not in demoted_ids]
    deferred = plan.scan_report.setdefault("deferred_subtitles", [])
    if not isinstance(deferred, list):
        raise PlanError("scan_report.deferred_subtitles 必须是数组")
    existing_sources = {
        _collision_key(str(row.get("source_path")))
        for row in deferred
        if isinstance(row, Mapping) and isinstance(row.get("source_path"), str)
    }
    deferred.extend(
        row for row in demoted
        if _collision_key(row["source_path"]) not in existing_sources
    )
    warning = (
        "同一视频的多份外挂字幕仅保留 1 条首选轨道；"
        f"{len(demoted_ids)} 个备选字幕已保留在源目录"
    )
    if warning not in plan.warnings:
        plan.warnings.append(warning)


def finalize_plan(plan: Plan) -> Plan:
    """Complete ordinary summaries needed by the automatic runner."""
    _retain_one_subtitle_track_per_exact_video(plan)
    existing = {(notice.code, notice.message) for notice in plan.notices}
    for warning in plan.warnings:
        key = ("planning_warning", warning)
        if key not in existing:
            plan.notices.append(PlanNotice(
                code=key[0],
                severity="warning",
                message=warning,
            ))
            existing.add(key)

    resource_gaps = [
        dict(item)
        for item in (plan.scan_report.get("resource_gaps") or [])
        if isinstance(item, Mapping)
    ]
    media_sources = {item.source_path for item in plan.files}
    cleanup_sources = {item.source_path for item in plan.cleanup_files}
    problem_sources = {item.source_path for item in plan.problem_files}
    report = dict(plan.scan_report)
    report.update({
        "total_files": len(media_sources | cleanup_sources | problem_sources),
        "matched_files": len(media_sources - problem_sources),
        "skipped_files": len((cleanup_sources | problem_sources) - media_sources),
        "anomaly_files": len(problem_sources),
        "cleanup_files": len(plan.cleanup_files),
        "source_root": plan.source_root,
        "target_root": plan.target_root,
    })
    if resource_gaps:
        report["resource_gaps"] = resource_gaps
    plan.scan_report = report
    return plan


def load_json_text(text: str) -> dict[str, Any]:
    """Load a JSON object for explicit mapping files used by planners."""
    try:
        value = json.loads(text)
    except (TypeError, json.JSONDecodeError) as exc:
        raise PlanError("JSON 内容无效") from exc
    return _object(value, "JSON 根节点")


__all__ = ["finalize_plan", "load_json_text", "plan_from_dict", "plan_to_dict"]
