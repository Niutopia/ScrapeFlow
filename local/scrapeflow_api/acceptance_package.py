"""Generate a local acceptance-package draft without touching runtime data."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

from .isolated_preflight import (
    isolated_preflight_issues,
    validate_isolated_preflight_report,
)
from .release_checks import local_deployment_contract_issues, project_root
from .release_checks import OFFLINE_ARIA2_RPC_SECRET_ENV
from .runtime_readiness import runtime_readiness_evidence_issues


ORDINARY_SAMPLES = (
    ("电影", "创建 RootJob 时选择 movie 后入库，回读正确"),
    ("番剧归档", "归档预处理后入库，回读正确"),
    ("美剧季度目录", "创建 RootJob 时选择 us_tv 后入库，回读正确"),
    ("错误密码", "停在归档错误，source 保留"),
    ("正式目标冲突", "停止，不覆盖"),
    ("cancel", "停止，source/staging 保留在本任务范围"),
    ("执行中重启", "重启后不重复写入"),
)
REPLENISHMENT_SAMPLES = (
    ("有效 quark_share", "第一阶完成，后二阶未调用"),
    (
        "quark_share 完整排除、有效 alist_offline",
        "AList/aria2 离线完成，本地 Torrent 未调用",
    ),
    ("quark_share 与 alist_offline 完整排除后 magnet", "本地 Torrent 完成"),
    ("有效 quark_share 候选时 Helper 不可用", "停在 quark_share，不降阶"),
    ("AList 提交响应丢失后 API 重启", "对账同一 AList task，不重复提交"),
    ("错误候选", "不创建 Engine child"),
    ("staging 内容不符", "不进入正式库"),
    ("缺字幕", "只安装正确目标语言侧车"),
)
FINAL_CHECKS = (
    "普通电影、番剧、美剧均正确",
    "归档和错误密码行为正确",
    "手工审计不修改正式库",
    "quark_share、alist_offline、magnet 三条获取线路全部真实可执行",
    "三条线路全部先到任务 staging",
    "三条线路全部使用同一个受限 Engine 和 writer",
    "第一阶成功时后二阶不调用",
    "第二阶成功时本地 Torrent 不调用",
    "基础设施故障绝不降阶",
    "in-doubt 不重复提交",
    "不进行媒体内容指纹校验",
    "正式目标不覆盖",
    "restart 不重复写入",
    "cleanup 不触碰正式库或其他任务",
    "AList、ScrapeFlow 状态、正式媒体库恢复点真实可用",
    "旧 backlog、gap、staging 未被批量恢复或删除",
    "所有自动 gate 初始关闭",
    "最终是否开启自动审计和自动补源已由用户单独授权",
)


# Git currently uses SHA-1 by default, but repositories initialized with the
# SHA-256 object format have a 64-character full object ID.  Evidence must use
# a full ID, never the display-oriented short hash shown in package headings.
_FULL_GIT_COMMIT_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_SHA512_RE = re.compile(r"[0-9a-f]{128}\Z")


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str = ""


CommandRunner = Callable[[tuple[str, ...], Path, dict[str, str] | None], CommandResult]


def _run_command(args: tuple[str, ...], cwd: Path, env: dict[str, str] | None = None) -> CommandResult:
    completed = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return CommandResult(completed.returncode, completed.stdout, completed.stderr)


def _one_line(value: str) -> str:
    return " ".join(value.strip().split())


def _git_value(
    root: Path,
    runner: CommandRunner,
    args: tuple[str, ...],
    *,
    fallback: str = "unavailable",
) -> str:
    result = runner(("git", *args), root, None)
    if result.returncode != 0:
        return fallback
    return _one_line(result.stdout) or fallback


def git_evidence(root: Path | None = None, runner: CommandRunner = _run_command) -> dict[str, object]:
    """Return branch, commit, and worktree status for the package header."""
    base = project_root() if root is None else Path(root)
    status = _git_value(base, runner, ("status", "--short"), fallback="")
    return {
        "branch": _git_value(base, runner, ("rev-parse", "--abbrev-ref", "HEAD")),
        "commit": _git_value(base, runner, ("rev-parse", "--short", "HEAD")),
        "worktree_clean": status == "",
        "status": status or "clean",
    }


def compose_evidence(root: Path | None = None, runner: CommandRunner = _run_command) -> dict[str, object]:
    """Return the resolved Compose service summary when Docker is available."""
    base = project_root() if root is None else Path(root)
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
        "SCRAPEFLOW_HOST_STATE_ROOT": "/tmp/scrapeflow-state",
        # This read-only Compose rendering needs a value solely to satisfy
        # required interpolation. Never include the resolved environment in
        # the evidence package.
        OFFLINE_ARIA2_RPC_SECRET_ENV: "acceptance-compose-placeholder-only",
    }
    result = runner(("docker", "compose", "config", "--format", "json"), base, env)
    if result.returncode != 0:
        return {
            "status": "不可用",
            "error": _one_line(result.stderr or result.stdout) or "docker compose config failed",
            "services": [],
        }
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {
            "status": "不可用",
            "error": f"docker compose JSON parse failed: {exc}",
            "services": [],
        }
    services = []
    for name, service in sorted((payload.get("services") or {}).items()):
        ports = [
            f"{row.get('host_ip', '')}:{row.get('published', '')}->{row.get('target', '')}/{row.get('protocol', '')}"
            for row in service.get("ports") or []
            if isinstance(row, dict)
        ]
        environment = service.get("environment") or {}
        services.append({
            "name": name,
            "image": service.get("image") or "",
            "ports": ports,
            "start_paused": environment.get("SCRAPEFLOW_START_PAUSED"),
            "intake_monitor": environment.get("SCRAPEFLOW_INTAKE_MONITOR"),
            "automatic_audit": environment.get("SCRAPEFLOW_AUTOMATIC_AUDIT"),
            "provider_gate": environment.get("SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED"),
            "provider_workers": environment.get("SCRAPEFLOW_PROVIDER_WORKERS"),
        })
    return {"status": "可读", "error": "", "services": services}


def isolated_preflight_evidence(
    declaration: Mapping[str, object] | None = None,
    *,
    report: Mapping[str, object] | None = None,
    report_path: Path | None = None,
    root: Path | None = None,
) -> dict[str, object]:
    """Return stage-10 evidence without confusing captured and live checks.

    A supplied report is the preferred path after startup: it is validated
    against its embedded declaration and its captured pass is not invalidated
    merely because the runtime directories are now populated.  Supplying only
    the legacy declaration intentionally performs a live recheck for backwards
    compatibility.
    """
    if report is not None:
        try:
            captured = validate_isolated_preflight_report(
                report,
                expected_declaration=declaration,
                root=root,
                require_passed=True,
            )
            captured_path, captured_digest = _captured_preflight_report_source(
                report,
                report_path,
            )
        except ValueError as exc:
            return {
                "status": "拒绝",
                "mode": "captured_report",
                "checked_at": "",
                "issues": [str(exc)],
                "summary": {},
                "report_path": "",
                "report_sha512": "",
            }
        captured_declaration = captured["declaration"]
        payload = dict(captured_declaration)
        summary = _isolated_preflight_summary(payload)
        return {
            "status": "通过",
            "mode": "captured_report",
            "checked_at": captured["checked_at"],
            "issues": [],
            "summary": summary,
            "report_path": captured_path,
            "report_sha512": captured_digest,
        }
    if report_path is not None:
        return {
            "status": "拒绝",
            "mode": "captured_report",
            "checked_at": "",
            "issues": ["preflight report path was provided without a report"],
            "summary": {},
            "report_path": "",
            "report_sha512": "",
        }
    if declaration is None:
        return {
            "status": "未提供", "mode": "none", "checked_at": "", "issues": [],
            "summary": {}, "report_path": "", "report_sha512": "",
        }
    base = project_root() if root is None else Path(root)
    payload = dict(declaration)
    issues = isolated_preflight_issues(payload, root=base)
    return {
        "status": "通过" if not issues else "失败",
        "mode": "live_recheck",
        "checked_at": "",
        "issues": issues,
        "summary": _isolated_preflight_summary(payload),
        "report_path": "",
        "report_sha512": "",
    }


def _captured_preflight_report_source(
    report: Mapping[str, object],
    report_path: Path | None,
) -> tuple[str, str]:
    if report_path is None:
        return "", ""
    raw_path = Path(report_path).expanduser()
    if raw_path.is_symlink():
        raise ValueError("preflight report path must not be a symlink")
    try:
        resolved = raw_path.resolve(strict=True)
        raw = resolved.read_bytes()
        loaded = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"preflight report source is unreadable: {exc}") from exc
    if not resolved.is_file() or not isinstance(loaded, dict):
        raise ValueError("preflight report source must be one JSON object")
    if loaded != dict(report):
        raise ValueError("preflight report source no longer matches the loaded report")
    declaration = report.get("declaration")
    if isinstance(declaration, Mapping):
        protected_roots: list[Path] = []
        for key in ("scrapeflow_state_dir", "alist_data_dir"):
            value = declaration.get(key)
            if isinstance(value, str) and value.strip():
                protected_roots.append(Path(value).expanduser().resolve(strict=False))
        manifest = declaration.get("offline_backup_manifest")
        if isinstance(manifest, str) and manifest.strip():
            protected_roots.append(Path(manifest).expanduser().resolve(strict=False).parent)
        for protected in protected_roots:
            try:
                resolved.relative_to(protected)
            except ValueError:
                continue
            raise ValueError("preflight report source is inside a protected runtime or backup root")
    return str(resolved), hashlib.sha512(raw).hexdigest()


def _isolated_preflight_summary(
    payload: Mapping[str, object],
    *,
    keys: tuple[str, ...] | None = None,
) -> dict[str, object]:
    summary_keys = keys or (
        "api_url",
        "alist_url",
        "scrapeflow_state_dir",
        "alist_data_dir",
        "media_root",
        "storage_label",
        "offline_backup_manifest",
        "media_recovery_point",
        "provider_workers",
        "start_paused",
        "intake_monitor",
        "automatic_audit",
        "audit_repair",
        "provider_auto_repair",
        "one_task_at_a_time",
        "old_backlog_restored",
        "bulk_retry",
        "bulk_cleanup",
    )
    return {key: payload.get(key, "") for key in summary_keys}


def _preflight_mode_label(mode: object) -> str:
    if mode == "captured_report":
        return "captured_report（已固化，不重跑启动前空目录检查）"
    if mode == "live_recheck":
        return "live_recheck（兼容模式，重跑当前目录检查）"
    if mode == "none":
        return "未提供"
    return str(mode or "unknown")


def runtime_readiness_evidence(report: Mapping[str, object] | None) -> dict[str, object]:
    """Return status and key startup facts from a runtime readiness report."""
    if report is None:
        return {"status": "未提供", "issues": [], "summary": {}}
    payload = dict(report)
    issues = payload.get("issues")
    normalized_issues = [
        str(issue)
        for issue in issues
        if isinstance(issue, str) and issue.strip()
    ] if isinstance(issues, list) else []
    for issue in runtime_readiness_evidence_issues(payload):
        if issue not in normalized_issues:
            normalized_issues.append(issue)
    health = payload.get("health")
    control = payload.get("control")
    health_map = dict(health) if isinstance(health, Mapping) else {}
    control_map = dict(control) if isinstance(control, Mapping) else {}
    gates = health_map.get("lane_gates")
    gate_map = dict(gates) if isinstance(gates, Mapping) else {}
    operations = health_map.get("operations")
    operation_map = dict(operations) if isinstance(operations, Mapping) else {}
    intake = health_map.get("intake")
    intake_map = dict(intake) if isinstance(intake, Mapping) else {}
    lanes = health_map.get("provider_capabilities")
    lane_names = sorted(key for key in lanes if isinstance(key, str)) if isinstance(lanes, Mapping) else []
    helpers = health_map.get("helper_readiness")
    helper_map = dict(helpers) if isinstance(helpers, Mapping) else {}
    quark_helper = helper_map.get("quark")
    quark_helper_map = (
        dict(quark_helper) if isinstance(quark_helper, Mapping) else {}
    )
    helper_actions = quark_helper_map.get("actions")
    helper_action_names = [
        action.strip()
        for action in helper_actions
        if isinstance(action, str) and action.strip()
    ] if isinstance(helper_actions, list) else []
    offline = payload.get("alist_offline")
    offline_map = dict(offline) if isinstance(offline, Mapping) else {}
    return {
        "status": (
            "失败"
            if normalized_issues or payload.get("status") != "通过"
            else "通过"
        ),
        "issues": normalized_issues,
        "summary": {
            "api_url": payload.get("api_url", ""),
            "expected_commit": payload.get("expected_commit", ""),
            "build_version": health_map.get("build_version", ""),
            "build_commit": health_map.get("build_commit", ""),
            "build_time": health_map.get("build_time", ""),
            "alist_offline_status": offline_map.get("status", ""),
            "alist_offline_verified": offline_map.get("verified", ""),
            "alist_offline_checked_at": offline_map.get("checked_at", ""),
            "connected": health_map.get("connected", ""),
            "tmdb_configured": health_map.get("tmdb_configured", ""),
            "engine_configured": health_map.get("engine_configured", ""),
            "control_paused": control_map.get("paused", ""),
            "scheduler_paused": control_map.get("scheduler_paused", ""),
            "provider_gate": gate_map.get("provider_auto_repair_enabled", ""),
            "audit_gate": gate_map.get("audit_auto_repair_enabled", ""),
            "intake_enabled": intake_map.get("enabled", ""),
            "jobs_total": operation_map.get("jobs_total", ""),
            "jobs_active": operation_map.get("jobs_active", ""),
            "formal_write_workers": operation_map.get("formal_write_workers", ""),
            "provider_workers": operation_map.get("provider_workers", ""),
            "provider_active": operation_map.get("provider_active", ""),
            "audit_running": operation_map.get("audit_running", ""),
            "provider_lanes": ", ".join(lane_names),
            "quark_helper_status": quark_helper_map.get("status", ""),
            "quark_helper_configured": quark_helper_map.get("configured", ""),
            "quark_helper_reachable": quark_helper_map.get("reachable", ""),
            "quark_helper_authenticated": quark_helper_map.get("authenticated", ""),
            "quark_helper_actions": ", ".join(helper_action_names),
        },
    }


def _release_evidence_issues(
    payload: Mapping[str, object],
    *,
    evidence_path: Path | None,
    root: Path | None,
    runner: CommandRunner,
) -> list[str]:
    """Reject a success claim that is not bound to a current raw gate artifact."""
    issues: list[str] = []
    returncode = payload.get("returncode")
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        issues.append("returncode must be an integer")
    command = payload.get("command")
    if command != ["python3", "scripts/scrapeflow_release_check.py"]:
        issues.append("command must run the full release gate without --skip-docker")
    if payload.get("include_docker") is not True:
        issues.append("include_docker must be true")

    commit = payload.get("commit")
    if not isinstance(commit, str) or _FULL_GIT_COMMIT_RE.fullmatch(commit) is None:
        issues.append("commit must be a full lowercase Git object ID")
    if payload.get("worktree_clean_before_gate") is not True:
        issues.append("worktree_clean_before_gate must be true")
    raw_log_sha512 = payload.get("raw_log_sha512")
    if (
        not isinstance(raw_log_sha512, str)
        or _SHA512_RE.fullmatch(raw_log_sha512) is None
    ):
        issues.append("raw_log_sha512 must be a lowercase SHA-512 digest")

    # The package is evaluated against its current checkout, so a passing
    # report from an earlier candidate cannot be presented as release evidence
    # for the current one.  Callers without a checkout can still inspect an
    # artifact, but the package builder always supplies one.
    if root is not None:
        current_commit = _git_value(root, runner, ("rev-parse", "HEAD"))
        if _FULL_GIT_COMMIT_RE.fullmatch(current_commit) is None:
            issues.append("current Git HEAD cannot be resolved as a full commit")
        elif isinstance(commit, str) and commit != current_commit:
            issues.append("evidence commit does not match current Git HEAD")

    parsed_times: list[datetime] = []
    for key in ("started_at", "finished_at"):
        value = payload.get(key)
        if not isinstance(value, str) or not value:
            issues.append(f"{key} must be a non-empty ISO-8601 timestamp")
            continue
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            issues.append(f"{key} must be a valid ISO-8601 timestamp")
            continue
        if parsed.tzinfo is None:
            issues.append(f"{key} must include a timezone")
            continue
        parsed_times.append(parsed)
    if len(parsed_times) == 2 and parsed_times[1] < parsed_times[0]:
        issues.append("finished_at must not precede started_at")

    for key in ("log_path", "report_path"):
        value = payload.get(key)
        if not isinstance(value, str) or not value:
            issues.append(f"{key} must be a non-empty path")

    if evidence_path is not None:
        expected_report = evidence_path.expanduser().resolve()
        reported_path = payload.get("report_path")
        if not isinstance(reported_path, str) or Path(reported_path).expanduser().resolve() != expected_report:
            issues.append("report_path does not bind this evidence JSON")
        log_value = payload.get("log_path")
        if isinstance(log_value, str) and log_value:
            log_path = Path(log_value).expanduser().resolve()
            try:
                if not log_path.is_file() or log_path.stat().st_size == 0:
                    issues.append("raw release log is missing or empty")
                else:
                    raw_log = log_path.read_bytes()
                    actual_digest = hashlib.sha512(raw_log).hexdigest()
                    if not isinstance(raw_log_sha512, str) or raw_log_sha512 != actual_digest:
                        issues.append("raw release log does not match raw_log_sha512")
                    try:
                        raw_log_text = raw_log.decode("utf-8")
                    except UnicodeDecodeError:
                        issues.append("raw release log is not UTF-8 text")
                    else:
                        if "$ git status --porcelain" not in raw_log_text:
                            issues.append("raw release log does not show the worktree gate")
            except OSError:
                issues.append("raw release log cannot be read")
    return issues


def release_evidence_summary(
    report: Mapping[str, object] | None,
    *,
    evidence_path: Path | None = None,
    root: Path | None = None,
    runner: CommandRunner = _run_command,
) -> dict[str, object]:
    """Return only a verifiable full-gate release evidence summary.

    A hand-written ``returncode: 0`` is not acceptance evidence: a passing
    record must name the full command, include Docker, bind a full current Git
    commit and clean pre-gate worktree, and (when supplied via the CLI) bind
    the exact raw log by SHA-512.
    """
    if report is None:
        return {"status": "未提供", "summary": {}, "issues": []}
    payload = dict(report)
    returncode = payload.get("returncode")
    if isinstance(returncode, bool):
        returncode = None
    issues = _release_evidence_issues(
        payload,
        evidence_path=evidence_path,
        root=root,
        runner=runner,
    )
    if isinstance(returncode, int) and returncode != 0:
        status = "失败"
    elif isinstance(returncode, int) and returncode == 0 and not issues:
        status = "通过"
    else:
        status = "无效"
    command = payload.get("command")
    command_text = ""
    if isinstance(command, list):
        command_text = " ".join(str(part) for part in command)
    return {
        "status": status,
        "issues": issues,
        "summary": {
            "command": command_text,
            "returncode": "" if returncode is None else returncode,
            "include_docker": payload.get("include_docker", ""),
            "commit": payload.get("commit", ""),
            "worktree_clean_before_gate": payload.get(
                "worktree_clean_before_gate", "",
            ),
            "started_at": payload.get("started_at", ""),
            "finished_at": payload.get("finished_at", ""),
            "log_path": payload.get("log_path", ""),
            "raw_log_sha512": payload.get("raw_log_sha512", ""),
            "report_path": payload.get("report_path", ""),
        },
    }


def _sample_table(rows: tuple[tuple[str, str], ...], input_header: str) -> list[str]:
    output = [
        f"| 样本 | {input_header} | 预期 | 结果 | 证据 |",
        "| --- | --- | --- | --- | --- |",
    ]
    output.extend(f"| {name} |  | {expected} | 未执行 |  |" for name, expected in rows)
    return output


def build_acceptance_package(
    *,
    root: Path | None = None,
    generated_at: datetime | None = None,
    release_check_status: str = "未执行",
    backup_manifest: str = "",
    media_recovery_point: str = "",
    release_evidence: Mapping[str, object] | None = None,
    release_evidence_path: Path | None = None,
    isolated_declaration: Mapping[str, object] | None = None,
    isolated_preflight_report: Mapping[str, object] | None = None,
    isolated_preflight_report_path: Path | None = None,
    runtime_readiness: Mapping[str, object] | None = None,
    runner: CommandRunner = _run_command,
) -> str:
    """Build a Markdown evidence draft for final isolated acceptance."""
    base = project_root() if root is None else Path(root)
    stamp = generated_at or datetime.now(UTC)
    git = git_evidence(base, runner)
    compose = compose_evidence(base, runner)
    deployment_issues = local_deployment_contract_issues(base)
    static_status = "通过" if not deployment_issues else "失败"
    release = release_evidence_summary(
        release_evidence,
        evidence_path=release_evidence_path,
        root=base,
        runner=runner,
    )
    if release["status"] != "未提供":
        release_check_status = str(release["status"])
    preflight = isolated_preflight_evidence(
        isolated_declaration,
        report=isolated_preflight_report,
        report_path=isolated_preflight_report_path,
        root=base,
    )
    readiness = runtime_readiness_evidence(runtime_readiness)
    preflight_summary = preflight["summary"]
    preflight_mode = _preflight_mode_label(preflight.get("mode"))
    if isinstance(preflight_summary, Mapping):
        captured_backup = str(preflight_summary.get("offline_backup_manifest") or "")
        captured_media = str(preflight_summary.get("media_recovery_point") or "")
        if isolated_preflight_report is not None:
            if backup_manifest and captured_backup and backup_manifest != captured_backup:
                raise ValueError("backup manifest conflicts with the captured preflight report")
            if media_recovery_point and captured_media and media_recovery_point != captured_media:
                raise ValueError("media recovery point conflicts with the captured preflight report")
            backup_manifest = captured_backup
            media_recovery_point = captured_media
        else:
            backup_manifest = backup_manifest or captured_backup
            media_recovery_point = media_recovery_point or captured_media

    lines = [
        "# ScrapeFlow 验收包草稿",
        "",
        "状态: 未完成，等待真实隔离验收和用户授权。",
        "",
        "本文件由本机仓库状态生成；它不会连接真实 AList、不会读取正式媒体库、不会把未执行样本标记为通过。",
        "",
        "## 本机证据",
        "",
        f"- 生成时间: {stamp.isoformat()}",
        f"- Git branch: {git['branch']}",
        f"- Git commit: {git['commit']}",
        f"- 工作树: {'干净' if git['worktree_clean'] else '有未提交改动'}",
        f"- Git status: {git['status']}",
        f"- 发布检查: {release_check_status}",
        f"- 发布证据: {release['status']}",
        f"- 静态部署合同: {static_status}",
        f"- 隔离 preflight: {preflight['status']}",
        f"- Preflight 证据模式: {preflight_mode}",
        f"- Preflight 固化时间: {preflight.get('checked_at') or '未提供'}",
        f"- Preflight 报告路径: {preflight.get('report_path') or '未提供'}",
        f"- Preflight 报告 SHA-512: {preflight.get('report_sha512') or '未提供'}",
        f"- Runtime readiness: {readiness['status']}",
        f"- 离线备份 manifest: {backup_manifest or '未提供'}",
        f"- 正式媒体库外部恢复点: {media_recovery_point or '未提供'}",
        "",
    ]
    if deployment_issues:
        lines.extend(["静态部署合同问题:", ""])
        lines.extend(f"- {issue}" for issue in deployment_issues)
        lines.append("")

    lines.extend(["## 发布检查证据", ""])
    release_summary = release["summary"]
    if release["status"] == "未提供":
        lines.extend([
            "- 状态: 未提供",
            "- 生成命令: `python3 scripts/scrapeflow_release_evidence.py --output-dir artifacts/release`",
            "",
        ])
    elif isinstance(release_summary, Mapping):
        lines.extend([
            f"- 状态: {release['status']}",
            "",
            "| 字段 | 值 |",
            "| --- | --- |",
        ])
        for key, value in release_summary.items():
            lines.append(f"| {key} | {value} |")
        lines.append("")
        release_issues = release.get("issues")
        if isinstance(release_issues, list) and release_issues:
            lines.append("发布证据问题:")
            lines.append("")
            lines.extend(f"- {issue}" for issue in release_issues)
            lines.append("")

    lines.extend(["## Compose 服务", ""])
    if compose["status"] != "可读":
        lines.extend([f"- 状态: {compose['status']}", f"- 原因: {compose['error']}", ""])
    else:
        lines.extend([
            "| 服务 | 镜像 | 端口 | paused | intake | audit | provider | workers |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ])
        for service in compose["services"]:
            ports = ", ".join(service["ports"]) or ""
            lines.append(
                f"| {service['name']} | {service['image']} | {ports} | "
                f"{service['start_paused'] or ''} | {service['intake_monitor'] or ''} | "
                f"{service['automatic_audit'] or ''} | {service['provider_gate'] or ''} | "
                f"{service['provider_workers'] or ''} |"
            )
        lines.append("")

    lines.extend(["## 隔离环境声明", ""])
    if preflight["status"] == "未提供":
        lines.extend([
            "- 状态: 未提供",
            "- 生成模板: `python3 scripts/scrapeflow_isolated_preflight.py --template`",
            "- 首次 live 检查并固化: "
            "`python3 scripts/scrapeflow_isolated_preflight.py declaration.json "
            "--report preflight-report.json`",
            "- 兼容 live 重检: `python3 scripts/scrapeflow_isolated_preflight.py declaration.json`",
            "",
        ])
    elif isinstance(preflight_summary, Mapping):
        lines.extend([
            f"- 状态: {preflight['status']}",
            f"- 证据模式: {preflight_mode}",
            f"- 固化时间: {preflight.get('checked_at') or '未提供'}",
            f"- 报告路径: {preflight.get('report_path') or '未提供'}",
            f"- 报告 SHA-512: {preflight.get('report_sha512') or '未提供'}",
            "",
            "| 字段 | 值 |",
            "| --- | --- |",
        ])
        for key, value in preflight_summary.items():
            lines.append(f"| {key} | {value} |")
        lines.append("")
        issues = preflight["issues"]
        if isinstance(issues, list) and issues:
            lines.extend(["preflight 问题:", ""])
            lines.extend(f"- {issue}" for issue in issues)
            lines.append("")

    lines.extend(["## Runtime Readiness", ""])
    readiness_summary = readiness["summary"]
    if readiness["status"] == "未提供":
        lines.extend([
            "- 状态: 未提供",
            "- 生成命令: `python3 scripts/scrapeflow_runtime_readiness.py --expected-commit <build-id> --json > readiness.json`",
            "",
        ])
    elif isinstance(readiness_summary, Mapping):
        lines.extend([
            f"- 状态: {readiness['status']}",
            "",
            "| 字段 | 值 |",
            "| --- | --- |",
        ])
        for key, value in readiness_summary.items():
            lines.append(f"| {key} | {value} |")
        lines.append("")
        readiness_issues = readiness["issues"]
        if isinstance(readiness_issues, list) and readiness_issues:
            lines.extend(["readiness 问题:", ""])
            lines.extend(f"- {issue}" for issue in readiness_issues)
            lines.append("")

    lines.extend([
        "## 预检查",
        "",
        f"- [ ] 发布检查通过，命令: `python3 scripts/scrapeflow_release_evidence.py --output-dir artifacts/release`，当前记录: {release_check_status}",
        f"- [ ] 发布检查原始日志已归档，当前记录: {release['status']}",
        "- [ ] 隔离 preflight 通过，首次命令: "
        "`python3 scripts/scrapeflow_isolated_preflight.py declaration.json "
        f"--report preflight-report.json`，当前记录: {preflight['status']} "
        f"({preflight_mode})",
        f"- [ ] Runtime readiness 通过，命令: `python3 scripts/scrapeflow_runtime_readiness.py --expected-commit <build-id> --json > readiness.json`，当前记录: {readiness['status']}",
        "- [ ] 离线备份 `verify` 通过。",
        "- [ ] 隔离恢复 `restore` 通过，恢复状态仍为 paused。",
        "- [ ] `/api/health` 显示预期 commit 或 build version。",
        "- [ ] `/api/control` 显示 paused。",
        "- [ ] 自动 gate 初始关闭。",
        "- [ ] 没有恢复旧 backlog。",
        "- [ ] 没有批量 retry。",
        "- [ ] 没有批量 cleanup。",
        "",
        "## 普通入库样本",
        "",
        *_sample_table(ORDINARY_SAMPLES, "输入"),
        "",
        "## 补源样本",
        "",
        *_sample_table(REPLENISHMENT_SAMPLES, "输入 gap"),
        "",
        "## 每个样本必须记录",
        "",
        "- AList 前后路径。",
        "- 对象类型。",
        "- 字节数。",
        "- 任务 ID。",
        "- provider lane。",
        "- attempt staging。",
        "- 内部 child ID。",
        "- 正式库回读结果。",
        "- 定向审计结果。",
        "- staging 成功清理或失败保留证据。",
        "",
        "## 最终判定",
        "",
    ])
    lines.extend(f"- [ ] {item}。" for item in FINAL_CHECKS)
    lines.append("")
    return "\n".join(lines)


__all__ = [
    "CommandResult",
    "build_acceptance_package",
    "compose_evidence",
    "git_evidence",
    "isolated_preflight_evidence",
    "release_evidence_summary",
    "runtime_readiness_evidence",
]
