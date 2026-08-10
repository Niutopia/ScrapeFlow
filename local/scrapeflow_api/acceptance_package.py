"""Generate a local acceptance-package draft without touching runtime data."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess

from .isolated_preflight import isolated_preflight_issues
from .release_checks import local_deployment_contract_issues, project_root


ORDINARY_SAMPLES = (
    ("电影", "选择 movie 后入库，回读正确"),
    ("番剧归档", "归档预处理后入库，回读正确"),
    ("美剧季度目录", "选择 us_tv 后入库，回读正确"),
    ("错误密码", "停在归档错误，source 保留"),
    ("正式目标冲突", "停止，不覆盖"),
    ("cancel", "停止，source/staging 保留在本任务范围"),
    ("执行中重启", "重启后不重复写入"),
)
REPLENISHMENT_SAMPLES = (
    ("有效夸克分享", "第一阶完成，后二阶未调用"),
    ("无分享、有效 magnet", "夸克离线完成，本地 Torrent 未调用"),
    ("前两阶完整排除后 Torrent", "本地 Torrent 完成"),
    ("Helper 断线", "停在当前云阶，不降阶"),
    ("Quark submit 后 API 重启", "继续同一外部 task"),
    ("错误候选", "不创建 Engine child"),
    ("staging 内容不符", "不进入正式库"),
    ("缺字幕", "只安装正确目标语言侧车"),
)
FINAL_CHECKS = (
    "普通电影、番剧、美剧均正确",
    "归档和错误密码行为正确",
    "手工审计不修改正式库",
    "三条获取线路全部真实可执行",
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
    declaration: Mapping[str, object] | None,
    *,
    root: Path | None = None,
) -> dict[str, object]:
    """Return status and safe summary fields from a stage-10 declaration."""
    if declaration is None:
        return {"status": "未提供", "issues": [], "summary": {}}
    base = project_root() if root is None else Path(root)
    payload = dict(declaration)
    issues = isolated_preflight_issues(payload, root=base)
    summary_keys = (
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
        "provider_auto_repair",
        "one_task_at_a_time",
        "old_backlog_restored",
        "bulk_retry",
        "bulk_cleanup",
    )
    return {
        "status": "通过" if not issues else "失败",
        "issues": issues,
        "summary": {
            key: payload.get(key, "")
            for key in summary_keys
        },
    }


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
    return {
        "status": str(payload.get("status") or ("失败" if normalized_issues else "通过")),
        "issues": normalized_issues,
        "summary": {
            "api_url": payload.get("api_url", ""),
            "expected_commit": payload.get("expected_commit", ""),
            "build_commit": health_map.get("build_commit", ""),
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
    isolated_declaration: Mapping[str, object] | None = None,
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
    preflight = isolated_preflight_evidence(isolated_declaration, root=base)
    readiness = runtime_readiness_evidence(runtime_readiness)
    preflight_summary = preflight["summary"]
    if isinstance(preflight_summary, Mapping):
        backup_manifest = backup_manifest or str(preflight_summary.get("offline_backup_manifest") or "")
        media_recovery_point = (
            media_recovery_point or str(preflight_summary.get("media_recovery_point") or "")
        )

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
        f"- 静态部署合同: {static_status}",
        f"- 隔离 preflight: {preflight['status']}",
        f"- Runtime readiness: {readiness['status']}",
        f"- 离线备份 manifest: {backup_manifest or '未提供'}",
        f"- 正式媒体库外部恢复点: {media_recovery_point or '未提供'}",
        "",
    ]
    if deployment_issues:
        lines.extend(["静态部署合同问题:", ""])
        lines.extend(f"- {issue}" for issue in deployment_issues)
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
            "- 校验命令: `python3 scripts/scrapeflow_isolated_preflight.py declaration.json`",
            "",
        ])
    elif isinstance(preflight_summary, Mapping):
        lines.extend([
            f"- 状态: {preflight['status']}",
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
            "- 生成命令: `python3 scripts/scrapeflow_runtime_readiness.py --json > readiness.json`",
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
        f"- [ ] 发布检查通过，命令: `python3 scripts/scrapeflow_release_check.py`，当前记录: {release_check_status}",
        f"- [ ] 隔离 preflight 通过，命令: `python3 scripts/scrapeflow_isolated_preflight.py declaration.json`，当前记录: {preflight['status']}",
        f"- [ ] Runtime readiness 通过，命令: `python3 scripts/scrapeflow_runtime_readiness.py --json > readiness.json`，当前记录: {readiness['status']}",
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
    "runtime_readiness_evidence",
]
