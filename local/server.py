#!/usr/bin/env python3
"""Local-only HTTP bridge between the ScrapeFlow UI and the Python engine."""

from __future__ import annotations

import hashlib
import json
import os
import posixpath
import re
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENGINE_ROOT = PROJECT_ROOT / "engine"
SCRAPER = ENGINE_ROOT / "scraper.py"
ARCHIVE_TOOL = ENGINE_ROOT / "tools" / "extract_archives.py"
MAX_BODY_BYTES = 64 * 1024
MAX_LOG_LINES = 600
ALLOWED_ORIGIN_RE = re.compile(r"^https?://(?:127\.0\.0\.1|localhost)(?::\d+)?$")


def load_local_env() -> None:
    """Load only the supported local keys without adding a dependency."""
    path = PROJECT_ROOT / ".env.local"
    if not path.exists():
        return
    allowed = {
        "ALIST_URL", "ALIST_USERNAME", "ALIST_PASSWORD", "TMDB_API_KEY",
        "ARCHIVE_PASSWORD", "SCRAPEFLOW_API_PORT", "SCRAPEFLOW_STATE_DIR",
    }
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in allowed:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_local_env()

STATE_ROOT = Path(os.getenv("SCRAPEFLOW_STATE_DIR", PROJECT_ROOT / ".scrapeflow"))
JOBS_ROOT = STATE_ROOT / "jobs"
STATE_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
JOBS_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_digest(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def normalize_remote_input(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("请输入 AList 媒体路径")
    raw = value.strip()
    parsed = urlsplit(raw)
    if parsed.scheme:
        if parsed.scheme not in {"http", "https"} or not parsed.path:
            raise ValueError("AList 链接格式无效")
        raw = parsed.path
    raw = unquote(raw).replace("\\", "/")
    if not raw.startswith("/") or "\x00" in raw:
        raise ValueError("AList 路径必须以 / 开头")
    parts = [part for part in raw.split("/") if part]
    if not parts or any(part in {".", ".."} for part in parts):
        raise ValueError("AList 路径无效")
    normalized = "/" + "/".join(parts)
    if len(normalized) > 2048:
        raise ValueError("AList 路径过长")
    return normalized


def default_parent(source: str) -> str:
    parent = posixpath.dirname(source.rstrip("/"))
    if not parent or parent == "/":
        raise ValueError("源目录必须位于媒体库父目录下")
    return parent


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"计划文件不是 JSON 对象: {path}")
    return value


def summarize_archive_plan(plan: dict[str, Any]) -> dict[str, Any]:
    archives = plan.get("archives") or []
    return {
        "kind": "archive",
        "archive_count": len(archives),
        "part_count": sum(len(item.get("parts") or []) for item in archives),
        "video_count": sum(int(item.get("video_count") or 0) for item in archives),
        "items": [
            {
                "archive_path": item.get("archive_path"),
                "destination": item.get("dst_dir"),
                "parts": len(item.get("parts") or []),
                "videos": int(item.get("video_count") or 0),
                "password_source": item.get("password_source"),
            }
            for item in archives
        ],
    }


def summarize_media_plan(plan: dict[str, Any]) -> dict[str, Any]:
    files = plan.get("files") or []
    metadata = plan.get("metadata") or {}
    return {
        "kind": "media",
        "mode": plan.get("mode"),
        "source_root": plan.get("source_root"),
        "target_root": plan.get("target_root"),
        "title": metadata.get("title"),
        "year": metadata.get("year"),
        "season": metadata.get("season"),
        "tmdb_id": metadata.get("tmdb_id"),
        "file_count": len(files),
        "warnings": list(plan.get("warnings") or []),
        "files": [
            {
                "source": item.get("source_path"),
                "target": f"{item.get('target_dir', '')}/{item.get('final_name', '')}",
                "name": item.get("final_name"),
            }
            for item in files[:250]
        ],
        "truncated": len(files) > 250,
    }


@dataclass
class Job:
    id: str
    source: str
    parent: str
    media_type: str
    absolute: bool
    prefer_simplified: bool
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    phase: str = "queued"
    logs: list[str] = field(default_factory=list)
    error: str | None = None
    digest: str | None = None
    plan_summary: dict[str, Any] | None = None
    process: subprocess.Popen[str] | None = field(default=None, repr=False)
    cancel_requested: bool = False

    @property
    def directory(self) -> Path:
        return JOBS_ROOT / self.id

    def public(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "parent": self.parent,
            "media_type": self.media_type,
            "absolute": self.absolute,
            "prefer_simplified": self.prefer_simplified,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "phase": self.phase,
            "logs": list(self.logs),
            "error": self.error,
            "digest": self.digest,
            "plan": self.plan_summary,
        }


JOBS: dict[str, Job] = {}
LOCK = threading.RLock()


def redact(text: str) -> str:
    output = text.rstrip("\r\n")
    for name in ("ALIST_PASSWORD", "TMDB_API_KEY", "ARCHIVE_PASSWORD"):
        secret = os.getenv(name)
        if secret:
            output = output.replace(secret, "[REDACTED]")
    return output


def update_job(job: Job, **changes: Any) -> None:
    with LOCK:
        for key, value in changes.items():
            setattr(job, key, value)
        job.updated_at = utc_now()


def append_log(job: Job, line: str) -> None:
    safe = redact(line)
    if not safe:
        return
    with LOCK:
        job.logs.append(safe)
        if len(job.logs) > MAX_LOG_LINES:
            del job.logs[: len(job.logs) - MAX_LOG_LINES]
        job.updated_at = utc_now()


def command_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    return env


def common_connection_args() -> list[str]:
    return [
        "--alist-url",
        os.getenv("ALIST_URL", "http://127.0.0.1:5244"),
        "--username",
        os.getenv("ALIST_USERNAME", "admin"),
    ]


def run_command(job: Job, command: list[str]) -> tuple[int, str]:
    append_log(job, f"$ {' '.join(command[:2])} …")
    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=command_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    update_job(job, process=process)
    captured: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        captured.append(line)
        append_log(job, line)
    code = process.wait()
    update_job(job, process=None)
    return code, "".join(captured)


def fail_job(job: Job, message: str) -> None:
    append_log(job, f"错误: {message}")
    update_job(job, phase="failed", error=message, digest=None)


def legacy_archive_candidates(source: str) -> list[str]:
    """Read-only fallback for old AList versions that lack safe archive APIs."""
    sys.path.insert(0, str(ENGINE_ROOT))
    from scraper import AListClient  # pylint: disable=import-outside-toplevel

    password = os.getenv("ALIST_PASSWORD")
    if not password:
        raise ValueError("缺少 ALIST_PASSWORD")
    client = AListClient(
        os.getenv("ALIST_URL", "http://127.0.0.1:5244"),
        os.getenv("ALIST_USERNAME", "admin"),
        password,
        timeout=20,
        retries=1,
    )
    client.login()
    candidates: list[str] = []
    for item in client.walk(source):
        name = str(item.get("name") or "")
        if re.search(r"\.(?:7z|zip)\.001$", name, re.I) or re.search(r"\.part0*1\.rar$", name, re.I):
            candidates.append(str(item.get("full_path") or name))
    return candidates


def plan_media(job: Job) -> None:
    if job.cancel_requested:
        update_job(job, phase="cancelled")
        return
    update_job(job, phase="planning_media", error=None, digest=None, plan_summary=None)
    plan_path = job.directory / "media-plan.json"
    command = [
        sys.executable,
        str(SCRAPER),
        *common_connection_args(),
        "--parent",
        job.parent,
        "--type",
        job.media_type,
        "--plan-json",
        str(plan_path),
    ]
    if job.media_type != "auto":
        command.append("--auto-match")
    if job.absolute:
        command.append("--absolute")
    if job.prefer_simplified:
        command.append("--prefer-simplified")
    command.extend(["--", job.source])
    code, _ = run_command(job, command)
    if job.cancel_requested:
        update_job(job, phase="cancelled")
        return
    if code != 0:
        fail_job(job, "媒体识别计划生成失败，请查看实时日志")
        return
    try:
        plan = load_json(plan_path)
        digest = canonical_digest(plan)
        summary = summarize_media_plan(plan)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        fail_job(job, f"无法读取媒体计划: {exc}")
        return
    update_job(
        job,
        phase="awaiting_media_approval",
        digest=digest,
        plan_summary=summary,
    )


def prepare_job(job: Job) -> None:
    update_job(job, phase="planning_archives", error=None)
    archive_path = job.directory / "archive-plan.json"
    command = [
        sys.executable,
        str(ARCHIVE_TOOL),
        *common_connection_args(),
        "--plan-json",
        str(archive_path),
        job.source,
    ]
    code, output = run_command(job, command)
    if job.cancel_requested:
        update_job(job, phase="cancelled")
        return
    if code == 0:
        try:
            plan = load_json(archive_path)
            digest = canonical_digest(plan)
            summary = summarize_archive_plan(plan)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            fail_job(job, f"无法读取解压计划: {exc}")
            return
        update_job(
            job,
            phase="awaiting_archive_approval",
            digest=digest,
            plan_summary=summary,
        )
        return
    if "未找到可支持的首卷" in output:
        append_log(job, "未发现需要解压的分卷，继续识别媒体。")
        plan_media(job)
        return
    if "不满足安全解压要求" in output:
        try:
            candidates = legacy_archive_candidates(job.source)
        except Exception as exc:  # read-only compatibility scan
            fail_job(job, f"旧版 AList 兼容扫描失败: {exc}")
            return
        if not candidates:
            append_log(job, "AList 版本较旧，但目录未发现分卷；继续识别媒体。")
            plan_media(job)
            return
        fail_job(
            job,
            f"发现 {len(candidates)} 个分卷首卷，但当前 AList 不支持安全的服务器端解压；"
            "请先升级到 v3.57.0 或更高版本。",
        )
        return
    fail_job(job, "压缩包检查失败，请查看实时日志")


def execute_archive(job: Job, digest: str) -> None:
    update_job(job, phase="extracting_archives", error=None)
    command = [
        sys.executable,
        str(ARCHIVE_TOOL),
        *common_connection_args(),
        "--execute-plan",
        str(job.directory / "archive-plan.json"),
        "--approve-plan-sha256",
        digest,
        "--journal",
        str(job.directory / "archive-journal.json"),
        "--execute",
    ]
    code, _ = run_command(job, command)
    if job.cancel_requested:
        update_job(job, phase="cancelled")
    elif code != 0:
        fail_job(job, "解压执行失败，请查看实时日志")
    else:
        append_log(job, "解压完成，继续生成媒体整理计划。")
        plan_media(job)


def execute_media(job: Job, digest: str) -> None:
    update_job(job, phase="executing_media", error=None)
    command = [
        sys.executable,
        str(SCRAPER),
        *common_connection_args(),
        "--execute-plan",
        str(job.directory / "media-plan.json"),
        "--approve-plan-sha256",
        digest,
        "--journal",
        str(job.directory / "media-journal.json"),
        "--execute",
    ]
    code, _ = run_command(job, command)
    if job.cancel_requested:
        update_job(job, phase="cancelled")
    elif code != 0:
        fail_job(job, "媒体整理执行失败，请查看实时日志")
    else:
        update_job(job, phase="completed", error=None)
        append_log(job, "整理任务已完成并通过最终校验。")


def start_thread(target: Callable[..., None], *args: Any) -> None:
    threading.Thread(target=target, args=args, daemon=True).start()


def create_job(payload: dict[str, Any]) -> Job:
    source = normalize_remote_input(payload.get("path"))
    parent_value = payload.get("parent")
    parent = normalize_remote_input(parent_value) if parent_value else default_parent(source)
    media_type = payload.get("type", "auto")
    if media_type not in {"auto", "tv", "movie"}:
        raise ValueError("媒体类型必须是自动、电视剧或电影")
    absolute = payload.get("absolute", False)
    simplified = payload.get("prefer_simplified", True)
    if type(absolute) is not bool or type(simplified) is not bool:
        raise ValueError("任务选项格式无效")
    job = Job(
        id=uuid.uuid4().hex[:12],
        source=source,
        parent=parent,
        media_type=media_type,
        absolute=absolute,
        prefer_simplified=simplified,
    )
    job.directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    with LOCK:
        JOBS[job.id] = job
    start_thread(prepare_job, job)
    return job


def approve_job(job: Job, payload: dict[str, Any]) -> None:
    digest = payload.get("digest")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("批准摘要必须是完整的 64 位 SHA-256")
    with LOCK:
        if job.digest != digest:
            raise ValueError("批准摘要与当前计划不一致，请刷新后重试")
        if job.phase == "awaiting_archive_approval":
            job.phase = "starting_archive_execution"
            target = execute_archive
        elif job.phase == "awaiting_media_approval":
            job.phase = "starting_media_execution"
            target = execute_media
        else:
            raise ValueError("当前任务没有等待批准的计划")
        job.updated_at = utc_now()
    start_thread(target, job, digest)


def cancel_job(job: Job) -> None:
    with LOCK:
        if job.phase in {"completed", "failed", "cancelled"}:
            return
        job.cancel_requested = True
        process = job.process
        job.updated_at = utc_now()
    if process is not None and process.poll() is None:
        process.terminate()
    append_log(job, "用户请求取消任务。")


def health_payload() -> dict[str, Any]:
    alist_password = os.getenv("ALIST_PASSWORD")
    tmdb_key = os.getenv("TMDB_API_KEY")
    result: dict[str, Any] = {
        "local": True,
        "engine": "3.3.2",
        "alist_url": os.getenv("ALIST_URL", "http://127.0.0.1:5244"),
        "alist_configured": bool(alist_password),
        "tmdb_configured": bool(tmdb_key),
        "connected": False,
        "archive_supported": False,
    }
    if not alist_password:
        result["message"] = "缺少 ALIST_PASSWORD"
        return result
    try:
        sys.path.insert(0, str(ENGINE_ROOT))
        from scraper import AListClient  # pylint: disable=import-outside-toplevel

        client = AListClient(
            result["alist_url"],
            os.getenv("ALIST_USERNAME", "admin"),
            alist_password,
            timeout=5,
            retries=0,
        )
        client.login()
        version = client.server_version()
        result["alist_version"] = version
        match = re.search(r"(\d+)\.(\d+)\.(\d+)", version)
        result["archive_supported"] = bool(match and tuple(map(int, match.groups())) >= (3, 57, 0))
        result["connected"] = True
        result["message"] = (
            "本地引擎与 AList 已连接"
            if result["archive_supported"]
            else "AList 已连接，但版本过旧，不能安全地服务器端解压"
        )
    except Exception as exc:  # health endpoint must return diagnostic JSON
        result["message"] = f"AList 连接失败: {exc}"
    return result


class Handler(BaseHTTPRequestHandler):
    server_version = "ScrapeFlowLocal/1.0"

    def log_message(self, format_string: str, *args: Any) -> None:
        print(f"[local-api] {format_string % args}")

    def _origin(self) -> str | None:
        origin = self.headers.get("Origin")
        return origin if origin and ALLOWED_ORIGIN_RE.fullmatch(origin) else None

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        origin = self._origin()
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.end_headers()
        self.wfile.write(body)

    def _json_body(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("请求长度无效") from exc
        if length <= 0 or length > MAX_BODY_BYTES:
            raise ValueError("请求内容为空或过大")
        value = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("请求必须是 JSON 对象")
        return value

    def do_OPTIONS(self) -> None:  # noqa: N802
        if not self._origin():
            self._send(HTTPStatus.FORBIDDEN, {"error": "仅允许本地页面访问"})
            return
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", self._origin() or "")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path == "/api/health":
            self._send(HTTPStatus.OK, health_payload())
            return
        if path == "/api/jobs":
            with LOCK:
                jobs = [job.public() for job in sorted(JOBS.values(), key=lambda row: row.created_at, reverse=True)]
            self._send(HTTPStatus.OK, {"jobs": jobs})
            return
        match = re.fullmatch(r"/api/jobs/([0-9a-f]{12})", path)
        if match:
            with LOCK:
                job = JOBS.get(match.group(1))
                payload = job.public() if job else None
            if payload is None:
                self._send(HTTPStatus.NOT_FOUND, {"error": "任务不存在"})
            else:
                self._send(HTTPStatus.OK, {"job": payload})
            return
        self._send(HTTPStatus.NOT_FOUND, {"error": "接口不存在"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        try:
            payload = self._json_body()
            if path == "/api/jobs":
                job = create_job(payload)
                self._send(HTTPStatus.ACCEPTED, {"job": job.public()})
                return
            match = re.fullmatch(r"/api/jobs/([0-9a-f]{12})/(approve|cancel)", path)
            if not match:
                self._send(HTTPStatus.NOT_FOUND, {"error": "接口不存在"})
                return
            with LOCK:
                job = JOBS.get(match.group(1))
            if job is None:
                self._send(HTTPStatus.NOT_FOUND, {"error": "任务不存在"})
                return
            if match.group(2) == "approve":
                approve_job(job, payload)
            else:
                cancel_job(job)
            self._send(HTTPStatus.ACCEPTED, {"job": job.public()})
        except (ValueError, json.JSONDecodeError) as exc:
            self._send(HTTPStatus.BAD_REQUEST, {"error": str(exc)})


def main() -> int:
    host = "127.0.0.1"
    port = int(os.getenv("SCRAPEFLOW_API_PORT", "8765"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"ScrapeFlow 本地 API: http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
