"""Runtime configuration for the local API and engine subprocesses."""

from __future__ import annotations

import os
import shlex
from pathlib import Path
from urllib.parse import urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENGINE_ROOT = PROJECT_ROOT / "engine"
SCRAPER = ENGINE_ROOT / "scraper.py"
ARCHIVE_TOOL = ENGINE_ROOT / "tools" / "extract_archives.py"


LOCAL_ENV_ALLOWED_KEYS = frozenset({
    "ALIST_URL", "ALIST_USERNAME", "ALIST_PASSWORD", "TMDB_API_KEY",
    "ARCHIVE_PASSWORD", "SCRAPEFLOW_API_HOST", "SCRAPEFLOW_API_PORT",
    "SCRAPEFLOW_STATE_DIR", "SCRAPEFLOW_DOCKER", "TMDB_LANGUAGE",
    "TMDB_BASE_URL", "TMDB_BASE_URL_DOCKER", "TMDB_IMAGE_BASE_URL",
    "TMDB_PROXY_URL", "TMDB_PROXY_URL_DOCKER", "SCRAPEFLOW_ANALYSIS_WORKERS",
    "SCRAPEFLOW_EXECUTION_WORKERS",
    "SCRAPEFLOW_AUTO_EXECUTE_MEDIA",
    "SCRAPEFLOW_AUTO_REPLENISH_MISSING",
    "SCRAPEFLOW_REPLENISHMENT_ADAPTER",
    "SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS",
    "SCRAPEFLOW_REPLENISHMENT_MIN_CLOUD_ATTEMPTS",
    "SCRAPEFLOW_REPLENISHMENT_RETRY_DELAY",
    "SCRAPEFLOW_REPLENISHMENT_SEARCH_URL",
    "SCRAPEFLOW_REPLENISHMENT_ACQUIRE_URL",
    "SCRAPEFLOW_REPLENISHMENT_STATUS_URL",
    "SCRAPEFLOW_REPLENISHMENT_ACQUIRE_TIMEOUT",
    "SCRAPEFLOW_REPLENISHMENT_POLL_INTERVAL",
    "SCRAPEFLOW_REPLENISHMENT_TOKEN",
    "SCRAPEFLOW_REPLENISHMENT_CATALOG",
    "SCRAPEFLOW_REPLENISHMENT_DOWNLOAD_DIR",
    "SCRAPEFLOW_REPLENISHMENT_UNSCRAPED_ROOT",
    "SCRAPEFLOW_REPLENISHMENT_ARCHIVE_STAGING_ROOT",
    "SCRAPEFLOW_REPLENISHMENT_BT_IDLE_TIMEOUT",
    "SCRAPEFLOW_REPLENISHMENT_TORRENT_TIMEOUT",
    "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH",
    "SCRAPEFLOW_REPLENISHMENT_ACG_PROXY",
    "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX",
    "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL",
    "SCRAPEFLOW_REPLENISHMENT_PANSOU_MAX_SHARES",
    "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH",
    "SCRAPEFLOW_REPLENISHMENT_SUBSPLEASE_SEARCH",
    "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH",
    "SCRAPEFLOW_REPLENISHMENT_SEARCH_WORKERS",
    "SCRAPEFLOW_REPLENISHMENT_SHARE_SEARCH_TIMEOUT",
    "SCRAPEFLOW_REPLENISHMENT_QUARK_OFFLINE",
    "SCRAPEFLOW_QUARK_HELPER_URL",
    "SCRAPEFLOW_QUARK_HELPER_TOKEN",
    "SCRAPEFLOW_QUARK_HELPER_TIMEOUT",
    "SCRAPEFLOW_QUARK_OFFLINE_PROGRESS_POLLS",
    "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_ATTEMPTS",
    "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_DELAY",
    "SCRAPEFLOW_REPLENISHMENT_OFFLINE_COOLDOWN",
    "SCRAPEFLOW_REPLENISHMENT_OFFLINE_TIMEOUT",
    "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT",
    "SCRAPEFLOW_REPLENISHMENT_ARRIVAL_TIMEOUT",
    "SCRAPEFLOW_SOURCE_REVIEW_INTERVAL",
})


def load_local_env(path: Path | None = None) -> None:
    """Load supported local keys without introducing a dotenv dependency."""
    if path is None:
        path = PROJECT_ROOT / ".env.local"
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in LOCAL_ENV_ALLOWED_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def apply_docker_env_overrides() -> None:
    """Apply explicit container endpoints after Compose/env-file merging.

    Compose interpolation does not read values from a service ``env_file``.
    The compose-level fallback can therefore populate ``TMDB_BASE_URL`` even
    when the same env file carries a deliberate ``TMDB_BASE_URL_DOCKER``.
    Prefer only non-empty Docker-specific values and only inside the container;
    local CLI runs retain their ordinary endpoint and proxy settings.
    """
    if os.getenv("SCRAPEFLOW_DOCKER") != "1":
        return
    for runtime_key, docker_key in (
        ("TMDB_BASE_URL", "TMDB_BASE_URL_DOCKER"),
        ("TMDB_PROXY_URL", "TMDB_PROXY_URL_DOCKER"),
    ):
        value = os.getenv(docker_key, "").strip()
        if value:
            os.environ[runtime_key] = value


if os.getenv("SCRAPEFLOW_IGNORE_LOCAL_ENV") != "1":
    load_local_env()
apply_docker_env_overrides()

STATE_ROOT = Path(os.getenv("SCRAPEFLOW_STATE_DIR", PROJECT_ROOT / ".scrapeflow"))
JOBS_ROOT = STATE_ROOT / "jobs"
STATE_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
JOBS_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)


def command_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    return env


def analysis_worker_count() -> int:
    """Return the bounded concurrency for read-only planning work."""
    raw = os.getenv("SCRAPEFLOW_ANALYSIS_WORKERS", "4").strip()
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError("SCRAPEFLOW_ANALYSIS_WORKERS 必须是 1–8 之间的整数")
    value = int(raw)
    if not 1 <= value <= 8:
        raise ValueError("SCRAPEFLOW_ANALYSIS_WORKERS 必须是 1–8 之间的整数")
    return value


def execution_worker_count() -> int:
    """Return bounded concurrency for non-overlapping remote mutations."""
    raw = os.getenv("SCRAPEFLOW_EXECUTION_WORKERS", "1").strip()
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError("SCRAPEFLOW_EXECUTION_WORKERS 必须是 1–4 之间的整数")
    value = int(raw)
    if not 1 <= value <= 4:
        raise ValueError("SCRAPEFLOW_EXECUTION_WORKERS 必须是 1–4 之间的整数")
    return value


def auto_execute_media_enabled() -> bool:
    """Return whether validated media plans should enter execution directly."""
    raw = os.getenv("SCRAPEFLOW_AUTO_EXECUTE_MEDIA", "1").strip().casefold()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(
        "SCRAPEFLOW_AUTO_EXECUTE_MEDIA 必须是 1/0、true/false、yes/no 或 on/off"
    )


def auto_replenish_missing_enabled() -> bool:
    """Return whether regular gaps should enter the post-scrape adapter."""
    raw = os.getenv("SCRAPEFLOW_AUTO_REPLENISH_MISSING", "1").strip().casefold()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(
        "SCRAPEFLOW_AUTO_REPLENISH_MISSING 必须是 1/0、true/false、yes/no 或 on/off"
    )


def replenishment_adapter_command() -> list[str]:
    """Return the configured provider adapter argv without shell expansion."""
    raw = os.getenv("SCRAPEFLOW_REPLENISHMENT_ADAPTER", "").strip()
    if not raw:
        return []
    command = shlex.split(raw)
    if not command:
        raise ValueError("SCRAPEFLOW_REPLENISHMENT_ADAPTER 命令为空")
    return command


def replenishment_max_rounds() -> int:
    """Bound automatic acquire/scrape/audit loops; zero keeps retrying."""
    raw = os.getenv("SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS", "0").strip()
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError("SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS 必须是 0–1000000 之间的整数")
    value = int(raw)
    if not 0 <= value <= 1_000_000:
        raise ValueError("SCRAPEFLOW_REPLENISHMENT_MAX_ROUNDS 必须是 0–1000000 之间的整数")
    return value


def replenishment_min_cloud_attempts() -> int:
    """Return the per-lane resource-attempt floor before local fallback."""
    raw = os.getenv("SCRAPEFLOW_REPLENISHMENT_MIN_CLOUD_ATTEMPTS", "30").strip()
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError(
            "SCRAPEFLOW_REPLENISHMENT_MIN_CLOUD_ATTEMPTS 必须是 30–1000 之间的整数"
        )
    value = int(raw)
    if not 30 <= value <= 1000:
        raise ValueError(
            "SCRAPEFLOW_REPLENISHMENT_MIN_CLOUD_ATTEMPTS 必须是 30–1000 之间的整数"
        )
    return value


def replenishment_retry_delay() -> int:
    """Return a bounded base delay for unattended replenishment retries."""
    raw = os.getenv("SCRAPEFLOW_REPLENISHMENT_RETRY_DELAY", "30").strip()
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError("SCRAPEFLOW_REPLENISHMENT_RETRY_DELAY 必须是 1–3600 之间的整数")
    value = int(raw)
    if not 1 <= value <= 3600:
        raise ValueError("SCRAPEFLOW_REPLENISHMENT_RETRY_DELAY 必须是 1–3600 之间的整数")
    return value


def alist_url() -> str:
    value = os.getenv("ALIST_URL", "http://127.0.0.1:5244")
    if os.getenv("SCRAPEFLOW_DOCKER") != "1":
        return value
    parsed = urlsplit(value)
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        return value
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://host.docker.internal{port}{parsed.path or ''}"


def docker_loopback_bridge() -> bool:
    if os.getenv("SCRAPEFLOW_DOCKER") != "1":
        return False
    parsed = urlsplit(os.getenv("ALIST_URL", "http://127.0.0.1:5244"))
    if parsed.scheme != "http":
        return False
    if parsed.hostname in {"127.0.0.1", "localhost"}:
        return True
    return os.getenv("SCRAPEFLOW_TRUST_DOCKER_ALIST") == "1" and parsed.hostname == "alist"


def common_connection_args() -> list[str]:
    args = ["--alist-url", alist_url(), "--username", os.getenv("ALIST_USERNAME", "admin")]
    if docker_loopback_bridge():
        args.append("--allow-insecure-http")
    return args
