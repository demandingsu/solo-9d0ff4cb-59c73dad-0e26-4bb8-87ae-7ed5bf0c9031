"""Runtime configuration for the collector API."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    db_path: str = os.environ.get("COLLECTOR_DB", "/data/collector.db")
    result_dir: str = os.environ.get("RESULT_DIR", "/data/results")
    tmp_dir: str = os.environ.get("TMP_DIR", "/data/tmp")

    poll_interval: float = float(os.environ.get("POLL_INTERVAL", "1.0"))
    http_timeout: float = float(os.environ.get("HTTP_TIMEOUT", "15.0"))

    # Reconciliation retries while the portal is unavailable (e.g. restarting).
    reconcile_max_attempts: int = _int("RECONCILE_MAX_ATTEMPTS", 10)
    reconcile_backoff: float = float(os.environ.get("RECONCILE_BACKOFF", "2.0"))

    # Download retries: every retry always hits the ORIGINAL portal task.
    download_max_attempts: int = _int("DOWNLOAD_MAX_ATTEMPTS", 5)
    download_backoff: float = float(os.environ.get("DOWNLOAD_BACKOFF", "2.0"))

    # Consecutive failed polls before the job is handed to a human.
    poll_error_limit: int = _int("POLL_ERROR_LIMIT", 30)
    # Overall wall-clock guard for a single job.
    job_timeout_seconds: int = _int("JOB_TIMEOUT_SECONDS", 3600)


settings = Settings()
