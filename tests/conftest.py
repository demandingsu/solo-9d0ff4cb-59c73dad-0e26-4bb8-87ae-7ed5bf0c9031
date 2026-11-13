"""Shared pytest fixtures: portal and collector run as isolated subprocesses.

Running each service in its own process mirrors the Docker Compose deployment
and avoids Python package-name collisions (both services expose ``app.main``).
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest_asyncio

ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class SubprocessServer:
    def __init__(self, app_dir: Path, port: int, env: dict[str, str],
                 ready_path: str):
        self.app_dir = app_dir
        self.port = port
        self.base_url = f"http://127.0.0.1:{port}"
        self._env = env
        self._ready_path = ready_path
        self._proc: subprocess.Popen | None = None

    def start(self) -> None:
        env = os.environ.copy()
        env.update(self._env)
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port),
             "--log-level", "error"],
            cwd=str(self.app_dir),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self._wait_ready()

    def _wait_ready(self, attempts: int = 60) -> None:
        last_error = ""
        for _ in range(attempts):
            if self._proc and self._proc.poll() is not None:
                output = self._proc.stdout.read().decode("utf-8", "replace")
                raise RuntimeError(
                    f"server {self.app_dir} exited early:\n{output}")
            try:
                resp = httpx.get(self.base_url + self._ready_path, timeout=1)
                if resp.status_code < 500:
                    return
            except httpx.HTTPError as exc:
                last_error = str(exc)
            time.sleep(0.25)
        raise RuntimeError(
            f"server on port {self.port} not ready: {last_error}")

    def stop(self) -> None:
        if self._proc is None:
            return
        self._proc.terminate()
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=5)
        self._proc = None

    def restart(self) -> None:
        """Stop and start again reusing the same data directory/env."""
        self.stop()
        time.sleep(0.5)
        self.start()


@pytest_asyncio.fixture()
async def portal(tmp_path):
    portal_dir = tmp_path / "portal-data"
    portal_dir.mkdir()
    server = SubprocessServer(
        ROOT / "portal",
        _free_port(),
        {"PORTAL_DATA": str(portal_dir)},
        ready_path="/tasks",
    )
    server.start()
    try:
        yield server
    finally:
        server.stop()


@pytest_asyncio.fixture()
async def collector(tmp_path):
    data = tmp_path / "collector-data"
    (data / "results").mkdir(parents=True)
    (data / "tmp").mkdir(parents=True)
    server = SubprocessServer(
        ROOT / "collector",
        _free_port(),
        {
            "COLLECTOR_DB": str(data / "collector.db"),
            "RESULT_DIR": str(data / "results"),
            "TMP_DIR": str(data / "tmp"),
            "POLL_INTERVAL": "0.2",
            "RECONCILE_BACKOFF": "0.2",
            "DOWNLOAD_BACKOFF": "0.2",
            "RECONCILE_MAX_ATTEMPTS": "20",
            "HTTP_TIMEOUT": "5",
        },
        ready_path="/health",
    )
    server.start()
    try:
        yield server
    finally:
        server.stop()
