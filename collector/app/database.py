"""SQLite persistence for collection jobs.

The database is the durable state machine: every state transition is
checkpointed before network work continues, so a service restart can resume
any job purely from the rows in this table. No in-memory state is required for
correctness (in-memory locks only deduplicate concurrent requests).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id                 TEXT PRIMARY KEY,
    portal_url         TEXT NOT NULL,
    batch_no           TEXT NOT NULL,
    portal_task_id     TEXT,
    status             TEXT NOT NULL,
    review_code        TEXT,
    review_reason      TEXT,
    retries            INTEGER NOT NULL DEFAULT 0,
    result_path        TEXT,
    result_sha256      TEXT,
    result_size        INTEGER,
    content_type       TEXT,
    last_error         TEXT,
    submit_payload     TEXT NOT NULL DEFAULT '{}',
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    UNIQUE (portal_url, batch_no)
);

CREATE TABLE IF NOT EXISTS attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL REFERENCES jobs(id),
    phase       TEXT NOT NULL,
    outcome     TEXT NOT NULL,
    detail      TEXT,
    at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
"""

# States the job state machine can be in.
SUBMITTING = "SUBMITTING"        # export form is being posted
RECONCILING = "RECONCILING"      # response lost: querying the portal task table
POLLING = "POLLING"              # portal task known, waiting for the file
DOWNLOADING = "DOWNLOADING"      # pulling the result file from the portal task
COMPLETED = "COMPLETED"          # file fully retrieved and verified
MANUAL_REVIEW = "MANUAL_REVIEW"  # blocked, requires a human decision
FAILED = "FAILED"                # terminal failure not resolvable by a human

TERMINAL_STATES = (COMPLETED, MANUAL_REVIEW, FAILED)
ACTIVE_STATES = (SUBMITTING, RECONCILING, POLLING, DOWNLOADING)

# Review codes explaining why a job needs a human.
REVIEW_PORTAL_ERROR = "PORTAL_ERROR"
REVIEW_TASK_NOT_FOUND = "TASK_NOT_FOUND"
REVIEW_DUPLICATE_TASKS = "DUPLICATE_TASKS"
REVIEW_TASK_FAILED = "PORTAL_TASK_FAILED"
REVIEW_DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
REVIEW_PORTAL_UNREACHABLE = "PORTAL_UNREACHABLE"
REVIEW_TIMEOUT = "JOB_TIMEOUT"


def utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        assert self._conn is not None, "Database.connect() was not called"
        return self._conn

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        async with self.conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ) as cur:
            row = await cur.fetchone()
        return _row_to_job(row) if row else None

    async def find_job(self, portal_url: str, batch_no: str) -> dict[str, Any] | None:
        async with self.conn.execute(
            "SELECT * FROM jobs WHERE portal_url = ? AND batch_no = ?",
            (portal_url, batch_no),
        ) as cur:
            row = await cur.fetchone()
        return _row_to_job(row) if row else None

    async def list_jobs(self, status: str | None = None) -> list[dict[str, Any]]:
        if status:
            query = "SELECT * FROM jobs WHERE status = ? ORDER BY created_at"
            params: Iterable[Any] = (status,)
        else:
            query = "SELECT * FROM jobs ORDER BY created_at"
            params = ()
        async with self.conn.execute(query, params) as cur:
            rows = await cur.fetchall()
        return [_row_to_job(r) for r in rows]

    async def insert_job(self, job: dict[str, Any]) -> bool:
        payload = json.dumps(job.get("submit_payload", {}), ensure_ascii=False)
        try:
            await self.conn.execute(
                """INSERT INTO jobs (id, portal_url, batch_no, portal_task_id,
                       status, review_code, review_reason, retries, result_path,
                       result_sha256, result_size, content_type, last_error,
                       submit_payload, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (job["id"], job["portal_url"], job["batch_no"],
                 job.get("portal_task_id"), job["status"],
                 job.get("review_code"), job.get("review_reason"),
                 job.get("retries", 0), job.get("result_path"),
                 job.get("result_sha256"), job.get("result_size"),
                 job.get("content_type"), job.get("last_error"),
                 payload, job["created_at"], job["updated_at"]),
            )
            await self.conn.commit()
            return True
        except aiosqlite.IntegrityError:
            # Duplicate (portal_url, batch_no) inserted concurrently.
            return False

    async def update_job(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = utcnow()
        if "submit_payload" in fields and not isinstance(
            fields["submit_payload"], str
        ):
            fields["submit_payload"] = json.dumps(
                fields["submit_payload"], ensure_ascii=False
            )
        assignments = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [job_id]
        await self.conn.execute(
            f"UPDATE jobs SET {assignments} WHERE id = ?", values
        )
        await self.conn.commit()

    async def log_attempt(self, job_id: str, phase: str, outcome: str,
                          detail: str = "") -> None:
        await self.conn.execute(
            """INSERT INTO attempts (job_id, phase, outcome, detail, at)
               VALUES (?,?,?,?,?)""",
            (job_id, phase, outcome, detail, utcnow()),
        )
        await self.conn.commit()

    async def list_attempts(self, job_id: str) -> list[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT * FROM attempts WHERE job_id = ? ORDER BY id", (job_id,)
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_active_jobs(self) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in ACTIVE_STATES)
        async with self.conn.execute(
            f"SELECT * FROM jobs WHERE status IN ({placeholders})",
            ACTIVE_STATES,
        ) as cur:
            rows = await cur.fetchall()
        return [_row_to_job(r) for r in rows]


def _row_to_job(row: aiosqlite.Row) -> dict[str, Any]:
    """Convert a row to a dict and parse the JSON form payload back to a dict."""
    job = dict(row)
    try:
        job["submit_payload"] = json.loads(job.get("submit_payload") or "{}")
    except (TypeError, ValueError):
        job["submit_payload"] = {}
    return job
