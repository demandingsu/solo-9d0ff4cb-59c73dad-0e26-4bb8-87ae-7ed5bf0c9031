"""Job state machine driving portal submission, reconciliation and download.

Safety contract implemented here:

* A portal export form is submitted at most once per job. If the submission
  response is lost (portal restart, dropped connection) the job moves to
  reconciliation and reads the portal task table instead of resubmitting.
* Reconciliation continues the job only when exactly ONE task for the batch
  exists. Zero tasks or several tasks both go to MANUAL_REVIEW; nothing is
  ever submitted a second time automatically.
* Download retries always reference the same stored portal task id. A result
  file is published only after a complete, length/checksum-verified download;
  incomplete files stay in the temp directory and are never downloadable.
* Every transition is persisted before the next network action, so the
  background machinery can be rebuilt from SQLite after a process restart.
"""
from __future__ import annotations

import asyncio
import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings
from .database import (
    ACTIVE_STATES,
    COMPLETED,
    DOWNLOADING,
    Database,
    MANUAL_REVIEW,
    POLLING,
    RECONCILING,
    REVIEW_DOWNLOAD_FAILED,
    REVIEW_DUPLICATE_TASKS,
    REVIEW_PORTAL_ERROR,
    REVIEW_PORTAL_UNREACHABLE,
    REVIEW_TASK_FAILED,
    REVIEW_TASK_NOT_FOUND,
    REVIEW_TIMEOUT,
    SUBMITTING,
    utcnow,
)
from .portal_client import PortalClient, PortalError, PortalRejected, PortalUnreachable

# Portal-side task status values (see portal/app/main.py).
PORTAL_PENDING = "PENDING"
PORTAL_RUNNING = "RUNNING"
PORTAL_DONE = "DONE"
PORTAL_FAILED = "FAILED"


def _age_seconds(iso_ts: str) -> float:
    created = datetime.fromisoformat(iso_ts)
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - created).total_seconds()


class JobManager:
    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.settings = settings
        self._key_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()

    # ------------------------------------------------------------------ locks
    def _lock_for(self, portal_url: str, batch_no: str) -> asyncio.Lock:
        key = (portal_url, batch_no)
        lock = self._key_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._key_locks[key] = lock
        return lock

    def _spawn(self, job_id: str, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks[job_id] = task
        task.add_done_callback(lambda _: self._tasks.pop(job_id, None))

    # ---------------------------------------------------------------- startup
    async def resume_on_startup(self) -> int:
        """Rebuild background work from durable rows after a restart."""
        active = await self.db.list_active_jobs()
        for job in active:
            # A job stuck in SUBMITTING never got a confirmed response: treat it
            # exactly like a lost submission and reconcile from the task table.
            if job["status"] == SUBMITTING:
                await self.db.update_job(job["id"], status=RECONCILING)
                job["status"] = RECONCILING
            await self.db.log_attempt(
                job["id"], "startup", "resume",
                f"resumed from state {job['status']} after service start")
            self._spawn(job["id"], self._run_job(job["id"]))
        return len(active)

    async def shutdown(self) -> None:
        self._stop.set()
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------------------------------------------ create path
    async def get_or_create_job(
        self, portal_url: str, batch_no: str, form_options: dict | None = None
    ) -> tuple[dict[str, Any], bool]:
        """Return (job, reused). Concurrent/duplicate calls share one job."""
        normalized = PortalClient._validate_base_url(portal_url)
        existing = await self.db.find_job(normalized, batch_no)
        if existing is not None:
            return existing, True

        async with self._lock_for(normalized, batch_no):
            existing = await self.db.find_job(normalized, batch_no)
            if existing is not None:
                return existing, True

            job = {
                "id": uuid.uuid4().hex,
                "portal_url": normalized,
                "batch_no": batch_no,
                "portal_task_id": None,
                "status": SUBMITTING,
                "submit_payload": PortalClient.build_form(batch_no, form_options),
                "created_at": utcnow(),
                "updated_at": utcnow(),
            }
            inserted = await self.db.insert_job(job)
            if not inserted:
                # Raced with another process/worker sharing the SQLite file.
                existing = await self.db.find_job(normalized, batch_no)
                assert existing is not None
                return existing, True

            await self.db.log_attempt(
                job["id"], "submit", "start",
                f"POST /export to {normalized} for batch {batch_no}")
            self._spawn(job["id"], self._run_job(job["id"]))
            return await self.db.get_job(job["id"]), False

    # ------------------------------------------------------------- state loop
    async def _run_job(self, job_id: str) -> None:
        try:
            job = await self.db.get_job(job_id)
            if job is None or job["status"] not in ACTIVE_STATES:
                return
            if _age_seconds(job["created_at"]) > self.settings.job_timeout_seconds:
                await self._manual_review(
                    job_id, REVIEW_TIMEOUT,
                    "job exceeded the maximum allowed wall-clock duration")
                return

            if job["status"] == SUBMITTING:
                await self._submit(job)
                job = await self.db.get_job(job_id)

            if job and job["status"] == RECONCILING:
                await self._reconcile(job_id)
                job = await self.db.get_job(job_id)

            if job and job["status"] == POLLING:
                await self._poll(job_id)
                job = await self.db.get_job(job_id)

            if job and job["status"] == DOWNLOADING:
                await self._download(job_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # defensive: never leave a dangling task
            await self.db.log_attempt(
                job_id, "loop", "error", f"{type(exc).__name__}: {exc}")
            await self._manual_review(
                job_id, REVIEW_PORTAL_ERROR,
                f"unexpected internal error: {type(exc).__name__}: {exc}")

    async def _submit(self, job: dict[str, Any]) -> None:
        client = PortalClient(job["portal_url"], self.settings.http_timeout)
        try:
            task_id = await client.submit_export(job["submit_payload"])
        except PortalRejected as exc:
            # The portal refused us outright; do not retry, do not resubmit.
            await self.db.log_attempt(
                job["id"], "submit", "rejected", str(exc))
            await self._manual_review(
                job["id"], REVIEW_PORTAL_ERROR,
                f"portal rejected the export form: {exc}")
            return
        except PortalUnreachable as exc:
            # Accepted-but-lost, connect failure, or timeout. The only safe
            # move is to inspect the portal task table; never resubmit.
            await self.db.log_attempt(
                job["id"], "submit", "response_lost", str(exc))
            await self.db.update_job(
                job["id"], status=RECONCILING, last_error=str(exc))
            await self._reconcile(job["id"])
            return

        await self.db.log_attempt(
            job["id"], "submit", "accepted", f"portal task {task_id}")
        await self.db.update_job(
            job["id"], portal_task_id=task_id, status=POLLING,
            last_error="")
        await self._poll(job["id"])

    # ---------------------------------------------------------- reconciliation
    async def _reconcile(self, job_id: str) -> None:
        """Recover a lost submission by reading the portal task table.

        Exactly one task for the batch -> adopt it. Zero or several -> manual
        review. The export form is never posted again.
        """
        job = await self.db.get_job(job_id)
        client = PortalClient(job["portal_url"], self.settings.http_timeout)

        for attempt in range(1, self.settings.reconcile_max_attempts + 1):
            if self._stop.is_set():
                return
            try:
                tasks = await client.list_tasks(job["batch_no"])
            except PortalRejected as exc:
                await self.db.log_attempt(
                    job_id, "reconcile", "rejected", str(exc))
                await self._manual_review(
                    job_id, REVIEW_PORTAL_ERROR,
                    f"portal rejected task table query: {exc}")
                return
            except PortalUnreachable as exc:
                # Portal might still be restarting: back off and retry the
                # QUERY only. No submission happens here.
                await self.db.log_attempt(
                    job_id, "reconcile", "unreachable",
                    f"attempt {attempt}: {exc}")
                if attempt >= self.settings.reconcile_max_attempts:
                    await self._manual_review(
                        job_id, REVIEW_PORTAL_UNREACHABLE,
                        "portal task table stayed unreachable during "
                        f"reconciliation: {exc}")
                    return
                await asyncio.sleep(
                    self.settings.reconcile_backoff * attempt)
                continue

            if len(tasks) == 1:
                task = tasks[0]
                task_id = task.get("task_id", "")
                if not task_id:
                    await self._manual_review(
                        job_id, REVIEW_PORTAL_ERROR,
                        "portal task row has no task id")
                    return
                await self.db.log_attempt(
                    job_id, "reconcile", "adopted",
                    f"exactly one task found, adopting {task_id}")
                await self.db.update_job(
                    job_id, portal_task_id=task_id, status=POLLING,
                    last_error="")
                await self._poll(job_id)
                return

            if len(tasks) == 0:
                await self.db.log_attempt(
                    job_id, "reconcile", "no_task",
                    "no portal task for this batch; submission may not have "
                    "been accepted")
                await self._manual_review(
                    job_id, REVIEW_TASK_NOT_FOUND,
                    "No task for this batch exists in the portal task table. "
                    "The export may not have been accepted; resubmission "
                    "requires a human decision to avoid duplicate exports.")
                return

            await self.db.log_attempt(
                job_id, "reconcile", "duplicate",
                f"{len(tasks)} tasks found: "
                + ", ".join(t.get("task_id", "?") for t in tasks))
            await self._manual_review(
                job_id, REVIEW_DUPLICATE_TASKS,
                f"{len(tasks)} tasks for this batch exist on the portal "
                f"({', '.join(t.get('task_id', '?') for t in tasks)}). "
                "A human must decide which one to follow; automatic "
                "resubmission is disabled.")
            return

    # ------------------------------------------------------------------ poll
    async def _poll(self, job_id: str) -> None:
        job = await self.db.get_job(job_id)
        client = PortalClient(job["portal_url"], self.settings.http_timeout)
        consecutive_errors = 0

        while not self._stop.is_set():
            job = await self.db.get_job(job_id)
            if job["status"] != POLLING:
                return
            if _age_seconds(job["created_at"]) > self.settings.job_timeout_seconds:
                await self._manual_review(
                    job_id, REVIEW_TIMEOUT,
                    "portal task did not finish before the job timeout")
                return

            try:
                task = await client.get_task(job["portal_task_id"])
                consecutive_errors = 0
            except PortalRejected as exc:
                await self.db.log_attempt(
                    job_id, "poll", "rejected", str(exc))
                await self._manual_review(
                    job_id, REVIEW_PORTAL_ERROR,
                    f"portal rejected status query: {exc}")
                return
            except PortalUnreachable as exc:
                consecutive_errors += 1
                await self.db.log_attempt(
                    job_id, "poll", "error",
                    f"consecutive={consecutive_errors}: {exc}")
                if consecutive_errors >= self.settings.poll_error_limit:
                    await self._manual_review(
                        job_id, REVIEW_PORTAL_UNREACHABLE,
                        f"portal unreachable for {consecutive_errors} status "
                        f"queries: {exc}")
                    return
                await asyncio.sleep(self.settings.poll_interval)
                continue

            state = task.get("status", "")
            if state == PORTAL_DONE:
                if not task.get("filename"):
                    await self._manual_review(
                        job_id, REVIEW_PORTAL_ERROR,
                        "portal reports DONE but no filename is attached")
                    return
                await self.db.log_attempt(
                    job_id, "poll", "ready",
                    f"portal task {job['portal_task_id']} is DONE")
                await self.db.update_job(job_id, status=DOWNLOADING)
                await self._download(job_id)
                return
            if state == PORTAL_FAILED:
                await self._manual_review(
                    job_id, REVIEW_TASK_FAILED,
                    f"portal task failed: {task.get('error', 'no detail')}")
                return

            # PENDING / RUNNING (and any unknown state): keep waiting.
            await asyncio.sleep(self.settings.poll_interval)

    # --------------------------------------------------------------- download
    async def _download(self, job_id: str) -> None:
        """Fetch the result of the original portal task, with bounded retries.

        Every retry reuses the stored portal_task_id; a different task is
        never contacted. The final file appears only after the full body has
        been streamed, length-checked and checksummed.
        """
        job = await self.db.get_job(job_id)
        client = PortalClient(job["portal_url"], self.settings.http_timeout)
        result_dir = Path(self.settings.result_dir)
        tmp_dir = Path(self.settings.tmp_dir)
        result_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        task_id = job["portal_task_id"]
        final_name = f"{job['batch_no']}_{task_id}.csv"
        final_path = result_dir / final_name

        for attempt in range(1, self.settings.download_max_attempts + 1):
            if self._stop.is_set():
                return
            tmp_name = f"{job['id']}.attempt{attempt}.part"
            try:
                tmp_path, content_type, size = await client.download(
                    task_id, tmp_dir, tmp_name)
                checksum = hashlib.sha256(tmp_path.read_bytes()).hexdigest()

                # Publish atomically: until this rename succeeds nothing
                # downloadable exists for the job.
                target = final_path
                tmp_path.replace(target)

                await self.db.update_job(
                    job_id,
                    status=COMPLETED,
                    result_path=str(target),
                    result_sha256=checksum,
                    result_size=size,
                    content_type=content_type,
                    last_error="",
                )
                await self.db.log_attempt(
                    job_id, "download", "complete",
                    f"{size} bytes sha256={checksum} "
                    f"(attempt {attempt}, task {task_id})")
                return
            except PortalError as exc:
                await self.db.update_job(
                    job_id, retries=attempt,
                    last_error=f"download attempt {attempt} failed: {exc}")
                await self.db.log_attempt(
                    job_id, "download", "retry",
                    f"attempt {attempt}/{self.settings.download_max_attempts} "
                    f"against task {task_id}: {exc}")
                if attempt >= self.settings.download_max_attempts:
                    break
                await asyncio.sleep(
                    self.settings.download_backoff * attempt)

        await self._manual_review(
            job_id, REVIEW_DOWNLOAD_FAILED,
            f"all {self.settings.download_max_attempts} download attempts "
            f"against portal task {task_id} failed: {job['last_error']}")

    # --------------------------------------------------------------- adoption
    async def adopt_task(self, job_id: str, portal_task_id: str) -> dict:
        """Human override: point a MANUAL_REVIEW job at a chosen portal task."""
        job = await self.db.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if job["status"] != MANUAL_REVIEW:
            raise ValueError(
                f"job is {job['status']}; only MANUAL_REVIEW jobs can be adopted")
        if not portal_task_id.strip():
            raise ValueError("portal_task_id must not be empty")

        client = PortalClient(job["portal_url"], self.settings.http_timeout)
        # Validate that the human-selected task really exists on the portal.
        await client.get_task(portal_task_id.strip())
        await self.db.log_attempt(
            job_id, "adopt", "manual",
            f"human selected portal task {portal_task_id} "
            f"(previous review: {job['review_code']})")
        await self.db.update_job(
            job_id, portal_task_id=portal_task_id.strip(),
            status=POLLING, review_code="", review_reason="",
            last_error="")
        self._spawn(job_id, self._run_job(job_id))
        return await self.db.get_job(job_id)

    # ---------------------------------------------------------------- helpers
    async def _manual_review(
        self, job_id: str, code: str, reason: str
    ) -> None:
        await self.db.update_job(
            job_id, status=MANUAL_REVIEW, review_code=code,
            review_reason=reason)
        await self.db.log_attempt(job_id, "review", code, reason)
